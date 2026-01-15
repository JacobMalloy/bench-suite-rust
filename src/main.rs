use anyhow::{Context, Result, anyhow};
use polars::prelude::*;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::hash::Hash;
use std::io::BufReader;
use std::path::Path;
use std::sync::Mutex;
use std::sync::mpsc;

use bench_suite_collect_results::{BenchSuiteCollect, FileInfo};
use bench_suite_config::BenchSuiteTasks;
use bench_suite_types::BenchSuiteRun;

struct ToCollectQueue<'a, T>
where
    T: Iterator<Item = (u64, &'a BenchSuiteRun, Vec<&'a str>)>,
{
    it: Mutex<T>,
}

impl<'a, T> ToCollectQueue<'a, T>
where
    T: Iterator<Item = (u64, &'a BenchSuiteRun, Vec<&'a str>)>,
{
    fn new(input: T) -> Self {
        Self {
            it: Mutex::new(input),
        }
    }

    fn consume(&self) -> Option<(u64, &'a BenchSuiteRun, Vec<&'a str>)> {
        let mut guard = self.it.lock().unwrap();
        guard.next()
    }
}

#[derive(Clone)]
struct TableSubmitter<'scope, 'env> {
    source: Arc<Mutex<HashMap<(String, String), mpsc::Sender<DataFrame>>>>,
    local: HashMap<(String, String), mpsc::Sender<DataFrame>>,
    scope: &'scope std::thread::Scope<'scope, 'env>,
    base_location: &'scope str,
}

fn parquet_thread(rx: std::sync::mpsc::Receiver<DataFrame>, location: String) {
    let mut index: u64 = 0;
    let mut data: Option<DataFrame> = None;
    while let Ok(msg) = rx.recv() {
        match &data {
            Some(v) => {
                v.vstack(&msg).unwrap();
            }
            None => {
                data = Some(msg);
            }
        };
        if let Some(v) = &mut data
            && v.estimated_size() >= 750 * 1024 * 1024
        {
            ParquetWriter::new(File::create(format!("{}_{}.parquet", location, index)).unwrap())
                .with_compression(ParquetCompression::Zstd(Some(
                    ZstdLevel::try_new(15).unwrap(),
                )))
                .finish(v)
                .unwrap();
            data = None;
            index += 1;
        }
    }
    if let Some(v) = &mut data {
        ParquetWriter::new(File::create(format!("{}_{}.parquet", location, index)).unwrap())
            .with_compression(ParquetCompression::Zstd(Some(
                ZstdLevel::try_new(15).unwrap(),
            )))
            .finish(v)
            .unwrap();
    }
}

impl<'scope, 'env> TableSubmitter<'scope, 'env>
where
    (String, String): Hash + std::cmp::Eq + Clone,
{
    pub fn submit(
        &mut self,
        key: (String, String),
        value: DataFrame,
    ) -> std::result::Result<(), std::sync::mpsc::SendError<DataFrame>> {
        let chan = self.local.entry(key.clone()).or_insert_with(|| {
            let mut locked = self.source.lock().unwrap();
            locked
                .entry(key.clone())
                .or_insert_with(|| {
                    let (tx, rx) = mpsc::channel();
                    self.scope.spawn(|| {
                        let path = Path::new(self.base_location).join(key.0).join(key.1);
                        parquet_thread(rx, path.to_str().unwrap().to_string());
                    });
                    tx
                })
                .clone()
        });
        chan.send(value)
    }
    pub fn new(
        scope: &'scope std::thread::Scope<'scope, 'env>,
        base_location: &'scope str,
    ) -> Self {
        Self {
            source: Arc::new(Mutex::new(HashMap::new())),
            local: HashMap::new(),
            scope,
            base_location,
        }
    }
}

fn process_run(run: &BenchSuiteRun) -> Result<HashMap<String, DataFrame>> {
    let tarfile = BufReader::new(File::open(&run.tar_file)?);
    let tarfile = xz2::read::XzDecoder::new(tarfile);
    let mut tarfile = tar::Archive::new(tarfile);

    let entries = tarfile
        .entries()
        .context("Failed to get entries from tar file")?;

    let mut collectors: Vec<Box<dyn BenchSuiteCollect>> =
        bench_suite_benchmark_configs::get_collect_config(&run.benchmark)?
            .iter()
            .map(|x| x())
            .collect();
    for file in entries {
        let file = file.context("Failed to get file from tar")?;
        let path = file
            .path()
            .context("Failed to get the path from tar file")?
            .to_str()
            .context("Failed to turn path to string".to_string())?
            .to_string();
        let mut file_info = FileInfo::new(path.as_str(), file);

        for i in collectors.iter_mut() {
            i.process_file(run, &mut file_info)?;
        }
    }

    let mut return_map: HashMap<String, DataFrame> = HashMap::new();
    for collector in collectors {
        let res = BenchSuiteCollect::get_result(collector, run);
        for (key, val) in res? {
            if return_map.insert(key, val).is_some() {
                return Err(anyhow!(std::format!("Repeated the table name ")));
            }
        }
    }

    Ok(return_map)
}

fn process_thread<'a, T>(queue: &ToCollectQueue<'a, T>, mut submitter: TableSubmitter)
where
    T: Iterator<Item = (u64, &'a BenchSuiteRun, Vec<&'a str>)>,
{
    while let Some((id, run, paths)) = queue.consume() {
        let result = process_run(run);
        let (mut map, mut status) = match result {
            Ok(v) => (v, "success".to_string()),
            Err(e) => (HashMap::new(), e.to_string()),
        };

        if map.contains_key("status") {
            status = "Run emmitted a status table".to_string();
            map = HashMap::new();
        }

        for (key, mut val) in map.into_iter() {
            val.with_column(Series::new(
                PlSmallStr::from_static("id"),
                vec![id; val.height()],
            ))
            .unwrap();
            for p in paths.iter() {
                //should probably try not to clone on the last one
                submitter
                    .submit((p.to_string(), key.clone()), val.clone())
                    .unwrap();
            }
        }
    }
}

fn main() {
    let config_file_path = env::args()
        .nth(1)
        .expect("You need to provide a an argument for the path");

    let config = BenchSuiteTasks::new(&config_file_path).unwrap();

    let queue = ToCollectQueue::new(config.to_collect());

    std::thread::scope(|x| {
        let s = TableSubmitter::new(x, config.get_path().to_str().unwrap());
        for _ in 0..1 {
            let tmp_s = s.clone();
            x.spawn(|| {
                process_thread(&queue, tmp_s);
            });
        }
        drop(s)
    });
}
