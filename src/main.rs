use anyhow::{Context, Result, anyhow};
use polars::prelude::*;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Mutex;
use std::sync::mpsc;

use string_intern::Intern;

use bench_suite_collect_results::{BenchSuiteCollect, FileInfo};
use bench_suite_config::BenchSuiteTasks;
use bench_suite_types::BenchSuiteRun;

struct ToCollectQueue<'a, T>
where
    T: Iterator<Item = (u64, &'a BenchSuiteRun, Vec<&'a str>)>,
{
    it: Mutex<T>,
    pb: indicatif::ProgressBar,
}

impl<'a, T> ToCollectQueue<'a, T>
where
    T: Iterator<Item = (u64, &'a BenchSuiteRun, Vec<&'a str>)>,
{
    fn new(input: T, progress: indicatif::ProgressBar) -> Self {
        Self {
            it: Mutex::new(input),
            pb: progress,
        }
    }

    fn consume(&self) -> Option<(u64, &'a BenchSuiteRun, Vec<&'a str>)> {
        let mut guard = self.it.lock().unwrap();
        let tmp = guard.next();
        self.pb.tick();
        self.pb.inc(1);
        tmp
    }
}

#[derive(Clone)]
struct TableSubmitter<'scope, 'env> {
    source: Arc<Mutex<HashMap<(Intern, Intern), mpsc::Sender<DataFrame>>>>,
    local: HashMap<(Intern, Intern), mpsc::Sender<DataFrame>>,
    scope: &'scope std::thread::Scope<'scope, 'env>,
    base_location: &'scope str,
}

fn parquet_thread(rx: std::sync::mpsc::Receiver<DataFrame>, location: std::path::PathBuf) {
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
            ParquetWriter::new(File::create(format!("{}_{}.parquet", location.display(), index)).unwrap())
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
        ParquetWriter::new(File::create(format!("{}_{}.parquet", location.display(), index)).unwrap())
            .with_compression(ParquetCompression::Zstd(Some(
                ZstdLevel::try_new(15).unwrap(),
            )))
            .finish(v)
            .unwrap();
    }
}

impl<'scope, 'env> TableSubmitter<'scope, 'env> {
    pub fn submit(
        &mut self,
        key: (Intern, Intern),
        value: DataFrame,
    ) -> std::result::Result<(), std::sync::mpsc::SendError<DataFrame>> {
        let base_location = self.base_location;
        let scope = self.scope;
        let source = &self.source;
        let chan = self.local.entry(key).or_insert_with(|| {
            let mut locked = source.lock().unwrap();
            locked
                .entry(key)
                .or_insert_with(|| {
                    let (tx, rx) = mpsc::channel();
                    scope.spawn(move || {
                        let path = Path::new(base_location).join(key.0).join(key.1);
                        parquet_thread(rx, path);
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

fn process_run(run: &BenchSuiteRun) -> Result<HashMap<Intern, DataFrame>> {
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

    let mut parsing_issues: Vec<String> = Vec::new();

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
            if let Err(e) = i.process_file(run, &mut file_info) {
                parsing_issues.push(format!("process_file({}): {}", path, e));
            }
        }
    }

    let mut return_map: HashMap<Intern, DataFrame> = HashMap::new();
    for collector in collectors {
        match BenchSuiteCollect::get_result(collector, run) {
            Ok(results) => {
                for (key, val) in results {
                    if return_map.insert(key, val).is_some() {
                        return Err(anyhow!(std::format!("Repeated the table name ")));
                    }
                }
            }
            Err(e) => {
                parsing_issues.push(format!("get_result: {}", e));
            }
        }
    }

    // Get or create the status DataFrame, then add parse_status column
    let status_df = return_map.entry(Intern::from_static("status")).or_insert_with(|| {
        parsing_issues.push("no status file".to_string());
        df!["status" => &["failed no status"]].unwrap()
    });

    let parse_status: Option<String> = if parsing_issues.is_empty() {
        None
    } else {
        Some(parsing_issues.join("; "))
    };
    status_df.with_column(Column::new("parse_status".into(), &[parse_status]))?;

    Ok(return_map)
}

fn process_thread<'a, T>(queue: &ToCollectQueue<'a, T>, mut submitter: TableSubmitter)
where
    T: Iterator<Item = (u64, &'a BenchSuiteRun, Vec<&'a str>)>,
{
    while let Some((id, run, paths)) = queue.consume() {
        let map = match process_run(run) {
            Ok(v) => v,
            Err(e) => {
                // process_run itself failed - create a status DataFrame with the error
                let status_df = df![
                    "status" => &["failed no status"],
                    "parse_status" => &[Some(e.to_string())],
                ]
                .unwrap();
                HashMap::from([(Intern::new("status"), status_df)])
            }
        };

        for (key, mut val) in map.into_iter() {
            val.with_column(Series::new(
                PlSmallStr::from_static("id"),
                vec![id; val.height()],
            ))
            .unwrap();
            for p in paths.iter() {
                //should probably try not to clone on the last one
                submitter
                    .submit((Intern::new(*p), key), val.clone())
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

    let progress = indicatif::MultiProgress::new();
    let main_progress = progress.add(indicatif::ProgressBar::new_spinner());
    main_progress.set_style(
        indicatif::ProgressStyle::default_spinner()
            .template("{spinner:.cyan} [{human_pos}] [{elapsed_precise}] {msg}")
            .unwrap()
            .tick_strings(&["▹▹▹▹▹", "▸▹▹▹▹", "▹▸▹▹▹", "▹▹▸▹▹", "▹▹▹▸▹", "▹▹▹▹▸"]),
    );
    main_progress.set_message("TodoStream...");

    let queue = ToCollectQueue::new(config.to_collect(), main_progress);
    std::thread::scope(|x| {
        let s = TableSubmitter::new(x, config.get_path().to_str().unwrap());
        for _ in 0..10 {
            let tmp_s = s.clone();
            x.spawn(|| {
                process_thread(&queue, tmp_s);
            });
        }
        drop(s)
    });
}
