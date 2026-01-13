use anyhow::{Context, Result};
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::sync::Mutex;

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

fn process_run(run: &BenchSuiteRun) -> Result<()> {
    let tarfile = BufReader::new(File::open(&run.tar_file)?);
    let tarfile = xz2::read::XzDecoder::new(tarfile);
    let mut tarfile = tar::Archive::new(tarfile);

    let entries = tarfile
        .entries()
        .context("Failed to get entries from tar file")?;

    for file in entries {
        let mut file = file.context("Failed to get file from tar")?;
        let path = file
            .path()
            .context("Failed to get the path from tar file")?
            .to_str()
            .context("Failed to turn path to string".to_string())?
            .to_string();
        let mut file_content = String::new();
        file.read_to_string(&mut file_content).context(format!(
            "Failed to read the file {} from {}",
            path, run.tar_file
        ))?;

    }

    Ok(())
}

fn process_thread<'a, T>(queue: &ToCollectQueue<'a, T>)
where
    T: Iterator<Item = (u64, &'a BenchSuiteRun, Vec<&'a str>)>,
{
    while let Some((id, run, paths)) = queue.consume() {
        println!("{} {:?} {:?}",id,run,paths);

    }
}

fn main() {
    let config_file_path = env::args()
        .nth(1)
        .expect("You need to provide a an argument for the path");

    let config = bench_suite_config::BenchSuiteTasks::new(&config_file_path).unwrap();

    let queue = ToCollectQueue::new(config.to_collect());

    std::thread::scope(|x| {
        for _ in 0..10 {
            x.spawn(|| {
                process_thread(&queue);
            });
        }
    });
}
