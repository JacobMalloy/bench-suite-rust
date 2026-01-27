use anyhow::{Context, Result};
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::BufReader,
    path::{self, PathBuf},
};

use bench_suite_types::{BenchSuiteConfig, BenchSuiteRun};

pub struct BenchSuiteTasks {
    runs: HashMap<u64, BenchSuiteRun>,
    collections: HashMap<String, BenchSuiteConfig>,
    location: PathBuf,
}

impl BenchSuiteTasks {
    pub fn new(config_file_path: &str) -> Result<Self> {
        let task_file = BufReader::new(File::open(config_file_path).context(std::format!(
            "Failed to open task file {}",
            config_file_path
        ))?);

        let task_config: BenchSuiteTaskConfig =
            serde_json::from_reader(task_file).context("Failed to parse task_file")?;

        let bench_suite_location = path::Path::new(&task_config.location);
        let status_location = bench_suite_location.join("status.json");
        let status_reader = BufReader::new(
            File::open(&status_location).context("Failed to open status file specified")?,
        );
        let status: BenchSuiteStatus =
            serde_json::from_reader(status_reader).context("Failed to parse status file")?;

        let BenchSuiteStatus {
            bench_index: _,
            benchmark_runs,
        } = status;

        let benchmark_runs: Result<HashMap<u64, BenchSuiteRun>, std::num::ParseIntError> =
            benchmark_runs
                .into_iter()
                .map(|(key, val)| key.parse::<u64>().map(|parsed| (parsed, val)))
                .collect();
        let benchmark_runs =
            benchmark_runs.context(std::format!("The runs in {}", status_location.display()))?;

        Ok(Self {
            runs: benchmark_runs,
            collections: task_config.collect,
            location: bench_suite_location.to_path_buf(),
        })
    }

    pub fn collection_names(&self) -> impl Iterator<Item = &str> {
        self.collections.keys().map(|s| s.as_str())
    }

    pub fn get_path(&self) -> &PathBuf {
        &self.location
    }

    pub fn tar_file_path(&self, id: u64) -> PathBuf {
        self.location
            .join("runs")
            .join(format!("{:016X}.tar.xz", id))
    }

    pub fn to_collect(&self) -> impl Iterator<Item = (u64, &BenchSuiteRun, Vec<&str>, PathBuf)> {
        self.runs.iter().filter_map(|(id, config)| {
            let tmp: HashSet<&str> = self
                .collections
                .iter()
                .filter_map(|(location, collect_vals)| {
                    collect_vals.contains(config).then_some(location.as_str())
                })
                .collect();
            let tar_path = self.tar_file_path(*id);
            (!tmp.is_empty()).then_some((*id, config, tmp.into_iter().collect(), tar_path))
        })
    }
}

#[derive(Debug, Deserialize)]
struct BenchSuiteTaskConfig {
    location: String,
    collect: HashMap<String, BenchSuiteConfig>,
}

#[derive(Debug, Deserialize)]
struct BenchSuiteStatus {
    bench_index: f64,
    benchmark_runs: HashMap<String, BenchSuiteRun>,
}
