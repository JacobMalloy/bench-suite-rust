use anyhow::{Context, Result};
use serde::Deserialize;
use std::{collections::HashMap, fs::File, io::BufReader, path};

use bench_suite_types::{BenchSuiteConfig,BenchSuiteRun};

pub struct BenchSuiteTasks {
    runs: HashMap<u64, BenchSuiteRun>,
    collections: HashMap<String, BenchSuiteConfig>,
}

impl BenchSuiteTasks {
    pub fn new(config_file_path: &str) -> Result<Self> {
        let task_file = BufReader::new(
            File::open(config_file_path)
                .with_context(|| std::format!("Failed to open task file {}", config_file_path))?,
        );

        let task_config: BenchSuiteTaskConfig =
            serde_json::from_reader(task_file).with_context(|| "Failed to parse task_file")?;

        let bench_suite_location = path::Path::new(&task_config.location);
        let status_location = bench_suite_location.join("status.json");
        let status_reader = BufReader::new(
            File::open(&status_location).with_context(|| "Failed to open status file specified")?,
        );
        let status: BenchSuiteStatus = serde_json::from_reader(status_reader)
            .with_context(|| "Failed to parse status file")?;

        let BenchSuiteStatus {
            bench_index,
            benchmark_runs,
        } = status;

        let benchmark_runs: Result<
            HashMap<u64, BenchSuiteRun>,
            std::num::ParseIntError,
        > = benchmark_runs
            .into_iter()
            .map(|(key, val)| key.parse::<u64>().map(|parsed| (parsed, val)))
            .collect();
        let benchmark_runs = benchmark_runs
            .with_context(|| std::format!("The runs in {}", status_location.display()))?;

        Ok(Self {
            runs: benchmark_runs,
            collections: task_config.collect,
        })
    }

    pub fn to_collect(&self) -> impl Iterator<Item=(&u64,&BenchSuiteRun,Vec<&str>)>{
        self.runs.iter().filter_map(|(id, config)| {
            let tmp:Vec<&str> = self.collections
                .iter()
                .filter_map(|(location, collect_vals)| collect_vals.contains(config).then_some(location.as_str())).collect();
            (!tmp.is_empty()).then_some((id,config,tmp))

        })
    }
}

#[derive(Debug, Deserialize)]
struct BenchSuiteTaskConfig {
    location: String,
    collect: HashMap<String,BenchSuiteConfig>,
}

#[derive(Debug, Deserialize)]
struct BenchSuiteStatus {
    bench_index: f64,
    benchmark_runs: HashMap<String,BenchSuiteRun>,
}



