use bench_suite_collect_results::BenchSuiteCollect;
use bench_suite_collector_config::BenchSuiteCollectConfig;
use bench_suite_collector_time::BenchSuiteCollectTime;

type Result<T> = std::result::Result<T, InvalidBenchmark>;

#[derive(Debug)]
pub struct InvalidBenchmark {
    name: String,
}

impl InvalidBenchmark {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl std::fmt::Display for InvalidBenchmark {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Benchmark {} does not havea collection config",
            self.name
        )
    }
}

impl std::error::Error for InvalidBenchmark {}

const DACAPO_SAMPLES2_CONFIG: [fn() -> Box<dyn BenchSuiteCollect>; 2] =
    [BenchSuiteCollectConfig::boxed, BenchSuiteCollectTime::boxed];

pub fn get_collect_config(bench: &str) -> Result<&'static [fn() -> Box<dyn BenchSuiteCollect>]> {
    Ok(match bench {
        "dacapo_samples2" => &DACAPO_SAMPLES2_CONFIG,
        _ => return Err(InvalidBenchmark::new(bench.to_string())),
    })
}
