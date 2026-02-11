use bench_suite_collect_results::BenchSuiteCollect;
use bench_suite_collector_config::BenchSuiteCollectConfig;
use bench_suite_collector_dacapo_iteration::BenchSuiteCollectDacapoIteration;
use bench_suite_collector_dacapo_latency::BenchSuiteCollectDacapoLatency;
use bench_suite_collector_java_threads::BenchSuiteCollectJavaThreads;
use bench_suite_collector_status::BenchSuiteCollectStatus;
use bench_suite_collector_threadstat::BenchSuiteCollectThreadstat;
use bench_suite_collector_time::BenchSuiteCollectTime;
use bench_suite_collector_system_load::BenchSuiteCollectSystemLoad;
use bench_suite_collector_zgc_phases::BenchSuiteCollectZgcPhases;

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

const DACAPO_SAMPLES2_CONFIG: [fn() -> Box<dyn BenchSuiteCollect>; 9] = [
    BenchSuiteCollectConfig::boxed,
    BenchSuiteCollectTime::boxed,
    BenchSuiteCollectDacapoIteration::boxed,
    BenchSuiteCollectDacapoLatency::boxed,
    BenchSuiteCollectJavaThreads::boxed,
    BenchSuiteCollectStatus::boxed,
    BenchSuiteCollectSystemLoad::boxed,
    BenchSuiteCollectThreadstat::boxed,
    BenchSuiteCollectZgcPhases::boxed,
];

const MARK_ABUSE_CONFIG: [fn() -> Box<dyn BenchSuiteCollect>; 7] = [
    BenchSuiteCollectConfig::boxed,
    BenchSuiteCollectTime::boxed,
    BenchSuiteCollectJavaThreads::boxed,
    BenchSuiteCollectStatus::boxed,
    BenchSuiteCollectSystemLoad::boxed,
    BenchSuiteCollectThreadstat::boxed,
    BenchSuiteCollectZgcPhases::boxed,
];

pub fn get_collect_config(bench: &str) -> Result<&'static [fn() -> Box<dyn BenchSuiteCollect>]> {
    Ok(match bench {
        "dacapo_samples2" => &DACAPO_SAMPLES2_CONFIG,
        "mark_abuse" => &MARK_ABUSE_CONFIG,
        _ => return Err(InvalidBenchmark::new(bench.to_string())),
    })
}
