use bench_suite_config::BenchSuiteTasks;
use std::env;

fn main() {
    let argv1 = env::args().nth(1).expect("Tasks file path is required");

    let task = BenchSuiteTasks::new(&argv1).expect("Failed to parse tasks file");
}
