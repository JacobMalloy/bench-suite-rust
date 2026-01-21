use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use regex::Regex;
use std::sync::LazyLock;
use string_intern::Intern;

static ITERATION_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"DaCapo.*in (\d+) msec").unwrap()
});

#[derive(Debug, Default)]
pub struct BenchSuiteCollectDacapoIteration {
    iteration_df: Option<DataFrame>,
}

impl BenchSuiteCollectDacapoIteration {
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

impl BenchSuiteCollect for BenchSuiteCollectDacapoIteration {
    fn process_file(
        &mut self,
        _: &bench_suite_types::BenchSuiteRun,
        file: &mut dyn bench_suite_collect_results::FileInfoInterface,
    ) -> anyhow::Result<()> {
        if file.name() != "jvm0.txt" {
            return Ok(());
        }

        if self.iteration_df.is_some() {
            return Err(anyhow::anyhow!("Duplicate jvm0.txt files"));
        }

        let content = file.content_string()?;

        let iterations: Vec<u64> = ITERATION_REGEX
            .captures_iter(content)
            .filter_map(|cap| cap.get(1)?.as_str().parse().ok())
            .collect();

        let indices: Vec<u32> = (0..iterations.len() as u32).collect();

        let df = df![
            "dacapo_iteration" => indices,
            "dacapo_iteration_time_ms" => iterations,
        ]
        .context("Failed to create iteration DataFrame")?;

        self.iteration_df = Some(df);

        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, DataFrame)>> {
        let mut rv = Vec::new();
        if let Some(df) = self.iteration_df {
            rv.push((Intern::from_static("iteration"), df));
        }
        Ok(rv)
    }
}
