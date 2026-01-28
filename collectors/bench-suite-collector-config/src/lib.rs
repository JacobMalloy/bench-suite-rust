use anyhow::Result;
use bench_suite_collect_results::{BenchSuiteCollect, FileInfoInterface};
use bench_suite_types::BenchSuiteRun;
use polars::prelude::*;
use string_intern::Intern;

#[derive(Default)]
pub struct BenchSuiteCollectConfig {}

impl BenchSuiteCollectConfig {
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

impl BenchSuiteCollect for BenchSuiteCollectConfig {
    fn process_file(
        &mut self,
        _: &bench_suite_types::BenchSuiteRun,
        _: &mut dyn FileInfoInterface,
    ) -> Result<()> {
        Ok(())
    }

    fn get_result(self: Box<Self>, config: &BenchSuiteRun) -> Result<Vec<(Intern, LazyFrame)>> {
        Ok(vec![(Intern::from_static("config"), config.to_df()?.lazy())])
    }
}
