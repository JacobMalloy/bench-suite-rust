use anyhow::Result;
use bench_suite_collect_results::{BenchSuiteCollect,FileInfoInterface};
use bench_suite_types::BenchSuiteRun;
use polars::prelude::DataFrame;

#[derive(Default)]
pub struct BenchSuiteCollectConfig {}


impl BenchSuiteCollectConfig{
    pub fn boxed()->Box<dyn BenchSuiteCollect>{
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

    fn get_result(self:Box<Self>, config: &BenchSuiteRun) -> Result<Vec<(String, DataFrame)>> {
        Ok(vec![(String::from("config"), config.to_df()?)])
    }
}
