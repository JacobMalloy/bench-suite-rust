use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::DataFrame;
use bench_suite_types::BenchSuiteRun;
use anyhow;

pub struct BenchSuiteCollectConfig{}

impl BenchSuiteCollect for BenchSuiteCollectConfig{
    fn process_file(&mut self,input:&str,config:&bench_suite_types::BenchSuiteRun,file:&bench_suite_collect_results::FileInfo) {
        
    }


    fn get_result(self,config:&BenchSuiteRun) -> anyhow::Result<Vec<(String,DataFrame)>>{
        vec![(String::from("config"),config.to_df()?)]
    }
    
}
