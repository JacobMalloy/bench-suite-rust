use bench_suite_types::BenchSuiteRun;
use polars::prelude::DataFrame;
use anyhow::Result;

pub struct FileInfo{
    pub content:String,
    pub name:String
}

impl FileInfo{
    pub fn name(&self)->&str{
        &self.name
    }
    pub fn content(&self)->&str{
        &self.content
    }
}


pub trait BenchSuiteCollect{
    fn process_file(&mut self,input:&str,config:&BenchSuiteRun,file:&FileInfo);
    fn get_result(self,config:&BenchSuiteRun) -> Result<Vec<(String,DataFrame)>>;
}
