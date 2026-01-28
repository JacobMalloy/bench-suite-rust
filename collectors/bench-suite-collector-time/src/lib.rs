use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use string_intern::Intern;

#[derive(Debug, Default)]
pub struct BenchSuiteCollectTime {
    time_df: Option<DataFrame>,
}

impl BenchSuiteCollectTime {
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

impl BenchSuiteCollect for BenchSuiteCollectTime {
    fn process_file(
        &mut self,
        _: &bench_suite_types::BenchSuiteRun,
        file: &mut dyn bench_suite_collect_results::FileInfoInterface,
    ) -> anyhow::Result<()> {
        if file.name() != "jvm0.time" {
            return Ok(());
        }

        if self.time_df.is_some() {
            return Err(anyhow::anyhow!("Duplicate time files"));
        }

        let cursor = std::io::Cursor::new(file.content_bytes()?);

        self.time_df = Some(CsvReader::new(cursor).finish()?);

        Ok(())
    }
    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, polars::prelude::LazyFrame)>> {
        let mut rv = Vec::new();
        let BenchSuiteCollectTime { time_df } = *self;
        if let Some(v) = time_df {
            rv.push((Intern::from_static("time"), v.lazy()));
        }
        Ok(rv)
    }
}
