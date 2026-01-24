use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use std::sync::LazyLock;
use string_intern::Intern;

#[derive(Debug, Default)]
pub struct BenchSuiteCollectThreadstat {
    threadstat_df: Option<DataFrame>,
}

impl BenchSuiteCollectThreadstat {
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

static THREADSTAT_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::from_iter(vec![
        Field::new("pid".into(), DataType::UInt32),
        Field::new("event".into(), DataType::String),
        Field::new("count".into(), DataType::UInt64),
    ]))
});

impl BenchSuiteCollect for BenchSuiteCollectThreadstat {
    fn process_file(
        &mut self,
        _: &bench_suite_types::BenchSuiteRun,
        file: &mut dyn bench_suite_collect_results::FileInfoInterface,
    ) -> anyhow::Result<()> {
        if file.name() != "threadstat.csv" {
            return Ok(());
        }

        if self.threadstat_df.is_some() {
            return Err(anyhow::anyhow!("Duplicate threadstat.csv files"));
        }

        let cursor = std::io::Cursor::new(file.content_bytes()?);

        let df = CsvReadOptions::default()
            .with_has_header(true)
            .with_schema(Some(THREADSTAT_SCHEMA.clone()))
            .into_reader_with_file_handle(cursor)
            .finish()
            .context("Failed to parse threadstat.csv")?;

        self.threadstat_df = Some(df);

        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, DataFrame)>> {
        let mut rv = Vec::new();
        if let Some(df) = self.threadstat_df {
            rv.push((Intern::from_static("threadstat"), df));
        }
        Ok(rv)
    }
}
