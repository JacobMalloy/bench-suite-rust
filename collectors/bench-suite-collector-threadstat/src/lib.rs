use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use std::sync::LazyLock;
use string_intern::Intern;

#[derive(Default)]
pub struct BenchSuiteCollectThreadstat {
    threadstat_event_df: Option<LazyFrame>,
    threadstat_event_description_df: Option<LazyFrame>,
    threadstat_read_df: Option<LazyFrame>,
}

impl BenchSuiteCollectThreadstat {
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

static THREADSTAT_EVENT_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::from_iter(vec![
        Field::new("read_id".into(), DataType::UInt64),
        Field::new("count".into(), DataType::Int64),
        Field::new("event_id".into(), DataType::UInt64),
    ]))
});

static THREADSTAT_EVENT_DESCRIPTION_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::from_iter(vec![
        Field::new("event_id".into(), DataType::UInt64),
        Field::new("name".into(), DataType::String),
        Field::new("pid".into(), DataType::UInt32),
    ]))
});

static THREADSTAT_READ_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::from_iter(vec![
        Field::new("read_id".into(), DataType::UInt64),
        Field::new("timestamp".into(), DataType::Int64),
        Field::new("time_running".into(), DataType::UInt64),
        Field::new("time_enabled".into(), DataType::UInt64),
    ]))
});



impl BenchSuiteCollect for BenchSuiteCollectThreadstat {
    fn process_file(
        &mut self,
        _: &bench_suite_types::BenchSuiteRun,
        file: &mut dyn bench_suite_collect_results::FileInfoInterface,
    ) -> anyhow::Result<()> {
        match file.name() {
            "threadstat-event.csv" => {
                if self.threadstat_event_df.is_some() {
                    return Err(anyhow::anyhow!("Duplicate threadstat-event.csv files"));
                }

                let cursor = std::io::Cursor::new(file.content_bytes()?);

                let df = CsvReadOptions::default()
                    .with_has_header(true)
                    .with_schema(Some(THREADSTAT_EVENT_SCHEMA.clone()))
                    .into_reader_with_file_handle(cursor)
                    .finish()
                    .context("Failed to parse threadstat-event.csv")?;

                //The count should be unsigned, but as seen in the schema above it is not
                //the reason for this is that threadstat outputs -1 when it fails to read the
                //file descriptor. I believe this would happen when we try to open the perf event after
                //the process has closed. So right now I drop those columns and cast our column to unsigned
                let lf = df
                    .lazy()
                    .filter(col("count").neq(lit(-1)))
                    .with_column(col("count").cast(DataType::UInt64));

                self.threadstat_event_df = Some(lf);
            }
            "threadstat-event-description.csv" => {
                if self.threadstat_event_description_df.is_some() {
                    return Err(anyhow::anyhow!(
                        "Duplicate threadstat-event-description.csv files"
                    ));
                }

                let cursor = std::io::Cursor::new(file.content_bytes()?);

                let df = CsvReadOptions::default()
                    .with_has_header(true)
                    .with_schema(Some(THREADSTAT_EVENT_DESCRIPTION_SCHEMA.clone()))
                    .into_reader_with_file_handle(cursor)
                    .finish()
                    .context("Failed to parse threadstat-event-description.csv")?;

                self.threadstat_event_description_df = Some(df.lazy());
            }
            "threadstat-read.csv" => {
                if self.threadstat_read_df.is_some() {
                    return Err(anyhow::anyhow!("Duplicate threadstat-read.csv files"));
                }

                let cursor = std::io::Cursor::new(file.content_bytes()?);

                let df = CsvReadOptions::default()
                    .with_has_header(true)
                    .with_schema(Some(THREADSTAT_READ_SCHEMA.clone()))
                    .into_reader_with_file_handle(cursor)
                    .finish()
                    .context("Failed to parse threadstat-read.csv")?;

                let lf = df.lazy().with_column(
                    col("timestamp")
                        .cast(DataType::Datetime(TimeUnit::Nanoseconds, None))
                        .alias("timestamp"),
                );

                self.threadstat_read_df = Some(lf);
            }
            _ => {}
        }

        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, LazyFrame)>> {
        let mut rv = Vec::new();
        if let Some(lf) = self.threadstat_event_df {
            rv.push((Intern::from_static("threadstat_event"), lf));
        }
        if let Some(lf) = self.threadstat_event_description_df {
            rv.push((Intern::from_static("threadstat_event_description"), lf));
        }
        if let Some(lf) = self.threadstat_read_df {
            rv.push((Intern::from_static("threadstat_read"), lf));
        }
        Ok(rv)
    }
}
