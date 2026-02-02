use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use string_intern::Intern;

#[derive(Default)]
pub struct BenchSuiteCollectSystemLoad {
    data_lf: Option<LazyFrame>,
}

impl BenchSuiteCollectSystemLoad {
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

impl BenchSuiteCollect for BenchSuiteCollectSystemLoad {
    fn process_file(
        &mut self,
        _: &bench_suite_types::BenchSuiteRun,
        file: &mut dyn bench_suite_collect_results::FileInfoInterface,
    ) -> anyhow::Result<()> {
        if file.name() != "cpu_data.csv" {
            return Ok(());
        }

        if self.data_lf.is_some() {
            return Err(anyhow::anyhow!("Duplicate cpu_data.csv files"));
        }

        let cursor = std::io::Cursor::new(file.content_bytes()?);

        let df = CsvReadOptions::default()
            .with_has_header(true)
            .into_reader_with_file_handle(cursor)
            .finish()
            .context("Failed to parse cpu_data.csv")?;

        let lf = df
            .lazy()
            .with_column(
                col("timestamp")
                    .str()
                    .to_datetime(
                        Some(TimeUnit::Milliseconds),
                        Some(TimeZone::UTC),
                        StrptimeOptions {
                            format: Some("%Y-%m-%d %H:%M:%S".into()),
                            exact: false,
                            ..Default::default()
                        },
                        lit("raise"),
                    ),
            )
            .select([
                col("timestamp"),
                col("CPU"),
                col("%user"),
                col("%nice"),
                col("%system"),
                col("%iowait"),
                col("%steal"),
                col("%idle"),
            ]);

        self.data_lf = Some(lf);

        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, LazyFrame)>> {
        let mut rv = Vec::new();
        if let Some(lf) = self.data_lf {
            rv.push((Intern::from_static("system_load"), lf));
        }
        Ok(rv)
    }
}
