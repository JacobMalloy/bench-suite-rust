use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use regex::Regex;
use std::sync::LazyLock;
use string_intern::Intern;

static MARK_ABUSE_FILE_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^mark_abuse(\d+)\.csv$").unwrap());

#[derive(Default)]
pub struct BenchSuiteCollectMarkAbuse {
    combined: Option<LazyFrame>,
}

impl BenchSuiteCollectMarkAbuse {
    #[must_use]
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

impl BenchSuiteCollect for BenchSuiteCollectMarkAbuse {
    fn process_file(
        &mut self,
        _: &bench_suite_types::BenchSuiteRun,
        file: &mut dyn bench_suite_collect_results::FileInfoInterface,
    ) -> anyhow::Result<()> {
        let Some(cap) = MARK_ABUSE_FILE_REGEX.captures(file.name()) else {
            return Ok(());
        };

        let process: u32 = cap
            .get(1)
            .context("Missing process number")?
            .as_str()
            .parse()
            .context("Failed to parse process number")?;

        let cursor = std::io::Cursor::new(file.content_bytes()?);

        let lf = CsvReadOptions::default()
            .with_has_header(true)
            .into_reader_with_file_handle(cursor)
            .finish()
            .context("Failed to parse mark_abuse CSV")?
            .lazy()
            .with_column(lit(process).alias("process"));

        self.combined = Some(match self.combined.take() {
            Some(existing) => concat([existing, lf], UnionArgs::default())?,
            None => lf,
        });

        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, LazyFrame)>> {
        let mut rv = Vec::new();
        if let Some(lf) = self.combined {
            let lf = lf
                .with_column(
                    col("timestamp_ms")
                        .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                        .alias("timestamp_ms"),
                )
                .with_column(
                    col("gc_time_delta_ms")
                        .cast(DataType::Duration(TimeUnit::Milliseconds)),
                )
                .with_column(
                    (col("time_s") * lit(1_000_000.0))
                        .cast(DataType::Int64)
                        .cast(DataType::Duration(TimeUnit::Microseconds))
                        .alias("time_s"),
                )
                .rename(["time_s"], ["time_us"], false);
            rv.push((Intern::from_static("mark_abuse"), lf));
        }
        Ok(rv)
    }
}
