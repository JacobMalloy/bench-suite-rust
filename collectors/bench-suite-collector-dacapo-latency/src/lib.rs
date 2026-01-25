use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use regex::Regex;
use std::collections::HashMap;
use std::sync::LazyLock;
use string_intern::Intern;

static LATENCY_FILE_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^dacapo-latency-usec-([a-zA-Z0-9-]+)-([0-9]+)\.csv$").unwrap());

static LATENCY_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::from_iter(vec![
        Field::new("start_ns".into(), DataType::UInt64),
        Field::new("end_ns".into(), DataType::UInt64),
        Field::new("owner".into(), DataType::UInt64),
    ]))
});

#[derive(Debug, Default)]
pub struct BenchSuiteCollectDacapoLatency {
    latency_tables: HashMap<Intern, DataFrame>,
}

impl BenchSuiteCollectDacapoLatency {
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

impl BenchSuiteCollect for BenchSuiteCollectDacapoLatency {
    fn process_file(
        &mut self,
        _: &bench_suite_types::BenchSuiteRun,
        file: &mut dyn bench_suite_collect_results::FileInfoInterface,
    ) -> anyhow::Result<()> {
        let name = file.name();

        let Some(captures) = LATENCY_FILE_REGEX.captures(name) else {
            return Ok(());
        };

        let file_type = captures
            .get(1)
            .context("Missing file type in regex capture")?
            .as_str()
            .replace('-', "_");

        let iteration: u32 = captures
            .get(2)
            .context("Missing iteration in regex capture")?
            .as_str()
            .parse()
            .context("Failed to parse iteration number")?;

        let cursor = std::io::Cursor::new(file.content_bytes()?);

        let mut df = CsvReadOptions::default()
            .with_has_header(false)
            .with_schema(Some(LATENCY_SCHEMA.clone()))
            .into_reader_with_file_handle(cursor)
            .finish()
            .context("Failed to parse latency CSV")?;

        // Rename columns from default names to expected names

        df.with_column(
            (df.column("end_ns")? - df.column("start_ns")?)?.with_name("duration".into()),
        )?;

        df.drop_in_place("end_ns")?;

        // Add iteration column
        let row_count = df.height();
        let iteration_col = Column::new("iteration".into(), vec![iteration; row_count]);
        let _ = df.with_column(iteration_col)?;

        let table_name = Intern::new(format!("dacapo_latency_{}", file_type));

        match self.latency_tables.get_mut(&table_name) {
            Some(existing) => {
                existing.vstack_mut(&df)?;
            }
            None => {
                self.latency_tables.insert(table_name, df);
            }
        }

        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, DataFrame)>> {
        Ok(self.latency_tables.into_iter().collect())
    }
}
