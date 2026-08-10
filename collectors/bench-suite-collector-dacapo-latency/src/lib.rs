use anyhow::Context;
use bench_suite_collect_results::{BenchSuiteCollect, ColumnEncoding, Encoding};
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

#[derive(Default)]
pub struct BenchSuiteCollectDacapoLatency {
    latency_tables: HashMap<Intern, LazyFrame>,
}

impl BenchSuiteCollectDacapoLatency {
    #[must_use]
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

        let df = CsvReadOptions::default()
            .with_has_header(false)
            .with_schema(Some(LATENCY_SCHEMA.clone()))
            .into_reader_with_file_handle(cursor)
            .finish()
            .context("Failed to parse latency CSV")?;

        // Rename columns from default names to expected names
        let lf = df
            .lazy()
            .with_columns([
                (col("end_ns") - col("start_ns")).alias("duration"),
                lit(iteration).alias("iteration"),
            ])
            .select([all().exclude_cols(["end_ns"]).as_expr()]);

        let table_name = Intern::new(format!("dacapo_latency_{file_type}"));

        match self.latency_tables.get_mut(&table_name) {
            Some(existing) => {
                let old = core::mem::take(existing);
                *existing = concat([old, lf], UnionArgs::default())?;
            }
            None => {
                self.latency_tables.insert(table_name, lf);
            }
        }

        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, LazyFrame)>> {
        Ok(self
            .latency_tables
            .into_iter()
            .map(|(name, lf)| {
                let lf =
                    lf.with_column(col("duration").cast(DataType::Duration(TimeUnit::Nanoseconds)));
                (name, lf)
            })
            .collect())
    }

    fn column_encoding(&self) -> ColumnEncoding {
        // Every table this collector emits (one per `dacapo_latency_<file_type>`)
        // shares the same schema, so matching on column name alone is safe here -
        // there's no other table with a differently-meaning `start_ns` to collide
        // with. Within-run `start_ns` values climb in small steps punctuated by a
        // jump at each run boundary; measured ~1.65x smaller with delta.
        // `duration` isn't monotonic, so delta coding wouldn't help it, and
        // BYTE_STREAM_SPLIT isn't implemented for integer columns by this
        // polars-parquet version (only Plain/DeltaBinaryPacked - see
        // `array_to_page_integer`), so it's left on Polars' automatic choice,
        // same as `owner`/`iteration`, which already lands on RLE_DICTIONARY for
        // their low-cardinality values.
        |_table, column| (column == "start_ns").then_some(Encoding::DeltaBinaryPacked)
    }
}
