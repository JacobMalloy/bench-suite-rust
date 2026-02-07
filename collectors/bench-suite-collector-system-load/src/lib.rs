use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use std::collections::HashMap;
use string_intern::Intern;
use std::sync::LazyLock;

#[derive(Default)]
pub struct BenchSuiteCollectSystemLoad {
    tables: HashMap<Intern, LazyFrame>,
}

impl BenchSuiteCollectSystemLoad {
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

fn transform_sadf(lf: LazyFrame) -> LazyFrame {
    lf.with_column(
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
    .select([all().exclude_cols(["hostname", "interval"]).as_expr()])
}

impl BenchSuiteCollectSystemLoad{
    fn drop_cols(&mut self,table:Intern,columns: impl IntoVec<PlSmallStr>){
        if let Some(v) = self.tables.get_mut(&table){
            let tmp = core::mem::take(v);
            *v = tmp.select([all().exclude_cols(columns).as_expr()])
        }
    }
}


static DROP_VALS:LazyLock<Vec<(Intern,Vec<&'static str>)>> = LazyLock::new(||{
    vec![
        (Intern::from_static("cpu_all_cores_sadf"),vec!["%idle"]),
        (Intern::from_static("cpu_sadf"),vec!["%idle"]),
        (Intern::from_static("memory_sadf"),vec!["%memused","%commit"]),
    ]
});

static ALL_CPU_NAME:LazyLock<Intern>= LazyLock::new(|| Intern::from_static("cpu_all_cores_sadf"));

impl BenchSuiteCollect for BenchSuiteCollectSystemLoad {
    fn process_file(
        &mut self,
        _: &bench_suite_types::BenchSuiteRun,
        file: &mut dyn bench_suite_collect_results::FileInfoInterface,
    ) -> anyhow::Result<()> {
        let (table_name, parse_options) = if file.name() == "cpu_data.csv" {
            (Intern::from_static("cpu_sadf"), CsvParseOptions::default())
        } else if let Some(stem) = file.name().strip_suffix(".sadf") {
            let tmp_name = format!("{stem}_sadf");
            (Intern::new(tmp_name), CsvParseOptions::default().with_separator(b';'))
        } else {
            return Ok(());
        };

        if self.tables.contains_key(&table_name) {
            return Err(anyhow::anyhow!("Duplicate {} files", file.name()));
        }

        let bytes = file.content_bytes()?;
        // sadf -d header line starts with "# ", strip it so Polars reads it as a normal header
        let bytes = bytes.strip_prefix(b"# ").unwrap_or(bytes);

        let cursor = std::io::Cursor::new(bytes);

        let df = CsvReadOptions::default()
            .with_has_header(true)
            .with_parse_options(parse_options)
            .into_reader_with_file_handle(cursor)
            .finish()
            .with_context(|| format!("Failed to parse {}", file.name()))?;

        // Find %idle[...] column to rename lazily
        let idle_col = df
            .get_column_names_str()
            .iter()
            .find(|c| c.starts_with("%idle["))
            .map(|s| s.to_string());

        let mut lf = df.lazy();
        if let Some(col_name) = idle_col {
            lf = lf.rename([col_name], ["%idle"], true);
        }

        self.tables.insert(table_name, transform_sadf(lf).collect()?.lazy());

        Ok(())
    }

    fn get_result(
        mut self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, LazyFrame)>> {
        for (name,cols) in DROP_VALS.iter(){
            self.drop_cols(*name, cols.iter().copied());
        }
        if let Some(v) = self.tables.get_mut(&ALL_CPU_NAME){
            let tmp = core::mem::take(v);
            *v = tmp.filter(col("CPU").neq(lit(-1)));
        }
        Ok(self.tables.into_iter().collect())
    }
}
