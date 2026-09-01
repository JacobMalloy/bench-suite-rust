use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use string_intern::Intern;

/// Collects `perf_stat.csv` - one aggregate count per perf event for the run,
/// written by `utils/setup.py`'s `PerfStat` when a config sets `perf_events`.
///
/// The file is already one row per event, so there is no reshaping to do here;
/// what this adds is dtypes. `value` arrives as text because an unreadable
/// counter leaves it empty (with perf's reason kept in `status`), and letting
/// Polars infer that per-run would give a String column on any run where every
/// counter failed, which then refuses to concat with the Float64 column from
/// runs that worked.
fn parse(bytes: &[u8]) -> anyhow::Result<LazyFrame> {
    let cursor = std::io::Cursor::new(bytes);

    Ok(CsvReadOptions::default()
        .with_has_header(true)
        .into_reader_with_file_handle(cursor)
        .finish()
        .context("Failed to parse perf_stat.csv")?
        .lazy()
        .with_columns([
            // Float64 rather than an integer type because one column holds
            // both RAPL Joules and raw event counts. A run long enough to
            // overflow the 2^53 exact-integer range would need ~1e16 events,
            // well past what a benchmark window reaches, and the ULP there is
            // still far below counter noise.
            col("value").cast(DataType::Float64),
            col("counter_ns").cast(DataType::UInt64),
            // Below 100 when the kernel had to multiplex the event, i.e. the
            // value is a scaled estimate rather than a full-window count.
            // Worth filtering on before trusting a grouped run.
            col("enabled_pct").cast(DataType::Float64),
        ]))
}

#[derive(Default)]
pub struct BenchSuiteCollectPerfStat {
    perf_stat_df: Option<LazyFrame>,
}

impl BenchSuiteCollectPerfStat {
    #[must_use]
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

impl BenchSuiteCollect for BenchSuiteCollectPerfStat {
    fn process_file(
        &mut self,
        _: &bench_suite_types::BenchSuiteRun,
        file: &mut dyn bench_suite_collect_results::FileInfoInterface,
    ) -> anyhow::Result<()> {
        if file.name() != "perf_stat.csv" {
            return Ok(());
        }

        if self.perf_stat_df.is_some() {
            return Err(anyhow::anyhow!("Duplicate perf_stat.csv files"));
        }

        self.perf_stat_df = Some(parse(file.content_bytes()?)?);

        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, polars::prelude::LazyFrame)>> {
        let mut rv = Vec::new();
        let BenchSuiteCollectPerfStat { perf_stat_df } = *self;
        if let Some(v) = perf_stat_df {
            rv.push((Intern::from_static("perf_stat"), v));
        }
        Ok(rv)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(content: &str) -> DataFrame {
        parse(content.as_bytes()).unwrap().collect().unwrap()
    }

    #[test]
    fn reads_mixed_joules_and_counts() {
        let df = collect(concat!(
            "event,value,unit,counter_ns,enabled_pct,status\n",
            "power/energy-pkg/,3947.10,Joules,1502334,100.00,ok\n",
            "instructions,248067,,584551,100.00,ok\n",
        ));
        assert_eq!(df.height(), 2);
        assert_eq!(df.column("value").unwrap().dtype(), &DataType::Float64);
        assert_eq!(df.column("counter_ns").unwrap().dtype(), &DataType::UInt64);
        let v = df.column("value").unwrap().f64().unwrap();
        assert!((v.get(0).unwrap() - 3947.10).abs() < 1e-9);
        assert!((v.get(1).unwrap() - 248_067.0).abs() < 1e-9);
    }

    #[test]
    fn quoted_raw_event_name_survives() {
        let df = collect(concat!(
            "event,value,unit,counter_ns,enabled_pct,status\n",
            "\"cpu/event=0xc0,umask=0x00/\",248067,,581310,100.00,ok\n",
        ));
        assert_eq!(df.height(), 1);
        assert_eq!(
            df.column("event").unwrap().str().unwrap().get(0).unwrap(),
            "cpu/event=0xc0,umask=0x00/"
        );
    }

    #[test]
    fn all_counters_unreadable_still_yields_float_column() {
        // The case the explicit cast exists for: with every value empty Polars
        // would otherwise infer String/Null here, and that frame refuses to
        // concat with a Float64 one from a run whose counters did read.
        let df = collect(concat!(
            "event,value,unit,counter_ns,enabled_pct,status\n",
            "power/energy-pkg/,,Joules,0,100.00,<not supported>\n",
        ));
        assert_eq!(df.column("value").unwrap().dtype(), &DataType::Float64);
        assert_eq!(df.column("value").unwrap().null_count(), 1);
        assert_eq!(
            df.column("status").unwrap().str().unwrap().get(0).unwrap(),
            "<not supported>"
        );
    }

    #[test]
    fn multiplexed_percentage_is_preserved() {
        let df = collect(concat!(
            "event,value,unit,counter_ns,enabled_pct,status\n",
            "uncore_imc_0/cas_count_read/,42.5,,1502334,49.98,ok\n",
        ));
        let pct = df.column("enabled_pct").unwrap().f64().unwrap();
        assert!((pct.get(0).unwrap() - 49.98).abs() < 1e-9);
    }
}
