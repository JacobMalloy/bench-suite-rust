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

    let df = CsvReadOptions::default()
        .with_has_header(true)
        .into_reader_with_file_handle(cursor)
        .finish()
        .context("Failed to parse perf_stat.csv")?;

    // time_s / wall_epoch_us exist only on runs recorded after interval
    // support was added, and every archive written before it has neither. A
    // full rebuild re-reads all of them, so these are added only when the
    // file actually carries them -- referencing a missing column fails the
    // whole run, which would have taken out every historical perf_stat run
    // the first time the collector was rebuilt.
    let has_interval = df.schema().contains("time_s");
    let mut casts = vec![
        // Float64 rather than an integer type because one column holds
        // both RAPL Joules and raw event counts. A run long enough to
        // overflow the 2^53 exact-integer range would need ~1e16 events,
        // well past what a benchmark window reaches, and the ULP there is
        // still far below counter noise.
        col("value").cast(DataType::Float64),
        col("counter_ns").cast(DataType::UInt64),
        // Below 100 when the kernel had to multiplex the event, i.e. the
        // value is a scaled estimate rather than a full-window count.
        // Worth filtering on before trusting a grouped run. On an uncore
        // PMU this is the only warning you get: four CHA events schedule
        // at 100.00, a fifth silently drops every one of them to 58-100%.
        col("enabled_pct").cast(DataType::Float64),
        // Set only on an interval run (`perf_stat_interval_ms`), null
        // otherwise, and cast for the same reason `value` is: a
        // non-interval run writes the column empty, which infers as
        // String and then refuses to concat with the numeric column from
        // an interval run.
        //
        // time_s is perf's own elapsed seconds. wall_time is that mapped
        // onto the clock zgc_phases uses, anchored at the END of the run
        // -- see PerfStat._convert in utils/setup.py for why the start is
        // the worse anchor. The values are PER-INTERVAL DELTAS, unlike
        // threadstat's cumulative counters: differencing them with a
        // lag() the way every threadstat metric does would subtract one
        // interval from the next.
    ];
    if has_interval {
        casts.push(col("time_s").cast(DataType::Float64));
        casts.push(
            (col("wall_epoch_us").cast(DataType::Float64) * lit(1_000.0))
                .cast(DataType::Int64)
                .cast(DataType::Datetime(TimeUnit::Nanoseconds, None))
                .alias("wall_time"),
        );
    }
    Ok(df.lazy().with_columns(casts))
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

    #[test]
    fn an_interval_run_gets_typed_time_and_wall_clock() {
        // The shape PerfStat writes under `perf_stat_interval_ms`, including
        // the raw-descriptor event name that contains a comma.
        let csv = "event,value,unit,counter_ns,enabled_pct,status,time_s,wall_epoch_us\n\
                   unc_cha_llc_victims.local_m,11711,,178172953,100.00,ok,0.010065669,1000000010066\n\
                   \"uncore_cha/event=0x37,umask=0x2f/\",15771,,523405725,100.00,ok,0.020062925,1000000020063\n";
        let df = parse(csv.as_bytes()).unwrap().collect().unwrap();
        assert_eq!(df.height(), 2);
        assert_eq!(df.column("time_s").unwrap().dtype(), &DataType::Float64);
        assert_eq!(
            df.column("wall_time").unwrap().dtype(),
            &DataType::Datetime(TimeUnit::Nanoseconds, None)
        );
        assert_eq!(
            df.column("value").unwrap().f64().unwrap().get(0),
            Some(11711.0)
        );
        // wall_epoch_us -> nanoseconds, so the stored instant is us * 1000
        assert_eq!(
            df.column("wall_time")
                .unwrap()
                .cast(&DataType::Int64)
                .unwrap()
                .i64()
                .unwrap()
                .get(0),
            Some(1_000_000_010_066_000)
        );
    }

    #[test]
    fn a_non_interval_run_has_no_time_columns_at_all() {
        // Every archive written before interval support looks like this, and a
        // full rebuild re-reads all of them.
        let csv = "event,value,unit,counter_ns,enabled_pct,status\n\
                   instructions,1234,,368727,100.00,ok\n";
        let df = parse(csv.as_bytes()).unwrap().collect().unwrap();
        assert!(!df.schema().contains("time_s"));
        assert!(!df.schema().contains("wall_time"));
        assert_eq!(
            df.column("value").unwrap().f64().unwrap().get(0),
            Some(1234.0)
        );
    }
}
