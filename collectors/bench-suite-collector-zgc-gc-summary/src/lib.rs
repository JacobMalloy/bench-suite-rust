use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use regex::Regex;
use std::sync::LazyLock;
use string_intern::Intern;

// Matches the completed-collection summary line, e.g.:
// [2026-07-17T10:50:06.042-0400][info   ][gc          ] GC(0) Major Collection (Metadata GC Threshold) 228M(0%)->34M(0%) 0.042s
// (the earlier start-of-cycle line for the same GC number has no memory/time suffix and is
// deliberately not matched by this regex)
static GC_SUMMARY_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"\[([^\]]*)\]\[info\s*\]\[gc\s*\] GC\((\d+)\) (Major|Minor) Collection \(([^)]+)\) (\d+)M\((\d+)%\)->(\d+)M\((\d+)%\) ([0-9.]+)s",
    )
    .unwrap()
});

#[derive(Debug, Default)]
pub struct BenchSuiteCollectZgcGcSummary {
    summary_df: Option<DataFrame>,
}

impl BenchSuiteCollectZgcGcSummary {
    #[must_use]
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

impl BenchSuiteCollect for BenchSuiteCollectZgcGcSummary {
    fn process_file(
        &mut self,
        run: &bench_suite_types::BenchSuiteRun,
        file: &mut dyn bench_suite_collect_results::FileInfoInterface,
    ) -> anyhow::Result<()> {
        // Only process if GC is ZGC
        match &run.gc {
            Some(gc) if gc.as_str().to_lowercase().contains("zgc") => {}
            _ => return Ok(()),
        }

        let name = file.name();
        if name != "gc.javalog"
            && name != "jvm0.txt" // LEGACY: remove once all tests use split files
        {
            return Ok(());
        }

        if self.summary_df.is_some() {
            return Err(anyhow::anyhow!("Duplicate gc log files"));
        }

        let content = file.content_string()?;

        let mut clock_times: Vec<String> = Vec::new();
        let mut gc_numbers: Vec<u32> = Vec::new();
        let mut types: Vec<String> = Vec::new();
        let mut causes: Vec<String> = Vec::new();
        let mut start_memory_mb: Vec<u64> = Vec::new();
        let mut start_percent: Vec<u8> = Vec::new();
        let mut end_memory_mb: Vec<u64> = Vec::new();
        let mut end_percent: Vec<u8> = Vec::new();
        let mut time_s: Vec<f64> = Vec::new();

        for cap in GC_SUMMARY_REGEX.captures_iter(content) {
            let clock_time = cap.get(1).context("Missing clock time")?.as_str();
            let gc_number: u32 = cap
                .get(2)
                .context("Missing GC number")?
                .as_str()
                .parse()
                .context("Failed to parse GC number")?;
            let gc_type = cap.get(3).context("Missing GC type")?.as_str();
            let cause = cap.get(4).context("Missing cause")?.as_str();
            let start_mb: u64 = cap
                .get(5)
                .context("Missing start memory")?
                .as_str()
                .parse()
                .context("Failed to parse start memory")?;
            let start_pct: u8 = cap
                .get(6)
                .context("Missing start percent")?
                .as_str()
                .parse()
                .context("Failed to parse start percent")?;
            let end_mb: u64 = cap
                .get(7)
                .context("Missing end memory")?
                .as_str()
                .parse()
                .context("Failed to parse end memory")?;
            let end_pct: u8 = cap
                .get(8)
                .context("Missing end percent")?
                .as_str()
                .parse()
                .context("Failed to parse end percent")?;
            let time: f64 = cap
                .get(9)
                .context("Missing time")?
                .as_str()
                .parse()
                .context("Failed to parse time")?;

            clock_times.push(clock_time.to_string());
            gc_numbers.push(gc_number);
            types.push(gc_type.to_lowercase());
            causes.push(cause.to_string());
            start_memory_mb.push(start_mb);
            start_percent.push(start_pct);
            end_memory_mb.push(end_mb);
            end_percent.push(end_pct);
            time_s.push(time);
        }

        let df = df![
            "clock_time" => clock_times,
            "gc_number" => gc_numbers,
            "type" => types,
            "cause" => causes,
            "start_memory_mb" => start_memory_mb,
            "start_percent" => start_percent,
            "end_memory_mb" => end_memory_mb,
            "end_percent" => end_percent,
            "time_s" => time_s,
        ]
        .context("Failed to create GC summary DataFrame")?;

        self.summary_df = Some(df);

        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, LazyFrame)>> {
        let mut rv = Vec::new();
        if let Some(df) = self.summary_df {
            rv.push((Intern::from_static("zgc_gc_summary"), transform_summary(df)));
        }
        Ok(rv)
    }
}

fn transform_summary(df: DataFrame) -> LazyFrame {
    df.lazy()
        .with_column(col("clock_time").str().to_datetime(
            Some(TimeUnit::Milliseconds),
            None,
            StrptimeOptions {
                format: Some("%Y-%m-%dT%H:%M:%S%.3f%z".into()),
                strict: false,
                exact: true,
                cache: true,
            },
            lit("raise"),
        ))
        .with_column(
            (col("time_s") * lit(1_000_000.0))
                .cast(DataType::Int64)
                .cast(DataType::Duration(TimeUnit::Microseconds))
                .alias("time_s"),
        )
        .rename(["time_s", "clock_time"], ["time_us", "end_time"], false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_major_and_minor_summary_lines() {
        let sample = "\
[2026-07-17T10:50:06.000-0400][info   ][gc          ] GC(0) Major Collection (Metadata GC Threshold)
[2026-07-17T10:50:06.042-0400][info   ][gc          ] GC(0) Major Collection (Metadata GC Threshold) 228M(0%)->34M(0%) 0.042s
[2026-07-17T10:51:25.505-0400][info   ][gc          ] GC(4) Minor Collection (Allocation Rate)
[2026-07-17T10:51:30.202-0400][info   ][gc          ] GC(4) Minor Collection (Allocation Rate) 49168M(30%)->20970M(13%) 4.697s
";
        let caps: Vec<_> = GC_SUMMARY_REGEX.captures_iter(sample).collect();
        assert_eq!(caps.len(), 2, "start-of-cycle lines must not match");

        assert_eq!(&caps[0][2], "0");
        assert_eq!(&caps[0][3], "Major");
        assert_eq!(&caps[0][4], "Metadata GC Threshold");
        assert_eq!(&caps[0][5], "228");
        assert_eq!(&caps[0][6], "0");
        assert_eq!(&caps[0][7], "34");
        assert_eq!(&caps[0][8], "0");
        assert_eq!(&caps[0][9], "0.042");

        assert_eq!(&caps[1][3], "Minor");
        assert_eq!(&caps[1][4], "Allocation Rate");
        assert_eq!(&caps[1][9], "4.697");
    }
}

#[cfg(test)]
mod schema_check {
    use super::*;

    #[test]
    fn end_time_column_is_a_real_datetime() {
        let df = df![
            "clock_time" => ["2026-07-17T10:50:06.042-0400"],
            "gc_number" => [0u32],
            "type" => ["major"],
            "cause" => ["Metadata GC Threshold"],
            "start_memory_mb" => [228u64],
            "start_percent" => [0u8],
            "end_memory_mb" => [34u64],
            "end_percent" => [0u8],
            "time_s" => [0.042f64],
        ]
        .unwrap();

        let mut lf = transform_summary(df);
        let schema = lf.collect_schema().unwrap();
        let dtype = schema.get("end_time").expect("end_time column must exist");
        assert!(
            matches!(dtype, DataType::Datetime(_, _)),
            "expected end_time to be Datetime, got {dtype:?}"
        );
        assert!(
            schema.get("clock_time").is_none(),
            "clock_time should have been renamed to end_time"
        );
    }
}
