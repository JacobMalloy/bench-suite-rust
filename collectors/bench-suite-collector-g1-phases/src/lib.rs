use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use regex::Regex;
use std::collections::HashMap;
use std::sync::LazyLock;
use string_intern::Intern;

static GC_PHASE_TIMES_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\[([^\]]*)\]\[info\s*\]\[gc,phases\s*\] GC\((\d+)\)\s+([A-Za-z ]+):\s+([0-9.]+)ms")
        .unwrap()
});

static GC_TYPE_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"\[info\s*\]\[gc\s*\] GC\((\d+)\) (Pause Young \([^)]+\)|Pause \w+|Concurrent [A-Za-z ]+?)(?:\s+\(G1|\s+\d|\s*$)",
    )
    .unwrap()
});

#[derive(Debug, Default)]
pub struct BenchSuiteCollectG1Phases {
    phases_df: Option<DataFrame>,
}

impl BenchSuiteCollectG1Phases {
    #[must_use]
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

impl BenchSuiteCollect for BenchSuiteCollectG1Phases {
    fn process_file(
        &mut self,
        run: &bench_suite_types::BenchSuiteRun,
        file: &mut dyn bench_suite_collect_results::FileInfoInterface,
    ) -> anyhow::Result<()> {
        match &run.gc {
            Some(gc) if gc.as_str().to_lowercase().contains("g1") => {}
            _ => return Ok(()),
        }

        if file.name() != "jvm0.txt" {
            return Ok(());
        }

        if self.phases_df.is_some() {
            return Err(anyhow::anyhow!("Duplicate jvm0.txt files"));
        }

        let content = file.content_string()?;

        // Build gc_number -> gc_type map from [gc] summary lines.
        // Use or_insert so that for gc_numbers with multiple [gc] lines (e.g. concurrent
        // mark cycle with embedded Remark/Cleanup pauses), we keep the first entry.
        let mut gc_types: HashMap<u32, String> = HashMap::new();
        for cap in GC_TYPE_REGEX.captures_iter(content) {
            let gc_number: u32 = cap
                .get(1)
                .context("Missing GC number")?
                .as_str()
                .parse()
                .context("Failed to parse GC number")?;
            let gc_type = cap
                .get(2)
                .context("Missing GC type")?
                .as_str()
                .trim()
                .to_string();
            gc_types.entry(gc_number).or_insert(gc_type);
        }

        let mut clock_times: Vec<String> = Vec::new();
        let mut gc_numbers: Vec<u32> = Vec::new();
        let mut gc_type_col: Vec<String> = Vec::new();
        let mut phase_names: Vec<String> = Vec::new();
        let mut phase_times_ms: Vec<f64> = Vec::new();

        for cap in GC_PHASE_TIMES_REGEX.captures_iter(content) {
            let clock_time = cap.get(1).context("Missing clock time")?.as_str();
            let gc_number: u32 = cap
                .get(2)
                .context("Missing GC number")?
                .as_str()
                .parse()
                .context("Failed to parse GC number")?;
            let phase_name = cap.get(3).context("Missing phase name")?.as_str().trim();
            let phase_time: f64 = cap
                .get(4)
                .context("Missing phase time")?
                .as_str()
                .parse()
                .context("Failed to parse phase time")?;

            let gc_type = gc_types.get(&gc_number).cloned().unwrap_or_default();

            clock_times.push(clock_time.to_string());
            gc_numbers.push(gc_number);
            gc_type_col.push(gc_type);
            phase_names.push(phase_name.to_string());
            phase_times_ms.push(phase_time);
        }

        let df = df![
            "clock_time" => clock_times,
            "gc_number" => gc_numbers,
            "gc_type" => gc_type_col,
            "name" => phase_names,
            "time_ms" => phase_times_ms,
        ]
        .context("Failed to create phases DataFrame")?;

        self.phases_df = Some(df);
        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, LazyFrame)>> {
        let mut rv = Vec::new();
        if let Some(df) = self.phases_df {
            // clock_time is the end-of-pause timestamp shared by all phases in a GC.
            // Suffix sum of time_ms within each gc_number group gives each phase's
            // offset from end-of-pause back to its own start.
            let lf = df
                .lazy()
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
                    (col("clock_time").cast(DataType::Int64)
                        - col("time_ms")
                            .reverse()
                            .cum_sum(false)
                            .reverse()
                            .over([col("gc_number")])
                            .cast(DataType::Int64))
                    .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                    .alias("start_time"),
                )
                .with_column(
                    (col("time_ms") * lit(1000.0))
                        .cast(DataType::Int64)
                        .cast(DataType::Duration(TimeUnit::Microseconds))
                        .alias("time_ms"),
                )
                .rename(["time_ms"], ["time_us"], false);
            rv.push((Intern::from_static("g1_phases"), lf));
        }
        Ok(rv)
    }
}
