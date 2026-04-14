use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use regex::Regex;
use std::sync::LazyLock;
use string_intern::Intern;

// Matches stat lines like:
//   [timestamp][info][gc,stats    ]        Contention: Mark Segment Reset Contention                    18 / 87               24 / 182              24 / 182              24 / 182         ops/s
//   [timestamp][info][gc,stats    ]          Critical: Allocation Stall                              0.000 / 0.000         3.589 / 14.601        3.589 / 14.601        3.589 / 14.601      ms
static ZGC_STATS_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"\[([^\]]*)\]\[info\s*\]\[gc,stats\s*\]\s+([\w ]+): (.+?)\s{2,}(\d+(?:\.\d+)?) / (\d+(?:\.\d+)?)\s+(\d+(?:\.\d+)?) / (\d+(?:\.\d+)?)\s+(\d+(?:\.\d+)?) / (\d+(?:\.\d+)?)\s+(\d+(?:\.\d+)?) / (\d+(?:\.\d+)?)\s+(\S+)",
    )
    .unwrap()
});

#[derive(Debug, Default)]
pub struct BenchSuiteCollectZgcStats {
    stats_df: Option<DataFrame>,
}

impl BenchSuiteCollectZgcStats {
    #[must_use]
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

impl BenchSuiteCollect for BenchSuiteCollectZgcStats {
    fn process_file(
        &mut self,
        run: &bench_suite_types::BenchSuiteRun,
        file: &mut dyn bench_suite_collect_results::FileInfoInterface,
    ) -> anyhow::Result<()> {
        match &run.gc {
            Some(gc) if gc.as_str().to_lowercase().contains("zgc") => {}
            _ => return Ok(()),
        }

        if file.name() != "jvm0.txt" {
            return Ok(());
        }

        if self.stats_df.is_some() {
            return Err(anyhow::anyhow!("Duplicate jvm0.txt files"));
        }

        let content = file.content_string()?;

        let mut clock_times: Vec<String> = Vec::new();
        let mut stat_types: Vec<String> = Vec::new();
        let mut names: Vec<String> = Vec::new();
        let mut last_10s_avg: Vec<f64> = Vec::new();
        let mut last_10s_max: Vec<f64> = Vec::new();
        let mut last_10m_avg: Vec<f64> = Vec::new();
        let mut last_10m_max: Vec<f64> = Vec::new();
        let mut last_10h_avg: Vec<f64> = Vec::new();
        let mut last_10h_max: Vec<f64> = Vec::new();
        let mut total_avg: Vec<f64> = Vec::new();
        let mut total_max: Vec<f64> = Vec::new();
        let mut units: Vec<String> = Vec::new();

        for cap in ZGC_STATS_REGEX.captures_iter(content) {
            let clock_time = cap.get(1).context("Missing clock time")?.as_str();
            let stat_type = cap.get(2).context("Missing stat type")?.as_str().trim();
            let name = cap.get(3).context("Missing name")?.as_str().trim();
            let l10s_avg: f64 = cap
                .get(4)
                .context("Missing last_10s_avg")?
                .as_str()
                .parse()
                .context("Failed to parse last_10s_avg")?;
            let l10s_max: f64 = cap
                .get(5)
                .context("Missing last_10s_max")?
                .as_str()
                .parse()
                .context("Failed to parse last_10s_max")?;
            let l10m_avg: f64 = cap
                .get(6)
                .context("Missing last_10m_avg")?
                .as_str()
                .parse()
                .context("Failed to parse last_10m_avg")?;
            let l10m_max: f64 = cap
                .get(7)
                .context("Missing last_10m_max")?
                .as_str()
                .parse()
                .context("Failed to parse last_10m_max")?;
            let l10h_avg: f64 = cap
                .get(8)
                .context("Missing last_10h_avg")?
                .as_str()
                .parse()
                .context("Failed to parse last_10h_avg")?;
            let l10h_max: f64 = cap
                .get(9)
                .context("Missing last_10h_max")?
                .as_str()
                .parse()
                .context("Failed to parse last_10h_max")?;
            let tot_avg: f64 = cap
                .get(10)
                .context("Missing total_avg")?
                .as_str()
                .parse()
                .context("Failed to parse total_avg")?;
            let tot_max: f64 = cap
                .get(11)
                .context("Missing total_max")?
                .as_str()
                .parse()
                .context("Failed to parse total_max")?;
            let unit = cap.get(12).context("Missing unit")?.as_str();

            clock_times.push(clock_time.to_string());
            stat_types.push(stat_type.to_string());
            names.push(name.to_string());
            last_10s_avg.push(l10s_avg);
            last_10s_max.push(l10s_max);
            last_10m_avg.push(l10m_avg);
            last_10m_max.push(l10m_max);
            last_10h_avg.push(l10h_avg);
            last_10h_max.push(l10h_max);
            total_avg.push(tot_avg);
            total_max.push(tot_max);
            units.push(unit.to_string());
        }

        let df = df![
            "clock_time" => clock_times,
            "stat_type" => stat_types,
            "name" => names,
            "unit" => units,
            "last_10s_avg" => last_10s_avg,
            "last_10s_max" => last_10s_max,
            "last_10m_avg" => last_10m_avg,
            "last_10m_max" => last_10m_max,
            "last_10h_avg" => last_10h_avg,
            "last_10h_max" => last_10h_max,
            "total_avg" => total_avg,
            "total_max" => total_max,
        ]
        .context("Failed to create zgc_stats DataFrame")?;

        self.stats_df = Some(df);

        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, LazyFrame)>> {
        let mut rv = Vec::new();
        if let Some(df) = self.stats_df {
            let lf = df.lazy().with_column(col("clock_time").str().to_datetime(
                Some(TimeUnit::Milliseconds),
                None,
                StrptimeOptions {
                    format: Some("%Y-%m-%dT%H:%M:%S%.3f%z".into()),
                    strict: false,
                    exact: true,
                    cache: true,
                },
                lit("raise"),
            ));
            rv.push((Intern::from_static("zgc_stats"), lf));
        }
        Ok(rv)
    }
}
