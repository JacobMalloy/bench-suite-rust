use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use regex::Regex;
use std::collections::HashMap;
use std::sync::LazyLock;
use string_intern::Intern;

// Matches the tail latency summary lines DaCapo prints directly to stdout, e.g.:
// ===== DaCapo tail latency, simple: 50% 143 usec, 90% 356 usec, 99% 932 usec, 99.9% 1897 usec, 99.99% 11327 usec, max 24142 usec, measured over 200000 events =====
static LATENCY_SUMMARY_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"===== DaCapo tail latency, ([a-zA-Z0-9 ]+): 50% (\d+) usec, 90% (\d+) usec, 99% (\d+) usec, 99\.9% (\d+) usec, 99\.99% (\d+) usec, max (\d+) usec, measured over (\d+) events =====",
    )
    .unwrap()
});

#[derive(Default)]
struct LatencySummaryRows {
    iteration: Vec<u32>,
    p50_usec: Vec<u64>,
    p90_usec: Vec<u64>,
    p99_usec: Vec<u64>,
    p99_9_usec: Vec<u64>,
    p99_99_usec: Vec<u64>,
    max_usec: Vec<u64>,
    events: Vec<u64>,
}

#[derive(Default)]
pub struct BenchSuiteCollectDacapoLatencySummary {
    rows_by_type: HashMap<String, LatencySummaryRows>,
    seen_stdout: bool,
}

impl BenchSuiteCollectDacapoLatencySummary {
    #[must_use]
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

impl BenchSuiteCollectDacapoLatencySummary {
    fn parse(&mut self, content: &str) -> anyhow::Result<()> {
        for cap in LATENCY_SUMMARY_REGEX.captures_iter(content) {
            let latency_type = cap
                .get(1)
                .context("Missing latency type")?
                .as_str()
                .trim()
                .replace(' ', "_");
            let p50: u64 = cap
                .get(2)
                .context("Missing p50")?
                .as_str()
                .parse()
                .context("Failed to parse p50")?;
            let p90: u64 = cap
                .get(3)
                .context("Missing p90")?
                .as_str()
                .parse()
                .context("Failed to parse p90")?;
            let p99: u64 = cap
                .get(4)
                .context("Missing p99")?
                .as_str()
                .parse()
                .context("Failed to parse p99")?;
            let p99_9: u64 = cap
                .get(5)
                .context("Missing p99.9")?
                .as_str()
                .parse()
                .context("Failed to parse p99.9")?;
            let p99_99: u64 = cap
                .get(6)
                .context("Missing p99.99")?
                .as_str()
                .parse()
                .context("Failed to parse p99.99")?;
            let max: u64 = cap
                .get(7)
                .context("Missing max")?
                .as_str()
                .parse()
                .context("Failed to parse max")?;
            let events: u64 = cap
                .get(8)
                .context("Missing events")?
                .as_str()
                .parse()
                .context("Failed to parse events")?;

            let rows = self.rows_by_type.entry(latency_type).or_default();
            let iteration = u32::try_from(rows.iteration.len())
                .context("Too many latency summary iterations")?;
            rows.iteration.push(iteration);
            rows.p50_usec.push(p50);
            rows.p90_usec.push(p90);
            rows.p99_usec.push(p99);
            rows.p99_9_usec.push(p99_9);
            rows.p99_99_usec.push(p99_99);
            rows.max_usec.push(max);
            rows.events.push(events);
        }

        Ok(())
    }
}

impl BenchSuiteCollect for BenchSuiteCollectDacapoLatencySummary {
    fn process_file(
        &mut self,
        _: &bench_suite_types::BenchSuiteRun,
        file: &mut dyn bench_suite_collect_results::FileInfoInterface,
    ) -> anyhow::Result<()> {
        let name = file.name();
        if name != "jvm0.stdout"
            && name != "jvm0.txt" // LEGACY: remove once all tests use split files
        {
            return Ok(());
        }

        if self.seen_stdout {
            return Err(anyhow::anyhow!("Duplicate stdout files"));
        }
        self.seen_stdout = true;

        let content = file.content_string()?;
        self.parse(content)
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, LazyFrame)>> {
        self.rows_by_type
            .into_iter()
            .map(|(latency_type, rows)| {
                let df = df![
                    "iteration" => rows.iteration,
                    "p50_usec" => rows.p50_usec,
                    "p90_usec" => rows.p90_usec,
                    "p99_usec" => rows.p99_usec,
                    "p99_9_usec" => rows.p99_9_usec,
                    "p99_99_usec" => rows.p99_99_usec,
                    "max_usec" => rows.max_usec,
                    "events" => rows.events,
                ]
                .context("Failed to create latency summary DataFrame")?;

                let lf = df.lazy().with_columns([
                    col("p50_usec").cast(DataType::Duration(TimeUnit::Microseconds)),
                    col("p90_usec").cast(DataType::Duration(TimeUnit::Microseconds)),
                    col("p99_usec").cast(DataType::Duration(TimeUnit::Microseconds)),
                    col("p99_9_usec").cast(DataType::Duration(TimeUnit::Microseconds)),
                    col("p99_99_usec").cast(DataType::Duration(TimeUnit::Microseconds)),
                    col("max_usec").cast(DataType::Duration(TimeUnit::Microseconds)),
                ]);

                let table_name = Intern::new(format!("dacapo_latency_summary_{latency_type}"));
                Ok((table_name, lf))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_all_three_latency_summary_types() {
        let sample = "\
===== DaCapo 23.11-MR2-chopin cassandra completed warmup 1 in 7745 msec =====
===== DaCapo processed 200000 requests in 4783 msec, 41814 requests per second =====
===== DaCapo tail latency, simple: 50% 143 usec, 90% 356 usec, 99% 932 usec, 99.9% 1897 usec, 99.99% 11327 usec, max 24142 usec, measured over 200000 events =====
===== DaCapo tail latency, metered 100ms smoothing: 50% 456 usec, 90% 4370 usec, 99% 11892 usec, 99.9% 19413 usec, 99.99% 23249 usec, max 26273 usec, measured over 200000 events =====
===== DaCapo tail latency, metered full smoothing: 50% 349522 usec, 90% 533387 usec, 99% 558427 usec, 99.9% 559851 usec, 99.99% 560040 usec, max 560272 usec, measured over 200000 events =====
";
        let caps: Vec<_> = LATENCY_SUMMARY_REGEX.captures_iter(sample).collect();
        assert_eq!(caps.len(), 3);

        assert_eq!(&caps[0][1], "simple");
        assert_eq!(&caps[0][2], "143");
        assert_eq!(&caps[0][8], "200000");

        assert_eq!(&caps[1][1], "metered 100ms smoothing");
        assert_eq!(&caps[1][7], "26273");

        assert_eq!(&caps[2][1], "metered full smoothing");
        assert_eq!(&caps[2][6], "560040");
    }

    #[test]
    fn assigns_sequential_iterations_per_type() {
        let sample = "\
===== DaCapo tail latency, simple: 50% 1 usec, 90% 2 usec, 99% 3 usec, 99.9% 4 usec, 99.99% 5 usec, max 6 usec, measured over 7 events =====
===== DaCapo tail latency, simple: 50% 8 usec, 90% 9 usec, 99% 10 usec, 99.9% 11 usec, 99.99% 12 usec, max 13 usec, measured over 14 events =====
";
        let mut collector = BenchSuiteCollectDacapoLatencySummary::default();
        collector.parse(sample).unwrap();

        let rows = collector.rows_by_type.get("simple").unwrap();
        assert_eq!(rows.iteration, vec![0, 1]);
        assert_eq!(rows.p50_usec, vec![1, 8]);
    }
}
