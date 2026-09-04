use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use regex::Regex;
use std::sync::LazyLock;
use string_intern::Intern;

// DaCapo prints two distinct "in N msec" lines per iteration on the
// latency-metered benchmarks (cassandra/h2/lusearch/spring/tomcat):
//
//   ===== DaCapo 23.11-MR2-chopin cassandra completed warmup 6 in 6151 msec =====
//   ===== DaCapo processed 200000 requests in 3842 msec, 52056 requests per second =====
//
// The second is the request-processing window nested *inside* the first, not a
// separate iteration -- summing both overshoots wall clock by up to 1.94x.
// Both used to match one broad `DaCapo.*in (\d+) msec` regex and land in
// `dacapo_iteration` as consecutive rows, so those runs silently reported two
// rows per iteration: `dacapo_iteration` became a positional row counter
// rather than an iteration index, and the time column interleaved two
// incommensurable quantities. Consumers averaging over it got the mean of an
// iteration time and the window nested in it (~6357ms and ~4024ms -> 5226ms on
// cassandra), and any "skip the first N iterations" filter skipped N/2.
//
// They are split into two tables here: `dacapo_iteration` keeps exactly one
// row per real iteration, and the metered window gets its own `dacapo_metered`
// table, keyed by `iteration` like the `dacapo_latency_summary_*` tables whose
// percentiles are computed over that same window. That also preserves the
// request count and throughput, which were previously discarded.
static METERED_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"===== DaCapo processed (\d+) requests in (\d+) msec, (\d+) requests per second =====",
    )
    .unwrap()
});

// Deliberately still broad: any other `DaCapo ... in N msec` line counts as an
// iteration, exactly as it did before, so older logs whose wording differs from
// 23.11's `completed warmup N` / `PASSED` pair keep parsing unchanged. Only the
// metered line above is carved out.
static ITERATION_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"DaCapo.*in (\d+) msec").unwrap());

#[derive(Debug, Default)]
struct MeteredRows {
    iteration: Vec<u32>,
    requests: Vec<u64>,
    metered_time_ms: Vec<u64>,
    requests_per_second: Vec<u64>,
}

#[derive(Debug, Default)]
pub struct BenchSuiteCollectDacapoIteration {
    iteration_times_ms: Vec<u64>,
    metered: MeteredRows,
    seen_stdout: bool,
}

impl BenchSuiteCollectDacapoIteration {
    #[must_use]
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }

    fn parse(&mut self, content: &str) -> anyhow::Result<()> {
        // Line at a time so the metered line can be carved out before the broad
        // iteration regex sees it; `.` does not match newlines, so this matches
        // the previous whole-content `captures_iter` behaviour otherwise.
        for line in content.lines() {
            if let Some(cap) = METERED_REGEX.captures(line) {
                let requests: u64 = cap
                    .get(1)
                    .context("Missing metered request count")?
                    .as_str()
                    .parse()
                    .context("Failed to parse metered request count")?;
                let metered_time_ms: u64 = cap
                    .get(2)
                    .context("Missing metered time")?
                    .as_str()
                    .parse()
                    .context("Failed to parse metered time")?;
                let requests_per_second: u64 = cap
                    .get(3)
                    .context("Missing metered throughput")?
                    .as_str()
                    .parse()
                    .context("Failed to parse metered throughput")?;

                let iteration = u32::try_from(self.metered.iteration.len())
                    .context("Too many metered iterations")?;
                self.metered.iteration.push(iteration);
                self.metered.requests.push(requests);
                self.metered.metered_time_ms.push(metered_time_ms);
                self.metered.requests_per_second.push(requests_per_second);
            } else if let Some(cap) = ITERATION_REGEX.captures(line) {
                let time_ms: u64 = cap
                    .get(1)
                    .context("Missing iteration time")?
                    .as_str()
                    .parse()
                    .context("Failed to parse iteration time")?;
                self.iteration_times_ms.push(time_ms);
            }
        }

        Ok(())
    }
}

impl BenchSuiteCollect for BenchSuiteCollectDacapoIteration {
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
        let mut rv = Vec::new();

        if self.seen_stdout {
            let indices: Vec<u64> = (0..self.iteration_times_ms.len() as u64).collect();
            let df = df![
                "dacapo_iteration" => indices,
                "dacapo_iteration_time_ms" => self.iteration_times_ms,
            ]
            .context("Failed to create iteration DataFrame")?;
            let lf = df.lazy().with_column(
                col("dacapo_iteration_time_ms").cast(DataType::Duration(TimeUnit::Milliseconds)),
            );
            rv.push((Intern::from_static("dacapo_iteration"), lf));
        }

        // Only the latency-metered benchmarks emit this line, so the table is
        // absent rather than empty for the rest.
        if !self.metered.iteration.is_empty() {
            let df = df![
                "iteration" => self.metered.iteration,
                "requests" => self.metered.requests,
                "metered_time_ms" => self.metered.metered_time_ms,
                "requests_per_second" => self.metered.requests_per_second,
            ]
            .context("Failed to create metered DataFrame")?;
            let lf = df.lazy().with_column(
                col("metered_time_ms").cast(DataType::Duration(TimeUnit::Milliseconds)),
            );
            rv.push((Intern::from_static("dacapo_metered"), lf));
        }

        Ok(rv)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Verbatim from desktop run id 5084 (cassandra, -n10), first two iterations
    // plus the final PASSED one.
    const SAMPLE: &str = "\
===== DaCapo 23.11-MR2-chopin cassandra starting warmup 1 =====
===== DaCapo 23.11-MR2-chopin cassandra completed warmup 1 in 7858 msec =====
===== DaCapo processed 200000 requests in 4935 msec, 40526 requests per second =====
===== DaCapo tail latency, simple: 50% 138 usec, 90% 402 usec, 99% 865 usec, 99.9% 1737 usec, 99.99% 41341 usec, max 52697 usec, measured over 200000 events =====
===== DaCapo 23.11-MR2-chopin cassandra starting warmup 2 =====
===== DaCapo 23.11-MR2-chopin cassandra completed warmup 2 in 6446 msec =====
===== DaCapo processed 200000 requests in 4072 msec, 49115 requests per second =====
===== DaCapo 23.11-MR2-chopin cassandra starting =====
===== DaCapo 23.11-MR2-chopin cassandra PASSED in 6122 msec =====
===== DaCapo processed 200000 requests in 3808 msec, 52521 requests per second =====
";

    #[test]
    fn metered_lines_do_not_become_iterations() {
        let mut collector = BenchSuiteCollectDacapoIteration::default();
        collector.parse(SAMPLE).unwrap();

        // One row per real iteration -- warmup 1, warmup 2, PASSED -- and none
        // of the "processed N requests" lines leaking in.
        assert_eq!(collector.iteration_times_ms, vec![7858, 6446, 6122]);
    }

    #[test]
    fn metered_window_captured_separately() {
        let mut collector = BenchSuiteCollectDacapoIteration::default();
        collector.parse(SAMPLE).unwrap();

        assert_eq!(collector.metered.iteration, vec![0, 1, 2]);
        assert_eq!(collector.metered.metered_time_ms, vec![4935, 4072, 3808]);
        assert_eq!(collector.metered.requests, vec![200_000, 200_000, 200_000]);
        assert_eq!(
            collector.metered.requests_per_second,
            vec![40526, 49115, 52521]
        );
    }

    #[test]
    fn non_latency_benchmark_has_no_metered_table() {
        // fop and the other non-metered benchmarks print only the iteration
        // lines; they must still parse, and must not gain an empty table.
        let sample = "\
===== DaCapo 23.11-MR2-chopin fop completed warmup 1 in 1234 msec =====
===== DaCapo 23.11-MR2-chopin fop PASSED in 987 msec =====
";
        let mut collector = BenchSuiteCollectDacapoIteration::default();
        collector.parse(sample).unwrap();

        assert_eq!(collector.iteration_times_ms, vec![1234, 987]);
        assert!(collector.metered.iteration.is_empty());
    }
}
