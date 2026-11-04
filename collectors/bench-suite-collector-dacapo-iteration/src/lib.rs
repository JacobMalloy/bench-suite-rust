use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use chrono::NaiveDateTime;
use polars::prelude::*;
// polars 0.55's prelude exports PolarsContext, whose context()/with_context()
// on PolarsResult make every anyhow .context() on a polars result ambiguous.
// An explicit import shadows the glob's, leaving only anyhow's in scope.
#[allow(unused_imports)]
use anyhow::Context as PolarsContext;
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

// The IterationDatetime DaCapo callback (config keys `classpath` +
// `dacapo_harness`, i.e. `-cp HOOK:dacapo.jar Harness -c IterationDatetime`)
// brackets every iteration:
//
//   =================================================
//   Iteration 1 - START [2026-10-19T20:57:07.453037856]
//   =================================================
//   ... benchmark output ...
//   Iteration 1 - END [2026-10-19T20:57:25.720434924]
//   =================================================
//
//   ===== DaCapo 23.11-MR2-chopin eclipse completed warmup 1 in 18259 msec ====
//
// That is the only wall-clock anchor for an iteration boundary anywhere in a
// run: everything else `dacapo_iteration` holds is a duration, so placing
// iteration k inside a threadstat sampling window meant fitting an offset
// against the run's own start. The hook prints nothing matching
// `DaCapo.*in N msec`, so neither table above changes -- 443 hooked runs on
// chile and brazil have exactly the `dacapo_iteration` row counts their
// unhooked replicates do.
//
// Two traps, both from `java.time`:
//
//  * The counter is the callback's own and 1-based, while `dacapo_iteration`
//    is 0-based. The index stored here is `n - 1`, so the two tables join on
//    it directly.
//  * `DateTimeFormatter.ISO_DATE_TIME` on a `LocalDateTime` trims *trailing*
//    zeros from the fraction, so `.19729306` is 197293060 ns and not
//    19729306 ns -- a tenfold error, silent, on 3 of the 40 timestamps in the
//    two sample runs. The fraction is left-aligned: pad it on the right.
static HOOK_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^Iteration (\d+) - (START|END) \[(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?\]$")
        .unwrap()
});

/// Epoch nanoseconds for one hook timestamp, with the fraction padded on the
/// right because `ISO_DATE_TIME` trimmed its trailing zeros.
///
/// The value is a *naive local* reading and is stored as one, the way
/// `threadstat_read.timestamp` stores its own -- see the note on
/// `HookRows::start_time_local`.
fn hook_timestamp_ns(date: &str, time: &str, fraction: Option<&str>) -> anyhow::Result<i64> {
    let whole = NaiveDateTime::parse_from_str(&format!("{date}T{time}"), "%Y-%m-%dT%H:%M:%S")
        .with_context(|| format!("Failed to parse hook timestamp {date}T{time}"))?
        .and_utc()
        .timestamp_nanos_opt()
        .with_context(|| format!("Hook timestamp {date}T{time} is out of nanosecond range"))?;

    let nanos: i64 = match fraction {
        Some(digits) => format!("{digits:0<9}")
            .parse()
            .with_context(|| format!("Failed to parse hook fraction .{digits}"))?,
        None => 0,
    };

    Ok(whole + nanos)
}

#[derive(Debug, Default)]
struct MeteredRows {
    iteration: Vec<u32>,
    requests: Vec<u64>,
    metered_time_ms: Vec<u64>,
    requests_per_second: Vec<u64>,
}

/// One row per iteration the hook bracketed, written to
/// `dacapo_hook_iteration` -- see the note at the `push` for why the name may
/// not start `dacapo_iteration_`.
///
/// `*_time_local` is naive **local** time, unlike every other timestamp in the
/// store: `LocalDateTime.now()` carries no offset, and the zone is not
/// recorded anywhere in the tar, so it is not guessed at here. Run 8240's
/// first START reads 20:57:07.453 against a `threadstat_read` span opening
/// 00:57:04.541 the following day -- EDT, +4h -- and resolving that belongs to
/// the consumer, which can anchor each run against its own UTC clock. The
/// `_local` suffix is the warning.
///
/// `end_time_local` is nullable: a run killed mid-iteration (the
/// `hazelcast_time_s` SIGTERM pattern) has a START with no END, which is
/// recorded rather than dropped or errored on.
#[derive(Debug, Default)]
struct HookRows {
    dacapo_iteration: Vec<u32>,
    start_time_local: Vec<i64>,
    end_time_local: Vec<Option<i64>>,
}

#[derive(Debug, Default)]
pub struct BenchSuiteCollectDacapoIteration {
    iteration_times_ms: Vec<u64>,
    metered: MeteredRows,
    hook: HookRows,
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
            if let Some(cap) = HOOK_REGEX.captures(line) {
                let counter: u32 = cap
                    .get(1)
                    .context("Missing hook iteration counter")?
                    .as_str()
                    .parse()
                    .context("Failed to parse hook iteration counter")?;
                let iteration = counter
                    .checked_sub(1)
                    .context("Hook iteration counter is 1-based; got 0")?;
                let timestamp = hook_timestamp_ns(
                    cap.get(3).context("Missing hook date")?.as_str(),
                    cap.get(4).context("Missing hook time")?.as_str(),
                    cap.get(5).map(|m| m.as_str()),
                )?;

                if cap.get(2).context("Missing hook boundary")?.as_str() == "START" {
                    self.hook.dacapo_iteration.push(iteration);
                    self.hook.start_time_local.push(timestamp);
                    self.hook.end_time_local.push(None);
                } else {
                    let row = self
                        .hook
                        .dacapo_iteration
                        .iter()
                        .rposition(|&i| i == iteration)
                        .with_context(|| format!("Iteration {counter} END without a START"))?;
                    self.hook.end_time_local[row] = Some(timestamp);
                }
            } else if let Some(cap) = METERED_REGEX.captures(line) {
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

        // Only runs carrying the hook have these lines, so the table is absent
        // rather than empty for every run without it -- the same shape as
        // `dacapo_metered` below, and nothing about an unhooked run changes.
        if !self.hook.dacapo_iteration.is_empty() {
            let df = df![
                "dacapo_iteration" => self.hook.dacapo_iteration,
                "start_time_local" => self.hook.start_time_local,
                "end_time_local" => self.hook.end_time_local,
            ]
            .context("Failed to create iteration datetime DataFrame")?;
            let lf = df.lazy().with_columns([
                col("start_time_local").cast(DataType::Datetime(TimeUnit::Nanoseconds, None)),
                col("end_time_local").cast(DataType::Datetime(TimeUnit::Nanoseconds, None)),
            ]);
            // NOT `dacapo_iteration_datetime`, and nothing else starting
            // `dacapo_iteration_`: shards are named `<table>_<n>.parquet`, so
            // such a name is matched by the `'dacapo_iteration_*.parquet'`
            // glob every consumer of the iteration times already uses.
            // Landing one would have broken `dacapo_metrics` in both analysis
            // repos for *every* run in the store, hooked or not, with a
            // schema-mismatch error from the glob rather than anything
            // pointing here.
            rv.push((Intern::from_static("dacapo_hook_iteration"), lf));
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

    // Verbatim from chile run 8240 (eclipse, hooked), iterations 1 and 2 with
    // the benchmark's own chatter between the boundaries cut down to one line.
    // Iteration 2's START is one of the trimmed-fraction timestamps.
    const HOOKED: &str = "\
=================================================
Iteration 1 - START [2026-10-19T20:57:07.453037856]
=================================================
Initialize workspace ...................
=================================================
Iteration 1 - END [2026-10-19T20:57:25.720434924]
=================================================

===== DaCapo 23.11-MR2-chopin eclipse completed warmup 1 in 18259 msec =====
=================================================
Iteration 2 - START [2026-10-19T20:57:26.07073844]
=================================================
Index workspace .....................
=================================================
Iteration 2 - END [2026-10-19T20:57:40.239521177]
=================================================

===== DaCapo 23.11-MR2-chopin eclipse completed warmup 2 in 14169 msec =====
";

    #[test]
    fn hook_lines_become_iteration_boundaries() {
        let mut collector = BenchSuiteCollectDacapoIteration::default();
        collector.parse(HOOKED).unwrap();

        // 0-based, so the callback's "Iteration 1" keys to dacapo_iteration 0
        // and joins the row the iteration time landed in.
        assert_eq!(collector.hook.dacapo_iteration, vec![0, 1]);
        assert_eq!(
            collector.hook.start_time_local,
            vec![1_792_443_427_453_037_856, 1_792_443_446_070_738_440]
        );
        assert_eq!(
            collector.hook.end_time_local,
            vec![
                Some(1_792_443_445_720_434_924),
                Some(1_792_443_460_239_521_177)
            ]
        );
    }

    #[test]
    fn hook_does_not_disturb_the_iteration_table() {
        let mut collector = BenchSuiteCollectDacapoIteration::default();
        collector.parse(HOOKED).unwrap();

        // The whole premise of the hook: one row per iteration either way.
        assert_eq!(collector.iteration_times_ms, vec![18259, 14169]);
        assert!(collector.metered.iteration.is_empty());
    }

    #[test]
    fn trimmed_fraction_is_padded_on_the_right() {
        // .07073844 is 70.73844 ms, not 70738.44 us: ISO_DATE_TIME drops
        // trailing zeros, so the digits are left-aligned. Reading them as
        // already-9-digit nanoseconds puts the boundary 63 ms early.
        assert_eq!(
            hook_timestamp_ns("2026-10-19", "20:57:26", Some("07073844")).unwrap(),
            1_792_443_446_070_738_440
        );
        // A whole second prints with no fraction at all.
        assert_eq!(
            hook_timestamp_ns("2026-10-19", "20:57:07", None).unwrap(),
            1_792_443_427_000_000_000
        );
        // Full precision, the common case.
        assert_eq!(
            hook_timestamp_ns("2026-10-19", "20:57:07", Some("453037856")).unwrap(),
            1_792_443_427_453_037_856
        );
    }

    #[test]
    fn unhooked_metered_run_gains_no_datetime_table() {
        // Verbatim from chile run 8226 (h2, unhooked): the benchmark family
        // whose metered line already collided with the iteration regex once.
        // Nothing about such a run may change.
        let sample = "\
===== DaCapo 23.11-MR2-chopin h2 completed warmup 1 in 6293 msec =====
===== DaCapo processed 100000 requests in 6258 msec, 15979 requests per second =====
===== DaCapo 23.11-MR2-chopin h2 completed warmup 2 in 4972 msec =====
===== DaCapo processed 100000 requests in 4969 msec, 20124 requests per second =====
";
        let mut collector = BenchSuiteCollectDacapoIteration::default();
        collector.parse(sample).unwrap();

        assert_eq!(collector.iteration_times_ms, vec![6293, 4972]);
        assert_eq!(collector.metered.metered_time_ms, vec![6258, 4969]);
        assert!(collector.hook.dacapo_iteration.is_empty());
    }

    #[test]
    fn truncated_iteration_keeps_its_start() {
        // A run killed mid-iteration prints START with no END. The row stays,
        // with a null end, rather than vanishing or failing the parse.
        let sample = "\
Iteration 1 - START [2026-10-19T20:57:07.453037856]
Iteration 1 - END [2026-10-19T20:57:25.720434924]
Iteration 2 - START [2026-10-19T20:57:26.07073844]
";
        let mut collector = BenchSuiteCollectDacapoIteration::default();
        collector.parse(sample).unwrap();

        assert_eq!(collector.hook.dacapo_iteration, vec![0, 1]);
        assert_eq!(
            collector.hook.end_time_local,
            vec![Some(1_792_443_445_720_434_924), None]
        );
    }
}
