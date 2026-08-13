use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use chrono::{Days, NaiveDate, NaiveTime};
use polars::prelude::*;
use regex::Regex;
use std::sync::LazyLock;
use string_intern::Intern;

// Matches Sinks.logger output lines from StreamingBenchmark, e.g.:
// 05:36:56.900 [ INFO] [c.h.j.i.c.W.loggerSink#0] time 1,816: latency 100 ms, cca. 80,000 keys
//
// The leading timestamp is time-of-day only (HH:mm:ss.SSS), with no date. The run's calendar
// date is recovered separately from generated_data.json's "datetime" field (captured just
// before the JVM is launched) and combined with these captures in `combine_clock_times`, which
// also accounts for the run crossing a midnight rollover.
static LATENCY_LINE_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(\d{2}:\d{2}:\d{2}\.\d{3}).*?time [\d,]+: latency (-?[\d,]+) ms, cca\. ([\d,]+) keys",
    )
    .unwrap()
});

fn parse_comma_u64(s: &str) -> anyhow::Result<u64> {
    s.replace(',', "").parse().context("Failed to parse number")
}

fn parse_comma_i64(s: &str) -> anyhow::Result<i64> {
    s.replace(',', "").parse().context("Failed to parse number")
}

/// Combines time-of-day captures with the run's start date, rolling the date forward whenever
/// a capture's time-of-day is earlier than the previous one (i.e. midnight was crossed).
///
/// Both `run_start_time` and `times_of_day` are naive local wall-clock times taken from the
/// same host in the same process invocation (the runner writes `generated_data.json` and then
/// immediately launches the JVM), so no timezone conversion is applied or needed here. This
/// does not special-case a DST "fall back" transition landing mid-run: log4j2 and Python's
/// `datetime.now()` both emit naive local time with no UTC offset, so a repeated hour is
/// genuinely ambiguous at the source and would be misread as a midnight rollover. Given
/// `hazelcast_time_s` runs last minutes and DST transitions are twice-yearly, fixed-hour
/// events, this is treated as an accepted limitation rather than guessed at.
fn combine_clock_times(
    run_date: NaiveDate,
    run_start_time: NaiveTime,
    times_of_day: &[NaiveTime],
) -> Vec<String> {
    let mut day_offset: u64 = 0;
    let mut prev = run_start_time;
    times_of_day
        .iter()
        .map(|&time| {
            if time < prev {
                day_offset += 1;
            }
            prev = time;
            (run_date + Days::new(day_offset))
                .and_time(time)
                .format("%Y-%m-%d %H:%M:%S%.3f")
                .to_string()
        })
        .collect()
}

#[derive(Default)]
pub struct BenchSuiteCollectHazelcastJet {
    latency_rows: Option<(Vec<NaiveTime>, Vec<i64>, Vec<u64>)>,
    run_start: Option<(NaiveDate, NaiveTime)>,
}

impl BenchSuiteCollectHazelcastJet {
    #[must_use]
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

impl BenchSuiteCollect for BenchSuiteCollectHazelcastJet {
    fn process_file(
        &mut self,
        _: &bench_suite_types::BenchSuiteRun,
        file: &mut dyn bench_suite_collect_results::FileInfoInterface,
    ) -> anyhow::Result<()> {
        match file.name() {
            "jvm0.stdout" => {
                if self.latency_rows.is_some() {
                    return Err(anyhow::anyhow!("Duplicate jvm0.stdout files"));
                }

                let content = file.content_string()?;

                let mut clock_times: Vec<NaiveTime> = Vec::new();
                let mut latency_ms: Vec<i64> = Vec::new();
                let mut keys: Vec<u64> = Vec::new();

                for cap in LATENCY_LINE_REGEX.captures_iter(content) {
                    let time = NaiveTime::parse_from_str(
                        cap.get(1).context("Missing clock time")?.as_str(),
                        "%H:%M:%S%.3f",
                    )
                    .context("Failed to parse latency line clock time")?;
                    clock_times.push(time);
                    latency_ms
                        .push(parse_comma_i64(cap.get(2).context("Missing latency")?.as_str())?);
                    keys.push(parse_comma_u64(cap.get(3).context("Missing keys")?.as_str())?);
                }

                self.latency_rows = Some((clock_times, latency_ms, keys));
            }
            "generated_data.json" => {
                if self.run_start.is_some() {
                    return Err(anyhow::anyhow!("Duplicate generated_data.json files"));
                }

                let content = file.content_string()?;
                let value: serde_json::Value =
                    serde_json::from_str(content).context("Failed to parse generated_data.json")?;
                let datetime = value
                    .get("datetime")
                    .and_then(serde_json::Value::as_str)
                    .context("generated_data.json missing datetime field")?;
                let (date_str, time_str) = datetime
                    .split_once('#')
                    .context("generated_data.json datetime missing '#' separator")?;
                let run_date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
                    .context("Failed to parse generated_data.json date")?;
                let run_start_time = NaiveTime::parse_from_str(time_str, "%H:%M:%S%.f")
                    .context("Failed to parse generated_data.json time")?;

                self.run_start = Some((run_date, run_start_time));
            }
            _ => {}
        }

        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, LazyFrame)>> {
        let mut rv = Vec::new();
        if let Some((times_of_day, latency_ms, keys)) = self.latency_rows {
            let (run_date, run_start_time) = self.run_start.context(
                "jvm0.stdout had latency lines but generated_data.json was missing, cannot recover their dates",
            )?;
            let clock_times = combine_clock_times(run_date, run_start_time, &times_of_day);

            let df = df![
                "clock_time" => clock_times,
                "latency_ms" => latency_ms,
                "keys" => keys,
            ]
            .context("Failed to create hazelcast jet latency DataFrame")?;

            rv.push((
                Intern::from_static("hazelcast_jet_latency"),
                transform_latency(df),
            ));
        }
        Ok(rv)
    }
}

fn transform_latency(df: DataFrame) -> LazyFrame {
    df.lazy()
        .with_column(col("clock_time").str().to_datetime(
            Some(TimeUnit::Milliseconds),
            None,
            StrptimeOptions {
                format: Some("%Y-%m-%d %H:%M:%S%.3f".into()),
                strict: false,
                exact: true,
                cache: true,
            },
            lit("raise"),
        ))
        .with_column(
            (col("clock_time").cast(DataType::Int64) - col("latency_ms"))
                .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                .alias("window_end_time"),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_regex_matches_sample_line() {
        let sample =
            "05:36:56.900 [ INFO] [c.h.j.i.c.W.loggerSink#0] time 1,816: latency 100 ms, cca. 80,000 keys";
        let cap = LATENCY_LINE_REGEX
            .captures(sample)
            .expect("line should match");
        assert_eq!(&cap[1], "05:36:56.900");
        assert_eq!(&cap[2], "100");
        assert_eq!(&cap[3], "80,000");
        assert_eq!(parse_comma_i64(&cap[2]).unwrap(), 100);
        assert_eq!(parse_comma_u64(&cap[3]).unwrap(), 80_000);
    }

    #[test]
    fn latency_regex_matches_line_with_ansi_color_codes() {
        let sample =
            "05:36:56.900 [\u{1b}[32m INFO\u{1b}[0m] [\u{1b}[34mc.h.j.i.c.W.loggerSink#0\u{1b}[0m] time 1,816: latency 5 ms, cca. 10,000 keys";
        let cap = LATENCY_LINE_REGEX
            .captures(sample)
            .expect("line should match");
        assert_eq!(&cap[1], "05:36:56.900");
        assert_eq!(&cap[2], "5");
        assert_eq!(&cap[3], "10,000");
    }

    #[test]
    fn combine_clock_times_rolls_date_forward_over_midnight() {
        let run_date = NaiveDate::from_ymd_opt(2026, 8, 8).unwrap();
        let run_start_time = NaiveTime::from_hms_milli_opt(23, 59, 58, 0).unwrap();
        let times = vec![
            NaiveTime::from_hms_milli_opt(23, 59, 59, 0).unwrap(),
            NaiveTime::from_hms_milli_opt(0, 0, 1, 0).unwrap(),
            NaiveTime::from_hms_milli_opt(0, 0, 2, 0).unwrap(),
        ];
        let combined = combine_clock_times(run_date, run_start_time, &times);
        assert_eq!(
            combined,
            vec![
                "2026-08-08 23:59:59.000",
                "2026-08-09 00:00:01.000",
                "2026-08-09 00:00:02.000",
            ]
        );
    }
}

#[cfg(test)]
mod schema_check {
    use super::*;

    #[test]
    fn window_end_time_is_derived_from_clock_time_and_latency() {
        let df = df![
            "clock_time" => ["2026-07-21 22:10:00.123"],
            "latency_ms" => [5123i64],
            "keys" => [10_000u64],
        ]
        .unwrap();

        let mut lf = transform_latency(df);
        let schema = lf.collect_schema().unwrap();
        assert!(matches!(
            schema.get("clock_time"),
            Some(DataType::Datetime(_, _))
        ));
        assert!(matches!(
            schema.get("window_end_time"),
            Some(DataType::Datetime(_, _))
        ));

        let collected = lf.clone().collect().unwrap();
        let window_end_time = collected
            .column("window_end_time")
            .unwrap()
            .datetime()
            .unwrap()
            .phys
            .get(0)
            .unwrap();
        // clock_time (22:10:00.123) minus latency_ms (5123ms) = 22:09:55.000
        assert_eq!(window_end_time % 60_000, 55_000);
    }
}
