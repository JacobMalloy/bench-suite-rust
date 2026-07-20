use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use regex::Regex;
use std::sync::LazyLock;
use string_intern::Intern;

// Matches lines like:
// jbb2015.result.metric.max-jOPS = 28800
// jbb2015.result.metric.critical-jOPS = 24958
// jbb2015.result.SLA-10000-jOPS = 20600
static METRIC_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"jbb2015\.result\.(metric\.max-jOPS|metric\.critical-jOPS|SLA-\d+-jOPS)\s*=\s*(\d+)")
        .unwrap()
});

// Matches Controller.log lines like:
// <Thu Jul 16 22:05:38 EDT 2026> org.spec.jbb.controller: PROFILE: steady, (rIR:aIR:PR = 2880:2786:2786) (tPR = 42789) [OK]
// <Thu Jul 16 20:21:25 EDT 2026> org.spec.jbb.controller: WARMUP: IR = 0 finished, settle status = [OK] (rIR:aIR:PR = 0:0:0) (tPR = 0)
static PROFILE_LINE_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"<([A-Za-z]{3}) ([A-Za-z]{3})\s+(\d{1,2}) (\d{2}:\d{2}:\d{2}) ([A-Za-z]+) (\d{4})> org\.spec\.jbb\.controller: (PROFILE|RT_CURVE|VALIDATION|TRANSITION|WARMUP): .*?\(rIR:aIR:PR = (\d+):(\d+):(\d+)\) \(tPR = (\d+)\)",
    )
    .unwrap()
});

// SPECjbb only logs a timezone abbreviation (e.g. "EDT"), not a numeric offset. Only US
// Eastern is supported since that's what the benchmark machines actually emit; any other
// abbreviation fails loudly rather than being silently mis-parsed.
fn tz_offset(abbr: &str) -> anyhow::Result<&'static str> {
    match abbr {
        "EDT" => Ok("-0400"),
        "EST" => Ok("-0500"),
        other => Err(anyhow::anyhow!("Unsupported specjbb Controller.log timezone abbreviation: {other}")),
    }
}

#[derive(Debug, Default)]
pub struct BenchSuiteCollectSpecjbb {
    summary_df: Option<DataFrame>,
    profile_df: Option<DataFrame>,
}

impl BenchSuiteCollectSpecjbb {
    #[must_use]
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

impl BenchSuiteCollect for BenchSuiteCollectSpecjbb {
    fn process_file(
        &mut self,
        _: &bench_suite_types::BenchSuiteRun,
        file: &mut dyn bench_suite_collect_results::FileInfoInterface,
    ) -> anyhow::Result<()> {
        let name = file.name();

        if name.ends_with(".raw") {
            if self.summary_df.is_some() {
                return Err(anyhow::anyhow!("Duplicate specjbb .raw files"));
            }

            let content = file.content_string()?;

            let mut metrics: Vec<String> = Vec::new();
            let mut values: Vec<u64> = Vec::new();

            for cap in METRIC_REGEX.captures_iter(content) {
                let metric = cap.get(1).context("Missing metric name")?.as_str();
                let value: u64 = cap
                    .get(2)
                    .context("Missing metric value")?
                    .as_str()
                    .parse()
                    .context("Failed to parse metric value")?;
                metrics.push(metric.to_string());
                values.push(value);
            }

            let df = df![
                "metric" => metrics,
                "value" => values,
            ]
            .context("Failed to create specjbb summary DataFrame")?;

            self.summary_df = Some(df);
        } else if name.ends_with("-Controller.log") {
            if self.profile_df.is_some() {
                return Err(anyhow::anyhow!("Duplicate specjbb Controller.log files"));
            }

            let content = file.content_string()?;

            let mut clock_times: Vec<String> = Vec::new();
            let mut phases: Vec<String> = Vec::new();
            let mut requested_ir: Vec<u64> = Vec::new();
            let mut achieved_ir: Vec<u64> = Vec::new();
            let mut passed_requests: Vec<u64> = Vec::new();
            let mut total_passed_requests: Vec<u64> = Vec::new();

            for cap in PROFILE_LINE_REGEX.captures_iter(content) {
                let dow = cap.get(1).context("Missing day of week")?.as_str();
                let mon = cap.get(2).context("Missing month")?.as_str();
                let day: u32 = cap
                    .get(3)
                    .context("Missing day")?
                    .as_str()
                    .parse()
                    .context("Failed to parse day")?;
                let time = cap.get(4).context("Missing time")?.as_str();
                let tz = cap.get(5).context("Missing timezone")?.as_str();
                let year = cap.get(6).context("Missing year")?.as_str();
                let phase = cap.get(7).context("Missing phase")?.as_str();
                let rir: u64 = cap
                    .get(8)
                    .context("Missing rIR")?
                    .as_str()
                    .parse()
                    .context("Failed to parse rIR")?;
                let air: u64 = cap
                    .get(9)
                    .context("Missing aIR")?
                    .as_str()
                    .parse()
                    .context("Failed to parse aIR")?;
                let pr: u64 = cap
                    .get(10)
                    .context("Missing PR")?
                    .as_str()
                    .parse()
                    .context("Failed to parse PR")?;
                let tpr: u64 = cap
                    .get(11)
                    .context("Missing tPR")?
                    .as_str()
                    .parse()
                    .context("Failed to parse tPR")?;
                let offset = tz_offset(tz)?;

                clock_times.push(format!("{dow} {mon} {day:02} {time} {offset} {year}"));
                phases.push(phase.to_string());
                requested_ir.push(rir);
                achieved_ir.push(air);
                passed_requests.push(pr);
                total_passed_requests.push(tpr);
            }

            let df = df![
                "clock_time" => clock_times,
                "phase" => phases,
                "requested_ir" => requested_ir,
                "achieved_ir" => achieved_ir,
                "passed_requests" => passed_requests,
                "total_passed_requests" => total_passed_requests,
            ]
            .context("Failed to create specjbb profile DataFrame")?;

            self.profile_df = Some(df);
        }

        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, LazyFrame)>> {
        let mut rv = Vec::new();
        if let Some(df) = self.summary_df {
            rv.push((Intern::from_static("specjbb_summary"), df.lazy()));
        }
        if let Some(df) = self.profile_df {
            // clock_time now carries a real numeric UTC offset (%z), so Polars normalizes it
            // to true UTC directly, same pattern as bench-suite-collector-zgc-phases.
            let lf = df.lazy().with_column(col("clock_time").str().to_datetime(
                Some(TimeUnit::Milliseconds),
                None,
                StrptimeOptions {
                    format: Some("%a %b %d %H:%M:%S %z %Y".into()),
                    strict: false,
                    exact: true,
                    cache: true,
                },
                lit("raise"),
            ));
            rv.push((Intern::from_static("specjbb_profile"), lf));
        }
        Ok(rv)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_regex_matches_sample() {
        let sample = "jbb2015.result.metric.max-jOPS = 28800\njbb2015.result.metric.critical-jOPS = 24958\njbb2015.result.SLA-10000-jOPS = 20600\n";
        let caps: Vec<_> = METRIC_REGEX.captures_iter(sample).collect();
        assert_eq!(caps.len(), 3);
        assert_eq!(&caps[0][1], "metric.max-jOPS");
        assert_eq!(&caps[0][2], "28800");
    }

    #[test]
    fn profile_regex_matches_normal_and_finished_lines() {
        let normal = "<Thu Jul 16 22:05:38 EDT 2026> org.spec.jbb.controller: PROFILE: settling, (rIR:aIR:PR = 2880:0:0) (tPR = 0) [IR is under limit] [PR is under limit] ";
        let cap = PROFILE_LINE_REGEX.captures(normal).expect("normal line should match");
        assert_eq!(&cap[5], "EDT");
        assert_eq!(&cap[7], "PROFILE");
        assert_eq!(&cap[8], "2880");
        assert_eq!(&cap[9], "0");
        assert_eq!(&cap[10], "0");
        assert_eq!(&cap[11], "0");

        let finished = "<Thu Jul 16 20:21:25 EDT 2026> org.spec.jbb.controller: WARMUP: IR = 0 finished, settle status = [OK] (rIR:aIR:PR = 0:0:0) (tPR = 0) ";
        let cap = PROFILE_LINE_REGEX.captures(finished).expect("finished line should match");
        assert_eq!(&cap[7], "WARMUP");
        assert_eq!(&cap[11], "0");
    }

    #[test]
    fn tz_offset_supports_only_eastern() {
        assert_eq!(tz_offset("EDT").unwrap(), "-0400");
        assert_eq!(tz_offset("EST").unwrap(), "-0500");
        assert!(tz_offset("PST").is_err());
    }
}
