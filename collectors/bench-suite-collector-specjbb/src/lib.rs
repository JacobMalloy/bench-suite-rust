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

/// `NaN` marks a step where nothing completed; keep it as a null so it neither
/// plots as a point nor drags an aggregate.
fn parse_rt(field: &str) -> anyhow::Result<Option<f64>> {
    if field.eq_ignore_ascii_case("nan") {
        return Ok(None);
    }
    Ok(Some(field.parse().with_context(|| {
        format!("Failed to parse specjbb response time: {field}")
    })?))
}

/// Parse the reporter's overall throughput/response-time dump.
///
/// The file opens with a title line, a `===` rule, and `Domain Marker` rows
/// repeating max-jOPS and critical-jOPS - all of which the `.raw` file already
/// provides - then a `jOPS;min;median;...` header followed by one row per
/// injection-rate step. Only the rows after that header are read, so the
/// preamble can grow markers without breaking this.
fn parse_rt_curve(content: &str) -> anyhow::Result<DataFrame> {
    let mut jops: Vec<f64> = Vec::new();
    let mut percentiles: [Vec<Option<f64>>; 6] = Default::default();
    let mut seen_header = false;

    for line in content.lines() {
        let line = line.trim();

        if !seen_header {
            // The header is the only line naming both the rate column and the
            // percentiles; markers and the rule are skipped by falling through.
            if (line.starts_with("jOPS;") || line.starts_with("IR;")) && line.contains("median") {
                seen_header = true;
            }
            continue;
        }

        if line.is_empty() {
            continue;
        }

        let fields: Vec<&str> = line.split(';').collect();
        if fields.len() != percentiles.len() + 1 {
            return Err(anyhow::anyhow!(
                "Unexpected specjbb rt-curve row with {} fields: {line}",
                fields.len()
            ));
        }

        jops.push(
            fields[0]
                .parse()
                .with_context(|| format!("Failed to parse specjbb rt-curve jOPS: {line}"))?,
        );
        for (column, field) in percentiles.iter_mut().zip(&fields[1..]) {
            column.push(parse_rt(field)?);
        }
    }

    if !seen_header {
        return Err(anyhow::anyhow!(
            "specjbb rt-curve dump has no jOPS header row"
        ));
    }

    let [min, median, p90, p95, p99, max] = percentiles;
    df![
        "jops" => jops,
        "rt_min_us" => min,
        "rt_median_us" => median,
        "rt_p90_us" => p90,
        "rt_p95_us" => p95,
        "rt_p99_us" => p99,
        "rt_max_us" => max,
    ]
    .context("Failed to create specjbb rt-curve DataFrame")
}

/// Which specjbb artifact, if any, an archive member is.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum SpecjbbMember {
    /// The result file: max-jOPS, critical-jOPS and the SLA-<usec>-jOPS points.
    Raw,
    /// The controller log, one line per `RT_CURVE`/`WARMUP`/`VALIDATION` tick.
    ControllerLog,
    /// The reporter's response-time curve dump: one row per injection-rate step.
    RtCurve,
    Other,
}

/// Classify a tar member by suffix rather than by exact path.
///
/// The harness used to drop the report files at the archive root and now nests
/// them under `specjbb/`, with the controller log a further level down in
/// `specjbb/logs/`. Matching on the suffix keeps both layouts readable, so
/// result sets collected before the move stay usable.
fn classify_member(name: &str) -> SpecjbbMember {
    if name.ends_with(".raw") {
        SpecjbbMember::Raw
    } else if name.ends_with("-Controller.log") {
        SpecjbbMember::ControllerLog
    } else if name.ends_with("-overall-throughput-rt.txt") {
        SpecjbbMember::RtCurve
    } else {
        SpecjbbMember::Other
    }
}

#[derive(Debug, Default)]
pub struct BenchSuiteCollectSpecjbb {
    summary: Option<DataFrame>,
    profile: Option<DataFrame>,
    rt_curve: Option<DataFrame>,
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
        let member = classify_member(file.name());

        if member == SpecjbbMember::Raw {
            if self.summary.is_some() {
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

            self.summary = Some(df);
        } else if member == SpecjbbMember::ControllerLog {
            if self.profile.is_some() {
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

            self.profile = Some(df);
        } else if member == SpecjbbMember::RtCurve {
            if self.rt_curve.is_some() {
                return Err(anyhow::anyhow!("Duplicate specjbb rt-curve dumps"));
            }

            self.rt_curve = Some(parse_rt_curve(file.content_string()?)?);
        }

        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, LazyFrame)>> {
        let mut rv = Vec::new();
        if let Some(df) = self.summary {
            rv.push((Intern::from_static("specjbb_summary"), df.lazy()));
        }
        if let Some(df) = self.profile {
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
        if let Some(df) = self.rt_curve {
            rv.push((Intern::from_static("specjbb_rt_curve"), df.lazy()));
        }
        Ok(rv)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every member name a run's tarball can contain, in the layout the harness
    /// used before the report was moved into its own directory.
    const ROOT_LAYOUT: &[&str] = &[
        "specjbb2015-C-20260817-00001.raw",
        "specjbb2015-C-20260817-00001-Controller.log",
        "specjbb_summary.txt",
        "data/",
        "data/rt-curve/",
        "data/rt-curve/specjbb2015-C-20260817-00001-overall-throughput-rt.txt",
        "data/rt-curve/specjbb2015-C-20260817-00001-probesIRToIR.txt",
        "data/specjbb2015-C-20260817-00001-runProperties.txt",
        "reporter.log",
        "gc.javalog",
        "current_config.json",
    ];

    /// The same run in the current layout: report members under `specjbb/`,
    /// with the controller log one level further down.
    const NESTED_LAYOUT: &[&str] = &[
        "specjbb/specjbb2015-C-20260817-00001.raw",
        "specjbb/logs/specjbb2015-C-20260817-00001-Controller.log",
        "specjbb/specjbb_summary.txt",
        "specjbb/data/",
        "specjbb/data/rt-curve/",
        "specjbb/data/rt-curve/specjbb2015-C-20260817-00001-overall-throughput-rt.txt",
        "specjbb/data/rt-curve/specjbb2015-C-20260817-00001-probesIRToIR.txt",
        "specjbb/data/specjbb2015-C-20260817-00001-runProperties.txt",
        "reporter.log",
        "gc.javalog",
        "current_config.json",
    ];

    fn classify_all(names: &[&str]) -> Vec<SpecjbbMember> {
        names.iter().map(|n| classify_member(n)).collect()
    }

    #[test]
    fn both_layouts_yield_exactly_one_raw_and_one_controller_log() {
        for (label, layout) in [("root", ROOT_LAYOUT), ("nested", NESTED_LAYOUT)] {
            let found = classify_all(layout);
            assert_eq!(
                found.iter().filter(|m| **m == SpecjbbMember::Raw).count(),
                1,
                "{label} layout should have exactly one .raw"
            );
            assert_eq!(
                found
                    .iter()
                    .filter(|m| **m == SpecjbbMember::ControllerLog)
                    .count(),
                1,
                "{label} layout should have exactly one Controller.log"
            );
        }
    }

    #[test]
    fn both_layouts_classify_identically() {
        // Same run, same verdicts - the move must not change what is collected.
        assert_eq!(
            classify_all(&ROOT_LAYOUT[..2]),
            classify_all(&NESTED_LAYOUT[..2]),
        );
        assert_eq!(
            classify_member("specjbb2015-C-20260817-00001.raw"),
            classify_member("specjbb/specjbb2015-C-20260817-00001.raw"),
        );
        assert_eq!(
            classify_member("specjbb2015-C-20260817-00001-Controller.log"),
            classify_member("specjbb/logs/specjbb2015-C-20260817-00001-Controller.log"),
        );
    }

    #[test]
    fn nothing_else_in_either_layout_is_claimed() {
        // Only the three known artifacts are claimed; the sibling probe dump and
        // the directory entries tar carries must fall through.
        for name in ROOT_LAYOUT.iter().chain(NESTED_LAYOUT.iter()) {
            if name.ends_with(".raw")
                || name.ends_with("-Controller.log")
                || name.ends_with("-overall-throughput-rt.txt")
            {
                continue;
            }
            assert_eq!(
                classify_member(name),
                SpecjbbMember::Other,
                "{name} should not be claimed"
            );
        }
    }

    #[test]
    fn rt_curve_is_claimed_in_both_layouts_and_the_probe_dump_is_not() {
        for layout in [ROOT_LAYOUT, NESTED_LAYOUT] {
            assert_eq!(
                classify_all(layout)
                    .iter()
                    .filter(|m| **m == SpecjbbMember::RtCurve)
                    .count(),
                1,
                "each layout has exactly one rt-curve dump"
            );
        }
        // Its sibling shares the directory and the run prefix but holds probe
        // coverage, not response times.
        assert_eq!(
            classify_member("specjbb/data/rt-curve/specjbb2015-C-20260825-00001-probesIRToIR.txt"),
            SpecjbbMember::Other,
        );
    }

    /// Trimmed from a real dump: the title line, the rule, both domain markers,
    /// the header, the all-`NaN` zero-rate row, and two ordinary steps - the
    /// last using the scientific notation the reporter emits for large maxima.
    const RT_CURVE_SAMPLE: &str = "\n\
===================================\n\
Domain Marker;critical-jOPS;20061.0\n\
Domain Marker;max-jOPS;27600.0\n\
jOPS;min;median;90-th percentile;95-th percentile;99-th percentile;max\n\
0.0;NaN;NaN;NaN;NaN;NaN;NaN\n\
400.0;300.0;400.0;500.0;500.0;600.0;4400.0\n\
29200.0;38000.0;670000.0;1100000.0;1300000.0;3100000.0;3.8E7\n";

    #[test]
    fn rt_curve_keeps_one_row_per_step_and_drops_the_preamble() {
        let df = parse_rt_curve(RT_CURVE_SAMPLE).unwrap();

        // Three data rows - the markers and the header must not become rows.
        assert_eq!(df.height(), 3);
        assert_eq!(
            df.get_column_names(),
            ["jops", "rt_min_us", "rt_median_us", "rt_p90_us", "rt_p95_us", "rt_p99_us", "rt_max_us"],
        );

        let jops = df.column("jops").unwrap().f64().unwrap();
        assert_eq!(jops.get(1), Some(400.0));
        assert_eq!(jops.get(2), Some(29200.0));

        let p99 = df.column("rt_p99_us").unwrap().f64().unwrap();
        assert_eq!(p99.get(1), Some(600.0));
        // Scientific notation survives the round trip.
        assert_eq!(
            df.column("rt_max_us").unwrap().f64().unwrap().get(2),
            Some(3.8e7),
        );
    }

    #[test]
    fn rt_curve_nan_becomes_null_not_a_nan_value() {
        let df = parse_rt_curve(RT_CURVE_SAMPLE).unwrap();
        let median = df.column("rt_median_us").unwrap().f64().unwrap();

        // A null is skipped by aggregations; a NaN would poison them.
        assert_eq!(median.get(0), None);
        assert_eq!(df.column("rt_median_us").unwrap().null_count(), 1);
        assert_eq!(median.get(1), Some(400.0));
    }

    #[test]
    fn rt_curve_rejects_a_dump_it_cannot_trust() {
        // No header: every line looks like preamble, so silently returning an
        // empty table would hide a reporter change.
        let headerless = "\n===================================\nDomain Marker;max-jOPS;27600.0\n";
        assert!(parse_rt_curve(headerless).is_err());

        // A row that lost a column would otherwise shift percentiles sideways.
        let short_row = "jOPS;min;median;90-th percentile;95-th percentile;99-th percentile;max\n400.0;300.0;400.0;500.0;500.0;600.0\n";
        assert!(parse_rt_curve(short_row).is_err());
    }

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
