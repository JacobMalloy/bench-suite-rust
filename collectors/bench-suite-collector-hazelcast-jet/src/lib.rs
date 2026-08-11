use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use regex::Regex;
use std::sync::LazyLock;
use string_intern::Intern;

// Matches Sinks.logger output lines from StreamingBenchmark, e.g.:
// 2026-07-21 22:10:00,123 [INFO ] [t.1] [Sinks.Logger.0]: time 3,844: latency 5 ms, cca. 10,000 keys
//
// The "time X" value in the message itself is NOT usable as a timestamp: StreamingBenchmark
// prints `MILLISECONDS.toSeconds(windowEnd) % 10_000`, i.e. epoch seconds wrapped every ~2.78
// hours, so it's deliberately left unparsed here. The leading log4j2 timestamp (default pattern
// `yyyy-MM-dd HH:mm:ss,SSS`) is captured instead and combined with `latency_ms` (a real
// millisecond delta) in `transform_latency` to recover the window's true end time.
static LATENCY_LINE_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}).*?time [\d,]+: latency (-?[\d,]+) ms, cca\. ([\d,]+) keys",
    )
    .unwrap()
});

fn parse_comma_u64(s: &str) -> anyhow::Result<u64> {
    s.replace(',', "").parse().context("Failed to parse number")
}

fn parse_comma_i64(s: &str) -> anyhow::Result<i64> {
    s.replace(',', "").parse().context("Failed to parse number")
}

#[derive(Default)]
pub struct BenchSuiteCollectHazelcastJet {
    latency_df: Option<DataFrame>,
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
        if file.name() != "jvm0.stdout" {
            return Ok(());
        }

        if self.latency_df.is_some() {
            return Err(anyhow::anyhow!("Duplicate jvm0.stdout files"));
        }

        let content = file.content_string()?;

        let mut clock_times: Vec<String> = Vec::new();
        let mut latency_ms: Vec<i64> = Vec::new();
        let mut keys: Vec<u64> = Vec::new();

        for cap in LATENCY_LINE_REGEX.captures_iter(content) {
            // Normalize log4j2's comma millisecond separator ("ss,SSS") to a period so it
            // parses with a standard strptime format in `transform_latency`.
            let clock_time = cap
                .get(1)
                .context("Missing clock time")?
                .as_str()
                .replacen(',', ".", 1);
            clock_times.push(clock_time);
            latency_ms.push(parse_comma_i64(cap.get(2).context("Missing latency")?.as_str())?);
            keys.push(parse_comma_u64(cap.get(3).context("Missing keys")?.as_str())?);
        }

        let df = df![
            "clock_time" => clock_times,
            "latency_ms" => latency_ms,
            "keys" => keys,
        ]
        .context("Failed to create hazelcast jet latency DataFrame")?;

        self.latency_df = Some(df);

        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, LazyFrame)>> {
        let mut rv = Vec::new();
        if let Some(df) = self.latency_df {
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
        let sample = "2026-07-21 22:10:00,123 [INFO ] [t.1] [Sinks.Logger.0]: time 3,844: latency 5 ms, cca. 10,000 keys";
        let cap = LATENCY_LINE_REGEX
            .captures(sample)
            .expect("line should match");
        assert_eq!(&cap[1], "2026-07-21 22:10:00,123");
        assert_eq!(&cap[2], "5");
        assert_eq!(&cap[3], "10,000");
        assert_eq!(parse_comma_i64(&cap[2]).unwrap(), 5);
        assert_eq!(parse_comma_u64(&cap[3]).unwrap(), 10_000);
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
