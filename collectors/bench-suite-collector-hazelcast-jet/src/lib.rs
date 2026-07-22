use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use regex::Regex;
use std::sync::LazyLock;
use string_intern::Intern;

// Matches Sinks.logger output lines from StreamingBenchmark, e.g.:
// 2026-07-21 22:10:00,123 [INFO ] [t.1] [Sinks.Logger.0]: time 3,844: latency 5 ms, cca. 10,000 keys
static LATENCY_LINE_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"time ([\d,]+): latency (-?[\d,]+) ms, cca\. ([\d,]+) keys").unwrap()
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

        let mut time_ms: Vec<u64> = Vec::new();
        let mut latency_ms: Vec<i64> = Vec::new();
        let mut keys: Vec<u64> = Vec::new();

        for cap in LATENCY_LINE_REGEX.captures_iter(content) {
            time_ms.push(parse_comma_u64(cap.get(1).context("Missing time")?.as_str())?);
            latency_ms.push(parse_comma_i64(cap.get(2).context("Missing latency")?.as_str())?);
            keys.push(parse_comma_u64(cap.get(3).context("Missing keys")?.as_str())?);
        }

        let df = df![
            "time_ms" => time_ms,
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
            rv.push((Intern::from_static("hazelcast_jet_latency"), df.lazy()));
        }
        Ok(rv)
    }
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
        assert_eq!(&cap[1], "3,844");
        assert_eq!(&cap[2], "5");
        assert_eq!(&cap[3], "10,000");
        assert_eq!(parse_comma_u64(&cap[1]).unwrap(), 3844);
        assert_eq!(parse_comma_i64(&cap[2]).unwrap(), 5);
        assert_eq!(parse_comma_u64(&cap[3]).unwrap(), 10_000);
    }
}
