use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use regex::Regex;
use std::collections::HashMap;
use std::sync::LazyLock;
use string_intern::Intern;

static THREAD_ID_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"\[[^\]]*\]\[[^\]]*\]\[[^\]]*\] Thread created tid: ([0-9]*), name:"([^"]*)", thread_type:"([^"]*)""#).unwrap()
});

static THREAD_ATTACH_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\[[^\]]*\]\[[^\]]*\]\[[^\]]*\] Thread attached \(tid: ([0-9]+)\s*,").unwrap()
});

#[derive(Debug, Default)]
pub struct BenchSuiteCollectJavaThreads {
    threads_df: Option<DataFrame>,
}

impl BenchSuiteCollectJavaThreads {
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

impl BenchSuiteCollect for BenchSuiteCollectJavaThreads {
    fn process_file(
        &mut self,
        _: &bench_suite_types::BenchSuiteRun,
        file: &mut dyn bench_suite_collect_results::FileInfoInterface,
    ) -> anyhow::Result<()> {
        if file.name() != "jvm0.txt" {
            return Ok(());
        }

        if self.threads_df.is_some() {
            return Err(anyhow::anyhow!("Duplicate jvm0.txt files"));
        }

        let content = file.content_string()?;

        // Map from tid -> (thread_name, thread_type)
        let mut thread_map: HashMap<u64, (String, String)> = HashMap::new();

        // Parse "Thread created" lines
        for cap in THREAD_ID_REGEX.captures_iter(content) {
            let tid: u64 = cap
                .get(1)
                .context("Missing tid")?
                .as_str()
                .parse()
                .context("Failed to parse tid")?;
            let name = cap
                .get(2)
                .context("Missing thread name")?
                .as_str()
                .to_string();
            let thread_type = cap
                .get(3)
                .context("Missing thread type")?
                .as_str()
                .to_string();

            thread_map.insert(tid, (name, thread_type));
        }

        // Parse "Thread attached" lines - add unknown entries for tids not already seen
        for cap in THREAD_ATTACH_REGEX.captures_iter(content) {
            let tid: u64 = cap
                .get(1)
                .context("Missing tid")?
                .as_str()
                .parse()
                .context("Failed to parse tid")?;

            thread_map
                .entry(tid)
                .or_insert_with(|| ("unknown".to_string(), "unknown".to_string()));
        }

        let mut pids: Vec<u64> = Vec::with_capacity(thread_map.len());
        let mut names: Vec<String> = Vec::with_capacity(thread_map.len());
        let mut types: Vec<String> = Vec::with_capacity(thread_map.len());

        for (pid, (name, thread_type)) in thread_map {
            pids.push(pid);
            names.push(name);
            types.push(thread_type);
        }

        let df = df![
            "pid" => pids,
            "thread_name" => names,
            "thread_type" => types,
        ]
        .context("Failed to create threads DataFrame")?;

        self.threads_df = Some(df);

        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, DataFrame)>> {
        let mut rv = Vec::new();
        if let Some(df) = self.threads_df {
            rv.push((Intern::from_static("java_threads"), df));
        }
        Ok(rv)
    }
}
