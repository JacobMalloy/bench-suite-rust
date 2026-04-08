use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use regex::Regex;
use std::collections::HashMap;
use std::sync::LazyLock;
use string_intern::Intern;

// Matches: GC(0) Major Collection (Warmup)  or  GC(0) Minor Collection (Eden)
static GC_COLLECTION_TYPE_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\[gc\s*\] GC\((\d+)\) (Major|Minor) Collection").unwrap()
});

// Matches:
//   GC(0) Using 4 Workers for Young Generation
//   GC(0) O: Using 4 Workers for Old Generation
static GC_TASK_WORKERS_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"\[gc,task\s*\] GC\((\d+)\)(?:( O:))? Using (\d+) Workers for (\w+) Generation",
    )
    .unwrap()
});

#[derive(Debug, Default)]
pub struct BenchSuiteCollectZgcTask {
    task_df: Option<DataFrame>,
}

impl BenchSuiteCollectZgcTask {
    #[must_use]
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

impl BenchSuiteCollect for BenchSuiteCollectZgcTask {
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

        if self.task_df.is_some() {
            return Err(anyhow::anyhow!("Duplicate jvm0.txt files"));
        }

        let content = file.content_string()?;

        // Build gc_number -> "major"/"minor" map
        let mut collection_types: HashMap<u64, &str> = HashMap::new();
        for cap in GC_COLLECTION_TYPE_REGEX.captures_iter(content) {
            let gc_number: u64 = cap
                .get(1)
                .context("Missing GC number")?
                .as_str()
                .parse()
                .context("Failed to parse GC number")?;
            let collection_type = match cap.get(2).context("Missing collection type")?.as_str() {
                "Major" => "major",
                _ => "minor",
            };
            collection_types.insert(gc_number, collection_type);
        }

        let mut gc_numbers: Vec<u64> = Vec::new();
        let mut types: Vec<String> = Vec::new();
        let mut ages: Vec<&str> = Vec::new();
        let mut num_workers: Vec<u32> = Vec::new();

        for cap in GC_TASK_WORKERS_REGEX.captures_iter(content) {
            let gc_number: u64 = cap
                .get(1)
                .context("Missing GC number")?
                .as_str()
                .parse()
                .context("Failed to parse GC number")?;
            let workers: u32 = cap
                .get(3)
                .context("Missing worker count")?
                .as_str()
                .parse()
                .context("Failed to parse worker count")?;
            let age = if cap
                .get(4)
                .context("Missing generation")?
                .as_str()
                .to_lowercase()
                == "young"
            {
                "y"
            } else {
                "o"
            };
            let collection_type = collection_types
                .get(&gc_number)
                .copied()
                .unwrap_or("unknown");

            gc_numbers.push(gc_number);
            types.push(collection_type.to_string());
            ages.push(age);
            num_workers.push(workers);
        }

        let df = df![
            "gc_number" => gc_numbers,
            "type" => types,
            "age" => ages,
            "num_workers" => num_workers,
        ]
        .context("Failed to create zgc_task DataFrame")?;

        self.task_df = Some(df);

        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, LazyFrame)>> {
        let mut rv = Vec::new();
        if let Some(df) = self.task_df {
            rv.push((Intern::from_static("zgc_task_workers"), df.lazy()));
        }
        Ok(rv)
    }
}
