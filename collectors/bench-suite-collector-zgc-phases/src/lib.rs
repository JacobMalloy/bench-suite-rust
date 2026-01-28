use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use regex::Regex;
use std::sync::LazyLock;
use string_intern::Intern;

static GC_PHASE_TIMES_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"\[([^\]]*)\]\[info\s*\]\[gc,phases\s*\] GC\((\d+)\) ([YO]): ([A-Za-z \-]+) ([0-9.]+)ms",
    )
    .unwrap()
});

#[derive(Debug, Default)]
pub struct BenchSuiteCollectZgcPhases {
    phases_df: Option<DataFrame>,
}

impl BenchSuiteCollectZgcPhases {
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

impl BenchSuiteCollect for BenchSuiteCollectZgcPhases {
    fn process_file(
        &mut self,
        run: &bench_suite_types::BenchSuiteRun,
        file: &mut dyn bench_suite_collect_results::FileInfoInterface,
    ) -> anyhow::Result<()> {
        // Only process if GC is ZGC
        match &run.gc {
            Some(gc) if gc.as_str().to_lowercase().contains("zgc") => {}
            _ => return Ok(()),
        }

        if file.name() != "jvm0.txt" {
            return Ok(());
        }

        if self.phases_df.is_some() {
            return Err(anyhow::anyhow!("Duplicate jvm0.txt files"));
        }

        let content = file.content_string()?;

        let mut clock_times: Vec<String> = Vec::new();
        let mut gc_numbers: Vec<u32> = Vec::new();
        let mut phase_types: Vec<String> = Vec::new();
        let mut phase_names: Vec<String> = Vec::new();
        let mut phase_times_ms: Vec<f64> = Vec::new();

        for cap in GC_PHASE_TIMES_REGEX.captures_iter(content) {
            let clock_time = cap.get(1).context("Missing clock time")?.as_str();
            let gc_number: u32 = cap
                .get(2)
                .context("Missing GC number")?
                .as_str()
                .parse()
                .context("Failed to parse GC number")?;
            let phase_type = cap.get(3).context("Missing phase type")?.as_str();
            let phase_name = cap.get(4).context("Missing phase name")?.as_str().trim();
            let phase_time: f64 = cap
                .get(5)
                .context("Missing phase time")?
                .as_str()
                .parse()
                .context("Failed to parse phase time")?;

            clock_times.push(clock_time.to_string());
            gc_numbers.push(gc_number);
            phase_types.push(phase_type.to_string());
            phase_names.push(phase_name.to_string());
            phase_times_ms.push(phase_time);
        }

        let df = df![
            "gc_phase_clock_time" => clock_times,
            "gc_phase_gc_number" => gc_numbers,
            "gc_phase_type" => phase_types,
            "gc_phase_name" => phase_names,
            "gc_phase_time_ms" => phase_times_ms,
        ]
        .context("Failed to create phases DataFrame")?;

        self.phases_df = Some(df);

        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, LazyFrame)>> {
        let mut rv = Vec::new();
        if let Some(df) = self.phases_df {
            rv.push((Intern::from_static("zgc_phases"), df.lazy()));
        }
        Ok(rv)
    }
}
