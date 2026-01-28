use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use std::collections::HashMap;
use string_intern::Intern;

#[derive(Debug, Default)]
pub struct BenchSuiteCollectStatus {
    status: Option<String>,
    runner_exits: HashMap<u32, i32>,
}

impl BenchSuiteCollectStatus {
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

impl BenchSuiteCollect for BenchSuiteCollectStatus {
    fn process_file(
        &mut self,
        _: &bench_suite_types::BenchSuiteRun,
        file: &mut dyn bench_suite_collect_results::FileInfoInterface,
    ) -> anyhow::Result<()> {
        let name = file.name();

        if name == "status.txt" {
            if self.status.is_some() {
                return Err(anyhow::anyhow!("Duplicate status.txt files"));
            }
            self.status = Some(file.content_string()?.trim().to_string());
            return Ok(());
        }

        // Check for runnerN.exit files
        if let Some(rest) = name.strip_prefix("runner")
            && let Some(num_str) = rest.strip_suffix(".exit")
            && let Ok(runner_num) = num_str.parse::<u32>()
        {
            let exit_code: i32 = file
                .content_string()?
                .trim()
                .parse()
                .context("Failed to parse runner exit code")?;
            self.runner_exits.insert(runner_num, exit_code);
        }

        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, LazyFrame)>> {
        let mut status = self.status.unwrap_or_else(|| "unknown".to_string());

        // If status is success, check runner exit codes
        if status.to_lowercase() == "success" {
            let mut runner_nums: Vec<_> = self.runner_exits.keys().collect();
            runner_nums.sort();

            for &runner_num in &runner_nums {
                let exit_code = self.runner_exits[runner_num];
                if exit_code != 0 {
                    status = format!("runner{} exited with code {}", runner_num, exit_code);
                    break;
                }
            }
        }
        let df = df![
            "status" => &[status],
        ]
        .context("Failed to create status DataFrame")?;

        Ok(vec![(Intern::from_static("status"), df.lazy())])
    }
}
