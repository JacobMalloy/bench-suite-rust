use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use std::collections::HashMap;
use string_intern::Intern;

#[derive(Debug, Default)]
pub struct BenchSuiteCollectStatus {
    status: Option<String>,
    runner_exits: HashMap<u32, i32>,
    resctrl_failed: bool,
}

impl BenchSuiteCollectStatus {
    #[must_use]
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

/// True when `resctrl_status.txt` holds one of runner.py's failure messages
/// rather than a `dump_status()` listing.
fn is_harness_resctrl_failure(content: &str) -> bool {
    content
        .lines()
        .any(|line| line.starts_with("resctrl ") && line.contains(" failed: "))
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

        if name == "os.javalog"
            || name == "jvm0.txt" // LEGACY: remove once all tests use split files
        {
            if file.content_string()?.contains("Resctrl: Failed") {
                self.resctrl_failed = true;
            }
            return Ok(());
        }

        // The HARNESS side of the same failure, which the check above cannot
        // see. runner.py catches a ResctrlError from resctrl.apply_config(),
        // writes the reason here, and LETS THE RUN PROCEED -- so without this
        // a run whose groups were never created reports success, while
        // ResctrlMon, which discovers groups when the benchmark starts, found
        // none and sampled only the root group. That is the same silently-
        // wrong shape as the 2026-09-16 cache-ways sweep: the treatment is on
        // the command line and in the config, and absent from the machine.
        //
        // dump_status() writes one "[<group>] ..." line per group on success,
        // so a leading "resctrl <something> failed: " cannot be confused with
        // it. Matching the prefix rather than a whole message covers all three
        // the runner emits (reset / setup / status dump).
        if name == "resctrl_status.txt" {
            if is_harness_resctrl_failure(file.content_string()?) {
                self.resctrl_failed = true;
            }
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
                    status = format!("runner{runner_num} exited with code {exit_code}");
                    break;
                }
            }

            if status.to_lowercase() == "success" && self.resctrl_failed {
                status = "resctrl_failed".to_string();
            }
        }
        let df = df![
            "status" => &[status],
        ]
        .context("Failed to create status DataFrame")?;

        Ok(vec![(Intern::from_static("status"), df.lazy())])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `dump_status` output must not be read as a failure: every line of it
    /// starts with "[<group>]".
    #[test]
    fn dump_status_is_not_a_failure() {
        let dump = concat!(
            "[default] schemata:     MB:0=100\n",
            "    L3:0=7ff\n",
            "[COS1] schemata: L3:0=7ff; cpus_list: -; tasks: 9\n",
            "[COS1/mon_groups/ymark] schemata: -; cpus_list: -; tasks: 8\n",
        );
        assert!(!is_harness_resctrl_failure(dump));
    }

    #[test]
    fn each_runner_failure_message_is_detected() {
        for line in [
            "resctrl setup failed: COS1 already exists\n",
            "resctrl reset failed: resctrl unavailable: [Errno 2] No such file\n",
            "resctrl status dump failed: failed to read /sys/fs/resctrl/schemata\n",
        ] {
            assert!(is_harness_resctrl_failure(line), "missed {line:?}");
        }
    }

    /// A group literally named so as to look like a message still cannot
    /// trigger it, because the dump always puts the name in brackets first.
    #[test]
    fn bracketed_group_named_like_a_message() {
        assert!(!is_harness_resctrl_failure(
            "[resctrl setup failed: x] schemata: -; cpus_list: -; tasks: 1\n"
        ));
    }
}
