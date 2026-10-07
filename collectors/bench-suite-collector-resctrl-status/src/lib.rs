//! Collects `resctrl_status.txt`, the per-run dump of every resctrl group's
//! allocation and membership that `utils/resctrl.py`'s `dump_status()` writes
//! after `apply_config()`.
//!
//! WHY THIS IS WORTH A TABLE. Cache allocation moved out of the JVM and into
//! the harness, and a sweep that measures per-phase cache behaviour no longer
//! asserts the way mask in its own preflight -- a group created without a mask
//! takes the kernel's default, so there is nothing for the config to check
//! before the run. This file is the only record of what the groups ACTUALLY
//! got, and of whether the JVM wrote any thread ids into the per-phase
//! monitoring groups at all. Without it, confirming that means untarring runs
//! by hand.
//!
//! FORMAT, which is not quite line-oriented:
//!
//! ```text
//! [default] schemata:     MB:0=100
//!     L3:0=7ff
//! [COS1] schemata: L3:0=7ff; cpus_list: ; tasks: 9
//! [COS1/mon_groups/ymark] schemata: -; cpus_list: -; tasks: 8
//! ```
//!
//! A record starts at a line beginning `[`, but a group's schemata spans
//! several lines whenever the kernel exposes more than one resource: resctrl
//! right-pads resource names to the widest one, so the `L3:` line arrives
//! indented under an `MB:` line and `_read()` only strips the ends. So records
//! are split on `[` and the three fields are then parsed from the RIGHT, on
//! the `; cpus_list: ` and `; tasks: ` literals -- a schemata body contains
//! `;` of its own (`L3:0=7ff;1=7ff` on a two-socket box) but cannot contain
//! those.
//!
//! Monitoring groups report `-` for schemata and `cpus_list`: they have no
//! allocation of their own, sharing their parent control group's. Those come
//! back as nulls rather than being dropped, because the row existing at all is
//! the evidence that the group was created.
//!
//! One row per (group, resource, domain). A group with no schemata gets a
//! single row with those three null, so every group is present exactly once in
//! the degenerate case.

use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use string_intern::Intern;

const FILE_NAME: &str = "resctrl_status.txt";

#[derive(Debug, Default)]
pub struct BenchSuiteCollectResctrlStatus {
    frame: Option<DataFrame>,
}

impl BenchSuiteCollectResctrlStatus {
    #[must_use]
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

#[derive(Debug, Default)]
struct Rows {
    group: Vec<String>,
    resource: Vec<Option<String>>,
    domain: Vec<Option<u32>>,
    value: Vec<Option<String>>,
    cpus_list: Vec<Option<String>>,
    n_tasks: Vec<Option<u32>>,
}

impl Rows {
    fn push(
        &mut self,
        group: &str,
        resource: Option<&str>,
        domain: Option<u32>,
        value: Option<&str>,
        cpus_list: Option<&str>,
        n_tasks: Option<u32>,
    ) {
        self.group.push(group.to_string());
        self.resource.push(resource.map(str::to_string));
        self.domain.push(domain);
        self.value.push(value.map(str::to_string));
        self.cpus_list.push(cpus_list.map(str::to_string));
        self.n_tasks.push(n_tasks);
    }
}

/// `-` is how `dump_status` spells "this group has no such file", which is the
/// normal case for a monitoring group's schemata and `cpus_list`. Empty is also
/// normal: a control group with no cpus assigned has an empty `cpus_list`.
fn dashed(s: &str) -> Option<&str> {
    let s = s.trim();
    if s.is_empty() || s == "-" {
        None
    } else {
        Some(s)
    }
}

/// Split the records. A record begins at a line starting with `[`; everything
/// up to the next such line belongs to it, because a multi-resource schemata
/// continues onto indented lines.
fn records(content: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for line in content.lines() {
        if line.starts_with('[') {
            out.push(line.to_string());
        } else if let Some(last) = out.last_mut() {
            last.push('\n');
            last.push_str(line);
        }
        // Lines before the first record are a harness failure message
        // ("resctrl setup failed: ..."); the status collector reports those,
        // and there is nothing to tabulate here.
    }
    out
}

fn parse(content: &str) -> anyhow::Result<Option<DataFrame>> {
    let mut rows = Rows::default();

    for record in records(content) {
        let Some((name, rest)) = record
            .strip_prefix('[')
            .and_then(|r| r.split_once(']'))
        else {
            return Err(anyhow::anyhow!(
                "unterminated group name in {FILE_NAME}: {record:?}"
            ));
        };
        let group = name.trim();

        // From the right: these two literals cannot occur inside a schemata.
        let (rest, n_tasks) = match rest.rsplit_once("; tasks: ") {
            Some((head, tasks)) => (head, dashed(tasks).map(str::parse).transpose()?),
            None => (rest, None),
        };
        let (rest, cpus_list) = match rest.rsplit_once("; cpus_list: ") {
            Some((head, cpus)) => (head, dashed(cpus)),
            None => (rest, None),
        };

        let schemata = rest
            .trim_start()
            .strip_prefix("schemata:")
            .map(str::trim)
            .unwrap_or_default();

        let mut any = false;
        if dashed(schemata).is_some() {
            // "MB:0=100\n    L3:0=7ff;1=7ff" -> one row per resource/domain.
            for line in schemata.lines() {
                let Some((resource, body)) = line.trim().split_once(':') else {
                    continue;
                };
                for part in body.split(';') {
                    let Some((domain, value)) = part.trim().split_once('=') else {
                        continue;
                    };
                    rows.push(
                        group,
                        Some(resource.trim()),
                        Some(domain.trim().parse()?),
                        Some(value.trim()),
                        cpus_list,
                        n_tasks,
                    );
                    any = true;
                }
            }
        }
        if !any {
            // No allocation of its own -- a monitoring group, or a box with no
            // L3 CAT. The row still has to exist: it is the evidence that the
            // group was created and how many tasks landed in it.
            rows.push(group, None, None, None, cpus_list, n_tasks);
        }
    }

    if rows.group.is_empty() {
        return Ok(None);
    }

    let height = rows.group.len();
    let df = DataFrame::new(height, vec![
        Column::new(PlSmallStr::from_static("group"), rows.group),
        Column::new(PlSmallStr::from_static("resource"), rows.resource),
        Column::new(PlSmallStr::from_static("domain"), rows.domain),
        Column::new(PlSmallStr::from_static("value"), rows.value),
        Column::new(PlSmallStr::from_static("cpus_list"), rows.cpus_list),
        Column::new(PlSmallStr::from_static("n_tasks"), rows.n_tasks),
    ])?;
    Ok(Some(df))
}

impl BenchSuiteCollect for BenchSuiteCollectResctrlStatus {
    fn process_file(
        &mut self,
        _: &bench_suite_types::BenchSuiteRun,
        file: &mut dyn bench_suite_collect_results::FileInfoInterface,
    ) -> anyhow::Result<()> {
        if file.name() != FILE_NAME {
            return Ok(());
        }
        if self.frame.is_some() {
            return Err(anyhow::anyhow!("Duplicate {FILE_NAME} files"));
        }
        // Not an error when it holds a failure message instead of a dump: the
        // status collector turns that into status='resctrl_failed', and this
        // table simply has no rows for the run.
        self.frame = parse(file.content_string()?)?;
        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, LazyFrame)>> {
        Ok(self
            .frame
            .map_or_else(Vec::new, |df| {
                vec![(Intern::from_static("resctrl_status"), df.lazy())]
            }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(content: &str) -> DataFrame {
        parse(content).unwrap().unwrap()
    }

    fn strs(df: &DataFrame, col: &str) -> Vec<Option<String>> {
        df.column(col)
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .map(|v| v.map(str::to_string))
            .collect()
    }

    #[test]
    fn single_resource_control_group() {
        let df = collect("[COS1] schemata: L3:0=7ff; cpus_list: ; tasks: 9\n");
        assert_eq!(df.height(), 1);
        assert_eq!(strs(&df, "group"), vec![Some("COS1".to_string())]);
        assert_eq!(strs(&df, "resource"), vec![Some("L3".to_string())]);
        assert_eq!(strs(&df, "value"), vec![Some("7ff".to_string())]);
        assert_eq!(df.column("domain").unwrap().u32().unwrap().get(0), Some(0));
        assert_eq!(df.column("n_tasks").unwrap().u32().unwrap().get(0), Some(9));
        // An empty cpus_list is a null, not an empty string.
        assert_eq!(strs(&df, "cpus_list"), vec![None]);
    }

    #[test]
    fn two_domains_become_two_rows() {
        let df = collect("[COS1] schemata: L3:0=fffff;1=fffff; cpus_list: 0-5; tasks: 4\n");
        assert_eq!(df.height(), 2);
        assert_eq!(
            df.column("domain").unwrap().u32().unwrap().into_no_null_iter().collect::<Vec<_>>(),
            vec![0, 1]
        );
        // cpus_list and n_tasks are group-level and repeat down the rows.
        assert_eq!(strs(&df, "cpus_list"), vec![Some("0-5".to_string()); 2]);
    }

    /// The reason records are not lines: resctrl right-pads resource names, so
    /// a second resource pushes L3 onto its own indented line.
    #[test]
    fn multi_line_schemata() {
        let df = collect(concat!(
            "[default] schemata:     MB:0=100\n",
            "    L3:0=7ff\n",
            "[COS1] schemata: L3:0=7ff; cpus_list: -; tasks: 9\n",
        ));
        assert_eq!(df.height(), 3);
        assert_eq!(
            strs(&df, "group"),
            vec![
                Some("default".to_string()),
                Some("default".to_string()),
                Some("COS1".to_string())
            ]
        );
        assert_eq!(
            strs(&df, "resource"),
            vec![
                Some("MB".to_string()),
                Some("L3".to_string()),
                Some("L3".to_string())
            ]
        );
        assert_eq!(
            strs(&df, "value"),
            vec![
                Some("100".to_string()),
                Some("7ff".to_string()),
                Some("7ff".to_string())
            ]
        );
        // The default group's line carries no cpus_list or tasks at all.
        assert_eq!(df.column("n_tasks").unwrap().u32().unwrap().get(0), None);
        assert_eq!(df.column("n_tasks").unwrap().u32().unwrap().get(2), Some(9));
    }

    /// A monitoring group shares its parent's allocation, so it reports "-" --
    /// and the row has to survive, because its task count is the only evidence
    /// that the JVM moved workers into that phase group.
    #[test]
    fn monitoring_group_keeps_its_row() {
        let df = collect(
            "[COS1/mon_groups/ymark] schemata: -; cpus_list: -; tasks: 8\n",
        );
        assert_eq!(df.height(), 1);
        assert_eq!(
            strs(&df, "group"),
            vec![Some("COS1/mon_groups/ymark".to_string())]
        );
        assert_eq!(strs(&df, "resource"), vec![None]);
        assert_eq!(strs(&df, "value"), vec![None]);
        assert_eq!(df.column("domain").unwrap().u32().unwrap().get(0), None);
        assert_eq!(df.column("n_tasks").unwrap().u32().unwrap().get(0), Some(8));
    }

    /// A CMT-only box has no L3 schemata at all; the group must still appear.
    #[test]
    fn no_cat_still_lists_the_group() {
        let df = collect("[COS1] schemata: -; cpus_list: -; tasks: 2\n");
        assert_eq!(df.height(), 1);
        assert_eq!(strs(&df, "resource"), vec![None]);
        assert_eq!(df.column("n_tasks").unwrap().u32().unwrap().get(0), Some(2));
    }

    /// runner.py writes the failure reason here instead of a dump, and lets the
    /// run proceed. No rows, no error -- the status collector flags the run.
    #[test]
    fn failure_message_yields_no_table() {
        assert!(parse("resctrl setup failed: COS1 already exists\n")
            .unwrap()
            .is_none());
        assert!(parse("").unwrap().is_none());
    }

    #[test]
    fn six_phase_groups_round_trip() {
        let mut text = String::from("[default] schemata: L3:0=7ff\n");
        text.push_str("[COS1] schemata: L3:0=7ff; cpus_list: -; tasks: 1\n");
        text.push_str("[COS2] schemata: L3:0=7ff; cpus_list: -; tasks: 40\n");
        for g in ["ymark", "ysel", "yreloc", "omark", "osel", "oreloc"] {
            use std::fmt::Write as _;
            writeln!(text, "[COS1/mon_groups/{g}] schemata: -; cpus_list: -; tasks: 8")
                .unwrap();
        }
        let df = collect(&text);
        assert_eq!(df.height(), 9);
        let groups = strs(&df, "group");
        assert_eq!(
            groups.iter().filter(|g| g.as_deref().is_some_and(|g| g.contains("mon_groups"))).count(),
            6
        );
    }
}
