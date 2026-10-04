use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use std::sync::Arc;
use string_intern::Intern;

/// Collects the resctrl monitoring series written by `utils/resctrl.py`'s
/// `ResctrlMon` when a config sets `l3_mon` or `memory_mon`.
///
/// These are the only counters in the suite attributed to a resctrl GROUP
/// rather than to a cpu or a thread, which is what lets them separate GC from
/// mutator by group membership instead of by core placement: the JVM writes
/// the GC's thread tids into COS1/tasks itself. And they measure a thing no
/// eviction counter can -- how much L3 each class is HOLDING, as opposed to
/// how many lines it is moving.
///
/// One file per event, each `timestamp,group,value`, all stacked into a single
/// long-form `resctrl_mon` table with an `event` column. Long rather than a
/// table per event so that adding a monitoring file later needs no schema
/// change, matching how `threadstat_event` carries its event in a column.
///
/// TWO SEMANTICS SHARE THIS TABLE and the `event` column is the only thing
/// separating them:
///
///   `llc_occupancy`    an instantaneous LEVEL in bytes. Do NOT difference it.
///                      Every threadstat metric in `gc_window_metrics` lags its
///                      counter because threadstat's are cumulative; doing
///                      that here is meaningless. It also does not decay when
///                      a group goes idle -- a line stays charged to whoever
///                      allocated it until eviction -- so occupancy built
///                      during a GC phase persists into the gap after it.
///   `mbm_total_bytes`  cumulative byte counters. These DO need differencing,
///   `mbm_local_bytes`  and they wrap.
const FILES: [(&str, &str); 3] = [
    ("resctrl_llc_occupancy.csv", "llc_occupancy"),
    ("resctrl_mbm_total_bytes.csv", "mbm_total_bytes"),
    ("resctrl_mbm_local_bytes.csv", "mbm_local_bytes"),
];

/// `ResctrlMon` writes an empty value when a counter read failed or when
/// resctrl answered "Unavailable", which it does while an RMID is being
/// recycled. Kept as a null rather than dropped so the gap stays visible in
/// the series, and never as a zero, which would read as "this group held
/// nothing" instead of "nobody knows".
fn parse(bytes: &[u8], file: &str, event: &'static str) -> anyhow::Result<LazyFrame> {
    // Pinned rather than inferred for the same reason the msr collector pins
    // its own: a file whose every read failed is all-empty, which would infer
    // as a different dtype than a file that read fine and then refuse to
    // concat with it.
    let schema = Schema::from_iter([
        (PlSmallStr::from_static("timestamp"), DataType::Int64),
        (PlSmallStr::from_static("group"), DataType::String),
        (PlSmallStr::from_static("value"), DataType::String),
    ]);

    let cursor = std::io::Cursor::new(bytes);
    let df = CsvReadOptions::default()
        .with_has_header(true)
        .with_schema(Some(Arc::new(schema)))
        .into_reader_with_file_handle(cursor)
        .finish()
        .with_context(|| format!("Failed to parse {file}"))?;

    // `group` arrives as "<resctrl group>/<mon_L3_NN>", because a counter is
    // per group PER L3 DOMAIN and ResctrlMon refuses to collapse the domains
    // -- summing two sockets silently would be the obvious way to get a
    // dual-socket answer wrong. Split back out here so a query can group on
    // either without string work.
    let labels = df.column("group")?.str()?;
    let mut groups: Vec<Option<&str>> = Vec::with_capacity(df.height());
    let mut domains: Vec<Option<&str>> = Vec::with_capacity(df.height());
    for label in labels {
        // A label with no domain suffix is not something ResctrlMon can
        // write, so rather than guess, keep the whole string as the group and
        // leave the domain null where it will be noticed.
        if let Some((group, domain)) = label.and_then(|l| l.rsplit_once('/')) {
            groups.push(Some(group));
            domains.push(Some(domain));
        } else {
            groups.push(label);
            domains.push(None);
        }
    }

    let mut values: Vec<Option<u64>> = Vec::with_capacity(df.height());
    for value in df.column("value")?.str()? {
        values.push(match value {
            None | Some("") => None,
            Some(text) => Some(
                text.parse()
                    .with_context(|| format!("Failed to parse {file} value {text:?}"))?,
            ),
        });
    }

    let stamps: Vec<Option<i64>> = df.column("timestamp")?.i64()?.to_vec();

    let out = df![
        "timestamp" => stamps,
        "group" => groups,
        "domain" => domains,
        "value" => values,
    ]
    .with_context(|| format!("Failed to build a frame for {file}"))?;

    Ok(out.lazy().select([
        // Epoch nanoseconds off time.time(), so this lands on exactly the
        // clock threadstat_read.timestamp and zgc_phases use -- no offset file
        // of the kind PerfRecord and PerfStat both need.
        col("timestamp")
            .cast(DataType::Datetime(TimeUnit::Nanoseconds, None))
            .alias("wall_time"),
        col("group"),
        col("domain"),
        lit(event).alias("event"),
        col("value"),
    ]))
}

#[derive(Default)]
pub struct BenchSuiteCollectResctrlMon {
    frames: Vec<LazyFrame>,
    seen: Vec<&'static str>,
}

impl BenchSuiteCollectResctrlMon {
    #[must_use]
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

impl BenchSuiteCollect for BenchSuiteCollectResctrlMon {
    fn process_file(
        &mut self,
        _: &bench_suite_types::BenchSuiteRun,
        file: &mut dyn bench_suite_collect_results::FileInfoInterface,
    ) -> anyhow::Result<()> {
        let Some((name, event)) = FILES.iter().find(|(name, _)| *name == file.name()) else {
            return Ok(());
        };
        if self.seen.contains(event) {
            return Err(anyhow::anyhow!("Duplicate {name} files"));
        }
        self.seen.push(event);
        self.frames.push(parse(file.content_bytes()?, name, event)?);
        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, LazyFrame)>> {
        let BenchSuiteCollectResctrlMon { frames, .. } = *self;
        if frames.is_empty() {
            return Ok(Vec::new());
        }
        let combined = concat(frames, UnionArgs::default())?;
        Ok(vec![(Intern::from_static("resctrl_mon"), combined)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER: &str = "timestamp,group,value\n";

    fn collect(content: &str, event: &'static str) -> DataFrame {
        parse(content.as_bytes(), "test.csv", event)
            .unwrap()
            .collect()
            .unwrap()
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
    fn splits_the_group_and_domain_and_dates_the_timestamp() {
        let df = collect(
            &format!(
                "{HEADER}1759600000000000000,root/mon_L3_00,123456\n\
                 1759600000000000000,COS1/mon_L3_00,7890\n"
            ),
            "llc_occupancy",
        );
        assert_eq!(df.height(), 2);
        assert_eq!(
            strs(&df, "group"),
            vec![Some("root".into()), Some("COS1".into())]
        );
        assert_eq!(
            strs(&df, "domain"),
            vec![Some("mon_L3_00".into()), Some("mon_L3_00".into())]
        );
        assert_eq!(
            df.column("value").unwrap().u64().unwrap().to_vec(),
            vec![Some(123_456), Some(7890)]
        );
        assert_eq!(
            df.column("wall_time").unwrap().dtype(),
            &DataType::Datetime(TimeUnit::Nanoseconds, None)
        );
        assert_eq!(strs(&df, "event")[0], Some("llc_occupancy".into()));
    }

    #[test]
    fn a_second_l3_domain_stays_separate() {
        // brazil has two; summing them would silently add a second socket's
        // cache to the answer.
        let df = collect(
            &format!("{HEADER}1,COS1/mon_L3_00,100\n1,COS1/mon_L3_01,200\n"),
            "llc_occupancy",
        );
        assert_eq!(
            strs(&df, "domain"),
            vec![Some("mon_L3_00".into()), Some("mon_L3_01".into())]
        );
        assert_eq!(strs(&df, "group"), vec![Some("COS1".into()); 2]);
    }

    #[test]
    fn an_unavailable_read_is_null_and_never_zero() {
        // ResctrlMon writes an empty value for "Unavailable" / a failed read.
        // Zero would read as "this group held nothing", which is a different
        // claim from "nobody knows".
        let df = collect(
            &format!("{HEADER}1,root/mon_L3_00,\n2,root/mon_L3_00,64\n"),
            "llc_occupancy",
        );
        assert_eq!(
            df.column("value").unwrap().u64().unwrap().to_vec(),
            vec![None, Some(64)]
        );
    }

    #[test]
    fn a_file_where_every_read_failed_still_types_as_u64() {
        let df = collect(&format!("{HEADER}1,root/mon_L3_00,\n"), "llc_occupancy");
        assert_eq!(df.column("value").unwrap().dtype(), &DataType::UInt64);
    }

    #[test]
    fn a_header_only_file_is_empty_and_still_typed() {
        // What a run gets if the sampler thread died before its first pass.
        let df = collect(HEADER, "mbm_total_bytes");
        assert_eq!(df.height(), 0);
        assert_eq!(df.column("value").unwrap().dtype(), &DataType::UInt64);
    }

    #[test]
    fn a_mangled_value_fails_the_file() {
        assert!(
            parse(
                format!("{HEADER}1,root/mon_L3_00,garbage\n").as_bytes(),
                "test.csv",
                "llc_occupancy"
            )
            .is_err()
        );
    }

    #[test]
    fn the_three_events_stack_into_one_long_table() {
        let occ = parse(
            format!("{HEADER}1,COS1/mon_L3_00,4096\n").as_bytes(),
            "a",
            "llc_occupancy",
        )
        .unwrap();
        let tot = parse(
            format!("{HEADER}1,COS1/mon_L3_00,999\n").as_bytes(),
            "b",
            "mbm_total_bytes",
        )
        .unwrap();
        let df = concat([occ, tot], UnionArgs::default())
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(df.height(), 2);
        assert_eq!(
            strs(&df, "event"),
            vec![Some("llc_occupancy".into()), Some("mbm_total_bytes".into())]
        );
    }
}
