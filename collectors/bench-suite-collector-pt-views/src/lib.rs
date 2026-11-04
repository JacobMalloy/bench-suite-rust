//! Page table view samples from `-XX:+UsePTViews` (bench-suite config key
//! `java_pt_views`, JDK branch `pt-views` off jdk-24).
//!
//! The JVM writes one `ptviews.<pid>.csv` per process into the run directory
//! (the format is documented in that branch's `gc/shared/ptViews.hpp`):
//!
//! ```text
//! time,gc,event,ranges,view0,view1
//! 2026-11-03T09:38:01.729-0500,12,Young,0xfa000000+24576,<base64>,<base64>
//! ```
//!
//! One line per collection plus one at exit. `ranges` is a `;`-separated list
//! of `<hex start>+<pages>` (4K kernel pages), and each view's bitmap covers
//! the ranges laid end to end: bit `n % 8` of byte `n / 8` is the n-th page.
//! view0 is every non-GC thread, view1 the GC threads and the VM thread. A
//! read clears the bits, so a sample holds the pages touched since the
//! previous one.
//!
//! Three tables, joined on `pt_view_sample`, which is unique within a run (numbered
//! across all of its `ptviews.<pid>.csv` files, in file order):
//!
//! - `pt_views_sample`: one row per line -- pid, time, `gc_id`, event, and the
//!   page counts (`pages` covered, `view0_pages` / `view1_pages` touched by
//!   each view, `both_pages` by both).
//! - `pt_views_page`: one row per page touched by either view in a sample:
//!   its address and a bool per view. Pages neither view touched have no row
//!   -- dense rows would be pages x samples, ~249M for one 40s 96m ZGC
//!   lusearch run, against ~69M touched -- so whether an untouched page was
//!   covered at all comes from `pt_views_range`.
//! - `pt_views_range`: the address ranges each sample covered. One range,
//!   the reserved heap, for Parallel and G1; for ZGC the mapped parts, which
//!   change from sample to sample.
//!
//! `ptviews.<pid>.javalog` (the `gc+ptviews` log) only repeats the counts, so
//! it is not collected.

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bench_suite_collect_results::{BenchSuiteCollect, FileInfoInterface};
use bench_suite_types::BenchSuiteRun;
use chrono::DateTime;
use polars::prelude::*;
use string_intern::Intern;

/// Kernel base page size the bitmaps are in (x86-64).
const PAGE_SIZE: u64 = 4096;

#[derive(Debug, Default)]
struct Samples {
    sample: Vec<u32>,
    pid: Vec<u32>,
    time: Vec<i64>,
    gc_id: Vec<Option<u64>>,
    event: Vec<String>,
    pages: Vec<u64>,
    view0_pages: Vec<u64>,
    view1_pages: Vec<u64>,
    both_pages: Vec<u64>,
}

#[derive(Debug, Default)]
struct Pages {
    sample: Vec<u32>,
    page: Vec<u64>,
    view0: Vec<bool>,
    view1: Vec<bool>,
}

#[derive(Debug, Default)]
struct Ranges {
    sample: Vec<u32>,
    start: Vec<u64>,
    npages: Vec<u64>,
}

#[derive(Debug, Default)]
struct Tables {
    samples: Samples,
    pages: Pages,
    ranges: Ranges,
}

#[derive(Debug, Default)]
pub struct BenchSuiteCollectPtViews {
    pids: Vec<u32>,
    tables: Tables,
}

impl BenchSuiteCollectPtViews {
    #[must_use]
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

/// The pid in `ptviews.<pid>.csv`, or `None` for any other file.
fn file_pid(name: &str) -> Option<Result<u32>> {
    let pid = name.strip_prefix("ptviews.")?.strip_suffix(".csv")?;
    Some(
        pid.parse()
            .with_context(|| format!("Bad pid in pt views file name {name}")),
    )
}

/// `time` as -Xlog writes it, e.g. `2026-11-03T09:38:01.729-0500`, to naive
/// UTC nanoseconds -- the same convention as `threadstat_read.timestamp`.
fn parse_time(s: &str) -> Result<i64> {
    DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f%z")
        .with_context(|| format!("Failed to parse time {s}"))?
        .timestamp_nanos_opt()
        .with_context(|| format!("Time {s} is out of nanosecond range"))
}

fn parse_ranges(s: &str) -> Result<Vec<(u64, u64)>> {
    if s.is_empty() {
        return Ok(Vec::new());
    }
    s.split(';')
        .map(|r| {
            let (start, npages) = r
                .split_once('+')
                .with_context(|| format!("Range {r} has no '+'"))?;
            let start = start
                .strip_prefix("0x")
                .with_context(|| format!("Range start {start} is not hex"))?;
            Ok((
                u64::from_str_radix(start, 16)
                    .with_context(|| format!("Bad range start {start}"))?,
                npages
                    .parse()
                    .with_context(|| format!("Bad range page count {npages}"))?,
            ))
        })
        .collect()
}

fn popcount(bytes: &[u8]) -> u64 {
    bytes.iter().map(|b| u64::from(b.count_ones())).sum()
}

struct Columns {
    time: usize,
    gc: usize,
    event: usize,
    ranges: usize,
    view0: usize,
    view1: usize,
    count: usize,
}

impl Columns {
    /// Columns are found by name: the format adds new ones at the end.
    fn new(header: &str) -> Result<Self> {
        let names: Vec<&str> = header.split(',').collect();
        let find = |name: &str| {
            names
                .iter()
                .position(|n| *n == name)
                .ok_or_else(|| anyhow!("No {name} column in header {header}"))
        };
        Ok(Self {
            time: find("time")?,
            gc: find("gc")?,
            event: find("event")?,
            ranges: find("ranges")?,
            view0: find("view0")?,
            view1: find("view1")?,
            count: names.len(),
        })
    }
}

impl Tables {
    /// Everything fallible happens before the first push, so a bad line adds
    /// no rows.
    fn add_line(&mut self, cols: &Columns, pid: u32, sample: u32, line: &str) -> Result<()> {
        let fields: Vec<&str> = line.split(',').collect();
        if fields.len() != cols.count {
            bail!("{} fields, header has {}", fields.len(), cols.count);
        }
        let ranges = parse_ranges(fields[cols.ranges])?;
        let pages: u64 = ranges.iter().map(|(_, n)| n).sum();
        let view0 = BASE64
            .decode(fields[cols.view0])
            .context("Bad base64 in view0")?;
        let view1 = BASE64
            .decode(fields[cols.view1])
            .context("Bad base64 in view1")?;
        for (name, view) in [("view0", &view0), ("view1", &view1)] {
            if view.len() as u64 * 8 != pages {
                bail!("{name} has {} bytes for {pages} pages", view.len());
            }
        }
        let gc = fields[cols.gc];
        let gc_id = if gc.is_empty() {
            None
        } else {
            Some(gc.parse().with_context(|| format!("Bad gc id {gc}"))?)
        };
        let time = parse_time(fields[cols.time])?;

        let s = &mut self.samples;
        s.sample.push(sample);
        s.pid.push(pid);
        s.time.push(time);
        s.gc_id.push(gc_id);
        s.event.push(fields[cols.event].to_string());
        s.pages.push(pages);
        s.view0_pages.push(popcount(&view0));
        s.view1_pages.push(popcount(&view1));
        s.both_pages.push(
            view0
                .iter()
                .zip(&view1)
                .map(|(a, b)| u64::from((a & b).count_ones()))
                .sum(),
        );

        // The bitmaps cover the ranges end to end; walk both together.
        let mut bit = 0usize;
        for &(start, npages) in &ranges {
            let r = &mut self.ranges;
            r.sample.push(sample);
            r.start.push(start);
            r.npages.push(npages);

            let mut page = start;
            // npages is a multiple of 64, so ranges start on a byte boundary.
            for (&b0, &b1) in view0[bit / 8..].iter().zip(&view1[bit / 8..]).take(
                usize::try_from(npages / 8).context("Range too large")?,
            ) {
                let mut either = b0 | b1;
                while either != 0 {
                    let i = either.trailing_zeros();
                    either &= either - 1;
                    let p = &mut self.pages;
                    p.sample.push(sample);
                    p.page.push(page + u64::from(i) * PAGE_SIZE);
                    p.view0.push(b0 >> i & 1 != 0);
                    p.view1.push(b1 >> i & 1 != 0);
                }
                page += 8 * PAGE_SIZE;
            }
            bit += usize::try_from(npages).context("Range too large")?;
        }
        Ok(())
    }

    fn append(&mut self, mut o: Self) {
        let (s, os) = (&mut self.samples, &mut o.samples);
        s.sample.append(&mut os.sample);
        s.pid.append(&mut os.pid);
        s.time.append(&mut os.time);
        s.gc_id.append(&mut os.gc_id);
        s.event.append(&mut os.event);
        s.pages.append(&mut os.pages);
        s.view0_pages.append(&mut os.view0_pages);
        s.view1_pages.append(&mut os.view1_pages);
        s.both_pages.append(&mut os.both_pages);
        let (p, op) = (&mut self.pages, &mut o.pages);
        p.sample.append(&mut op.sample);
        p.page.append(&mut op.page);
        p.view0.append(&mut op.view0);
        p.view1.append(&mut op.view1);
        let (r, or) = (&mut self.ranges, &mut o.ranges);
        r.sample.append(&mut or.sample);
        r.start.append(&mut or.start);
        r.npages.append(&mut or.npages);
    }
}

impl BenchSuiteCollectPtViews {
    fn add_file(&mut self, pid: u32, content: &str) -> Result<()> {
        let mut lines = content.split_inclusive('\n');
        let header = lines.next().context("Empty file")?;
        let cols = Columns::new(header.trim_end_matches('\n'))?;
        let first = u32::try_from(self.tables.samples.sample.len()).context("Too many samples")?;
        let mut parsed = Tables::default();
        let mut sample = first;
        for line in lines {
            match parsed.add_line(&cols, pid, sample, line.trim_end_matches('\n')) {
                Ok(()) => sample += 1,
                // A JVM killed mid-write can leave a partial last line; keep
                // the complete samples before it.
                Err(_) if !line.ends_with('\n') => {}
                Err(e) => return Err(e.context(format!("Line {}", sample - first + 2))),
            }
        }
        // Only a file that parsed adds rows.
        self.tables.append(parsed);
        Ok(())
    }
}

impl BenchSuiteCollect for BenchSuiteCollectPtViews {
    fn process_file(&mut self, _: &BenchSuiteRun, file: &mut dyn FileInfoInterface) -> Result<()> {
        let Some(pid) = file_pid(file.name()) else {
            return Ok(());
        };
        let pid = pid?;
        if self.pids.contains(&pid) {
            bail!("Duplicate ptviews.{pid}.csv files");
        }
        self.pids.push(pid);
        let name = file.name().to_string();
        let content = file.content_string()?;
        self.add_file(pid, content)
            .with_context(|| format!("Failed to parse {name}"))
    }

    fn get_result(self: Box<Self>, _: &BenchSuiteRun) -> Result<Vec<(Intern, LazyFrame)>> {
        if self.pids.is_empty() {
            return Ok(Vec::new());
        }
        let Tables {
            samples: s,
            pages: p,
            ranges: r,
        } = self.tables;
        let samples = df![
            "pt_view_sample" => s.sample,
            "pid" => s.pid,
            "time" => s.time,
            "gc_id" => s.gc_id,
            "event" => s.event,
            "pages" => s.pages,
            "view0_pages" => s.view0_pages,
            "view1_pages" => s.view1_pages,
            "both_pages" => s.both_pages,
        ]
        .context("Failed to build pt_views_sample")?;
        let pages = df![
            "pt_view_sample" => p.sample,
            "page" => p.page,
            "view0" => p.view0,
            "view1" => p.view1,
        ]
        .context("Failed to build pt_views_page")?;
        let ranges = df![
            "pt_view_sample" => r.sample,
            "start" => r.start,
            "npages" => r.npages,
        ]
        .context("Failed to build pt_views_range")?;
        Ok(vec![
            (
                Intern::from_static("pt_views_sample"),
                samples.lazy().with_column(
                    col("time").cast(DataType::Datetime(TimeUnit::Nanoseconds, None)),
                ),
            ),
            (Intern::from_static("pt_views_page"), pages.lazy()),
            (Intern::from_static("pt_views_range"), ranges.lazy()),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER: &str = "time,gc,event,ranges,view0,view1\n";

    // 64 pages = 8 bytes. view0: page 0. view1: pages 0-7 and 8.
    const V0: &str = "AQAAAAAAAAA=";
    const V1: &str = "/wEAAAAAAAA=";

    fn collect(files: &[(u32, &str)]) -> Result<BenchSuiteCollectPtViews> {
        let mut c = BenchSuiteCollectPtViews::default();
        for (pid, content) in files {
            c.add_file(*pid, content)?;
        }
        Ok(c)
    }

    #[test]
    fn one_range() {
        let c = collect(&[(
            7,
            &format!("{HEADER}2026-11-03T09:38:01.729-0500,12,Young,0xfa000000+64,{V0},{V1}\n"),
        )])
        .unwrap();
        let s = &c.tables.samples;
        assert_eq!(s.pid, [7]);
        assert_eq!(s.gc_id, [Some(12)]);
        assert_eq!(s.event, ["Young"]);
        assert_eq!(
            (s.pages[0], s.view0_pages[0], s.view1_pages[0], s.both_pages[0]),
            (64, 1, 9, 1)
        );
        // 09:38:01.729 at -0500 is 14:38:01.729 UTC
        assert_eq!(
            s.time[0],
            DateTime::parse_from_rfc3339("2026-11-03T14:38:01.729Z")
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap()
        );
        let p = &c.tables.pages;
        let expect: Vec<u64> = (0..9).map(|i| 0xfa00_0000 + i * 4096).collect();
        assert_eq!(p.page, expect);
        assert_eq!(p.view0, [true, false, false, false, false, false, false, false, false]);
        assert!(p.view1.iter().all(|&b| b));
        assert_eq!(c.tables.ranges.start, [0xfa00_0000]);
    }

    #[test]
    fn multiple_ranges_and_files() {
        // 128 pages over two ranges = 16 bytes per view; the last page of
        // the second range touched by view0 only.
        let mut b = [0u8; 16];
        b[15] = 0x80;
        let v0 = BASE64.encode(b);
        let v1 = BASE64.encode([0u8; 16]);
        let line = |gc: &str, ev: &str| {
            format!("2026-11-03T09:38:01.729-0500,{gc},{ev},0x40000000+64;0x80000000+64,{v0},{v1}\n")
        };
        let a = format!("{HEADER}{}{}", line("3", "Minor"), line("", "Exit"));
        let b = format!("{HEADER}{}", line("1", "Major"));
        let c = collect(&[(7, &a), (8, &b)]).unwrap();
        let t = &c.tables;
        assert_eq!(t.samples.sample, [0, 1, 2]);
        assert_eq!(t.samples.pid, [7, 7, 8]);
        assert_eq!(t.samples.gc_id, [Some(3), None, Some(1)]);
        assert_eq!(t.ranges.sample, [0, 0, 1, 1, 2, 2]);
        assert_eq!(t.pages.sample, [0, 1, 2]);
        assert_eq!(t.pages.page, [0x8000_0000 + 63 * 4096; 3]);
        assert_eq!(t.pages.view0, [true; 3]);
        assert_eq!(t.pages.view1, [false; 3]);
    }

    #[test]
    fn columns_by_name() {
        let c = collect(&[(
            7,
            &format!(
                "event,view1,time,ranges,gc,view0,extra\n\
                 Full,{V1},2026-11-03T09:38:01.729-0500,0x1000+64,5,{V0},x\n"
            ),
        )])
        .unwrap();
        let s = &c.tables.samples;
        assert_eq!((s.view0_pages[0], s.view1_pages[0]), (1, 9));
    }

    #[test]
    fn partial_last_line_is_dropped() {
        let c = collect(&[(
            7,
            &format!(
                "{HEADER}2026-11-03T09:38:01.729-0500,1,Young,0x1000+64,{V0},{V1}\n\
                 2026-11-03T09:38:02.729-0500,2,Young,0x1000+64;0x2000+64,{V0}"
            ),
        )])
        .unwrap();
        assert_eq!(c.tables.samples.sample.len(), 1);
        assert_eq!(c.tables.ranges.sample.len(), 1);
    }

    #[test]
    fn bad_file_adds_nothing() {
        let mut c = BenchSuiteCollectPtViews::default();
        let err = c
            .add_file(
                7,
                &format!(
                    "{HEADER}2026-11-03T09:38:01.729-0500,1,Young,0x1000+64,{V0},{V1}\n\
                     2026-11-03T09:38:02.729-0500,2,Young,0x1000+128,{V0},{V1}\n"
                ),
            )
            .unwrap_err();
        assert!(format!("{err:#}").contains("8 bytes for 128 pages"), "{err:#}");
        assert!(format!("{err:#}").contains("Line 3"), "{err:#}");
        let t = &c.tables;
        assert!(t.samples.sample.is_empty() && t.pages.sample.is_empty() && t.ranges.sample.is_empty());
    }

    #[test]
    fn file_names() {
        assert_eq!(file_pid("ptviews.393985.csv").unwrap().unwrap(), 393_985);
        assert!(file_pid("ptviews.393985.javalog").is_none());
        assert!(file_pid("ptviews.x.csv").unwrap().is_err());
    }
}
