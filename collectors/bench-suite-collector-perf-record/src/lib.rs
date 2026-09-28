use anyhow::{Context, anyhow};
use bench_suite_collect_results::{BenchSuiteCollect, FileInfoInterface};
use polars::prelude::*;
use regex::Regex;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::LazyLock;
use string_intern::Intern;

/// Matches one sample line exactly as `PerfRecord._script_dump` renders it
/// (see `utils/setup.py`), regardless of the `-F` field order that was
/// requested - `perf script` lays every sample out in one fixed order of its
/// own, `-F` only selects which of these pieces are present at all:
///
/// ```text
/// <comm> <pid>/<tid> [<cpu>] <time>: <event>: [<addr> <data_src>] <ip> <sym>+0x<off>|[unknown] (<dso>)
///   <srcline>                                                                         <- own line, optional
/// ```
///
/// Every column is padded to the widest value *anywhere in the file* for
/// human readability, so leading/inter-field whitespace carries no meaning
/// and can never be used to find a field boundary - hence matching on shape
/// (an integer here, a bracketed number there, a trailing hex address) and
/// never on a fixed width or exact spacing. `comm` and a resolved symbol can
/// both legitimately contain spaces (e.g. "GC Thread#0", or a template
/// instantiation with comma-separated arguments), which is exactly why this
/// anchors the *dso* group at the end of the line and works inward from
/// there rather than splitting on whitespace naively.
///
/// `addr`/`data_src` appear only when the run's `PerfRecord` had `data=True`
/// (`-d`), and only as this exact pair. This does not yet handle
/// `phys_data`/branch-stack fields (`--phys-data`/`branch`), which land in
/// different positions in the line - a run using those needs this regex
/// extended first, and will fail loudly (a `captures()` miss becomes a parse
/// error, not silently wrong data) rather than mis-parse.
///
/// A line that fails to match this pattern at all is a `srcline` line
/// continuing the *previous* sample, never a sample of its own - `srcline`
/// has no field capture here because it is never on the sample's own line.
static SAMPLE_LINE_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?x)
        ^\s*
        (?P<comm>.*?)\s*
        (?P<pid>-?\d+)/(?P<tid>-?\d+)\s+
        \[(?P<cpu>\d+)\]\s+
        (?P<time>\d+\.\d+):\s+
        (?P<event>.+):\s+
        (?:(?P<addr>[0-9a-f]+)\s+(?P<data_src>.*?)\s+)?
        (?P<ip>[0-9a-f]+)\s+
        (?P<symrest>.*?)\s+
        \((?P<dso>[^()]*)\)\s*$
        ",
    )
    .expect("SAMPLE_LINE_REGEX is a fixed pattern")
});

/// Splits a resolved `<sym>+0x<offset>` into its parts; `[unknown]` (or
/// anything else without a `+0x` suffix - perf never omits it for a symbol
/// it actually resolved) has no match, meaning "unresolved".
static SYMOFF_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(?P<sym>.*)\+0x(?P<off>[0-9a-f]+)$").expect("SYMOFF_REGEX is a fixed pattern")
});

/// Written once by `PerfRecord.__enter__` (`utils/setup.py`) as
/// `time.time()` and `time.monotonic()` read back to back. Every sample's
/// `time` column is on the same clock as `time.monotonic()` (both are
/// `CLOCK_MONOTONIC` - the only clock hardware/PEBS events can use at all,
/// see `PerfRecord`'s docstring for why), so wall clock for a sample is
/// `realtime_epoch + (sample_time - monotonic_seconds)`.
#[derive(Deserialize)]
struct ClockOffset {
    realtime_epoch: f64,
    monotonic_seconds: f64,
}

/// One entry per distinct instruction address seen in this run.  A resolved
/// symbol always carries `symbol_offset` (0 included - perf never omits it),
/// so `symbol.is_none()` is exactly the `[unknown]` case.
struct SymbolInfo {
    symbol: Option<String>,
    symbol_offset: Option<u64>,
    dso: String,
    srcline: Option<String>,
}

#[derive(Default)]
struct ParsedSamples {
    comm: Vec<String>,
    pid: Vec<u64>,
    tid: Vec<u64>,
    cpu: Vec<u32>,
    time: Vec<f64>,
    event: Vec<String>,
    ip: Vec<u64>,
    // The `-d` fields, present only when the run's `PerfRecord` had
    // `data=True`. Null throughout otherwise, rather than absent columns, so
    // that runs with and without them concat into one table. `data_src` is
    // kept exactly as perf renders it, which is its raw integer value
    // followed by the decoded form -- e.g.
    // `5080022 |OP LOAD|LVL L3 hit|SNP None|TLB L1 or L2 hit|LCK No`. Both
    // halves are preserved because that string packs several independent
    // fields (op, level, snoop, TLB, lock) and which of them a consumer wants
    // is not knowable here; the decoded part starts at the first space.
    addr: Vec<Option<u64>>,
    data_src: Vec<Option<String>>,
    // ip -> its (stable, deterministic within one run) resolution. Kept
    // separate from the sample rows above so the same libjvm.so function
    // hit by thousands of samples is stored once, not once per sample.
    symbols: HashMap<u64, SymbolInfo>,
}

fn parse_hex_u64(text: &str, what: &str) -> anyhow::Result<u64> {
    u64::from_str_radix(text, 16).with_context(|| format!("Failed to parse {what} {text:?} as hex"))
}

/// Parses a whole `perf_record_symbols.txt`. Fails the file - not just the
/// one line - on the first line that is neither a sample nor a continuation
/// of the sample right before it, since that means this parser's
/// understanding of perf's layout (see `SAMPLE_LINE_REGEX`) no longer
/// matches what actually got written, and everything downstream of that
/// point cannot be trusted either.
fn parse_symbols_txt(content: &str) -> anyhow::Result<ParsedSamples> {
    let mut out = ParsedSamples::default();

    let mut lines = content.lines().peekable();
    while let Some(line) = lines.next() {
        let Some(caps) = SAMPLE_LINE_REGEX.captures(line) else {
            return Err(anyhow!(
                "perf_record_symbols.txt: line is neither a sample nor a continuation \
                 of the previous one (perf's output layout may have changed): {line:?}"
            ));
        };

        let ip = parse_hex_u64(&caps["ip"], "ip")?;

        out.comm.push(caps["comm"].to_string());
        out.pid.push(caps["pid"].parse().context("Failed to parse pid")?);
        out.tid.push(caps["tid"].parse().context("Failed to parse tid")?);
        out.cpu.push(caps["cpu"].parse().context("Failed to parse cpu")?);
        out.time.push(caps["time"].parse().context("Failed to parse time")?);
        out.event.push(caps["event"].to_string());
        out.ip.push(ip);
        // Both come from one optional group, so they are either both there or
        // both absent -- but they are read independently anyway, so a future
        // perf that emits only one does not silently shift the other.
        out.addr.push(
            caps.name("addr")
                .map(|m| parse_hex_u64(m.as_str(), "data address"))
                .transpose()?,
        );
        out.data_src
            .push(caps.name("data_src").map(|m| m.as_str().to_string()));

        // The srcline continuation, if there is one, has to be consumed here
        // regardless of whether this ip's resolution is cached below already
        // - otherwise it would be mistaken for an orphan continuation or for
        // the next sample when this loop comes back around.
        let srcline = lines
            .peek()
            .filter(|next| !SAMPLE_LINE_REGEX.is_match(next))
            .map(|s| s.trim().to_string());
        if srcline.is_some() {
            let _ = lines.next();
        }

        if let std::collections::hash_map::Entry::Vacant(entry) = out.symbols.entry(ip) {
            let symrest = &caps["symrest"];
            let (symbol, symbol_offset) = match SYMOFF_REGEX.captures(symrest) {
                Some(c) => (
                    Some(c["sym"].to_string()),
                    Some(parse_hex_u64(&c["off"], "symbol offset")?),
                ),
                None => (None, None),
            };
            entry.insert(SymbolInfo {
                symbol,
                symbol_offset,
                dso: caps["dso"].to_string(),
                srcline,
            });
        }
    }

    Ok(out)
}

/// Builds the `perf_record_samples` table, converting `time` (an arbitrary
/// per-boot monotonic clock, useless on its own for lining a sample up
/// against another table's wall-clock intervals like `zgc_phases`) to a real
/// `wall_time` wherever an offset was captured. A run with no offset file
/// (`clockid` explicitly disabled on `PerfRecord`) still gets every other
/// column; `wall_time` is just null throughout.
fn samples_lazyframe(samples: &ParsedSamples, offset: Option<&ClockOffset>) -> anyhow::Result<LazyFrame> {
    let samples_df = df![
        "comm" => &samples.comm,
        "pid" => &samples.pid,
        "tid" => &samples.tid,
        "cpu" => &samples.cpu,
        "time" => &samples.time,
        "event" => &samples.event,
        "ip" => &samples.ip,
        "addr" => &samples.addr,
        "data_src" => &samples.data_src,
    ]
    .context("Failed to create perf_record_samples DataFrame")?;

    let wall_time_expr = match offset {
        Some(o) => ((col("time") - lit(o.monotonic_seconds) + lit(o.realtime_epoch)) * lit(1_000_000.0))
            .cast(DataType::Int64)
            .cast(DataType::Datetime(TimeUnit::Microseconds, None)),
        None => lit(NULL).cast(DataType::Datetime(TimeUnit::Microseconds, None)),
    };
    Ok(samples_df.lazy().with_column(wall_time_expr.alias("wall_time")))
}

#[derive(Default)]
pub struct BenchSuiteCollectPerfRecord {
    samples: Option<ParsedSamples>,
    offset: Option<ClockOffset>,
}

impl BenchSuiteCollectPerfRecord {
    #[must_use]
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

impl BenchSuiteCollect for BenchSuiteCollectPerfRecord {
    fn process_file(
        &mut self,
        _: &bench_suite_types::BenchSuiteRun,
        file: &mut dyn FileInfoInterface,
    ) -> anyhow::Result<()> {
        match file.name() {
            "perf_record_symbols.txt" => {
                if self.samples.is_some() {
                    return Err(anyhow!("Duplicate perf_record_symbols.txt files"));
                }
                self.samples = Some(parse_symbols_txt(file.content_string()?)?);
            }
            "perf_record_clock_offset.json" => {
                if self.offset.is_some() {
                    return Err(anyhow!("Duplicate perf_record_clock_offset.json files"));
                }
                self.offset = Some(
                    serde_json::from_str(file.content_string()?)
                        .context("Failed to parse perf_record_clock_offset.json")?,
                );
            }
            _ => {}
        }
        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, LazyFrame)>> {
        let mut rv = Vec::new();
        let BenchSuiteCollectPerfRecord { samples, offset } = *self;
        let Some(samples) = samples else {
            return Ok(rv);
        };

        rv.push((
            Intern::from_static("perf_record_samples"),
            samples_lazyframe(&samples, offset.as_ref())?,
        ));

        let ParsedSamples { symbols, .. } = samples;
        if !symbols.is_empty() {
            let mut ips = Vec::with_capacity(symbols.len());
            let mut syms: Vec<Option<String>> = Vec::with_capacity(symbols.len());
            let mut offs: Vec<Option<u64>> = Vec::with_capacity(symbols.len());
            let mut dsos = Vec::with_capacity(symbols.len());
            let mut srclines: Vec<Option<String>> = Vec::with_capacity(symbols.len());
            for (ip, info) in symbols {
                ips.push(ip);
                syms.push(info.symbol);
                offs.push(info.symbol_offset);
                dsos.push(info.dso);
                srclines.push(info.srcline);
            }
            let symbols_df = df![
                "ip" => ips,
                "symbol" => syms,
                "symbol_offset" => offs,
                "dso" => dsos,
                "srcline" => srclines,
            ]
            .context("Failed to create perf_record_symbols DataFrame")?;
            rv.push((Intern::from_static("perf_record_symbols"), symbols_df.lazy()));
        }

        Ok(rv)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn samples(content: &str) -> ParsedSamples {
        parse_symbols_txt(content).unwrap()
    }

    #[test]
    fn parses_a_plain_resolved_sample_with_srcline() {
        let df = samples(
            "         swapper     0/0     [015] 5521401.553801863: cycles:ppp:  \
             ffffffff8d3b0a9d poll_idle+0x8d (/usr/lib/debug/boot/vmlinux-6.1.27)\n\
             \x20\x20bitops.h:207\n",
        );
        assert_eq!(df.comm, vec!["swapper"]);
        assert_eq!(df.pid, vec![0]);
        assert_eq!(df.tid, vec![0]);
        assert_eq!(df.cpu, vec![15]);
        assert_eq!(df.event, vec!["cycles:ppp"]);
        assert_eq!(df.ip, vec![0xffff_ffff_8d3b_0a9d]);
        let info = &df.symbols[&0xffff_ffff_8d3b_0a9d];
        assert_eq!(info.symbol.as_deref(), Some("poll_idle"));
        assert_eq!(info.symbol_offset, Some(0x8d));
        assert_eq!(info.dso, "/usr/lib/debug/boot/vmlinux-6.1.27");
        assert_eq!(info.srcline.as_deref(), Some("bitops.h:207"));
    }

    #[test]
    fn unresolved_symbol_has_no_offset_and_no_srcline() {
        let df = samples(
            "2983417/2983417 [002] 5520679.812756885: cycles:ppp:  \
             7ffff7fede90 [unknown] (/usr/lib/x86_64-linux-gnu/ld-2.31.so)\n",
        );
        let info = &df.symbols[&0x7fff_f7fe_de90];
        assert_eq!(info.symbol, None);
        assert_eq!(info.symbol_offset, None);
        assert_eq!(info.srcline, None);
    }

    #[test]
    fn addr_and_data_src_land_between_event_and_ip() {
        let df = samples(
            "         swapper     0/0     [012] 5521569.016425435: \
             cpu/event=0xd0,umask=0x81,period=2000003/ppp: ffff9a1dc1512000         \
             5080022 |OP LOAD|LVL N/A|SNP N/A|TLB N/A|LCK N/A|BLK  N/A \
             ffffffff8d3b0a9d poll_idle+0x8d (/usr/lib/debug/boot/vmlinux-6.1.27)\n",
        );
        assert_eq!(df.event, vec!["cpu/event=0xd0,umask=0x81,period=2000003/ppp"]);
        assert_eq!(df.ip, vec![0xffff_ffff_8d3b_0a9d]);
        // The point of -d: the address the access TOUCHED, which is what
        // separates (say) a ZGC heap object header from a CHeapBitMap livemap
        // word without depending on symbols -- the hot livemap helpers are
        // .inline.hpp and inline away.
        assert_eq!(df.addr, vec![Some(0xffff_9a1d_c151_2000)]);
        // perf prints data_src as its raw value AND its decoded form; both are
        // kept, so this is the whole token run between addr and ip.
        assert_eq!(
            df.data_src.first().unwrap().as_deref(),
            Some("5080022 |OP LOAD|LVL N/A|SNP N/A|TLB N/A|LCK N/A|BLK  N/A")
        );
    }

    #[test]
    fn a_run_without_data_sampling_leaves_addr_and_data_src_null() {
        let df = samples(
            "         swapper     0/0     [015] 5521401.553801863: cycles:ppp:  \
             ffffffff8d3b0a9d poll_idle+0x8d (/usr/lib/debug/boot/vmlinux-6.1.27)\n",
        );
        assert_eq!(df.addr, vec![None]);
        assert_eq!(df.data_src, vec![None]);
    }

    #[test]
    fn a_comm_with_spaces_does_not_break_the_pid_tid_split() {
        // G1's worker threads are named like this (ZGC's never are, but the
        // parser has to survive any comm, not just ZGC's).
        let df = samples(
            "  GC Thread#0 2983932/2983951 [004] 5521401.554133885: cycles:ppp:  \
             7ffff76efafd some_symbol+0x10 (/lib/libjvm.so)\n",
        );
        assert_eq!(df.comm, vec!["GC Thread#0"]);
        assert_eq!(df.pid, vec![2_983_932]);
        assert_eq!(df.tid, vec![2_983_951]);
    }

    #[test]
    fn a_templated_symbol_with_commas_and_parens_is_not_split_early() {
        let df = samples(
            "2983417/2983419 [004] 5520679.817622609: cycles:ppp:  \
             7ffff76efafd QuickSort::sort<Method*, int (*)(Method*, Method*)>+0x535 \
             (/lib/libjvm.so)\n",
        );
        let info = &df.symbols[&0x7fff_f76e_fafd];
        assert_eq!(
            info.symbol.as_deref(),
            Some("QuickSort::sort<Method*, int (*)(Method*, Method*)>")
        );
        assert_eq!(info.symbol_offset, Some(0x535));
        assert_eq!(info.dso, "/lib/libjvm.so");
    }

    #[test]
    fn the_same_ip_across_many_samples_is_stored_once() {
        let df = samples(
            "0/0 [000] 1.0: cycles:ppp:  aa foo+0x1 (/lib/x.so)\n\
             0/0 [001] 2.0: cycles:ppp:  aa foo+0x1 (/lib/x.so)\n\
             0/0 [002] 3.0: cycles:ppp:  aa foo+0x1 (/lib/x.so)\n",
        );
        assert_eq!(df.ip.len(), 3);
        assert_eq!(df.symbols.len(), 1);
    }

    #[test]
    fn an_orphan_continuation_line_fails_the_whole_file() {
        assert!(parse_symbols_txt("  bitops.h:207\n").is_err());
    }

    #[test]
    fn wall_time_is_null_without_an_offset_file() {
        let samples = samples("0/0 [000] 1.0: cycles:ppp:  aa foo+0x1 (/lib/x.so)\n");
        let df = samples_lazyframe(&samples, None).unwrap().collect().unwrap();
        assert_eq!(df.column("wall_time").unwrap().null_count(), 1);
    }

    #[test]
    fn wall_time_converts_monotonic_to_the_captured_wall_clock() {
        let samples = samples("0/0 [000] 100.0: cycles:ppp:  aa foo+0x1 (/lib/x.so)\n");
        let offset = Some(ClockOffset {
            realtime_epoch: 1_700_000_000.0,
            monotonic_seconds: 90.0,
        });
        let df = samples_lazyframe(&samples, offset.as_ref()).unwrap().collect().unwrap();
        // sample was 10s after the offset capture on the monotonic clock, so
        // wall clock should be 10s after realtime_epoch.
        let wall = df.column("wall_time").unwrap().datetime().unwrap().phys.get(0).unwrap();
        assert_eq!(wall, 1_700_000_010_000_000);
    }
}
