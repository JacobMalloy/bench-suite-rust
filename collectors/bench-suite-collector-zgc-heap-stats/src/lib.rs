use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use regex::Regex;
use std::sync::LazyLock;
use string_intern::Intern;

// Matches a [gc,heap] log line and splits it into timestamp, GC number, age
// (Y/O for major, y/o for minor) and the remainder of the line, e.g.:
//   [ts][info][gc,heap     ] GC(0) y: Min Capacity: 2M(0%)
//   [ts][info][gc,heap     ] GC(0) y: Heap Statistics:
//   [ts][info][gc,heap     ] GC(0) y:  Capacity:     2048M (6%)         2048M (6%) ...
static ZGC_HEAP_LINE_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\[([^\]]*)\]\[info\s*\]\[gc,heap\s*\] GC\((\d+)\) ([A-Za-z]):\s*(.*)").unwrap()
});

// Matches the three standalone capacity lines, e.g. `Min Capacity: 2M(0%)`.
static CAPACITY_SUMMARY_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(Min Capacity|Max Capacity|Soft Max Capacity): (\d+)M\(\d+%\)$").unwrap()
});

// Matches a table data row's label and the (unparsed) values that follow it, e.g.
// ` Capacity:     2048M (6%)         2048M (6%) ...` or `      Live:         -   0M (0%) ...`.
static TABLE_ROW_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^([A-Za-z][A-Za-z ]*?):\s+(.+)$").unwrap());

// Matches a single value cell in a table row: either `NNNM (NN%)` or a bare `-`.
static VALUE_CELL_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(\d+)M\s*\(\d+%\)|-").unwrap());

const HEAP_STATISTICS_PHASES: [&str; 6] = [
    "mark_start",
    "mark_end",
    "relocate_start",
    "relocate_end",
    "high",
    "low",
];

const GENERATION_STATISTICS_PHASES: [&str; 4] =
    ["mark_start", "mark_end", "relocate_start", "relocate_end"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Table {
    HeapStatistics,
    GenerationStatistics,
}

impl Table {
    fn name(self) -> &'static str {
        match self {
            Table::HeapStatistics => "heap_statistics",
            Table::GenerationStatistics => "generation_statistics",
        }
    }

    fn phases(self) -> &'static [&'static str] {
        match self {
            Table::HeapStatistics => &HEAP_STATISTICS_PHASES,
            Table::GenerationStatistics => &GENERATION_STATISTICS_PHASES,
        }
    }
}

struct RowContext<'a> {
    clock_time: &'a str,
    gc_number: u32,
    age: char,
}

#[derive(Default)]
struct Rows {
    clock_times: Vec<String>,
    gc_numbers: Vec<u32>,
    ages: Vec<String>,
    types: Vec<String>,
    tables: Vec<String>,
    metrics: Vec<String>,
    phases: Vec<Option<String>>,
    values_mb: Vec<Option<u64>>,
}

impl Rows {
    fn push(
        &mut self,
        ctx: &RowContext,
        table: &str,
        metric: &str,
        phase: Option<&str>,
        value_mb: Option<u64>,
    ) {
        let type_str = if ctx.age.is_uppercase() {
            "major"
        } else {
            "minor"
        };

        self.clock_times.push(ctx.clock_time.to_string());
        self.gc_numbers.push(ctx.gc_number);
        self.ages.push(ctx.age.to_lowercase().to_string());
        self.types.push(type_str.to_string());
        self.tables.push(table.to_string());
        self.metrics.push(metric.to_string());
        self.phases.push(phase.map(str::to_string));
        self.values_mb.push(value_mb);
    }
}

#[derive(Debug, Default)]
pub struct BenchSuiteCollectZgcHeapStats {
    heap_stats_df: Option<DataFrame>,
}

impl BenchSuiteCollectZgcHeapStats {
    #[must_use]
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

fn parse_heap_log(content: &str) -> anyhow::Result<Rows> {
    let mut rows = Rows::default();
    let mut current_table: Option<Table> = None;

    for line in content.lines() {
        let Some(cap) = ZGC_HEAP_LINE_REGEX.captures(line) else {
            continue;
        };
        let clock_time = cap.get(1).context("Missing clock time")?.as_str();
        let gc_number: u32 = cap
            .get(2)
            .context("Missing GC number")?
            .as_str()
            .parse()
            .context("Failed to parse GC number")?;
        let age = cap
            .get(3)
            .context("Missing age")?
            .as_str()
            .chars()
            .next()
            .context("ZGC age does not have a single char")?;
        let rest = cap.get(4).context("Missing line remainder")?.as_str();
        let ctx = RowContext {
            clock_time,
            gc_number,
            age,
        };

        if let Some(cap) = CAPACITY_SUMMARY_REGEX.captures(rest) {
            let metric = cap.get(1).context("Missing capacity metric")?.as_str();
            let value_mb: u64 = cap
                .get(2)
                .context("Missing capacity value")?
                .as_str()
                .parse()
                .context("Failed to parse capacity value")?;
            rows.push(&ctx, "capacity_summary", metric, None, Some(value_mb));
            continue;
        }

        if rest == "Heap Statistics:" {
            current_table = Some(Table::HeapStatistics);
            continue;
        }
        if rest == "Young Generation Statistics:" || rest == "Old Generation Statistics:" {
            current_table = Some(Table::GenerationStatistics);
            continue;
        }
        // The column header row (e.g. "Mark Start  Mark End  ...") carries no values.
        if rest.starts_with("Mark Start") {
            continue;
        }

        let Some(table) = current_table else {
            continue;
        };
        let Some(cap) = TABLE_ROW_REGEX.captures(rest) else {
            continue;
        };
        let metric = cap.get(1).context("Missing row metric")?.as_str().trim();
        let values_str = cap.get(2).context("Missing row values")?.as_str();

        for (phase, value_cap) in table
            .phases()
            .iter()
            .zip(VALUE_CELL_REGEX.captures_iter(values_str))
        {
            let value_mb = value_cap.get(1).map(|m| m.as_str().parse()).transpose()?;
            rows.push(&ctx, table.name(), metric, Some(phase), value_mb);
        }
    }

    Ok(rows)
}

impl BenchSuiteCollect for BenchSuiteCollectZgcHeapStats {
    fn process_file(
        &mut self,
        run: &bench_suite_types::BenchSuiteRun,
        file: &mut dyn bench_suite_collect_results::FileInfoInterface,
    ) -> anyhow::Result<()> {
        match &run.gc {
            Some(gc) if gc.as_str().to_lowercase().contains("zgc") => {}
            _ => return Ok(()),
        }

        let name = file.name();
        if name != "gc.javalog"
            && name != "jvm0.txt" // LEGACY: remove once all tests use split files
        {
            return Ok(());
        }

        if self.heap_stats_df.is_some() {
            return Err(anyhow::anyhow!("Duplicate gc log files"));
        }

        let content = file.content_string()?;
        let rows = parse_heap_log(content)?;

        let df = df![
            "clock_time" => rows.clock_times,
            "gc_number" => rows.gc_numbers,
            "age" => rows.ages,
            "type" => rows.types,
            "table" => rows.tables,
            "metric" => rows.metrics,
            "phase" => rows.phases,
            "value_mb" => rows.values_mb,
        ]
        .context("Failed to create zgc_heap_stats DataFrame")?;

        self.heap_stats_df = Some(df);

        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, LazyFrame)>> {
        let mut rv = Vec::new();
        if let Some(df) = self.heap_stats_df {
            let lf = df.lazy().with_column(col("clock_time").str().to_datetime(
                Some(TimeUnit::Milliseconds),
                None,
                StrptimeOptions {
                    format: Some("%Y-%m-%dT%H:%M:%S%.3f%z".into()),
                    strict: false,
                    exact: true,
                    cache: true,
                },
                lit("raise"),
            ));
            rv.push((Intern::from_static("zgc_heap_stats"), lf));
        }
        Ok(rv)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "\
[2026-08-01T22:10:12.161-0400][info][gc,heap     ] GC(0) y: Min Capacity: 2M(0%)
[2026-08-01T22:10:12.161-0400][info][gc,heap     ] GC(0) y: Max Capacity: 32768M(100%)
[2026-08-01T22:10:12.161-0400][info][gc,heap     ] GC(0) y: Soft Max Capacity: 32768M(100%)
[2026-08-01T22:10:12.161-0400][info][gc,heap     ] GC(0) y: Heap Statistics:
[2026-08-01T22:10:12.161-0400][info][gc,heap     ] GC(0) y:                Mark Start          Mark End        Relocate Start      Relocate End           High               Low
[2026-08-01T22:10:12.161-0400][info][gc,heap     ] GC(0) y:  Capacity:     2048M (6%)         2048M (6%)         2048M (6%)         2048M (6%)         2048M (6%)         2048M (6%)
[2026-08-01T22:10:12.161-0400][info][gc,heap     ] GC(0) y:      Free:    32764M (100%)      32762M (100%)      32762M (100%)      32764M (100%)      32764M (100%)      32760M (100%)
[2026-08-01T22:10:12.161-0400][info][gc,heap     ] GC(0) y:      Used:        4M (0%)            6M (0%)            6M (0%)            4M (0%)            8M (0%)            4M (0%)
[2026-08-01T22:10:12.161-0400][info][gc,heap     ] GC(0) y: Young Generation Statistics:
[2026-08-01T22:10:12.161-0400][info][gc,heap     ] GC(0) y:                Mark Start          Mark End        Relocate Start      Relocate End
[2026-08-01T22:10:12.161-0400][info][gc,heap     ] GC(0) y:      Used:        4M (0%)            6M (0%)            6M (0%)            4M (0%)
[2026-08-01T22:10:12.161-0400][info][gc,heap     ] GC(0) y:      Live:         -                 0M (0%)            0M (0%)            0M (0%)
[2026-08-01T22:10:12.161-0400][info][gc,heap     ] GC(0) y:   Garbage:         -                 3M (0%)            3M (0%)            0M (0%)
[2026-08-01T22:10:12.161-0400][info][gc,heap     ] GC(0) y: Allocated:         -                 2M (0%)            2M (0%)            3M (0%)
[2026-08-01T22:10:12.161-0400][info][gc,heap     ] GC(0) y: Reclaimed:         -                  -                 0M (0%)            3M (0%)
[2026-08-01T22:10:12.161-0400][info][gc,heap     ] GC(0) y:  Promoted:         -                  -                 0M (0%)            0M (0%)
[2026-08-01T22:10:12.161-0400][info][gc,heap     ] GC(0) y: Compacted:         -                  -                  -                 0M (0%)
";

    #[test]
    fn parses_capacity_summary_lines() {
        let rows = parse_heap_log(SAMPLE).unwrap();
        let idx = rows
            .metrics
            .iter()
            .position(|m| m == "Min Capacity")
            .unwrap();
        assert_eq!(rows.tables[idx], "capacity_summary");
        assert_eq!(rows.phases[idx], None);
        assert_eq!(rows.values_mb[idx], Some(2));
        assert_eq!(rows.types[idx], "minor");
        assert_eq!(rows.ages[idx], "y");
    }

    #[test]
    fn parses_heap_statistics_table_with_six_phases() {
        let rows = parse_heap_log(SAMPLE).unwrap();
        let capacity_rows: Vec<usize> = rows
            .metrics
            .iter()
            .enumerate()
            .filter(|(i, m)| *m == "Capacity" && rows.tables[*i] == "heap_statistics")
            .map(|(i, _)| i)
            .collect();
        assert_eq!(capacity_rows.len(), 6);
        assert_eq!(
            rows.phases[capacity_rows[0]].as_deref(),
            Some("mark_start")
        );
        assert_eq!(rows.phases[capacity_rows[5]].as_deref(), Some("low"));
        assert!(capacity_rows.iter().all(|&i| rows.values_mb[i] == Some(2048)));
    }

    #[test]
    fn parses_generation_statistics_table_with_dashes() {
        let rows = parse_heap_log(SAMPLE).unwrap();
        let live_rows: Vec<usize> = rows
            .metrics
            .iter()
            .enumerate()
            .filter(|(i, m)| *m == "Live" && rows.tables[*i] == "generation_statistics")
            .map(|(i, _)| i)
            .collect();
        assert_eq!(live_rows.len(), 4);
        assert_eq!(rows.values_mb[live_rows[0]], None);
        assert_eq!(rows.values_mb[live_rows[1]], Some(0));
    }

    #[test]
    fn derives_major_minor_from_age_case() {
        let major_sample = SAMPLE.replace("GC(0) y", "GC(0) Y");
        let rows = parse_heap_log(&major_sample).unwrap();
        assert!(rows.types.iter().all(|t| t == "major"));
        assert!(rows.ages.iter().all(|a| a == "y"));
    }
}
