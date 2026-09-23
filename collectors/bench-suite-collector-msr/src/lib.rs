use anyhow::Context;
use bench_suite_collect_results::BenchSuiteCollect;
use polars::prelude::*;
use std::sync::Arc;
use string_intern::Intern;

/// Collects the per-run MSR dumps written by `utils/msr.py`'s `MsrDump`: every
/// register in its `MSR_DUMP` list, read on every online cpu as the benchmark
/// starts (`msr_before.csv`) and again as it ends (`msr_after.csv`).
///
/// Both files have the same `address,cpu,value` shape, so they become one
/// `msr` table with a `phase` column saying which dump a row came from - that
/// way "did anything move during the run" is a self-join on (address, cpu)
/// rather than a join across two tables.
const BEFORE_FILE: &str = "msr_before.csv";
const AFTER_FILE: &str = "msr_after.csv";

/// The dump's stand-in for a register that could not be read on a cpu - it
/// does not exist on this model/microcode, or the msr driver is not loaded.
/// Kept as a null rather than dropped, so a register that reads on only some
/// cpus still shows which ones.
const UNREADABLE: &str = "None";

fn parse_hex(text: &str) -> anyhow::Result<u64> {
    let digits = text
        .strip_prefix("0x")
        .with_context(|| format!("MSR value {text:?} is not an 0x-prefixed hex literal"))?;
    u64::from_str_radix(digits, 16).with_context(|| format!("Failed to parse MSR value {text:?}"))
}

/// The schema is pinned rather than inferred: a dump where every read failed
/// is all `None` values, which would infer as something other than the column
/// a dump that read fine produces, and then refuse to concat with it.
///
/// `address` stays the hex string it was dumped as, since it is an identifier
/// to filter and group on. `value` becomes a `UInt64` - the bit fields are the
/// point of the dump, and an `Int64` would overflow on the registers that use
/// bit 63 as a lock bit (`MSR_PKG_POWER_LIMIT`, say).
fn parse(bytes: &[u8], phase: &'static str) -> anyhow::Result<LazyFrame> {
    let schema = Schema::from_iter([
        (PlSmallStr::from_static("address"), DataType::String),
        (PlSmallStr::from_static("cpu"), DataType::UInt32),
        (PlSmallStr::from_static("value"), DataType::String),
    ]);

    let cursor = std::io::Cursor::new(bytes);
    let mut df = CsvReadOptions::default()
        .with_has_header(true)
        .with_schema(Some(Arc::new(schema)))
        .into_reader_with_file_handle(cursor)
        .finish()
        .with_context(|| format!("Failed to parse msr_{phase}.csv"))?;

    let mut values: Vec<Option<u64>> = Vec::with_capacity(df.height());
    for value in df.column("value")?.str()? {
        values.push(match value {
            None | Some(UNREADABLE) => None,
            Some(text) => Some(parse_hex(text)?),
        });
    }

    let _ = df.with_column(
        UInt64Chunked::from_iter_options(PlSmallStr::from_static("value"), values.into_iter())
            .into_column(),
    )?;

    Ok(df.lazy().with_column(lit(phase).alias("phase")))
}

/// Stacked in dump order rather than tar order, so the table reads the way the
/// run happened. A run cut short by Ctrl-C has no `msr_after.csv`, and its
/// before rows are still worth having, so a missing half is not an error.
fn combine(before: Option<LazyFrame>, after: Option<LazyFrame>) -> PolarsResult<Option<LazyFrame>> {
    Ok(match (before, after) {
        (Some(before), Some(after)) => Some(concat([before, after], UnionArgs::default())?),
        (Some(only), None) | (None, Some(only)) => Some(only),
        (None, None) => None,
    })
}

#[derive(Default)]
pub struct BenchSuiteCollectMsr {
    before: Option<LazyFrame>,
    after: Option<LazyFrame>,
}

impl BenchSuiteCollectMsr {
    #[must_use]
    pub fn boxed() -> Box<dyn BenchSuiteCollect> {
        Box::new(Self::default())
    }
}

impl BenchSuiteCollect for BenchSuiteCollectMsr {
    fn process_file(
        &mut self,
        _: &bench_suite_types::BenchSuiteRun,
        file: &mut dyn bench_suite_collect_results::FileInfoInterface,
    ) -> anyhow::Result<()> {
        let (slot, phase) = match file.name() {
            BEFORE_FILE => (&mut self.before, "before"),
            AFTER_FILE => (&mut self.after, "after"),
            _ => return Ok(()),
        };

        if slot.is_some() {
            return Err(anyhow::anyhow!("Duplicate msr_{phase}.csv files"));
        }

        *slot = Some(parse(file.content_bytes()?, phase)?);

        Ok(())
    }

    fn get_result(
        self: Box<Self>,
        _: &bench_suite_types::BenchSuiteRun,
    ) -> anyhow::Result<Vec<(Intern, LazyFrame)>> {
        let BenchSuiteCollectMsr { before, after } = *self;
        Ok(combine(before, after)?
            .map(|lf| (Intern::from_static("msr"), lf))
            .into_iter()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER: &str = "address,cpu,value\n";

    fn collect(content: &str, phase: &'static str) -> DataFrame {
        parse(content.as_bytes(), phase).unwrap().collect().unwrap()
    }

    fn values(df: &DataFrame) -> Vec<Option<u64>> {
        df.column("value").unwrap().u64().unwrap().to_vec()
    }

    fn phases(df: &DataFrame) -> Vec<Option<&str>> {
        df.column("phase")
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .collect()
    }

    #[test]
    fn reads_a_dump_and_tags_its_phase() {
        let df = collect(
            &format!("{HEADER}0x1a4,0,0x000000000000000f\n0x1a4,1,0x000000000000000f\n"),
            "before",
        );
        assert_eq!(df.height(), 2);
        assert_eq!(values(&df), vec![Some(0xf), Some(0xf)]);
        assert_eq!(
            df.column("cpu").unwrap().u32().unwrap().to_vec(),
            vec![Some(0), Some(1)]
        );
        assert_eq!(phases(&df), vec![Some("before"), Some("before")]);
        // Addresses stay as dumped, to be matched by eye and by filter.
        assert_eq!(
            df.column("address").unwrap().str().unwrap().get(0).unwrap(),
            "0x1a4"
        );
    }

    #[test]
    fn keeps_the_lock_bit_registers_whole() {
        // Bit 63 is the lock bit in MSR_PKG_POWER_LIMIT, which an Int64 column
        // could not hold - the whole reason `value` is unsigned.
        let df = collect(&format!("{HEADER}0x610,0,0x8000000000000001\n"), "after");
        assert_eq!(values(&df), vec![Some(0x8000_0000_0000_0001)]);
    }

    #[test]
    fn unreadable_registers_become_nulls() {
        let df = collect(
            &format!("{HEADER}0xc90,0,None\n0xc90,1,0x00000000000007ff\n"),
            "before",
        );
        assert_eq!(values(&df), vec![None, Some(0x7ff)]);
    }

    #[test]
    fn a_dump_with_no_readable_registers_still_types_as_u64() {
        // The case the pinned schema exists for: every value unreadable, which
        // inferred on its own would not concat with a dump that did read.
        let df = collect(&format!("{HEADER}0xc90,0,None\n"), "before");
        assert_eq!(df.column("value").unwrap().dtype(), &DataType::UInt64);
    }

    #[test]
    fn an_empty_dump_still_types_as_u64() {
        // msr.py writes a header-only file when it cannot read the online cpu
        // list at all.
        let df = collect(HEADER, "before");
        assert_eq!(df.height(), 0);
        assert_eq!(df.column("value").unwrap().dtype(), &DataType::UInt64);
    }

    #[test]
    fn a_mangled_value_fails_the_file() {
        assert!(parse(format!("{HEADER}0x1a4,0,garbage\n").as_bytes(), "before").is_err());
    }

    #[test]
    fn both_dumps_stack_into_one_table() {
        let before = parse(
            format!("{HEADER}0x1a4,0,0x0000000000000000\n").as_bytes(),
            "before",
        )
        .unwrap();
        let after = parse(
            format!("{HEADER}0x1a4,0,0x000000000000000f\n").as_bytes(),
            "after",
        )
        .unwrap();

        let df = combine(Some(before), Some(after))
            .unwrap()
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(df.height(), 2);
        assert_eq!(values(&df), vec![Some(0x0), Some(0xf)]);
        assert_eq!(phases(&df), vec![Some("before"), Some("after")]);
    }

    #[test]
    fn an_interrupted_run_keeps_its_before_dump() {
        let before = parse(
            format!("{HEADER}0x1a4,0,0x0000000000000000\n").as_bytes(),
            "before",
        )
        .unwrap();
        let df = combine(Some(before), None)
            .unwrap()
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(phases(&df), vec![Some("before")]);
    }

    #[test]
    fn a_run_without_msr_dumps_yields_no_table() {
        assert!(combine(None, None).unwrap().is_none());
    }
}
