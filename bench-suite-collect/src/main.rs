use anyhow::{Context, Result, anyhow};
use crossbeam::channel;
use polars::io::parquet::metadata::{ParquetStatistics, deserialize as deserialize_statistics};
use polars::polars_utils::compression::ZstdLevel;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use string_intern::Intern;

use bench_suite_collect_results::{BenchSuiteCollect, FileInfo};
use bench_suite_config::BenchSuiteTasks;
use bench_suite_types::BenchSuiteRun;

/// Identifies the exact build of this binary: the git commit it was built from
/// plus the millisecond timestamp it was compiled at (see `build.rs`). Any
/// rebuild - even without a new commit - produces a new identifier, which is
/// intentional: it forces a full re-collection whenever the collection logic
/// might have changed, rather than silently trusting stale incremental state.
const CODE_VERSION: &str = concat!(
    env!("BENCH_SUITE_GIT_HASH"),
    "@",
    env!("BENCH_SUITE_BUILD_TIME_MS")
);

const MARKER_FILE_NAME: &str = ".bench_suite_marker.json";

#[derive(Serialize, Deserialize)]
struct CollectionMarker {
    version: String,
    timestamp_ms: i64,
}

/// Whether a collection directory is being rebuilt from scratch this run, or
/// incrementally updated against a `modified` table of dirty ids.
enum CollectionMode {
    Full,
    Incremental { modified: Vec<bool> },
}

fn marker_path(collection_path: &Path) -> PathBuf {
    collection_path.join(MARKER_FILE_NAME)
}

fn write_marker(collection_path: &Path, timestamp_ms: i64) -> Result<()> {
    let marker = CollectionMarker {
        version: CODE_VERSION.to_string(),
        timestamp_ms,
    };
    let content = serde_json::to_string(&marker).context("Failed to serialize marker")?;
    fs::write(marker_path(collection_path), content).context("Failed to write marker file")
}

/// Reads a previously-written marker for a collection directory, if one exists,
/// matches the current build, and can be parsed. Anything else (no marker, a
/// version mismatch, a corrupt file) means we can't trust the existing data
/// layout and fall back to a full rebuild.
fn read_marker(collection_path: &Path) -> Option<CollectionMarker> {
    let content = fs::read_to_string(marker_path(collection_path)).ok()?;
    let marker: CollectionMarker = serde_json::from_str(&content).ok()?;
    (marker.version == CODE_VERSION).then_some(marker)
}

fn tar_mtime_ms(tar_path: &Path) -> Option<i64> {
    let modified = fs::metadata(tar_path).ok()?.modified().ok()?;
    let ms = modified.duration_since(UNIX_EPOCH).ok()?.as_millis();
    i64::try_from(ms).ok()
}

/// Looks up whether `id` is marked dirty in a per-collection `modified`
/// table. An out-of-range id (shouldn't normally happen) falls back to
/// `default_dirty`.
fn is_dirty(modified: &[bool], id: u64, default_dirty: bool) -> bool {
    usize::try_from(id).map_or(default_dirty, |i| {
        modified.get(i).copied().unwrap_or(default_dirty)
    })
}

/// Decides whether `collection_path` can be updated incrementally against
/// its existing marker, or needs a full rebuild - and if it's a full
/// rebuild, wipes and recreates the directory so it's ready for one.
fn determine_collection_mode(config: &BenchSuiteTasks, collection_path: &Path) -> CollectionMode {
    let marker = collection_path
        .exists()
        .then(|| read_marker(collection_path))
        .flatten();

    if let Some(marker) = marker {
        return CollectionMode::Incremental {
            modified: build_modified_table(config, marker.timestamp_ms),
        };
    }

    if collection_path.exists() {
        fs::remove_dir_all(collection_path)
            .expect("Failed to delete existing collection directory");
    }
    fs::create_dir_all(collection_path).expect("Failed to create collection directory");
    CollectionMode::Full
}

/// Builds the per-id "does this id need collecting" table: for every id in
/// `0..bench_index`, mark it dirty if its tar file is missing (so the normal
/// missing-tar error handling in `process_run` will fire) or if it exists and
/// was modified after `since_ms`.
fn build_modified_table(config: &BenchSuiteTasks, since_ms: i64) -> Vec<bool> {
    (0..config.bench_index())
        .map(|id| {
            let tar_path = config.tar_file_path(id);
            match tar_mtime_ms(&tar_path) {
                Some(mtime_ms) => mtime_ms > since_ms,
                None => true,
            }
        })
        .collect()
}

#[derive(Default)]
struct PruneStats {
    rewritten: u64,
    deleted: u64,
    skipped_by_statistics: u64,
    skipped_by_id_column: u64,
}

impl PruneStats {
    fn add(&mut self, other: &Self) {
        self.rewritten += other.rewritten;
        self.deleted += other.deleted;
        self.skipped_by_statistics += other.skipped_by_statistics;
        self.skipped_by_id_column += other.skipped_by_id_column;
    }
}

/// Number of worker threads to use for parquet read/rewrite work: one per
/// available CPU, since this is the CPU-bound (decompress/filter/recompress)
/// counterpart to the I/O-bound tar mtime scan.
fn parquet_worker_count() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZero::get)
}

/// Whether any id in the inclusive range `lo..=hi` is marked dirty. Ids past
/// the end of the `modified` table are ones this run isn't collecting at all,
/// so they're never dirty and a range entirely past the end has no hits.
fn any_dirty_in_range(modified: &[bool], lo: u64, hi: u64) -> bool {
    let lo = usize::try_from(lo).unwrap_or(usize::MAX);
    let hi = usize::try_from(hi)
        .unwrap_or(usize::MAX)
        .min(modified.len().saturating_sub(1));
    lo <= hi
        && modified
            .get(lo..=hi)
            .is_some_and(|range| range.contains(&true))
}

/// Pulls a row group's min or max statistic - handed back as a single-element
/// array - out as a `u64`, whatever integer width `shrink_int_columns` picked
/// for the column on the way in.
fn statistic_as_u64(array: ArrayRef) -> Option<u64> {
    Series::from_arrow(PlSmallStr::from_static("id"), array)
        .ok()?
        .cast(&DataType::UInt64)
        .ok()?
        .u64()
        .ok()?
        .get(0)
}

/// Uses the row-group statistics in a parquet file's footer to decide whether
/// the file could contain a row for a dirty id at all. Every row group records
/// the min and max `id` it holds, so a file whose id ranges all miss the dirty
/// set is ruled out from the footer alone - without reading, let alone
/// decompressing, a single column. Anything that can't be answered from the
/// statistics (no `id` in the column lookup, or missing/unreadable min-max)
/// conservatively answers `true`, leaving the real decision to the caller.
fn may_contain_dirty_id(path: &Path, modified: &[bool]) -> Result<bool> {
    let mut reader = ParquetReader::new(BufReader::new(
        File::open(path).context("Failed to open parquet file for pruning")?,
    ));
    let schema = reader
        .schema()
        .context("Failed to read parquet schema for pruning")?;
    let Some(id_field) = schema.get("id") else {
        return Ok(true);
    };
    let metadata = reader
        .get_metadata()
        .context("Failed to read parquet metadata for pruning")?
        .clone();

    for row_group in &metadata.row_groups {
        let Some(mut columns) = row_group.columns_under_root_iter("id") else {
            return Ok(true);
        };
        let statistics = deserialize_statistics(id_field, &mut columns)
            .map_err(|e| anyhow!("Failed to read id statistics: {e}"))?;
        let Some(ParquetStatistics::Column(column)) = statistics else {
            return Ok(true);
        };
        let column = column
            .into_arrow()
            .map_err(|e| anyhow!("Failed to decode id statistics: {e}"))?;
        let (Some(min), Some(max)) = (column.min_value, column.max_value) else {
            return Ok(true);
        };
        let (Some(min), Some(max)) = (statistic_as_u64(min), statistic_as_u64(max)) else {
            return Ok(true);
        };
        if any_dirty_in_range(modified, min, max) {
            return Ok(true);
        }
    }

    Ok(false)
}

/// Turns the `id` column of `df` into a keep mask: `false` for rows whose id
/// the `modified` table marked dirty, `true` for everything else.
fn keep_mask(df: &DataFrame, modified: &[bool]) -> Result<BooleanChunked> {
    // `shrink_int_columns` (run before every parquet write) may have
    // narrowed "id" down to whatever integer width fit, so normalize
    // back to u64 before reading it out.
    let id_col = df
        .column("id")
        .context("Existing parquet file is missing its id column")?
        .as_materialized_series()
        .cast(&DataType::UInt64)
        .context("id column could not be read as an integer")?;
    Ok(id_col
        .u64()
        .unwrap()
        .into_iter()
        .map(|id| id.is_none_or(|id| !is_dirty(modified, id, false)))
        .collect())
}

/// Drops any rows belonging to a dirty id out of a single existing parquet
/// file, leaving the file untouched if none of its rows are dirty, rewriting
/// it in place if some (but not all) rows were dropped, or deleting it
/// outright if every row was dropped.
///
/// Reading the whole table is the expensive part, so it's the last thing
/// tried: parquet's footer statistics rule most files out for free, and the
/// columnar layout lets the survivors be checked by decompressing just the
/// one narrow `id` column.
fn prune_one_file(path: &Path, modified: &[bool], stats: &mut PruneStats) -> Result<()> {
    if !may_contain_dirty_id(path, modified)? {
        stats.skipped_by_statistics += 1;
        return Ok(());
    }

    // Statistics only bound the id range, so a hit there doesn't mean any of
    // the ids actually present are dirty. Read back just the `id` column -
    // parquet stores each column separately, so this decompresses one narrow
    // integer column instead of the entire table - and check for real.
    let ids = ParquetReader::new(BufReader::new(
        File::open(path).context("Failed to open parquet file for pruning")?,
    ))
    .with_columns(Some(vec!["id".to_string()]))
    .finish()
    .context("Failed to read id column for pruning")?;
    let keep = keep_mask(&ids, modified)?;

    // Nothing in this particular file belongs to a dirty id - leave it on
    // disk untouched rather than paying for a pointless read and rewrite.
    if keep.all() {
        stats.skipped_by_id_column += 1;
        return Ok(());
    }

    // Rows really are being dropped, so now pay for the full read. The
    // projected read above yielded the file's rows in their stored order, so
    // its mask lines up with this one row for row.
    let mut df = ParquetReader::new(BufReader::new(
        File::open(path).context("Failed to open parquet file for pruning")?,
    ))
    .finish()
    .context("Failed to read parquet file for pruning")?;

    df = df
        .filter(&keep)
        .context("Failed to filter stale rows out of parquet file")?;

    if df.height() == 0 {
        fs::remove_file(path).context("Failed to delete emptied parquet file")?;
        stats.deleted += 1;
    } else {
        ParquetWriter::new(File::create(path).context("Failed to recreate pruned parquet file")?)
            .with_compression(ParquetCompression::Zstd(Some(
                ZstdLevel::try_new(18).unwrap(),
            )))
            .with_statistics(StatisticsOptions::default())
            .with_row_group_size(Some(1_000_000))
            .finish(&mut df)
            .context("Failed to rewrite pruned parquet file")?;
        stats.rewritten += 1;
    }

    Ok(())
}

fn prune_worker(
    queue: &Mutex<std::vec::IntoIter<PathBuf>>,
    modified: &[bool],
) -> Result<PruneStats> {
    let mut stats = PruneStats::default();
    loop {
        let path = {
            let mut guard = queue.lock().unwrap();
            guard.next()
        };
        let Some(path) = path else {
            return Ok(stats);
        };
        prune_one_file(&path, modified, &mut stats)
            .with_context(|| format!("Failed to prune {}", path.display()))?;
    }
}

/// Before an incremental collection is reprocessed, drop any rows in its
/// existing parquet files that belong to ids the `modified` table marked
/// dirty - those ids are about to be recollected from their (changed) tar
/// files, so the old rows would otherwise become duplicates alongside the
/// new ones. A file that ends up empty is deleted outright. Files are spread
/// across one worker thread per CPU, since this work is dominated by parquet
/// decompression/recompression rather than I/O wait.
fn prune_stale_rows(collection_path: &Path, modified: &[bool]) -> Result<PruneStats> {
    if !modified.iter().any(|&dirty| dirty) {
        return Ok(PruneStats::default());
    }

    let paths: Vec<PathBuf> = fs::read_dir(collection_path)
        .context("Failed to read collection directory")?
        .map(|entry| Ok(entry.context("Failed to read directory entry")?.path()))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .filter(|path| path.extension().and_then(|e| e.to_str()) == Some("parquet"))
        .collect();

    if paths.is_empty() {
        return Ok(PruneStats::default());
    }

    let thread_count = parquet_worker_count().min(paths.len());
    let queue = Mutex::new(paths.into_iter());

    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..thread_count)
            .map(|_| scope.spawn(|| prune_worker(&queue, modified)))
            .collect();

        let mut stats = PruneStats::default();
        for handle in handles {
            stats.add(&handle.join().unwrap()?);
        }
        Ok(stats)
    })
}

/// Existing `<prefix>_<N>.parquet` files already occupy indexes `0..=max`, so
/// a fresh write for this table prefix in an incrementally-updated directory
/// must continue after `max` instead of restarting at 0, or it would silently
/// overwrite (and lose the rows in) an untouched pre-existing file.
fn next_parquet_index(location: &Path) -> u64 {
    let Some(dir) = location.parent() else {
        return 0;
    };
    let Some(prefix) = location.file_name().and_then(|n| n.to_str()) else {
        return 0;
    };

    let Ok(entries) = fs::read_dir(dir) else {
        return 0;
    };

    entries
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name();
            let name = name.to_str()?;
            let rest = name.strip_prefix(prefix)?.strip_prefix('_')?;
            rest.strip_suffix(".parquet")?.parse::<u64>().ok()
        })
        .max()
        .map_or(0, |max| max + 1)
}

struct ToCollectQueue<'a, T>
where
    T: Iterator<Item = (u64, &'a BenchSuiteRun, Vec<&'a str>, PathBuf)>,
{
    it: Mutex<T>,
    pb: indicatif::ProgressBar,
}

impl<'a, T> ToCollectQueue<'a, T>
where
    T: Iterator<Item = (u64, &'a BenchSuiteRun, Vec<&'a str>, PathBuf)>,
{
    fn new(input: T, progress: indicatif::ProgressBar) -> Self {
        Self {
            it: Mutex::new(input),
            pb: progress,
        }
    }

    fn consume(&self) -> Option<(u64, &'a BenchSuiteRun, Vec<&'a str>, PathBuf)> {
        let mut guard = self.it.lock().unwrap();
        let tmp = guard.next();
        if tmp.is_some() {
            self.pb.tick();
            self.pb.inc(1);
        }
        tmp
    }

    /// Retires the progress bar, leaving it drawn at its final state, and
    /// puts the cursor on a fresh line ready for ordinary output.
    ///
    /// Both halves matter to anything printed afterwards. A live bar rewrites
    /// its own line on every tick, so output printed underneath one is first
    /// appended to the bar's line and then erased by the next redraw. Stopping
    /// it is not enough on its own, though: a finished bar is left drawn
    /// *without* a trailing newline, so the next `println!` still lands on the
    /// end of it. Callers must be done consuming the queue.
    fn finish(&self) {
        self.pb.finish();
        println!();
    }
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct DatabaseLocation {
    directory: Intern,
    db_name: Intern,
}

type ParquetSubmit = (String, DataFrame);
type LazyFrameSendChannel = mpsc::SyncSender<LazyFrame>;

#[derive(Clone)]
struct TableSubmitter<'scope, 'env> {
    source: Arc<Mutex<HashMap<DatabaseLocation, LazyFrameSendChannel>>>,
    local: HashMap<DatabaseLocation, LazyFrameSendChannel>,
    scope: &'scope std::thread::Scope<'scope, 'env>,
    base_location: &'scope str,
    submit_queue: channel::Sender<ParquetSubmit>,
    drop_tables: &'scope HashSet<Intern>,
}

fn parquet_thread(
    rx: &std::sync::mpsc::Receiver<LazyFrame>,
    location: &Path,
    write_channel: &channel::Sender<ParquetSubmit>,
    start_index: u64,
) {
    let mut index: u64 = start_index;
    let mut data: Option<DataFrame> = None;
    while let Ok(msg) = rx.recv() {
        match &mut data {
            Some(v) => {
                v.vstack_mut(&msg.collect().unwrap()).unwrap();
            }
            None => {
                data = Some(msg.collect().unwrap());
            }
        }

        data = if let Some(mut df) = data.take() {
            if df.estimated_size() >= 750 * 1024 * 1024 {
                df = polars_helpers::shrink_int_columns(&df).unwrap();
                write_channel
                    .send((format!("{}_{}.parquet", location.display(), index), df))
                    .unwrap();
                index += 1;
                None
            } else {
                Some(df)
            }
        } else {
            None
        }
    }
    if let Some(mut df) = data {
        df = polars_helpers::shrink_int_columns(&df).unwrap();
        write_channel
            .send((format!("{}_{}.parquet", location.display(), index), df))
            .unwrap();
    }
}

fn parquet_write_thread(inputs: channel::Receiver<ParquetSubmit>, written_files: &AtomicU64) {
    for (s, mut df) in inputs {
        ParquetWriter::new(File::create(s).unwrap())
            .with_compression(ParquetCompression::Zstd(Some(
                ZstdLevel::try_new(18).unwrap(),
            )))
            .with_statistics(StatisticsOptions::default())
            .with_row_group_size(Some(1_000_000))
            .finish(&mut df)
            .unwrap();
        written_files.fetch_add(1, Ordering::Relaxed);
    }
}

impl<'scope, 'env> TableSubmitter<'scope, 'env> {
    pub fn submit(
        &mut self,
        key: DatabaseLocation,
        value: LazyFrame,
    ) -> std::result::Result<(), Box<std::sync::mpsc::SendError<LazyFrame>>> {
        if self.drop_tables.contains(&key.db_name) {
            return Ok(());
        }
        let base_location = self.base_location;
        let scope = self.scope;
        let source = &self.source;
        let chan = self.local.entry(key).or_insert_with(|| {
            let mut locked = source.lock().unwrap();
            locked
                .entry(key)
                .or_insert_with(|| {
                    let (tx, rx) = mpsc::sync_channel(1);
                    let submit = self.submit_queue.clone();
                    thread::Builder::new()
                        .name(format!("{}_{}", key.db_name, key.directory))
                        .spawn_scoped(scope, move || {
                            let path = Path::new(base_location)
                                .join(key.directory)
                                .join(key.db_name);
                            let start_index = next_parquet_index(&path);
                            parquet_thread(&rx, &path, &submit, start_index);
                        })
                        .unwrap();
                    tx
                })
                .clone()
        });
        chan.send(value).map_err(Box::new)
    }
    pub fn new(
        scope: &'scope std::thread::Scope<'scope, 'env>,
        base_location: &'scope str,
        write_channel: channel::Sender<ParquetSubmit>,
        drop_tables: &'scope HashSet<Intern>,
    ) -> Self {
        Self {
            source: Arc::new(Mutex::new(HashMap::new())),
            local: HashMap::new(),
            scope,
            base_location,
            submit_queue: write_channel,
            drop_tables,
        }
    }
}

fn process_run(run: &BenchSuiteRun, tar_path: &Path) -> Result<HashMap<Intern, LazyFrame>> {
    let tarfile = BufReader::new(File::open(tar_path)?);
    let tarfile = xz2::read::XzDecoder::new(tarfile);
    let mut tarfile = tar::Archive::new(tarfile);

    let entries = tarfile
        .entries()
        .context("Failed to get entries from tar file")?;

    let mut collectors: Vec<Box<dyn BenchSuiteCollect>> =
        bench_suite_benchmark_configs::get_collect_config(&run.benchmark)?
            .iter()
            .map(|x| x())
            .collect();

    let mut parsing_issues: Vec<String> = Vec::new();

    for file in entries {
        let file = file.context("Failed to get file from tar")?;
        let path = file
            .path()
            .context("Failed to get the path from tar file")?
            .to_str()
            .context("Failed to turn path to string".to_string())?
            .to_string();
        let mut file_info = FileInfo::new(path.as_str(), file);

        for i in &mut collectors {
            if let Err(e) = i.process_file(run, &mut file_info) {
                parsing_issues.push(format!("process_file({path}): {e:?}"));
            }
        }
    }

    drop(tarfile);

    let mut return_map: HashMap<Intern, LazyFrame> = HashMap::new();
    for collector in collectors {
        match BenchSuiteCollect::get_result(collector, run) {
            Ok(results) => {
                for (key, val) in results {
                    if return_map.insert(key, val).is_some() {
                        return Err(anyhow!(std::format!("Repeated the table name ")));
                    }
                }
            }
            Err(e) => {
                parsing_issues.push(format!("get_result: {e:?}"));
            }
        }
    }

    // Create parse_status table with one entry per error
    if !parsing_issues.is_empty() {
        let parse_status_df = df![
            "message" => &parsing_issues,
        ]?;
        return_map.insert(Intern::from_static("parse_status"), parse_status_df.lazy());
    }

    Ok(return_map)
}

fn process_thread<'a, T>(queue: &ToCollectQueue<'a, T>, mut submitter: TableSubmitter)
where
    T: Iterator<Item = (u64, &'a BenchSuiteRun, Vec<&'a str>, PathBuf)>,
{
    while let Some((id, run, paths, tar_path)) = queue.consume() {
        let map = match process_run(run, &tar_path) {
            Ok(v) => v,
            Err(e) => {
                // process_run itself failed
                let parse_status_df = df![
                    "message" => &[format!("{e:?}")],
                ]
                .unwrap();
                HashMap::from([(Intern::new("parse_status"), parse_status_df.lazy())])
            }
        };

        for (key, mut val) in map {
            val = val.with_column(lit(id).alias("id"));
            if let Some((last, remaining)) = paths.split_last() {
                for p in remaining {
                    submitter
                        .submit(
                            DatabaseLocation {
                                directory: Intern::new(*p),
                                db_name: key,
                            },
                            val.clone(),
                        )
                        .unwrap();
                }
                submitter
                    .submit(
                        DatabaseLocation {
                            directory: Intern::new(*last),
                            db_name: key,
                        },
                        val,
                    )
                    .unwrap();
            }
        }
    }
}

fn main() {
    let config_file_path = PathBuf::from(
        env::args()
            .nth(1)
            .expect("You need to provide a an argument for the path"),
    );

    let config = BenchSuiteTasks::new(&config_file_path).unwrap();

    // Timestamp marking the start of this run: recorded now (before any tar
    // files are inspected) and, on success, written into every collection's
    // marker. Using the start time - not the end time - means a tar file that
    // gets modified while this run is in flight will still be seen as dirty
    // on the *next* run rather than being incorrectly considered up to date.
    let run_start_ms = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();

    let base_path = config.get_path().clone();

    // Decide, per collection, whether this is a full rebuild (no usable
    // marker) or an incremental update (marker matches this build), and
    // build the incremental one's dirty-id table up front.
    let mut collection_mode: HashMap<String, CollectionMode> = HashMap::new();
    for name in config.collection_names() {
        let collection_path = base_path.join(name);
        let mode = determine_collection_mode(&config, &collection_path);
        collection_mode.insert(name.to_string(), mode);
    }

    // Prune stale rows for dirty ids out of each incremental collection's
    // existing parquet files before any new data is written for them.
    let mut prune_stats = PruneStats::default();
    for (name, mode) in &collection_mode {
        if let CollectionMode::Incremental { modified } = mode {
            let stats = prune_stale_rows(&base_path.join(name), modified)
                .with_context(|| format!("Failed to prune stale rows from collection {name}"))
                .unwrap();
            prune_stats.add(&stats);
        }
    }

    // Cut the full set of ids down to just those with at least one collection
    // that still wants them: for a `Full` collection that's every id it's
    // configured for, for an `Incremental` one it's only ids the modified
    // table marked dirty. An id with nothing left to do for it is skipped
    // entirely, so its tar file is never even opened.
    let entries: Vec<(u64, &BenchSuiteRun, Vec<&str>, PathBuf)> = config
        .to_collect()
        .filter_map(|(id, run, paths, tar_path)| {
            let filtered: Vec<&str> = paths
                .into_iter()
                .filter(|p| match &collection_mode[*p] {
                    CollectionMode::Full => true,
                    CollectionMode::Incremental { modified } => is_dirty(modified, id, true),
                })
                .collect();
            (!filtered.is_empty()).then_some((id, run, filtered, tar_path))
        })
        .collect();

    let progress = indicatif::MultiProgress::new();
    let main_progress = progress.add(
        indicatif::ProgressBar::new_spinner().with_finish(indicatif::ProgressFinish::AndLeave),
    );
    main_progress.set_style(
        indicatif::ProgressStyle::default_spinner()
            .template("{spinner:.cyan} [{human_pos}] [{elapsed_precise}] {msg}")
            .unwrap()
            .tick_strings(&["▹▹▹▹▹", "▸▹▹▹▹", "▹▸▹▹▹", "▹▹▸▹▹", "▹▹▹▸▹", "▹▹▹▹▸"]),
    );
    main_progress.set_message("TodoStream...");

    let queue = ToCollectQueue::new(entries.into_iter(), main_progress);

    let (write_send, write_recieve) = channel::bounded(5);
    let written_files = AtomicU64::new(0);

    std::thread::scope(|x| {
        let s = TableSubmitter::new(
            x,
            base_path.to_str().unwrap(),
            write_send,
            config.get_drop_tables(),
        );
        for i in 0..16 {
            let tmp_recieve = write_recieve.clone();
            let written_files = &written_files;
            thread::Builder::new()
                .name(format!("writer-{i}"))
                .spawn_scoped(x, move || parquet_write_thread(tmp_recieve, written_files))
                .unwrap();
        }
        for _ in 0..16 {
            let tmp_s = s.clone();
            x.spawn(|| {
                process_thread(&queue, tmp_s);
            });
        }
        drop(s);
    });

    for name in collection_mode.keys() {
        write_marker(&base_path.join(name), run_start_ms)
            .with_context(|| format!("Failed to write marker for collection {name}"))
            .unwrap();
    }

    // The spinner is owned by `queue`, so it would otherwise stay live - and
    // keep redrawing over the summary below - until it drops at the end of
    // main. Retiring it here separates the two cleanly.
    queue.finish();

    let written_files = written_files.load(Ordering::Relaxed);
    let PruneStats {
        rewritten,
        deleted,
        skipped_by_statistics,
        skipped_by_id_column,
    } = prune_stats;
    println!(
        "Done: {written_files} parquet file(s) written, {rewritten} rewritten and {deleted} deleted while pruning stale rows ({} file(s) touched in total).",
        written_files + rewritten + deleted
    );
    println!(
        "Pruning left {skipped_by_statistics} file(s) unread on row-group statistics alone, and {skipped_by_id_column} more after reading just their id column."
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// Writes a parquet file holding just an `id` column, through the same
    /// shrink-then-write path the collector uses, so the row-group statistics
    /// land in the footer exactly as they do in a real collection.
    fn write_ids(path: &Path, ids: &[u64]) {
        let mut df = polars_helpers::shrink_int_columns(
            &df!("id" => ids.to_vec()).expect("failed to build test frame"),
        )
        .expect("failed to shrink test frame");
        ParquetWriter::new(File::create(path).expect("failed to create test parquet file"))
            .with_compression(ParquetCompression::Zstd(Some(
                ZstdLevel::try_new(18).unwrap(),
            )))
            .with_statistics(StatisticsOptions::default())
            .with_row_group_size(Some(1_000_000))
            .finish(&mut df)
            .expect("failed to write test parquet file");
    }

    fn read_ids(path: &Path) -> Vec<u64> {
        let df = ParquetReader::new(BufReader::new(
            File::open(path).expect("failed to open test parquet file"),
        ))
        .finish()
        .expect("failed to read test parquet file");
        let ids = df.column("id").unwrap().cast(&DataType::UInt64).unwrap();
        ids.u64().unwrap().into_no_null_iter().collect()
    }

    /// A fresh, empty directory of our own, cleaned up when the guard drops.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static COUNTER: AtomicUsize = AtomicUsize::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!("prune-test-{}-{n}", std::process::id()));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).expect("failed to create test directory");
            Self(path)
        }

        fn file(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    /// `modified` table of length `len` with just the listed ids dirty.
    fn dirty(len: usize, ids: &[usize]) -> Vec<bool> {
        let mut modified = vec![false; len];
        for &id in ids {
            modified[id] = true;
        }
        modified
    }

    #[test]
    fn any_dirty_in_range_handles_edges() {
        let modified = dirty(10, &[4]);
        assert!(any_dirty_in_range(&modified, 4, 4));
        assert!(any_dirty_in_range(&modified, 0, 9));
        assert!(!any_dirty_in_range(&modified, 5, 9));
        assert!(!any_dirty_in_range(&modified, 0, 3));
        // Ids past the end of the table aren't being collected, so a range
        // that runs off the end - or starts past it - has no dirty ids there.
        assert!(!any_dirty_in_range(&modified, 10, 100));
        assert!(any_dirty_in_range(&modified, 4, 100));
        assert!(!any_dirty_in_range(&modified, 0, 0));
        assert!(!any_dirty_in_range(&[], 0, 100));
    }

    #[test]
    fn statistics_rule_out_a_file_whose_ids_are_all_clean() {
        let dir = TempDir::new();
        let path = dir.file("a.parquet");
        write_ids(&path, &[10, 11, 12, 13]);

        // Dirty ids sit entirely below and above this file's [10, 13] range.
        assert!(!may_contain_dirty_id(&path, &dirty(30, &[3, 20])).unwrap());
        // ... and now one lands inside it.
        assert!(may_contain_dirty_id(&path, &dirty(30, &[12])).unwrap());
        // A dirty id inside the min/max range but absent from the file can't
        // be ruled out by statistics alone - that's the id column's job.
        assert!(may_contain_dirty_id(&path, &dirty(30, &[11])).unwrap());
    }

    #[test]
    fn a_clean_file_is_skipped_without_being_rewritten() {
        let dir = TempDir::new();
        let path = dir.file("a.parquet");
        write_ids(&path, &[10, 12, 14]);
        let before = fs::metadata(&path).unwrap().len();

        let mut stats = PruneStats::default();
        prune_one_file(&path, &dirty(30, &[20]), &mut stats).unwrap();
        assert_eq!(stats.skipped_by_statistics, 1);

        // Id 13 is inside [10, 14] but not actually in the file, so the
        // statistics can't rule it out and the id column has to be read.
        prune_one_file(&path, &dirty(30, &[13]), &mut stats).unwrap();
        assert_eq!(stats.skipped_by_id_column, 1);

        assert_eq!(stats.rewritten, 0);
        assert_eq!(stats.deleted, 0);
        assert_eq!(fs::metadata(&path).unwrap().len(), before);
        assert_eq!(read_ids(&path), vec![10, 12, 14]);
    }

    #[test]
    fn dirty_rows_are_dropped_and_the_file_rewritten() {
        let dir = TempDir::new();
        let path = dir.file("a.parquet");
        write_ids(&path, &[10, 12, 14, 16]);

        let mut stats = PruneStats::default();
        prune_one_file(&path, &dirty(30, &[12, 16]), &mut stats).unwrap();

        assert_eq!(stats.rewritten, 1);
        assert_eq!(stats.deleted, 0);
        assert_eq!(read_ids(&path), vec![10, 14]);
    }

    #[test]
    fn a_fully_dirty_file_is_deleted() {
        let dir = TempDir::new();
        let path = dir.file("a.parquet");
        write_ids(&path, &[10, 12]);

        let mut stats = PruneStats::default();
        prune_one_file(&path, &dirty(30, &[10, 12]), &mut stats).unwrap();

        assert_eq!(stats.deleted, 1);
        assert_eq!(stats.rewritten, 0);
        assert!(!path.exists());
    }

    #[test]
    fn pruning_a_directory_reports_every_outcome() {
        let dir = TempDir::new();
        write_ids(&dir.file("clean.parquet"), &[1, 2]);
        write_ids(&dir.file("partial.parquet"), &[10, 11]);
        write_ids(&dir.file("gone.parquet"), &[20, 21]);
        fs::write(dir.file("ignored.txt"), "not a parquet file").unwrap();

        let stats = prune_stale_rows(&dir.0, &dirty(30, &[11, 20, 21])).unwrap();

        assert_eq!(stats.rewritten, 1);
        assert_eq!(stats.deleted, 1);
        assert_eq!(stats.skipped_by_statistics, 1);
        assert_eq!(read_ids(&dir.file("clean.parquet")), vec![1, 2]);
        assert_eq!(read_ids(&dir.file("partial.parquet")), vec![10]);
        assert!(!dir.file("gone.parquet").exists());
    }
}
