use anyhow::{Context, Result};
use bench_suite_types::BenchSuiteRun;
use lazy_read::{self, LazyRead};
use polars::prelude::LazyFrame;
use std::io::Read;

use string_intern::Intern;

pub use polars_parquet::write::Encoding;

/// Per-column Parquet encoding override, keyed by column name.
///
/// Returning `Some(encoding)` forces that encoding for the named column;
/// `None` keeps Polars' automatic choice. A plain `fn` pointer (not a closure)
/// so it is `Copy` and can travel through the collection pipeline cheaply.
pub type ColumnEncoding = fn(&str) -> Option<Encoding>;

pub trait FileInfoInterface {
    fn name(&self) -> &str;

    /// Returns the file contents as a UTF-8 string.
    ///
    /// # Errors
    ///
    /// Returns `Err` if reading the file fails or if the contents are not valid UTF-8.
    fn content_string(&mut self) -> Result<&str>;

    /// Returns the file contents as raw bytes.
    ///
    /// # Errors
    ///
    /// Returns `Err` if reading the file fails.
    fn content_bytes(&mut self) -> Result<&[u8]>;
}

pub struct FileInfo<'a, T>
where
    T: Read,
{
    content: lazy_read::LazyRead<T>,
    name: &'a str,
}

impl<T> FileInfoInterface for FileInfo<'_, T>
where
    T: Read,
{
    fn name(&self) -> &str {
        self.name
    }
    fn content_string(&mut self) -> Result<&str> {
        self.content
            .get_string()
            .context("Failed to read the files contents")
    }
    fn content_bytes(&mut self) -> Result<&[u8]> {
        self.content
            .get_bytes()
            .context("Failed to read the files contents")
    }
}

impl<'a, T> FileInfo<'a, T>
where
    T: std::io::Read,
{
    pub fn new(name: &'a str, content: T) -> Self {
        FileInfo {
            name,
            content: LazyRead::new(content),
        }
    }
    pub fn name(&self) -> &str {
        self.name
    }
}

pub trait BenchSuiteCollect {
    /// Processes a single file from a benchmark run's archive.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the file is a duplicate, cannot be read, or fails to parse.
    fn process_file(
        &mut self,
        config: &BenchSuiteRun,
        file: &mut dyn FileInfoInterface,
    ) -> Result<()>;

    /// Consumes the collector and returns the collected data as named `LazyFrame`s.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the collected data cannot be assembled into a `LazyFrame`.
    fn get_result(self: Box<Self>, config: &BenchSuiteRun) -> Result<Vec<(Intern, LazyFrame)>>;

    /// Per-column Parquet encoding override applied to every table this
    /// collector produces. The default forces no encoding, matching Polars'
    /// automatic behavior.
    fn column_encoding(&self) -> ColumnEncoding {
        |_| None
    }
}
