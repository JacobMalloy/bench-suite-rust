use std::io::Write;

use polars::prelude::*;
use polars_core::frame::chunk_df_for_writing;
use polars_parquet::read::ParquetError;
use polars_parquet::write::{
    Compressor, DynIter, DynStreamingIterator, Encoding, FallibleStreamingIterator, FileWriter,
    ParquetType, StatisticsOptions, Version, WriteOptions, array_to_columns, get_dtype_encoding,
    to_parquet_schema,
};

pub use polars_parquet::write::{CompressionOptions, ZstdLevel};

/// The Arrow/Parquet schema and per-column encoding for one table shape,
/// derived once from a Polars `Schema` and reusable across every Parquet file
/// written for that shape.
///
/// Deriving this (`schema_to_arrow_checked`, `to_parquet_schema`,
/// `get_dtype_encoding`) is real, repeated work if done on every file written
/// for what is logically "the same table" - callers that write many shards of
/// one table (see `bench-suite-collect`'s per-table accumulator) should
/// resolve it once and pass the same `ResolvedSchema` to each `write_parquet`
/// call, re-resolving only when a shard's actual `Schema` differs (e.g. after
/// int-shrinking picks a different width).
pub struct ResolvedSchema {
    source: Schema,
    arrow_schema: ArrowSchema,
    fields: Vec<ParquetType>,
    encodings: Vec<Vec<Encoding>>,
}

impl ResolvedSchema {
    /// Derives the Arrow/Parquet schema and per-column encoding for `source`.
    ///
    /// `encoding_for(column_name)` returning `Some(encoding)` forces that
    /// encoding for the named column; `None` keeps Polars' automatic choice.
    ///
    /// # Errors
    ///
    /// Returns `Err` if `source` can't be converted to an Arrow/Parquet schema.
    pub fn resolve(
        source: &Schema,
        encoding_for: impl Fn(&str) -> Option<Encoding>,
    ) -> PolarsResult<Self> {
        let arrow_schema =
            polars_io::schema_to_arrow_checked(source, CompatLevel::newest(), "parquet")?;
        let parquet_schema = to_parquet_schema(&arrow_schema)?;
        let fields = parquet_schema.fields().to_vec();

        let encodings: Vec<Vec<Encoding>> = arrow_schema
            .iter_values()
            .map(|f| match encoding_for(f.name.as_str()) {
                Some(encoding) => vec![encoding],
                None => get_dtype_encoding(&f.dtype),
            })
            .collect();

        Ok(Self {
            source: source.clone(),
            arrow_schema,
            fields,
            encodings,
        })
    }

    /// Whether this resolved schema still matches `schema` and can be reused
    /// as-is, rather than re-resolved (e.g. against a shard whose columns got
    /// shrunk to a narrower int width).
    #[must_use]
    pub fn matches(&self, schema: &Schema) -> bool {
        &self.source == schema
    }
}

/// Writes `df` to `writer` as a single Parquet file, driving `polars-parquet`'s
/// writer directly instead of going through `polars_io::ParquetWriter`.
///
/// This exists so callers can override the per-column encoding Polars would
/// otherwise pick automatically (always dictionary/plain for primitives - see
/// `get_dtype_encoding`), via `schema` (see `ResolvedSchema`).
///
/// # Errors
///
/// Returns `Err` if `df`'s schema doesn't match `schema` (it must be resolved
/// from - or re-resolved after - the same shape of `df` being written), or if
/// encoding, compressing, or writing any column fails.
///
/// # Panics
///
/// Panics if a column's Arrow array can't be encoded into Parquet pages, which
/// should not happen since `schema` was checked against `df`'s own schema.
pub fn write_parquet<W: Write>(
    df: &mut DataFrame,
    writer: W,
    compression: CompressionOptions,
    row_group_size: Option<usize>,
    schema: &ResolvedSchema,
) -> PolarsResult<u64> {
    let chunked_df = chunk_df_for_writing(df, row_group_size.unwrap_or(512 * 512))?;
    polars_ensure!(
        schema.matches(chunked_df.schema()),
        SchemaMismatch: "write_parquet called with a ResolvedSchema that no longer matches the DataFrame being written; re-resolve it for this shard's schema"
    );
    let arrow_schema = schema.arrow_schema.clone();
    let fields = schema.fields.as_slice();
    let encodings = schema.encodings.as_slice();

    let options = WriteOptions {
        statistics: StatisticsOptions::default(),
        compression,
        version: Version::V1,
        data_page_size: None,
    };

    let mut file_writer = FileWriter::try_new(writer, arrow_schema, options)?;

    for batch in chunked_df.iter_chunks(CompatLevel::newest(), false) {
        if batch.is_empty() {
            continue;
        }
        let num_rows = batch.len();

        let columns: Vec<_> = batch
            .columns()
            .iter()
            .zip(fields)
            .zip(encodings)
            .flat_map(|((array, type_), encoding)| {
                let encoded_columns = array_to_columns(array, type_.clone(), options, encoding)
                    .expect("array_to_columns should not fail for a schema derived from this same DataFrame");
                encoded_columns.into_iter().map(|encoded_pages| {
                    Ok(DynStreamingIterator::new(
                        Compressor::new_from_vec(
                            encoded_pages.map(|page| {
                                page.map_err(|e| {
                                    ParquetError::FeatureNotSupported(format!(
                                        "reraised in polars: {e}"
                                    ))
                                })
                            }),
                            options.compression,
                            vec![],
                        )
                        .map_err(PolarsError::from),
                    ))
                })
            })
            .collect();

        let row_group = DynIter::new(columns.into_iter());
        file_writer.write(num_rows as u64, row_group)?;
    }

    file_writer.end(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_back(bytes: &[u8]) -> DataFrame {
        ParquetReader::new(std::io::Cursor::new(bytes))
            .finish()
            .unwrap()
    }

    #[test]
    fn default_encoding_round_trips() {
        let mut df = df![
            "id" => (0i64..1000).collect::<Vec<_>>(),
            "name" => (0..1000).map(|i| format!("row-{i}")).collect::<Vec<_>>(),
        ]
        .unwrap();

        let schema = ResolvedSchema::resolve(df.schema(), |_| None).unwrap();

        let mut buf = Vec::new();
        write_parquet(
            &mut df,
            &mut buf,
            CompressionOptions::Uncompressed,
            None,
            &schema,
        )
        .unwrap();

        let read = read_back(&buf);
        assert_eq!(read, df);
    }

    #[test]
    fn forced_delta_encoding_round_trips_and_shrinks() {
        // A monotonically-increasing id column, mirroring threadstat's event_id/read_id.
        let mut df = df![
            "event_id" => (0i64..200_000).collect::<Vec<_>>(),
        ]
        .unwrap();

        let default_schema = ResolvedSchema::resolve(df.schema(), |_| None).unwrap();
        let mut default_buf = Vec::new();
        write_parquet(
            &mut df.clone(),
            &mut default_buf,
            CompressionOptions::Uncompressed,
            None,
            &default_schema,
        )
        .unwrap();

        let delta_schema = ResolvedSchema::resolve(df.schema(), |name| {
            (name == "event_id").then_some(Encoding::DeltaBinaryPacked)
        })
        .unwrap();
        let mut delta_buf = Vec::new();
        write_parquet(
            &mut df,
            &mut delta_buf,
            CompressionOptions::Uncompressed,
            None,
            &delta_schema,
        )
        .unwrap();

        assert!(
            delta_buf.len() < default_buf.len() / 4,
            "expected delta encoding to shrink a monotonic id column by more than 4x, got default={} delta={}",
            default_buf.len(),
            delta_buf.len()
        );

        let read = read_back(&delta_buf);
        let expected = df![
            "event_id" => (0i64..200_000).collect::<Vec<_>>(),
        ]
        .unwrap();
        assert_eq!(read, expected);
    }

    #[test]
    fn stale_resolved_schema_is_rejected() {
        // Mimics reusing a cached ResolvedSchema (see `resolve_schema_cached`
        // in bench-suite-collect) against a later shard whose columns don't
        // match anymore, e.g. `shrink_int_columns` picked a narrower width.
        let narrow = df!["count" => [1u8, 2, 3]].unwrap();
        let schema = ResolvedSchema::resolve(narrow.schema(), |_| None).unwrap();

        let mut wide = df!["count" => [1u32, 2, 3]].unwrap();
        let err = write_parquet(
            &mut wide,
            Vec::new(),
            CompressionOptions::Uncompressed,
            None,
            &schema,
        )
        .unwrap_err();

        assert!(matches!(err, PolarsError::SchemaMismatch(_)), "{err:?}");
    }
}
