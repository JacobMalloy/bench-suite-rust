use std::io::Write;

use polars::prelude::*;
use polars_core::frame::chunk_df_for_writing;
use polars_parquet::read::ParquetError;
use polars_parquet::write::{
    Compressor, DynIter, DynStreamingIterator, Encoding, FallibleStreamingIterator, FileWriter,
    StatisticsOptions, Version, WriteOptions, array_to_columns, get_dtype_encoding,
    to_parquet_schema,
};

pub use polars_parquet::write::{CompressionOptions, ZstdLevel};

/// Writes `df` to `writer` as a single Parquet file, driving `polars-parquet`'s
/// writer directly instead of going through `polars_io::ParquetWriter`.
///
/// This exists so callers can override the per-column encoding Polars would
/// otherwise pick automatically (always dictionary/plain for primitives - see
/// `get_dtype_encoding`). `encoding_for(column_name)` returning `Some(encoding)`
/// forces that encoding for the named column; `None` keeps Polars' default.
///
/// # Errors
///
/// Returns `Err` if the schema can't be converted to an Arrow/Parquet schema,
/// or if encoding, compressing, or writing any column fails.
///
/// # Panics
///
/// Panics if a column's Arrow array can't be encoded into Parquet pages, which
/// should not happen since the schema is derived from `df` itself.
pub fn write_parquet<W: Write>(
    df: &mut DataFrame,
    writer: W,
    compression: CompressionOptions,
    row_group_size: Option<usize>,
    encoding_for: impl Fn(&str) -> Option<Encoding>,
) -> PolarsResult<u64> {
    let chunked_df = chunk_df_for_writing(df, row_group_size.unwrap_or(512 * 512))?;
    let arrow_schema =
        polars_io::schema_to_arrow_checked(chunked_df.schema(), CompatLevel::newest(), "parquet")?;
    let parquet_schema = to_parquet_schema(&arrow_schema)?;
    let fields = parquet_schema.fields().to_vec();

    let encodings: Vec<Vec<Encoding>> = arrow_schema
        .iter_values()
        .map(|f| match encoding_for(f.name.as_str()) {
            Some(encoding) => vec![encoding],
            None => get_dtype_encoding(&f.dtype),
        })
        .collect();

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
            .zip(&fields)
            .zip(&encodings)
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

        let mut buf = Vec::new();
        write_parquet(&mut df, &mut buf, CompressionOptions::Uncompressed, None, |_| None).unwrap();

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

        let mut default_buf = Vec::new();
        write_parquet(
            &mut df.clone(),
            &mut default_buf,
            CompressionOptions::Uncompressed,
            None,
            |_| None,
        )
        .unwrap();

        let mut delta_buf = Vec::new();
        write_parquet(
            &mut df,
            &mut delta_buf,
            CompressionOptions::Uncompressed,
            None,
            |name| (name == "event_id").then_some(Encoding::DeltaBinaryPacked),
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
}
