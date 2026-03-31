use polars::prelude::*;

/// Shrinks integer columns in a `DataFrame` to the smallest fitting integer type.
///
/// # Errors
///
/// Returns `Err` if casting a column to the target integer type fails, or if constructing
/// the resulting `DataFrame` fails.
pub fn shrink_int_columns(df: &DataFrame) -> PolarsResult<DataFrame> {
    let height = df.height();
    let mut columns = Vec::new();

    for series in df.columns() {
        let shrunk = match series.dtype() {
            DataType::Int64 => {
                let ca = series.i64()?;
                let min = ca.min().unwrap_or(0);
                let max = ca.max().unwrap_or(0);

                // Check if can be unsigned
                if min >= 0 {
                    if max <= u8::MAX.into() {
                        series.cast(&DataType::UInt8)?
                    } else if max <= u16::MAX.into() {
                        series.cast(&DataType::UInt16)?
                    } else if max <= u32::MAX.into() {
                        series.cast(&DataType::UInt32)?
                    } else {
                        series.cast(&DataType::UInt64)?
                    }
                } else {
                    // Has negative values, stay signed
                    if min >= i8::MIN.into() && max <= i8::MAX.into() {
                        series.cast(&DataType::Int8)?
                    } else if min >= i16::MIN.into() && max <= i16::MAX.into() {
                        series.cast(&DataType::Int16)?
                    } else if min >= i32::MIN.into() && max <= i32::MAX.into() {
                        series.cast(&DataType::Int32)?
                    } else {
                        series.clone()
                    }
                }
            }
            DataType::UInt64 => {
                let ca = series.u64()?;
                let max = ca.max().unwrap_or(0);

                if max <= u8::MAX.into() {
                    series.cast(&DataType::UInt8)?
                } else if max <= u16::MAX.into() {
                    series.cast(&DataType::UInt16)?
                } else if max <= u32::MAX.into() {
                    series.cast(&DataType::UInt32)?
                } else {
                    series.clone()
                }
            }
            _ => series.clone(),
        };
        columns.push(shrunk);
    }

    DataFrame::new(height, columns)
}
