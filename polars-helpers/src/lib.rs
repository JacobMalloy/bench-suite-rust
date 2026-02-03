use polars::prelude::*;


pub fn shrink_int_columns(df: DataFrame) -> PolarsResult<DataFrame> {
    let mut columns = Vec::new();
    
    for series in df.take_columns() {
        let shrunk = match series.dtype() {
            DataType::Int64 => {
                let ca = series.i64()?;
                let min = ca.min().unwrap_or(0);
                let max = ca.max().unwrap_or(0);
                
                // Check if can be unsigned
                if min >= 0 {
                    if max <= u8::MAX as i64 {
                        series.cast(&DataType::UInt8)?
                    } else if max <= u16::MAX as i64 {
                        series.cast(&DataType::UInt16)?
                    } else if max <= u32::MAX as i64 {
                        series.cast(&DataType::UInt32)?
                    } else {
                        series.cast(&DataType::UInt64)?
                    }
                } else {
                    // Has negative values, stay signed
                    if min >= i8::MIN as i64 && max <= i8::MAX as i64 {
                        series.cast(&DataType::Int8)?
                    } else if min >= i16::MIN as i64 && max <= i16::MAX as i64 {
                        series.cast(&DataType::Int16)?
                    } else if min >= i32::MIN as i64 && max <= i32::MAX as i64 {
                        series.cast(&DataType::Int32)?
                    } else {
                        series
                    }
                }
            },
            DataType::UInt64 => {
                let ca = series.u64()?;
                let max = ca.max().unwrap_or(0);
                
                if max <= u8::MAX as u64 {
                    series.cast(&DataType::UInt8)?
                } else if max <= u16::MAX as u64 {
                    series.cast(&DataType::UInt16)?
                } else if max <= u32::MAX as u64 {
                    series.cast(&DataType::UInt32)?
                } else {
                    series
                }
            },
            _ => series,
        };
        columns.push(shrunk);
    }
    
    DataFrame::new(columns)
}
