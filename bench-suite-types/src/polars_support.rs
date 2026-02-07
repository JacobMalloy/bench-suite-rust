use core::num::NonZero;
use core::slice;
use custom_float::PositiveNonZeroF64;
use polars::prelude::*;
use string_intern::Intern;

// Trait to convert a single value to a Series column
// This abstracts over different types so the macro can use a uniform interface
pub trait ToSeriesColumn {
    fn to_series_column(&self, name: PlSmallStr) -> Series;
    fn get_null(name: PlSmallStr) -> Series;
}

impl ToSeriesColumn for Intern {
    fn to_series_column(&self, name: PlSmallStr) -> Series {
        StringChunked::from_slice(name, &[self.as_str()]).into_series()
    }
    fn get_null(name: PlSmallStr) -> Series {
        Series::full_null(name, 1, &DataType::String)
    }
}

impl ToSeriesColumn for String {
    fn to_series_column(&self, name: PlSmallStr) -> Series {
        StringChunked::from_slice(name, &[self.as_str()]).into_series()
    }
    fn get_null(name: PlSmallStr) -> Series {
        Series::full_null(name, 1, &DataType::String)
    }
}

impl ToSeriesColumn for u64 {
    fn to_series_column(&self, name: PlSmallStr) -> Series {
        UInt64Chunked::from_slice(name, slice::from_ref(self)).into_series()
    }
    fn get_null(name: PlSmallStr) -> Series {
        Series::full_null(name, 1, &DataType::UInt64)
    }
}

impl ToSeriesColumn for f64 {
    fn to_series_column(&self, name: PlSmallStr) -> Series {
        Float64Chunked::from_slice(name, slice::from_ref(self)).into_series()
    }
    fn get_null(name: PlSmallStr) -> Series {
        Series::full_null(name, 1, &DataType::Float64)
    }
}

impl ToSeriesColumn for PositiveNonZeroF64 {
    fn to_series_column(&self, name: PlSmallStr) -> Series {
        Float64Chunked::from_slice(name, slice::from_ref(&self.get())).into_series()
    }
    fn get_null(name: PlSmallStr) -> Series {
        Series::full_null(name, 1, &DataType::Float64)
    }
}

impl ToSeriesColumn for NonZero<u64> {
    fn to_series_column(&self, name: PlSmallStr) -> Series {
        UInt64Chunked::from_slice(name, slice::from_ref(&self.get())).into_series()
    }
    fn get_null(name: PlSmallStr) -> Series {
        Series::full_null(name, 1, &DataType::UInt64)
    }
}

impl<T: ToSeriesColumn> ToSeriesColumn for Option<T> {
    fn to_series_column(&self, name: PlSmallStr) -> Series {
        match self {
            Some(v) => v.to_series_column(name),
            None => T::get_null(name),
        }
    }
    fn get_null(name: PlSmallStr) -> Series {
        T::get_null(name)
    }
}
