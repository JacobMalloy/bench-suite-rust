use core::slice;
use custom_float::PositiveNonZeroF64;
use polars::prelude::*;
use serde::Deserialize;
use string_intern::Intern;

use core::num::NonZero;

// Trait to convert a single value to a Series column
// This abstracts over different types so the macro can use a uniform interface
trait ToSeriesColumn {
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

macro_rules! make_vectorized {
    ($original:ident, $vectorized:ident ,  { $($field:ident : $typ:ty),* $(,)? },
     optional:{$($opt_field:ident : $opt_typ:ty),* $(,)?}) => {
        #[allow(non_snake_case)]
        #[derive(Debug, Clone, PartialEq,Deserialize,Hash)]
        #[serde(deny_unknown_fields)]
        pub struct $original {
            $(pub $field: $typ),*,
            $(pub $opt_field: Option<$opt_typ>),*
        }

        #[allow(non_snake_case)]
        #[derive(Debug, Clone,Deserialize)]
        pub struct $vectorized {
            $($field: Option<Vec<$typ>>),*,
            $($opt_field: Option<Vec<$opt_typ>>),*,
        }

        impl $vectorized {
            pub fn contains(&self, item: &$original) -> bool {
                $(
                     if ! match &self.$field{
                        Some(v)=>{
                            v.contains(&item.$field)
                        }
                        None=>true
                    }{
                        return false;
                    }
                )*
                $(
                     if ! match &self.$opt_field{
                        Some(v)=>{
                            match &item.$opt_field{
                                Some(o)=>{
                                    let tmp = v.contains(o);
                                    tmp
                                },
                                None=>false
                            }
                        }
                        None=>true
                    }{
                        return false;
                    }
                )*
                true
            }

        }

        impl $original{
            pub fn to_df(&self)->Result<DataFrame,polars::error::PolarsError>{
                let columns: Vec<Column> = vec![
                    $(
                        self.$field.to_series_column(stringify!($field).into()).into(),
                    )*
                    $(
                        self.$opt_field.to_series_column(stringify!($opt_field).into()).into(),
                    )*
                ];
                DataFrame::new(columns)
            }
        }
    };
}

make_vectorized!(BenchSuiteRun,BenchSuiteConfig,{
    benchmark:Intern,
    tar_file:String,
    iteration:u64,
} , optional:{
    timeout:NonZero<u64>,
    cpu_mask:NonZero<u64>,
    //java
    jdk:Intern,
    process_count:NonZero<u64>,
    gc:Intern,
    classpath:Intern,

    gc_logging:Intern,
    memory_ratio:PositiveNonZeroF64,
    concgcthreads:NonZero<u64>,

    GCThreadCPUs:Intern,
    NonGCThreadCPUs:Intern,

    ResctrlIdleGCMask:NonZero<u64>,
    ResctrlMarkingGCMask:NonZero<u64>,
    ResctrlCollectingGCMask:NonZero<u64>,

    ResctrlIdleAppMask:NonZero<u64>,
    ResctrlMarkingAppMask:NonZero<u64>,
    ResctrlCollectingAppMask:NonZero<u64>,


    //dacapo
    dacapo_benchmark:Intern,
    dacapo_location:Intern,
    dacapo_threads:NonZero<u64>,
    dacapo_harness:Intern,

    //threadstat
    threadstat_location:Intern,

    //cos
    cos_config:Intern,


});
