use core::num::NonZero;
use custom_float::PositiveNonZeroF64;
#[cfg(feature = "polars")]
use polars::prelude::*;
#[cfg(feature = "serde")]
use serde::Deserialize;
use string_intern::Intern;

#[cfg(feature = "polars")]
mod polars_support;
#[cfg(feature = "polars")]
use polars_support::ToSeriesColumn;

macro_rules! make_vectorized {
    ($original:ident, $vectorized:ident ,  { $($field:ident : $typ:ty),* $(,)? },
     optional:{$($opt_field:ident : $opt_typ:ty),* $(,)?}) => {
        #[allow(non_snake_case)]
        #[cfg_attr(feature = "serde", derive(Deserialize))]
        #[derive(Debug, Clone, PartialEq,Hash)]
        #[serde(deny_unknown_fields)]
        pub struct $original {
            $(pub $field: $typ),*,
            $(pub $opt_field: Option<$opt_typ>),*
        }

        #[allow(non_snake_case)]
        #[cfg_attr(feature = "serde", derive(Deserialize))]
        #[derive(Debug, Clone)]
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

        #[cfg(feature="polars")]
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
