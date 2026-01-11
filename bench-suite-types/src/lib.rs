use serde::Deserialize;
use polars::prelude::*;
macro_rules! make_vectorized {
    ($original:ident, $vectorized:ident ,  { $($field:ident : $typ:ty),* $(,)? }) => {
        #[derive(Debug, Clone, PartialEq,Deserialize)]
        #[serde(deny_unknown_fields)] 
        pub struct $original {
            $($field: Option<$typ>),*
        }

        #[derive(Debug, Clone,Deserialize)]
        pub struct $vectorized {
            $($field: Option<Vec<$typ>>),*
        }

        impl $vectorized {
            pub fn contains(&self, item: &$original) -> bool {
                $(
                     if ! match &self.$field{
                        Some(v)=>{
                            match &item.$field{
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
                df![
                    $(
                        stringify!($field) => std::slice::from_ref(&self.$field),
                   )*
                ]
            }
        }
    };
}

make_vectorized!(BenchSuiteRun,BenchSuiteConfig,{
    benchmark:String,
    iteration:u64,
    timeout:u64,
    cpu_mask:u64,
    //java
    jdk:String,
    process_count:u64,
    gc:String,

    gc_logging:String,
    memory_ratio:f64,
    concgcthreads:u64,
    

    //dacapo
    dacapo_benchmark:String,
    dacapo_location:String,
    dacapo_threads:u64,

    //threadstat
    threadstat_location:String,

    //cos
    cos_config:String,


    tar_file:String,

});



