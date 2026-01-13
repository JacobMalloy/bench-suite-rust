use serde::Deserialize;
use polars::prelude::*;
macro_rules! make_vectorized {
    ($original:ident, $vectorized:ident ,  { $($field:ident : $typ:ty),* $(,)? },
     optional:{$($opt_field:ident : $opt_typ:ty),* $(,)?}) => {
        #[derive(Debug, Clone, PartialEq,Deserialize)]
        #[serde(deny_unknown_fields)] 
        pub struct $original {
            $(pub $field: $typ),*,
            $(pub $opt_field: Option<$opt_typ>),*
        }

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
                df![
                    $(
                        stringify!($field) => std::slice::from_ref(&self.$field),
                   )*
                    $(
                        stringify!($opt_field) => std::slice::from_ref(&self.$opt_field),
                   )*
                ]
            }
        }
    };
}

make_vectorized!(BenchSuiteRun,BenchSuiteConfig,{ 
    benchmark:String,
    tar_file:String,
    iteration:u64,
} , optional:{
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


});



