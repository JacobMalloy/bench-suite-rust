use core::num::NonZero;
use custom_float::{PositiveF64, PositiveNonZeroF64};
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
        #[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
        #[derive(Debug, Clone, PartialEq,Hash)]
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
            /// Converts this run config into a single-row Polars `DataFrame`.
            ///
            /// # Errors
            ///
            /// Returns `Err` if Polars fails to construct the `DataFrame` from the column series.
            pub fn to_df(&self)->Result<DataFrame,polars::error::PolarsError>{
                let columns: Vec<Column> = vec![
                    $(
                        self.$field.to_series_column(stringify!($field).into()).into(),
                    )*
                    $(
                        self.$opt_field.to_series_column(stringify!($opt_field).into()).into(),
                    )*
                ];
                DataFrame::new(1,columns)
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
    msr:Intern,
    //java
    jdk:Intern,
    process_count:NonZero<u64>,
    gc:Intern,
    classpath:Intern,

    gc_logging:Intern,
    java_log_gc:Intern,
    java_log_os:Intern,
    java_log_filecount:u64,
    java_disable_async_logging:bool,
    dacapo_disable_latency_csv:bool,
    java_perf_per_thread:bool,
    memory_ratio:PositiveNonZeroF64,
    memory_config:NonZero<u64>,
    softmax:Intern,
    softmax_ratio:PositiveNonZeroF64,
    concgcthreads:NonZero<u64>,
    jdk_tiered_compilation:bool,
    zgc_barrier_use_global_variable:bool,
    zgc_barrier_rewrite_on_phase_change:bool,
    java_active_processor_count:NonZero<u64>,

    GCThreadCPUs:Intern,
    NonGCThreadCPUs:Intern,
    numactl_cpus:Intern,
    numactl_mem:Intern,

    zgc_deactivate_proactive:bool,

    opp_zgc:bool,
    opp_zgc_period:NonZero<u64>,
    opp_zgc_minor_threshold:u64,
    opp_zgc_major_threshold:u64,
    opp_zgc_avg_cpu_window:NonZero<u64>,
    opp_zgc_concurrent_base_cost:PositiveNonZeroF64,
    opp_zgc_cpu_weight:PositiveF64,
    opp_zgc_require_warm:bool,
    opp_zgc_post_gc_growth:bool,
    opp_zgc_growth_ratio:PositiveNonZeroF64,
    opp_zgc_cpu_exponent:PositiveNonZeroF64,
    opp_zgc_min_alloc_percent:PositiveF64,
    opp_zgc_slow_path_nanos:PositiveF64,
    opp_zgc_assumed_workers:PositiveF64,
    opp_zgc_garbage_gate:bool,

    java_thp:bool,

    // Superseded by resctrl_groups below, which moved cache allocation out of
    // the JVM and into the harness. utils/resctrl.py's apply_config now raises
    // on any of these rather than half-applying them -- they stay here only so
    // that the runs already in the store keep parsing.
    ResctrlIdleGCMask:NonZero<u64>,
    ResctrlMarkingGCMask:NonZero<u64>,
    ResctrlCollectingGCMask:NonZero<u64>,

    ResctrlIdleAppMask:NonZero<u64>,
    ResctrlMarkingAppMask:NonZero<u64>,
    ResctrlCollectingAppMask:NonZero<u64>,

    // resctrl cache allocation and group layout, owned by the harness. A
    // semicolon-separated spec: "<name>=<hex mask>" is a control group whose
    // L3 mask the harness writes, "<name>" one left at its default mask, and
    // "mon:<name>" a monitoring-only group under the GC group, which separates
    // counters without taking an allocation of its own (the only kind of group
    // a box with CMT/MBM but no L3 CAT can offer).
    resctrl_groups:Intern,

    // Which resctrl group each thread class, and each concurrent ZGC phase,
    // runs in. These pass straight through to the java command line; the JVM
    // moves its GC workers between groups as phases change, and every name
    // here must also appear in resctrl_groups. A phase left unset keeps its
    // workers in ResctrlGCGroup. ZGC only.
    ResctrlGCGroup:Intern,
    ResctrlNonGCGroup:Intern,
    ResctrlYoungMarkGroup:Intern,
    ResctrlYoungSelectGroup:Intern,
    ResctrlYoungRelocateGroup:Intern,
    ResctrlOldMarkGroup:Intern,
    ResctrlOldSelectGroup:Intern,
    ResctrlOldRelocateGroup:Intern,


    //dacapo
    dacapo_benchmark:Intern,
    dacapo_location:Intern,
    dacapo_threads:NonZero<u64>,
    dacapo_harness:Intern,

    //specjbb
    specjbb_location:Intern,
    specjbb_props:Intern,
    specjbb_opts:Intern,
    specjbb_args:Intern,
    specjbb_report_level:u8,

    //mark abuse
    mark_abuse_location:Intern,
    mark_abuse_cardinality:NonZero<u64>,
    mark_abuse_keys:NonZero<u64>,
    mark_abuse_iterations:NonZero<u64>,
    mark_abuse_warmup:NonZero<u64>,
    mark_abuse_graph_nodes:NonZero<u64>,
    mark_abuse_edges_per_node:NonZero<u64>,
    mark_abuse_rotate_interval:u64,
    mark_abuse_rotate_fraction:PositiveNonZeroF64,


    //threadstat
    threadstat_location:Intern,
    threadstat_wrapper_location:Intern,
    threadstat_event:Intern,
    threadstat_frequency:NonZero<u64>,


    perf_events:Intern,
    perf_location:Intern,
    // `perf stat -I` interval in milliseconds. Unset keeps the one
    // aggregate count per event per run that PerfStat has always produced;
    // set, perf prints one row per event per interval and perf_stat.csv
    // gains time_s / wall_epoch_us. Needed here and not just in the
    // collector because this struct is deny_unknown_fields: a config key the
    // Rust side has never heard of makes the whole status file unparseable,
    // so EVERY run of a sweep that sets it fails to collect, not just its
    // perf_stat table.
    perf_stat_interval_ms:NonZero<u64>,

    // resctrl monitoring (CMT / MBM), sampled by utils/resctrl.py's
    // ResctrlMon into resctrl_<event>.csv. l3_mon adds llc_occupancy,
    // memory_mon adds mbm_total_bytes and mbm_local_bytes. Unlike every other
    // counter in this suite these are attributed to a resctrl GROUP, which is
    // what lets them separate GC from mutator without relying on core
    // placement -- COS1 holds the GC's threads.
    l3_mon:bool,
    memory_mon:bool,
    resctrl_mon_frequency:NonZero<u64>,
    resctrl_mon_groups:Intern,

    perf_record:bool,
    perf_record_events:Intern,
    // "none" for no call-graph, otherwise a `--call-graph` mode ("dwarf",
    // "fp", "lbr") - kept a single string type (see PerfRecord's docstring)
    // rather than a bool, since a real value goes there just as often as a
    // disable does.
    perf_record_call_graph:Intern,
    perf_record_cpus:Intern,
    // "none" for the (not recommended - see PerfRecord) un-set-clockid
    // kernel default, otherwise the `-k` clockid ("monotonic" in practice).
    perf_record_clockid:Intern,
    perf_record_delay_secs:PositiveNonZeroF64,
    perf_record_duration_secs:PositiveNonZeroF64,
    perf_record_mmap_pages:NonZero<u64>,
    perf_record_data:bool,
    perf_record_phys_data:bool,
    perf_record_branch:Intern,
    perf_record_intr_regs:Intern,
    perf_record_user_regs:Intern,
    perf_record_freq:NonZero<u64>,

    java_dump_perf_map:bool,

    //cos
    cos_config:Intern,
    cache_ways:NonZero<u64>,

    //hazelcast jet
    jet_gc_benchmark_location:Intern,
    jet_num_keys:NonZero<u64>,
    jet_items_per_second:NonZero<u64>,
    jet_win_size_millis:NonZero<u64>,
    jet_sliding_step_millis:NonZero<u64>,
    hazelcast_time_s:NonZero<u64>,

});
