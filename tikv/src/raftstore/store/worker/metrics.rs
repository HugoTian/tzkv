// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use prometheus::{exponential_buckets, CounterVec, Histogram, HistogramVec};

lazy_static! {
    pub static ref SNAP_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_raftstore_snapshot_total",
            "Total number of raftstore snapshot processed.",
            &["type", "status"]
        ).unwrap();

    pub static ref CHECK_SPILT_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_raftstore_check_split_total",
            "Total number of raftstore split check.",
            &["type"]
        ).unwrap();

    pub static ref SNAP_HISTOGRAM: HistogramVec =
        register_histogram_vec!(
            "tikv_raftstore_snapshot_duration_seconds",
            "Bucketed histogram of raftstore snapshot process duration",
            &["type"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref CHECK_SPILT_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_raftstore_check_split_duration_seconds",
            "Bucketed histogram of raftstore split check duration",
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref COMPACT_RANGE_CF: HistogramVec =
        register_histogram_vec!(
            "tikv_compact_range_cf_duration_seconds",
            "Bucketed histogram of compact range for cf execution",
            &["cf"]
        ).unwrap();

    pub static ref REGION_HASH_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_raftstore_hash_duration_seconds",
            "Bucketed histogram of raftstore hash compution duration"
        ).unwrap();

    pub static ref APPLY_PROPOSAL: Histogram =
        register_histogram!(
            "tikv_raftstore_apply_proposal",
            "Proposal count of all regions in a mio tick",
            exponential_buckets(1.0, 2.0, 20).unwrap()
        ).unwrap();
}
