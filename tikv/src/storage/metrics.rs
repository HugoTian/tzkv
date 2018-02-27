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

use prometheus::*;

lazy_static! {
    pub static ref KV_COMMAND_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_storage_command_total",
            "Total number of commands received.",
            &["type"]
        ).unwrap();

    pub static ref SCHED_STAGE_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_scheduler_stage_total",
            "Total number of commands on each stage.",
            &["type", "stage"]
        ).unwrap();

    pub static ref SCHED_WRITING_BYTES_GAUGE: Gauge =
        register_gauge!(
            "tikv_scheduler_writing_bytes",
            "Total number of writing kv."
        ).unwrap();

    pub static ref SCHED_CONTEX_GAUGE: Gauge =
        register_gauge!(
            "tikv_scheduler_contex_total",
            "Total number of pending commands."
        ).unwrap();

    pub static ref SCHED_HISTOGRAM_VEC: HistogramVec =
        register_histogram_vec!(
            "tikv_scheduler_command_duration_seconds",
            "Bucketed histogram of command execution",
            &["type"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref SCHED_LATCH_HISTOGRAM_VEC: HistogramVec =
        register_histogram_vec!(
            "tikv_scheduler_latch_wait_duration_seconds",
            "Bucketed histogram of latch wait",
            &["type"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref SCHED_PROCESSING_READ_HISTOGRAM_VEC: HistogramVec =
        register_histogram_vec!(
            "tikv_scheduler_processing_read_duration_seconds",
            "Bucketed histogram of processing read duration",
            &["type"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref SCHED_PROCESSING_WRITE_HISTOGRAM_VEC: HistogramVec =
        register_histogram_vec!(
            "tikv_scheduler_processing_write_duration_seconds",
            "Bucketed histogram of processing write duration",
            &["type"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref SCHED_TOO_BUSY_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_scheduler_too_busy_total",
            "Total count of scheduler too busy",
            &["type"]
        ).unwrap();

    pub static ref SCHED_COMMANDS_PRI_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_scheduler_commands_pri_total",
            "Total count of different priority commands",
            &["priority"]
        ).unwrap();

    pub static ref KV_COMMAND_KEYREAD_HISTOGRAM_VEC: HistogramVec =
        register_histogram_vec!(
            "tikv_scheduler_kv_command_key_read",
            "Bucketed histogram of keys read of a kv command",
            &["type"],
            exponential_buckets(1.0, 2.0, 21).unwrap()
        ).unwrap();

    pub static ref KV_COMMAND_SCAN_DETAILS: CounterVec =
        register_counter_vec!(
            "tikv_scheduler_kv_scan_details",
            "Bucketed counter of kv keys scan details for each cf",
            &["req","cf","tag"]
        ).unwrap();

    pub static ref RAWKV_COMMAND_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_storage_rawkv_command_total",
            "Total number of rawkv commands received.",
            &["type"]
        ).unwrap();

    pub static ref KV_COMMAND_GC_EMPTY_RANGE_COUNTER: Counter =
        register_counter!(
            "tikv_storage_gc_empty_range_total",
            "Total number of empty range found by gc"
        ).unwrap();

    pub static ref KV_COMMAND_GC_SKIPPED_COUNTER: Counter =
        register_counter!(
            "tikv_storage_gc_skipped_counter",
            "Total number of gc command skipped owing to optimization"
        ).unwrap();

    pub static ref BATCH_COMMANDS: HistogramVec =
        register_histogram_vec!(
            "tikv_storage_batch_commands_total",
            "Bucketed histogram of total number of a batch of commands",
            &["type"],
            vec![1.0, 2.0, 4.0, 6.0, 8.0, 10.0, 12.0, 14.0, 16.0, 18.0,
            20.0, 24.0, 28.0, 32.0, 48.0, 64.0, 96.0, 128.0, 192.0, 256.0]
        ).unwrap();

    pub static ref KV_COMMAND_KEYWRITE_HISTOGRAM_VEC: HistogramVec =
        register_histogram_vec!(
            "tikv_scheduler_kv_command_key_write",
            "Bucketed histogram of keys write of a kv command",
            &["type"],
            exponential_buckets(1.0, 2.0, 21).unwrap()
        ).unwrap();
}
