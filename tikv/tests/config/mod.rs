// Copyright 2017 PingCAP, Inc.
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

use std::path::PathBuf;
use std::io::Read;
use std::fs::File;

use log::LogLevelFilter;
use rocksdb::{CompactionPriority, DBCompressionType, DBRecoveryMode};
use tikv::pd::Config as PdConfig;
use tikv::server::Config as ServerConfig;
use tikv::server::readpool::Config as ReadPoolConfig;
use tikv::raftstore::store::Config as RaftstoreConfig;
use tikv::raftstore::coprocessor::Config as CopConfig;
use tikv::config::*;
use tikv::storage::Config as StorageConfig;
use tikv::import::Config as ImportConfig;
use tikv::util::config::{ReadableDuration, ReadableSize};
use tikv::util::security::SecurityConfig;

use toml;

#[test]
fn test_toml_serde() {
    let value = TiKvConfig::default();
    let dump = toml::to_string_pretty(&value).unwrap();
    let load = toml::from_str(&dump).unwrap();
    assert_eq!(value, load);
}

// Read a file in project directory. It is similar to `include_str!`,
// but `include_str!` a large string literal increases compile time.
// See more: https://github.com/rust-lang/rust/issues/39352
fn read_file_in_project_dir(path: &str) -> String {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push(path);
    let mut f = File::open(p).unwrap();
    let mut buffer = String::new();
    f.read_to_string(&mut buffer).unwrap();
    buffer
}

#[test]
fn test_serde_custom_tikv_config() {
    let mut value = TiKvConfig::default();
    value.log_level = LogLevelFilter::Debug;
    value.log_file = "foo".to_owned();
    value.server = ServerConfig {
        cluster_id: 0, // KEEP IT ZERO, it is skipped by serde.
        addr: "example.com:443".to_owned(),
        labels: map!{ "a".to_owned() => "b".to_owned() },
        advertise_addr: "example.com:443".to_owned(),
        notify_capacity: 12_345,
        messages_per_tick: 123,
        grpc_concurrency: 123,
        grpc_concurrent_stream: 1_234,
        grpc_raft_conn_num: 123,
        grpc_stream_initial_window_size: ReadableSize(12_345),
        end_point_concurrency: 12,
        end_point_max_tasks: 12,
        end_point_stack_size: ReadableSize::mb(12),
        end_point_recursion_limit: 100,
        end_point_batch_row_limit: 64,
        end_point_request_max_handle_duration: ReadableDuration::secs(12),
        snap_max_write_bytes_per_sec: ReadableSize::mb(10),
        snap_max_total_size: ReadableSize::gb(10),
    };
    value.readpool = ReadPoolConfig {
        high_concurrency: 1,
        normal_concurrency: 3,
        low_concurrency: 7,
        max_tasks_high: 10000,
        max_tasks_normal: 20000,
        max_tasks_low: 30000,
        stack_size: ReadableSize::mb(20),
    };
    value.metric = MetricConfig {
        interval: ReadableDuration::secs(12),
        address: "example.com:443".to_owned(),
        job: "tikv_1".to_owned(),
    };
    value.raft_store = RaftstoreConfig {
        sync_log: false,
        raftdb_path: "/var".to_owned(),
        capacity: ReadableSize(123),
        raft_base_tick_interval: ReadableDuration::secs(12),
        raft_heartbeat_ticks: 1,
        raft_election_timeout_ticks: 12,
        raft_max_size_per_msg: ReadableSize::mb(12),
        raft_max_inflight_msgs: 123,
        raft_entry_max_size: ReadableSize::mb(12),
        raft_log_gc_tick_interval: ReadableDuration::secs(12),
        raft_log_gc_threshold: 12,
        raft_log_gc_count_limit: 12,
        raft_log_gc_size_limit: ReadableSize::kb(1),
        split_region_check_tick_interval: ReadableDuration::secs(12),
        region_split_check_diff: ReadableSize::mb(6),
        region_compact_check_interval: ReadableDuration::secs(12),
        region_compact_delete_keys_count: 1_234,
        pd_heartbeat_tick_interval: ReadableDuration::minutes(12),
        pd_store_heartbeat_tick_interval: ReadableDuration::secs(12),
        notify_capacity: 12_345,
        snap_mgr_gc_tick_interval: ReadableDuration::minutes(12),
        snap_gc_timeout: ReadableDuration::hours(12),
        messages_per_tick: 12_345,
        max_peer_down_duration: ReadableDuration::minutes(12),
        max_leader_missing_duration: ReadableDuration::hours(12),
        abnormal_leader_missing_duration: ReadableDuration::hours(6),
        snap_apply_batch_size: ReadableSize::mb(12),
        lock_cf_compact_interval: ReadableDuration::minutes(12),
        lock_cf_compact_bytes_threshold: ReadableSize::mb(123),
        consistency_check_interval: ReadableDuration::secs(12),
        report_region_flow_interval: ReadableDuration::minutes(12),
        raft_store_max_leader_lease: ReadableDuration::secs(12),
        right_derive_when_split: false,
        allow_remove_leader: true,
        use_delete_range: true,
        region_max_size: ReadableSize(0),
        region_split_size: ReadableSize(0),
    };
    value.pd = PdConfig {
        endpoints: vec!["example.com:443".to_owned()],
    };
    value.rocksdb = DbConfig {
        wal_recovery_mode: DBRecoveryMode::AbsoluteConsistency,
        wal_dir: "/var".to_owned(),
        wal_ttl_seconds: 1,
        wal_size_limit: ReadableSize::kb(1),
        max_total_wal_size: ReadableSize::gb(1),
        max_background_jobs: 12,
        max_manifest_file_size: ReadableSize::mb(12),
        create_if_missing: false,
        max_open_files: 12_345,
        enable_statistics: false,
        stats_dump_period: ReadableDuration::minutes(12),
        compaction_readahead_size: ReadableSize::kb(1),
        info_log_max_size: ReadableSize::kb(1),
        info_log_roll_time: ReadableDuration::secs(12),
        info_log_dir: "/var".to_owned(),
        rate_bytes_per_sec: ReadableSize::kb(1),
        bytes_per_sync: ReadableSize::mb(1),
        wal_bytes_per_sync: ReadableSize::kb(32),
        max_sub_compactions: 12,
        writable_file_max_buffer_size: ReadableSize::mb(12),
        use_direct_io_for_flush_and_compaction: true,
        enable_pipelined_write: false,
        defaultcf: DefaultCfConfig {
            block_size: ReadableSize::kb(12),
            block_cache_size: ReadableSize::gb(12),
            disable_block_cache: false,
            cache_index_and_filter_blocks: false,
            pin_l0_filter_and_index_blocks: false,
            use_bloom_filter: false,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 123,
            block_based_bloom_filter: true,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Lz4,
            ],
            write_buffer_size: ReadableSize::mb(1),
            max_write_buffer_number: 12,
            min_write_buffer_number_to_merge: 12,
            max_bytes_for_level_base: ReadableSize::kb(12),
            target_file_size_base: ReadableSize::kb(123),
            level0_file_num_compaction_trigger: 123,
            level0_slowdown_writes_trigger: 123,
            level0_stop_writes_trigger: 123,
            max_compaction_bytes: ReadableSize::gb(1),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 4,
            max_bytes_for_level_multiplier: 8,
        },
        writecf: WriteCfConfig {
            block_size: ReadableSize::kb(12),
            block_cache_size: ReadableSize::gb(12),
            disable_block_cache: false,
            cache_index_and_filter_blocks: false,
            pin_l0_filter_and_index_blocks: false,
            use_bloom_filter: false,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 123,
            block_based_bloom_filter: true,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Lz4,
            ],
            write_buffer_size: ReadableSize::mb(1),
            max_write_buffer_number: 12,
            min_write_buffer_number_to_merge: 12,
            max_bytes_for_level_base: ReadableSize::kb(12),
            target_file_size_base: ReadableSize::kb(123),
            level0_file_num_compaction_trigger: 123,
            level0_slowdown_writes_trigger: 123,
            level0_stop_writes_trigger: 123,
            max_compaction_bytes: ReadableSize::gb(1),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 4,
            max_bytes_for_level_multiplier: 8,
        },
        lockcf: LockCfConfig {
            block_size: ReadableSize::kb(12),
            block_cache_size: ReadableSize::gb(12),
            disable_block_cache: false,
            cache_index_and_filter_blocks: false,
            pin_l0_filter_and_index_blocks: false,
            use_bloom_filter: false,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 123,
            block_based_bloom_filter: true,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Lz4,
            ],
            write_buffer_size: ReadableSize::mb(1),
            max_write_buffer_number: 12,
            min_write_buffer_number_to_merge: 12,
            max_bytes_for_level_base: ReadableSize::kb(12),
            target_file_size_base: ReadableSize::kb(123),
            level0_file_num_compaction_trigger: 123,
            level0_slowdown_writes_trigger: 123,
            level0_stop_writes_trigger: 123,
            max_compaction_bytes: ReadableSize::gb(1),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 4,
            max_bytes_for_level_multiplier: 8,
        },
        raftcf: RaftCfConfig {
            block_size: ReadableSize::kb(12),
            block_cache_size: ReadableSize::gb(12),
            disable_block_cache: false,
            cache_index_and_filter_blocks: false,
            pin_l0_filter_and_index_blocks: false,
            use_bloom_filter: false,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 123,
            block_based_bloom_filter: true,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Lz4,
            ],
            write_buffer_size: ReadableSize::mb(1),
            max_write_buffer_number: 12,
            min_write_buffer_number_to_merge: 12,
            max_bytes_for_level_base: ReadableSize::kb(12),
            target_file_size_base: ReadableSize::kb(123),
            level0_file_num_compaction_trigger: 123,
            level0_slowdown_writes_trigger: 123,
            level0_stop_writes_trigger: 123,
            max_compaction_bytes: ReadableSize::gb(1),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 4,
            max_bytes_for_level_multiplier: 8,
        },
    };
    value.raftdb = RaftDbConfig {
        wal_recovery_mode: DBRecoveryMode::SkipAnyCorruptedRecords,
        wal_dir: "/var".to_owned(),
        wal_ttl_seconds: 1,
        wal_size_limit: ReadableSize::kb(12),
        max_total_wal_size: ReadableSize::gb(1),
        max_manifest_file_size: ReadableSize::mb(12),
        create_if_missing: false,
        max_open_files: 12_345,
        enable_statistics: false,
        stats_dump_period: ReadableDuration::minutes(12),
        compaction_readahead_size: ReadableSize::kb(1),
        info_log_max_size: ReadableSize::kb(1),
        info_log_roll_time: ReadableDuration::secs(1),
        info_log_dir: "/var".to_owned(),
        max_sub_compactions: 12,
        writable_file_max_buffer_size: ReadableSize::mb(12),
        use_direct_io_for_flush_and_compaction: true,
        enable_pipelined_write: false,
        allow_concurrent_memtable_write: true,
        bytes_per_sync: ReadableSize::mb(1),
        wal_bytes_per_sync: ReadableSize::kb(32),
        defaultcf: RaftDefaultCfConfig {
            block_size: ReadableSize::kb(12),
            block_cache_size: ReadableSize::gb(12),
            disable_block_cache: false,
            cache_index_and_filter_blocks: false,
            pin_l0_filter_and_index_blocks: false,
            use_bloom_filter: false,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 123,
            block_based_bloom_filter: true,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Lz4,
            ],
            write_buffer_size: ReadableSize::mb(1),
            max_write_buffer_number: 12,
            min_write_buffer_number_to_merge: 12,
            max_bytes_for_level_base: ReadableSize::kb(12),
            target_file_size_base: ReadableSize::kb(123),
            level0_file_num_compaction_trigger: 123,
            level0_slowdown_writes_trigger: 123,
            level0_stop_writes_trigger: 123,
            max_compaction_bytes: ReadableSize::gb(1),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 4,
            max_bytes_for_level_multiplier: 8,
        },
    };
    value.storage = StorageConfig {
        data_dir: "/var".to_owned(),
        gc_ratio_threshold: 1.2,
        max_key_size: 8192,
        scheduler_notify_capacity: 123,

        scheduler_messages_per_tick: 123,
        scheduler_concurrency: 123,
        scheduler_worker_pool_size: 1,
        scheduler_pending_write_threshold: ReadableSize::kb(123),
    };
    value.coprocessor = CopConfig {
        split_region_on_table: true,
        region_max_size: ReadableSize::mb(12),
        region_split_size: ReadableSize::mb(12),
    };
    value.security = SecurityConfig {
        ca_path: "invalid path".to_owned(),
        cert_path: "invalid path".to_owned(),
        key_path: "invalid path".to_owned(),
        override_ssl_target: "".to_owned(),
    };
    value.import = ImportConfig {
        num_threads: 123,
        stream_channel_window: 123,
    };

    let custom = read_file_in_project_dir("tests/config/test-custom.toml");
    let load = toml::from_str(&custom).unwrap();
    assert_eq!(value, load);
    let dump = toml::to_string_pretty(&load).unwrap();
    assert_eq!(dump, custom);
}
