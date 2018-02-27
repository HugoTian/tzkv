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

use sys_info;

use util::collections::HashMap;
use util::config::{self, ReadableDuration, ReadableSize};
use coprocessor::DEFAULT_REQUEST_MAX_HANDLE_SECS;
use util::io_limiter::DEFAULT_SNAP_MAX_BYTES_PER_SEC;
use super::Result;

pub use raftstore::store::Config as RaftStoreConfig;
pub use storage::Config as StorageConfig;

pub const DEFAULT_CLUSTER_ID: u64 = 0;
pub const DEFAULT_LISTENING_ADDR: &str = "127.0.0.1:20160";
const DEFAULT_ADVERTISE_LISTENING_ADDR: &str = "";
const DEFAULT_NOTIFY_CAPACITY: usize = 40960;
const DEFAULT_GRPC_CONCURRENCY: usize = 4;
const DEFAULT_GRPC_CONCURRENT_STREAM: usize = 1024;
const DEFAULT_GRPC_RAFT_CONN_NUM: usize = 10;
const DEFAULT_GRPC_STREAM_INITIAL_WINDOW_SIZE: u64 = 2 * 1024 * 1024;
const DEFAULT_MESSAGES_PER_TICK: usize = 4096;
// Enpoints may occur very deep recursion,
// so enlarge their stack size to 10 MB.
const DEFAULT_ENDPOINT_STACK_SIZE_MB: u64 = 10;

// Assume a request can be finished in 1ms, a request at position x will wait about
// 0.001 * x secs to be actual started. A server-is-busy error will trigger 2 seconds
// backoff. So when it needs to wait for more than 2 seconds, return error won't causse
// larger latency.
pub const DEFAULT_MAX_RUNNING_TASK_COUNT: usize = 2 as usize * 1000;

// Number of rows in each chunk.
pub const DEFAULT_ENDPOINT_BATCH_ROW_LIMIT: usize = 64;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    #[serde(skip)] pub cluster_id: u64,

    // Server listening address.
    pub addr: String,

    // Server advertise listening address for outer communication.
    // If not set, we will use listening address instead.
    pub advertise_addr: String,
    pub notify_capacity: usize,
    pub messages_per_tick: usize,
    pub grpc_concurrency: usize,
    pub grpc_concurrent_stream: usize,
    pub grpc_raft_conn_num: usize,
    pub grpc_stream_initial_window_size: ReadableSize,
    pub end_point_concurrency: usize,
    pub end_point_max_tasks: usize,
    pub end_point_stack_size: ReadableSize,
    pub end_point_recursion_limit: u32,
    pub end_point_batch_row_limit: usize,
    pub end_point_request_max_handle_duration: ReadableDuration,
    pub snap_max_write_bytes_per_sec: ReadableSize,
    pub snap_max_total_size: ReadableSize,

    // Server labels to specify some attributes about this server.
    #[serde(with = "config::order_map_serde")] pub labels: HashMap<String, String>,
}

impl Default for Config {
    fn default() -> Config {
        let cpu_num = sys_info::cpu_num().unwrap();
        let concurrency = if cpu_num > 8 {
            (f64::from(cpu_num) * 0.8) as usize
        } else {
            4
        };
        Config {
            cluster_id: DEFAULT_CLUSTER_ID,
            addr: DEFAULT_LISTENING_ADDR.to_owned(),
            labels: HashMap::default(),
            advertise_addr: DEFAULT_ADVERTISE_LISTENING_ADDR.to_owned(),
            notify_capacity: DEFAULT_NOTIFY_CAPACITY,
            messages_per_tick: DEFAULT_MESSAGES_PER_TICK,
            grpc_concurrency: DEFAULT_GRPC_CONCURRENCY,
            grpc_concurrent_stream: DEFAULT_GRPC_CONCURRENT_STREAM,
            grpc_raft_conn_num: DEFAULT_GRPC_RAFT_CONN_NUM,
            grpc_stream_initial_window_size: ReadableSize(DEFAULT_GRPC_STREAM_INITIAL_WINDOW_SIZE),
            end_point_concurrency: concurrency,
            end_point_max_tasks: DEFAULT_MAX_RUNNING_TASK_COUNT,
            end_point_stack_size: ReadableSize::mb(DEFAULT_ENDPOINT_STACK_SIZE_MB),
            end_point_recursion_limit: 1000,
            end_point_batch_row_limit: DEFAULT_ENDPOINT_BATCH_ROW_LIMIT,
            end_point_request_max_handle_duration: ReadableDuration::secs(
                DEFAULT_REQUEST_MAX_HANDLE_SECS,
            ),
            snap_max_write_bytes_per_sec: ReadableSize(DEFAULT_SNAP_MAX_BYTES_PER_SEC),
            snap_max_total_size: ReadableSize(0),
        }
    }
}

impl Config {
    pub fn validate(&mut self) -> Result<()> {
        box_try!(config::check_addr(&self.addr));
        if !self.advertise_addr.is_empty() {
            box_try!(config::check_addr(&self.advertise_addr));
        } else {
            info!("no advertise-addr is specified, fall back to addr.");
            self.advertise_addr = self.addr.clone();
        }
        if self.advertise_addr.starts_with("0.") {
            return Err(box_err!(
                "invalid advertise-addr: {:?}",
                self.advertise_addr
            ));
        }

        if self.end_point_concurrency == 0 {
            return Err(box_err!("server.end-point-concurrency should not be 0."));
        }

        if self.end_point_max_tasks == 0 {
            return Err(box_err!("server.end-point-max-tasks should not be 0."));
        }

        // 2MB is the default stack size for threads in rust, but endpoints may occur
        // very deep recursion, 2MB considered too small.
        //
        // See more: https://doc.rust-lang.org/std/thread/struct.Builder.html#method.stack_size
        if self.end_point_stack_size.0 < ReadableSize::mb(2).0 {
            return Err(box_err!("server.end-point-stack-size is too small."));
        }

        if self.end_point_recursion_limit < 100 {
            return Err(box_err!("server.end-point-recursion-limit is too small"));
        }

        if self.end_point_request_max_handle_duration.as_secs() < DEFAULT_REQUEST_MAX_HANDLE_SECS {
            return Err(box_err!(
                "server.end-point-request-max-handle-secs is too small."
            ));
        }

        for (k, v) in &self.labels {
            validate_label(k, "key")?;
            validate_label(v, "value")?;
        }

        Ok(())
    }
}

fn validate_label(s: &str, tp: &str) -> Result<()> {
    let report_err = || {
        box_err!(
            "store label {}: {:?} not match ^[a-zA-Z0-9]([a-zA-Z0-9-._]*[a-zA-Z0-9])?",
            tp,
            s
        )
    };
    if s.is_empty() {
        return Err(report_err());
    }
    let mut chrs = s.chars();
    let first_char = chrs.next().unwrap();
    if !first_char.is_ascii_alphanumeric() {
        return Err(report_err());
    }
    let last_char = match chrs.next_back() {
        None => return Ok(()),
        Some(c) => c,
    };
    if !last_char.is_ascii_alphanumeric() {
        return Err(report_err());
    }
    for c in chrs {
        if !c.is_ascii_alphanumeric() && !"-._".contains(c) {
            return Err(report_err());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use util::config::ReadableDuration;

    #[test]
    fn test_config_validate() {
        let mut cfg = Config::default();
        assert!(cfg.advertise_addr.is_empty());
        cfg.validate().unwrap();
        assert_eq!(cfg.addr, cfg.advertise_addr);

        let mut invalid_cfg = cfg.clone();
        invalid_cfg.end_point_concurrency = 0;
        assert!(invalid_cfg.validate().is_err());

        let mut invalid_cfg = cfg.clone();
        invalid_cfg.end_point_stack_size = ReadableSize::mb(1);
        assert!(invalid_cfg.validate().is_err());

        let mut invalid_cfg = cfg.clone();
        invalid_cfg.end_point_max_tasks = 0;
        assert!(invalid_cfg.validate().is_err());

        let mut invalid_cfg = cfg.clone();
        invalid_cfg.end_point_recursion_limit = 0;
        assert!(invalid_cfg.validate().is_err());

        let mut invalid_cfg = cfg.clone();
        invalid_cfg.end_point_request_max_handle_duration = ReadableDuration::secs(0);
        assert!(invalid_cfg.validate().is_err());

        invalid_cfg = Config::default();
        invalid_cfg.addr = "0.0.0.0:1000".to_owned();
        assert!(invalid_cfg.validate().is_err());
        invalid_cfg.advertise_addr = "127.0.0.1:1000".to_owned();
        invalid_cfg.validate().unwrap();

        cfg.labels.insert("k1".to_owned(), "v1".to_owned());
        cfg.validate().unwrap();
        cfg.labels.insert("k2".to_owned(), "v2?".to_owned());
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_store_labels() {
        let invalid_cases = vec!["", "123*", ".123", "💖"];

        for case in invalid_cases {
            assert!(validate_label(case, "dummy").is_err());
        }

        let valid_cases = vec![
            "a", "0", "a.1-2", "Cab", "abC", "b_1.2", "cab-012", "3ac.8b2"
        ];

        for case in valid_cases {
            validate_label(case, "dummy").unwrap();
        }
    }
}
