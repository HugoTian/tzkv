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

#![allow(stable_features)]
#![feature(mpsc_recv_timeout)]
#![feature(plugin)]
#![feature(test)]
#![cfg_attr(feature = "dev", plugin(clippy))]
#![cfg_attr(not(feature = "dev"), allow(unknown_lints))]
#![feature(btree_range, collections_bound)]
#![feature(box_syntax)]
#![allow(new_without_default)]
#![feature(const_fn)]
#![allow(needless_pass_by_value)]
#![allow(unreadable_literal)]

extern crate futures;
extern crate futures_cpupool;
extern crate grpcio as grpc;
#[cfg(feature = "mem-profiling")]
extern crate jemallocator;
extern crate kvproto;
#[macro_use]
extern crate log;
extern crate protobuf;
extern crate rand;
extern crate rocksdb;
extern crate tempdir;
extern crate test;
#[macro_use]
extern crate tikv;
extern crate tipb;
extern crate toml;
extern crate uuid;

mod raft;
mod raftstore;
mod raftstore_cases;
mod coprocessor;
mod storage;
mod storage_cases;
mod util;
mod pd;
mod config;
mod import;

use std::env;

#[test]
fn _0_ci_setup() {
    // Set up ci test fail case log.
    // The prefix "_" here is to guarantee running this case first.
    if env::var("CI").is_ok() && env::var("LOG_FILE").is_ok() {
        self::util::init_log();
    }
}

#[test]
fn _1_check_system_requirement() {
    if let Err(e) = tikv::util::config::check_max_open_fds(4096) {
        panic!(
            "To run test, please make sure the maximum number of open file descriptors not \
             less than 2000: {:?}",
            e
        );
    }
}
