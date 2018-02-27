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

#![feature(slice_patterns)]
#![feature(box_syntax)]
#![feature(test)]
#![cfg_attr(feature = "dev", feature(plugin))]
#![cfg_attr(feature = "dev", plugin(clippy))]
#![cfg_attr(not(feature = "dev"), allow(unknown_lints))]
#![cfg_attr(feature = "no-fail", allow(dead_code))]
#![recursion_limit = "100"]
#![allow(module_inception)]
#![allow(should_implement_trait)]
#![allow(large_enum_variant)]
#![allow(needless_pass_by_value)]
#![allow(unreadable_literal)]
#![allow(new_without_default_derive)]
#![allow(verbose_bit_mask)]

extern crate fail;
extern crate futures;
extern crate futures_cpupool;
extern crate grpcio as grpc;
extern crate kvproto;
#[macro_use]
extern crate lazy_static;
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

#[allow(dead_code)]
mod raftstore;
#[allow(dead_code)]
mod storage;
#[allow(dead_code)]
mod util;
#[cfg(not(feature = "no-fail"))]
mod failpoints_cases;

use std::sync::*;
use std::{env, thread};

use tikv::util::panic_hook;

lazy_static! {
    /// Failpoints are global structs, hence rules set in different cases
    /// may affect each other. So use a global lock to synchronize them.
    static ref LOCK: Mutex<()> = {
        // Set up ci test fail case log.
        if env::var("CI").is_ok() && env::var("LOG_FILE").is_ok() {
            self::util::init_log();
        }

        Mutex::new(())
    };
}

fn setup<'a>() -> MutexGuard<'a, ()> {
    // We don't want a failed test breaks others.
    let guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    fail::teardown();
    fail::setup();
    guard
}

#[test]
fn test_setup() {
    let _ = thread::spawn(move || {
        panic_hook::mute();
        let _g = setup();
        panic!("Poison!");
    }).join();

    let _g = setup();
}
