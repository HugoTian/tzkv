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

use std::thread;
use std::sync::mpsc::channel;
use std::time::Duration;
use fail;
use kvproto::kvrpcpb::Context;
use tikv::storage;
use tikv::storage::*;
use tikv::storage::config::Config;
use tikv::util::HandyRwLock;
use raftstore::server::new_server_cluster;
use storage::util::new_raft_engine;

#[test]
fn test_storage_1gc() {
    let _guard = ::setup();
    let snapshot_fp = "raftkv_async_snapshot_finish";
    let batch_snapshot_fp = "raftkv_async_batch_snapshot_finish";
    let (_cluster, engine, ctx) = new_raft_engine(3, "");
    let config = Config::default();
    let mut storage = Storage::from_engine(engine.clone(), &config).unwrap();
    storage.start(&config).unwrap();
    fail::cfg(snapshot_fp, "pause").unwrap();
    fail::cfg(batch_snapshot_fp, "pause").unwrap();
    let (tx1, rx1) = channel();
    storage
        .async_gc(ctx.clone(), 1, box move |res: storage::Result<()>| {
            assert!(res.is_ok());
            tx1.send(1).unwrap();
        })
        .unwrap();
    // Sleep to make sure the failpoint is triggered.
    thread::sleep(Duration::from_millis(2000));
    // Old GC command is blocked at snapshot stage, the other one will get ServerIsBusy error.
    let (tx2, rx2) = channel();
    storage
        .async_gc(Context::new(), 1, box move |res: storage::Result<()>| {
            match res {
                Err(storage::Error::SchedTooBusy) => {}
                _ => panic!("expect too busy"),
            }
            tx2.send(1).unwrap();
        })
        .unwrap();

    rx2.recv().unwrap();
    fail::remove(snapshot_fp);
    fail::remove(batch_snapshot_fp);
    rx1.recv().unwrap();
}

#[test]
fn test_scheduler_leader_change_twice() {
    let _guard = ::setup();
    let snapshot_fp = "raftkv_async_snapshot_finish";
    let mut cluster = new_server_cluster(0, 2);
    cluster.run();
    let region0 = cluster.get_region(b"");
    let peers = region0.get_peers();
    cluster.must_transfer_leader(region0.get_id(), peers[0].clone());
    let config = Config::default();

    let engine0 = cluster.sim.rl().storages[&peers[0].get_id()].clone();
    let mut storage0 = Storage::from_engine(engine0.clone(), &config).unwrap();
    storage0.start(&config).unwrap();

    let mut ctx0 = Context::new();
    ctx0.set_region_id(region0.get_id());
    ctx0.set_region_epoch(region0.get_region_epoch().clone());
    ctx0.set_peer(peers[0].clone());
    let (prewrite_tx, prewrite_rx) = channel();
    fail::cfg(snapshot_fp, "pause").unwrap();
    storage0
        .async_prewrite(
            ctx0,
            vec![Mutation::Put((make_key(b"k"), b"v".to_vec()))],
            b"k".to_vec(),
            10,
            Options::default(),
            box move |res: storage::Result<_>| match res {
                Err(storage::Error::Txn(txn::Error::Engine(engine::Error::Request(ref e))))
                | Err(storage::Error::Engine(engine::Error::Request(ref e))) => {
                    assert!(e.has_stale_command(), "{:?}", e);
                    prewrite_tx.send(false).unwrap();
                }
                Ok(_) => {
                    prewrite_tx.send(true).unwrap();
                }
                _ => {
                    panic!("expect stale command, but got {:?}", res);
                }
            },
        )
        .unwrap();
    // Sleep to make sure the failpoint is triggered.
    thread::sleep(Duration::from_millis(2000));
    // Transfer leader twice, then unblock snapshot.
    cluster.must_transfer_leader(region0.get_id(), peers[1].clone());
    cluster.must_transfer_leader(region0.get_id(), peers[0].clone());
    fail::remove(snapshot_fp);

    // the snapshot request may meet read index, scheduler will retry the request.
    let ok = prewrite_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    if ok {
        let region1 = cluster.get_region(b"");
        cluster.must_transfer_leader(region1.get_id(), peers[1].clone());

        let engine1 = cluster.sim.rl().storages[&peers[1].get_id()].clone();
        let mut storage1 = Storage::from_engine(engine1, &config).unwrap();
        storage1.start(&config).unwrap();
        let mut ctx1 = Context::new();
        ctx1.set_region_id(region1.get_id());
        ctx1.set_region_epoch(region1.get_region_epoch().clone());
        ctx1.set_peer(peers[1].clone());

        let (commit_tx, commit_rx) = channel();
        storage1
            .async_commit(
                ctx1,
                vec![make_key(b"k")],
                10,
                11,
                box move |res: storage::Result<_>| {
                    commit_tx.send(res).unwrap();
                },
            )
            .unwrap();
        // wait for the commit result.
        let res = commit_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        if res.as_ref().is_err() {
            panic!("expect Ok(_), but got {:?}", res);
        }
    }
}
