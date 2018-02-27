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

use tikv::storage::Engine;
use tikv::storage::config::Config;
use kvproto::kvrpcpb::Context;
use raftstore::cluster::Cluster;
use raftstore::server::ServerCluster;
use raftstore::server::new_server_cluster;
use tikv::util::HandyRwLock;
use super::sync_storage::SyncStorage;

pub fn new_raft_engine(count: usize, key: &str) -> (Cluster<ServerCluster>, Box<Engine>, Context) {
    let mut cluster = new_server_cluster(0, count);
    cluster.run();
    // make sure leader has been elected.
    assert_eq!(cluster.must_get(b""), None);
    let region = cluster.get_region(key.as_bytes());
    let leader = cluster.leader_of_region(region.get_id()).unwrap();
    let engine = cluster.sim.rl().storages[&leader.get_id()].clone();
    let mut ctx = Context::new();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(leader.clone());
    (cluster, engine, ctx)
}

pub fn new_raft_storage_with_store_count(
    count: usize,
    key: &str,
) -> (Cluster<ServerCluster>, SyncStorage, Context) {
    let (cluster, engine, ctx) = new_raft_engine(count, key);
    (
        cluster,
        SyncStorage::from_engine(engine, &Config::default()),
        ctx,
    )
}
