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
use std::sync::{mpsc, Arc};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use grpc::EnvBuilder;
use futures::Future;
use futures_cpupool::Builder;
use kvproto::metapb;
use kvproto::pdpb;

use tikv::pd::{validate_endpoints, Config, Error as PdError, PdClient, RegionStat, RpcClient};
use tikv::util::security::{SecurityConfig, SecurityManager};

use super::mock::mocker::*;
use super::mock::Server as MockServer;
use util;

fn new_config(eps: Vec<String>) -> Config {
    let mut cfg = Config::default();
    cfg.endpoints = eps;
    cfg
}

fn new_client(eps: Vec<String>) -> RpcClient {
    let cfg = new_config(eps);
    let mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());
    RpcClient::new(&cfg, mgr).unwrap()
}

#[test]
fn test_rpc_client() {
    let eps_count = 1;
    let se = Arc::new(Service::new());
    let server = MockServer::run::<Service>(eps_count, Arc::clone(&se), None);
    let eps: Vec<String> = server
        .bind_addrs()
        .into_iter()
        .map(|addr| format!("{}:{}", addr.0, addr.1))
        .collect();

    thread::sleep(Duration::from_secs(1));

    let client = new_client(eps.clone());
    assert_ne!(client.get_cluster_id().unwrap(), 0);

    let store_id = client.alloc_id().unwrap();
    let mut store = metapb::Store::new();
    store.set_id(store_id);
    debug!("bootstrap store {:?}", store);

    let peer_id = client.alloc_id().unwrap();
    let mut peer = metapb::Peer::new();
    peer.set_id(peer_id);
    peer.set_store_id(store_id);

    let region_id = client.alloc_id().unwrap();
    let mut region = metapb::Region::new();
    region.set_id(region_id);
    region.mut_peers().push(peer.clone());
    debug!("bootstrap region {:?}", region);

    client
        .bootstrap_cluster(store.clone(), region.clone())
        .unwrap();
    assert_eq!(client.is_cluster_bootstrapped().unwrap(), true);

    let tmp_stores = client.get_all_stores().unwrap();
    assert_eq!(tmp_stores.len(), 1);
    assert_eq!(tmp_stores[0], store);

    let tmp_store = client.get_store(store_id).unwrap();
    assert_eq!(tmp_store.get_id(), store.get_id());

    let region_key = region.get_start_key();
    let tmp_region = client.get_region(region_key).unwrap();
    assert_eq!(tmp_region.get_id(), region.get_id());

    let region_info = client.get_region_info(region_key).unwrap();
    assert_eq!(region_info.region, region);
    assert_eq!(region_info.leader, None);

    let tmp_region = client.get_region_by_id(region_id).wait().unwrap().unwrap();
    assert_eq!(tmp_region.get_id(), region.get_id());

    let mut prev_id = 0;
    for _ in 0..100 {
        let client = new_client(eps.clone());
        let alloc_id = client.alloc_id().unwrap();
        assert!(alloc_id > prev_id);
        prev_id = alloc_id;
    }

    let poller = Builder::new()
        .pool_size(1)
        .name_prefix(thd_name!("poller"))
        .create();
    let (tx, rx) = mpsc::channel();
    let f = client.handle_region_heartbeat_response(1, move |resp| {
        let _ = tx.send(resp);
    });
    poller.spawn(f).forget();
    poller
        .spawn(client.region_heartbeat(region.clone(), peer.clone(), RegionStat::default()))
        .forget();
    rx.recv_timeout(Duration::from_secs(3)).unwrap();

    let region_info = client.get_region_info(region_key).unwrap();
    assert_eq!(region_info.region, region);
    assert_eq!(region_info.leader.unwrap(), peer);

    client
        .store_heartbeat(pdpb::StoreStats::new())
        .wait()
        .unwrap();
    client.ask_split(metapb::Region::new()).wait().unwrap();
    client
        .report_split(metapb::Region::new(), metapb::Region::new())
        .wait()
        .unwrap();

    let region_info = client.get_region_info(region_key).unwrap();
    client.scatter_region(region_info).unwrap();
}

#[test]
fn test_reboot() {
    let eps_count = 1;
    let se = Arc::new(Service::new());
    let al = Arc::new(AlreadyBootstrapped);
    let server = MockServer::run(eps_count, Arc::clone(&se), Some(al));
    let eps: Vec<String> = server
        .bind_addrs()
        .into_iter()
        .map(|addr| format!("{}:{}", addr.0, addr.1))
        .collect();

    thread::sleep(Duration::from_secs(1));

    let client = new_client(eps);

    assert!(!client.is_cluster_bootstrapped().unwrap());

    match client.bootstrap_cluster(metapb::Store::new(), metapb::Region::new()) {
        Err(PdError::ClusterBootstrapped(_)) => (),
        _ => {
            panic!("failed, should return ClusterBootstrapped");
        }
    }
}

#[test]
fn test_validate_endpoints() {
    let eps_count = 3;
    let se = Arc::new(Service::new());
    let sp = Arc::new(Split::new());
    let server = MockServer::run(eps_count, se, Some(sp));
    let env = Arc::new(
        EnvBuilder::new()
            .cq_count(1)
            .name_prefix(thd_name!("test_pd"))
            .build(),
    );
    let eps: Vec<String> = server
        .bind_addrs()
        .into_iter()
        .map(|addr| format!("{}:{}", addr.0, addr.1))
        .collect();

    thread::sleep(Duration::from_secs(1));

    let mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());
    assert!(validate_endpoints(env, &new_config(eps), &mgr).is_err());
}

fn test_retry<F: Fn(&RpcClient)>(func: F) {
    let eps_count = 1;
    let se = Arc::new(Service::new());
    // Retry mocker returns `Err(_)` for most request, here two thirds are `Err(_)`.
    let retry = Arc::new(Retry::new(3));
    let server = MockServer::run(eps_count, Arc::clone(&se), Some(retry));
    let eps: Vec<String> = server
        .bind_addrs()
        .into_iter()
        .map(|addr| format!("{}:{}", addr.0, addr.1))
        .collect();

    thread::sleep(Duration::from_secs(1));

    let client = new_client(eps);

    for _ in 0..3 {
        func(&client);
    }
}

#[test]
fn test_retry_async() {
    let async = |client: &RpcClient| {
        let region = client.get_region_by_id(1);
        region.wait().unwrap();
    };
    test_retry(async);
}

#[test]
fn test_retry_sync() {
    let sync = |client: &RpcClient| {
        client.get_store(1).unwrap();
    };
    test_retry(sync)
}

#[test]
fn test_restart_leader() {
    let eps_count = 3;
    // Service has only one GetMembersResponse, so the leader never changes.
    let se = Arc::new(Service::new());
    // Start mock servers.
    let server = MockServer::run::<Service>(eps_count, Arc::clone(&se), None);
    let eps: Vec<String> = server
        .bind_addrs()
        .into_iter()
        .map(|addr| format!("{}:{}", addr.0, addr.1))
        .collect();

    thread::sleep(Duration::from_secs(2));

    let client = new_client(eps);
    // Put a region.
    let store_id = client.alloc_id().unwrap();
    let mut store = metapb::Store::new();
    store.set_id(store_id);

    let peer_id = client.alloc_id().unwrap();
    let mut peer = metapb::Peer::new();
    peer.set_id(peer_id);
    peer.set_store_id(store_id);

    let region_id = client.alloc_id().unwrap();
    let mut region = metapb::Region::new();
    region.set_id(region_id);
    region.mut_peers().push(peer);
    client
        .bootstrap_cluster(store.clone(), region.clone())
        .unwrap();

    let region = client
        .get_region_by_id(region.get_id())
        .wait()
        .unwrap()
        .unwrap();

    // Get the random binded addrs.
    let eps = server.bind_addrs();

    // Kill servers.
    drop(server);
    // Restart them again.
    let mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());
    let _server = MockServer::run_with_eps::<Service>(&mgr, eps, Arc::clone(&se), None);

    // RECONNECT_INTERVAL_SEC is 1s.
    thread::sleep(Duration::from_secs(1));

    let region = client.get_region_by_id(region.get_id());
    region.wait().unwrap();
}

// A copy of test_restart_leader with secure connections
#[test]
fn test_secure_restart_leader() {
    // Service has only one GetMembersResponse, so the leader never changes.
    let se = Arc::new(Service::new());
    let security_cfg = util::new_security_cfg();
    let mgr = Arc::new(SecurityManager::new(&security_cfg).unwrap());
    // Start mock servers.
    let eps: Vec<(String, u16)> = (0..3).map(|_| ("127.0.0.1".to_owned(), 0)).collect();
    let server = MockServer::run_with_eps::<Service>(&mgr, eps, Arc::clone(&se), None);
    let eps: Vec<String> = server
        .bind_addrs()
        .into_iter()
        .map(|addr| format!("{}:{}", addr.0, addr.1))
        .collect();

    thread::sleep(Duration::from_secs(2));

    let cfg = new_config(eps);
    let client = RpcClient::new(&cfg, Arc::clone(&mgr)).unwrap();
    // Put a region.
    let store_id = client.alloc_id().unwrap();
    let mut store = metapb::Store::new();
    store.set_id(store_id);

    let peer_id = client.alloc_id().unwrap();
    let mut peer = metapb::Peer::new();
    peer.set_id(peer_id);
    peer.set_store_id(store_id);

    let region_id = client.alloc_id().unwrap();
    let mut region = metapb::Region::new();
    region.set_id(region_id);
    region.mut_peers().push(peer);
    client
        .bootstrap_cluster(store.clone(), region.clone())
        .unwrap();

    let region = client.get_region_by_id(region_id).wait().unwrap().unwrap();
    assert_eq!(region.get_id(), region_id);

    // Get the random binded addrs.
    let eps = server.bind_addrs().into_iter().collect();

    // Kill servers.
    drop(server);
    // Restart them again.
    let _server = MockServer::run_with_eps::<Service>(&mgr, eps, Arc::clone(&se), None);

    // RECONNECT_INTERVAL_SEC is 1s.
    thread::sleep(Duration::from_secs(1));

    let region = client.get_region_by_id(region_id).wait().unwrap().unwrap();
    assert_eq!(region.get_id(), region_id);
}

#[test]
fn test_change_leader_async() {
    let eps_count = 3;
    let se = Arc::new(Service::new());
    let lc = Arc::new(LeaderChange::new());
    let server = MockServer::run(eps_count, Arc::clone(&se), Some(Arc::clone(&lc)));
    let eps: Vec<String> = server
        .bind_addrs()
        .into_iter()
        .map(|addr| format!("{}:{}", addr.0, addr.1))
        .collect();

    thread::sleep(Duration::from_secs(1));

    let counter = Arc::new(AtomicUsize::new(0));
    let client = new_client(eps);
    let counter1 = Arc::clone(&counter);
    client.handle_reconnect(move || {
        counter1.fetch_add(1, Ordering::SeqCst);
    });
    let leader = client.get_leader();

    for _ in 0..5 {
        let region = client.get_region_by_id(1);
        region.wait().ok();

        let new = client.get_leader();
        if new != leader {
            assert_eq!(1, counter.load(Ordering::SeqCst));
            return;
        }
        thread::sleep(LeaderChange::get_leader_interval());
    }

    panic!("failed, leader should changed");
}
