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

use std::sync::{Arc, RwLock};
use std::sync::mpsc::Sender;
use kvproto::raft_serverpb::RaftMessage;
use kvproto::raft_cmdpb::RaftCmdRequest;

use util::transport::SendCh;
use util::HandyRwLock;
use util::worker::Scheduler;
use util::collections::HashSet;
use raft::SnapshotStatus;
use raftstore::store::{BatchReadCallback, Callback, Msg as StoreMsg, SignificantMsg, Transport};
use raftstore::Result as RaftStoreResult;
use server::raft_client::RaftClient;
use server::Result;
use super::snap::Task as SnapTask;
use super::resolve::StoreAddrResolver;
use super::metrics::*;

pub trait RaftStoreRouter: Send + Clone {
    /// Send StoreMsg, retry if failed. Try times may vary from implementation.
    fn send(&self, msg: StoreMsg) -> RaftStoreResult<()>;

    /// Send StoreMsg.
    fn try_send(&self, msg: StoreMsg) -> RaftStoreResult<()>;

    // Send RaftMessage to local store.
    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        self.try_send(StoreMsg::RaftMessage(msg))
    }

    // Send RaftCmdRequest to local store.
    fn send_command(&self, req: RaftCmdRequest, cb: Callback) -> RaftStoreResult<()> {
        self.try_send(StoreMsg::new_raft_cmd(req, cb))
    }

    // Send a batch of RaftCmdRequests to local store.
    fn send_batch_commands(
        &self,
        batch: Vec<RaftCmdRequest>,
        on_finished: BatchReadCallback,
    ) -> RaftStoreResult<()> {
        self.try_send(StoreMsg::new_batch_raft_snapshot_cmd(batch, on_finished))
    }

    // Send significant message. We should guarantee that the message can't be dropped.
    fn significant_send(&self, msg: SignificantMsg) -> RaftStoreResult<()>;

    // Report the peer of the region is unreachable.
    fn report_unreachable(&self, region_id: u64, to_peer_id: u64) -> RaftStoreResult<()> {
        self.significant_send(SignificantMsg::Unreachable {
            region_id,
            to_peer_id,
        })
    }

    // Report the sending snapshot status to the peer of the region.
    fn report_snapshot_status(
        &self,
        region_id: u64,
        to_peer_id: u64,
        status: SnapshotStatus,
    ) -> RaftStoreResult<()> {
        self.significant_send(SignificantMsg::SnapshotStatus {
            region_id: region_id,
            to_peer_id: to_peer_id,
            status: status,
        })
    }
}

#[derive(Clone)]
pub struct ServerRaftStoreRouter {
    pub ch: SendCh<StoreMsg>,
    pub significant_msg_sender: Sender<SignificantMsg>,
}

impl ServerRaftStoreRouter {
    pub fn new(
        ch: SendCh<StoreMsg>,
        significant_msg_sender: Sender<SignificantMsg>,
    ) -> ServerRaftStoreRouter {
        ServerRaftStoreRouter {
            ch: ch,
            significant_msg_sender: significant_msg_sender,
        }
    }
}

impl RaftStoreRouter for ServerRaftStoreRouter {
    fn try_send(&self, msg: StoreMsg) -> RaftStoreResult<()> {
        self.ch.try_send(msg)?;
        Ok(())
    }

    fn send(&self, msg: StoreMsg) -> RaftStoreResult<()> {
        self.ch.send(msg)?;
        Ok(())
    }

    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        self.try_send(StoreMsg::RaftMessage(msg))
    }

    fn send_command(&self, req: RaftCmdRequest, cb: Callback) -> RaftStoreResult<()> {
        self.try_send(StoreMsg::new_raft_cmd(req, cb))
    }

    fn send_batch_commands(
        &self,
        batch: Vec<RaftCmdRequest>,
        on_finished: BatchReadCallback,
    ) -> RaftStoreResult<()> {
        self.try_send(StoreMsg::new_batch_raft_snapshot_cmd(batch, on_finished))
    }

    fn significant_send(&self, msg: SignificantMsg) -> RaftStoreResult<()> {
        if let Err(e) = self.significant_msg_sender.send(msg) {
            return Err(box_err!("failed to sendsignificant msg {:?}", e));
        }

        Ok(())
    }
}

pub struct ServerTransport<T, S>
where
    T: RaftStoreRouter + 'static,
    S: StoreAddrResolver + 'static,
{
    raft_client: Arc<RwLock<RaftClient>>,
    snap_scheduler: Scheduler<SnapTask>,
    pub raft_router: T,
    resolving: Arc<RwLock<HashSet<u64>>>,
    resolver: S,
}

impl<T, S> Clone for ServerTransport<T, S>
where
    T: RaftStoreRouter + 'static,
    S: StoreAddrResolver + 'static,
{
    fn clone(&self) -> Self {
        ServerTransport {
            raft_client: Arc::clone(&self.raft_client),
            snap_scheduler: self.snap_scheduler.clone(),
            raft_router: self.raft_router.clone(),
            resolving: Arc::clone(&self.resolving),
            resolver: self.resolver.clone(),
        }
    }
}

impl<T: RaftStoreRouter + 'static, S: StoreAddrResolver + 'static> ServerTransport<T, S> {
    pub fn new(
        raft_client: Arc<RwLock<RaftClient>>,
        snap_scheduler: Scheduler<SnapTask>,
        raft_router: T,
        resolver: S,
    ) -> ServerTransport<T, S> {
        ServerTransport {
            raft_client: raft_client,
            snap_scheduler: snap_scheduler,
            raft_router: raft_router,
            resolving: Arc::new(RwLock::new(Default::default())),
            resolver: resolver,
        }
    }

    fn send_store(&self, store_id: u64, msg: RaftMessage) {
        // check the corresponding token for store.
        // TODO: avoid clone
        let addr = self.raft_client.rl().addrs.get(&store_id).cloned();
        if let Some(addr) = addr {
            self.write_data(store_id, &addr, msg);
            return;
        }

        // No connection, try to resolve it.
        if self.resolving.rl().contains(&store_id) {
            RESOLVE_STORE_COUNTER
                .with_label_values(&["resolving"])
                .inc();
            // If we are resolving the address, drop the message here.
            debug!(
                "store {} address is being resolved, drop msg {:?}",
                store_id, msg
            );
            self.report_unreachable(msg);
            return;
        }

        debug!("begin to resolve store {} address", store_id);
        RESOLVE_STORE_COUNTER.with_label_values(&["resolve"]).inc();

        self.resolving.wl().insert(store_id);
        self.resolve(store_id, msg);
    }

    fn resolve(&self, store_id: u64, msg: RaftMessage) {
        let trans = self.clone();
        let msg1 = msg.clone();
        let cb = box move |addr: Result<String>| {
            // clear resolving.
            trans.resolving.wl().remove(&store_id);

            if let Err(e) = addr {
                RESOLVE_STORE_COUNTER.with_label_values(&["failed"]).inc();
                debug!("resolve store {} address failed {:?}", store_id, e);
                trans.report_unreachable(msg);
                return;
            }

            RESOLVE_STORE_COUNTER.with_label_values(&["success"]).inc();
            let addr = addr.unwrap();
            info!("resolve store {} address ok, addr {}", store_id, addr);
            trans.raft_client.wl().addrs.insert(store_id, addr.clone());
            trans.write_data(store_id, &addr, msg);
            // There may be no messages in the near future, so flush it immediately.
            trans.raft_client.wl().flush();
        };
        if let Err(e) = self.resolver.resolve(store_id, cb) {
            error!("try to resolve err {:?}", e);
            self.resolving.wl().remove(&store_id);
            RESOLVE_STORE_COUNTER.with_label_values(&["failed"]).inc();
            debug!("resolve store {} address failed {:?}", store_id, e);
            self.report_unreachable(msg1);
        }
    }

    fn write_data(&self, store_id: u64, addr: &str, msg: RaftMessage) {
        if msg.get_message().has_snapshot() {
            return self.send_snapshot_sock(addr, msg);
        }
        if let Err(e) = self.raft_client.wl().send(store_id, addr, msg) {
            error!("send raft msg err {:?}", e);
        }
    }

    fn send_snapshot_sock(&self, addr: &str, msg: RaftMessage) {
        let rep = self.new_snapshot_reporter(&msg);
        let cb = box move |res: Result<()>| {
            if res.is_err() {
                rep.report(SnapshotStatus::Failure);
            } else {
                rep.report(SnapshotStatus::Finish);
            }
        };
        if let Err(e) = self.snap_scheduler.schedule(SnapTask::SendTo {
            addr: addr.to_owned(),
            msg: msg,
            cb: cb,
        }) {
            if let SnapTask::SendTo { cb, .. } = e.into_inner() {
                error!(
                    "channel is unavaliable, failed to schedule snapshot to {}",
                    addr
                );
                cb(Err(box_err!("failed to schedule snapshot")));
            }
        }
    }

    fn new_snapshot_reporter(&self, msg: &RaftMessage) -> SnapshotReporter<T> {
        let region_id = msg.get_region_id();
        let to_peer_id = msg.get_to_peer().get_id();
        let to_store_id = msg.get_to_peer().get_store_id();

        SnapshotReporter {
            raft_router: self.raft_router.clone(),
            region_id: region_id,
            to_peer_id: to_peer_id,
            to_store_id: to_store_id,
        }
    }

    pub fn report_unreachable(&self, msg: RaftMessage) {
        let region_id = msg.get_region_id();
        let to_peer_id = msg.get_to_peer().get_id();
        let store_id = msg.get_to_peer().get_store_id();

        if let Err(e) = self.raft_router.report_unreachable(region_id, to_peer_id) {
            error!(
                "report peer {} on store {} unreachable for region {} failed {:?}",
                to_peer_id, store_id, region_id, e
            );
        }
    }

    pub fn flush_raft_client(&mut self) {
        self.raft_client.wl().flush();
    }
}

impl<T, S> Transport for ServerTransport<T, S>
where
    T: RaftStoreRouter + 'static,
    S: StoreAddrResolver + 'static,
{
    fn send(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        let to_store_id = msg.get_to_peer().get_store_id();
        self.send_store(to_store_id, msg);
        Ok(())
    }

    fn flush(&mut self) {
        self.flush_raft_client();
    }
}

struct SnapshotReporter<T: RaftStoreRouter + 'static> {
    raft_router: T,
    region_id: u64,
    to_peer_id: u64,
    to_store_id: u64,
}

impl<T: RaftStoreRouter + 'static> SnapshotReporter<T> {
    pub fn report(&self, status: SnapshotStatus) {
        debug!(
            "send snapshot to {} for {} {:?}",
            self.to_peer_id, self.region_id, status
        );

        if status == SnapshotStatus::Failure {
            let store = self.to_store_id.to_string();
            REPORT_FAILURE_MSG_COUNTER
                .with_label_values(&["snapshot", &*store])
                .inc();
        };

        if let Err(e) =
            self.raft_router
                .report_snapshot_status(self.region_id, self.to_peer_id, status)
        {
            error!(
                "report snapshot to peer {} in store {} with region {} err {:?}",
                self.to_peer_id, self.to_store_id, self.region_id, e
            );
        }
    }
}
