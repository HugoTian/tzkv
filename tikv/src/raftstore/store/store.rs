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

use std::sync::Arc;
use std::sync::mpsc::{self, Receiver as StdReceiver, TryRecvError};
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::Bound::{Excluded, Included, Unbounded};
use std::time::{Duration, Instant};
use std::thread;
use std::u64;

use rocksdb::{CompactionJobInfo, WriteBatch, DB};
use rocksdb::rocksdb_options::WriteOptions;
use mio::{self, EventLoop, EventLoopConfig, Sender};
use protobuf;
use time::{self, Timespec};

use kvproto::raft_serverpb::{PeerState, RaftMessage, RaftSnapshotData, RaftTruncatedState,
                             RegionLocalState};
use kvproto::eraftpb::{ConfChangeType, MessageType};
use kvproto::pdpb::StoreStats;
use util::{escape, rocksdb};
use util::time::{duration_to_sec, SlowTimer};
use pd::{PdClient, PdRunner, PdTask};
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, RaftCmdRequest, RaftCmdResponse,
                          StatusCmdType, StatusResponse};
use protobuf::Message;
use raft::{self, SnapshotStatus, INVALID_INDEX};
use raftstore::{Error, Result};
use kvproto::metapb;
use util::worker::{FutureWorker, Scheduler, Stopped, Worker};
use util::transport::SendCh;
use util::RingQueue;
use util::collections::{HashMap, HashSet};
use util::rocksdb::{CompactedEvent, CompactionListener};
use util::sys as util_sys;
use storage::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use raftstore::coprocessor::CoprocessorHost;
use raftstore::coprocessor::split_observer::SplitObserver;
use super::worker::{ApplyRunner, ApplyTask, ApplyTaskRes, CompactRunner, CompactTask,
                    ConsistencyCheckRunner, ConsistencyCheckTask, RaftlogGcRunner, RaftlogGcTask,
                    RegionRunner, RegionTask, SplitCheckRunner, SplitCheckTask};
use super::worker::apply::{ChangePeer, ExecResult};
use super::{util, Msg, SignificantMsg, SnapKey, SnapManager, SnapshotDeleter, Tick};
use super::keys::{self, data_end_key, data_key, enc_end_key, enc_start_key};
use super::engine::{Iterable, Peekable, Snapshot as EngineSnapshot};
use super::config::Config;
use super::peer::{self, ConsistencyState, Peer, ReadyContext, StaleState};
use super::peer_storage::{self, ApplySnapResult, CacheQueryStats};
use super::msg::{Callback, ReadResponse};
use super::cmd_resp::{bind_term, new_error};
use super::transport::Transport;
use super::metrics::*;
use super::local_metrics::RaftMetrics;

type Key = Vec<u8>;

const MIO_TICK_RATIO: u64 = 10;
const PENDING_VOTES_CAP: usize = 20;

#[derive(Clone)]
pub struct Engines {
    pub kv_engine: Arc<DB>,
    pub raft_engine: Arc<DB>,
}

impl Engines {
    pub fn new(kv_engine: Arc<DB>, raft_engine: Arc<DB>) -> Engines {
        Engines {
            kv_engine: kv_engine,
            raft_engine: raft_engine,
        }
    }
}

// A helper structure to bundle all channels for messages to `Store`.
pub struct StoreChannel {
    pub sender: Sender<Msg>,
    pub significant_msg_receiver: StdReceiver<SignificantMsg>,
}

pub struct StoreStat {
    pub lock_cf_bytes_written: u64,

    pub engine_total_bytes_written: u64,
    pub engine_total_keys_written: u64,

    pub engine_last_total_bytes_written: u64,
    pub engine_last_total_keys_written: u64,
}

impl Default for StoreStat {
    fn default() -> StoreStat {
        StoreStat {
            lock_cf_bytes_written: 0,
            engine_total_bytes_written: 0,
            engine_total_keys_written: 0,

            engine_last_total_bytes_written: 0,
            engine_last_total_keys_written: 0,
        }
    }
}

pub struct DestroyPeerJob {
    pub initialized: bool,
    pub async_remove: bool,
    pub region_id: u64,
    pub peer: metapb::Peer,
}

pub struct StoreInfo {
    pub engine: Arc<DB>,
    pub capacity: u64,
}

pub struct Store<T, C: 'static> {
    cfg: Rc<Config>,
    kv_engine: Arc<DB>,
    raft_engine: Arc<DB>,
    store: metapb::Store,
    sendch: SendCh<Msg>,

    significant_msg_receiver: StdReceiver<SignificantMsg>,

    // region_id -> peers
    region_peers: HashMap<u64, Peer>,
    pending_raft_groups: HashSet<u64>,
    // region end key -> region id
    region_ranges: BTreeMap<Key, u64>,
    // the regions with pending snapshots between two mio ticks.
    pending_snapshot_regions: Vec<metapb::Region>,
    split_check_worker: Worker<SplitCheckTask>,
    region_worker: Worker<RegionTask>,
    raftlog_gc_worker: Worker<RaftlogGcTask>,
    compact_worker: Worker<CompactTask>,
    pd_worker: FutureWorker<PdTask>,
    consistency_check_worker: Worker<ConsistencyCheckTask>,
    pub apply_worker: Worker<ApplyTask>,
    apply_res_receiver: Option<StdReceiver<ApplyTaskRes>>,

    trans: T,
    pd_client: Arc<C>,

    pub coprocessor_host: Arc<CoprocessorHost>,

    snap_mgr: SnapManager,

    raft_metrics: RaftMetrics,
    pub entry_cache_metries: Rc<RefCell<CacheQueryStats>>,

    tag: String,

    start_time: Timespec,
    is_busy: bool,

    pending_votes: RingQueue<RaftMessage>,

    store_stat: StoreStat,
}

pub fn create_event_loop<T, C>(cfg: &Config) -> Result<EventLoop<Store<T, C>>>
where
    T: Transport,
    C: PdClient,
{
    let mut config = EventLoopConfig::new();
    // To make raft base tick more accurate, timer tick should be small enough.
    config.timer_tick_ms(cfg.raft_base_tick_interval.as_millis() / MIO_TICK_RATIO);
    config.notify_capacity(cfg.notify_capacity);
    config.messages_per_tick(cfg.messages_per_tick);
    let event_loop = EventLoop::configured(config)?;
    Ok(event_loop)
}

impl<T, C> Store<T, C> {
    #[allow(too_many_arguments)]
    pub fn new(
        ch: StoreChannel,
        meta: metapb::Store,
        cfg: Config,
        engines: Engines,
        trans: T,
        pd_client: Arc<C>,
        mgr: SnapManager,
        pd_worker: FutureWorker<PdTask>,
        mut coprocessor_host: CoprocessorHost,
    ) -> Result<Store<T, C>> {
        // TODO: we can get cluster meta regularly too later.
        cfg.validate()?;

        let sendch = SendCh::new(ch.sender, "raftstore");
        let tag = format!("[store {}]", meta.get_id());

        // TODO load coprocessors from configuration
        coprocessor_host
            .registry
            .register_admin_observer(100, box SplitObserver);

        let mut s = Store {
            cfg: Rc::new(cfg),
            store: meta,
            kv_engine: engines.kv_engine,
            raft_engine: engines.raft_engine,
            sendch: sendch,
            significant_msg_receiver: ch.significant_msg_receiver,
            region_peers: HashMap::default(),
            pending_raft_groups: HashSet::default(),
            split_check_worker: Worker::new("split check worker"),
            region_worker: Worker::new("snapshot worker"),
            raftlog_gc_worker: Worker::new("raft gc worker"),
            compact_worker: Worker::new("compact worker"),
            pd_worker: pd_worker,
            consistency_check_worker: Worker::new("consistency check worker"),
            apply_worker: Worker::new("apply worker"),
            apply_res_receiver: None,
            region_ranges: BTreeMap::new(),
            pending_snapshot_regions: vec![],
            trans: trans,
            pd_client: pd_client,
            coprocessor_host: Arc::new(coprocessor_host),
            snap_mgr: mgr,
            raft_metrics: RaftMetrics::default(),
            entry_cache_metries: Rc::new(RefCell::new(CacheQueryStats::default())),
            pending_votes: RingQueue::with_capacity(PENDING_VOTES_CAP),
            tag: tag,
            start_time: time::get_time(),
            is_busy: false,
            store_stat: StoreStat::default(),
        };
        s.init()?;
        Ok(s)
    }

    /// Initialize this store. It scans the db engine, loads all regions
    /// and their peers from it, and schedules snapshot worker if necessary.
    /// WARN: This store should not be used before initialized.
    fn init(&mut self) -> Result<()> {
        // Scan region meta to get saved regions.
        let start_key = keys::REGION_META_MIN_KEY;
        let end_key = keys::REGION_META_MAX_KEY;
        let kv_engine = Arc::clone(&self.kv_engine);
        let mut total_count = 0;
        let mut tomebstone_count = 0;
        let mut applying_count = 0;

        let t = Instant::now();
        let mut kv_wb = WriteBatch::new();
        let mut raft_wb = WriteBatch::new();
        let mut applying_regions = vec![];
        kv_engine.scan_cf(CF_RAFT, start_key, end_key, false, &mut |key, value| {
            let (region_id, suffix) = keys::decode_region_meta_key(key)?;
            if suffix != keys::REGION_STATE_SUFFIX {
                return Ok(true);
            }

            total_count += 1;

            let local_state = protobuf::parse_from_bytes::<RegionLocalState>(value)?;
            let region = local_state.get_region();
            if local_state.get_state() == PeerState::Tombstone {
                tomebstone_count += 1;
                debug!(
                    "region {:?} is tombstone in store {}",
                    region,
                    self.store_id()
                );
                self.clear_stale_meta(&mut kv_wb, &mut raft_wb, region);
                return Ok(true);
            }
            if local_state.get_state() == PeerState::Applying {
                // in case of restart happen when we just write region state to Applying,
                // but not write raft_local_state to raft rocksdb in time.
                peer_storage::recover_from_applying_state(
                    &self.kv_engine,
                    &self.raft_engine,
                    &raft_wb,
                    region_id,
                )?;
                applying_count += 1;
                applying_regions.push(region.clone());
                return Ok(true);
            }

            let peer = Peer::create(self, region)?;
            self.region_ranges.insert(enc_end_key(region), region_id);
            // No need to check duplicated here, because we use region id as the key
            // in DB.
            self.region_peers.insert(region_id, peer);
            Ok(true)
        })?;

        if !kv_wb.is_empty() {
            self.kv_engine.write(kv_wb).unwrap();
            self.kv_engine.sync_wal().unwrap();
        }
        if !raft_wb.is_empty() {
            self.raft_engine.write(raft_wb).unwrap();
            self.raft_engine.sync_wal().unwrap();
        }

        // schedule applying snapshot after raft writebatch were written.
        for region in applying_regions {
            info!(
                "region {:?} is applying in store {}",
                region,
                self.store_id()
            );
            let mut peer = Peer::create(self, &region)?;
            peer.mut_store().schedule_applying_snapshot();
            self.region_ranges
                .insert(enc_end_key(&region), region.get_id());
            self.region_peers.insert(region.get_id(), peer);
        }

        info!(
            "{} starts with {} regions, including {} tombstones and {} applying \
             regions, takes {:?}",
            self.tag,
            total_count,
            tomebstone_count,
            applying_count,
            t.elapsed()
        );

        self.clear_stale_data()?;

        Ok(())
    }

    fn clear_stale_meta(
        &mut self,
        kv_wb: &mut WriteBatch,
        raft_wb: &mut WriteBatch,
        region: &metapb::Region,
    ) {
        let raft_key = keys::raft_state_key(region.get_id());
        let raft_state = match self.raft_engine.get_msg(&raft_key).unwrap() {
            // it has been cleaned up.
            None => return,
            Some(value) => value,
        };

        peer_storage::clear_meta(
            &self.kv_engine,
            &self.raft_engine,
            kv_wb,
            raft_wb,
            region.get_id(),
            &raft_state,
        ).unwrap();
        peer_storage::write_peer_state(&self.kv_engine, kv_wb, region, PeerState::Tombstone)
            .unwrap();
    }

    /// `clear_stale_data` clean up all possible garbage data.
    fn clear_stale_data(&mut self) -> Result<()> {
        let t = Instant::now();

        let mut ranges = Vec::new();
        let mut last_start_key = keys::data_key(b"");
        for region_id in self.region_ranges.values() {
            let region = self.region_peers[region_id].region();
            let start_key = keys::enc_start_key(region);
            ranges.push((last_start_key, start_key));
            last_start_key = keys::enc_end_key(region);
        }
        ranges.push((last_start_key, keys::DATA_MAX_KEY.to_vec()));

        rocksdb::roughly_cleanup_ranges(&self.kv_engine, &ranges)?;

        info!(
            "{} cleans up {} ranges garbage data, takes {:?}",
            self.tag,
            ranges.len(),
            t.elapsed()
        );

        Ok(())
    }

    pub fn get_sendch(&self) -> SendCh<Msg> {
        self.sendch.clone()
    }

    #[inline]
    pub fn get_snap_mgr(&self) -> SnapManager {
        self.snap_mgr.clone()
    }

    pub fn snap_scheduler(&self) -> Scheduler<RegionTask> {
        self.region_worker.scheduler()
    }

    pub fn apply_scheduler(&self) -> Scheduler<ApplyTask> {
        self.apply_worker.scheduler()
    }

    pub fn kv_engine(&self) -> Arc<DB> {
        Arc::clone(&self.kv_engine)
    }

    pub fn raft_engine(&self) -> Arc<DB> {
        Arc::clone(&self.raft_engine)
    }

    pub fn store_id(&self) -> u64 {
        self.store.get_id()
    }

    pub fn get_peers(&self) -> &HashMap<u64, Peer> {
        &self.region_peers
    }

    pub fn config(&self) -> Rc<Config> {
        Rc::clone(&self.cfg)
    }

    fn poll_significant_msg(&mut self) {
        // Poll all snapshot messages and handle them.
        loop {
            match self.significant_msg_receiver.try_recv() {
                Ok(SignificantMsg::SnapshotStatus {
                    region_id,
                    to_peer_id,
                    status,
                }) => {
                    // Report snapshot status to the corresponding peer.
                    self.report_snapshot_status(region_id, to_peer_id, status);
                }
                Ok(SignificantMsg::Unreachable {
                    region_id,
                    to_peer_id,
                }) => if let Some(peer) = self.region_peers.get_mut(&region_id) {
                    peer.raft_group.report_unreachable(to_peer_id);
                },
                Err(TryRecvError::Empty) => {
                    // The snapshot status receiver channel is empty
                    return;
                }
                Err(e) => {
                    error!(
                        "{} unexpected error {:?} when receive from snapshot channel",
                        self.tag, e
                    );
                    return;
                }
            }
        }
    }

    fn report_snapshot_status(&mut self, region_id: u64, to_peer_id: u64, status: SnapshotStatus) {
        if let Some(peer) = self.region_peers.get_mut(&region_id) {
            let to_peer = match peer.get_peer_from_cache(to_peer_id) {
                Some(peer) => peer,
                None => {
                    // If to_peer is gone, ignore this snapshot status
                    warn!(
                        "[region {}] peer {} not found, ignore snapshot status {:?}",
                        region_id, to_peer_id, status
                    );
                    return;
                }
            };
            info!(
                "[region {}] report snapshot status {:?} {:?}",
                region_id, to_peer, status
            );
            peer.raft_group.report_snapshot(to_peer_id, status)
        }
    }
}

impl<T: Transport, C: PdClient> Store<T, C> {
    pub fn run(&mut self, event_loop: &mut EventLoop<Self>) -> Result<()> {
        self.snap_mgr.init()?;

        self.register_raft_base_tick(event_loop);
        self.register_raft_gc_log_tick(event_loop);
        self.register_split_region_check_tick(event_loop);
        self.register_compact_check_tick(event_loop);
        self.register_pd_store_heartbeat_tick(event_loop);
        self.register_pd_heartbeat_tick(event_loop);
        self.register_snap_mgr_gc_tick(event_loop);
        self.register_compact_lock_cf_tick(event_loop);
        self.register_consistency_check_tick(event_loop);

        let split_check_runner = SplitCheckRunner::new(
            Arc::clone(&self.kv_engine),
            self.sendch.clone(),
            Arc::clone(&self.coprocessor_host),
        );

        box_try!(self.split_check_worker.start(split_check_runner));

        let runner = RegionRunner::new(
            Arc::clone(&self.kv_engine),
            Arc::clone(&self.raft_engine),
            self.snap_mgr.clone(),
            self.cfg.snap_apply_batch_size.0 as usize,
            self.cfg.use_delete_range,
        );
        box_try!(self.region_worker.start(runner));

        let raftlog_gc_runner = RaftlogGcRunner::new(None);
        box_try!(self.raftlog_gc_worker.start(raftlog_gc_runner));

        let compact_runner = CompactRunner::new(Arc::clone(&self.kv_engine));
        box_try!(self.compact_worker.start(compact_runner));

        let pd_runner = PdRunner::new(
            self.store_id(),
            Arc::clone(&self.pd_client),
            self.sendch.clone(),
            Arc::clone(&self.kv_engine),
        );
        box_try!(self.pd_worker.start(pd_runner));

        let consistency_check_runner = ConsistencyCheckRunner::new(self.sendch.clone());
        box_try!(
            self.consistency_check_worker
                .start(consistency_check_runner)
        );

        let (tx, rx) = mpsc::channel();
        let apply_runner = ApplyRunner::new(self, tx, self.cfg.sync_log, self.cfg.use_delete_range);
        self.apply_res_receiver = Some(rx);
        box_try!(self.apply_worker.start(apply_runner));

        if let Err(e) = util_sys::pri::set_priority(util_sys::HIGH_PRI) {
            warn!("set priority for raftstore failed, error: {:?}", e);
        }

        event_loop.run(self)?;
        Ok(())
    }

    fn stop(&mut self) {
        info!("start to stop raftstore.");

        // Applying snapshot may take an unexpected long time.
        for peer in self.region_peers.values_mut() {
            peer.stop();
        }

        // Wait all workers finish.
        let mut handles: Vec<Option<thread::JoinHandle<()>>> = vec![];
        handles.push(self.split_check_worker.stop());
        handles.push(self.region_worker.stop());
        handles.push(self.raftlog_gc_worker.stop());
        handles.push(self.compact_worker.stop());
        handles.push(self.pd_worker.stop());
        handles.push(self.consistency_check_worker.stop());
        handles.push(self.apply_worker.stop());

        for h in handles {
            if let Some(h) = h {
                h.join().unwrap();
            }
        }

        self.coprocessor_host.shutdown();

        info!("stop raftstore finished.");
    }

    fn register_raft_base_tick(&self, event_loop: &mut EventLoop<Self>) {
        // If we register raft base tick failed, the whole raft can't run correctly,
        // TODO: shutdown the store?
        if let Err(e) = register_timer(
            event_loop,
            Tick::Raft,
            self.cfg.raft_base_tick_interval.as_millis(),
        ) {
            error!("{} register raft base tick err: {:?}", self.tag, e);
        };
    }

    fn on_raft_base_tick(&mut self, event_loop: &mut EventLoop<Self>) {
        let timer = self.raft_metrics.process_tick.start_coarse_timer();
        let mut leader_missing = 0;
        for peer in &mut self.region_peers.values_mut() {
            if peer.pending_remove {
                continue;
            }
            // When having pending snapshot, if election timeout is met, it can't pass
            // the pending conf change check because first index has been updated to
            // a value that is larger than last index.
            if peer.is_applying_snapshot() || peer.has_pending_snapshot() {
                // need to check if snapshot is applied.
                peer.mark_to_be_checked(&mut self.pending_raft_groups);
                continue;
            }

            if peer.raft_group.tick() {
                peer.mark_to_be_checked(&mut self.pending_raft_groups);
            }

            // If this peer detects the leader is missing for a long long time,
            // it should consider itself as a stale peer which is removed from
            // the original cluster.
            // This most likely happens in the following scenario:
            // At first, there are three peer A, B, C in the cluster, and A is leader.
            // Peer B gets down. And then A adds D, E, F into the cluster.
            // Peer D becomes leader of the new cluster, and then removes peer A, B, C.
            // After all these peer in and out, now the cluster has peer D, E, F.
            // If peer B goes up at this moment, it still thinks it is one of the cluster
            // and has peers A, C. However, it could not reach A, C since they are removed
            // from the cluster or probably destroyed.
            // Meantime, D, E, F would not reach B, since it's not in the cluster anymore.
            // In this case, peer B would notice that the leader is missing for a long time,
            // and it would check with pd to confirm whether it's still a member of the cluster.
            // If not, it destroys itself as a stale peer which is removed out already.
            match peer.check_stale_state() {
                StaleState::Valid => (),
                StaleState::LeaderMissing => {
                    warn!(
                        "{} leader missing longer than abnormal_leader_missing_duration {:?}",
                        peer.tag, self.cfg.abnormal_leader_missing_duration.0,
                    );
                    leader_missing += 1;
                }
                StaleState::ToValidate => {
                    // for peer B in case 1 above
                    warn!(
                        "{} leader missing longer than max_leader_missing_duration {:?}. \
                         To check with pd whether it's still valid",
                        peer.tag, self.cfg.max_leader_missing_duration.0,
                    );
                    let task = PdTask::ValidatePeer {
                        peer: peer.peer.clone(),
                        region: peer.region().clone(),
                    };
                    if let Err(e) = self.pd_worker.schedule(task) {
                        error!("{} failed to notify pd: {}", peer.tag, e)
                    }
                }
            }
        }
        self.raft_metrics.leader_missing = leader_missing;

        self.poll_significant_msg();

        timer.observe_duration();

        self.raft_metrics.flush();
        self.entry_cache_metries.borrow_mut().flush();

        self.register_raft_base_tick(event_loop);
    }

    fn poll_apply(&mut self) {
        loop {
            match self.apply_res_receiver.as_ref().unwrap().try_recv() {
                Ok(ApplyTaskRes::Applys(multi_res)) => for res in multi_res {
                    if let Some(p) = self.region_peers.get_mut(&res.region_id) {
                        debug!("{} async apply finish: {:?}", p.tag, res);
                        p.post_apply(&res, &mut self.pending_raft_groups, &mut self.store_stat);
                    }
                    self.store_stat.lock_cf_bytes_written += res.metrics.lock_cf_written_bytes;
                    self.on_ready_result(res.region_id, res.exec_res);
                },
                Ok(ApplyTaskRes::Destroy(p)) => {
                    let store_id = self.store_id();
                    self.destroy_peer(p.region_id(), util::new_peer(store_id, p.id()));
                }
                Err(TryRecvError::Empty) => break,
                Err(e) => panic!("unexpected error {:?}", e),
            }
        }
    }

    /// If target peer doesn't exist, create it.
    ///
    /// return false to indicate that target peer is in invalid state or
    /// doesn't exist and can't be created.
    fn maybe_create_peer(&mut self, region_id: u64, msg: &RaftMessage) -> Result<bool> {
        let target = msg.get_to_peer();
        // we may encounter a message with larger peer id, which means
        // current peer is stale, then we should remove current peer
        let mut has_peer = false;
        let mut job = None;
        if let Some(p) = self.region_peers.get_mut(&region_id) {
            has_peer = true;
            let target_peer_id = target.get_id();
            if p.peer_id() < target_peer_id {
                job = p.maybe_destroy();
                if job.is_none() {
                    self.raft_metrics.message_dropped.applying_snap += 1;
                    return Ok(false);
                }
            } else if p.peer_id() > target_peer_id {
                info!(
                    "[region {}] target peer id {} is less than {}, msg maybe stale.",
                    region_id,
                    target_peer_id,
                    p.peer_id()
                );
                self.raft_metrics.message_dropped.stale_msg += 1;
                return Ok(false);
            }
        }

        if let Some(job) = job {
            info!(
                "[region {}] try to destroy stale peer {:?}",
                region_id, job.peer
            );
            if !self.handle_destroy_peer(job) {
                return Ok(false);
            }
            has_peer = false;
        }

        if has_peer {
            return Ok(true);
        }

        let message = msg.get_message();
        let msg_type = message.get_msg_type();
        if msg_type != MessageType::MsgRequestVote
            && (msg_type != MessageType::MsgHeartbeat || message.get_commit() != INVALID_INDEX)
        {
            debug!(
                "target peer {:?} doesn't exist, stale message {:?}.",
                target, msg_type
            );
            self.raft_metrics.message_dropped.stale_msg += 1;
            return Ok(false);
        }

        let start_key = data_key(msg.get_start_key());
        if let Some((_, &exist_region_id)) = self.region_ranges
            .range((Excluded(start_key), Unbounded::<Key>))
            .next()
        {
            let exist_region = self.region_peers[&exist_region_id].region();
            if enc_start_key(exist_region) < data_end_key(msg.get_end_key()) {
                debug!("msg {:?} is overlapped with region {:?}", msg, exist_region);
                if util::is_first_vote_msg(msg) {
                    self.pending_votes.push(msg.to_owned());
                }
                self.raft_metrics.message_dropped.region_overlap += 1;
                return Ok(false);
            }
        }

        let peer = Peer::replicate(self, region_id, target.get_id())?;
        // following snapshot may overlap, should insert into region_ranges after
        // snapshot is applied.
        self.region_peers.insert(region_id, peer);
        Ok(true)
    }

    fn on_raft_message(&mut self, mut msg: RaftMessage) -> Result<()> {
        let region_id = msg.get_region_id();
        if !self.validate_raft_msg(&msg) {
            return Ok(());
        }

        if msg.get_is_tombstone() {
            // we receive a message tells us to remove ourself.
            self.handle_gc_peer_msg(&msg);
            return Ok(());
        }

        if self.check_msg(&msg)? {
            return Ok(());
        }

        if !self.maybe_create_peer(region_id, &msg)? {
            return Ok(());
        }

        if let Some(key) = self.check_snapshot(&msg)? {
            // If the snapshot file is not used again, then it's OK to
            // delete them here. If the snapshot file will be reused when
            // receiving, then it will fail to pass the check again, so
            // missing snapshot files should not be noticed.
            let s = self.snap_mgr.get_snapshot_for_applying(&key)?;
            self.snap_mgr.delete_snapshot(&key, s.as_ref(), false);
            return Ok(());
        }

        let peer = self.region_peers.get_mut(&region_id).unwrap();
        peer.insert_peer_cache(msg.take_from_peer());
        peer.step(msg.take_message())?;

        // Add into pending raft groups for later handling ready.
        peer.mark_to_be_checked(&mut self.pending_raft_groups);

        Ok(())
    }

    // return false means the message is invalid, and can be ignored.
    fn validate_raft_msg(&mut self, msg: &RaftMessage) -> bool {
        let region_id = msg.get_region_id();
        let from = msg.get_from_peer();
        let to = msg.get_to_peer();

        debug!(
            "[region {}] handle raft message {:?}, from {} to {}",
            region_id,
            msg.get_message().get_msg_type(),
            from.get_id(),
            to.get_id()
        );

        if to.get_store_id() != self.store_id() {
            warn!(
                "[region {}] store not match, to store id {}, mine {}, ignore it",
                region_id,
                to.get_store_id(),
                self.store_id()
            );
            self.raft_metrics.message_dropped.mismatch_store_id += 1;
            return false;
        }

        if !msg.has_region_epoch() {
            error!(
                "[region {}] missing epoch in raft message, ignore it",
                region_id
            );
            self.raft_metrics.message_dropped.mismatch_region_epoch += 1;
            return false;
        }

        true
    }

    fn check_msg(&mut self, msg: &RaftMessage) -> Result<bool> {
        let region_id = msg.get_region_id();
        let from_epoch = msg.get_region_epoch();
        let msg_type = msg.get_message().get_msg_type();
        let is_vote_msg = msg_type == MessageType::MsgRequestVote;
        let from_store_id = msg.get_from_peer().get_store_id();

        // Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
        // a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
        //  We should ignore this stale message and let 2 remove itself after
        //  applying the ConfChange log.
        // b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
        //  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
        // c. 2 is isolated but can communicate with 3. 1 removes 3.
        //  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
        // d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
        //  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
        // e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
        //  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
        //  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
        //  rejoin the raft group again.
        // f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
        //  unlike case e, 2 will be stale forever.
        // TODO: for case f, if 2 is stale for a long time, 2 will communicate with pd and pd will
        // tell 2 is stale, so 2 can remove itself.
        let trans = &self.trans;
        let raft_metrics = &mut self.raft_metrics;
        if let Some(peer) = self.region_peers.get(&region_id) {
            let region = peer.region();
            let epoch = region.get_region_epoch();

            if util::is_epoch_stale(from_epoch, epoch)
                && util::find_peer(region, from_store_id).is_none()
            {
                // The message is stale and not in current region.
                Self::handle_stale_msg(trans, msg, epoch, is_vote_msg, raft_metrics);
                return Ok(true);
            }

            return Ok(false);
        }

        // no exist, check with tombstone key.
        let state_key = keys::region_state_key(region_id);
        if let Some(local_state) = self.kv_engine
            .get_msg_cf::<RegionLocalState>(CF_RAFT, &state_key)?
        {
            if local_state.get_state() != PeerState::Tombstone {
                // Maybe split, but not registered yet.
                raft_metrics.message_dropped.region_nonexistent += 1;
                if util::is_first_vote_msg(msg) {
                    self.pending_votes.push(msg.to_owned());
                    info!(
                        "[region {}] doesn't exist yet, wait for it to be split",
                        region_id
                    );
                    return Ok(true);
                }
                return Err(box_err!(
                    "[region {}] region not exist but not tombstone: {:?}",
                    region_id,
                    local_state
                ));
            }
            let region = local_state.get_region();
            let region_epoch = region.get_region_epoch();
            // The region in this peer is already destroyed
            if util::is_epoch_stale(from_epoch, region_epoch) {
                info!(
                    "[region {}] tombstone peer [epoch: {:?}] \
                     receive a stale message {:?}",
                    region_id, region_epoch, msg_type,
                );

                let not_exist = util::find_peer(region, from_store_id).is_none();
                Self::handle_stale_msg(
                    trans,
                    msg,
                    region_epoch,
                    is_vote_msg && not_exist,
                    raft_metrics,
                );

                return Ok(true);
            }

            if from_epoch.get_conf_ver() == region_epoch.get_conf_ver() {
                raft_metrics.message_dropped.region_tombstone_peer += 1;
                return Err(box_err!(
                    "tombstone peer [epoch: {:?}] receive an invalid \
                     message {:?}, ignore it",
                    region_epoch,
                    msg_type
                ));
            }
        }

        Ok(false)
    }

    fn handle_stale_msg(
        trans: &T,
        msg: &RaftMessage,
        cur_epoch: &metapb::RegionEpoch,
        need_gc: bool,
        raft_metrics: &mut RaftMetrics,
    ) {
        let region_id = msg.get_region_id();
        let from_peer = msg.get_from_peer();
        let to_peer = msg.get_to_peer();
        let msg_type = msg.get_message().get_msg_type();

        if !need_gc {
            info!(
                "[region {}] raft message {:?} is stale, current {:?}, ignore it",
                region_id, msg_type, cur_epoch
            );
            raft_metrics.message_dropped.stale_msg += 1;
            return;
        }

        info!(
            "[region {}] raft message {:?} is stale, current {:?}, tell to gc",
            region_id, msg_type, cur_epoch
        );

        let mut gc_msg = RaftMessage::new();
        gc_msg.set_region_id(region_id);
        gc_msg.set_from_peer(to_peer.clone());
        gc_msg.set_to_peer(from_peer.clone());
        gc_msg.set_region_epoch(cur_epoch.clone());
        gc_msg.set_is_tombstone(true);
        if let Err(e) = trans.send(gc_msg) {
            error!("[region {}] send gc message failed {:?}", region_id, e);
        }
    }

    fn handle_gc_peer_msg(&mut self, msg: &RaftMessage) {
        let region_id = msg.get_region_id();

        let mut job = None;
        if let Some(peer) = self.region_peers.get_mut(&region_id) {
            let from_epoch = msg.get_region_epoch();
            if util::is_epoch_stale(peer.get_store().region.get_region_epoch(), from_epoch) {
                if peer.peer != *msg.get_to_peer() {
                    info!("[region {}] receive stale gc message, ignore.", region_id);
                    self.raft_metrics.message_dropped.stale_msg += 1;
                    return;
                }
                // TODO: ask pd to guarantee we are stale now.
                info!(
                    "[region {}] peer {:?} receives gc message, trying to remove",
                    region_id,
                    msg.get_to_peer()
                );
                job = peer.maybe_destroy();
                if job.is_none() {
                    self.raft_metrics.message_dropped.applying_snap += 1;
                    return;
                }
            }
        }

        if let Some(job) = job {
            self.handle_destroy_peer(job);
        }
    }

    fn check_snapshot(&mut self, msg: &RaftMessage) -> Result<Option<SnapKey>> {
        let region_id = msg.get_region_id();

        // Check if we can accept the snapshot
        if self.region_peers[&region_id].get_store().is_initialized()
            || !msg.get_message().has_snapshot()
        {
            return Ok(None);
        }

        let snap = msg.get_message().get_snapshot();
        let key = SnapKey::from_region_snap(region_id, snap);
        let mut snap_data = RaftSnapshotData::new();
        snap_data.merge_from_bytes(snap.get_data())?;
        let snap_region = snap_data.take_region();
        let peer_id = msg.get_to_peer().get_id();
        if snap_region
            .get_peers()
            .into_iter()
            .all(|p| p.get_id() != peer_id)
        {
            info!(
                "[region {}] {:?} doesn't contain peer {:?}, skip.",
                snap_region.get_id(),
                snap_region,
                msg.get_to_peer()
            );
            self.raft_metrics.message_dropped.region_no_peer += 1;
            return Ok(Some(key));
        }
        if let Some((_, &exist_region_id)) = self.region_ranges
            .range((Excluded(enc_start_key(&snap_region)), Unbounded::<Key>))
            .next()
        {
            let exist_region = self.region_peers[&exist_region_id].region();
            if enc_start_key(exist_region) < enc_end_key(&snap_region) {
                info!("region overlapped {:?}, {:?}", exist_region, snap_region);
                self.raft_metrics.message_dropped.region_overlap += 1;
                return Ok(Some(key));
            }
        }
        for region in &self.pending_snapshot_regions {
            if enc_start_key(region) < enc_end_key(&snap_region) &&
               enc_end_key(region) > enc_start_key(&snap_region) &&
               // Same region can overlap, we will apply the latest version of snapshot.
               region.get_id() != snap_region.get_id()
            {
                info!("pending region overlapped {:?}, {:?}", region, snap_region);
                self.raft_metrics.message_dropped.region_overlap += 1;
                return Ok(Some(key));
            }
        }
        // check if snapshot file exists.
        self.snap_mgr.get_snapshot_for_applying(&key)?;

        self.pending_snapshot_regions.push(snap_region);

        Ok(None)
    }

    fn on_raft_ready(&mut self) {
        let t = SlowTimer::new();
        let pending_count = self.pending_raft_groups.len();
        let previous_ready_metrics = self.raft_metrics.ready.clone();

        self.raft_metrics.ready.pending_region += pending_count as u64;

        let mut region_proposals = Vec::with_capacity(pending_count);
        let (kv_wb, raft_wb, append_res, sync_log) = {
            let mut ctx = ReadyContext::new(&mut self.raft_metrics, &self.trans, pending_count);
            for region_id in self.pending_raft_groups.drain() {
                if let Some(peer) = self.region_peers.get_mut(&region_id) {
                    if let Some(region_proposal) = peer.take_apply_proposals() {
                        region_proposals.push(region_proposal);
                    }
                    peer.handle_raft_ready_append(&mut ctx, &self.pd_worker);
                }
            }
            (ctx.kv_wb, ctx.raft_wb, ctx.ready_res, ctx.sync_log)
        };

        if !region_proposals.is_empty() {
            self.apply_worker
                .schedule(ApplyTask::Proposals(region_proposals))
                .unwrap();

            // In most cases, if the leader proposes a message, it will also
            // broadcast the message to other followers, so we should flush the
            // messages ASAP.
            self.trans.flush();
        }

        self.raft_metrics.ready.has_ready_region += append_res.len() as u64;

        // apply_snapshot, peer_destroy will clear_meta, so we need write region state first.
        // otherwise, if program restart between two write, raft log will be removed,
        // but region state may not changed in disk.
        fail_point!("raft_before_save");
        if !kv_wb.is_empty() {
            // RegionLocalState, ApplyState
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(true);
            self.kv_engine
                .write_opt(kv_wb, &write_opts)
                .unwrap_or_else(|e| {
                    panic!("{} failed to save append state result: {:?}", self.tag, e);
                });
        }
        fail_point!("raft_between_save");

        if !raft_wb.is_empty() {
            // RaftLocalState, Raft Log Entry
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(self.cfg.sync_log || sync_log);
            self.raft_engine
                .write_opt(raft_wb, &write_opts)
                .unwrap_or_else(|e| {
                    panic!("{} failed to save raft append result: {:?}", self.tag, e);
                });
        }
        fail_point!("raft_after_save");

        let mut ready_results = Vec::with_capacity(append_res.len());
        for (mut ready, invoke_ctx) in append_res {
            let region_id = invoke_ctx.region_id;
            let res = self.region_peers
                .get_mut(&region_id)
                .unwrap()
                .post_raft_ready_append(
                    &mut self.raft_metrics,
                    &self.trans,
                    &mut ready,
                    invoke_ctx,
                );
            ready_results.push((region_id, ready, res));
        }

        self.raft_metrics
            .append_log
            .observe(duration_to_sec(t.elapsed()) as f64);

        slow_log!(
            t,
            "{} handle {} pending peers include {} ready, {} entries, {} messages and {} \
             snapshots",
            self.tag,
            pending_count,
            ready_results.capacity(),
            self.raft_metrics.ready.append - previous_ready_metrics.append,
            self.raft_metrics.ready.message - previous_ready_metrics.message,
            self.raft_metrics.ready.snapshot - previous_ready_metrics.snapshot
        );

        if !ready_results.is_empty() {
            let mut apply_tasks = Vec::with_capacity(ready_results.len());
            for (region_id, ready, res) in ready_results {
                self.region_peers
                    .get_mut(&region_id)
                    .unwrap()
                    .handle_raft_ready_apply(ready, &mut apply_tasks);
                if let Some(apply_result) = res {
                    self.on_ready_apply_snapshot(apply_result);
                }
            }
            self.apply_worker
                .schedule(ApplyTask::applies(apply_tasks))
                .unwrap();
        }

        let dur = t.elapsed();
        if !self.is_busy {
            let election_timeout = Duration::from_millis(
                self.cfg.raft_base_tick_interval.as_millis()
                    * self.cfg.raft_election_timeout_ticks as u64,
            );
            if dur >= election_timeout {
                self.is_busy = true;
            }
        }

        self.raft_metrics
            .process_ready
            .observe(duration_to_sec(dur) as f64);

        self.trans.flush();

        slow_log!(t, "{} on {} regions raft ready", self.tag, pending_count);
    }

    fn handle_destroy_peer(&mut self, job: DestroyPeerJob) -> bool {
        if job.initialized {
            self.apply_worker
                .schedule(ApplyTask::destroy(job.region_id))
                .unwrap();
        }
        if job.async_remove {
            info!(
                "[region {}] {} is destroyed asychroniously",
                job.region_id,
                job.peer.get_id()
            );
            false
        } else {
            self.destroy_peer(job.region_id, job.peer);
            true
        }
    }

    pub fn destroy_peer(&mut self, region_id: u64, peer: metapb::Peer) {
        // Can we destroy it in another thread later?

        // Suppose cluster removes peer a from store and then add a new
        // peer b to the same store again, if peer a is applying snapshot,
        // then it will be considered stale and removed immediately, and the
        // apply meta will be removed asynchronously. So the `destroy_peer` will
        // be called again when `poll_apply`. We need to check if the peer exists
        // and is the very target.
        let mut p = match self.region_peers.remove(&region_id) {
            None => return,
            Some(p) => if p.peer_id() == peer.get_id() {
                p
            } else {
                assert!(p.peer_id() > peer.get_id());
                // It has been destroyed.
                self.region_peers.insert(region_id, p);
                return;
            },
        };

        info!("[region {}] destroy peer {:?}", region_id, peer);
        // We can't destroy a peer which is applying snapshot.
        assert!(!p.is_applying_snapshot());
        let task = PdTask::DestroyPeer {
            region_id: region_id,
        };
        if let Err(e) = self.pd_worker.schedule(task) {
            error!("{} failed to notify pd: {}", self.tag, e);
        }
        let is_initialized = p.is_initialized();
        if let Err(e) = p.destroy() {
            // If not panic here, the peer will be recreated in the next restart,
            // then it will be gc again. But if some overlap region is created
            // before restarting, the gc action will delete the overlap region's
            // data too.
            panic!(
                "[region {}] destroy peer {:?} in store {} err {:?}",
                region_id,
                peer,
                self.store_id(),
                e
            );
        }

        if is_initialized
            && self.region_ranges
                .remove(&enc_end_key(p.region()))
                .is_none()
        {
            panic!(
                "[region {}] remove peer {:?} in store {}",
                region_id,
                peer,
                self.store_id()
            );
        }
    }

    fn on_ready_change_peer(&mut self, region_id: u64, cp: ChangePeer) {
        let my_peer_id;
        let change_type = cp.conf_change.get_change_type();
        if let Some(p) = self.region_peers.get_mut(&region_id) {
            p.raft_group.apply_conf_change(&cp.conf_change);
            if cp.conf_change.get_node_id() == raft::INVALID_ID {
                // Apply failed, skip.
                return;
            }
            p.mut_store().region = cp.region;
            if p.is_leader() {
                // Notify pd immediately.
                info!(
                    "{} notify pd with change peer region {:?}",
                    p.tag,
                    p.region()
                );
                p.heartbeat_pd(&self.pd_worker);
            }

            match change_type {
                ConfChangeType::AddNode => {
                    // Add this peer to cache.
                    let peer = cp.peer.clone();
                    p.peer_heartbeats.insert(peer.get_id(), Instant::now());
                    p.insert_peer_cache(peer);
                }
                ConfChangeType::RemoveNode => {
                    // Remove this peer from cache.
                    p.peer_heartbeats.remove(&cp.peer.get_id());
                    p.remove_peer_from_cache(cp.peer.get_id());
                }
                ConfChangeType::AddLearnerNode => unimplemented!(),
            }

            my_peer_id = p.peer_id();
        } else {
            panic!("{} missing region {}", self.tag, region_id);
        }

        let peer = cp.peer;

        // We only care remove itself now.
        if change_type == ConfChangeType::RemoveNode && peer.get_store_id() == self.store_id() {
            if my_peer_id == peer.get_id() {
                self.destroy_peer(region_id, peer)
            } else {
                panic!("{} trying to remove unknown peer {:?}", self.tag, peer);
            }
        }
    }

    fn on_ready_compact_log(
        &mut self,
        region_id: u64,
        first_index: u64,
        state: RaftTruncatedState,
    ) {
        let peer = self.region_peers.get_mut(&region_id).unwrap();
        let total_cnt = peer.last_applying_idx - first_index;
        // the size of current CompactLog command can be ignored.
        let remain_cnt = peer.last_applying_idx - state.get_index() - 1;
        peer.raft_log_size_hint = peer.raft_log_size_hint * remain_cnt / total_cnt;
        let task = RaftlogGcTask {
            raft_engine: Arc::clone(&peer.get_store().get_raft_engine()),
            region_id: peer.get_store().get_region_id(),
            start_idx: peer.last_compacted_idx,
            end_idx: state.get_index() + 1,
        };
        peer.last_compacted_idx = task.end_idx;
        peer.mut_store().compact_to(task.end_idx);
        if let Err(e) = self.raftlog_gc_worker.schedule(task) {
            error!(
                "[region {}] failed to schedule compact task: {}",
                region_id, e
            );
        }
    }

    fn on_ready_split_region(
        &mut self,
        region_id: u64,
        left: metapb::Region,
        right: metapb::Region,
        right_derive: bool,
    ) {
        let (origin_region, new_region) = if right_derive {
            (right.clone(), left.clone())
        } else {
            (left.clone(), right.clone())
        };

        self.region_peers
            .get_mut(&region_id)
            .unwrap()
            .mut_store()
            .region = origin_region.clone();
        let new_region_id = new_region.get_id();
        if let Some(peer) = self.region_peers.get(&new_region_id) {
            // If the store received a raft msg with the new region raft group
            // before splitting, it will creates a uninitialized peer.
            // We can remove this uninitialized peer directly.
            if peer.get_store().is_initialized() {
                panic!("duplicated region {} for split region", new_region_id);
            }
        }

        let mut campaigned = false;
        let peer;
        match Peer::create(self, &new_region) {
            Err(e) => {
                // peer information is already written into db, can't recover.
                // there is probably a bug.
                panic!("create new split region {:?} err {:?}", new_region, e);
            }
            Ok(mut new_peer) => {
                for peer in new_region.get_peers() {
                    // Add this peer to cache.
                    new_peer.insert_peer_cache(peer.clone());
                }
                peer = new_peer.peer.clone();
                if let Some(origin_peer) = self.region_peers.get(&region_id) {
                    // New peer derive write flow from parent region,
                    // this will be used by balance write flow.
                    new_peer.peer_stat = origin_peer.peer_stat.clone();

                    campaigned =
                        new_peer.maybe_campaign(origin_peer, &mut self.pending_raft_groups);

                    if origin_peer.is_leader() {
                        // Notify pd immediately to let it update the region meta.
                        if right_derive {
                            self.report_split_pd(&new_peer, origin_peer);
                        } else {
                            self.report_split_pd(origin_peer, &new_peer);
                        }
                    }
                }

                // Insert new regions and validation
                info!("insert new regions left: {:?}, right:{:?}", left, right);
                if self.region_ranges
                    .insert(enc_end_key(&left), left.get_id())
                    .is_some()
                {
                    panic!("region should not exist, {:?}", left);
                }
                if self.region_ranges
                    .insert(enc_end_key(&right), right.get_id())
                    .is_none()
                {
                    panic!("region should exist, {:?}", right);
                }

                // To prevent from big region, the right region need run split
                // check again after split.
                if right_derive {
                    self.region_peers
                        .get_mut(&region_id)
                        .unwrap()
                        .size_diff_hint = self.cfg.region_split_check_diff.0;
                } else {
                    new_peer.size_diff_hint = self.cfg.region_split_check_diff.0;
                }
                self.apply_worker
                    .schedule(ApplyTask::register(&new_peer))
                    .unwrap();
                self.region_peers.insert(new_region_id, new_peer);
            }
        }

        if !campaigned {
            if let Some(msg) = self.pending_votes
                .swap_remove_front(|m| m.get_to_peer() == &peer)
            {
                let _ = self.on_raft_message(msg);
            }
        }
    }

    fn report_split_pd(&self, left: &Peer, right: &Peer) {
        let left_region = left.region();
        let right_region = right.region();

        info!(
            "notify pd with split left {:?}, right {:?}",
            left_region, right_region
        );
        right.heartbeat_pd(&self.pd_worker);
        left.heartbeat_pd(&self.pd_worker);

        // Now pd only uses ReportSplit for history operation show,
        // so we send it independently here.
        let task = PdTask::ReportSplit {
            left: left_region.clone(),
            right: right_region.clone(),
        };

        if let Err(e) = self.pd_worker.schedule(task) {
            error!("{} failed to notify pd: {}", self.tag, e);
        }
    }

    fn on_ready_apply_snapshot(&mut self, apply_result: ApplySnapResult) {
        let prev_region = apply_result.prev_region;
        let region = apply_result.region;
        let region_id = region.get_id();

        info!(
            "[region {}] snapshot for region {:?} is applied",
            region_id, region
        );

        if !prev_region.get_peers().is_empty() {
            info!(
                "[region {}] region changed from {:?} -> {:?} after applying snapshot",
                region_id, prev_region, region
            );
            // we have already initialized the peer, so it must exist in region_ranges.
            if self.region_ranges
                .remove(&enc_end_key(&prev_region))
                .is_none()
            {
                panic!(
                    "[region {}] region should exist {:?}",
                    region_id, prev_region
                );
            }
        }

        self.region_ranges
            .insert(enc_end_key(&region), region.get_id());
    }

    fn on_ready_result(&mut self, region_id: u64, exec_results: Vec<ExecResult>) {
        // handle executing committed log results
        for result in exec_results {
            match result {
                ExecResult::ChangePeer(cp) => self.on_ready_change_peer(region_id, cp),
                ExecResult::CompactLog { first_index, state } => {
                    self.on_ready_compact_log(region_id, first_index, state)
                }
                ExecResult::SplitRegion {
                    left,
                    right,
                    right_derive,
                } => self.on_ready_split_region(region_id, left, right, right_derive),
                ExecResult::ComputeHash {
                    region,
                    index,
                    snap,
                } => self.on_ready_compute_hash(region, index, snap),
                ExecResult::VerifyHash { index, hash } => {
                    self.on_ready_verify_hash(region_id, index, hash)
                }
                ExecResult::DeleteRange { .. } => {
                    // TODO: clean user properties?
                }
            }
        }
    }

    fn pre_propose_raft_command(
        &mut self,
        msg: &RaftCmdRequest,
    ) -> Result<Option<RaftCmdResponse>> {
        self.validate_store_id(msg)?;
        if msg.has_status_request() {
            // For status commands, we handle it here directly.
            let resp = self.execute_status_command(msg)?;
            return Ok(Some(resp));
        }
        self.validate_region(msg)?;
        Ok(None)
    }

    fn propose_raft_command(&mut self, msg: RaftCmdRequest, cb: Callback) {
        match self.pre_propose_raft_command(&msg) {
            Ok(Some(resp)) => {
                cb.invoke_with_response(resp);
                return;
            }
            Err(e) => {
                cb.invoke_with_response(new_error(e));
                return;
            }
            _ => (),
        }

        // Note:
        // The peer that is being checked is a leader. It might step down to be a follower later. It
        // doesn't matter whether the peer is a leader or not. If it's not a leader, the proposing
        // command log entry can't be committed.

        let mut resp = RaftCmdResponse::new();
        let region_id = msg.get_header().get_region_id();
        let peer = self.region_peers.get_mut(&region_id).unwrap();
        let term = peer.term();
        bind_term(&mut resp, term);
        if peer.propose(cb, msg, resp, &mut self.raft_metrics.propose) {
            peer.mark_to_be_checked(&mut self.pending_raft_groups);
        }

        // TODO: add timeout, if the command is not applied after timeout,
        // we will call the callback with timeout error.
    }

    fn propose_batch_raft_snapshot_command(
        &mut self,
        batch: Vec<RaftCmdRequest>,
        on_finished: Callback,
    ) {
        let size = batch.len();
        BATCH_SNAPSHOT_COMMANDS.observe(size as f64);
        let mut ret = Vec::with_capacity(size);
        for msg in batch {
            match self.pre_propose_raft_command(&msg) {
                Ok(Some(resp)) => {
                    ret.push(Some(ReadResponse {
                        response: resp,
                        snapshot: None,
                    }));
                    continue;
                }
                Err(e) => {
                    ret.push(Some(ReadResponse {
                        response: new_error(e),
                        snapshot: None,
                    }));
                    continue;
                }
                _ => (),
            }

            let region_id = msg.get_header().get_region_id();
            let peer = self.region_peers.get_mut(&region_id).unwrap();
            ret.push(peer.propose_snapshot(msg, &mut self.raft_metrics.propose));
        }
        match on_finished {
            Callback::BatchRead(on_finished) => on_finished(ret),
            _ => unreachable!(),
        }
    }

    fn validate_store_id(&self, msg: &RaftCmdRequest) -> Result<()> {
        let store_id = msg.get_header().get_peer().get_store_id();
        if store_id != self.store.get_id() {
            return Err(Error::StoreNotMatch(store_id, self.store.get_id()));
        }
        Ok(())
    }

    fn validate_region(&self, msg: &RaftCmdRequest) -> Result<()> {
        let region_id = msg.get_header().get_region_id();
        let peer_id = msg.get_header().get_peer().get_id();

        let peer = match self.region_peers.get(&region_id) {
            Some(peer) => peer,
            None => return Err(Error::RegionNotFound(region_id)),
        };
        if !peer.is_leader() {
            return Err(Error::NotLeader(
                region_id,
                peer.get_peer_from_cache(peer.leader_id()),
            ));
        }
        if peer.peer_id() != peer_id {
            return Err(box_err!(
                "mismatch peer id {} != {}",
                peer.peer_id(),
                peer_id
            ));
        }

        let header = msg.get_header();
        // If header's term is 2 verions behind current term, leadership may have been changed away.
        if header.get_term() > 0 && peer.term() > header.get_term() + 1 {
            return Err(Error::StaleCommand);
        }

        let res = peer::check_epoch(peer.region(), msg);
        if let Err(Error::StaleEpoch(msg, mut new_regions)) = res {
            // Attach the region which might be split from the current region. But it doesn't
            // matter if the region is not split from the current region. If the region meta
            // received by the TiKV driver is newer than the meta cached in the driver, the meta is
            // updated.
            let sibling_region_id = self.find_sibling_region(peer.region());
            if let Some(sibling_region_id) = sibling_region_id {
                let sibling_region = self.region_peers[&sibling_region_id].region();
                new_regions.push(sibling_region.to_owned());
            }
            return Err(Error::StaleEpoch(msg, new_regions));
        }
        res
    }

    pub fn find_sibling_region(&self, region: &metapb::Region) -> Option<u64> {
        let start = if self.cfg.right_derive_when_split {
            Included(enc_start_key(region))
        } else {
            Excluded(enc_end_key(region))
        };
        self.region_ranges
            .range((start, Unbounded::<Key>))
            .next()
            .map(|(_, &region_id)| region_id)
    }

    fn register_raft_gc_log_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(
            event_loop,
            Tick::RaftLogGc,
            self.cfg.raft_log_gc_tick_interval.as_millis(),
        ) {
            // If failed, we can't cleanup the raft log regularly.
            // Although the log size will grow larger and larger, it doesn't affect
            // whole raft logic, and we can send truncate log command to compact it.
            error!("{} register raft gc log tick err: {:?}", self.tag, e);
        };
    }

    #[allow(if_same_then_else)]
    fn on_raft_gc_log_tick(&mut self, event_loop: &mut EventLoop<Self>) {
        let mut total_gc_logs = 0;

        for (&region_id, peer) in &mut self.region_peers {
            if !peer.is_leader() {
                continue;
            }

            // Leader will replicate the compact log command to followers,
            // If we use current replicated_index (like 10) as the compact index,
            // when we replicate this log, the newest replicated_index will be 11,
            // but we only compact the log to 10, not 11, at that time,
            // the first index is 10, and replicated_index is 11, with an extra log,
            // and we will do compact again with compact index 11, in cycles...
            // So we introduce a threshold, if replicated index - first index > threshold,
            // we will try to compact log.
            // raft log entries[..............................................]
            //                  ^                                       ^
            //                  |-----------------threshold------------ |
            //              first_index                         replicated_index
            let replicated_idx = peer.raft_group
                .raft
                .prs()
                .iter()
                .map(|(_, p)| p.matched)
                .min()
                .unwrap();
            // When an election happened or a new peer is added, replicated_idx can be 0.
            if replicated_idx > 0 {
                let last_idx = peer.raft_group.raft.raft_log.last_index();
                assert!(
                    last_idx >= replicated_idx,
                    "expect last index {} >= replicated index {}",
                    last_idx,
                    replicated_idx
                );
                REGION_MAX_LOG_LAG.observe((last_idx - replicated_idx) as f64);
            }
            let applied_idx = peer.get_store().applied_index();
            let first_idx = peer.get_store().first_index();
            let mut compact_idx;
            if applied_idx > first_idx
                && applied_idx - first_idx >= self.cfg.raft_log_gc_count_limit
            {
                compact_idx = applied_idx;
            } else if peer.raft_log_size_hint >= self.cfg.raft_log_gc_size_limit.0 {
                compact_idx = applied_idx;
            } else if replicated_idx < first_idx
                || replicated_idx - first_idx <= self.cfg.raft_log_gc_threshold
            {
                continue;
            } else {
                compact_idx = replicated_idx;
            }

            // Have no idea why subtract 1 here, but original code did this by magic.
            assert!(compact_idx > 0);
            compact_idx -= 1;
            if compact_idx < first_idx {
                // In case compact_idx == first_idx before subtraction.
                continue;
            }

            total_gc_logs += compact_idx - first_idx;

            let term = peer.raft_group.raft.raft_log.term(compact_idx).unwrap();

            // Create a compact log request and notify directly.
            let request = new_compact_log_request(region_id, peer.peer.clone(), compact_idx, term);

            if let Err(e) = self.sendch
                .try_send(Msg::new_raft_cmd(request, Callback::None))
            {
                error!("{} send compact log {} err {:?}", peer.tag, compact_idx, e);
            }
        }

        PEER_GC_RAFT_LOG_COUNTER
            .inc_by(total_gc_logs as f64)
            .unwrap();
        self.register_raft_gc_log_tick(event_loop);
    }

    fn register_split_region_check_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(
            event_loop,
            Tick::SplitRegionCheck,
            self.cfg.split_region_check_tick_interval.as_millis(),
        ) {
            error!("{} register split region check tick err: {:?}", self.tag, e);
        };
    }

    fn on_compaction_finished(&mut self, event: CompactedEvent) {
        // If size declining is trivial, skip.
        let total_bytes_declined = if event.total_input_bytes > event.total_output_bytes {
            event.total_input_bytes - event.total_output_bytes
        } else {
            0
        };
        if total_bytes_declined < self.cfg.region_split_check_diff.0
            || total_bytes_declined * 10 < event.total_input_bytes
        {
            return;
        }

        let output_level_str = event.output_level.to_string();
        COMPACTION_DECLINED_BYTES
            .with_label_values(&[&output_level_str])
            .observe(total_bytes_declined as f64);

        // self.cfg.region_split_check_diff.0 / 16 is an experienced value.
        let mut region_declined_bytes = calc_region_declined_bytes(
            event,
            &self.region_ranges,
            self.cfg.region_split_check_diff.0 / 16,
        );

        COMPACTION_RELATED_REGION_COUNT
            .with_label_values(&[&output_level_str])
            .observe(region_declined_bytes.len() as f64);

        for (region_id, declined_bytes) in region_declined_bytes.drain(..) {
            if let Some(peer) = self.region_peers.get_mut(&region_id) {
                peer.compaction_declined_bytes += declined_bytes;
                if peer.compaction_declined_bytes >= self.cfg.region_split_check_diff.0 {
                    UPDATE_REGION_SIZE_BY_COMPACTION_COUNTER.inc();
                }
            }
        }
    }

    fn on_split_region_check_tick(&mut self, event_loop: &mut EventLoop<Self>) {
        // To avoid frequent scan, we only add new scan tasks if all previous tasks
        // have finished.
        // TODO: check whether a gc progress has been started.
        if self.split_check_worker.is_busy() {
            self.register_split_region_check_tick(event_loop);
            return;
        }
        for peer in self.region_peers.values_mut() {
            if !peer.is_leader() {
                continue;
            }
            // When restart, the approximate size will be None. The
            // split check will first check the region size, and then
            // check whether the region should split.  This should
            // work even if we change the region max size.
            // If peer says should update approximate size, update region
            // size and check whether the region should split.
            if peer.approximate_size.is_some()
                && peer.compaction_declined_bytes < self.cfg.region_split_check_diff.0
                && peer.size_diff_hint < self.cfg.region_split_check_diff.0
            {
                continue;
            }
            let task = SplitCheckTask::new(peer.region());
            if let Err(e) = self.split_check_worker.schedule(task) {
                error!("{} failed to schedule split check: {}", self.tag, e);
            }
            peer.size_diff_hint = 0;
            peer.compaction_declined_bytes = 0;
        }

        self.register_split_region_check_tick(event_loop);
    }

    fn register_compact_check_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(
            event_loop,
            Tick::CompactCheck,
            self.cfg.region_compact_check_interval.as_millis(),
        ) {
            error!("{} register compact check tick err: {:?}", self.tag, e);
        }
    }

    fn on_compact_check_tick(&mut self, event_loop: &mut EventLoop<Self>) {
        for peer in self.region_peers.values_mut() {
            if peer.delete_keys_hint < self.cfg.region_compact_delete_keys_count {
                continue;
            }
            for &cf in &[CF_DEFAULT, CF_WRITE] {
                let task = CompactTask {
                    cf_name: String::from(cf),
                    start_key: Some(keys::enc_start_key(peer.region())),
                    end_key: Some(keys::enc_end_key(peer.region())),
                };
                if let Err(e) = self.compact_worker.schedule(task) {
                    error!("{} failed to schedule compact task: {}", self.tag, e);
                }
            }
            peer.delete_keys_hint = 0;
            // Compact only 1 region each check in case compact task accumulates.
            break;
        }
        self.register_compact_check_tick(event_loop);
    }

    fn on_prepare_split_region(
        &mut self,
        region_id: u64,
        region_epoch: metapb::RegionEpoch,
        split_key: Vec<u8>, // `split_key` is a encoded key.
        cb: Callback,
    ) {
        if let Err(e) = self.validate_split_region(region_id, &region_epoch, &split_key) {
            cb.invoke_with_response(new_error(e));
            return;
        }
        let peer = &self.region_peers[&region_id];
        let region = peer.region();
        let task = PdTask::AskSplit {
            region: region.clone(),
            split_key: split_key,
            peer: peer.peer.clone(),
            right_derive: self.cfg.right_derive_when_split,
            callback: cb,
        };
        if let Err(Stopped(t)) = self.pd_worker.schedule(task) {
            error!("{} failed to notify pd to split: Stopped", peer.tag);
            match t {
                PdTask::AskSplit { callback, .. } => {
                    callback.invoke_with_response(new_error(box_err!("failed to split: Stopped")));
                }
                _ => unreachable!(),
            }
        }
    }

    fn validate_split_region(
        &mut self,
        region_id: u64,
        epoch: &metapb::RegionEpoch,
        split_key: &[u8], // `split_key` is a encoded key.
    ) -> Result<()> {
        if split_key.is_empty() {
            error!("[region {}] split key should not be empty!!!", region_id);
            return Err(box_err!(
                "[region {}] split key should not be empty",
                region_id
            ));
        }
        let peer = match self.region_peers.get(&region_id) {
            None => {
                info!(
                    "[region {}] region on {} doesn't exist, skip.",
                    region_id,
                    self.store_id()
                );
                return Err(Error::RegionNotFound(region_id));
            }
            Some(peer) => {
                if !peer.is_leader() {
                    // region on this store is no longer leader, skipped.
                    info!(
                        "[region {}] region on {} is not leader, skip.",
                        region_id,
                        self.store_id()
                    );
                    return Err(Error::NotLeader(
                        region_id,
                        peer.get_peer_from_cache(peer.leader_id()),
                    ));
                }
                peer
            }
        };

        let region = peer.region();
        let latest_epoch = region.get_region_epoch();

        if latest_epoch.get_version() != epoch.get_version() {
            info!(
                "{} epoch changed {:?} != {:?}, retry later",
                peer.tag,
                region.get_region_epoch(),
                epoch
            );
            return Err(Error::StaleEpoch(
                format!(
                    "{} epoch changed {:?} != {:?}, retry later",
                    peer.tag, latest_epoch, epoch
                ),
                vec![region.to_owned()],
            ));
        }
        Ok(())
    }

    fn on_approximate_region_size(&mut self, region_id: u64, region_size: u64) {
        let peer = match self.region_peers.get_mut(&region_id) {
            Some(peer) => peer,
            None => {
                warn!(
                    "[region {}] receive stale approximate size {}",
                    region_id, region_size,
                );
                return;
            }
        };
        peer.approximate_size = Some(region_size);
    }

    fn on_pd_heartbeat_tick(&mut self, event_loop: &mut EventLoop<Self>) {
        for peer in self.region_peers.values_mut() {
            peer.check_peers();
        }
        let mut leader_count = 0;
        for peer in self.region_peers.values() {
            if peer.is_leader() {
                leader_count += 1;
                peer.heartbeat_pd(&self.pd_worker);
            }
        }
        STORE_PD_HEARTBEAT_GAUGE_VEC
            .with_label_values(&["leader"])
            .set(f64::from(leader_count));
        STORE_PD_HEARTBEAT_GAUGE_VEC
            .with_label_values(&["region"])
            .set(self.region_peers.len() as f64);

        self.register_pd_heartbeat_tick(event_loop);
    }

    fn register_pd_heartbeat_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(
            event_loop,
            Tick::PdHeartbeat,
            self.cfg.pd_heartbeat_tick_interval.as_millis(),
        ) {
            error!("{} register pd heartbeat tick err: {:?}", self.tag, e);
        };
    }

    fn store_heartbeat_pd(&mut self) {
        let mut stats = StoreStats::new();

        let used_size = self.snap_mgr.get_total_snap_size();
        stats.set_used_size(used_size);
        stats.set_store_id(self.store_id());
        stats.set_region_count(self.region_peers.len() as u32);

        let snap_stats = self.snap_mgr.stats();
        stats.set_sending_snap_count(snap_stats.sending_count as u32);
        stats.set_receiving_snap_count(snap_stats.receiving_count as u32);
        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
            .with_label_values(&["sending"])
            .set(snap_stats.sending_count as f64);
        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
            .with_label_values(&["receiving"])
            .set(snap_stats.receiving_count as f64);

        let mut apply_snapshot_count = 0;
        for peer in self.region_peers.values_mut() {
            if peer.mut_store().check_applying_snap() {
                apply_snapshot_count += 1;
            }
        }

        stats.set_applying_snap_count(apply_snapshot_count as u32);
        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
            .with_label_values(&["applying"])
            .set(f64::from(apply_snapshot_count));

        stats.set_start_time(self.start_time.sec as u32);

        // report store write flow to pd
        stats.set_bytes_written(
            self.store_stat.engine_total_bytes_written
                - self.store_stat.engine_last_total_bytes_written,
        );
        stats.set_keys_written(
            self.store_stat.engine_total_keys_written
                - self.store_stat.engine_last_total_keys_written,
        );
        self.store_stat.engine_last_total_bytes_written =
            self.store_stat.engine_total_bytes_written;
        self.store_stat.engine_last_total_keys_written = self.store_stat.engine_total_keys_written;

        stats.set_is_busy(self.is_busy);
        self.is_busy = false;

        let store_info = StoreInfo {
            engine: Arc::clone(&self.kv_engine),
            capacity: self.cfg.capacity.0,
        };

        let task = PdTask::StoreHeartbeat {
            stats: stats,
            store_info: store_info,
        };
        if let Err(e) = self.pd_worker.schedule(task) {
            error!("{} failed to notify pd: {}", self.tag, e);
        }
    }

    fn on_pd_store_heartbeat_tick(&mut self, event_loop: &mut EventLoop<Self>) {
        self.store_heartbeat_pd();
        self.register_pd_store_heartbeat_tick(event_loop);
    }

    fn handle_snap_mgr_gc(&mut self) -> Result<()> {
        let snap_keys = self.snap_mgr.list_idle_snap()?;
        if snap_keys.is_empty() {
            return Ok(());
        }
        let (mut last_region_id, mut compacted_idx, mut compacted_term) = (0, u64::MAX, u64::MAX);
        let mut is_applying_snap = false;
        for (key, is_sending) in snap_keys {
            if last_region_id != key.region_id {
                last_region_id = key.region_id;
                match self.region_peers.get(&key.region_id) {
                    None => {
                        // region is deleted
                        compacted_idx = u64::MAX;
                        compacted_term = u64::MAX;
                        is_applying_snap = false;
                    }
                    Some(peer) => {
                        let s = peer.get_store();
                        compacted_idx = s.truncated_index();
                        compacted_term = s.truncated_term();
                        is_applying_snap = s.is_applying_snapshot();
                    }
                };
            }

            if is_sending {
                let s = self.snap_mgr.get_snapshot_for_sending(&key)?;
                if key.term < compacted_term || key.idx < compacted_idx {
                    info!(
                        "[region {}] snap file {} has been compacted, delete.",
                        key.region_id, key
                    );
                    self.snap_mgr.delete_snapshot(&key, s.as_ref(), false);
                } else if let Ok(meta) = s.meta() {
                    let modified = box_try!(meta.modified());
                    if let Ok(elapsed) = modified.elapsed() {
                        if elapsed > self.cfg.snap_gc_timeout.0 {
                            info!(
                                "[region {}] snap file {} has been expired, delete.",
                                key.region_id, key
                            );
                            self.snap_mgr.delete_snapshot(&key, s.as_ref(), false);
                        }
                    }
                }
            } else if key.term <= compacted_term
                && (key.idx < compacted_idx || key.idx == compacted_idx && !is_applying_snap)
            {
                info!(
                    "[region {}] snap file {} has been applied, delete.",
                    key.region_id, key
                );
                let a = self.snap_mgr.get_snapshot_for_applying(&key)?;
                self.snap_mgr.delete_snapshot(&key, a.as_ref(), false);
            }
        }
        Ok(())
    }

    fn on_snap_mgr_gc(&mut self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = self.handle_snap_mgr_gc() {
            error!("{} failed to gc snap manager: {:?}", self.tag, e);
        }
        self.register_snap_mgr_gc_tick(event_loop);
    }

    fn on_compact_lock_cf(&mut self, event_loop: &mut EventLoop<Self>) {
        // Create a compact lock cf task(compact whole range) and schedule directly.
        if self.store_stat.lock_cf_bytes_written > self.cfg.lock_cf_compact_bytes_threshold.0 {
            self.store_stat.lock_cf_bytes_written = 0;
            let task = CompactTask {
                cf_name: String::from(CF_LOCK),
                start_key: None,
                end_key: None,
            };
            if let Err(e) = self.compact_worker.schedule(task) {
                error!(
                    "{} failed to schedule compact lock cf task: {:?}",
                    self.tag, e
                );
            }
        }

        self.register_compact_lock_cf_tick(event_loop);
    }

    fn register_pd_store_heartbeat_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(
            event_loop,
            Tick::PdStoreHeartbeat,
            self.cfg.pd_store_heartbeat_tick_interval.as_millis(),
        ) {
            error!("{} register pd store heartbeat tick err: {:?}", self.tag, e);
        };
    }

    fn register_snap_mgr_gc_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(
            event_loop,
            Tick::SnapGc,
            self.cfg.snap_mgr_gc_tick_interval.as_millis(),
        ) {
            error!("{} register snap mgr gc tick err: {:?}", self.tag, e);
        }
    }

    fn register_compact_lock_cf_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(
            event_loop,
            Tick::CompactLockCf,
            self.cfg.lock_cf_compact_interval.as_millis(),
        ) {
            error!("{} register compact cf-lock tick err: {:?}", self.tag, e);
        }
    }
}

// Consistency Check implementation.

/// Verify and store the hash to state. return true means the hash has been stored successfully.
fn verify_and_store_hash(
    region_id: u64,
    state: &mut ConsistencyState,
    expected_index: u64,
    expected_hash: Vec<u8>,
) -> bool {
    if expected_index < state.index {
        REGION_HASH_COUNTER_VEC
            .with_label_values(&["verify", "miss"])
            .inc();
        warn!(
            "[region {}] has scheduled a new hash: {} > {}, skip.",
            region_id, state.index, expected_index
        );
        return false;
    }

    if state.index == expected_index {
        if state.hash.is_empty() {
            warn!(
                "[region {}] duplicated consistency check detected, skip.",
                region_id
            );
            return false;
        }
        if state.hash != expected_hash {
            panic!(
                "[region {}] hash at {} not correct, want \"{}\", got \"{}\"!!!",
                region_id,
                state.index,
                escape(&expected_hash),
                escape(&state.hash)
            );
        }
        info!(
            "[region {}] consistency check at {} pass.",
            region_id, state.index
        );
        REGION_HASH_COUNTER_VEC
            .with_label_values(&["verify", "matched"])
            .inc();
        state.hash = vec![];
        return false;
    }

    if state.index != INVALID_INDEX && !state.hash.is_empty() {
        // Maybe computing is too slow or computed result is dropped due to channel full.
        // If computing is too slow, miss count will be increased twice.
        REGION_HASH_COUNTER_VEC
            .with_label_values(&["verify", "miss"])
            .inc();
        warn!(
            "[region {}] hash belongs to index {}, but we want {}, skip.",
            region_id, state.index, expected_index
        );
    }

    info!(
        "[region {}] save hash of {} for consistency check later.",
        region_id, expected_index
    );
    state.index = expected_index;
    state.hash = expected_hash;
    true
}

impl<T: Transport, C: PdClient> Store<T, C> {
    fn register_consistency_check_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(
            event_loop,
            Tick::ConsistencyCheck,
            self.cfg.consistency_check_interval.as_millis(),
        ) {
            error!("{} register consistency check tick err: {:?}", self.tag, e);
        };
    }

    fn on_consistency_check_tick(&mut self, event_loop: &mut EventLoop<Self>) {
        if self.consistency_check_worker.is_busy() {
            // To avoid frequent scan, schedule new check only when all the
            // scheduled check is done.
            self.register_consistency_check_tick(event_loop);
            return;
        }
        let (mut candidate_id, mut candidate_check_time) = (0, Instant::now());
        for (&region_id, peer) in &mut self.region_peers {
            if !peer.is_leader() {
                continue;
            }
            if peer.consistency_state.last_check_time < candidate_check_time {
                candidate_id = region_id;
                candidate_check_time = peer.consistency_state.last_check_time;
            }
        }

        if candidate_id != 0 {
            let peer = &self.region_peers[&candidate_id];

            info!("{} scheduling consistent check", peer.tag);
            let msg = Msg::new_raft_cmd(
                new_compute_hash_request(candidate_id, peer.peer.clone()),
                Callback::None,
            );

            if let Err(e) = self.sendch.send(msg) {
                error!("{} failed to schedule consistent check: {:?}", peer.tag, e);
            }
        }

        self.register_consistency_check_tick(event_loop);
    }

    fn on_ready_compute_hash(&mut self, region: metapb::Region, index: u64, snap: EngineSnapshot) {
        let region_id = region.get_id();
        self.region_peers
            .get_mut(&region_id)
            .unwrap()
            .consistency_state
            .last_check_time = Instant::now();
        let task = ConsistencyCheckTask::compute_hash(region, index, snap);
        info!("[region {}] schedule {}", region_id, task);
        if let Err(e) = self.consistency_check_worker.schedule(task) {
            error!("[region {}] schedule failed: {:?}", region_id, e);
        }
    }

    fn on_ready_verify_hash(
        &mut self,
        region_id: u64,
        expected_index: u64,
        expected_hash: Vec<u8>,
    ) {
        let state = match self.region_peers.get_mut(&region_id) {
            None => {
                warn!(
                    "[region {}] receive stale hash at index {}",
                    region_id, expected_index
                );
                return;
            }
            Some(p) => &mut p.consistency_state,
        };

        verify_and_store_hash(region_id, state, expected_index, expected_hash);
    }

    fn on_hash_computed(&mut self, region_id: u64, index: u64, hash: Vec<u8>) {
        let (state, peer) = match self.region_peers.get_mut(&region_id) {
            None => {
                warn!(
                    "[region {}] receive stale hash at index {}",
                    region_id, index
                );
                return;
            }
            Some(p) => (&mut p.consistency_state, &p.peer),
        };

        if !verify_and_store_hash(region_id, state, index, hash) {
            return;
        }

        let msg = Msg::new_raft_cmd(
            new_verify_hash_request(region_id, peer.clone(), state),
            Callback::None,
        );
        if let Err(e) = self.sendch.send(msg) {
            error!(
                "[region {}] failed to schedule verify command for index {}: {:?}",
                region_id, index, e
            );
        }
    }
}

fn new_admin_request(region_id: u64, peer: metapb::Peer) -> RaftCmdRequest {
    let mut request = RaftCmdRequest::new();
    request.mut_header().set_region_id(region_id);
    request.mut_header().set_peer(peer);
    request
}

fn new_verify_hash_request(
    region_id: u64,
    peer: metapb::Peer,
    state: &ConsistencyState,
) -> RaftCmdRequest {
    let mut request = new_admin_request(region_id, peer);

    let mut admin = AdminRequest::new();
    admin.set_cmd_type(AdminCmdType::VerifyHash);
    admin.mut_verify_hash().set_index(state.index);
    admin.mut_verify_hash().set_hash(state.hash.clone());
    request.set_admin_request(admin);
    request
}

fn new_compute_hash_request(region_id: u64, peer: metapb::Peer) -> RaftCmdRequest {
    let mut request = new_admin_request(region_id, peer);

    let mut admin = AdminRequest::new();
    admin.set_cmd_type(AdminCmdType::ComputeHash);
    request.set_admin_request(admin);
    request
}

fn register_timer<T: Transport, C: PdClient>(
    event_loop: &mut EventLoop<Store<T, C>>,
    tick: Tick,
    delay: u64,
) -> Result<()> {
    // TODO: now mio TimerError doesn't implement Error trait,
    // so we can't use `try!` directly.
    if delay == 0 {
        // 0 delay means turn off the timer.
        return Ok(());
    }
    if let Err(e) = event_loop.timeout_ms(tick, delay) {
        return Err(box_err!(
            "failed to register timeout [{:?}, delay: {:?}ms]: {:?}",
            tick,
            delay,
            e
        ));
    }
    Ok(())
}

fn new_compact_log_request(
    region_id: u64,
    peer: metapb::Peer,
    compact_index: u64,
    compact_term: u64,
) -> RaftCmdRequest {
    let mut request = new_admin_request(region_id, peer);

    let mut admin = AdminRequest::new();
    admin.set_cmd_type(AdminCmdType::CompactLog);
    admin.mut_compact_log().set_compact_index(compact_index);
    admin.mut_compact_log().set_compact_term(compact_term);
    request.set_admin_request(admin);
    request
}

impl<T: Transport, C: PdClient> mio::Handler for Store<T, C> {
    type Timeout = Tick;
    type Message = Msg;

    fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: Msg) {
        match msg {
            Msg::RaftMessage(data) => if let Err(e) = self.on_raft_message(data) {
                error!("{} handle raft message err: {:?}", self.tag, e);
            },
            Msg::RaftCmd {
                send_time,
                request,
                callback,
            } => {
                self.raft_metrics
                    .propose
                    .request_wait_time
                    .observe(duration_to_sec(send_time.elapsed()) as f64);
                self.propose_raft_command(request, callback)
            }
            // For now, it is only called by batch snapshot.
            Msg::BatchRaftSnapCmds {
                send_time,
                batch,
                on_finished,
            } => {
                self.raft_metrics
                    .propose
                    .request_wait_time
                    .observe(duration_to_sec(send_time.elapsed()) as f64);
                self.propose_batch_raft_snapshot_command(batch, on_finished);
            }
            Msg::Quit => {
                info!("{} receive quit message", self.tag);
                event_loop.shutdown();
            }
            Msg::SnapshotStats => self.store_heartbeat_pd(),
            Msg::ComputeHashResult {
                region_id,
                index,
                hash,
            } => {
                self.on_hash_computed(region_id, index, hash);
            }
            Msg::SplitRegion {
                region_id,
                region_epoch,
                split_key,
                callback,
            } => {
                info!(
                    "[region {}] on split region at key {:?}.",
                    region_id, split_key
                );
                self.on_prepare_split_region(region_id, region_epoch, split_key, callback);
            }
            Msg::ApproximateRegionSize {
                region_id,
                region_size,
            } => self.on_approximate_region_size(region_id, region_size),
            Msg::CompactedEvent(event) => self.on_compaction_finished(event),
        }
    }

    fn timeout(&mut self, event_loop: &mut EventLoop<Self>, timeout: Tick) {
        let t = SlowTimer::new();
        match timeout {
            Tick::Raft => self.on_raft_base_tick(event_loop),
            Tick::RaftLogGc => self.on_raft_gc_log_tick(event_loop),
            Tick::SplitRegionCheck => self.on_split_region_check_tick(event_loop),
            Tick::CompactCheck => self.on_compact_check_tick(event_loop),
            Tick::PdHeartbeat => self.on_pd_heartbeat_tick(event_loop),
            Tick::PdStoreHeartbeat => self.on_pd_store_heartbeat_tick(event_loop),
            Tick::SnapGc => self.on_snap_mgr_gc(event_loop),
            Tick::CompactLockCf => self.on_compact_lock_cf(event_loop),
            Tick::ConsistencyCheck => self.on_consistency_check_tick(event_loop),
        }
        slow_log!(t, "{} handle timeout {:?}", self.tag, timeout);
    }

    // This method is invoked very frequently, should avoid time consuming operation.
    fn tick(&mut self, event_loop: &mut EventLoop<Self>) {
        if !event_loop.is_running() {
            self.stop();
            return;
        }

        // We handle raft ready in event loop.
        if !self.pending_raft_groups.is_empty() {
            self.on_raft_ready();
        }

        self.poll_apply();

        self.pending_snapshot_regions.clear();
    }
}

impl<T: Transport, C: PdClient> Store<T, C> {
    /// load the target peer of request as mutable borrow.
    fn mut_target_peer(&mut self, request: &RaftCmdRequest) -> Result<&mut Peer> {
        let region_id = request.get_header().get_region_id();
        match self.region_peers.get_mut(&region_id) {
            None => Err(Error::RegionNotFound(region_id)),
            Some(peer) => Ok(peer),
        }
    }

    // Handle status commands here, separate the logic, maybe we can move it
    // to another file later.
    // Unlike other commands (write or admin), status commands only show current
    // store status, so no need to handle it in raft group.
    fn execute_status_command(&mut self, request: &RaftCmdRequest) -> Result<RaftCmdResponse> {
        let cmd_type = request.get_status_request().get_cmd_type();
        let region_id = request.get_header().get_region_id();

        let mut response = match cmd_type {
            StatusCmdType::RegionLeader => self.execute_region_leader(request),
            StatusCmdType::RegionDetail => self.execute_region_detail(request),
            StatusCmdType::InvalidStatus => Err(box_err!("invalid status command!")),
        }?;
        response.set_cmd_type(cmd_type);

        let mut resp = RaftCmdResponse::new();
        resp.set_status_response(response);
        // Bind peer current term here.
        if let Some(peer) = self.region_peers.get(&region_id) {
            bind_term(&mut resp, peer.term());
        }
        Ok(resp)
    }

    fn execute_region_leader(&mut self, request: &RaftCmdRequest) -> Result<StatusResponse> {
        let peer = self.mut_target_peer(request)?;

        let mut resp = StatusResponse::new();
        if let Some(leader) = peer.get_peer_from_cache(peer.leader_id()) {
            resp.mut_region_leader().set_leader(leader);
        }

        Ok(resp)
    }

    fn execute_region_detail(&mut self, request: &RaftCmdRequest) -> Result<StatusResponse> {
        let peer = self.mut_target_peer(request)?;
        if !peer.get_store().is_initialized() {
            let region_id = request.get_header().get_region_id();
            return Err(Error::RegionNotInitialized(region_id));
        }
        let mut resp = StatusResponse::new();
        resp.mut_region_detail().set_region(peer.region().clone());
        if let Some(leader) = peer.get_peer_from_cache(peer.leader_id()) {
            resp.mut_region_detail().set_leader(leader);
        }

        Ok(resp)
    }
}

fn size_change_filter(info: &CompactionJobInfo) -> bool {
    // When calculating region size, we only consider write and default
    // column families.
    let cf = info.cf_name();
    if cf != CF_WRITE && cf != CF_DEFAULT {
        return false;
    }
    // Compactions in level 0 and level 1 are very frequently.
    if info.output_level() < 2 {
        return false;
    }

    true
}

pub fn new_compaction_listener(ch: SendCh<Msg>) -> CompactionListener {
    let compacted_handler = box move |compacted_event: CompactedEvent| {
        if let Err(e) = ch.try_send(Msg::CompactedEvent(compacted_event)) {
            error!(
                "Send compaction finished event to raftstore failed: {:?}",
                e
            );
        }
    };
    CompactionListener::new(compacted_handler, Some(size_change_filter))
}

fn calc_region_declined_bytes(
    event: CompactedEvent,
    region_ranges: &BTreeMap<Key, u64>,
    bytes_threshold: u64,
) -> Vec<(u64, u64)> {
    // Calculate influenced regions.
    let mut influenced_regions = vec![];
    for (end_key, region_id) in
        region_ranges.range((Excluded(event.start_key), Included(event.end_key.clone())))
    {
        influenced_regions.push((region_id, end_key.clone()));
    }
    if let Some((end_key, region_id)) = region_ranges
        .range((Included(event.end_key), Unbounded))
        .next()
    {
        influenced_regions.push((region_id, end_key.clone()));
    }

    // Calculate declined bytes for each region.
    // `end_key` in influenced_regions are in incremental order.
    let mut region_declined_bytes = vec![];
    let mut last_end_key: Vec<u8> = vec![];
    for (region_id, end_key) in influenced_regions {
        let mut old_size = 0;
        for prop in &event.input_props {
            old_size += prop.get_approximate_size_in_range(&last_end_key, &end_key);
        }
        let mut new_size = 0;
        for prop in &event.output_props {
            new_size += prop.get_approximate_size_in_range(&last_end_key, &end_key);
        }
        last_end_key = end_key;

        // Filter some trivial declines for better performance.
        if old_size > new_size && old_size - new_size > bytes_threshold {
            region_declined_bytes.push((*region_id, old_size - new_size));
        }
    }

    region_declined_bytes
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use util::rocksdb::CompactedEvent;
    use util::rocksdb::properties::{IndexHandle, IndexHandles, SizeProperties};

    use super::*;

    #[test]
    fn test_calc_region_declined_bytes() {
        let index_handle1 = IndexHandle {
            size: 4 * 1024,
            offset: 4 * 1024,
        };
        let index_handle2 = IndexHandle {
            size: 4 * 1024,
            offset: 8 * 1024,
        };
        let index_handle3 = IndexHandle {
            size: 4 * 1024,
            offset: 12 * 1024,
        };
        let mut index_handles = IndexHandles::new();
        index_handles.add(b"a".to_vec(), index_handle1);
        index_handles.add(b"b".to_vec(), index_handle2);
        index_handles.add(b"c".to_vec(), index_handle3);
        let size_prop = SizeProperties {
            total_size: 12 * 1024,
            index_handles: index_handles,
        };
        let event = CompactedEvent {
            cf: "default".to_owned(),
            output_level: 3,
            total_input_bytes: 12 * 1024,
            total_output_bytes: 0,
            start_key: size_prop.smallest_key().unwrap(),
            end_key: size_prop.largest_key().unwrap(),
            input_props: vec![size_prop],
            output_props: vec![],
        };

        let mut region_ranges = BTreeMap::new();
        region_ranges.insert(b"a".to_vec(), 1);
        region_ranges.insert(b"b".to_vec(), 2);
        region_ranges.insert(b"c".to_vec(), 3);

        let declined_bytes = calc_region_declined_bytes(event, &region_ranges, 1024);
        let expected_declined_bytes = vec![(2, 8192), (3, 4096)];
        assert_eq!(declined_bytes, expected_declined_bytes);
    }
}
