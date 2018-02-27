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

use kvproto::raft_serverpb::RaftMessage;
use kvproto::eraftpb::MessageType;
use tikv::raftstore::{Error, Result};
use tikv::raftstore::store::{Msg as StoreMsg, SignificantMsg, Transport};
use tikv::server::transport::*;
use tikv::server::StoreAddrResolver;
use tikv::util::{transport, Either, HandyRwLock};

use rand;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::sync::mpsc::Sender;
use std::marker::PhantomData;
use std::{thread, time, usize};
use std::sync::atomic::*;

pub trait Channel<M>: Send + Clone {
    fn send(&self, m: M) -> Result<()>;
    fn significant_send(&self, _: SignificantMsg) -> Result<()> {
        unimplemented!()
    }
    fn flush(&mut self) {}
}

impl<T, S> Channel<RaftMessage> for ServerTransport<T, S>
where
    T: RaftStoreRouter,
    S: StoreAddrResolver,
{
    fn send(&self, m: RaftMessage) -> Result<()> {
        Transport::send(self, m)
    }

    fn flush(&mut self) {
        self.flush_raft_client();
    }
}

impl Channel<StoreMsg> for ServerRaftStoreRouter {
    fn send(&self, m: StoreMsg) -> Result<()> {
        RaftStoreRouter::try_send(self, m)
    }

    fn significant_send(&self, msg: SignificantMsg) -> Result<()> {
        RaftStoreRouter::significant_send(self, msg)
    }
}

pub fn check_messages<M>(msgs: &[M]) -> Result<()> {
    if msgs.is_empty() {
        Err(Error::Transport(transport::Error::Discard(String::from(
            "messages dropped by \
             SimulateTransport Filter",
        ))))
    } else {
        Ok(())
    }
}

pub trait Filter<M>: Send + Sync {
    /// `before` is run before sending the messages.
    fn before(&self, msgs: &mut Vec<M>) -> Result<()>;
    /// `after` is run after sending the messages,
    /// so that the returned value could be changed if necessary.
    fn after(&self, res: Result<()>) -> Result<()> {
        res
    }
}

pub type SendFilter = Box<Filter<RaftMessage>>;
pub type RecvFilter = Box<Filter<StoreMsg>>;

#[derive(Clone)]
pub struct DropPacketFilter {
    rate: u32,
}

impl DropPacketFilter {
    pub fn new(rate: u32) -> DropPacketFilter {
        DropPacketFilter { rate: rate }
    }
}

impl<M> Filter<M> for DropPacketFilter {
    fn before(&self, msgs: &mut Vec<M>) -> Result<()> {
        msgs.retain(|_| rand::random::<u32>() % 100u32 >= self.rate);
        check_messages(msgs)
    }
}

#[derive(Clone)]
pub struct DelayFilter {
    duration: time::Duration,
}

impl DelayFilter {
    pub fn new(duration: time::Duration) -> DelayFilter {
        DelayFilter { duration: duration }
    }
}

impl<M> Filter<M> for DelayFilter {
    fn before(&self, _: &mut Vec<M>) -> Result<()> {
        thread::sleep(self.duration);
        Ok(())
    }
}

pub struct SimulateTransport<M, C: Channel<M>> {
    filters: Arc<RwLock<Vec<Box<Filter<M>>>>>,
    ch: Arc<Mutex<C>>,
}

impl<M, C: Channel<M>> SimulateTransport<M, C> {
    pub fn new(ch: C) -> SimulateTransport<M, C> {
        SimulateTransport {
            filters: Arc::new(RwLock::new(vec![])),
            ch: Arc::new(Mutex::new(ch)),
        }
    }

    pub fn clear_filters(&mut self) {
        self.filters.wl().clear();
    }

    pub fn add_filter(&mut self, filter: Box<Filter<M>>) {
        self.filters.wl().push(filter);
    }
}

impl<M, C: Channel<M>> Channel<M> for SimulateTransport<M, C> {
    fn send(&self, msg: M) -> Result<()> {
        let mut taken = 0;
        let mut msgs = vec![msg];
        let filters = self.filters.rl();
        let mut res = Ok(());
        for filter in filters.iter() {
            taken += 1;
            res = filter.before(&mut msgs);
            if res.is_err() {
                break;
            }
        }
        if res.is_ok() {
            for msg in msgs {
                res = self.ch.lock().unwrap().send(msg);
                if res.is_err() {
                    break;
                }
            }
        }
        for filter in filters[..taken].iter().rev() {
            res = filter.after(res);
        }
        res
    }
}

impl<M, C: Channel<M>> Clone for SimulateTransport<M, C> {
    fn clone(&self) -> SimulateTransport<M, C> {
        SimulateTransport {
            filters: Arc::clone(&self.filters),
            ch: Arc::clone(&self.ch),
        }
    }
}

impl<C: Channel<RaftMessage>> Transport for SimulateTransport<RaftMessage, C> {
    fn send(&self, m: RaftMessage) -> Result<()> {
        Channel::send(self, m)
    }

    fn flush(&mut self) {
        self.ch.lock().unwrap().flush();
    }
}

impl<C: Channel<StoreMsg>> RaftStoreRouter for SimulateTransport<StoreMsg, C> {
    fn send(&self, m: StoreMsg) -> Result<()> {
        Channel::send(self, m)
    }

    fn try_send(&self, m: StoreMsg) -> Result<()> {
        Channel::send(self, m)
    }

    fn significant_send(&self, m: SignificantMsg) -> Result<()> {
        self.ch.lock().unwrap().significant_send(m)
    }
}

pub trait FilterFactory {
    fn generate(&self, node_id: u64) -> Vec<SendFilter>;
}

#[derive(Default)]
pub struct DefaultFilterFactory<F: Filter<RaftMessage> + Default>(PhantomData<F>);

impl<F: Filter<RaftMessage> + Default + 'static> FilterFactory for DefaultFilterFactory<F> {
    fn generate(&self, _: u64) -> Vec<SendFilter> {
        vec![Box::new(F::default())]
    }
}

pub struct CloneFilterFactory<F: Filter<RaftMessage> + Clone>(pub F);

impl<F: Filter<RaftMessage> + Clone + 'static> FilterFactory for CloneFilterFactory<F> {
    fn generate(&self, _: u64) -> Vec<SendFilter> {
        vec![Box::new(self.0.clone())]
    }
}

struct PartitionFilter {
    node_ids: Vec<u64>,
}

impl Filter<RaftMessage> for PartitionFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        msgs.retain(|m| !self.node_ids.contains(&m.get_to_peer().get_store_id()));
        check_messages(msgs)
    }
}

pub struct PartitionFilterFactory {
    s1: Vec<u64>,
    s2: Vec<u64>,
}

impl PartitionFilterFactory {
    pub fn new(s1: Vec<u64>, s2: Vec<u64>) -> PartitionFilterFactory {
        PartitionFilterFactory { s1: s1, s2: s2 }
    }
}

impl FilterFactory for PartitionFilterFactory {
    fn generate(&self, node_id: u64) -> Vec<SendFilter> {
        if self.s1.contains(&node_id) {
            return vec![
                box PartitionFilter {
                    node_ids: self.s2.clone(),
                },
            ];
        }
        return vec![
            box PartitionFilter {
                node_ids: self.s1.clone(),
            },
        ];
    }
}

pub struct IsolationFilterFactory {
    node_id: u64,
}

impl IsolationFilterFactory {
    pub fn new(node_id: u64) -> IsolationFilterFactory {
        IsolationFilterFactory { node_id: node_id }
    }
}

impl FilterFactory for IsolationFilterFactory {
    fn generate(&self, node_id: u64) -> Vec<SendFilter> {
        if node_id == self.node_id {
            return vec![box DropPacketFilter { rate: 100 }];
        }
        vec![
            box PartitionFilter {
                node_ids: vec![self.node_id],
            },
        ]
    }
}

#[derive(Clone, Copy)]
pub enum Direction {
    Recv,
    Send,
    Both,
}

impl Direction {
    pub fn is_recv(&self) -> bool {
        match *self {
            Direction::Recv | Direction::Both => true,
            Direction::Send => false,
        }
    }

    pub fn is_send(&self) -> bool {
        match *self {
            Direction::Send | Direction::Both => true,
            Direction::Recv => false,
        }
    }
}

/// Drop specified messages for the store with special region.
///
/// If `msg_type` is None, all message will be filtered.
#[derive(Clone)]
pub struct RegionPacketFilter {
    region_id: u64,
    store_id: u64,
    direction: Direction,
    block: Either<Arc<AtomicUsize>, Arc<AtomicBool>>,
    msg_type: Option<MessageType>,
}

impl Filter<RaftMessage> for RegionPacketFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        msgs.retain(|m| {
            let region_id = m.get_region_id();
            let from_store_id = m.get_from_peer().get_store_id();
            let to_store_id = m.get_to_peer().get_store_id();

            if self.region_id == region_id
                && (self.direction.is_send() && self.store_id == from_store_id
                    || self.direction.is_recv() && self.store_id == to_store_id)
                && self.msg_type
                    .as_ref()
                    .map_or(true, |t| t == &m.get_message().get_msg_type())
            {
                return match self.block {
                    Either::Left(ref count) => loop {
                        let left = count.load(Ordering::SeqCst);
                        if left == 0 {
                            return false;
                        }
                        if count.compare_and_swap(left, left - 1, Ordering::SeqCst) == left {
                            return true;
                        }
                    },
                    Either::Right(ref block) => !block.load(Ordering::SeqCst),
                };
            }
            true
        });
        check_messages(msgs)
    }
}

impl RegionPacketFilter {
    pub fn new(region_id: u64, store_id: u64) -> RegionPacketFilter {
        RegionPacketFilter {
            region_id: region_id,
            store_id: store_id,
            direction: Direction::Both,
            msg_type: None,
            block: Either::Right(Arc::new(AtomicBool::new(true))),
        }
    }

    pub fn direction(mut self, direction: Direction) -> RegionPacketFilter {
        self.direction = direction;
        self
    }

    pub fn msg_type(mut self, m_type: MessageType) -> RegionPacketFilter {
        self.msg_type = Some(m_type);
        self
    }

    pub fn allow(mut self, number: usize) -> RegionPacketFilter {
        self.block = Either::Left(Arc::new(AtomicUsize::new(number)));
        self
    }

    pub fn when(mut self, condition: Arc<AtomicBool>) -> RegionPacketFilter {
        self.block = Either::Right(condition);
        self
    }
}

#[derive(Default)]
pub struct SnapshotFilter {
    drop: AtomicBool,
}

impl Filter<RaftMessage> for SnapshotFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        msgs.retain(|m| m.get_message().get_msg_type() != MessageType::MsgSnapshot);
        self.drop.store(msgs.is_empty(), Ordering::Relaxed);
        check_messages(msgs)
    }

    fn after(&self, x: Result<()>) -> Result<()> {
        if self.drop.load(Ordering::Relaxed) {
            Ok(())
        } else {
            x
        }
    }
}

/// `CollectSnapshotFilter` is a simulation transport filter to simulate the simultaneous delivery
/// of multiple snapshots from different peers. It collects the snapshots from different
/// peers and drop the subsequent snapshots from the same peers. Currently, if there are
/// more than 1 snapshots in this filter, all the snapshots will be dilivered at once.
pub struct CollectSnapshotFilter {
    dropped: AtomicBool,
    stale: AtomicBool,
    pending_msg: Mutex<HashMap<u64, StoreMsg>>,
    pending_count_sender: Mutex<Sender<usize>>,
}

impl CollectSnapshotFilter {
    pub fn new(sender: Sender<usize>) -> CollectSnapshotFilter {
        CollectSnapshotFilter {
            dropped: AtomicBool::new(false),
            stale: AtomicBool::new(false),
            pending_msg: Mutex::new(HashMap::new()),
            pending_count_sender: Mutex::new(sender),
        }
    }
}

impl Filter<StoreMsg> for CollectSnapshotFilter {
    fn before(&self, msgs: &mut Vec<StoreMsg>) -> Result<()> {
        if self.stale.load(Ordering::Relaxed) {
            return Ok(());
        }
        let mut to_send = vec![];
        let mut pending_msg = self.pending_msg.lock().unwrap();
        for m in msgs.drain(..) {
            let (is_pending, from_peer_id) = match m {
                StoreMsg::RaftMessage(ref msg) => {
                    if msg.get_message().get_msg_type() == MessageType::MsgSnapshot {
                        let from_peer_id = msg.get_from_peer().get_id();
                        if pending_msg.contains_key(&from_peer_id) {
                            // Drop this snapshot message directly since it's from a seen peer
                            continue;
                        } else {
                            // Pile the snapshot from unseen peer
                            (true, from_peer_id)
                        }
                    } else {
                        (false, 0)
                    }
                }
                _ => (false, 0),
            };
            if is_pending {
                self.dropped
                    .compare_and_swap(false, true, Ordering::Relaxed);
                pending_msg.insert(from_peer_id, m);
                let sender = self.pending_count_sender.lock().unwrap();
                sender.send(pending_msg.len()).unwrap();
            } else {
                to_send.push(m);
            }
        }
        // Deliver those pending snapshots if there are more than 1.
        if pending_msg.len() > 1 {
            self.dropped
                .compare_and_swap(true, false, Ordering::Relaxed);
            msgs.extend(pending_msg.drain().map(|(_, v)| v));
            self.stale.compare_and_swap(false, true, Ordering::Relaxed);
        }
        msgs.extend(to_send);
        check_messages(msgs)
    }

    fn after(&self, res: Result<()>) -> Result<()> {
        if res.is_err() && self.dropped.load(Ordering::Relaxed) {
            self.dropped
                .compare_and_swap(true, false, Ordering::Relaxed);
            Ok(())
        } else {
            res
        }
    }
}

pub struct DropSnapshotFilter {
    notifier: Mutex<Sender<u64>>,
}

impl DropSnapshotFilter {
    pub fn new(ch: Sender<u64>) -> DropSnapshotFilter {
        DropSnapshotFilter {
            notifier: Mutex::new(ch),
        }
    }
}

impl Filter<StoreMsg> for DropSnapshotFilter {
    fn before(&self, msgs: &mut Vec<StoreMsg>) -> Result<()> {
        let notifier = self.notifier.lock().unwrap();
        msgs.retain(|m| match *m {
            StoreMsg::RaftMessage(ref msg) => {
                if msg.get_message().get_msg_type() != MessageType::MsgSnapshot {
                    true
                } else {
                    let idx = msg.get_message().get_snapshot().get_metadata().get_index();
                    if let Err(e) = notifier.send(idx) {
                        error!("failed to notify snapshot {:?}: {:?}", msg, e);
                    }
                    false
                }
            }
            _ => true,
        });
        Ok(())
    }
}

/// Filter leading duplicated Snap.
///
/// It will pause the first snapshot and fiter out all the snapshot that
/// are same as first snapshot msg until the first different snapshot shows up.
pub struct LeadingDuplicatedSnapshotFilter {
    dropped: AtomicBool,
    stale: Arc<AtomicBool>,
    last_msg: Mutex<Option<RaftMessage>>,
}

impl LeadingDuplicatedSnapshotFilter {
    pub fn new(stale: Arc<AtomicBool>) -> LeadingDuplicatedSnapshotFilter {
        LeadingDuplicatedSnapshotFilter {
            dropped: AtomicBool::new(false),
            stale: stale,
            last_msg: Mutex::new(None),
        }
    }
}

impl Filter<StoreMsg> for LeadingDuplicatedSnapshotFilter {
    fn before(&self, msgs: &mut Vec<StoreMsg>) -> Result<()> {
        let mut last_msg = self.last_msg.lock().unwrap();
        let mut stale = self.stale.load(Ordering::Relaxed);
        if stale {
            if last_msg.is_some() {
                msgs.push(StoreMsg::RaftMessage(last_msg.take().unwrap()));
            }
            return check_messages(msgs);
        }
        let mut to_send = vec![];
        for m in msgs.drain(..) {
            match m {
                StoreMsg::RaftMessage(msg) => {
                    if msg.get_message().get_msg_type() == MessageType::MsgSnapshot && !stale {
                        if last_msg.as_ref().map_or(false, |l| l != &msg) {
                            to_send.push(StoreMsg::RaftMessage(last_msg.take().unwrap()));
                            *last_msg = Some(msg);
                            stale = true;
                        } else {
                            self.dropped.store(true, Ordering::Relaxed);
                            *last_msg = Some(msg);
                        }
                    } else {
                        to_send.push(StoreMsg::RaftMessage(msg));
                    }
                }
                _ => {
                    to_send.push(m);
                }
            }
        }
        self.stale.store(stale, Ordering::Relaxed);
        msgs.extend(to_send);
        check_messages(msgs)
    }

    fn after(&self, res: Result<()>) -> Result<()> {
        let dropped = self.dropped
            .compare_and_swap(true, false, Ordering::Relaxed);
        if res.is_err() && dropped {
            Ok(())
        } else {
            res
        }
    }
}

/// `RandomLatencyFilter` is a transport filter to simulate randomized network latency.
/// Based on a randomized rate, `RandomLatencyFilter` will decide whether to delay
/// the sending of any message. It's could be used to simulate the message sending
/// in a network with random latency, where messages could be delayed, disordered or lost.
pub struct RandomLatencyFilter {
    delay_rate: u32,
    delayed_msgs: Mutex<Vec<RaftMessage>>,
}

impl RandomLatencyFilter {
    pub fn new(rate: u32) -> RandomLatencyFilter {
        RandomLatencyFilter {
            delay_rate: rate,
            delayed_msgs: Mutex::new(vec![]),
        }
    }

    fn will_delay(&self, _: &RaftMessage) -> bool {
        rand::random::<u32>() % 100u32 >= self.delay_rate
    }
}

impl Filter<RaftMessage> for RandomLatencyFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        let mut to_send = vec![];
        let mut to_delay = vec![];
        let mut delayed_msgs = self.delayed_msgs.lock().unwrap();
        // check whether to send those messages which are delayed previouly
        // and check whether to send any newly incoming message if they are not delayed
        for m in delayed_msgs.drain(..).chain(msgs.drain(..)) {
            if self.will_delay(&m) {
                to_delay.push(m);
            } else {
                to_send.push(m);
            }
        }
        delayed_msgs.extend(to_delay);
        msgs.extend(to_send);
        Ok(())
    }
}

impl Clone for RandomLatencyFilter {
    fn clone(&self) -> RandomLatencyFilter {
        let delayed_msgs = self.delayed_msgs.lock().unwrap();
        RandomLatencyFilter {
            delay_rate: self.delay_rate,
            delayed_msgs: Mutex::new(delayed_msgs.clone()),
        }
    }
}

#[derive(Clone, Default)]
pub struct LeaseReadFilter {
    pub ctx: Arc<RwLock<HashSet<Vec<u8>>>>,
    pub take: bool,
}

impl Filter<RaftMessage> for LeaseReadFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        let mut ctx = self.ctx.wl();
        for m in msgs {
            let msg = m.mut_message();
            if msg.get_msg_type() == MessageType::MsgHeartbeat && !msg.get_context().is_empty() {
                ctx.insert(msg.get_context().to_owned());
            }
            if self.take {
                msg.take_context();
            }
        }
        Ok(())
    }
}
