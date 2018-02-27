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

use std::fmt::{self, Display, Formatter};
use std::boxed::FnBox;
use std::time::Instant;
use std::sync::{Arc, RwLock};

use mio::Token;
use futures::{Async, Future, Poll, Stream};
use futures::stream::{self, Once};
use grpc::{ChannelBuilder, Environment, WriteFlags};
use kvproto::raft_serverpb::SnapshotChunk;
use kvproto::raft_serverpb::RaftMessage;
use kvproto::tikvpb_grpc::TikvClient;

use raftstore::store::{SnapEntry, SnapKey, SnapManager, Snapshot};
use util::threadpool::{DefaultContext, ThreadPool, ThreadPoolBuilder};
use util::worker::Runnable;
use util::buf::PipeBuffer;
use util::security::SecurityManager;
use util::collections::{HashMap, HashMapEntry as Entry};
use util::HandyRwLock;

use super::metrics::*;
use super::{Error, Result};
use super::transport::RaftStoreRouter;

pub type Callback = Box<FnBox(Result<()>) + Send>;

const DEFAULT_SENDER_POOL_SIZE: usize = 3;

/// `Task` that `Runner` can handle.
///
/// `Register` register a pending snapshot file with token;
/// `Write` write data to snapshot file;
/// `Close` save the snapshot file;
/// `Discard` discard all the unsaved changes made to snapshot file;
/// `SendTo` send the snapshot file to specified address.
pub enum Task {
    Register(Token, RaftMessage),
    Write(Token, PipeBuffer),
    Close(Token),
    Discard(Token),
    SendTo {
        addr: String,
        msg: RaftMessage,
        cb: Callback,
    },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Register(token, ref meta) => write!(f, "Register {:?} token: {:?}", meta, token),
            Task::Write(token, _) => write!(f, "Write snap for {:?}", token),
            Task::Close(token) => write!(f, "Close file {:?}", token),
            Task::Discard(token) => write!(f, "Discard file {:?}", token),
            Task::SendTo {
                ref addr, ref msg, ..
            } => write!(f, "SendTo Snap[to: {}, snap: {:?}]", addr, msg),
        }
    }
}

struct SnapChunk {
    snap: Arc<RwLock<Box<Snapshot>>>,
    remain_bytes: usize,
}

const SNAP_CHUNK_LEN: usize = 1024 * 1024;

impl Stream for SnapChunk {
    type Item = (SnapshotChunk, WriteFlags);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Error> {
        let mut buf = match self.remain_bytes {
            0 => return Ok(Async::Ready(None)),
            n if n > SNAP_CHUNK_LEN => vec![0; SNAP_CHUNK_LEN],
            n => vec![0; n],
        };
        let result = self.snap.wl().read_exact(buf.as_mut_slice());
        match result {
            Ok(_) => {
                self.remain_bytes -= buf.len();
                let mut chunk = SnapshotChunk::new();
                chunk.set_data(buf);
                Ok(Async::Ready(Some((
                    chunk,
                    WriteFlags::default().buffer_hint(true),
                ))))
            }
            Err(e) => Err(box_err!("failed to read snapshot chunk: {}", e)),
        }
    }
}

/// Send the snapshot to specified address.
///
/// It will first send the normal raft snapshot message and then send the snapshot file.
fn send_snap(
    env: Arc<Environment>,
    mgr: SnapManager,
    security_mgr: Arc<SecurityManager>,
    addr: &str,
    msg: RaftMessage,
) -> Result<()> {
    assert!(msg.get_message().has_snapshot());
    let timer = Instant::now();

    let send_timer = SEND_SNAP_HISTOGRAM.start_coarse_timer();

    let key = {
        let snap = msg.get_message().get_snapshot();
        SnapKey::from_snap(snap)?
    };
    mgr.register(key.clone(), SnapEntry::Sending);
    defer!({
        mgr.deregister(&key, &SnapEntry::Sending);
    });
    let s = box_try!(mgr.get_snapshot_for_sending(&key));
    if !s.exists() {
        return Err(box_err!("missing snap file: {:?}", s.path()));
    }
    let total_size = s.total_size()?;

    // snapshot file has been validated when created, so no need to validate again.
    let s = Arc::new(RwLock::new(s));

    let chunks = {
        let snap_chunk = SnapChunk {
            snap: Arc::clone(&s),
            remain_bytes: total_size as usize,
        };
        let first: Once<(SnapshotChunk, _), Error> = stream::once({
            let mut chunk = SnapshotChunk::new();
            chunk.set_message(msg);
            Ok((chunk, WriteFlags::default().buffer_hint(true)))
        });
        first.chain(snap_chunk)
    };

    let cb = ChannelBuilder::new(env);
    let channel = security_mgr.connect(cb, addr);
    let client = TikvClient::new(channel);
    let (sink, receiver) = client.snapshot()?;
    let send = chunks.forward(sink);
    let res = send.and_then(|_| receiver.map_err(Error::from))
        .and_then(|_| {
            info!(
                "[region {}] sent snapshot {} [size: {}, dur: {:?}]",
                key.region_id,
                key,
                total_size,
                timer.elapsed()
            );
            s.wl().delete();
            Ok(())
        })
        .wait()
        .map_err(Error::from);

    send_timer.observe_duration();
    res
}

pub struct Runner<R: RaftStoreRouter + 'static> {
    env: Arc<Environment>,
    snap_mgr: SnapManager,
    files: HashMap<Token, (Box<Snapshot>, RaftMessage)>,
    pool: ThreadPool<DefaultContext>,
    raft_router: R,
    security_mgr: Arc<SecurityManager>,
}

impl<R: RaftStoreRouter + 'static> Runner<R> {
    pub fn new(
        env: Arc<Environment>,
        snap_mgr: SnapManager,
        r: R,
        security_mgr: Arc<SecurityManager>,
    ) -> Runner<R> {
        Runner {
            env: env,
            snap_mgr: snap_mgr,
            files: map![],
            pool: ThreadPoolBuilder::with_default_factory(thd_name!("snap sender"))
                .thread_count(DEFAULT_SENDER_POOL_SIZE)
                .build(),
            raft_router: r,
            security_mgr: security_mgr,
        }
    }
}

impl<R: RaftStoreRouter + 'static> Runnable<Task> for Runner<R> {
    fn run(&mut self, task: Task) {
        match task {
            Task::Register(token, meta) => {
                SNAP_TASK_COUNTER.with_label_values(&["register"]).inc();
                let mgr = self.snap_mgr.clone();
                let key = match SnapKey::from_snap(meta.get_message().get_snapshot()) {
                    Ok(k) => k,
                    Err(e) => {
                        error!("failed to create snap key for token {:?}: {:?}", token, e);
                        return;
                    }
                };
                match mgr.get_snapshot_for_receiving(
                    &key,
                    meta.get_message().get_snapshot().get_data(),
                ) {
                    Ok(snap) => {
                        if snap.exists() {
                            info!(
                                "snapshot file {} already exists, skip receiving.",
                                snap.path()
                            );
                            if let Err(e) = self.raft_router.send_raft_msg(meta) {
                                error!("send snapshot for key {} token {:?}: {:?}", key, token, e);
                            }
                            return;
                        }
                        debug!("begin to receive snap {:?}", meta);
                        mgr.register(key, SnapEntry::Receiving);
                        self.files.insert(token, (snap, meta));
                    }
                    Err(e) => {
                        error!(
                            "failed to create snapshot file for token {:?}: {:?}",
                            token, e
                        );
                        return;
                    }
                }
            }
            Task::Write(token, mut data) => {
                SNAP_TASK_COUNTER.with_label_values(&["write"]).inc();
                match self.files.entry(token) {
                    Entry::Occupied(mut e) => {
                        if let Err(err) = data.write_all_to(&mut e.get_mut().0) {
                            error!(
                                "failed to write data to snapshot file {} for token {:?}: {:?}",
                                e.get_mut().0.path(),
                                token,
                                err
                            );
                            let (_, msg) = e.remove();
                            let key = SnapKey::from_snap(msg.get_message().get_snapshot()).unwrap();
                            self.snap_mgr.deregister(&key, &SnapEntry::Receiving);
                        }
                    }
                    Entry::Vacant(_) => error!("invalid snap token {:?}", token),
                }
            }
            Task::Close(token) => {
                SNAP_TASK_COUNTER.with_label_values(&["close"]).inc();
                match self.files.remove(&token) {
                    Some((mut snap, msg)) => {
                        let key = SnapKey::from_snap(msg.get_message().get_snapshot()).unwrap();
                        info!("saving snapshot to {}", snap.path());
                        defer!({
                            self.snap_mgr.deregister(&key, &SnapEntry::Receiving);
                        });
                        if let Err(e) = snap.save() {
                            error!(
                                "failed to save snapshot file {} for token {:?}: {:?}",
                                snap.path(),
                                token,
                                e
                            );
                            return;
                        }
                        if let Err(e) = self.raft_router.send_raft_msg(msg) {
                            error!("send snapshot for token {:?} err {:?}", token, e);
                        }
                    }
                    None => error!("invalid snap token {:?}", token),
                }
            }
            Task::Discard(token) => {
                SNAP_TASK_COUNTER.with_label_values(&["discard"]).inc();
                if let Some((_, msg)) = self.files.remove(&token) {
                    debug!("discard snapshot: {:?}", msg);
                    // because token is inserted, following can't panic.
                    let key = SnapKey::from_snap(msg.get_message().get_snapshot()).unwrap();
                    self.snap_mgr.deregister(&key, &SnapEntry::Receiving);
                }
            }
            Task::SendTo { addr, msg, cb } => {
                SNAP_TASK_COUNTER.with_label_values(&["send"]).inc();
                let env = Arc::clone(&self.env);
                let mgr = self.snap_mgr.clone();
                let security_mgr = Arc::clone(&self.security_mgr);
                self.pool.execute(move |_| {
                    let res = send_snap(env, mgr, security_mgr, &addr, msg);
                    if res.is_err() {
                        error!("failed to send snap to {}: {:?}", addr, res);
                    }
                    cb(res)
                });
            }
        }
    }
}
