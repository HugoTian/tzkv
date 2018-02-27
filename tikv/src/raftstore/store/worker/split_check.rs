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
use std::fmt::{self, Display, Formatter};
use std::collections::BinaryHeap;
use std::cmp::Ordering;

use rocksdb::{DBIterator, DB};
use kvproto::metapb::RegionEpoch;
use kvproto::metapb::Region;

use raftstore::coprocessor::CoprocessorHost;
use raftstore::store::{keys, Callback, Msg};
use raftstore::store::engine::{IterOption, Iterable};
use raftstore::Result;
use util::escape;
use util::transport::{RetryableSendCh, Sender};
use util::worker::Runnable;
use storage::{CfName, LARGE_CFS};

use super::metrics::*;

#[derive(PartialEq, Eq)]
struct KeyEntry {
    key: Option<Vec<u8>>,
    pos: usize,
    value_size: usize,
}

impl KeyEntry {
    fn new(key: Vec<u8>, pos: usize, value_size: usize) -> KeyEntry {
        KeyEntry {
            key: Some(key),
            pos: pos,
            value_size: value_size,
        }
    }

    fn take(&mut self) -> KeyEntry {
        KeyEntry::new(self.key.take().unwrap(), self.pos, self.value_size)
    }
}

impl PartialOrd for KeyEntry {
    fn partial_cmp(&self, rhs: &KeyEntry) -> Option<Ordering> {
        // BinaryHeap is max heap, so we have to reverse order to get a min heap.
        Some(
            self.key
                .as_ref()
                .unwrap()
                .cmp(rhs.key.as_ref().unwrap())
                .reverse(),
        )
    }
}

impl Ord for KeyEntry {
    fn cmp(&self, rhs: &KeyEntry) -> Ordering {
        self.partial_cmp(rhs).unwrap()
    }
}

struct MergedIterator<'a> {
    iters: Vec<DBIterator<&'a DB>>,
    heap: BinaryHeap<KeyEntry>,
}

impl<'a> MergedIterator<'a> {
    fn new(
        db: &'a DB,
        cfs: &[CfName],
        start_key: &[u8],
        end_key: &[u8],
        fill_cache: bool,
    ) -> Result<MergedIterator<'a>> {
        let mut iters = Vec::with_capacity(cfs.len());
        let mut heap = BinaryHeap::with_capacity(cfs.len());
        for (pos, cf) in cfs.into_iter().enumerate() {
            let iter_opt =
                IterOption::new(Some(start_key.to_vec()), Some(end_key.to_vec()), fill_cache);
            let mut iter = db.new_iterator_cf(cf, iter_opt)?;
            if iter.seek(start_key.into()) {
                heap.push(KeyEntry::new(iter.key().to_vec(), pos, iter.value().len()));
            }
            iters.push(iter);
        }
        Ok(MergedIterator {
            iters: iters,
            heap: heap,
        })
    }

    fn next(&mut self) -> Option<KeyEntry> {
        let pos = match self.heap.peek() {
            None => return None,
            Some(e) => e.pos,
        };
        let iter = &mut self.iters[pos];
        if iter.next() {
            // TODO: avoid copy key.
            let e = KeyEntry::new(iter.key().to_vec(), pos, iter.value().len());
            let mut front = self.heap.peek_mut().unwrap();
            let res = front.take();
            *front = e;
            Some(res)
        } else {
            self.heap.pop()
        }
    }
}

/// Split checking task.
pub struct Task {
    region: Region,
}

impl Task {
    pub fn new(region: &Region) -> Task {
        Task {
            region: region.clone(),
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Split Check Task for {}", self.region.get_id())
    }
}

pub struct Runner<C> {
    engine: Arc<DB>,
    ch: RetryableSendCh<Msg, C>,
    coprocessor: Arc<CoprocessorHost>,
}

impl<C: Sender<Msg>> Runner<C> {
    pub fn new(
        engine: Arc<DB>,
        ch: RetryableSendCh<Msg, C>,
        coprocessor: Arc<CoprocessorHost>,
    ) -> Runner<C> {
        Runner {
            engine: engine,
            ch: ch,
            coprocessor: coprocessor,
        }
    }

    fn check_split(&mut self, region: &Region) {
        let mut split_ctx = self.coprocessor
            .new_split_check_status(region, &self.engine);
        if split_ctx.skip() {
            debug!("[region {}] skip split check", region.get_id());
            return;
        }

        let region_id = region.get_id();
        let start_key = keys::enc_start_key(region);
        let end_key = keys::enc_end_key(region);
        debug!(
            "[region {}] executing task {} {}",
            region_id,
            escape(&start_key),
            escape(&end_key)
        );
        CHECK_SPILT_COUNTER_VEC.with_label_values(&["all"]).inc();

        let mut split_key = None;
        let coprocessor = &mut self.coprocessor;

        let timer = CHECK_SPILT_HISTOGRAM.start_coarse_timer();
        let res = MergedIterator::new(self.engine.as_ref(), LARGE_CFS, &start_key, &end_key, false)
            .map(|mut iter| {
                while let Some(e) = iter.next() {
                    if let Some(key) = coprocessor.on_split_check(
                        region,
                        &mut split_ctx,
                        e.key.as_ref().unwrap(),
                        e.value_size as u64,
                    ) {
                        split_key = Some(key);
                        break;
                    }
                }
            });
        timer.observe_duration();

        if let Err(e) = res {
            error!("[region {}] failed to scan split key: {}", region_id, e);
            return;
        }

        if let Some(split_key) = split_key {
            let region_epoch = region.get_region_epoch().clone();
            let res = self.ch
                .try_send(new_split_region(region_id, region_epoch, split_key));
            if let Err(e) = res {
                warn!("[region {}] failed to send check result: {}", region_id, e);
            }

            CHECK_SPILT_COUNTER_VEC
                .with_label_values(&["success"])
                .inc();
        } else {
            debug!(
                "[region {}] no need to send, split key not found",
                region_id,
            );

            CHECK_SPILT_COUNTER_VEC.with_label_values(&["ignore"]).inc();
        }
    }
}

impl<C: Sender<Msg>> Runnable<Task> for Runner<C> {
    fn run(&mut self, task: Task) {
        let region = &task.region;
        self.check_split(region);
    }
}

fn new_split_region(region_id: u64, epoch: RegionEpoch, split_key: Vec<u8>) -> Msg {
    let key = keys::origin_key(split_key.as_slice()).to_vec();
    Msg::SplitRegion {
        region_id: region_id,
        region_epoch: epoch,
        split_key: key,
        callback: Callback::None,
    }
}
