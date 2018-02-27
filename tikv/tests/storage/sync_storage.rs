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
use std::sync::atomic::{AtomicUsize, Ordering};

use tikv::util::collections::HashMap;
use tikv::storage::{Engine, Key, KvPair, Mutation, Options, Result, Storage, Value};
use tikv::storage::config::Config;
use kvproto::kvrpcpb::{Context, LockInfo};

/// `SyncStorage` wraps `Storage` with sync API, usually used for testing.
pub struct SyncStorage {
    store: Storage,
    cnt: Arc<AtomicUsize>,
}

impl SyncStorage {
    pub fn new(config: &Config) -> SyncStorage {
        let storage = Storage::new(config).unwrap();
        let mut s = SyncStorage {
            store: storage,
            cnt: Arc::new(AtomicUsize::new(0)),
        };
        s.start(config);
        s
    }

    pub fn from_engine(engine: Box<Engine>, config: &Config) -> SyncStorage {
        let mut s = SyncStorage::prepare(engine, config);
        s.start(config);
        s
    }

    pub fn prepare(engine: Box<Engine>, config: &Config) -> SyncStorage {
        let storage = Storage::from_engine(engine, config).unwrap();
        SyncStorage {
            store: storage,
            cnt: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn start(&mut self, config: &Config) {
        self.store.start(config).unwrap();
    }

    pub fn get_storage(&self) -> Storage {
        self.store.clone()
    }

    pub fn get_engine(&self) -> Box<Engine> {
        self.store.get_engine()
    }

    pub fn get(&self, ctx: Context, key: &Key, start_ts: u64) -> Result<Option<Value>> {
        wait_op!(|cb| self.store.async_get(ctx, key.to_owned(), start_ts, cb)).unwrap()
    }

    #[allow(dead_code)]
    pub fn batch_get(
        &self,
        ctx: Context,
        keys: &[Key],
        start_ts: u64,
    ) -> Result<Vec<Result<KvPair>>> {
        wait_op!(|cb| self.store
            .async_batch_get(ctx, keys.to_owned(), start_ts, cb))
            .unwrap()
    }

    pub fn scan(
        &self,
        ctx: Context,
        key: Key,
        limit: usize,
        key_only: bool,
        start_ts: u64,
    ) -> Result<Vec<Result<KvPair>>> {
        wait_op!(|cb| self.store.async_scan(
            ctx,
            key,
            limit,
            start_ts,
            Options::new(0, false, key_only),
            cb,
        )).unwrap()
    }

    pub fn prewrite(
        &self,
        ctx: Context,
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: u64,
    ) -> Result<Vec<Result<()>>> {
        wait_op!(|cb| self.store.async_prewrite(
            ctx,
            mutations,
            primary,
            start_ts,
            Options::default(),
            cb
        )).unwrap()
    }

    pub fn commit(
        &self,
        ctx: Context,
        keys: Vec<Key>,
        start_ts: u64,
        commit_ts: u64,
    ) -> Result<()> {
        wait_op!(|cb| self.store.async_commit(ctx, keys, start_ts, commit_ts, cb)).unwrap()
    }

    pub fn cleanup(&self, ctx: Context, key: Key, start_ts: u64) -> Result<()> {
        wait_op!(|cb| self.store.async_cleanup(ctx, key, start_ts, cb)).unwrap()
    }

    pub fn rollback(&self, ctx: Context, keys: Vec<Key>, start_ts: u64) -> Result<()> {
        wait_op!(|cb| self.store.async_rollback(ctx, keys, start_ts, cb)).unwrap()
    }

    pub fn scan_lock(
        &self,
        ctx: Context,
        max_ts: u64,
        start_key: Vec<u8>,
        limit: usize,
    ) -> Result<Vec<LockInfo>> {
        wait_op!(|cb| self.store
            .async_scan_lock(ctx, max_ts, start_key, limit, cb))
            .unwrap()
    }

    pub fn resolve_lock(&self, ctx: Context, start_ts: u64, commit_ts: Option<u64>) -> Result<()> {
        let mut txn_status = HashMap::default();
        txn_status.insert(start_ts, commit_ts.unwrap_or(0));
        wait_op!(|cb| self.store.async_resolve_lock(ctx, txn_status, cb)).unwrap()
    }

    pub fn resolve_lock_batch(&self, ctx: Context, txns: Vec<(u64, u64)>) -> Result<()> {
        let txn_status: HashMap<u64, u64> = txns.into_iter().collect();
        wait_op!(|cb| self.store.async_resolve_lock(ctx, txn_status, cb)).unwrap()
    }

    pub fn gc(&self, ctx: Context, safe_point: u64) -> Result<()> {
        wait_op!(|cb| self.store.async_gc(ctx, safe_point, cb)).unwrap()
    }

    pub fn raw_get(&self, ctx: Context, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        wait_op!(|cb| self.store.async_raw_get(ctx, key, cb)).unwrap()
    }

    pub fn raw_put(&self, ctx: Context, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        wait_op!(|cb| self.store.async_raw_put(ctx, key, value, cb)).unwrap()
    }

    pub fn raw_delete(&self, ctx: Context, key: Vec<u8>) -> Result<()> {
        wait_op!(|cb| self.store.async_raw_delete(ctx, key, cb)).unwrap()
    }

    pub fn raw_scan(
        &self,
        ctx: Context,
        start_key: Vec<u8>,
        limit: usize,
    ) -> Result<Vec<Result<KvPair>>> {
        wait_op!(|cb| self.store.async_raw_scan(ctx, start_key, limit, cb)).unwrap()
    }
}

impl Clone for SyncStorage {
    fn clone(&self) -> SyncStorage {
        self.cnt.fetch_add(1, Ordering::SeqCst);
        SyncStorage {
            store: self.store.clone(),
            cnt: Arc::clone(&self.cnt),
        }
    }
}

impl Drop for SyncStorage {
    fn drop(&mut self) {
        if self.cnt.fetch_sub(1, Ordering::SeqCst) == 0 {
            self.store.stop().unwrap()
        }
    }
}
