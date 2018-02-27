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

use std::fmt;
use storage::{is_short_value, Key, Mutation, Options, Statistics, Value, CF_DEFAULT, CF_LOCK,
              CF_WRITE};
use storage::engine::{Modify, ScanMode, Snapshot};
use super::reader::MvccReader;
use super::lock::{Lock, LockType};
use super::write::{Write, WriteType};
use super::{Error, Result};
use super::metrics::*;
use kvproto::kvrpcpb::IsolationLevel;

pub const MAX_TXN_WRITE_SIZE: usize = 32 * 1024;

pub struct MvccTxn {
    reader: MvccReader,
    start_ts: u64,
    writes: Vec<Modify>,
    write_size: usize,
}

impl fmt::Debug for MvccTxn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "txn @{}", self.start_ts)
    }
}

impl MvccTxn {
    pub fn new(
        snapshot: Box<Snapshot>,
        start_ts: u64,
        mode: Option<ScanMode>,
        isolation_level: IsolationLevel,
        fill_cache: bool,
    ) -> MvccTxn {
        MvccTxn {
            // Todo: use session variable to indicate fill cache or not
            reader: MvccReader::new(snapshot, mode, fill_cache, None, None, isolation_level),
            start_ts: start_ts,
            writes: vec![],
            write_size: 0,
        }
    }

    pub fn into_modifies(self) -> Vec<Modify> {
        self.writes
    }

    pub fn get_statistics(&self) -> &Statistics {
        self.reader.get_statistics()
    }

    pub fn write_size(&self) -> usize {
        self.write_size
    }

    fn lock_key(
        &mut self,
        key: Key,
        lock_type: LockType,
        primary: Vec<u8>,
        ttl: u64,
        short_value: Option<Value>,
    ) {
        let lock = Lock::new(lock_type, primary, self.start_ts, ttl, short_value).to_bytes();
        self.write_size += CF_LOCK.len() + key.encoded().len() + lock.len();
        self.writes.push(Modify::Put(CF_LOCK, key, lock));
    }

    fn unlock_key(&mut self, key: Key) {
        self.write_size += CF_LOCK.len() + key.encoded().len();
        self.writes.push(Modify::Delete(CF_LOCK, key));
    }

    fn put_value(&mut self, key: &Key, ts: u64, value: Value) {
        let key = key.append_ts(ts);
        self.write_size += key.encoded().len() + value.len();
        self.writes.push(Modify::Put(CF_DEFAULT, key, value));
    }

    fn delete_value(&mut self, key: &Key, ts: u64) {
        let key = key.append_ts(ts);
        self.write_size += key.encoded().len();
        self.writes.push(Modify::Delete(CF_DEFAULT, key));
    }

    fn put_write(&mut self, key: &Key, ts: u64, value: Value) {
        let key = key.append_ts(ts);
        self.write_size += CF_WRITE.len() + key.encoded().len() + value.len();
        self.writes.push(Modify::Put(CF_WRITE, key, value));
    }

    fn delete_write(&mut self, key: &Key, ts: u64) {
        let key = key.append_ts(ts);
        self.write_size += CF_WRITE.len() + key.encoded().len();
        self.writes.push(Modify::Delete(CF_WRITE, key));
    }

    pub fn get(&mut self, key: &Key) -> Result<Option<Value>> {
        self.reader.get(key, self.start_ts)
    }

    pub fn prewrite(
        &mut self,
        mutation: Mutation,
        primary: &[u8],
        options: &Options,
    ) -> Result<()> {
        let key = mutation.key();
        if !options.skip_constraint_check {
            if let Some((commit, _)) = self.reader.seek_write(key, u64::max_value())? {
                // Abort on writes after our start timestamp ...
                if commit >= self.start_ts {
                    MVCC_CONFLICT_COUNTER
                        .with_label_values(&["prewrite_write_conflict"])
                        .inc();
                    return Err(Error::WriteConflict {
                        start_ts: self.start_ts,
                        conflict_ts: commit,
                        key: key.encoded().to_owned(),
                        primary: primary.to_vec(),
                    });
                }
            }
        }
        // ... or locks at any timestamp.
        if let Some(lock) = self.reader.load_lock(key)? {
            if lock.ts != self.start_ts {
                return Err(Error::KeyIsLocked {
                    key: key.raw()?,
                    primary: lock.primary,
                    ts: lock.ts,
                    ttl: lock.ttl,
                });
            }
            // No need to overwrite the lock and data.
            // If we use single delete, we can't put a key multiple times.
            MVCC_DUPLICATE_CMD_COUNTER_VEC
                .with_label_values(&["prewrite"])
                .inc();
            return Ok(());
        }

        let short_value = if let Mutation::Put((_, ref value)) = mutation {
            if is_short_value(value) {
                Some(value.clone())
            } else {
                None
            }
        } else {
            None
        };

        self.lock_key(
            key.clone(),
            LockType::from_mutation(&mutation),
            primary.to_vec(),
            options.lock_ttl,
            short_value,
        );

        if let Mutation::Put((_, ref value)) = mutation {
            if !is_short_value(value) {
                let ts = self.start_ts;
                self.put_value(key, ts, value.clone());
            }
        }
        Ok(())
    }

    pub fn commit(&mut self, key: &Key, commit_ts: u64) -> Result<()> {
        let (lock_type, short_value) = match self.reader.load_lock(key)? {
            Some(ref mut lock) if lock.ts == self.start_ts => {
                (lock.lock_type, lock.short_value.take())
            }
            _ => {
                return match self.reader.get_txn_commit_info(key, self.start_ts)? {
                    Some((_, WriteType::Rollback)) | None => {
                        MVCC_CONFLICT_COUNTER
                            .with_label_values(&["commit_lock_not_found"])
                            .inc();
                        // TODO:None should not appear
                        // Rollbacked by concurrent transaction.
                        info!(
                            "txn conflict (lock not found), key:{}, start_ts:{}, commit_ts:{}",
                            key, self.start_ts, commit_ts
                        );
                        Err(Error::TxnLockNotFound {
                            start_ts: self.start_ts,
                            commit_ts: commit_ts,
                            key: key.encoded().to_owned(),
                        })
                    }
                    // Committed by concurrent transaction.
                    Some((_, WriteType::Put))
                    | Some((_, WriteType::Delete))
                    | Some((_, WriteType::Lock)) => {
                        MVCC_DUPLICATE_CMD_COUNTER_VEC
                            .with_label_values(&["commit"])
                            .inc();
                        Ok(())
                    }
                };
            }
        };
        let write = Write::new(
            WriteType::from_lock_type(lock_type),
            self.start_ts,
            short_value,
        );
        self.put_write(key, commit_ts, write.to_bytes());
        self.unlock_key(key.clone());
        Ok(())
    }

    pub fn rollback(&mut self, key: &Key) -> Result<()> {
        match self.reader.load_lock(key)? {
            Some(ref lock) if lock.ts == self.start_ts => {
                // If prewrite type is DEL or LOCK, it is no need to delete value.
                if lock.short_value.is_none() && lock.lock_type == LockType::Put {
                    self.delete_value(key, lock.ts);
                }
            }
            _ => {
                return match self.reader.get_txn_commit_info(key, self.start_ts)? {
                    Some((ts, write_type)) => {
                        if write_type == WriteType::Rollback {
                            // return Ok on Rollback already exist
                            MVCC_DUPLICATE_CMD_COUNTER_VEC
                                .with_label_values(&["rollback"])
                                .inc();
                            Ok(())
                        } else {
                            MVCC_CONFLICT_COUNTER
                                .with_label_values(&["rollback_committed"])
                                .inc();
                            info!(
                                "txn conflict (committed), key:{}, start_ts:{}, commit_ts:{}",
                                key, self.start_ts, ts
                            );
                            Err(Error::Committed { commit_ts: ts })
                        }
                    }
                    None => {
                        let ts = self.start_ts;
                        // insert a Rollback to WriteCF when receives Rollback before Prewrite
                        let write = Write::new(WriteType::Rollback, ts, None);
                        self.put_write(key, ts, write.to_bytes());
                        Ok(())
                    }
                };
            }
        }
        let write = Write::new(WriteType::Rollback, self.start_ts, None);
        let ts = self.start_ts;
        self.put_write(key, ts, write.to_bytes());
        self.unlock_key(key.clone());
        Ok(())
    }

    pub fn gc(&mut self, key: &Key, safe_point: u64) -> Result<()> {
        let mut remove_older = false;
        let mut ts: u64 = u64::max_value();
        let mut versions = 0;
        let mut delete_versions = 0;
        let mut latest_delete = None;
        while let Some((commit, write)) = self.reader.seek_write(key, ts)? {
            ts = commit - 1;
            versions += 1;

            if self.write_size >= MAX_TXN_WRITE_SIZE {
                // Cannot remove latest delete when we haven't iterate all versions.
                latest_delete = None;
                break;
            }

            if remove_older {
                self.delete_write(key, commit);
                if write.write_type == WriteType::Put && write.short_value.is_none() {
                    self.delete_value(key, write.start_ts);
                }
                delete_versions += 1;
                continue;
            }

            if commit > safe_point {
                continue;
            }

            // Set `remove_older` after we find the latest value.
            match write.write_type {
                WriteType::Put | WriteType::Delete => {
                    remove_older = true;
                }
                WriteType::Rollback | WriteType::Lock => {}
            }

            // Latest write before `safe_point` can be deleted if its type is Delete,
            // Rollback or Lock.
            match write.write_type {
                WriteType::Delete => {
                    latest_delete = Some(commit);
                }
                WriteType::Rollback | WriteType::Lock => {
                    self.delete_write(key, commit);
                    delete_versions += 1;
                }
                WriteType::Put => {}
            }
        }
        if let Some(commit) = latest_delete {
            self.delete_write(key, commit);
            delete_versions += 1;
        }
        MVCC_VERSIONS_HISTOGRAM.observe(f64::from(versions));
        if delete_versions > 0 {
            GC_DELETE_VERSIONS_HISTOGRAM.observe(f64::from(delete_versions));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;
    use kvproto::kvrpcpb::{Context, IsolationLevel};
    use super::MvccTxn;
    use super::super::MvccReader;
    use super::super::write::{Write, WriteType};
    use storage::{make_key, Mutation, Options, ScanMode, ALL_CFS, CF_WRITE, SHORT_VALUE_MAX_LEN};
    use storage::engine::{self, Engine, Modify, TEMP_DIR};

    fn gen_value(v: u8, len: usize) -> Vec<u8> {
        let mut value = Vec::with_capacity(len);
        for _ in 0..len {
            value.push(v);
        }

        value
    }

    fn write(engine: &Engine, ctx: &Context, modifies: Vec<Modify>) {
        if !modifies.is_empty() {
            engine.write(ctx, modifies).unwrap();
        }
    }

    fn test_mvcc_txn_read_imp(k: &[u8], v: &[u8]) {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();

        must_get_none(engine.as_ref(), k, 1);

        must_prewrite_put(engine.as_ref(), k, v, k, 5);
        must_get_none(engine.as_ref(), k, 3);
        must_get_err(engine.as_ref(), k, 7);

        must_commit(engine.as_ref(), k, 5, 10);
        must_get_none(engine.as_ref(), k, 3);
        must_get_none(engine.as_ref(), k, 7);
        must_get(engine.as_ref(), k, 13, v);
        must_prewrite_delete(engine.as_ref(), k, k, 15);
        must_commit(engine.as_ref(), k, 15, 20);
        must_get_none(engine.as_ref(), k, 3);
        must_get_none(engine.as_ref(), k, 7);
        must_get(engine.as_ref(), k, 13, v);
        must_get(engine.as_ref(), k, 17, v);
        must_get_none(engine.as_ref(), k, 23);
    }

    #[test]
    fn test_mvcc_txn_read() {
        test_mvcc_txn_read_imp(b"k1", b"v1");

        let long_value = gen_value(b'v', SHORT_VALUE_MAX_LEN + 1);
        test_mvcc_txn_read_imp(b"k2", &long_value);
    }

    fn test_mvcc_txn_prewrite_imp(k: &[u8], v: &[u8]) {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();

        must_prewrite_put(engine.as_ref(), k, v, k, 5);
        // Key is locked.
        must_locked(engine.as_ref(), k, 5);
        // Retry prewrite.
        must_prewrite_put(engine.as_ref(), k, v, k, 5);
        // Conflict.
        must_prewrite_lock_err(engine.as_ref(), k, k, 6);

        must_commit(engine.as_ref(), k, 5, 10);
        must_written(engine.as_ref(), k, 5, 10, WriteType::Put);
        // Write conflict.
        must_prewrite_lock_err(engine.as_ref(), k, k, 6);
        must_unlocked(engine.as_ref(), k);
        // Not conflict.
        must_prewrite_lock(engine.as_ref(), k, k, 12);
        must_locked(engine.as_ref(), k, 12);
        must_rollback(engine.as_ref(), k, 12);
        must_unlocked(engine.as_ref(), k);
        must_written(engine.as_ref(), k, 12, 12, WriteType::Rollback);
        // Cannot retry Prewrite after rollback.
        must_prewrite_lock_err(engine.as_ref(), k, k, 12);
        // Can prewrite after rollback.
        must_prewrite_delete(engine.as_ref(), k, k, 13);
        must_rollback(engine.as_ref(), k, 13);
        must_unlocked(engine.as_ref(), k);
    }

    #[test]
    fn test_rollback_lock() {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();

        let (k, v) = (b"k1", b"v1");
        must_prewrite_put(engine.as_ref(), k, v, k, 5);
        must_commit(engine.as_ref(), k, 5, 10);

        // Lock
        must_prewrite_lock(engine.as_ref(), k, k, 15);
        must_locked(engine.as_ref(), k, 15);

        // Rollback lock
        must_rollback(engine.as_ref(), k, 15);
    }

    #[test]
    fn test_rollback_del() {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();

        let (k, v) = (b"k1", b"v1");
        must_prewrite_put(engine.as_ref(), k, v, k, 5);
        must_commit(engine.as_ref(), k, 5, 10);

        // Prewrite delete
        must_prewrite_delete(engine.as_ref(), k, k, 15);
        must_locked(engine.as_ref(), k, 15);

        // Rollback delete
        must_rollback(engine.as_ref(), k, 15);
    }

    #[test]
    fn test_mvcc_txn_prewrite() {
        test_mvcc_txn_prewrite_imp(b"k1", b"v1");

        let long_value = gen_value(b'v', SHORT_VALUE_MAX_LEN + 1);
        test_mvcc_txn_prewrite_imp(b"k2", &long_value);
    }

    fn test_mvcc_txn_commit_ok_imp(k1: &[u8], v1: &[u8], k2: &[u8], k3: &[u8]) {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
        must_prewrite_put(engine.as_ref(), k1, v1, k1, 10);
        must_prewrite_lock(engine.as_ref(), k2, k1, 10);
        must_prewrite_delete(engine.as_ref(), k3, k1, 10);
        must_locked(engine.as_ref(), k1, 10);
        must_locked(engine.as_ref(), k2, 10);
        must_locked(engine.as_ref(), k3, 10);
        must_commit(engine.as_ref(), k1, 10, 15);
        must_commit(engine.as_ref(), k2, 10, 15);
        must_commit(engine.as_ref(), k3, 10, 15);
        must_written(engine.as_ref(), k1, 10, 15, WriteType::Put);
        must_written(engine.as_ref(), k2, 10, 15, WriteType::Lock);
        must_written(engine.as_ref(), k3, 10, 15, WriteType::Delete);
        // commit should be idempotent
        must_commit(engine.as_ref(), k1, 10, 15);
        must_commit(engine.as_ref(), k2, 10, 15);
        must_commit(engine.as_ref(), k3, 10, 15);
    }

    #[test]
    fn test_mvcc_txn_commit_ok() {
        test_mvcc_txn_commit_ok_imp(b"x", b"v", b"y", b"z");

        let long_value = gen_value(b'v', SHORT_VALUE_MAX_LEN + 1);
        test_mvcc_txn_commit_ok_imp(b"x", &long_value, b"y", b"z");
    }

    fn test_mvcc_txn_commit_err_imp(k: &[u8], v: &[u8]) {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();

        // Not prewrite yet
        must_commit_err(engine.as_ref(), k, 1, 2);
        must_prewrite_put(engine.as_ref(), k, v, k, 5);
        // start_ts not match
        must_commit_err(engine.as_ref(), k, 4, 5);
        must_rollback(engine.as_ref(), k, 5);
        // commit after rollback
        must_commit_err(engine.as_ref(), k, 5, 6);
    }

    #[test]
    fn test_mvcc_txn_commit_err() {
        test_mvcc_txn_commit_err_imp(b"k", b"v");

        let long_value = gen_value(b'v', SHORT_VALUE_MAX_LEN + 1);
        test_mvcc_txn_commit_err_imp(b"k2", &long_value);
    }

    fn test_mvcc_txn_rollback_imp(k: &[u8], v: &[u8]) {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();

        must_prewrite_put(engine.as_ref(), k, v, k, 5);
        must_rollback(engine.as_ref(), k, 5);
        // rollback should be idempotent
        must_rollback(engine.as_ref(), k, 5);
        // lock should be released after rollback
        must_unlocked(engine.as_ref(), k);
        must_prewrite_lock(engine.as_ref(), k, k, 10);
        must_rollback(engine.as_ref(), k, 10);
        // data should be dropped after rollback
        must_get_none(engine.as_ref(), k, 20);
    }

    #[test]
    fn test_mvcc_txn_rollback_after_commit() {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();

        let k = b"k";
        let v = b"v";
        let t1 = 1;
        let t2 = 10;
        let t3 = 20;
        let t4 = 30;

        must_prewrite_put(engine.as_ref(), k, v, k, t1);

        must_rollback(engine.as_ref(), k, t2);
        must_rollback(engine.as_ref(), k, t2);
        must_rollback(engine.as_ref(), k, t4);

        must_commit(engine.as_ref(), k, t1, t3);
        // The rollback should be failed since the transaction
        // was committed before.
        must_rollback_err(engine.as_ref(), k, t1);
        must_get(engine.as_ref(), k, t4, v);
    }

    #[test]
    fn test_mvcc_txn_rollback() {
        test_mvcc_txn_rollback_imp(b"k", b"v");

        let long_value = gen_value(b'v', SHORT_VALUE_MAX_LEN + 1);
        test_mvcc_txn_rollback_imp(b"k2", &long_value);
    }

    fn test_mvcc_txn_rollback_err_imp(k: &[u8], v: &[u8]) {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();

        must_prewrite_put(engine.as_ref(), k, v, k, 5);
        must_commit(engine.as_ref(), k, 5, 10);
        must_rollback_err(engine.as_ref(), k, 5);
        must_rollback_err(engine.as_ref(), k, 5);
    }

    #[test]
    fn test_mvcc_txn_rollback_err() {
        test_mvcc_txn_rollback_err_imp(b"k", b"v");

        let long_value = gen_value(b'v', SHORT_VALUE_MAX_LEN + 1);
        test_mvcc_txn_rollback_err_imp(b"k2", &long_value);
    }

    #[test]
    fn test_mvcc_txn_rollback_before_prewrite() {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
        let key = b"key";
        must_rollback(engine.as_ref(), key, 5);
        must_prewrite_lock_err(engine.as_ref(), key, key, 5);
    }

    fn test_gc_imp(k: &[u8], v1: &[u8], v2: &[u8], v3: &[u8], v4: &[u8]) {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();

        must_prewrite_put(engine.as_ref(), k, v1, k, 5);
        must_commit(engine.as_ref(), k, 5, 10);
        must_prewrite_put(engine.as_ref(), k, v2, k, 15);
        must_commit(engine.as_ref(), k, 15, 20);
        must_prewrite_delete(engine.as_ref(), k, k, 25);
        must_commit(engine.as_ref(), k, 25, 30);
        must_prewrite_put(engine.as_ref(), k, v3, k, 35);
        must_commit(engine.as_ref(), k, 35, 40);
        must_prewrite_lock(engine.as_ref(), k, k, 45);
        must_commit(engine.as_ref(), k, 45, 50);
        must_prewrite_put(engine.as_ref(), k, v4, k, 55);
        must_rollback(engine.as_ref(), k, 55);

        // Transactions:
        // startTS commitTS Command
        // --
        // 55      -        PUT "x55" (Rollback)
        // 45      50       LOCK
        // 35      40       PUT "x35"
        // 25      30       DELETE
        // 15      20       PUT "x15"
        //  5      10       PUT "x5"

        // CF data layout:
        // ts CFDefault   CFWrite
        // --
        // 55             Rollback(PUT,50)
        // 50             Commit(LOCK,45)
        // 45
        // 40             Commit(PUT,35)
        // 35   x35
        // 30             Commit(Delete,25)
        // 25
        // 20             Commit(PUT,15)
        // 15   x15
        // 10             Commit(PUT,5)
        // 5    x5

        must_gc(engine.as_ref(), k, 12);
        must_get(engine.as_ref(), k, 12, v1);

        must_gc(engine.as_ref(), k, 22);
        must_get(engine.as_ref(), k, 22, v2);
        must_get_none(engine.as_ref(), k, 12);

        must_gc(engine.as_ref(), k, 32);
        must_get_none(engine.as_ref(), k, 22);
        must_get_none(engine.as_ref(), k, 35);

        must_gc(engine.as_ref(), k, 60);
        must_get(engine.as_ref(), k, 62, v3);
    }

    #[test]
    fn test_gc() {
        test_gc_imp(b"k1", b"v1", b"v2", b"v3", b"v4");

        let v1 = gen_value(b'x', SHORT_VALUE_MAX_LEN + 1);
        let v2 = gen_value(b'y', SHORT_VALUE_MAX_LEN + 1);
        let v3 = gen_value(b'z', SHORT_VALUE_MAX_LEN + 1);
        let v4 = gen_value(b'v', SHORT_VALUE_MAX_LEN + 1);
        test_gc_imp(b"k2", &v1, &v2, &v3, &v4);
    }

    fn test_write_imp(k: &[u8], v: &[u8], k2: &[u8], k3: &[u8]) {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();

        must_prewrite_put(engine.as_ref(), k, v, k, 5);
        must_seek_write_none(engine.as_ref(), k, 5);

        must_commit(engine.as_ref(), k, 5, 10);
        must_seek_write(engine.as_ref(), k, u64::max_value(), 5, 10, WriteType::Put);
        must_reverse_seek_write(engine.as_ref(), k, 5, 5, 10, WriteType::Put);
        must_seek_write_none(engine.as_ref(), k2, u64::max_value());
        must_reverse_seek_write_none(engine.as_ref(), k3, 5);
        must_get_commit_ts(engine.as_ref(), k, 5, 10);

        must_prewrite_delete(engine.as_ref(), k, k, 15);
        must_rollback(engine.as_ref(), k, 15);
        must_seek_write(
            engine.as_ref(),
            k,
            u64::max_value(),
            15,
            15,
            WriteType::Rollback,
        );
        must_reverse_seek_write(engine.as_ref(), k, 15, 15, 15, WriteType::Rollback);
        must_get_commit_ts(engine.as_ref(), k, 5, 10);
        must_get_commit_ts_none(engine.as_ref(), k, 15);

        must_prewrite_lock(engine.as_ref(), k, k, 25);
        must_commit(engine.as_ref(), k, 25, 30);
        must_seek_write(
            engine.as_ref(),
            k,
            u64::max_value(),
            25,
            30,
            WriteType::Lock,
        );
        must_reverse_seek_write(engine.as_ref(), k, 25, 25, 30, WriteType::Lock);
        must_get_commit_ts(engine.as_ref(), k, 25, 30);
    }

    #[test]
    fn test_write() {
        test_write_imp(b"kk", b"v1", b"k", b"kkk");

        let v2 = gen_value(b'x', SHORT_VALUE_MAX_LEN + 1);
        test_write_imp(b"kk", &v2, b"k", b"kkk");
    }

    fn test_scan_keys_imp(keys: Vec<&[u8]>, values: Vec<&[u8]>) {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
        must_prewrite_put(engine.as_ref(), keys[0], values[0], keys[0], 1);
        must_commit(engine.as_ref(), keys[0], 1, 10);
        must_prewrite_lock(engine.as_ref(), keys[1], keys[1], 1);
        must_commit(engine.as_ref(), keys[1], 1, 5);
        must_prewrite_delete(engine.as_ref(), keys[2], keys[2], 1);
        must_commit(engine.as_ref(), keys[2], 1, 20);
        must_prewrite_put(engine.as_ref(), keys[3], values[1], keys[3], 1);
        must_prewrite_lock(engine.as_ref(), keys[4], keys[4], 10);
        must_prewrite_delete(engine.as_ref(), keys[5], keys[5], 5);

        must_scan_keys(
            engine.as_ref(),
            None,
            100,
            vec![keys[0], keys[1], keys[2]],
            None,
        );
        must_scan_keys(
            engine.as_ref(),
            None,
            3,
            vec![keys[0], keys[1], keys[2]],
            None,
        );
        must_scan_keys(
            engine.as_ref(),
            None,
            2,
            vec![keys[0], keys[1]],
            Some(keys[1]),
        );
        must_scan_keys(
            engine.as_ref(),
            Some(keys[1]),
            1,
            vec![keys[1]],
            Some(keys[1]),
        );
    }

    #[test]
    fn test_scan_keys() {
        test_scan_keys_imp(vec![b"a", b"c", b"e", b"b", b"d", b"f"], vec![b"a", b"b"]);

        let v1 = gen_value(b'x', SHORT_VALUE_MAX_LEN + 1);
        let v4 = gen_value(b'v', SHORT_VALUE_MAX_LEN + 1);
        test_scan_keys_imp(vec![b"a", b"c", b"e", b"b", b"d", b"f"], vec![&v1, &v4]);
    }

    fn test_write_size_imp(k: &[u8], v: &[u8], pk: &[u8]) {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, 10, None, IsolationLevel::SI, true);
        let key = make_key(k);
        assert_eq!(txn.write_size, 0);

        assert!(txn.get(&key).unwrap().is_none());
        assert_eq!(txn.write_size, 0);

        txn.prewrite(
            Mutation::Put((key.clone(), v.to_vec())),
            pk,
            &Options::default(),
        ).unwrap();
        assert!(txn.write_size() > 0);
        engine.write(&ctx, txn.into_modifies()).unwrap();

        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, 10, None, IsolationLevel::SI, true);
        txn.commit(&key, 15).unwrap();
        assert!(txn.write_size() > 0);
        engine.write(&ctx, txn.into_modifies()).unwrap();
    }

    #[test]
    fn test_write_size() {
        test_write_size_imp(b"key", b"value", b"pk");

        let v = gen_value(b'x', SHORT_VALUE_MAX_LEN + 1);
        test_write_size_imp(b"key", &v, b"pk");
    }

    #[test]
    fn test_skip_constraint_check() {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
        let (key, value) = (b"key", b"value");

        must_prewrite_put(engine.as_ref(), key, value, key, 5);
        must_commit(engine.as_ref(), key, 5, 10);

        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, 5, None, IsolationLevel::SI, true);
        assert!(txn.prewrite(
            Mutation::Put((make_key(key), value.to_vec())),
            key,
            &Options::default()
        ).is_err());

        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, 5, None, IsolationLevel::SI, true);
        let mut opt = Options::default();
        opt.skip_constraint_check = true;
        assert!(
            txn.prewrite(Mutation::Put((make_key(key), value.to_vec())), key, &opt)
                .is_ok()
        );
    }

    #[test]
    fn test_read_commit() {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
        let (key, v1, v2) = (b"key", b"v1", b"v2");

        must_prewrite_put(engine.as_ref(), key, v1, key, 5);
        must_commit(engine.as_ref(), key, 5, 10);
        must_prewrite_put(engine.as_ref(), key, v2, key, 15);
        must_get_err(engine.as_ref(), key, 20);
        must_get_rc(engine.as_ref(), key, 12, v1);
        must_get_rc(engine.as_ref(), key, 20, v1);
    }

    fn must_get(engine: &Engine, key: &[u8], ts: u64, expect: &[u8]) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, ts, None, IsolationLevel::SI, true);
        assert_eq!(txn.get(&make_key(key)).unwrap().unwrap(), expect);
    }

    fn must_get_rc(engine: &Engine, key: &[u8], ts: u64, expect: &[u8]) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, ts, None, IsolationLevel::RC, true);
        assert_eq!(txn.get(&make_key(key)).unwrap().unwrap(), expect)
    }

    fn must_get_none(engine: &Engine, key: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, ts, None, IsolationLevel::SI, true);
        assert!(txn.get(&make_key(key)).unwrap().is_none());
    }

    fn must_get_err(engine: &Engine, key: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, ts, None, IsolationLevel::SI, true);
        assert!(txn.get(&make_key(key)).is_err());
    }

    fn must_prewrite_put(engine: &Engine, key: &[u8], value: &[u8], pk: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, ts, None, IsolationLevel::SI, true);
        txn.prewrite(
            Mutation::Put((make_key(key), value.to_vec())),
            pk,
            &Options::default(),
        ).unwrap();
        write(engine, &ctx, txn.into_modifies());
    }

    fn must_prewrite_delete(engine: &Engine, key: &[u8], pk: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, ts, None, IsolationLevel::SI, true);
        txn.prewrite(Mutation::Delete(make_key(key)), pk, &Options::default())
            .unwrap();
        engine.write(&ctx, txn.into_modifies()).unwrap();
    }

    fn must_prewrite_lock(engine: &Engine, key: &[u8], pk: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, ts, None, IsolationLevel::SI, true);
        txn.prewrite(Mutation::Lock(make_key(key)), pk, &Options::default())
            .unwrap();
        engine.write(&ctx, txn.into_modifies()).unwrap();
    }

    fn must_prewrite_lock_err(engine: &Engine, key: &[u8], pk: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, ts, None, IsolationLevel::SI, true);
        assert!(
            txn.prewrite(Mutation::Lock(make_key(key)), pk, &Options::default())
                .is_err()
        );
    }

    fn must_commit(engine: &Engine, key: &[u8], start_ts: u64, commit_ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, start_ts, None, IsolationLevel::SI, true);
        txn.commit(&make_key(key), commit_ts).unwrap();
        write(engine, &ctx, txn.into_modifies());
    }

    fn must_commit_err(engine: &Engine, key: &[u8], start_ts: u64, commit_ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, start_ts, None, IsolationLevel::SI, true);
        assert!(txn.commit(&make_key(key), commit_ts).is_err());
    }

    fn must_rollback(engine: &Engine, key: &[u8], start_ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, start_ts, None, IsolationLevel::SI, true);
        txn.rollback(&make_key(key)).unwrap();
        write(engine, &ctx, txn.into_modifies());
    }

    fn must_rollback_err(engine: &Engine, key: &[u8], start_ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, start_ts, None, IsolationLevel::SI, true);
        assert!(txn.rollback(&make_key(key)).is_err());
    }

    fn must_gc(engine: &Engine, key: &[u8], safe_point: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, 0, None, IsolationLevel::SI, true);
        txn.gc(&make_key(key), safe_point).unwrap();
        write(engine, &ctx, txn.into_modifies());
    }

    fn must_locked(engine: &Engine, key: &[u8], start_ts: u64) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, None, None, IsolationLevel::SI);
        let lock = reader.load_lock(&make_key(key)).unwrap().unwrap();
        assert_eq!(lock.ts, start_ts);
    }

    fn must_unlocked(engine: &Engine, key: &[u8]) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, None, None, IsolationLevel::SI);
        assert!(reader.load_lock(&make_key(key)).unwrap().is_none());
    }

    fn must_written(engine: &Engine, key: &[u8], start_ts: u64, commit_ts: u64, tp: WriteType) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let k = make_key(key).append_ts(commit_ts);
        let v = snapshot.get_cf(CF_WRITE, &k).unwrap().unwrap();
        let write = Write::parse(&v).unwrap();
        assert_eq!(write.start_ts, start_ts);
        assert_eq!(write.write_type, tp);
    }

    fn must_seek_write_none(engine: &Engine, key: &[u8], ts: u64) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, None, None, IsolationLevel::SI);
        assert!(reader.seek_write(&make_key(key), ts).unwrap().is_none());
    }

    fn must_seek_write(
        engine: &Engine,
        key: &[u8],
        ts: u64,
        start_ts: u64,
        commit_ts: u64,
        write_type: WriteType,
    ) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, None, None, IsolationLevel::SI);
        let (t, write) = reader.seek_write(&make_key(key), ts).unwrap().unwrap();
        assert_eq!(t, commit_ts);
        assert_eq!(write.start_ts, start_ts);
        assert_eq!(write.write_type, write_type);
    }

    fn must_reverse_seek_write_none(engine: &Engine, key: &[u8], ts: u64) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, None, None, IsolationLevel::SI);
        assert!(
            reader
                .reverse_seek_write(&make_key(key), ts)
                .unwrap()
                .is_none()
        );
    }

    fn must_reverse_seek_write(
        engine: &Engine,
        key: &[u8],
        ts: u64,
        start_ts: u64,
        commit_ts: u64,
        write_type: WriteType,
    ) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, None, None, IsolationLevel::SI);
        let (t, write) = reader
            .reverse_seek_write(&make_key(key), ts)
            .unwrap()
            .unwrap();
        assert_eq!(t, commit_ts);
        assert_eq!(write.start_ts, start_ts);
        assert_eq!(write.write_type, write_type);
    }

    fn must_get_commit_ts(engine: &Engine, key: &[u8], start_ts: u64, commit_ts: u64) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, None, None, IsolationLevel::SI);
        let (ts, write_type) = reader
            .get_txn_commit_info(&make_key(key), start_ts)
            .unwrap()
            .unwrap();
        assert_ne!(write_type, WriteType::Rollback);
        assert_eq!(ts, commit_ts);
    }

    fn must_get_commit_ts_none(engine: &Engine, key: &[u8], start_ts: u64) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, None, None, IsolationLevel::SI);

        let ret = reader.get_txn_commit_info(&make_key(key), start_ts);
        assert!(ret.is_ok());
        match ret.unwrap() {
            None => {}
            Some((_, write_type)) => {
                assert_eq!(write_type, WriteType::Rollback);
            }
        }
    }

    fn must_scan_keys(
        engine: &Engine,
        start: Option<&[u8]>,
        limit: usize,
        keys: Vec<&[u8]>,
        next_start: Option<&[u8]>,
    ) {
        let expect = (
            keys.into_iter().map(make_key).collect(),
            next_start.map(|x| make_key(x).append_ts(0)),
        );
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(
            snapshot,
            Some(ScanMode::Mixed),
            false,
            None,
            None,
            IsolationLevel::SI,
        );
        assert_eq!(
            reader.scan_keys(start.map(make_key), limit).unwrap(),
            expect
        );
    }

    #[test]
    fn test_scan_values_in_default() {
        let path = TempDir::new("_test_scan_values_in_default").expect("");
        let path = path.path().to_str().unwrap();
        let engine = engine::new_local_engine(path, ALL_CFS).unwrap();

        must_prewrite_put(
            engine.as_ref(),
            &[2],
            &gen_value(b'v', SHORT_VALUE_MAX_LEN + 1),
            &[2],
            3,
        );
        must_commit(engine.as_ref(), &[2], 3, 3);

        must_prewrite_put(
            engine.as_ref(),
            &[3],
            &gen_value(b'a', SHORT_VALUE_MAX_LEN + 1),
            &[3],
            3,
        );
        must_commit(engine.as_ref(), &[3], 3, 4);

        must_prewrite_put(
            engine.as_ref(),
            &[3],
            &gen_value(b'b', SHORT_VALUE_MAX_LEN + 1),
            &[3],
            5,
        );
        must_commit(engine.as_ref(), &[3], 5, 5);

        must_prewrite_put(
            engine.as_ref(),
            &[6],
            &gen_value(b'x', SHORT_VALUE_MAX_LEN + 1),
            &[6],
            3,
        );
        must_commit(engine.as_ref(), &[6], 3, 6);

        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(
            snapshot,
            Some(ScanMode::Forward),
            true,
            None,
            None,
            IsolationLevel::SI,
        );

        let v = reader.scan_values_in_default(&make_key(&[3])).unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(v[1], (3, gen_value(b'a', SHORT_VALUE_MAX_LEN + 1)));
        assert_eq!(v[0], (5, gen_value(b'b', SHORT_VALUE_MAX_LEN + 1)));
    }

    #[test]
    fn test_seek_ts() {
        let path = TempDir::new("_test_seek_ts").expect("");
        let path = path.path().to_str().unwrap();
        let engine = engine::new_local_engine(path, ALL_CFS).unwrap();

        must_prewrite_put(engine.as_ref(), &[2], &gen_value(b'v', 2), &[2], 3);
        must_commit(engine.as_ref(), &[2], 3, 3);

        must_prewrite_put(
            engine.as_ref(),
            &[3],
            &gen_value(b'a', SHORT_VALUE_MAX_LEN + 1),
            &[3],
            4,
        );
        must_commit(engine.as_ref(), &[3], 4, 4);

        must_prewrite_put(
            engine.as_ref(),
            &[5],
            &gen_value(b'b', SHORT_VALUE_MAX_LEN + 1),
            &[5],
            2,
        );
        must_commit(engine.as_ref(), &[5], 2, 5);

        must_prewrite_put(engine.as_ref(), &[6], &gen_value(b'x', 3), &[6], 3);
        must_commit(engine.as_ref(), &[6], 3, 6);

        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(
            snapshot,
            Some(ScanMode::Forward),
            true,
            None,
            None,
            IsolationLevel::SI,
        );

        assert_eq!(reader.seek_ts(3).unwrap().unwrap(), make_key(&[2]));
    }
}
