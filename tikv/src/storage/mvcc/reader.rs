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

use storage::engine::{Cursor, ScanMode, Snapshot, Statistics};
use storage::{Key, Value, CF_LOCK, CF_WRITE};
use super::{Error, Result};
use super::lock::Lock;
use super::write::{Write, WriteType};
use raftstore::store::engine::IterOption;
use std::u64;
use kvproto::kvrpcpb::IsolationLevel;
use util::properties::MvccProperties;

const GC_MAX_ROW_VERSIONS_THRESHOLD: u64 = 100;

pub struct MvccReader {
    snapshot: Box<Snapshot>,
    statistics: Statistics,
    // cursors are used for speeding up scans.
    data_cursor: Option<Cursor>,
    lock_cursor: Option<Cursor>,
    write_cursor: Option<Cursor>,

    scan_mode: Option<ScanMode>,
    key_only: bool,

    fill_cache: bool,
    lower_bound: Option<Vec<u8>>,
    upper_bound: Option<Vec<u8>>,
    isolation_level: IsolationLevel,
}

impl MvccReader {
    pub fn new(
        snapshot: Box<Snapshot>,
        scan_mode: Option<ScanMode>,
        fill_cache: bool,
        lower_bound: Option<Vec<u8>>,
        upper_bound: Option<Vec<u8>>,
        isolation_level: IsolationLevel,
    ) -> MvccReader {
        MvccReader {
            snapshot: snapshot,
            statistics: Statistics::default(),
            data_cursor: None,
            lock_cursor: None,
            write_cursor: None,
            scan_mode: scan_mode,
            isolation_level: isolation_level,
            key_only: false,
            fill_cache: fill_cache,
            lower_bound: lower_bound,
            upper_bound: upper_bound,
        }
    }

    pub fn get_statistics(&self) -> &Statistics {
        &self.statistics
    }

    pub fn set_key_only(&mut self, key_only: bool) {
        self.key_only = key_only;
    }

    pub fn load_data(&mut self, key: &Key, ts: u64) -> Result<Value> {
        if self.key_only {
            return Ok(vec![]);
        }
        if self.scan_mode.is_some() && self.data_cursor.is_none() {
            let iter_opt = IterOption::new(None, None, self.fill_cache);
            self.data_cursor = Some(self.snapshot.iter(iter_opt, self.get_scan_mode(true))?);
        }

        let k = key.append_ts(ts);
        let res = if let Some(ref mut cursor) = self.data_cursor {
            match cursor.get(&k, &mut self.statistics.data)? {
                None => panic!("key {} not found, ts {}", key, ts),
                Some(v) => v.to_vec(),
            }
        } else {
            self.statistics.data.get += 1;
            match self.snapshot.get(&k)? {
                None => panic!("key {} not found, ts: {}", key, ts),
                Some(v) => v,
            }
        };

        self.statistics.data.processed += 1;
        self.statistics.data.flow_stats.read_bytes += k.raw().unwrap_or_default().len() + res.len();
        self.statistics.data.flow_stats.read_keys += 1;
        Ok(res)
    }

    pub fn load_lock(&mut self, key: &Key) -> Result<Option<Lock>> {
        if self.scan_mode.is_some() && self.lock_cursor.is_none() {
            let iter_opt = IterOption::new(None, None, true);
            let iter = self.snapshot
                .iter_cf(CF_LOCK, iter_opt, self.get_scan_mode(true))?;
            self.lock_cursor = Some(iter);
        }

        let res = if let Some(ref mut cursor) = self.lock_cursor {
            match cursor.get(key, &mut self.statistics.lock)? {
                Some(v) => Some(Lock::parse(v)?),
                None => None,
            }
        } else {
            self.statistics.lock.get += 1;
            match self.snapshot.get_cf(CF_LOCK, key)? {
                Some(v) => Some(Lock::parse(&v)?),
                None => None,
            }
        };

        if res.is_some() {
            self.statistics.lock.processed += 1;
        }

        Ok(res)
    }

    fn get_scan_mode(&self, allow_backward: bool) -> ScanMode {
        match self.scan_mode {
            Some(ScanMode::Forward) => ScanMode::Forward,
            Some(ScanMode::Backward) if allow_backward => ScanMode::Backward,
            _ => ScanMode::Mixed,
        }
    }

    pub fn seek_write(&mut self, key: &Key, ts: u64) -> Result<Option<(u64, Write)>> {
        self.seek_write_impl(key, ts, false)
    }

    pub fn reverse_seek_write(&mut self, key: &Key, ts: u64) -> Result<Option<(u64, Write)>> {
        self.seek_write_impl(key, ts, true)
    }

    fn seek_write_impl(
        &mut self,
        key: &Key,
        ts: u64,
        reverse: bool,
    ) -> Result<Option<(u64, Write)>> {
        if self.scan_mode.is_some() {
            if self.write_cursor.is_none() {
                let iter_opt = IterOption::new(None, None, self.fill_cache);
                let iter = self.snapshot
                    .iter_cf(CF_WRITE, iter_opt, self.get_scan_mode(false))?;
                self.write_cursor = Some(iter);
            }
        } else {
            // use prefix bloom filter
            let iter_opt = IterOption::default()
                .use_prefix_seek()
                .set_prefix_same_as_start(true);
            let iter = self.snapshot.iter_cf(CF_WRITE, iter_opt, ScanMode::Mixed)?;
            self.write_cursor = Some(iter);
        }

        let cursor = self.write_cursor.as_mut().unwrap();
        let ok = if reverse {
            cursor.near_seek_for_prev(&key.append_ts(ts), &mut self.statistics.write)?
        } else {
            cursor.near_seek(&key.append_ts(ts), &mut self.statistics.write)?
        };
        if !ok {
            return Ok(None);
        }
        let write_key = Key::from_encoded(cursor.key().to_vec());
        let commit_ts = write_key.decode_ts()?;
        let k = write_key.truncate_ts()?;
        if &k != key {
            return Ok(None);
        }
        let write = Write::parse(cursor.value())?;
        self.statistics.write.processed += 1;
        self.statistics.write.flow_stats.read_bytes += cursor.key().len() + cursor.value().len();
        self.statistics.write.flow_stats.read_keys += 1;
        Ok(Some((commit_ts, write)))
    }

    fn check_lock(&mut self, key: &Key, mut ts: u64) -> Result<Option<u64>> {
        if let Some(lock) = self.load_lock(key)? {
            if lock.ts <= ts {
                if ts == u64::MAX && key.raw()? == lock.primary {
                    // when ts==u64::MAX(which means to get latest committed version for
                    // primary key),and current key is the primary key, returns the latest
                    // commit version's value
                    ts = lock.ts - 1;
                } else {
                    // There is a pending lock. Client should wait or clean it.
                    return Err(Error::KeyIsLocked {
                        key: key.raw()?,
                        primary: lock.primary,
                        ts: lock.ts,
                        ttl: lock.ttl,
                    });
                }
            }
        }
        Ok(Some(ts))
    }

    pub fn get(&mut self, key: &Key, mut ts: u64) -> Result<Option<Value>> {
        // Check for locks that signal concurrent writes.
        match self.isolation_level {
            IsolationLevel::SI => if let Some(new_ts) = self.check_lock(key, ts)? {
                ts = new_ts;
            },
            IsolationLevel::RC => {}
        }
        loop {
            match self.seek_write(key, ts)? {
                Some((commit_ts, mut write)) => match write.write_type {
                    WriteType::Put => {
                        if write.short_value.is_some() {
                            if self.key_only {
                                return Ok(Some(vec![]));
                            }
                            return Ok(write.short_value.take());
                        }
                        return self.load_data(key, write.start_ts).map(Some);
                    }
                    WriteType::Delete => {
                        return Ok(None);
                    }
                    WriteType::Lock | WriteType::Rollback => ts = commit_ts - 1,
                },
                None => return Ok(None),
            }
        }
    }

    pub fn get_txn_commit_info(
        &mut self,
        key: &Key,
        start_ts: u64,
    ) -> Result<Option<(u64, WriteType)>> {
        let mut seek_ts = start_ts;
        while let Some((commit_ts, write)) = self.reverse_seek_write(key, seek_ts)? {
            if write.start_ts == start_ts {
                return Ok(Some((commit_ts, write.write_type)));
            }
            seek_ts = commit_ts + 1;
        }
        Ok(None)
    }

    fn create_data_cursor(&mut self) -> Result<()> {
        self.scan_mode = Some(ScanMode::Forward);
        if self.data_cursor.is_none() {
            let iter_opt = IterOption::new(None, None, self.fill_cache);
            let iter = self.snapshot.iter(iter_opt, self.get_scan_mode(true))?;
            self.data_cursor = Some(iter);
        }
        Ok(())
    }

    fn create_write_cursor(&mut self) -> Result<()> {
        if self.write_cursor.is_none() {
            let iter_opt = IterOption::new(
                self.lower_bound.as_ref().cloned(),
                self.upper_bound.as_ref().cloned(),
                self.fill_cache,
            );
            let iter = self.snapshot
                .iter_cf(CF_WRITE, iter_opt, self.get_scan_mode(false))?;
            self.write_cursor = Some(iter);
        }
        Ok(())
    }

    fn create_lock_cursor(&mut self) -> Result<()> {
        if self.lock_cursor.is_none() {
            let iter_opt = IterOption::new(
                self.lower_bound.as_ref().cloned(),
                self.upper_bound.as_ref().cloned(),
                true,
            );
            let iter = self.snapshot
                .iter_cf(CF_LOCK, iter_opt, self.get_scan_mode(true))?;
            self.lock_cursor = Some(iter);
        }
        Ok(())
    }

    // Return the first committed key which start_ts equals to ts
    pub fn seek_ts(&mut self, ts: u64) -> Result<Option<Key>> {
        assert!(self.scan_mode.is_some());
        self.create_write_cursor()?;

        let cursor = self.write_cursor.as_mut().unwrap();
        let mut ok = cursor.seek_to_first(&mut self.statistics.write);

        while ok {
            if Write::parse(cursor.value())?.start_ts == ts {
                return Ok(Some(Key::from_encoded(cursor.key().to_vec())
                    .truncate_ts()?));
            }
            ok = cursor.next(&mut self.statistics.write);
        }
        Ok(None)
    }

    pub fn seek(&mut self, mut key: Key, ts: u64) -> Result<Option<(Key, Value)>> {
        assert!(self.scan_mode.is_some());
        self.create_write_cursor()?;
        self.create_lock_cursor()?;

        let (mut write_valid, mut lock_valid) = (true, true);

        loop {
            key = {
                let w_cur = self.write_cursor.as_mut().unwrap();
                let l_cur = self.lock_cursor.as_mut().unwrap();
                let (mut w_key, mut l_key) = (None, None);
                if write_valid {
                    if w_cur.near_seek(&key, &mut self.statistics.write)? {
                        w_key = Some(w_cur.key());
                    } else {
                        w_key = None;
                        write_valid = false;
                    }
                }
                if lock_valid {
                    if l_cur.near_seek(&key, &mut self.statistics.lock)? {
                        l_key = Some(l_cur.key());
                    } else {
                        l_key = None;
                        lock_valid = false;
                    }
                }
                match (w_key, l_key) {
                    (None, None) => return Ok(None),
                    (None, Some(k)) => Key::from_encoded(k.to_vec()),
                    (Some(k), None) => Key::from_encoded(k.to_vec()).truncate_ts()?,
                    (Some(wk), Some(lk)) => if wk < lk {
                        Key::from_encoded(wk.to_vec()).truncate_ts()?
                    } else {
                        Key::from_encoded(lk.to_vec())
                    },
                }
            };
            if let Some(v) = self.get(&key, ts)? {
                return Ok(Some((key, v)));
            }
            key = key.append_ts(0);
        }
    }

    pub fn reverse_seek(&mut self, mut key: Key, ts: u64) -> Result<Option<(Key, Value)>> {
        assert!(self.scan_mode.is_some());
        self.create_write_cursor()?;
        self.create_lock_cursor()?;

        let (mut write_valid, mut lock_valid) = (true, true);

        loop {
            key = {
                let w_cur = self.write_cursor.as_mut().unwrap();
                let l_cur = self.lock_cursor.as_mut().unwrap();
                let (mut w_key, mut l_key) = (None, None);
                if write_valid {
                    if w_cur.near_reverse_seek(&key, &mut self.statistics.write)? {
                        w_key = Some(w_cur.key());
                    } else {
                        w_key = None;
                        write_valid = false;
                    }
                }
                if lock_valid {
                    if l_cur.near_reverse_seek(&key, &mut self.statistics.lock)? {
                        l_key = Some(l_cur.key());
                    } else {
                        l_key = None;
                        lock_valid = false;
                    }
                }
                match (w_key, l_key) {
                    (None, None) => return Ok(None),
                    (None, Some(k)) => Key::from_encoded(k.to_vec()),
                    (Some(k), None) => Key::from_encoded(k.to_vec()).truncate_ts()?,
                    (Some(wk), Some(lk)) => if wk < lk {
                        Key::from_encoded(lk.to_vec())
                    } else {
                        Key::from_encoded(wk.to_vec()).truncate_ts()?
                    },
                }
            };
            if let Some(v) = self.get(&key, ts)? {
                return Ok(Some((key, v)));
            }
        }
    }

    #[allow(type_complexity)]
    pub fn scan_lock<F>(
        &mut self,
        start: Option<Key>,
        filter: F,
        limit: usize,
    ) -> Result<(Vec<(Key, Lock)>, Option<Key>)>
    where
        F: Fn(&Lock) -> bool,
    {
        self.create_lock_cursor()?;
        let cursor = self.lock_cursor.as_mut().unwrap();
        let ok = match start {
            Some(ref x) => cursor.seek(x, &mut self.statistics.lock)?,
            None => cursor.seek_to_first(&mut self.statistics.lock),
        };
        if !ok {
            return Ok((vec![], None));
        }
        let mut locks = vec![];
        while cursor.valid() {
            let key = Key::from_encoded(cursor.key().to_vec());
            let lock = Lock::parse(cursor.value())?;
            if filter(&lock) {
                locks.push((key.clone(), lock));
                if limit > 0 && locks.len() >= limit {
                    return Ok((locks, Some(key)));
                }
            }
            cursor.next(&mut self.statistics.lock);
        }
        self.statistics.lock.processed += locks.len();
        Ok((locks, None))
    }

    pub fn scan_keys(
        &mut self,
        mut start: Option<Key>,
        limit: usize,
    ) -> Result<(Vec<Key>, Option<Key>)> {
        let iter_opt = IterOption::new(None, None, self.fill_cache);
        let scan_mode = self.get_scan_mode(false);
        let mut cursor = self.snapshot.iter_cf(CF_WRITE, iter_opt, scan_mode)?;
        let mut keys = vec![];
        loop {
            let ok = match start {
                Some(ref x) => cursor.near_seek(x, &mut self.statistics.write)?,
                None => cursor.seek_to_first(&mut self.statistics.write),
            };
            if !ok {
                return Ok((keys, None));
            }
            if keys.len() >= limit {
                self.statistics.write.processed += keys.len();
                return Ok((keys, start));
            }
            let key = Key::from_encoded(cursor.key().to_vec()).truncate_ts()?;
            start = Some(key.append_ts(0));
            keys.push(key);
        }
    }

    // Get all Value of the given key in CF_DEFAULT
    pub fn scan_values_in_default(&mut self, key: &Key) -> Result<Vec<(u64, Value)>> {
        self.create_data_cursor()?;
        let cursor = self.data_cursor.as_mut().unwrap();
        let mut ok = cursor.seek(key, &mut self.statistics.data)?;
        if !ok {
            return Ok(vec![]);
        }
        let mut v = vec![];
        while ok {
            let cur_key = Key::from_encoded(cursor.key().to_vec());
            let cur_key_without_ts = cur_key.truncate_ts()?;
            if cur_key_without_ts.encoded().as_slice() == key.encoded().as_slice() {
                v.push((cur_key.decode_ts()?, cursor.value().to_vec()));
            }
            if cur_key_without_ts.encoded().as_slice() != key.encoded().as_slice() {
                break;
            }
            ok = cursor.next(&mut self.statistics.data);
        }
        Ok(v)
    }

    // Returns true if it needs gc.
    // This is for optimization purpose, does not mean to be accurate.
    pub fn need_gc(&self, safe_point: u64, ratio_threshold: f64) -> bool {
        // Always GC.
        if ratio_threshold < 1.0 {
            return true;
        }

        let props = match self.get_mvcc_properties(safe_point) {
            Some(v) => v,
            None => return true,
        };

        // No data older than safe_point to GC.
        if props.min_ts > safe_point {
            return false;
        }

        // Note: Since the properties are file-based, it can be false positive.
        // For example, multiple files can have a different version of the same row.

        // A lot of MVCC versions to GC.
        if props.num_versions as f64 > props.num_rows as f64 * ratio_threshold {
            return true;
        }
        // A lot of non-effective MVCC versions to GC.
        if props.num_versions as f64 > props.num_puts as f64 * ratio_threshold {
            return true;
        }

        // A lot of MVCC versions of a single row to GC.
        props.max_row_versions > GC_MAX_ROW_VERSIONS_THRESHOLD
    }

    fn get_mvcc_properties(&self, safe_point: u64) -> Option<MvccProperties> {
        let collection = match self.snapshot.get_properties_cf(CF_WRITE) {
            Ok(v) => v,
            Err(_) => return None,
        };
        if collection.is_empty() {
            return None;
        }
        // Aggregate MVCC properties.
        let mut props = MvccProperties::new();
        for (_, v) in &*collection {
            let mvcc = match MvccProperties::decode(v.user_collected_properties()) {
                Ok(v) => v,
                Err(_) => return None,
            };
            // Filter out properties after safe_point.
            if mvcc.min_ts > safe_point {
                continue;
            }
            props.add(&mvcc);
        }
        Some(props)
    }
}

#[cfg(test)]
mod tests {
    use std::u64;
    use kvproto::metapb::{Peer, Region};
    use kvproto::kvrpcpb::IsolationLevel;
    use rocksdb::{self, Writable, WriteBatch, DB};
    use std::sync::Arc;
    use storage::{make_key, Mutation, Options, ALL_CFS, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
    use storage::engine::Modify;
    use storage::mvcc::{MvccReader, MvccTxn};
    use tempdir::TempDir;
    use raftstore::store::RegionSnapshot;
    use raftstore::store::keys;
    use util::rocksdb::{self as rocksdb_util, CFOptions};
    use util::properties::{MvccProperties, MvccPropertiesCollectorFactory};

    struct RegionEngine {
        db: Arc<DB>,
        region: Region,
    }

    impl RegionEngine {
        pub fn new(db: Arc<DB>, region: Region) -> RegionEngine {
            RegionEngine {
                db: Arc::clone(&db),
                region: region,
            }
        }

        pub fn put(&mut self, pk: &[u8], start_ts: u64, commit_ts: u64) {
            let m = Mutation::Put((make_key(pk), vec![]));
            self.prewrite(m, pk, start_ts);
            self.commit(pk, start_ts, commit_ts);
        }

        pub fn lock(&mut self, pk: &[u8], start_ts: u64, commit_ts: u64) {
            let m = Mutation::Lock(make_key(pk));
            self.prewrite(m, pk, start_ts);
            self.commit(pk, start_ts, commit_ts);
        }

        pub fn delete(&mut self, pk: &[u8], start_ts: u64, commit_ts: u64) {
            let m = Mutation::Delete(make_key(pk));
            self.prewrite(m, pk, start_ts);
            self.commit(pk, start_ts, commit_ts);
        }

        fn prewrite(&mut self, m: Mutation, pk: &[u8], start_ts: u64) {
            let snap = RegionSnapshot::from_raw(Arc::clone(&self.db), self.region.clone());
            let mut txn = MvccTxn::new(Box::new(snap), start_ts, None, IsolationLevel::SI, true);
            txn.prewrite(m, pk, &Options::default()).unwrap();

            self.write(txn.into_modifies());
        }

        fn commit(&mut self, pk: &[u8], start_ts: u64, commit_ts: u64) {
            let k = make_key(pk);
            let snap = RegionSnapshot::from_raw(Arc::clone(&self.db), self.region.clone());
            let mut txn = MvccTxn::new(Box::new(snap), start_ts, None, IsolationLevel::SI, true);
            txn.commit(&k, commit_ts).unwrap();
            self.write(txn.into_modifies());
        }

        fn gc(&mut self, pk: &[u8], safe_point: u64) {
            let k = make_key(pk);
            let snap = RegionSnapshot::from_raw(Arc::clone(&self.db), self.region.clone());
            let mut txn = MvccTxn::new(Box::new(snap), safe_point, None, IsolationLevel::SI, true);
            txn.gc(&k, safe_point).unwrap();
            self.write(txn.into_modifies());
        }

        fn write(&mut self, modifies: Vec<Modify>) {
            let db = &self.db;
            let wb = WriteBatch::new();
            for rev in modifies {
                match rev {
                    Modify::Put(cf, k, v) => {
                        let k = keys::data_key(k.encoded());
                        let handle = rocksdb_util::get_cf_handle(db, cf).unwrap();
                        wb.put_cf(handle, &k, &v).unwrap();
                    }
                    Modify::Delete(cf, k) => {
                        let k = keys::data_key(k.encoded());
                        let handle = rocksdb_util::get_cf_handle(db, cf).unwrap();
                        wb.delete_cf(handle, &k).unwrap();
                    }
                    Modify::DeleteRange(cf, k1, k2) => {
                        let k1 = keys::data_key(k1.encoded());
                        let k2 = keys::data_key(k2.encoded());
                        let handle = rocksdb_util::get_cf_handle(db, cf).unwrap();
                        wb.delete_range_cf(handle, &k1, &k2).unwrap();
                    }
                }
            }
            db.write(wb).unwrap();
        }

        fn flush(&mut self) {
            for cf in ALL_CFS {
                let cf = rocksdb_util::get_cf_handle(&self.db, cf).unwrap();
                self.db.flush_cf(cf, true).unwrap();
            }
        }

        fn compact(&mut self) {
            for cf in ALL_CFS {
                let cf = rocksdb_util::get_cf_handle(&self.db, cf).unwrap();
                self.db.compact_range_cf(cf, None, None);
            }
        }
    }

    fn open_db(path: &str, with_properties: bool) -> Arc<DB> {
        let db_opts = rocksdb::DBOptions::new();
        let mut cf_opts = rocksdb::ColumnFamilyOptions::new();
        if with_properties {
            let f = Box::new(MvccPropertiesCollectorFactory::default());
            cf_opts.add_table_properties_collector_factory("tikv.test-collector", f);
        }
        let cfs_opts = vec![
            CFOptions::new(CF_DEFAULT, rocksdb::ColumnFamilyOptions::new()),
            CFOptions::new(CF_RAFT, rocksdb::ColumnFamilyOptions::new()),
            CFOptions::new(CF_LOCK, rocksdb::ColumnFamilyOptions::new()),
            CFOptions::new(CF_WRITE, cf_opts),
        ];
        Arc::new(rocksdb_util::new_engine_opt(path, db_opts, cfs_opts).unwrap())
    }

    fn make_region(id: u64, start_key: Vec<u8>, end_key: Vec<u8>) -> Region {
        let mut peer = Peer::new();
        peer.set_id(id);
        peer.set_store_id(id);
        let mut region = Region::new();
        region.set_id(id);
        region.set_start_key(start_key);
        region.set_end_key(end_key);
        region.mut_peers().push(peer);
        region
    }

    fn check_need_gc(
        db: Arc<DB>,
        region: Region,
        safe_point: u64,
        need_gc: bool,
    ) -> Option<MvccProperties> {
        let snap = RegionSnapshot::from_raw(Arc::clone(&db), region.clone());
        let reader = MvccReader::new(Box::new(snap), None, false, None, None, IsolationLevel::SI);
        assert_eq!(reader.need_gc(safe_point, 1.0), need_gc);
        reader.get_mvcc_properties(safe_point)
    }

    #[test]
    fn test_need_gc() {
        let path = TempDir::new("_test_storage_mvcc_reader").expect("");
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![0], vec![10]);
        test_without_properties(path, &region);
        test_with_properties(path, &region);
    }

    fn test_without_properties(path: &str, region: &Region) {
        let db = open_db(path, false);
        let mut engine = RegionEngine::new(Arc::clone(&db), region.clone());

        // Put 2 keys.
        engine.put(&[1], 1, 1);
        engine.put(&[4], 2, 2);
        assert!(check_need_gc(Arc::clone(&db), region.clone(), 10, true).is_none());
        engine.flush();
        // After this flush, we have a SST file without properties.
        // Without properties, we always need GC.
        assert!(check_need_gc(Arc::clone(&db), region.clone(), 10, true).is_none());
    }

    #[allow(cyclomatic_complexity)]
    fn test_with_properties(path: &str, region: &Region) {
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(Arc::clone(&db), region.clone());

        // Put 2 keys.
        engine.put(&[2], 3, 3);
        engine.put(&[3], 4, 4);
        engine.flush();
        // After this flush, we have a SST file w/ properties, plus the SST
        // file w/o properties from previous flush. We always need GC as
        // long as we can't get properties from any SST files.
        assert!(check_need_gc(Arc::clone(&db), region.clone(), 10, true).is_none());
        engine.compact();
        // After this compact, the two SST files are compacted into a new
        // SST file with properties. Now all SST files have properties and
        // all keys have only one version, so we don't need gc.
        let props = check_need_gc(Arc::clone(&db), region.clone(), 10, false).unwrap();
        assert_eq!(props.min_ts, 1);
        assert_eq!(props.max_ts, 4);
        assert_eq!(props.num_rows, 4);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 4);
        assert_eq!(props.max_row_versions, 1);

        // Put 2 more keys and delete them.
        engine.put(&[5], 5, 5);
        engine.put(&[6], 6, 6);
        engine.delete(&[5], 7, 7);
        engine.delete(&[6], 8, 8);
        engine.flush();
        // After this flush, keys 5,6 in the new SST file have more than one
        // versions, so we need gc.
        let props = check_need_gc(Arc::clone(&db), region.clone(), 10, true).unwrap();
        assert_eq!(props.min_ts, 1);
        assert_eq!(props.max_ts, 8);
        assert_eq!(props.num_rows, 6);
        assert_eq!(props.num_puts, 6);
        assert_eq!(props.num_versions, 8);
        assert_eq!(props.max_row_versions, 2);
        // But if the `safe_point` is older than all versions, we don't need gc too.
        let props = check_need_gc(Arc::clone(&db), region.clone(), 0, false).unwrap();
        assert_eq!(props.min_ts, u64::MAX);
        assert_eq!(props.max_ts, 0);
        assert_eq!(props.num_rows, 0);
        assert_eq!(props.num_puts, 0);
        assert_eq!(props.num_versions, 0);
        assert_eq!(props.max_row_versions, 0);

        // We gc the two deleted keys manually.
        engine.gc(&[5], 10);
        engine.gc(&[6], 10);
        engine.compact();
        // After this compact, all versions of keys 5,6 are deleted,
        // no keys have more than one versions, so we don't need gc.
        let props = check_need_gc(Arc::clone(&db), region.clone(), 10, false).unwrap();
        assert_eq!(props.min_ts, 1);
        assert_eq!(props.max_ts, 4);
        assert_eq!(props.num_rows, 4);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 4);
        assert_eq!(props.max_row_versions, 1);

        // A single lock version need gc.
        engine.lock(&[7], 9, 9);
        engine.flush();
        let props = check_need_gc(Arc::clone(&db), region.clone(), 10, true).unwrap();
        assert_eq!(props.min_ts, 1);
        assert_eq!(props.max_ts, 9);
        assert_eq!(props.num_rows, 5);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 5);
        assert_eq!(props.max_row_versions, 1);
    }
}
