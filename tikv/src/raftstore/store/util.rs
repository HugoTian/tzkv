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

use std::option::Option;
use std::{fmt, u64};

use kvproto::metapb;
use kvproto::eraftpb::{self, ConfChangeType, MessageType};
use kvproto::raft_serverpb::RaftMessage;
use raftstore::{Error, Result};
use raftstore::store::keys;
use rocksdb::{Range, TablePropertiesCollection, Writable, WriteBatch, DB};
use time::{Duration, Timespec};

use storage::{Key, CF_LOCK, CF_RAFT, CF_WRITE, LARGE_CFS};
use util::properties::SizeProperties;
use util::{rocksdb as rocksdb_util, Either};
use util::time::monotonic_raw_now;

use super::engine::{IterOption, Iterable};
use super::peer_storage;

pub fn find_peer(region: &metapb::Region, store_id: u64) -> Option<&metapb::Peer> {
    for peer in region.get_peers() {
        if peer.get_store_id() == store_id {
            return Some(peer);
        }
    }

    None
}

pub fn remove_peer(region: &mut metapb::Region, store_id: u64) -> Option<metapb::Peer> {
    region
        .get_peers()
        .iter()
        .position(|x| x.get_store_id() == store_id)
        .map(|i| region.mut_peers().remove(i))
}

// a helper function to create peer easily.
pub fn new_peer(store_id: u64, peer_id: u64) -> metapb::Peer {
    let mut peer = metapb::Peer::new();
    peer.set_store_id(store_id);
    peer.set_id(peer_id);
    peer
}

/// Check if key in region range [`start_key`, `end_key`].
pub fn check_key_in_region_inclusive(key: &[u8], region: &metapb::Region) -> Result<()> {
    let end_key = region.get_end_key();
    let start_key = region.get_start_key();
    if key >= start_key && (end_key.is_empty() || key <= end_key) {
        Ok(())
    } else {
        Err(Error::KeyNotInRegion(key.to_vec(), region.clone()))
    }
}

/// Check if key in region range [`start_key`, `end_key`).
pub fn check_key_in_region(key: &[u8], region: &metapb::Region) -> Result<()> {
    let end_key = region.get_end_key();
    let start_key = region.get_start_key();
    if key >= start_key && (end_key.is_empty() || key < end_key) {
        Ok(())
    } else {
        Err(Error::KeyNotInRegion(key.to_vec(), region.clone()))
    }
}

#[inline]
pub fn is_first_vote_msg(msg: &RaftMessage) -> bool {
    msg.get_message().get_msg_type() == MessageType::MsgRequestVote
        && msg.get_message().get_term() == peer_storage::RAFT_INIT_LOG_TERM + 1
}

const STR_CONF_CHANGE_ADD_NODE: &str = "AddNode";
const STR_CONF_CHANGE_REMOVE_NODE: &str = "RemoveNode";

pub fn conf_change_type_str(conf_type: &eraftpb::ConfChangeType) -> &'static str {
    match *conf_type {
        ConfChangeType::AddNode => STR_CONF_CHANGE_ADD_NODE,
        ConfChangeType::RemoveNode => STR_CONF_CHANGE_REMOVE_NODE,
        ConfChangeType::AddLearnerNode => unimplemented!(),
    }
}

const MAX_WRITE_BATCH_SIZE: usize = 4 * 1024 * 1024;

pub fn delete_all_in_range(
    db: &DB,
    start_key: &[u8],
    end_key: &[u8],
    use_delete_range: bool,
) -> Result<()> {
    if start_key >= end_key {
        return Ok(());
    }

    for cf in db.cf_names() {
        delete_all_in_range_cf(db, cf, start_key, end_key, use_delete_range)?;
    }

    Ok(())
}

pub fn delete_all_in_range_cf(
    db: &DB,
    cf: &str,
    start_key: &[u8],
    end_key: &[u8],
    use_delete_range: bool,
) -> Result<()> {
    let handle = rocksdb_util::get_cf_handle(db, cf)?;
    let mut wb = WriteBatch::new();
    // Since CF_RAFT and CF_LOCK is usually small, so using
    // traditional way to cleanup.
    if use_delete_range && cf != CF_RAFT && cf != CF_LOCK {
        if cf == CF_WRITE {
            let start = Key::from_encoded(start_key.to_vec()).append_ts(u64::MAX);
            wb.delete_range_cf(handle, start.encoded(), end_key)?;
        } else {
            wb.delete_range_cf(handle, start_key, end_key)?;
        }
    } else {
        let iter_opt = IterOption::new(Some(start_key.to_vec()), Some(end_key.to_vec()), false);
        let mut it = db.new_iterator_cf(cf, iter_opt)?;
        it.seek(start_key.into());
        while it.valid() {
            wb.delete_cf(handle, it.key())?;
            if wb.data_size() >= MAX_WRITE_BATCH_SIZE {
                // Can't use write_without_wal here.
                // Otherwise it may cause dirty data when applying snapshot.
                db.write(wb)?;
                wb = WriteBatch::new();
            }

            if !it.next() {
                break;
            }
        }
    }

    if wb.count() > 0 {
        db.write(wb)?;
    }

    Ok(())
}

// check whether epoch is staler than check_epoch.
pub fn is_epoch_stale(epoch: &metapb::RegionEpoch, check_epoch: &metapb::RegionEpoch) -> bool {
    epoch.get_version() < check_epoch.get_version()
        || epoch.get_conf_ver() < check_epoch.get_conf_ver()
}

pub fn get_region_properties_cf(
    db: &DB,
    cfname: &str,
    region: &metapb::Region,
) -> Result<TablePropertiesCollection> {
    let cf = rocksdb_util::get_cf_handle(db, cfname)?;
    let start = keys::enc_start_key(region);
    let end = keys::enc_end_key(region);
    let range = Range::new(&start, &end);
    db.get_properties_of_tables_in_range(cf, &[range])
        .map_err(|e| e.into())
}

pub fn get_region_approximate_size_cf(
    db: &DB,
    cfname: &str,
    region: &metapb::Region,
) -> Result<u64> {
    let cf = rocksdb_util::get_cf_handle(db, cfname)?;
    let start = keys::enc_start_key(region);
    let end = keys::enc_end_key(region);
    let range = Range::new(&start, &end);
    let (_, mut size) = db.get_approximate_memtable_stats_cf(cf, &range);
    let collection = db.get_properties_of_tables_in_range(cf, &[range])?;
    for (_, v) in &*collection {
        let props = SizeProperties::decode(v.user_collected_properties())?;
        size += props.get_approximate_size_in_range(&start, &end);
    }
    Ok(size)
}

pub fn get_region_approximate_size(db: &DB, region: &metapb::Region) -> Result<u64> {
    let mut size = 0;
    for cfname in LARGE_CFS {
        size += get_region_approximate_size_cf(db, cfname, region)?
    }
    Ok(size)
}

/// Lease records an expired time, for examining the current moment is in lease or not.
/// It's dedicated to the Raft leader lease mechanism, contains either state of
///   1. Suspect Timestamp
///      A suspicious leader lease timestamp, which marks the leader may still hold or lose
///      its lease until the clock time goes over this timestamp.
///   2. Valid Timestamp
///      A valid leader lease timestamp, which marks the leader holds the lease for now.
///      The lease is valid until the clock time goes over this timestamp.
///
/// ```text
/// Time
/// |---------------------------------->
///         ^               ^
///        Now           Suspect TS
/// State:  |    Suspect    |   Suspect
///
/// |---------------------------------->
///         ^               ^
///        Now           Valid TS
/// State:  |     Valid     |   Expired
/// ```
///
/// Note:
///   - Valid timestamp would increase when raft log entries are applied in current term.
///   - Suspect timestamp would be set after the message `MsgTimeoutNow` is sent by current peer.
///     The message `MsgTimeoutNow` starts a leader transfer procedure. During this procedure,
///     current peer as an old leader may still hold its lease or lose it.
///     It's possible there is a new leader elected and current peer as an old leader
///     doesn't step down due to network partition from the new leader. In that case,
///     current peer lose its leader lease.
///     Within this suspect leader lease expire time, read requests could not be performed
///     locally.
///   - The valid leader lease should be `lease = max_lease - (commit_ts - send_ts)`
///     And the expired timestamp for that leader lease is `commit_ts + lease`,
///     which is `send_ts + max_lease` in short.
// TODO: add a remote Lease. A special lease that derives from Lease, it will be sent
//       to the local read thread, so name it remote. If Lease expires, the remote must
//       expire too.
pub struct Lease {
    // A suspect timestamp is in the Either::Left(_),
    // a valid timestamp is in the Either::Right(_).
    bound: Option<Either<Timespec, Timespec>>,
    max_lease: Duration,
}

#[derive(PartialEq, Eq, Debug)]
pub enum LeaseState {
    /// The lease is suspicious, may be invalid.
    Suspect,
    /// The lease is valid.
    Valid,
    /// The lease is expired.
    Expired,
}

impl Lease {
    pub fn new(max_lease: Duration) -> Lease {
        Lease {
            bound: None,
            max_lease: max_lease,
        }
    }

    /// The valid leader lease should be `lease = max_lease - (commit_ts - send_ts)`
    /// And the expired timestamp for that leader lease is `commit_ts + lease`,
    /// which is `send_ts + max_lease` in short.
    fn next_expired_time(&self, send_ts: Timespec) -> Timespec {
        send_ts + self.max_lease
    }

    /// Renew the lease to the bound.
    pub fn renew(&mut self, send_ts: Timespec) {
        let bound = self.next_expired_time(send_ts);
        match self.bound {
            // Longer than suspect ts or longer than valid ts.
            Some(Either::Left(ts)) | Some(Either::Right(ts)) => if ts <= bound {
                self.bound = Some(Either::Right(bound));
            },
            // Or an empty lease
            None => {
                self.bound = Some(Either::Right(bound));
            }
        }
    }

    /// Suspect the lease to the bound.
    pub fn suspect(&mut self, send_ts: Timespec) {
        let bound = self.next_expired_time(send_ts);
        self.bound = Some(Either::Left(bound));
    }

    /// Inspect the lease state for the ts or now.
    pub fn inspect(&self, ts: Option<Timespec>) -> LeaseState {
        match self.bound {
            Some(Either::Left(_)) => LeaseState::Suspect,
            Some(Either::Right(bound)) => if ts.unwrap_or_else(monotonic_raw_now) < bound {
                LeaseState::Valid
            } else {
                LeaseState::Expired
            },
            None => LeaseState::Expired,
        }
    }

    pub fn expire(&mut self) {
        self.bound = None;
    }
}

impl fmt::Debug for Lease {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let mut fmter = fmt.debug_struct("Lease");
        match self.bound {
            Some(Either::Left(ts)) => fmter.field("suspect", &ts).finish(),
            Some(Either::Right(ts)) => fmter.field("valid", &ts).finish(),
            None => fmter.finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::process;
    use std::thread;

    use kvproto::metapb;
    use kvproto::raft_serverpb::RaftMessage;
    use kvproto::eraftpb::{ConfChangeType, Message, MessageType};
    use rocksdb::{ColumnFamilyOptions, DBOptions, SeekKey, Writable, WriteBatch, DB};
    use tempdir::TempDir;
    use time::Duration as TimeDuration;

    use raftstore::store::peer_storage;
    use util::properties::SizePropertiesCollectorFactory;
    use util::rocksdb::{get_cf_handle, new_engine_opt, CFOptions};
    use util::time::monotonic_raw_now;
    use storage::{Key, ALL_CFS};
    use super::*;

    #[test]
    fn test_lease() {
        let duration = TimeDuration::milliseconds(1500);

        // Empty lease.
        let mut lease = Lease::new(duration);
        assert_eq!(
            lease.inspect(Some(monotonic_raw_now())),
            LeaseState::Expired
        );

        let now = monotonic_raw_now();
        let next_expired_time = lease.next_expired_time(now);
        assert_eq!(next_expired_time, now + duration);

        // Transit to the Valid state.
        lease.renew(now);
        assert_eq!(lease.inspect(Some(monotonic_raw_now())), LeaseState::Valid);
        assert_eq!(lease.inspect(None), LeaseState::Valid);

        // After lease expired time.
        thread::sleep(duration.to_std().unwrap());
        assert_eq!(
            lease.inspect(Some(monotonic_raw_now())),
            LeaseState::Expired
        );
        assert_eq!(lease.inspect(None), LeaseState::Expired);

        // Transit to the Suspect state.
        lease.suspect(monotonic_raw_now());
        assert_eq!(
            lease.inspect(Some(monotonic_raw_now())),
            LeaseState::Suspect
        );
        assert_eq!(lease.inspect(None), LeaseState::Suspect);

        // After lease expired time. Always suspect.
        thread::sleep(duration.to_std().unwrap());
        assert_eq!(
            lease.inspect(Some(monotonic_raw_now())),
            LeaseState::Suspect
        );

        // Clear lease.
        lease.expire();
        assert_eq!(
            lease.inspect(Some(monotonic_raw_now())),
            LeaseState::Expired
        );
    }

    // Tests the util function `check_key_in_region`.
    #[test]
    fn test_check_key_in_region() {
        let test_cases = vec![
            ("", "", "", true, true),
            ("", "", "6", true, true),
            ("", "3", "6", false, false),
            ("4", "3", "6", true, true),
            ("4", "3", "", true, true),
            ("2", "3", "6", false, false),
            ("", "3", "6", false, false),
            ("", "3", "", false, false),
            ("6", "3", "6", false, true),
        ];
        for (key, start_key, end_key, is_in_region, is_in_region_inclusive) in test_cases {
            let mut region = metapb::Region::new();
            region.set_start_key(start_key.as_bytes().to_vec());
            region.set_end_key(end_key.as_bytes().to_vec());
            let mut result = check_key_in_region(key.as_bytes(), &region);
            assert_eq!(result.is_ok(), is_in_region);
            result = check_key_in_region_inclusive(key.as_bytes(), &region);
            assert_eq!(result.is_ok(), is_in_region_inclusive)
        }
    }

    #[test]
    fn test_peer() {
        let mut region = metapb::Region::new();
        region.set_id(1);
        region.mut_peers().push(new_peer(1, 1));

        assert!(find_peer(&region, 1).is_some());
        assert!(find_peer(&region, 10).is_none());

        assert!(remove_peer(&mut region, 1).is_some());
        assert!(remove_peer(&mut region, 1).is_none());
        assert!(find_peer(&region, 1).is_none());
    }

    #[test]
    fn test_first_vote_msg() {
        let tbl = vec![
            (
                MessageType::MsgRequestVote,
                peer_storage::RAFT_INIT_LOG_TERM + 1,
                true,
            ),
            (
                MessageType::MsgRequestVote,
                peer_storage::RAFT_INIT_LOG_TERM,
                false,
            ),
            (
                MessageType::MsgHup,
                peer_storage::RAFT_INIT_LOG_TERM + 1,
                false,
            ),
        ];

        for (msg_type, term, is_vote) in tbl {
            let mut msg = Message::new();
            msg.set_msg_type(msg_type);
            msg.set_term(term);

            let mut m = RaftMessage::new();
            m.set_message(msg);
            assert_eq!(is_first_vote_msg(&m), is_vote);
        }
    }

    #[test]
    fn test_conf_change_type_str() {
        assert_eq!(
            conf_change_type_str(&ConfChangeType::AddNode),
            STR_CONF_CHANGE_ADD_NODE
        );
        assert_eq!(
            conf_change_type_str(&ConfChangeType::RemoveNode),
            STR_CONF_CHANGE_REMOVE_NODE
        );
    }

    #[test]
    fn test_epoch_stale() {
        let mut epoch = metapb::RegionEpoch::new();
        epoch.set_version(10);
        epoch.set_conf_ver(10);

        let tbl = vec![
            (11, 10, true),
            (10, 11, true),
            (10, 10, false),
            (10, 9, false),
        ];

        for (version, conf_version, is_stale) in tbl {
            let mut check_epoch = metapb::RegionEpoch::new();
            check_epoch.set_version(version);
            check_epoch.set_conf_ver(conf_version);
            assert_eq!(is_epoch_stale(&epoch, &check_epoch), is_stale);
        }
    }

    fn make_region(id: u64, start_key: Vec<u8>, end_key: Vec<u8>) -> metapb::Region {
        let mut peer = metapb::Peer::new();
        peer.set_id(id);
        peer.set_store_id(id);
        let mut region = metapb::Region::new();
        region.set_id(id);
        region.set_start_key(start_key);
        region.set_end_key(end_key);
        region.mut_peers().push(peer);
        region
    }

    #[test]
    fn test_region_approximate_size() {
        let path = TempDir::new("_test_raftstore_region_approximate_size").expect("");
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_level_zero_file_num_compaction_trigger(10);
        let f = Box::new(SizePropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("tikv.size-collector", f);
        let cfs_opts = LARGE_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let db = rocksdb_util::new_engine_opt(path_str, db_opts, cfs_opts).unwrap();

        let cases = [("a", 1024), ("b", 2048), ("c", 4096)];
        let cf_size = 2 + 1024 + 2 + 2048 + 2 + 4096;
        for &(key, vlen) in &cases {
            for cfname in LARGE_CFS {
                let k1 = keys::data_key(key.as_bytes());
                let v1 = vec![0; vlen as usize];
                assert_eq!(k1.len(), 2);
                let cf = db.cf_handle(cfname).unwrap();
                db.put_cf(cf, &k1, &v1).unwrap();
                db.flush_cf(cf, true).unwrap();
            }
        }

        let region = make_region(1, vec![], vec![]);
        let size = get_region_approximate_size(&db, &region).unwrap();
        assert_eq!(size, cf_size * LARGE_CFS.len() as u64);
        for cfname in LARGE_CFS {
            let size = get_region_approximate_size_cf(&db, cfname, &region).unwrap();
            assert_eq!(size, cf_size);
        }
    }

    fn check_data(db: &DB, cfs: &[&str], expected: &[(&[u8], &[u8])]) {
        for cf in cfs {
            let handle = get_cf_handle(db, cf).unwrap();
            let mut iter = db.iter_cf(handle);
            iter.seek(SeekKey::Start);
            for &(k, v) in expected {
                assert_eq!(k, iter.key());
                assert_eq!(v, iter.value());
                iter.next();
            }
            assert!(!iter.valid());
        }
    }

    fn test_delete_all_in_range(use_delete_range: bool) {
        let path = TempDir::new("_raftstore_util_delete_all_in_range").expect("");
        let path_str = path.path().to_str().unwrap();

        let cfs_opts = ALL_CFS
            .into_iter()
            .map(|cf| CFOptions::new(cf, ColumnFamilyOptions::new()))
            .collect();
        let db = new_engine_opt(path_str, DBOptions::new(), cfs_opts).unwrap();

        let wb = WriteBatch::new();
        let ts: u64 = 12345;
        let keys = vec![
            Key::from_raw(b"k1").append_ts(ts),
            Key::from_raw(b"k2").append_ts(ts),
            Key::from_raw(b"k3").append_ts(ts),
            Key::from_raw(b"k4").append_ts(ts),
        ];

        let mut kvs: Vec<(&[u8], &[u8])> = vec![];
        for (_, key) in keys.iter().enumerate() {
            kvs.push((key.encoded().as_slice(), b"value"));
        }
        let kvs_left: Vec<(&[u8], &[u8])> = vec![(kvs[0].0, kvs[0].1), (kvs[3].0, kvs[3].1)];
        for &(k, v) in kvs.as_slice() {
            for cf in ALL_CFS {
                let handle = get_cf_handle(&db, cf).unwrap();
                wb.put_cf(handle, k, v).unwrap();
            }
        }
        db.write(wb).unwrap();
        check_data(&db, ALL_CFS, kvs.as_slice());

        // Delete all in ["k2", "k4").
        let start = Key::from_raw(b"k2");
        let end = Key::from_raw(b"k4");
        delete_all_in_range(
            &db,
            start.encoded().as_slice(),
            end.encoded().as_slice(),
            use_delete_range,
        ).unwrap();
        check_data(&db, ALL_CFS, kvs_left.as_slice());
    }

    #[test]
    fn test_delete_all_in_range_use_delete_range() {
        test_delete_all_in_range(true);
    }

    #[test]
    fn test_delete_all_in_range_not_use_delete_range() {
        test_delete_all_in_range(false);
    }

    fn exit_with_err(msg: String) -> ! {
        error!("{}", msg);
        process::exit(1)
    }

    #[test]
    fn test_delete_range_prefix_bloom_case() {
        let path = TempDir::new("_raftstore_util_delete_range_prefix_bloom").expect("");
        let path_str = path.path().to_str().unwrap();

        let mut opts = DBOptions::new();
        opts.create_if_missing(true);

        let mut cf_opts = ColumnFamilyOptions::new();
        // Prefix extractor(trim the timestamp at tail) for write cf.
        cf_opts
            .set_prefix_extractor(
                "FixedSuffixSliceTransform",
                Box::new(rocksdb_util::FixedSuffixSliceTransform::new(8)),
            )
            .unwrap_or_else(|err| exit_with_err(format!("{:?}", err)));
        // Create prefix bloom filter for memtable.
        cf_opts.set_memtable_prefix_bloom_size_ratio(0.1 as f64);
        let cf = "default";
        let db = DB::open_cf(opts, path_str, vec![(cf, cf_opts)]).unwrap();
        let wb = WriteBatch::new();
        let kvs: Vec<(&[u8], &[u8])> = vec![
            (b"kabcdefg1", b"v1"),
            (b"kabcdefg2", b"v2"),
            (b"kabcdefg3", b"v3"),
            (b"kabcdefg4", b"v4"),
        ];
        let kvs_left: Vec<(&[u8], &[u8])> = vec![(b"kabcdefg1", b"v1"), (b"kabcdefg4", b"v4")];

        for &(k, v) in kvs.as_slice() {
            let handle = get_cf_handle(&db, cf).unwrap();
            wb.put_cf(handle, k, v).unwrap();
        }
        db.write(wb).unwrap();
        check_data(&db, &[cf], kvs.as_slice());

        // Delete all in ["k2", "k4").
        delete_all_in_range(&db, b"kabcdefg2", b"kabcdefg4", true).unwrap();
        check_data(&db, &[cf], kvs_left.as_slice());
    }
}
