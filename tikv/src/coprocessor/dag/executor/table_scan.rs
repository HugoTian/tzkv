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

use std::vec::IntoIter;

use kvproto::coprocessor::KeyRange;
use tipb::executor::TableScan;

use coprocessor::codec::table;
use coprocessor::endpoint::is_point;
use coprocessor::{Error, Result};
use storage::{Key, SnapshotStore};
use util::collections::HashSet;

use super::{Executor, ExecutorMetrics, Row};
use super::scanner::{ScanOn, Scanner};

pub struct TableScanExecutor {
    store: SnapshotStore,
    desc: bool,
    col_ids: HashSet<i64>,
    key_ranges: IntoIter<KeyRange>,
    scanner: Option<Scanner>,
    count: i64,
    metrics: ExecutorMetrics,
    first_collect: bool,
}

impl TableScanExecutor {
    pub fn new(
        meta: &TableScan,
        mut key_ranges: Vec<KeyRange>,
        store: SnapshotStore,
    ) -> Result<TableScanExecutor> {
        box_try!(table::check_table_ranges(&key_ranges));
        let col_ids = meta.get_columns()
            .iter()
            .filter(|c| !c.get_pk_handle())
            .map(|c| c.get_column_id())
            .collect();

        let desc = meta.get_desc();
        if desc {
            key_ranges.reverse();
        }

        Ok(TableScanExecutor {
            store: store,
            desc: desc,
            col_ids: col_ids,
            key_ranges: key_ranges.into_iter(),
            scanner: None,
            count: 0,
            metrics: Default::default(),
            first_collect: true,
        })
    }

    fn get_row_from_range_scanner(&mut self) -> Result<Option<Row>> {
        if let Some(scanner) = self.scanner.as_mut() {
            self.metrics.scan_counter.inc_range();
            let (key, value) = match scanner.next_row()? {
                Some((key, value)) => (key, value),
                None => return Ok(None),
            };
            let row_data = box_try!(table::cut_row(value, &self.col_ids));
            let h = box_try!(table::decode_handle(&key));
            return Ok(Some(Row::new(h, row_data)));
        }
        Ok(None)
    }

    fn get_row_from_point(&mut self, range: KeyRange) -> Result<Option<Row>> {
        let key = range.get_start();
        let value = self.store
            .get(&Key::from_raw(key), &mut self.metrics.cf_stats)?;
        if let Some(value) = value {
            let values = box_try!(table::cut_row(value, &self.col_ids));
            let h = box_try!(table::decode_handle(key));
            return Ok(Some(Row::new(h, values)));
        }
        Ok(None)
    }

    fn new_scanner(&self, range: KeyRange) -> Result<Scanner> {
        Scanner::new(
            &self.store,
            ScanOn::Table,
            self.desc,
            self.col_ids.is_empty(),
            range,
        ).map_err(Error::from)
    }
}

impl Executor for TableScanExecutor {
    fn next(&mut self) -> Result<Option<Row>> {
        loop {
            if let Some(row) = self.get_row_from_range_scanner()? {
                self.count += 1;
                return Ok(Some(row));
            }

            if let Some(range) = self.key_ranges.next() {
                if is_point(&range) {
                    self.metrics.scan_counter.inc_point();
                    if let Some(row) = self.get_row_from_point(range)? {
                        self.count += 1;
                        return Ok(Some(row));
                    }
                    continue;
                }
                self.scanner = match self.scanner.take() {
                    Some(mut scanner) => {
                        box_try!(scanner.reset_range(range, &self.store));
                        Some(scanner)
                    }
                    None => Some(self.new_scanner(range)?),
                };
                continue;
            }
            return Ok(None);
        }
    }

    fn collect_output_counts(&mut self, counts: &mut Vec<i64>) {
        counts.push(self.count);
        self.count = 0;
    }

    fn collect_metrics_into(&mut self, metrics: &mut ExecutorMetrics) {
        metrics.merge(&mut self.metrics);
        if let Some(scanner) = self.scanner.take() {
            scanner.collect_statistics_into(&mut metrics.cf_stats);
        }

        if self.first_collect {
            metrics.executor_count.table_scan += 1;
            self.first_collect = false;
        }
    }
}

#[cfg(test)]
mod test {
    use std::i64;

    use kvproto::kvrpcpb::IsolationLevel;
    use protobuf::RepeatedField;
    use tipb::schema::ColumnInfo;

    use storage::SnapshotStore;

    use super::*;
    use super::super::scanner::test::{get_point_range, get_range, prepare_table_data, Data,
                                      TestStore};

    const TABLE_ID: i64 = 1;
    const KEY_NUMBER: usize = 10;

    struct TableScanTestWrapper {
        data: Data,
        store: TestStore,
        table_scan: TableScan,
        ranges: Vec<KeyRange>,
        cols: Vec<ColumnInfo>,
    }

    impl TableScanTestWrapper {
        fn get_point_range(&self, handle: i64) -> KeyRange {
            get_point_range(TABLE_ID, handle)
        }
    }

    impl Default for TableScanTestWrapper {
        fn default() -> TableScanTestWrapper {
            let test_data = prepare_table_data(KEY_NUMBER, TABLE_ID);
            let test_store = TestStore::new(&test_data.kv_data);
            let mut table_scan = TableScan::new();
            // prepare cols
            let cols = test_data.get_prev_2_cols();
            let col_req = RepeatedField::from_vec(cols.clone());
            table_scan.set_columns(col_req);
            // prepare range
            let range = get_range(TABLE_ID, i64::MIN, i64::MAX);
            let key_ranges = vec![range];
            TableScanTestWrapper {
                data: test_data,
                store: test_store,
                table_scan: table_scan,
                ranges: key_ranges,
                cols: cols,
            }
        }
    }

    #[test]
    fn test_point_get() {
        let mut wrapper = TableScanTestWrapper::default();
        // point get returns none
        let r1 = wrapper.get_point_range(i64::MIN);
        // point get return something
        let handle = 0;
        let r2 = wrapper.get_point_range(handle);
        wrapper.ranges = vec![r1, r2];

        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let mut table_scanner =
            TableScanExecutor::new(&wrapper.table_scan, wrapper.ranges, store).unwrap();

        let row = table_scanner.next().unwrap().unwrap();
        assert_eq!(row.handle, handle as i64);
        assert_eq!(row.data.len(), wrapper.cols.len());

        let expect_row = &wrapper.data.expect_rows[handle as usize];
        for col in &wrapper.cols {
            let cid = col.get_column_id();
            let v = row.data.get(cid).unwrap();
            assert_eq!(expect_row[&cid], v.to_vec());
        }
        assert!(table_scanner.next().unwrap().is_none());
        let expected_counts = vec![1];
        let mut counts = Vec::with_capacity(1);
        table_scanner.collect_output_counts(&mut counts);
        assert_eq!(expected_counts, counts);
    }

    #[test]
    fn test_multiple_ranges() {
        let mut wrapper = TableScanTestWrapper::default();
        // prepare range
        let r1 = get_range(TABLE_ID, i64::MIN, 0);
        let r2 = get_range(TABLE_ID, 0, (KEY_NUMBER / 2) as i64);

        // prepare point get
        let handle = KEY_NUMBER / 2;
        let r3 = wrapper.get_point_range(handle as i64);

        let r4 = get_range(TABLE_ID, (handle + 1) as i64, i64::MAX);
        wrapper.ranges = vec![r1, r2, r3, r4];

        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let mut table_scanner =
            TableScanExecutor::new(&wrapper.table_scan, wrapper.ranges, store).unwrap();

        for handle in 0..KEY_NUMBER {
            let row = table_scanner.next().unwrap().unwrap();
            assert_eq!(row.handle, handle as i64);
            assert_eq!(row.data.len(), wrapper.cols.len());
            let expect_row = &wrapper.data.expect_rows[handle];
            for col in &wrapper.cols {
                let cid = col.get_column_id();
                let v = row.data.get(cid).unwrap();
                assert_eq!(expect_row[&cid], v.to_vec());
            }
        }
        assert!(table_scanner.next().unwrap().is_none());
    }

    #[test]
    fn test_reverse_scan() {
        let mut wrapper = TableScanTestWrapper::default();
        wrapper.table_scan.set_desc(true);

        // prepare range
        let r1 = get_range(TABLE_ID, i64::MIN, 0);
        let r2 = get_range(TABLE_ID, 0, (KEY_NUMBER / 2) as i64);

        // prepare point get
        let handle = KEY_NUMBER / 2;
        let r3 = wrapper.get_point_range(handle as i64);

        let r4 = get_range(TABLE_ID, (handle + 1) as i64, i64::MAX);
        wrapper.ranges = vec![r1, r2, r3, r4];

        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let mut table_scanner =
            TableScanExecutor::new(&wrapper.table_scan, wrapper.ranges, store).unwrap();

        for tid in 0..KEY_NUMBER {
            let handle = KEY_NUMBER - tid - 1;
            let row = table_scanner.next().unwrap().unwrap();
            assert_eq!(row.handle, handle as i64);
            assert_eq!(row.data.len(), wrapper.cols.len());
            let expect_row = &wrapper.data.expect_rows[handle];
            for col in &wrapper.cols {
                let cid = col.get_column_id();
                let v = row.data.get(cid).unwrap();
                assert_eq!(expect_row[&cid], v.to_vec());
            }
        }
        assert!(table_scanner.next().unwrap().is_none());
    }
}
