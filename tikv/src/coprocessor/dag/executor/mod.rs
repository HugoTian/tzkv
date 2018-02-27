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

use std::sync::Arc;

use tipb::executor::{self, ExecType};
use tipb::expression::{Expr, ExprType};
use tipb::schema::ColumnInfo;
use kvproto::coprocessor::KeyRange;

use coprocessor::codec::mysql;
use coprocessor::codec::datum::{self, Datum};
use coprocessor::codec::table::{RowColsDict, TableDecoder};
use coprocessor::endpoint::get_pk;
use coprocessor::dag::expr::EvalContext;
use coprocessor::{Error, Result};
use storage::SnapshotStore;
use util::codec::number::NumberDecoder;
use util::collections::HashSet;

mod scanner;
mod table_scan;
mod index_scan;
mod selection;
mod topn;
mod topn_heap;
mod limit;
mod aggregation;
mod aggregate;

mod metrics;

pub use self::table_scan::TableScanExecutor;
pub use self::index_scan::IndexScanExecutor;
pub use self::selection::SelectionExecutor;
pub use self::topn::TopNExecutor;
pub use self::limit::LimitExecutor;
pub use self::aggregation::{HashAggExecutor, StreamAggExecutor};
pub use self::scanner::{ScanOn, Scanner};
pub use self::metrics::*;

pub struct ExprColumnRefVisitor {
    cols_offset: HashSet<usize>,
    cols_len: usize,
}

impl ExprColumnRefVisitor {
    pub fn new(cols_len: usize) -> ExprColumnRefVisitor {
        ExprColumnRefVisitor {
            cols_offset: HashSet::default(),
            cols_len: cols_len,
        }
    }

    pub fn visit(&mut self, expr: &Expr) -> Result<()> {
        if expr.get_tp() == ExprType::ColumnRef {
            let offset = box_try!(expr.get_val().decode_i64()) as usize;
            if offset >= self.cols_len {
                return Err(Error::Other(box_err!(
                    "offset {} overflow, should be less than {}",
                    offset,
                    self.cols_len
                )));
            }
            self.cols_offset.insert(offset);
        } else {
            for sub_expr in expr.get_children() {
                self.visit(sub_expr)?;
            }
        }
        Ok(())
    }

    pub fn batch_visit(&mut self, exprs: &[Expr]) -> Result<()> {
        for expr in exprs {
            self.visit(expr)?;
        }
        Ok(())
    }

    pub fn column_offsets(self) -> Vec<usize> {
        self.cols_offset.into_iter().collect()
    }
}

#[derive(Debug)]
pub struct Row {
    pub handle: i64,
    pub data: RowColsDict,
}

impl Row {
    pub fn new(handle: i64, data: RowColsDict) -> Row {
        Row {
            handle: handle,
            data: data,
        }
    }

    // get binary of each column in order of columns
    pub fn get_binary_cols(&self, columns: &[ColumnInfo]) -> Result<Vec<Vec<u8>>> {
        let mut res = Vec::with_capacity(columns.len());
        for col in columns {
            if col.get_pk_handle() {
                let v = get_pk(col, self.handle);
                let bt = box_try!(datum::encode_value(&[v]));
                res.push(bt);
                continue;
            }
            let col_id = col.get_column_id();
            let value = match self.data.get(col_id) {
                None if col.has_default_val() => col.get_default_val().to_vec(),
                None if mysql::has_not_null_flag(col.get_flag() as u64) => {
                    return Err(box_err!("column {} of {} is missing", col_id, self.handle));
                }
                None => box_try!(datum::encode_value(&[Datum::Null])),
                Some(bs) => bs.to_vec(),
            };
            res.push(value);
        }
        Ok(res)
    }
}

pub trait Executor {
    fn next(&mut self) -> Result<Option<Row>>;
    fn collect_output_counts(&mut self, counts: &mut Vec<i64>);
    fn collect_metrics_into(&mut self, metrics: &mut ExecutorMetrics);
}

pub struct DAGExecutor {
    pub exec: Box<Executor>,
    pub columns: Arc<Vec<ColumnInfo>>,
    pub has_aggr: bool,
}

pub fn build_exec(
    execs: Vec<executor::Executor>,
    store: SnapshotStore,
    ranges: Vec<KeyRange>,
    ctx: Arc<EvalContext>,
) -> Result<DAGExecutor> {
    let mut execs = execs.into_iter();
    let first = execs
        .next()
        .ok_or_else(|| Error::Other(box_err!("has no executor")))?;
    let (mut src, columns) = build_first_executor(first, store, ranges)?;
    let mut has_aggr = false;
    for mut exec in execs {
        let curr: Box<Executor> = match exec.get_tp() {
            ExecType::TypeTableScan | ExecType::TypeIndexScan => {
                return Err(box_err!("got too much *scan exec, should be only one"))
            }
            ExecType::TypeSelection => Box::new(SelectionExecutor::new(
                exec.take_selection(),
                Arc::clone(&ctx),
                Arc::clone(&columns),
                src,
            )?),
            ExecType::TypeAggregation => {
                has_aggr = true;
                Box::new(HashAggExecutor::new(
                    exec.take_aggregation(),
                    Arc::clone(&ctx),
                    Arc::clone(&columns),
                    src,
                )?)
            }
            ExecType::TypeStreamAgg => {
                has_aggr = true;
                Box::new(StreamAggExecutor::new(
                    Arc::clone(&ctx),
                    src,
                    exec.take_aggregation(),
                    Arc::clone(&columns),
                )?)
            }
            ExecType::TypeTopN => Box::new(TopNExecutor::new(
                exec.take_topN(),
                Arc::clone(&ctx),
                Arc::clone(&columns),
                src,
            )?),
            ExecType::TypeLimit => Box::new(LimitExecutor::new(exec.take_limit(), src)),
        };
        src = curr;
    }
    Ok(DAGExecutor {
        exec: src,
        columns: columns,
        has_aggr: has_aggr,
    })
}

type FirstExecutor = (Box<Executor>, Arc<Vec<ColumnInfo>>);

fn build_first_executor(
    mut first: executor::Executor,
    store: SnapshotStore,
    ranges: Vec<KeyRange>,
) -> Result<FirstExecutor> {
    match first.get_tp() {
        ExecType::TypeTableScan => {
            let cols = Arc::new(first.get_tbl_scan().get_columns().to_vec());
            let ex = Box::new(TableScanExecutor::new(first.get_tbl_scan(), ranges, store)?);
            Ok((ex, cols))
        }
        ExecType::TypeIndexScan => {
            let cols = Arc::new(first.get_idx_scan().get_columns().to_vec());
            let unique = first.get_idx_scan().get_unique();
            let ex = Box::new(IndexScanExecutor::new(
                first.take_idx_scan(),
                ranges,
                store,
                unique,
            )?);
            Ok((ex, cols))
        }
        _ => Err(box_err!(
            "first exec type should be *Scan, but get {:?}",
            first.get_tp()
        )),
    }
}

pub fn inflate_with_col_for_dag(
    ctx: &EvalContext,
    values: &RowColsDict,
    columns: &[ColumnInfo],
    offsets: &[usize],
    h: i64,
) -> Result<Vec<Datum>> {
    let mut res = vec![Datum::Null; columns.len()];
    for offset in offsets {
        let col = &columns[*offset];
        if col.get_pk_handle() {
            let v = get_pk(col, h);
            res[*offset] = v;
        } else {
            let col_id = col.get_column_id();
            let value = match values.get(col_id) {
                None if col.has_default_val() => {
                    // TODO: optimize it to decode default value only once.
                    box_try!(col.get_default_val().decode_col_value(ctx, col))
                }
                None if mysql::has_not_null_flag(col.get_flag() as u64) => {
                    return Err(box_err!("column {} of {} is missing", col_id, h));
                }
                None => Datum::Null,
                Some(mut bs) => box_try!(bs.decode_col_value(ctx, col)),
            };
            res[*offset] = value;
        }
    }
    Ok(res)
}
