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

use std::fmt::{self, Debug, Formatter};
use std::io::Error as IoError;
use std::time::Duration;
use std::result;

use kvproto::raft_cmdpb::{CmdType, DeleteRangeRequest, DeleteRequest, PutRequest, RaftCmdRequest,
                          RaftCmdResponse, RaftRequestHeader, Request, Response};
use kvproto::errorpb;
use kvproto::kvrpcpb::Context;
use protobuf::RepeatedField;

use server::transport::RaftStoreRouter;
use raftstore::store::{self, Callback as StoreCallback, ReadResponse, WriteResponse};
use raftstore::errors::Error as RaftServerError;
use raftstore::store::{RegionIterator, RegionSnapshot};
use raftstore::store::engine::Peekable;
use raftstore::store::engine::IterOption;
use rocksdb::TablePropertiesCollection;
use storage::{self, engine, CfName, Key, Value, CF_DEFAULT};
use super::{BatchCallback, Callback, CbContext, Cursor, Engine, Iterator as EngineIterator,
            Modify, ScanMode, Snapshot};
use super::metrics::*;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        RequestFailed(e: errorpb::Error) {
            from()
            description(e.get_message())
        }
        Io(e: IoError) {
            from()
            cause(e)
            description(e.description())
        }
        RocksDb(reason: String) {
            description(reason)
        }
        Server(e: RaftServerError) {
            from()
            cause(e)
            description(e.description())
        }
        InvalidResponse(reason: String) {
            description(reason)
        }
        InvalidRequest(reason: String) {
            description(reason)
        }
        Timeout(d: Duration) {
            description("request timeout")
            display("timeout after {:?}", d)
        }
    }
}

fn get_tag_from_error(e: &Error) -> &'static str {
    match *e {
        Error::RequestFailed(ref header) => storage::get_tag_from_header(header),
        Error::Io(_) => "io",
        Error::RocksDb(_) => "rocksdb",
        Error::Server(_) => "server",
        Error::InvalidResponse(_) => "invalid_resp",
        Error::InvalidRequest(_) => "invalid_req",
        Error::Timeout(_) => "timeout",
    }
}

fn get_tag_from_engine_error(e: &engine::Error) -> &'static str {
    match *e {
        engine::Error::Request(ref header) => storage::get_tag_from_header(header),
        engine::Error::RocksDb(_) => "rocksdb",
        engine::Error::Timeout(_) => "timeout",
        engine::Error::EmptyRequest => "empty request",
        engine::Error::Other(_) => "other",
    }
}

pub type Result<T> = result::Result<T, Error>;

impl From<Error> for engine::Error {
    fn from(e: Error) -> engine::Error {
        match e {
            Error::RequestFailed(e) => engine::Error::Request(e),
            Error::Server(e) => e.into(),
            e => box_err!(e),
        }
    }
}

impl From<RaftServerError> for engine::Error {
    fn from(e: RaftServerError) -> engine::Error {
        engine::Error::Request(e.into())
    }
}

/// `RaftKv` is a storage engine base on `RaftStore`.
#[derive(Clone)]
pub struct RaftKv<S: RaftStoreRouter + 'static> {
    router: S,
}

enum CmdRes {
    Resp(Vec<Response>),
    Snap(RegionSnapshot),
}

fn new_ctx(resp: &RaftCmdResponse) -> CbContext {
    let mut cb_ctx = CbContext::new();
    cb_ctx.term = Some(resp.get_header().get_current_term());
    cb_ctx
}

fn check_raft_cmd_response(resp: &mut RaftCmdResponse, req_cnt: usize) -> Result<()> {
    if resp.get_header().has_error() {
        return Err(Error::RequestFailed(resp.take_header().take_error()));
    }
    if req_cnt != resp.get_responses().len() {
        return Err(Error::InvalidResponse(format!(
            "responses count {} is not equal to requests count {}",
            resp.get_responses().len(),
            req_cnt
        )));
    }

    Ok(())
}

fn on_write_result(mut write_resp: WriteResponse, req_cnt: usize) -> (CbContext, Result<CmdRes>) {
    let cb_ctx = new_ctx(&write_resp.response);
    if let Err(e) = check_raft_cmd_response(&mut write_resp.response, req_cnt) {
        return (cb_ctx, Err(e));
    }
    let resps = write_resp.response.take_responses();
    (cb_ctx, Ok(CmdRes::Resp(resps.into_vec())))
}

fn on_read_result(mut read_resp: ReadResponse, req_cnt: usize) -> (CbContext, Result<CmdRes>) {
    let cb_ctx = new_ctx(&read_resp.response);
    if let Err(e) = check_raft_cmd_response(&mut read_resp.response, req_cnt) {
        return (cb_ctx, Err(e));
    }
    let resps = read_resp.response.take_responses();
    if resps.len() >= 1 || resps[0].get_cmd_type() == CmdType::Snap {
        (cb_ctx, Ok(CmdRes::Snap(read_resp.snapshot.unwrap())))
    } else {
        (cb_ctx, Ok(CmdRes::Resp(resps.into_vec())))
    }
}

impl<S: RaftStoreRouter> RaftKv<S> {
    /// Create a RaftKv using specified configuration.
    pub fn new(router: S) -> RaftKv<S> {
        RaftKv { router }
    }

    fn batch_call_snap_commands(
        &self,
        batch: Vec<RaftCmdRequest>,
        on_finished: BatchCallback<CmdRes>,
    ) -> Result<()> {
        let batch_size = batch.len();
        let mut counts = Vec::with_capacity(batch_size);
        for req in &batch {
            let count = req.get_requests().len();
            counts.push(count);
        }
        let on_finished: store::BatchReadCallback =
            Box::new(move |resps: Vec<Option<store::ReadResponse>>| {
                assert_eq!(batch_size, resps.len());
                let mut cmd_resps = Vec::with_capacity(resps.len());
                for (count, resp) in counts.into_iter().zip(resps) {
                    match resp {
                        Some(resp) => {
                            let (cb_ctx, cmd_res) = on_read_result(resp, count);
                            cmd_resps.push(Some((cb_ctx, cmd_res.map_err(Error::into))));
                        }
                        None => cmd_resps.push(None),
                    }
                }
                on_finished(cmd_resps);
            });

        self.router.send_batch_commands(batch, on_finished)?;
        Ok(())
    }

    fn new_request_header(&self, ctx: &Context) -> RaftRequestHeader {
        let mut header = RaftRequestHeader::new();
        header.set_region_id(ctx.get_region_id());
        header.set_peer(ctx.get_peer().clone());
        header.set_region_epoch(ctx.get_region_epoch().clone());
        if ctx.get_term() != 0 {
            header.set_term(ctx.get_term());
        }
        header.set_sync_log(ctx.get_sync_log());
        header
    }

    fn exec_read_requests(
        &self,
        ctx: &Context,
        reqs: Vec<Request>,
        cb: Callback<CmdRes>,
    ) -> Result<()> {
        let len = reqs.len();
        let header = self.new_request_header(ctx);
        let mut cmd = RaftCmdRequest::new();
        cmd.set_header(header);
        cmd.set_requests(RepeatedField::from_vec(reqs));

        self.router
            .send_command(
                cmd,
                StoreCallback::Read(box move |resp| {
                    let (cb_ctx, res) = on_read_result(resp, len);
                    cb((cb_ctx, res.map_err(Error::into)));
                }),
            )
            .map_err(From::from)
    }

    fn exec_write_requests(
        &self,
        ctx: &Context,
        reqs: Vec<Request>,
        cb: Callback<CmdRes>,
    ) -> Result<()> {
        let len = reqs.len();
        let header = self.new_request_header(ctx);
        let mut cmd = RaftCmdRequest::new();
        cmd.set_header(header);
        cmd.set_requests(RepeatedField::from_vec(reqs));

        self.router
            .send_command(
                cmd,
                StoreCallback::Write(box move |resp| {
                    let (cb_ctx, res) = on_write_result(resp, len);
                    cb((cb_ctx, res.map_err(Error::into)));
                }),
            )
            .map_err(From::from)
    }

    fn batch_exec_snap_requests(
        &self,
        batch: Vec<(Context, Vec<Request>)>,
        on_finished: BatchCallback<CmdRes>,
    ) -> Result<()> {
        let batch = batch.into_iter().map(|(ctx, reqs)| {
            let header = self.new_request_header(&ctx);
            let mut cmd = RaftCmdRequest::new();
            cmd.set_header(header);
            cmd.set_requests(RepeatedField::from_vec(reqs));

            cmd
        });

        self.batch_call_snap_commands(batch.collect(), on_finished)
    }
}

fn invalid_resp_type(exp: CmdType, act: CmdType) -> Error {
    Error::InvalidResponse(format!(
        "cmd type not match, want {:?}, got {:?}!",
        exp, act
    ))
}

impl<S: RaftStoreRouter> Debug for RaftKv<S> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "RaftKv")
    }
}

impl<S: RaftStoreRouter> Engine for RaftKv<S> {
    fn async_write(
        &self,
        ctx: &Context,
        modifies: Vec<Modify>,
        cb: Callback<()>,
    ) -> engine::Result<()> {
        fail_point!("raftkv_async_write");
        if modifies.is_empty() {
            return Err(engine::Error::EmptyRequest);
        }

        let mut reqs = Vec::with_capacity(modifies.len());
        for m in modifies {
            let mut req = Request::new();
            match m {
                Modify::Delete(cf, k) => {
                    let mut delete = DeleteRequest::new();
                    delete.set_key(k.encoded().to_owned());
                    if cf != CF_DEFAULT {
                        delete.set_cf(cf.to_string());
                    }
                    req.set_cmd_type(CmdType::Delete);
                    req.set_delete(delete);
                }
                Modify::Put(cf, k, v) => {
                    let mut put = PutRequest::new();
                    put.set_key(k.encoded().to_owned());
                    put.set_value(v);
                    if cf != CF_DEFAULT {
                        put.set_cf(cf.to_string());
                    }
                    req.set_cmd_type(CmdType::Put);
                    req.set_put(put);
                }
                Modify::DeleteRange(cf, start_key, end_key) => {
                    let mut delete_range = DeleteRangeRequest::new();
                    delete_range.set_cf(cf.to_string());
                    delete_range.set_start_key(start_key.encoded().to_owned());
                    delete_range.set_end_key(end_key.encoded().to_owned());
                    req.set_cmd_type(CmdType::DeleteRange);
                    req.set_delete_range(delete_range);
                }
            }
            reqs.push(req);
        }

        ASYNC_REQUESTS_COUNTER_VEC
            .with_label_values(&["write", "all"])
            .inc();
        let req_timer = ASYNC_REQUESTS_DURATIONS_VEC
            .with_label_values(&["write"])
            .start_coarse_timer();

        self.exec_write_requests(ctx, reqs, box move |(cb_ctx, res)| match res {
            Ok(CmdRes::Resp(_)) => {
                req_timer.observe_duration();
                ASYNC_REQUESTS_COUNTER_VEC
                    .with_label_values(&["write", "success"])
                    .inc();
                fail_point!("raftkv_async_write_finish");
                cb((cb_ctx, Ok(())))
            }
            Ok(CmdRes::Snap(_)) => cb((
                cb_ctx,
                Err(box_err!("unexpect snapshot, should mutate instead.")),
            )),
            Err(e) => {
                let tag = get_tag_from_engine_error(&e);
                ASYNC_REQUESTS_COUNTER_VEC
                    .with_label_values(&["write", tag])
                    .inc();
                cb((cb_ctx, Err(e)))
            }
        }).map_err(|e| {
            let tag = get_tag_from_error(&e);
            ASYNC_REQUESTS_COUNTER_VEC
                .with_label_values(&["write", tag])
                .inc();
            e.into()
        })
    }

    fn async_snapshot(&self, ctx: &Context, cb: Callback<Box<Snapshot>>) -> engine::Result<()> {
        fail_point!("raftkv_async_snapshot");
        let mut req = Request::new();
        req.set_cmd_type(CmdType::Snap);

        ASYNC_REQUESTS_COUNTER_VEC
            .with_label_values(&["snapshot", "all"])
            .inc();
        let req_timer = ASYNC_REQUESTS_DURATIONS_VEC
            .with_label_values(&["snapshot"])
            .start_coarse_timer();

        self.exec_read_requests(ctx, vec![req], box move |(cb_ctx, res)| match res {
            Ok(CmdRes::Resp(r)) => cb((
                cb_ctx,
                Err(invalid_resp_type(CmdType::Snap, r[0].get_cmd_type()).into()),
            )),
            Ok(CmdRes::Snap(s)) => {
                req_timer.observe_duration();
                ASYNC_REQUESTS_COUNTER_VEC
                    .with_label_values(&["snapshot", "success"])
                    .inc();
                fail_point!("raftkv_async_snapshot_finish");
                cb((cb_ctx, Ok(box s)))
            }
            Err(e) => {
                let tag = get_tag_from_engine_error(&e);
                ASYNC_REQUESTS_COUNTER_VEC
                    .with_label_values(&["snapshot", tag])
                    .inc();
                cb((cb_ctx, Err(e)))
            }
        }).map_err(|e| {
            let tag = get_tag_from_error(&e);
            ASYNC_REQUESTS_COUNTER_VEC
                .with_label_values(&["snapshot", tag])
                .inc();
            e.into()
        })
    }

    fn async_batch_snapshot(
        &self,
        batch: Vec<Context>,
        on_finished: BatchCallback<Box<Snapshot>>,
    ) -> engine::Result<()> {
        fail_point!("raftkv_async_batch_snapshot");
        if batch.is_empty() {
            return Err(engine::Error::EmptyRequest);
        }

        let batch_size = batch.len();
        ASYNC_REQUESTS_COUNTER_VEC
            .with_label_values(&["snapshot", "all"])
            .inc_by(batch_size as f64)
            .unwrap();
        let req_timer = ASYNC_REQUESTS_DURATIONS_VEC
            .with_label_values(&["snapshot"])
            .start_coarse_timer();

        let on_finished: BatchCallback<CmdRes> = box move |cmd_resps: super::BatchResults<_>| {
            req_timer.observe_duration();
            assert_eq!(batch_size, cmd_resps.len());
            let mut snapshots = Vec::with_capacity(cmd_resps.len());
            for resp in cmd_resps {
                match resp {
                    Some((cb_ctx, Ok(CmdRes::Resp(r)))) => {
                        snapshots.push(Some((
                            cb_ctx,
                            Err(invalid_resp_type(CmdType::Snap, r[0].get_cmd_type()).into()),
                        )));
                    }
                    Some((cb_ctx, Ok(CmdRes::Snap(s)))) => {
                        ASYNC_REQUESTS_COUNTER_VEC
                            .with_label_values(&["snapshot", "success"])
                            .inc();
                        snapshots.push(Some((cb_ctx, Ok(box s as Box<Snapshot>))));
                    }
                    Some((cb_ctx, Err(e))) => {
                        let tag = get_tag_from_engine_error(&e);
                        ASYNC_REQUESTS_COUNTER_VEC
                            .with_label_values(&["snapshot", tag])
                            .inc();
                        snapshots.push(Some((cb_ctx, Err(e))));
                    }
                    None => snapshots.push(None),
                }
            }
            fail_point!("raftkv_async_batch_snapshot_finish");
            on_finished(snapshots);
        };

        let batch = batch.into_iter().map(|ctx| {
            let mut req = Request::new();
            req.set_cmd_type(CmdType::Snap);

            (ctx, vec![req])
        });

        self.batch_exec_snap_requests(batch.collect(), on_finished)
            .map_err(|e| {
                let tag = get_tag_from_error(&e);
                ASYNC_REQUESTS_COUNTER_VEC
                    .with_label_values(&["snapshot", tag])
                    .inc_by(batch_size as f64)
                    .unwrap();
                e.into()
            })
    }

    fn clone(&self) -> Box<Engine> {
        Box::new(RaftKv::new(self.router.clone()))
    }
}

impl Snapshot for RegionSnapshot {
    fn get(&self, key: &Key) -> engine::Result<Option<Value>> {
        fail_point!("raftkv_snapshot_get", |_| Err(box_err!(
            "injected error for get"
        )));
        let v = box_try!(self.get_value(key.encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_cf(&self, cf: CfName, key: &Key) -> engine::Result<Option<Value>> {
        fail_point!("raftkv_snapshot_get_cf", |_| Err(box_err!(
            "injected error for get_cf"
        )));
        let v = box_try!(self.get_value_cf(cf, key.encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn iter(&self, iter_opt: IterOption, mode: ScanMode) -> engine::Result<Cursor> {
        fail_point!("raftkv_snapshot_iter", |_| Err(box_err!(
            "injected error for iter"
        )));
        Ok(Cursor::new(
            Box::new(RegionSnapshot::iter(self, iter_opt)),
            mode,
        ))
    }

    fn iter_cf(&self, cf: CfName, iter_opt: IterOption, mode: ScanMode) -> engine::Result<Cursor> {
        fail_point!("raftkv_snapshot_iter_cf", |_| Err(box_err!(
            "injected error for iter_cf"
        )));
        Ok(Cursor::new(
            Box::new(RegionSnapshot::iter_cf(self, cf, iter_opt)?),
            mode,
        ))
    }

    fn get_properties_cf(&self, cf: CfName) -> engine::Result<TablePropertiesCollection> {
        RegionSnapshot::get_properties_cf(self, cf).map_err(|e| e.into())
    }

    fn clone(&self) -> Box<Snapshot> {
        Box::new(RegionSnapshot::clone(self))
    }
}

impl EngineIterator for RegionIterator {
    fn next(&mut self) -> bool {
        RegionIterator::next(self)
    }

    fn prev(&mut self) -> bool {
        RegionIterator::prev(self)
    }

    fn seek(&mut self, key: &Key) -> engine::Result<bool> {
        fail_point!("raftkv_iter_seek", |_| Err(box_err!(
            "injected error for iter_seek"
        )));
        RegionIterator::seek(self, key.encoded()).map_err(From::from)
    }

    fn seek_for_prev(&mut self, key: &Key) -> engine::Result<bool> {
        fail_point!("raftkv_iter_seek_for_prev", |_| Err(box_err!(
            "injected error for iter_seek_for_prev"
        )));
        RegionIterator::seek_for_prev(self, key.encoded()).map_err(From::from)
    }

    fn seek_to_first(&mut self) -> bool {
        RegionIterator::seek_to_first(self)
    }

    fn seek_to_last(&mut self) -> bool {
        RegionIterator::seek_to_last(self)
    }

    fn valid(&self) -> bool {
        RegionIterator::valid(self)
    }

    fn key(&self) -> &[u8] {
        RegionIterator::key(self)
    }

    fn value(&self) -> &[u8] {
        RegionIterator::value(self)
    }

    fn validate_key(&self, key: &Key) -> engine::Result<()> {
        self.should_seekable(key.encoded()).map_err(From::from)
    }
}
