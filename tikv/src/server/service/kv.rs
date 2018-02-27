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

use std::boxed::FnBox;
use std::fmt::Debug;
use std::io::Write;
use std::iter::{self, FromIterator};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use mio::Token;
use grpc::{ClientStreamingSink, RequestStream, RpcContext, RpcStatus, RpcStatusCode,
           ServerStreamingSink, UnarySink};
use futures::{future, Future, Stream};
use futures::sync::oneshot;
use protobuf::RepeatedField;
use kvproto::tikvpb_grpc;
use kvproto::raft_serverpb::*;
use kvproto::kvrpcpb::*;
use kvproto::coprocessor::*;
use kvproto::errorpb::{Error as RegionError, ServerIsBusy};

use util::worker::Scheduler;
use util::collections::HashMap;
use util::buf::PipeBuffer;
use storage::{self, Key, Mutation, Options, Storage, Value};
use storage::txn::Error as TxnError;
use storage::mvcc::{Error as MvccError, Write as MvccWrite, WriteType};
use storage::engine::Error as EngineError;
use server::transport::RaftStoreRouter;
use server::snap::Task as SnapTask;
use server::metrics::*;
use server::Error;
use raftstore::store::{Callback, Msg as StoreMessage};
use coprocessor::{EndPointTask, RequestTask};

const SCHEDULER_IS_BUSY: &str = "scheduler is busy";

#[derive(Clone)]
pub struct Service<T: RaftStoreRouter + 'static> {
    // For handling KV requests.
    storage: Storage,
    // For handling coprocessor requests.
    end_point_scheduler: Scheduler<EndPointTask>,
    // For handling raft messages.
    ch: T,
    // For handling snapshot.
    snap_scheduler: Scheduler<SnapTask>,
    token: Arc<AtomicUsize>, // TODO: remove it.
    recursion_limit: u32,
    request_max_handle_secs: u64,
}

impl<T: RaftStoreRouter + 'static> Service<T> {
    pub fn new(
        storage: Storage,
        end_point_scheduler: Scheduler<EndPointTask>,
        ch: T,
        snap_scheduler: Scheduler<SnapTask>,
        recursion_limit: u32,
        request_max_handle_secs: u64,
    ) -> Service<T> {
        Service {
            storage: storage,
            end_point_scheduler: end_point_scheduler,
            ch: ch,
            snap_scheduler: snap_scheduler,
            token: Arc::new(AtomicUsize::new(1)),
            recursion_limit: recursion_limit,
            request_max_handle_secs: request_max_handle_secs,
        }
    }

    fn send_fail_status<M>(
        &self,
        ctx: RpcContext,
        sink: UnarySink<M>,
        err: Error,
        code: RpcStatusCode,
    ) {
        let status = RpcStatus::new(code, Some(format!("{}", err)));
        ctx.spawn(sink.fail(status).map_err(|_| ()));
    }
}

fn make_callback<T: Debug + Send + 'static>() -> (Box<FnBox(T) + Send>, oneshot::Receiver<T>) {
    let (tx, rx) = oneshot::channel();
    let callback = move |resp| {
        tx.send(resp).unwrap();
    };
    (box callback, rx)
}

impl<T: RaftStoreRouter + 'static> tikvpb_grpc::Tikv for Service<T> {
    fn kv_get(&self, ctx: RpcContext, mut req: GetRequest, sink: UnarySink<GetResponse>) {
        let label = "kv_get";
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .with_label_values(&[label])
            .start_coarse_timer();

        let (cb, future) = make_callback();
        let res = self.storage.async_get(
            req.take_context(),
            Key::from_raw(req.get_key()),
            req.get_version(),
            cb,
        );
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut res = GetResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    res.set_region_error(err);
                } else {
                    match v {
                        Ok(Some(val)) => res.set_value(val),
                        Ok(None) => res.set_value(vec![]),
                        Err(e) => res.set_error(extract_key_error(&e)),
                    }
                }
                res
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", label, e);
                GRPC_MSG_FAIL_COUNTER.with_label_values(&[label]).inc();
            });

        ctx.spawn(future);
    }

    fn kv_scan(&self, ctx: RpcContext, mut req: ScanRequest, sink: UnarySink<ScanResponse>) {
        let label = "kv_scan";
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .with_label_values(&[label])
            .start_coarse_timer();

        let storage = self.storage.clone();
        let mut options = Options::default();
        options.key_only = req.get_key_only();

        let (cb, future) = make_callback();
        let res = storage.async_scan(
            req.take_context(),
            Key::from_raw(req.get_start_key()),
            req.get_limit() as usize,
            req.get_version(),
            options,
            cb,
        );
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = ScanResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else {
                    resp.set_pairs(RepeatedField::from_vec(extract_kv_pairs(v)));
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", label, e);
                GRPC_MSG_FAIL_COUNTER.with_label_values(&[label]).inc();
            });

        ctx.spawn(future);
    }

    fn kv_prewrite(
        &self,
        ctx: RpcContext,
        mut req: PrewriteRequest,
        sink: UnarySink<PrewriteResponse>,
    ) {
        let label = "kv_prewrite";
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .with_label_values(&[label])
            .start_coarse_timer();

        let mutations = req.take_mutations()
            .into_iter()
            .map(|mut x| match x.get_op() {
                Op::Put => Mutation::Put((Key::from_raw(x.get_key()), x.take_value())),
                Op::Del => Mutation::Delete(Key::from_raw(x.get_key())),
                Op::Lock => Mutation::Lock(Key::from_raw(x.get_key())),
                _ => panic!("mismatch Op in prewrite mutations"),
            })
            .collect();
        let mut options = Options::default();
        options.lock_ttl = req.get_lock_ttl();
        options.skip_constraint_check = req.get_skip_constraint_check();

        let (cb, future) = make_callback();
        let res = self.storage.async_prewrite(
            req.take_context(),
            mutations,
            req.take_primary_lock(),
            req.get_start_version(),
            options,
            cb,
        );
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = PrewriteResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else {
                    resp.set_errors(RepeatedField::from_vec(extract_key_errors(v)));
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", label, e);
                GRPC_MSG_FAIL_COUNTER.with_label_values(&[label]).inc();
            });

        ctx.spawn(future);
    }

    fn kv_commit(&self, ctx: RpcContext, mut req: CommitRequest, sink: UnarySink<CommitResponse>) {
        let label = "kv_commit";
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .with_label_values(&[label])
            .start_coarse_timer();

        let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();

        let (cb, future) = make_callback();
        let res = self.storage.async_commit(
            req.take_context(),
            keys,
            req.get_start_version(),
            req.get_commit_version(),
            cb,
        );
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = CommitResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else if let Err(e) = v {
                    resp.set_error(extract_key_error(&e));
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", label, e);
                GRPC_MSG_FAIL_COUNTER.with_label_values(&[label]).inc();
            });

        ctx.spawn(future);
    }

    fn kv_import(&self, _: RpcContext, _: ImportRequest, _: UnarySink<ImportResponse>) {
        unimplemented!();
    }

    fn kv_cleanup(
        &self,
        ctx: RpcContext,
        mut req: CleanupRequest,
        sink: UnarySink<CleanupResponse>,
    ) {
        let label = "kv_cleanup";
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .with_label_values(&[label])
            .start_coarse_timer();

        let (cb, future) = make_callback();
        let res = self.storage.async_cleanup(
            req.take_context(),
            Key::from_raw(req.get_key()),
            req.get_start_version(),
            cb,
        );
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = CleanupResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else if let Err(e) = v {
                    if let Some(ts) = extract_committed(&e) {
                        resp.set_commit_version(ts);
                    } else {
                        resp.set_error(extract_key_error(&e));
                    }
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", label, e);
                GRPC_MSG_FAIL_COUNTER.with_label_values(&[label]).inc();
            });

        ctx.spawn(future);
    }

    fn kv_batch_get(
        &self,
        ctx: RpcContext,
        mut req: BatchGetRequest,
        sink: UnarySink<BatchGetResponse>,
    ) {
        let label = "kv_batchget";
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .with_label_values(&[label])
            .start_coarse_timer();

        let keys = req.get_keys()
            .into_iter()
            .map(|x| Key::from_raw(x))
            .collect();

        let (cb, future) = make_callback();
        let res = self.storage
            .async_batch_get(req.take_context(), keys, req.get_version(), cb);
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = BatchGetResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else {
                    resp.set_pairs(RepeatedField::from_vec(extract_kv_pairs(v)))
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", label, e);
                GRPC_MSG_FAIL_COUNTER.with_label_values(&[label]).inc();
            });

        ctx.spawn(future);
    }

    fn kv_batch_rollback(
        &self,
        ctx: RpcContext,
        mut req: BatchRollbackRequest,
        sink: UnarySink<BatchRollbackResponse>,
    ) {
        let label = "kv_batch_rollback";
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .with_label_values(&[label])
            .start_coarse_timer();

        let keys = req.get_keys()
            .into_iter()
            .map(|x| Key::from_raw(x))
            .collect();

        let (cb, future) = make_callback();
        let res =
            self.storage
                .async_rollback(req.take_context(), keys, req.get_start_version(), cb);
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = BatchRollbackResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else if let Err(e) = v {
                    resp.set_error(extract_key_error(&e));
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", label, e);
                GRPC_MSG_FAIL_COUNTER.with_label_values(&[label]).inc();
            });

        ctx.spawn(future);
    }

    fn kv_scan_lock(
        &self,
        ctx: RpcContext,
        mut req: ScanLockRequest,
        sink: UnarySink<ScanLockResponse>,
    ) {
        let label = "kv_scan_lock";
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .with_label_values(&[label])
            .start_coarse_timer();

        let (cb, future) = make_callback();
        let res = self.storage.async_scan_lock(
            req.take_context(),
            req.get_max_version(),
            req.take_start_key(),
            req.get_limit() as usize,
            cb,
        );
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = ScanLockResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else {
                    match v {
                        Ok(locks) => resp.set_locks(RepeatedField::from_vec(locks)),
                        Err(e) => resp.set_error(extract_key_error(&e)),
                    }
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", label, e);
                GRPC_MSG_FAIL_COUNTER.with_label_values(&[label]).inc();
            });

        ctx.spawn(future);
    }

    fn kv_resolve_lock(
        &self,
        ctx: RpcContext,
        mut req: ResolveLockRequest,
        sink: UnarySink<ResolveLockResponse>,
    ) {
        let label = "kv_resolve_lock";
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .with_label_values(&[label])
            .start_coarse_timer();

        let txn_status = if req.get_start_version() > 0 {
            HashMap::from_iter(iter::once((
                req.get_start_version(),
                req.get_commit_version(),
            )))
        } else {
            HashMap::from_iter(
                req.take_txn_infos()
                    .into_iter()
                    .map(|info| (info.txn, info.status)),
            )
        };

        let (cb, future) = make_callback();
        let res = self.storage
            .async_resolve_lock(req.take_context(), txn_status, cb);
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = ResolveLockResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else if let Err(e) = v {
                    resp.set_error(extract_key_error(&e));
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", label, e);
                GRPC_MSG_FAIL_COUNTER.with_label_values(&[label]).inc();
            });

        ctx.spawn(future);
    }

    fn kv_gc(&self, ctx: RpcContext, mut req: GCRequest, sink: UnarySink<GCResponse>) {
        let label = "kv_gc";
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .with_label_values(&[label])
            .start_coarse_timer();

        let (cb, future) = make_callback();
        let res = self.storage
            .async_gc(req.take_context(), req.get_safe_point(), cb);
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = GCResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else if let Err(e) = v {
                    resp.set_error(extract_key_error(&e));
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", label, e);
                GRPC_MSG_FAIL_COUNTER.with_label_values(&[label]).inc();
            });

        ctx.spawn(future);
    }

    fn kv_delete_range(
        &self,
        ctx: RpcContext,
        mut req: DeleteRangeRequest,
        sink: UnarySink<DeleteRangeResponse>,
    ) {
        let label = "kv_delete_range";
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .with_label_values(&[label])
            .start_coarse_timer();

        let (cb, future) = make_callback();
        let res = self.storage.async_delete_range(
            req.take_context(),
            Key::from_raw(req.get_start_key()),
            Key::from_raw(req.get_end_key()),
            cb,
        );
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = DeleteRangeResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else if let Err(e) = v {
                    resp.set_error(format!("{}", e));
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", label, e);
                GRPC_MSG_FAIL_COUNTER.with_label_values(&[label]).inc();
            });

        ctx.spawn(future);
    }

    fn raw_get(&self, ctx: RpcContext, mut req: RawGetRequest, sink: UnarySink<RawGetResponse>) {
        let label = "raw_get";
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .with_label_values(&[label])
            .start_coarse_timer();

        let (cb, future) = make_callback();
        let res = self.storage
            .async_raw_get(req.take_context(), req.take_key(), cb);
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = RawGetResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else {
                    match v {
                        Ok(Some(val)) => resp.set_value(val),
                        Ok(None) => {}
                        Err(e) => resp.set_error(format!("{}", e)),
                    }
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", label, e);
                GRPC_MSG_FAIL_COUNTER.with_label_values(&[label]).inc();
            });

        ctx.spawn(future);
    }

    fn raw_scan(&self, ctx: RpcContext, mut req: RawScanRequest, sink: UnarySink<RawScanResponse>) {
        let label = "raw_scan";
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .with_label_values(&[label])
            .start_coarse_timer();

        let (cb, future) = make_callback();
        let res = self.storage.async_raw_scan(
            req.take_context(),
            req.take_start_key(),
            req.get_limit() as usize,
            cb,
        );
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = RawScanResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else {
                    resp.set_kvs(RepeatedField::from_vec(extract_kv_pairs(v)));
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", label, e);
                GRPC_MSG_FAIL_COUNTER.with_label_values(&[label]).inc();
            });

        ctx.spawn(future);
    }

    fn raw_put(&self, ctx: RpcContext, mut req: RawPutRequest, sink: UnarySink<RawPutResponse>) {
        let label = "raw_put";
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .with_label_values(&[label])
            .start_coarse_timer();

        let (cb, future) = make_callback();
        let res =
            self.storage
                .async_raw_put(req.take_context(), req.take_key(), req.take_value(), cb);
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = RawPutResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else if let Err(e) = v {
                    resp.set_error(format!("{}", e));
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", label, e);
                GRPC_MSG_FAIL_COUNTER.with_label_values(&[label]).inc();
            });

        ctx.spawn(future);
    }

    fn raw_delete(
        &self,
        ctx: RpcContext,
        mut req: RawDeleteRequest,
        sink: UnarySink<RawDeleteResponse>,
    ) {
        let label = "raw_delete";
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .with_label_values(&[label])
            .start_coarse_timer();

        let (cb, future) = make_callback();
        let res = self.storage
            .async_raw_delete(req.take_context(), req.take_key(), cb);
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = RawDeleteResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else if let Err(e) = v {
                    resp.set_error(format!("{}", e));
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", label, e);
                GRPC_MSG_FAIL_COUNTER.with_label_values(&[label]).inc();
            });

        ctx.spawn(future);
    }

    fn coprocessor(&self, ctx: RpcContext, req: Request, sink: UnarySink<Response>) {
        let label = "coprocessor";
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .with_label_values(&[label])
            .start_coarse_timer();

        let (cb, future) = make_callback();
        let res = self.end_point_scheduler
            .schedule(EndPointTask::Request(RequestTask::new(
                req,
                cb,
                self.recursion_limit,
                self.request_max_handle_secs,
            )));
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", label, e);
                GRPC_MSG_FAIL_COUNTER.with_label_values(&[label]).inc();
            });

        ctx.spawn(future);
    }

    fn coprocessor_stream(&self, ctx: RpcContext, _: Request, sink: ServerStreamingSink<Response>) {
        let f = sink.fail(RpcStatus::new(RpcStatusCode::Unimplemented, None))
            .map_err(|e| error!("failed to report unimplemented method: {:?}", e));
        ctx.spawn(f);
    }

    fn raft(
        &self,
        ctx: RpcContext,
        stream: RequestStream<RaftMessage>,
        _: ClientStreamingSink<Done>,
    ) {
        let ch = self.ch.clone();
        ctx.spawn(
            stream
                .map_err(Error::from)
                .for_each(move |msg| {
                    RAFT_MESSAGE_RECV_COUNTER.inc();
                    future::result(ch.send_raft_msg(msg)).map_err(Error::from)
                })
                .map_err(|e| error!("send raft msg to raft store fail: {}", e))
                .then(|_| future::ok::<_, ()>(())),
        );
    }

    fn snapshot(
        &self,
        ctx: RpcContext,
        stream: RequestStream<SnapshotChunk>,
        sink: ClientStreamingSink<Done>,
    ) {
        let token = Token(self.token.fetch_add(1, Ordering::SeqCst));
        let sched = self.snap_scheduler.clone();
        let sched2 = sched.clone();
        ctx.spawn(
            stream
                .map_err(Error::from)
                .for_each(move |mut chunk| {
                    let res = if chunk.has_message() {
                        sched
                            .schedule(SnapTask::Register(token, chunk.take_message()))
                            .map_err(Error::from)
                    } else if !chunk.get_data().is_empty() {
                        // TODO: Remove PipeBuffer or take good use of it.
                        let mut b = PipeBuffer::new(chunk.get_data().len());
                        b.write_all(chunk.get_data()).unwrap();
                        sched
                            .schedule(SnapTask::Write(token, b))
                            .map_err(Error::from)
                    } else {
                        Err(box_err!("empty chunk"))
                    };
                    future::result(res)
                })
                .then(move |res| {
                    let res = match res {
                        Ok(_) => sched2.schedule(SnapTask::Close(token)),
                        Err(e) => {
                            error!("receive snapshot err: {}", e);
                            sched2.schedule(SnapTask::Discard(token))
                        }
                    };
                    future::result(res.map_err(Error::from))
                })
                .and_then(|_| sink.success(Done::new()).map_err(Error::from))
                .then(|_| future::ok::<_, ()>(())),
        );
    }

    fn mvcc_get_by_key(
        &self,
        ctx: RpcContext,
        mut req: MvccGetByKeyRequest,
        sink: UnarySink<MvccGetByKeyResponse>,
    ) {
        let label = "mvcc_get_by_key";
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .with_label_values(&[label])
            .start_coarse_timer();

        let storage = self.storage.clone();

        let key = Key::from_raw(req.get_key());
        let (cb, future) = make_callback();
        let res = storage.async_mvcc_by_key(req.take_context(), key.clone(), cb);
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = MvccGetByKeyResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else {
                    match v {
                        Ok(mvcc) => {
                            resp.set_info(extract_mvcc_info(key, mvcc));
                        }
                        Err(e) => resp.set_error(format!("{}", e)),
                    };
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", label, e);
                GRPC_MSG_FAIL_COUNTER.with_label_values(&[label]).inc();
            });

        ctx.spawn(future);
    }

    fn mvcc_get_by_start_ts(
        &self,
        ctx: RpcContext,
        mut req: MvccGetByStartTsRequest,
        sink: UnarySink<MvccGetByStartTsResponse>,
    ) {
        let label = "mvcc_get_by_start_ts";
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .with_label_values(&[label])
            .start_coarse_timer();

        let storage = self.storage.clone();

        let (cb, future) = make_callback();

        let res = storage.async_mvcc_by_start_ts(req.take_context(), req.get_start_ts(), cb);
        if let Err(e) = res {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|v| {
                let mut resp = MvccGetByStartTsResponse::new();
                if let Some(err) = extract_region_error(&v) {
                    resp.set_region_error(err);
                } else {
                    match v {
                        Ok(Some((k, vv))) => {
                            resp.set_key(k.raw().unwrap());
                            resp.set_info(extract_mvcc_info(k, vv));
                        }
                        Ok(None) => {
                            resp.set_info(Default::default());
                        }
                        Err(e) => resp.set_error(format!("{}", e)),
                    }
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", label, e);
                GRPC_MSG_FAIL_COUNTER.with_label_values(&[label]).inc();
            });
        ctx.spawn(future);
    }

    fn split_region(
        &self,
        ctx: RpcContext,
        mut req: SplitRegionRequest,
        sink: UnarySink<SplitRegionResponse>,
    ) {
        let label = "split_region";
        let timer = GRPC_MSG_HISTOGRAM_VEC
            .with_label_values(&[label])
            .start_coarse_timer();

        let (cb, future) = make_callback();
        let req = StoreMessage::SplitRegion {
            region_id: req.get_context().get_region_id(),
            region_epoch: req.take_context().take_region_epoch(),
            split_key: Key::from_raw(req.get_split_key()).encoded().clone(),
            callback: Callback::Write(cb),
        };

        if let Err(e) = self.ch.try_send(req) {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(|mut v| {
                let mut resp = SplitRegionResponse::new();
                if v.response.get_header().has_error() {
                    resp.set_region_error(v.response.mut_header().take_error());
                } else {
                    let admin_resp = v.response.mut_admin_response();
                    let split_resp = admin_resp.mut_split();
                    resp.set_left(split_resp.take_left());
                    resp.set_right(split_resp.take_right());
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                debug!("{} failed: {:?}", label, e);
                GRPC_MSG_FAIL_COUNTER.with_label_values(&[label]).inc();
            });

        ctx.spawn(future);
    }
}

fn extract_region_error<T>(res: &storage::Result<T>) -> Option<RegionError> {
    use storage::Error;
    match *res {
        // TODO: use `Error::cause` instead.
        Err(Error::Engine(EngineError::Request(ref e)))
        | Err(Error::Txn(TxnError::Engine(EngineError::Request(ref e))))
        | Err(Error::Txn(TxnError::Mvcc(MvccError::Engine(EngineError::Request(ref e))))) => {
            Some(e.to_owned())
        }
        Err(Error::SchedTooBusy) => {
            let mut err = RegionError::new();
            let mut server_is_busy_err = ServerIsBusy::new();
            server_is_busy_err.set_reason(SCHEDULER_IS_BUSY.to_owned());
            err.set_server_is_busy(server_is_busy_err);
            Some(err)
        }
        _ => None,
    }
}

fn extract_committed(err: &storage::Error) -> Option<u64> {
    match *err {
        storage::Error::Txn(TxnError::Mvcc(MvccError::Committed { commit_ts })) => Some(commit_ts),
        _ => None,
    }
}

fn extract_key_error(err: &storage::Error) -> KeyError {
    let mut key_error = KeyError::new();
    match *err {
        storage::Error::Txn(TxnError::Mvcc(MvccError::KeyIsLocked {
            ref key,
            ref primary,
            ts,
            ttl,
        })) => {
            let mut lock_info = LockInfo::new();
            lock_info.set_key(key.to_owned());
            lock_info.set_primary_lock(primary.to_owned());
            lock_info.set_lock_version(ts);
            lock_info.set_lock_ttl(ttl);
            key_error.set_locked(lock_info);
        }
        storage::Error::Txn(TxnError::Mvcc(MvccError::WriteConflict { .. }))
        | storage::Error::Txn(TxnError::Mvcc(MvccError::TxnLockNotFound { .. })) => {
            warn!("txn conflicts: {:?}", err);
            key_error.set_retryable(format!("{:?}", err));
        }
        _ => {
            error!("txn aborts: {:?}", err);
            key_error.set_abort(format!("{:?}", err));
        }
    }
    key_error
}

fn extract_kv_pairs(res: storage::Result<Vec<storage::Result<storage::KvPair>>>) -> Vec<KvPair> {
    match res {
        Ok(res) => res.into_iter()
            .map(|r| match r {
                Ok((key, value)) => {
                    let mut pair = KvPair::new();
                    pair.set_key(key);
                    pair.set_value(value);
                    pair
                }
                Err(e) => {
                    let mut pair = KvPair::new();
                    pair.set_error(extract_key_error(&e));
                    pair
                }
            })
            .collect(),
        Err(e) => {
            let mut pair = KvPair::new();
            pair.set_error(extract_key_error(&e));
            vec![pair]
        }
    }
}

fn extract_mvcc_info(key: Key, mvcc: storage::MvccInfo) -> MvccInfo {
    let mut mvcc_info = MvccInfo::new();
    if let Some(lock) = mvcc.lock {
        let mut lock_info = LockInfo::new();
        lock_info.set_primary_lock(lock.primary);
        lock_info.set_key(key.raw().unwrap());
        lock_info.set_lock_ttl(lock.ttl);
        lock_info.set_lock_version(lock.ts);
        mvcc_info.set_lock(lock_info);
    }
    let vv = extract_2pc_values(mvcc.values);
    let vw = extract_2pc_writes(mvcc.writes);
    mvcc_info.set_writes(RepeatedField::from_vec(vw));
    mvcc_info.set_values(RepeatedField::from_vec(vv));
    mvcc_info
}

fn extract_2pc_values(res: Vec<(u64, bool, Value)>) -> Vec<ValueInfo> {
    res.into_iter()
        .map(|(start_ts, is_short, value)| {
            let mut value_info = ValueInfo::new();
            value_info.set_ts(start_ts);
            value_info.set_value(value);
            value_info.set_is_short_value(is_short);
            value_info
        })
        .collect()
}

fn extract_2pc_writes(res: Vec<(u64, MvccWrite)>) -> Vec<WriteInfo> {
    res.into_iter()
        .map(|(commit_ts, write)| {
            let mut write_info = WriteInfo::new();
            write_info.set_start_ts(write.start_ts);
            let op = match write.write_type {
                WriteType::Put => Op::Put,
                WriteType::Delete => Op::Del,
                WriteType::Lock => Op::Lock,
                WriteType::Rollback => Op::Rollback,
            };
            write_info.set_field_type(op);
            write_info.set_commit_ts(commit_ts);
            write_info
        })
        .collect()
}

fn extract_key_errors(res: storage::Result<Vec<storage::Result<()>>>) -> Vec<KeyError> {
    match res {
        Ok(res) => res.into_iter()
            .filter_map(|x| match x {
                Err(e) => Some(extract_key_error(&e)),
                Ok(_) => None,
            })
            .collect(),
        Err(e) => vec![extract_key_error(&e)],
    }
}
