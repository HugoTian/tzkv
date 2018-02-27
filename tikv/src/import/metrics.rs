// Copyright 2018 PingCAP, Inc.
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

use prometheus::*;

lazy_static! {
    pub static ref IMPORT_RPC_DURATION: HistogramVec =
        register_histogram_vec!(
            "tikv_import_rpc_duration",
            "Bucketed histogram of import rpc duration",
            &["request", "result"],
            exponential_buckets(0.001, 2.0, 30).unwrap()
        ).unwrap();

    pub static ref IMPORT_UPLOAD_CHUNK_BYTES: Histogram =
        register_histogram!(
            "tikv_import_upload_chunk_bytes",
            "Bucketed histogram of import upload chunk bytes",
            exponential_buckets(1024.0, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref IMPORT_UPLOAD_CHUNK_DURATION: Histogram =
        register_histogram!(
            "tikv_import_upload_chunk_duration",
            "Bucketed histogram of import upload chunk duration",
            exponential_buckets(0.001, 2.0, 20).unwrap()
        ).unwrap();
}
