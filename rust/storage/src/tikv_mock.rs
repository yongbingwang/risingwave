// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
use std::future::Future;
use std::ops::RangeBounds;

use async_trait::async_trait;
use bytes::Bytes;
use risingwave_common::error::Result;

use super::StateStore;
use crate::storage_value::StorageValue;
use crate::store::*;
use crate::*;

#[derive(Clone)]
pub struct TikvStateStore {}

impl TikvStateStore {
    pub fn new(_pd_endpoints: Vec<String>) -> Self {
        unimplemented!()
    }
}

impl StateStore for TikvStateStore {
    type Iter<'a> = TikvStateStoreIter;
    define_state_store_associated_type!();

    fn get<'a>(&'a self, key: &'a [u8], epoch: u64) -> Self::GetFuture<'a> {
        async move { unimplemented!() }
    }

    fn ingest_batch(
        &self,
        _kv_pairs: Vec<(Bytes, Option<StorageValue>)>,
        _epoch: u64,
    ) -> Self::IngestBatchFuture<'_> {
        async move { unimplemented!() }
    }

    fn iter_inner<'a, R: 'a>(&'a self, key_range: &'a R, epoch: u64) -> Self::IterInnerFuture<'_, R>
    where
        R: RangeBounds<&'a [u8]> + Send + Sync + Clone,
    {
        async move { unimplemented!() }
    }

    fn iter<'a, R: 'a, B: 'a>(&'a self, key_range: R, epoch: u64) -> Self::IterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send + Clone,
        B: AsRef<[u8]> + Send + Clone,
    {
        // define_iter!(self, key_range, epoch)
        async move {
            use std::ops::Bound;
            let start_bound = match key_range.start_bound() {
                Bound::Included(start) => Bound::Included(start.as_ref()),
                Bound::Excluded(start) => Bound::Excluded(start.as_ref()),
                Bound::Unbounded => Bound::Unbounded,
            };
            let end_bound = match key_range.end_bound() {
                Bound::Included(start) => Bound::Included(start.as_ref()),
                Bound::Excluded(start) => Bound::Excluded(start.as_ref()),
                Bound::Unbounded => Bound::Unbounded,
            };
            let range = (start_bound, end_bound);
            self.iter_inner(&range, epoch).await
        }
    }

    fn replicate_batch(
        &self,
        _kv_pairs: Vec<(Bytes, Option<StorageValue>)>,
        _epoch: u64,
    ) -> Self::ReplicateBatchFuture<'_> {
        async move { unimplemented!() }
    }

    fn reverse_iter<'a, R: 'a, B: 'a>(
        &'a self,
        key_range: R,
        epoch: u64,
    ) -> Self::ReverseIterFuture<'a, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move { unimplemented!() }
    }

    fn wait_epoch(&self, _epoch: u64) -> Self::WaitEpochFuture<'_> {
        async move { unimplemented!() }
    }

    fn sync(&self, _epoch: Option<u64>) -> Self::SyncFuture<'_> {
        async move { unimplemented!() }
    }

    fn scan<'a, R: 'a, B: 'a>(
        &'a self,
        key_range: R,
        limit: Option<usize>,
        epoch: u64,
    ) -> Self::ScanFuture<'a, R, B>
    where
        R: RangeBounds<B> + Send + Clone,
        B: AsRef<[u8]> + Send + Clone,
    {
        async move { unimplemented!() }
    }

    fn reverse_scan<'a, R: 'a, B: 'a>(
        &'a self,
        key_range: R,
        limit: Option<usize>,
        epoch: u64,
    ) -> Self::ReverseScanFuture<'a, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move { unimplemented!() }
    }
}

pub struct TikvStateStoreIter;

impl TikvStateStoreIter {
    pub async fn new(_store: TikvStateStore, _prefix: Vec<u8>) -> Self {
        unimplemented!()
    }
}

#[async_trait]
impl StateStoreIter for TikvStateStoreIter {
    async fn next(&mut self) -> Result<Option<Self::Item>> {
        unimplemented!()
    }

    type Item = (Bytes, StorageValue);
}
