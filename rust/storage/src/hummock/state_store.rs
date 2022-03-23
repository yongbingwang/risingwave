use std::future::Future;
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
use std::ops::RangeBounds;

use async_trait::async_trait;
use bytes::Bytes;
use risingwave_common::error::Result;

use super::HummockStorage;
use crate::hummock::iterator::DirectedUserIterator;
use crate::storage_value::StorageValue;
use crate::store::{collect_from_iter, *};
use crate::*;

/// A wrapper over [`HummockStorage`] as a state store.
#[derive(Clone)]
pub struct HummockStateStore {
    pub storage: HummockStorage,
}

impl HummockStateStore {
    pub fn new(storage: HummockStorage) -> Self {
        Self { storage }
    }

    pub fn storage(&self) -> &HummockStorage {
        &self.storage
    }
}

impl StateStore for HummockStateStore {
    type Iter<'a> = HummockStateStoreIter<'a>;
    define_state_store_associated_type!();

    fn get<'a>(&'a self, key: &'a [u8], epoch: u64) -> Self::GetFuture<'a> {
        async move {
            let value = self.storage.get(key, epoch).await?;
            let value = value.map(Bytes::from);
            let storage_value = value.map(StorageValue::from);
            Ok(storage_value)
        }
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
        async move { collect_from_iter(self.iter(key_range, epoch).await?, limit).await }
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
        async move { collect_from_iter(self.reverse_iter(key_range, epoch).await?, limit).await }
    }

    fn ingest_batch(
        &self,
        kv_pairs: Vec<(Bytes, Option<StorageValue>)>,
        epoch: u64,
    ) -> Self::IngestBatchFuture<'_> {
        async move {
            self.storage
                .write_batch(kv_pairs.into_iter().map(|(k, v)| (k, v.into())), epoch)
                .await?;
            Ok(())
        }
    }

    fn replicate_batch(
        &self,
        kv_pairs: Vec<(Bytes, Option<StorageValue>)>,
        epoch: u64,
    ) -> Self::ReplicateBatchFuture<'_> {
        async move {
            self.storage
                .replicate_batch(kv_pairs.into_iter().map(|(k, v)| (k, v.into())), epoch)
                .await?;
            Ok(())
        }
    }

    fn iter_inner<'a, R: 'a>(&'a self, key_range: &'a R, epoch: u64) -> Self::IterInnerFuture<'_, R>
    where
        R: RangeBounds<&'a [u8]> + Send + Sync + Clone,
    {
        async move {
            let inner = self.storage.range_scan(key_range.clone(), epoch).await?;
            let mut res = DirectedUserIterator::Forward(inner);
            res.rewind().await?;
            Ok(HummockStateStoreIter::new(res))
        }
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

    fn reverse_iter<'a, R: 'a, B: 'a>(
        &'a self,
        key_range: R,
        epoch: u64,
    ) -> Self::ReverseIterFuture<'a, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            let mut res = self.storage.reverse_range_scan(key_range, epoch).await?;
            res.rewind().await?;
            Ok(HummockStateStoreIter::new(DirectedUserIterator::Backward(
                res,
            )))
        }
    }

    fn wait_epoch(&self, epoch: u64) -> Self::WaitEpochFuture<'_> {
        async move { self.storage.wait_epoch(epoch).await }
    }

    fn sync(&self, epoch: Option<u64>) -> Self::SyncFuture<'_> {
        async move {
            self.storage.sync(epoch).await?;
            Ok(())
        }
    }
}

pub struct HummockStateStoreIter<'a> {
    inner: DirectedUserIterator<'a>,
}

impl<'a> HummockStateStoreIter<'a> {
    fn new(inner: DirectedUserIterator<'a>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<'a> StateStoreIter for HummockStateStoreIter<'a> {
    // TODO: directly return `&[u8]` to user instead of `Bytes`.
    type Item = (Bytes, StorageValue);

    async fn next(&mut self) -> Result<Option<Self::Item>> {
        let iter = &mut self.inner;

        if iter.is_valid() {
            let kv = (
                Bytes::copy_from_slice(iter.key()),
                StorageValue::from(Bytes::copy_from_slice(iter.value())),
            );
            iter.next().await?;
            Ok(Some(kv))
        } else {
            Ok(None)
        }
    }
}
