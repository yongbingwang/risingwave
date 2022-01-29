use async_trait::async_trait;
use bytes::Bytes;
use risingwave_common::error::Result;

use super::StateStore;
use crate::StateStoreIter;

#[derive(Clone)]
pub struct TikvStateStore {}

impl TikvStateStore {
    pub fn new(_pd_endpoints: Vec<String>) -> Self {
        unimplemented!()
    }
}
#[async_trait]
impl StateStore for TikvStateStore {
    type Iter<'a> = TikvStateStoreIter;

    async fn get(&self, _key: &[u8], _epoch: u64) -> Result<Option<Bytes>> {
        unimplemented!()
    }

    async fn scan(
        &self,
        _prefix: &[u8],
        _limit: Option<usize>,
        _epoch: u64,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        unimplemented!()
    }

    async fn ingest_batch(
        &self,
        _kv_pairs: Vec<(Bytes, Option<Bytes>)>,
        _epoch: u64,
    ) -> Result<()> {
        unimplemented!()
    }

    async fn iter(&'_ self, _prefix: &[u8], _epoch: u64) -> Result<Self::Iter<'_>> {
        unimplemented!()
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
    async fn next(&'_ mut self) -> Result<Option<Self::Item>> {
        unimplemented!()
    }

    type Item = (Bytes, Bytes);
}
