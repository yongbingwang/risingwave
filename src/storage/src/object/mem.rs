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

use std::collections::HashMap;

use bytes::Bytes;
use futures::future::try_join_all;
use futures::stream::BoxStream;
use futures::StreamExt;
use itertools::Itertools;
use risingwave_common::ensure;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use tokio::sync::Mutex;

use crate::object::{BlockLocation, ObjectMetadata, ObjectStore};

/// In-memory object storage, useful for testing.
#[derive(Default)]
pub struct InMemObjectStore {
    objects: Mutex<HashMap<String, Bytes>>,
}

#[async_trait::async_trait]
impl ObjectStore for InMemObjectStore {
    async fn upload(&self, path: &str, obj: Bytes) -> Result<()> {
        ensure!(!obj.is_empty());
        self.objects.lock().await.insert(path.into(), obj);
        Ok(())
    }

    async fn upload_stream(&self, path: &str, mut stream: BoxStream<'static, Bytes>) -> Result<()> {
        let mut payload = Vec::new();
        while let Some(bytes) = stream.next().await {
            payload.extend(bytes);
        }
        self.upload(path, Bytes::from(payload)).await
    }

    async fn read(&self, path: &str, block: Option<BlockLocation>) -> Result<Bytes> {
        if let Some(loc) = block {
            self.get_object(path, |obj| find_block(obj, loc)).await?
        } else {
            self.get_object(path, |obj| Ok(obj.clone())).await?
        }
    }

    async fn readv(&self, path: &str, block_locs: Vec<BlockLocation>) -> Result<Vec<Bytes>> {
        let futures = block_locs
            .into_iter()
            .map(|block_loc| self.read(path, Some(block_loc)))
            .collect_vec();
        try_join_all(futures).await
    }

    async fn metadata(&self, path: &str) -> Result<ObjectMetadata> {
        let total_size = self.get_object(path, |v| v.len()).await?;
        Ok(ObjectMetadata { total_size })
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.objects.lock().await.remove(path);
        Ok(())
    }
}

impl InMemObjectStore {
    pub fn new() -> Self {
        Self {
            objects: Mutex::new(HashMap::new()),
        }
    }

    async fn get_object<R, F>(&self, path: &str, f: F) -> Result<R>
    where
        F: Fn(&Bytes) -> R,
    {
        self.objects
            .lock()
            .await
            .get(path)
            .ok_or_else(|| RwError::from(InternalError(format!("no object at path '{}'", path))))
            .map(f)
    }
}

fn find_block(obj: &Bytes, block: BlockLocation) -> Result<Bytes> {
    ensure!(block.offset + block.size <= obj.len());
    Ok(obj.slice(block.offset..(block.offset + block.size)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use tokio::sync::mpsc::channel;
    use tokio_stream::wrappers::ReceiverStream;

    use super::*;

    #[tokio::test]
    async fn test_upload() {
        let block = Bytes::from("123456");

        let s3 = InMemObjectStore::new();
        s3.upload("/abc", block).await.unwrap();

        // No such object.
        s3.read("/ab", Some(BlockLocation { offset: 0, size: 3 }))
            .await
            .unwrap_err();

        let bytes = s3
            .read("/abc", Some(BlockLocation { offset: 4, size: 2 }))
            .await
            .unwrap();
        assert_eq!(String::from_utf8(bytes.to_vec()).unwrap(), "56".to_string());

        // Overflow.
        s3.read("/abc", Some(BlockLocation { offset: 4, size: 4 }))
            .await
            .unwrap_err();

        s3.delete("/abc").await.unwrap();

        // No such object.
        s3.read("/abc", Some(BlockLocation { offset: 0, size: 3 }))
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn test_metadata() {
        let block = Bytes::from("123456");

        let obj_store = InMemObjectStore::new();
        obj_store.upload("/abc", block).await.unwrap();

        let metadata = obj_store.metadata("/abc").await.unwrap();
        assert_eq!(metadata.total_size, 6);
    }

    #[tokio::test]
    async fn test_channel_receiver_stream() {
        let (tx, rx) = channel(1024);
        let path = "path";
        let in_mem_object_store = Arc::new(InMemObjectStore::new());
        let join_handle = tokio::spawn({
            let object_store = in_mem_object_store.clone();
            async move {
                object_store
                    .upload_stream(path, ReceiverStream::new(rx).boxed())
                    .await
            }
        });
        let data = Bytes::from("hello stream".as_bytes());
        tx.send(data.clone()).await.unwrap();
        drop(tx);
        join_handle.await.unwrap().unwrap();
        let object_store_data = in_mem_object_store.read(path, None).await.unwrap();
        assert_eq!(data, object_store_data);
    }
}
