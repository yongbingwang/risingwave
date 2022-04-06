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

use std::marker::PhantomData;
use std::mem::size_of;

use bytes::{BufMut, Bytes, BytesMut};
use risingwave_common::config::StorageConfig;
use risingwave_common::error::{Result, ToRwResult};
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

use super::bloom::Bloom;
use super::utils::CompressionAlgorithm;
use super::{
    BlockBuilder, BlockBuilderOptions, BlockMeta, SstableMeta, DEFAULT_BLOCK_SIZE,
    DEFAULT_ENTRY_SIZE, DEFAULT_RESTART_INTERVAL, VERSION,
};
use crate::hummock::key::user_key;
use crate::hummock::value::HummockValue;

pub const DEFAULT_SSTABLE_SIZE: usize = 4 * 1024 * 1024;
pub const DEFAULT_BLOOM_FALSE_POSITIVE: f64 = 0.1;

#[derive(Clone, Debug)]
pub struct SSTableBuilderOptions {
    /// Approximate sstable capacity.
    pub capacity: usize,
    /// Approximate block capacity.
    pub block_capacity: usize,
    /// Restart point interval.
    pub restart_interval: usize,
    /// False prsitive probability of bloom filter.
    pub bloom_false_positive: f64,
    /// Compression algorithm.
    pub compression_algorithm: CompressionAlgorithm,
}

impl SSTableBuilderOptions {
    pub fn from_storage_config(options: &StorageConfig) -> SSTableBuilderOptions {
        SSTableBuilderOptions {
            capacity: options.sstable_size as usize,
            block_capacity: options.block_size as usize,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: options.bloom_false_positive,
            // TODO: Make this configurable.
            compression_algorithm: CompressionAlgorithm::None,
        }
    }
}

impl Default for SSTableBuilderOptions {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_SSTABLE_SIZE,
            block_capacity: DEFAULT_BLOCK_SIZE,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: DEFAULT_BLOOM_FALSE_POSITIVE,
            compression_algorithm: CompressionAlgorithm::None,
        }
    }
}

#[async_trait::async_trait]
pub trait SstableWriter<O> {
    fn flushed_len(&self) -> usize;
    async fn flush(&mut self, data: Bytes) -> Result<()>;
    async fn finish(self) -> Result<O>;
}

pub struct InMemSstableWriter {
    pub buf: BytesMut,
}

impl InMemSstableWriter {
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(capacity),
        }
    }
}

#[async_trait::async_trait]
impl SstableWriter<Bytes> for InMemSstableWriter {
    fn flushed_len(&self) -> usize {
        self.buf.len()
    }

    async fn flush(&mut self, data: Bytes) -> Result<()> {
        self.buf.put_slice(&data);
        Ok(())
    }

    async fn finish(self) -> Result<Bytes> {
        Ok(self.buf.freeze())
    }
}

pub struct ConcurrentUploadSstableWriter {
    tx: Sender<Bytes>,
    upload_join_handle: JoinHandle<Result<()>>,
    flushed_len: usize,
}

impl ConcurrentUploadSstableWriter {
    pub fn new(tx: Sender<Bytes>, upload_join_handle: JoinHandle<Result<()>>) -> Self {
        Self {
            tx,
            upload_join_handle,
            flushed_len: 0,
        }
    }
}

#[async_trait::async_trait]
impl SstableWriter<JoinHandle<Result<()>>> for ConcurrentUploadSstableWriter {
    fn flushed_len(&self) -> usize {
        self.flushed_len
    }

    async fn flush(&mut self, data: Bytes) -> Result<()> {
        self.flushed_len += data.len();
        self.tx.send(data).await.to_rw_result()?;
        Ok(())
    }

    async fn finish(self) -> Result<JoinHandle<Result<()>>> {
        Ok(self.upload_join_handle)
    }
}

pub struct SSTableBuildOutput<WO> {
    pub writer_output: WO,
    pub meta: SstableMeta,
    /// Store the bytes of each block. Store for adding to block cache later
    pub block_bytes: Option<Vec<Bytes>>,
}

pub struct SSTableBuilder<W, WO>
where
    W: SstableWriter<WO>,
{
    /// Options.
    options: SSTableBuilderOptions,
    /// Writer
    writer: W,
    /// Block collector
    block_collector: Option<Vec<Bytes>>,
    /// Current block builder.
    block_builder: Option<BlockBuilder>,
    /// Block metadata vec.
    block_metas: Vec<BlockMeta>,
    /// Hashes of user keys.
    user_key_hashes: Vec<u32>,
    /// Last added full key.
    last_full_key: Bytes,
    key_count: usize,
    _phantom: PhantomData<WO>,
}

impl<W: SstableWriter<WO>, WO> SSTableBuilder<W, WO> {
    pub fn new(options: SSTableBuilderOptions, writer: W, collect_all_blocks: bool) -> Self {
        Self {
            options: options.clone(),
            writer,
            block_collector: if collect_all_blocks {
                Some(Vec::new())
            } else {
                None
            },
            block_builder: None,
            block_metas: Vec::with_capacity(options.capacity / options.block_capacity + 1),
            user_key_hashes: Vec::with_capacity(options.capacity / DEFAULT_ENTRY_SIZE + 1),
            last_full_key: Bytes::default(),
            key_count: 0,
            _phantom: PhantomData::default(),
        }
    }

    /// Add kv pair to sstable.
    pub async fn add(&mut self, full_key: &[u8], value: HummockValue<&[u8]>) -> Result<()> {
        // Rotate block builder if the previous one has been built.
        if self.block_builder.is_none() {
            self.last_full_key.clear();
            self.block_builder = Some(BlockBuilder::new(BlockBuilderOptions {
                capacity: self.options.capacity,
                restart_interval: self.options.restart_interval,
                compression_algorithm: self.options.compression_algorithm,
            }));
            self.block_metas.push(BlockMeta {
                offset: self.writer.flushed_len() as u32,
                len: 0,
                smallest_key: vec![],
            })
        }

        let block_builder = self.block_builder.as_mut().unwrap();

        // TODO: refine me
        let mut raw_value = BytesMut::default();
        value.encode(&mut raw_value);
        let raw_value = raw_value.freeze();

        block_builder.add(full_key, &raw_value);

        let user_key = user_key(full_key);
        self.user_key_hashes.push(farmhash::fingerprint32(user_key));

        if self.last_full_key.is_empty() {
            self.block_metas.last_mut().unwrap().smallest_key = full_key.to_vec();
        }
        self.last_full_key = Bytes::copy_from_slice(full_key);

        if block_builder.approximate_len() >= self.options.block_capacity {
            self.build_block().await?;
        }
        self.key_count += 1;
        Ok(())
    }

    /// Finish building sst.
    ///
    /// Unlike most LSM-Tree implementations, sstable meta and data are encoded separately.
    /// Both meta and data has its own object (file).
    ///
    /// # Format
    ///
    /// data:
    ///
    /// ```plain
    /// | Block 0 | ... | Block N-1 | N (4B) |
    /// ```
    pub async fn finish(mut self) -> Result<SSTableBuildOutput<WO>> {
        let smallest_key = self.block_metas[0].smallest_key.clone();
        let largest_key = self.last_full_key.to_vec();
        self.build_block().await?;
        let mut size_footer = BytesMut::with_capacity(size_of::<u32>());
        size_footer.put_u32_le(self.block_metas.len() as u32);
        self.writer.flush(size_footer.freeze()).await?;

        let meta = SstableMeta {
            block_metas: self.block_metas,
            bloom_filter: if self.options.bloom_false_positive > 0.0 {
                let bits_per_key = Bloom::bloom_bits_per_key(
                    self.user_key_hashes.len(),
                    self.options.bloom_false_positive,
                );
                Bloom::build_from_key_hashes(&self.user_key_hashes, bits_per_key).to_vec()
            } else {
                vec![]
            },
            estimated_size: self.writer.flushed_len() as u32,
            key_count: self.key_count as u32,
            smallest_key,
            largest_key,
            version: VERSION,
        };

        Ok(SSTableBuildOutput {
            writer_output: self.writer.finish().await?,
            meta,
            block_bytes: self.block_collector,
        })
    }

    pub fn approximate_len(&self) -> usize {
        self.writer.flushed_len()
            + self
                .block_builder
                .as_ref()
                .map(|b| b.approximate_len())
                .unwrap_or(0)
            + 4
    }

    async fn build_block(&mut self) -> Result<()> {
        // Skip empty block.
        if self.block_builder.is_none() {
            return Ok(());
        }

        let block = self.block_builder.take().unwrap().build();
        if let Some(collector) = &mut self.block_collector {
            collector.push(block.clone());
        }
        self.block_metas.last_mut().unwrap().len = block.len() as u32;
        self.writer.flush(block).await?;
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.user_key_hashes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.user_key_hashes.is_empty()
    }

    /// Returns true if we roughly reached capacity
    pub fn reach_capacity(&self) -> bool {
        self.approximate_len() >= self.options.capacity
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::sync::Arc;

    use futures::executor::block_on;
    use futures::StreamExt;
    use risingwave_common::error::{ErrorCode, RwError};
    use tokio::sync::mpsc::channel;
    use tokio::sync::oneshot;
    use tokio_stream::wrappers::ReceiverStream;

    use super::*;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::test_utils::{
        default_builder_opt_for_test, gen_default_test_sstable, test_key_of, test_value_of,
        TEST_KEYS_COUNT,
    };
    use crate::object::{InMemObjectStore, ObjectStore};

    #[test]
    #[should_panic]
    fn test_empty() {
        let opt = SSTableBuilderOptions {
            capacity: 0,
            block_capacity: 4096,
            restart_interval: 16,
            bloom_false_positive: 0.1,
            compression_algorithm: CompressionAlgorithm::None,
        };

        let buf = InMemSstableWriter::new(opt.capacity);

        let b = SSTableBuilder::new(opt, buf, false);
        block_on(b.finish()).unwrap();
    }

    #[test]
    fn test_smallest_key_and_largest_key() {
        let opt = default_builder_opt_for_test();
        let buf = InMemSstableWriter::new(opt.capacity);
        let mut b = SSTableBuilder::new(opt, buf, false);

        for i in 0..TEST_KEYS_COUNT {
            block_on(b.add(&test_key_of(i), HummockValue::put(&test_value_of(i)))).unwrap();
        }

        let output = block_on(b.finish()).unwrap();

        assert_eq!(test_key_of(0), output.meta.smallest_key);
        assert_eq!(test_key_of(TEST_KEYS_COUNT - 1), output.meta.largest_key);
    }

    async fn test_with_bloom_filter(with_blooms: bool) {
        let key_count = 1000;

        let opts = SSTableBuilderOptions {
            capacity: 0,
            block_capacity: 4096,
            restart_interval: 16,
            bloom_false_positive: if with_blooms { 0.01 } else { 0.0 },
            compression_algorithm: CompressionAlgorithm::None,
        };

        // build remote table
        let sstable_store = mock_sstable_store();
        let table = gen_default_test_sstable(opts, 0, sstable_store).await;

        assert_eq!(table.has_bloom_filter(), with_blooms);
        for i in 0..key_count {
            let full_key = test_key_of(i);
            assert!(!table.surely_not_have_user_key(user_key(full_key.as_slice())));
        }
    }

    #[tokio::test]
    async fn test_bloom_filter() {
        test_with_bloom_filter(false).await;
        test_with_bloom_filter(true).await;
    }

    #[tokio::test]
    async fn test_stream_sstable_builder() {
        let opt = SSTableBuilderOptions::default();
        let (stream_block_bytes, stream_meta, stream_object_store_data) = {
            let in_mem_object_store = Arc::new(InMemObjectStore::new());
            let (tx, rx) = channel(1024);
            let path = "stream_upload_path";
            let join_handle = tokio::spawn({
                let object_store = in_mem_object_store.clone();
                async move {
                    object_store
                        .upload_stream(path, ReceiverStream::new(rx).boxed())
                        .await
                }
            });
            let writer = ConcurrentUploadSstableWriter::new(tx, join_handle);
            let mut builder = SSTableBuilder::new(opt.clone(), writer, true);
            for i in 0..TEST_KEYS_COUNT {
                builder
                    .add(&test_key_of(i), HummockValue::put(&test_value_of(i)))
                    .await
                    .unwrap();
            }

            let SSTableBuildOutput {
                writer_output: join_handle,
                meta,
                block_bytes,
            } = builder.finish().await.unwrap();

            join_handle.await.unwrap().unwrap();

            (
                block_bytes.unwrap(),
                meta,
                in_mem_object_store.read(path, None).await.unwrap(),
            )
        };

        // build a monolithic upload data and compare with stream upload
        let (mono_block_bytes, mono_meta, mono_write_data) = {
            let writer = InMemSstableWriter::new(opt.capacity);
            let mut builder = SSTableBuilder::new(opt, writer, true);
            for i in 0..TEST_KEYS_COUNT {
                builder
                    .add(&test_key_of(i), HummockValue::put(&test_value_of(i)))
                    .await
                    .unwrap();
            }

            let SSTableBuildOutput {
                writer_output: write_data,
                meta,
                block_bytes,
            } = builder.finish().await.unwrap();

            (block_bytes.unwrap(), meta, write_data)
        };
        assert_eq!(mono_block_bytes, stream_block_bytes);
        assert_eq!(mono_meta, stream_meta);
        assert_eq!(mono_write_data, stream_object_store_data);
    }

    #[tokio::test]
    async fn test_bad_upload_stream() {
        let opt = SSTableBuilderOptions::default();
        let (tx, rx) = channel(1024);

        // channel that notifies the stream has been dropped
        let (drop_tx, drop_rx) = oneshot::channel();
        let join_handle = tokio::spawn(async move {
            let stream = ReceiverStream::new(rx);
            // drop the stream and expect `tx.send` will fail
            drop(stream);
            drop_tx.send(()).unwrap();
            Err(RwError::from(ErrorCode::InternalError(
                "bad stream".to_string(),
            )))
        });
        drop_rx.await.unwrap();
        let writer = ConcurrentUploadSstableWriter::new(tx, join_handle);
        let mut builder = SSTableBuilder::new(opt.clone(), writer, true);
        let mut has_err = false;
        let mut err = None;
        for i in 0..TEST_KEYS_COUNT {
            let result = builder
                .add(&test_key_of(i), HummockValue::put(&test_value_of(i)))
                .await;
            if result.is_err() {
                has_err = true;
                err = Some(result.err().unwrap());
                break;
            }
        }

        if !has_err {
            let result = builder.finish().await;
            if result.is_err() {
                has_err = true;
                err = Some(result.err().unwrap());
            }
        }

        assert!(has_err);
        assert!(err.is_some());
        println!("err is: {:?}", err.unwrap().inner());
    }
}
