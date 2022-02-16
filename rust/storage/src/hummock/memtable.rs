use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::sync::mpsc::Sender;
use std::sync::Arc;

use async_trait::async_trait;
use futures::SinkExt;
use itertools::Itertools;
use parking_lot::RwLock as PLRwLock;
use risingwave_pb::hummock::{KeyRange, SstableInfo};
use tokio::sync::mpsc::error::TryRecvError;

use super::cloud::gen_remote_sstable;
use super::hummock_meta_client::HummockMetaClient;
use super::iterator::variants::{BACKWARD, FORWARD};
use super::iterator::HummockIterator;
use super::key::{user_key, FullKey};
use super::local_version_manager::LocalVersionManager;
use super::multi_builder::CapacitySplitTableBuilder;
use super::utils::range_overlap;
use super::value::HummockValue;
use super::{HummockError, HummockOptions, HummockResult, HummockStorage};
use crate::monitor::StateStoreStats;
use crate::object::ObjectStore;

type MemtableItem = (Vec<u8>, HummockValue<Vec<u8>>);

#[derive(Clone)]
pub struct ImmutableMemtable {
    inner: Arc<Vec<MemtableItem>>,
}

impl ImmutableMemtable {
    pub fn new(sorted_items: Vec<MemtableItem>) -> Self {
        Self {
            inner: Arc::new(sorted_items),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<HummockValue<&[u8]>> {
        match self
            .inner
            .binary_search_by(|m| user_key(m.0.as_slice()).cmp(key))
        {
            Ok(i) => match &self.inner[i].1 {
                HummockValue::Put(v) => Some(HummockValue::Put(v.as_slice())),
                HummockValue::Delete => Some(HummockValue::Delete),
            },
            Err(_) => None,
        }
    }

    pub fn iter(&self) -> ImmutableMemtableIterator<FORWARD> {
        ImmutableMemtableIterator::<FORWARD>::new(self.inner.clone())
    }

    pub fn reverse_iter(&self) -> ImmutableMemtableIterator<BACKWARD> {
        ImmutableMemtableIterator::<BACKWARD>::new(self.inner.clone())
    }

    pub fn start_key(&self) -> &[u8] {
        self.inner.first().unwrap().0.as_slice()
    }

    pub fn end_key(&self) -> &[u8] {
        self.inner.last().unwrap().0.as_slice()
    }

    pub fn start_user_key(&self) -> &[u8] {
        user_key(self.inner.first().unwrap().0.as_slice())
    }

    pub fn end_user_key(&self) -> &[u8] {
        user_key(self.inner.last().unwrap().0.as_slice())
    }

    pub fn into_inner(self) -> Arc<Vec<MemtableItem>> {
        self.inner
    }
}

pub struct ImmutableMemtableIterator<const DIRECTION: usize> {
    inner: Arc<Vec<MemtableItem>>,
    current_idx: usize,
}

impl<const DIRECTION: usize> ImmutableMemtableIterator<DIRECTION> {
    pub fn new(inner: Arc<Vec<MemtableItem>>) -> Self {
        Self {
            inner,
            current_idx: 0,
        }
    }

    fn current_item(&self) -> &MemtableItem {
        assert!(self.is_valid());
        let idx = match DIRECTION {
            FORWARD => self.current_idx,
            BACKWARD => self.inner.len() - self.current_idx - 1,
            _ => unreachable!(),
        };
        self.inner.get(idx).unwrap()
    }
}

#[async_trait]
impl<const DIRECTION: usize> HummockIterator for ImmutableMemtableIterator<DIRECTION> {
    async fn next(&mut self) -> super::HummockResult<()> {
        assert!(self.is_valid());
        self.current_idx += 1;
        Ok(())
    }

    fn key(&self) -> &[u8] {
        self.current_item().0.as_slice()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        match &self.current_item().1 {
            HummockValue::Put(v) => HummockValue::Put(v.as_slice()),
            HummockValue::Delete => HummockValue::Delete,
        }
    }

    fn is_valid(&self) -> bool {
        self.current_idx < self.inner.len()
    }

    async fn rewind(&mut self) -> super::HummockResult<()> {
        self.current_idx = 0;
        Ok(())
    }

    async fn seek(&mut self, key: &[u8]) -> super::HummockResult<()> {
        match self
            .inner
            .binary_search_by(|probe| probe.0.as_slice().cmp(key))
        {
            Ok(i) => self.current_idx = i,
            Err(i) => self.current_idx = i,
        }
        Ok(())
    }
}

enum MemtableUploaderItem {
    MEMTABLE(ImmutableMemtable),
    SYNC(Option<tokio::sync::oneshot::Sender<()>>),
}

pub struct MemtableUploader {
    memtables_to_upload: Vec<ImmutableMemtable>,

    local_version_manager: Arc<LocalVersionManager>,
    options: Arc<HummockOptions>,
    obj_client: Arc<dyn ObjectStore>,
    /// Notify the compactor to compact after every write_batch().
    compactor_tx: tokio::sync::mpsc::UnboundedSender<()>,

    /// Statistics.
    stats: Arc<StateStoreStats>,
    hummock_meta_client: Arc<dyn HummockMetaClient>,

    rx: tokio::sync::mpsc::UnboundedReceiver<MemtableUploaderItem>,
}

impl MemtableUploader {
    pub fn new(
        options: Arc<HummockOptions>,
        local_version_manager: Arc<LocalVersionManager>,
        obj_client: Arc<dyn ObjectStore>,
    compactor_tx: tokio::sync::mpsc::UnboundedSender<()>,
        stats: Arc<StateStoreStats>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        rx: tokio::sync::mpsc::UnboundedReceiver<MemtableUploaderItem>,
    ) -> Self {
        Self {
            memtables_to_upload: Vec::new(),
            options,
            local_version_manager,
            obj_client,
            compactor_tx,
            stats,
            hummock_meta_client,
            rx,
        }
    }

    async fn sync(&mut self) -> HummockResult<()> {
        // Sort the memtables. Assume all memtables are non-overlapping.
        self.memtables_to_upload
            .sort_by(|l, r| l.start_key().cmp(r.start_key()));

        let get_id_and_builder = || async {
            let id = self.hummock_meta_client.get_new_table_id().await?;
            let timer = self.stats.batch_write_build_table_latency.start_timer();
            let builder = HummockStorage::get_builder(&self.options);
            timer.observe_duration();
            Ok((id, builder))
        };
        let mut builder = CapacitySplitTableBuilder::new(get_id_and_builder);

        for m in self.memtables_to_upload {
            for (k, v) in m.into_inner().iter() {
                builder
                    .add_full_key(FullKey::from_slice(k.as_slice()), v.clone(), true)
                    .await?;
            }
        }

        let tables = {
            let mut tables = Vec::with_capacity(builder.len());

            // TODO: decide upload concurrency
            for (table_id, blocks, meta) in builder.finish() {
                let table = gen_remote_sstable(
                    self.obj_client.clone(),
                    table_id,
                    blocks,
                    meta,
                    self.options.remote_dir.as_str(),
                    Some(self.local_version_manager.block_cache.clone()),
                )
                .await?;
                tables.push(table);
            }

            tables
        };

        if tables.is_empty() {
            return Ok(());
        }

        // Add all tables at once.
        let timer = self.stats.batch_write_add_l0_latency.start_timer();
        self.hummock_meta_client
            .add_tables(
                epoch,
                tables
                    .iter()
                    .map(|table| SstableInfo {
                        id: table.id,
                        key_range: Some(KeyRange {
                            left: table.meta.smallest_key.clone(),
                            right: table.meta.largest_key.clone(),
                            inf: false,
                        }),
                    })
                    .collect_vec(),
            )
            .await?;
        timer.observe_duration();

        // Notify the compactor
        self.compactor_tx.send(()).ok();

        Ok(())
    }

    async fn handle(&mut self, item: MemtableUploaderItem) {
        match item {
            MemtableUploaderItem::MEMTABLE(m) => {
                self.memtables_to_upload.push(m);
            }
            MemtableUploaderItem::SYNC(tx_opt) => {
                self.sync().await;
                if let Some(tx) = tx_opt {
                    tx.send(());
                }
            }
        }
    }

    pub async fn run(&mut self) {
        loop {
            match self.rx.try_recv() {
                Ok(m) => {
                    self.handle(m);
                }
                Err(TryRecvError::Empty) => {
                    // Wait for the next item
                    // Is there a better way to do this?
                    match self.rx.recv().await {
                        Some(m) => {
                            self.handle(m);
                        }
                        None => break,
                    }
                }
                Err(TryRecvError::Disconnected) => break,
            }
        }
    }
}

pub struct MemtableManager {
    /// Immutable memtables grouped by (epoch, end_key)
    /// Memtables from the same epoch are non-overlapping.
    imm_memtables: PLRwLock<BTreeMap<u64, BTreeMap<Vec<u8>, ImmutableMemtable>>>,
    tx: tokio::sync::mpsc::UnboundedSender<MemtableUploaderItem>,
    uploader: MemtableUploader,
}

impl MemtableManager {
    pub fn new() -> Self {
        // TODO: make channel capacity configurable
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            imm_memtables: PLRwLock::new(BTreeMap::new()),
            tx,
            uploader: MemtableUploader::new(rx),
        }
    }

    pub fn write_batch(&self, batch: Vec<MemtableItem>, epoch: u64) -> HummockResult<()> {
        let immu_memtable = ImmutableMemtable::new(batch);
        self.imm_memtables
            .write()
            .entry(epoch)
            .or_insert(BTreeMap::new())
            .insert(immu_memtable.end_user_key().to_vec(), immu_memtable.clone());
        self.tx
            .send(immu_memtable)
            .map_err(HummockError::memtable_error)
    }

    pub fn iters<R, B>(&self, key_range: &R, epoch: u64) -> Vec<ImmutableMemtableIterator<FORWARD>>
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        self.imm_memtables
            .read()
            .range(..=epoch)
            .flat_map(|entry| {
                entry
                    .1
                    .range((
                        key_range.start_bound().map(|b| b.as_ref().to_vec()),
                        std::ops::Bound::Unbounded,
                    ))
                    .filter(|m| {
                        range_overlap(key_range, m.1.start_user_key(), m.1.end_user_key(), false)
                    })
                    .map(|m| m.1.iter())
            })
            .collect_vec()
    }

    pub fn reverse_iters<R, B>(
        &self,
        key_range: &R,
        epoch: u64,
    ) -> Vec<ImmutableMemtableIterator<BACKWARD>>
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        self.imm_memtables
            .read()
            .range(..=epoch)
            .flat_map(|entry| {
                entry
                    .1
                    .range((
                        key_range.end_bound().map(|b| b.as_ref().to_vec()),
                        std::ops::Bound::Unbounded,
                    ))
                    .filter(|m| {
                        range_overlap(key_range, m.1.start_user_key(), m.1.end_user_key(), true)
                    })
                    .map(|m| m.1.reverse_iter())
            })
            .collect_vec()
    }

    /// Memtables of a given epoch is deleted after the epoch is committed.
    pub fn delete(&self, epoch: u64) {
        self.imm_memtables.write().remove(&epoch);
    }
}

// #[cfg(test)]
// mod tests {
//     use std::sync::Arc;

//     use rand::distributions::Alphanumeric;
//     use rand::{thread_rng, Rng};

//     use super::SkiplistMemTable;
//     use crate::hummock::memtable::MemTable;
//     use crate::hummock::value::HummockValue;

//     fn generate_random_bytes(len: usize) -> Vec<u8> {
//         thread_rng().sample_iter(&Alphanumeric).take(len).collect()
//     }

//     #[tokio::test]
//     async fn test_memtable() {
//         // Generate random kv pairs with duplicate keys
//         let memtable = Arc::new(SkiplistMemTable::new());
//         let mut kv_pairs: Vec<(Vec<u8>, Vec<HummockValue<Vec<u8>>>)> = vec![];
//         let mut rng = thread_rng();
//         for _ in 0..1000 {
//             let val =
//                 HummockValue::from(Some(generate_random_bytes(thread_rng().gen_range(1..50))));
//             if rng.gen_bool(0.5) && kv_pairs.len() > 0 {
//                 let idx = rng.gen_range(0..kv_pairs.len());
//                 kv_pairs[idx].1.push(val);
//             } else {
//                 let key = generate_random_bytes(thread_rng().gen_range(1..10));
//                 kv_pairs.push((key, vec![val]))
//             }
//         }

//         // Concurrent put
//         let mut handles = vec![];
//         for (key, vals) in kv_pairs.clone() {
//             let memtable = memtable.clone();
//             let handle = tokio::spawn(async move {
//                 let batch: Vec<(Vec<u8>, HummockValue<Vec<u8>>)> =
//                     vals.into_iter().map(|v| (key.clone(), v)).collect();
//                 memtable.put(batch.into_iter()).unwrap();
//             });
//             handles.push(handle);
//         }

//         for h in handles {
//             h.await.unwrap();
//         }

//         // Concurrent read
//         for (key, vals) in kv_pairs.clone() {
//             let memtable = memtable.clone();
//             tokio::spawn(async move {
//                 let latest_value = memtable.get(key.as_slice()).unwrap();
//                 assert_eq!(latest_value, vals.last().unwrap().clone().into_put_value());
//             });
//         }
//     }
// }
