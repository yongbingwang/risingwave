use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::sync::mpsc::Sender;
use std::sync::Arc;

use async_trait::async_trait;
use futures::SinkExt;
use itertools::Itertools;
use parking_lot::RwLock as PLRwLock;
use tokio::sync::mpsc::error::TryRecvError;

use super::iterator::variants::{BACKWARD, FORWARD};
use super::iterator::HummockIterator;
use super::key::user_key;
use super::utils::range_overlap;
use super::value::HummockValue;
use super::{HummockError, HummockResult};

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

    pub fn start_user_key(&self) -> &[u8] {
        self.inner.first().unwrap().0.as_slice()
    }

    pub fn end_user_key(&self) -> &[u8] {
        self.inner.last().unwrap().0.as_slice()
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

pub struct MemtableUploader {
    rx: tokio::sync::mpsc::UnboundedReceiver<ImmutableMemtable>,
    memtables_to_send: Vec<ImmutableMemtable>,
}

impl MemtableUploader {
    pub fn new(rx: tokio::sync::mpsc::UnboundedReceiver<ImmutableMemtable>) -> Self {
        Self {
            rx,
            memtables_to_send: Vec::new(),
        }
    }

    pub fn run(&mut self) {
        loop {
            match self.rx.try_recv() {
                Ok(m) => self.memtables_to_send.push(m),
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => break,
            }
        }
    }
}

pub struct MemtableManager {
    /// Immutable memtables grouped by (epoch, end_key)
    /// Memtables from the same epoch are non-overlapping.
    imm_memtables: PLRwLock<BTreeMap<u64, BTreeMap<Vec<u8>, ImmutableMemtable>>>,
    tx: tokio::sync::mpsc::UnboundedSender<ImmutableMemtable>,
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
