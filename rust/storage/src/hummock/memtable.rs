use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::ForEach;
use itertools::Itertools;
use parking_lot::Mutex as PLMutex;

use super::iterator::variants::{BACKWARD, FORWARD};
use super::iterator::HummockIterator;
use super::utils::range_overlap;
use super::value::HummockValue;

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

pub struct MemtableManager {
    /// Immutable memtables grouped by epoch.
    /// Memtables from the same epoch are non-overlapping.
    imm_memtables: Arc<PLMutex<BTreeMap<u64, Vec<ImmutableMemtable>>>>,

    syncing_memtables: Arc<PLMutex<BTreeMap<u64, Vec<ImmutableMemtable>>>>,
}

impl MemtableManager {
    pub fn new() -> Self {
        Self {
            imm_memtables: Arc::new(PLMutex::new(BTreeMap::new())),
            syncing_memtables: Arc::new(PLMutex::new(BTreeMap::new())),
        }
    }

    pub fn write_batch(&self, batch: Vec<MemtableItem>, epoch: u64) {
        let immu_memtable = ImmutableMemtable::new(batch);
        self.imm_memtables
            .lock()
            .entry(epoch)
            .or_insert(vec![])
            .push(immu_memtable);
    }

    pub fn sync(&self, epo)

    pub fn iters<R, B>(&self, key_range: &R, epoch: u64) -> Vec<ImmutableMemtableIterator<FORWARD>>
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        self.imm_memtables
            .lock()
            .range(..=epoch)
            .chain(self.syncing_memtables.lock().range(..=epoch))
            .filter_map(|entry| {
                entry
                    .1
                    .iter()
                    .find(|m| range_overlap(key_range, m.start_user_key(), m.end_user_key(), false))
                    .map(|m| m.iter())
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
            .lock()
            .range(0..=epoch)
            .chain(self.syncing_memtables.lock().range(..=epoch))
            .filter_map(|entry| {
                entry
                    .1
                    .iter()
                    .find(|m| range_overlap(key_range, m.start_user_key(), m.end_user_key(), true))
                    .map(|m| m.reverse_iter())
            })
            .collect_vec()
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
