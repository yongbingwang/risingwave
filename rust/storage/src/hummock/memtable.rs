use std::ops::Bound;
use std::sync::Arc;

use crossbeam_skiplist::{map, SkipMap};

use super::iterator::IteratorType;
use super::value::HummockValue;
use super::HummockResult;

pub type BoxedMemtableIterator<'a> = Box<dyn MemtableIterator + 'a>;

pub trait MemTable: Send + Sync {
    fn get(&self, key: &[u8]) -> HummockResult<Option<Vec<u8>>>;
    fn get_iterator(&self, range: (Bound<Vec<u8>>, Bound<Vec<u8>>)) -> BoxedMemtableIterator;
    fn put(
        &self,
        kv_pairs: impl Iterator<Item = (Vec<u8>, HummockValue<Vec<u8>>)>,
    ) -> HummockResult<()>;
    fn is_empty(&self) -> bool;
}


pub trait MemtableIterator: Send + Sync {
    /// Move a valid iterator to the next key.
    ///
    /// Note:
    /// - Before calling this function, make sure the iterator `is_valid`.
    /// - After calling this function, you may first check whether the iterator `is_valid` again,
    ///   then get the new data by calling `key` and `value`.
    /// - If the position after calling this is invalid, this function WON'T return an `Err`. You
    ///   should check `is_valid` before continuing the iteration.
    ///
    /// # Panics
    /// This function will panic if the iterator is invalid.
    fn next(&mut self) -> HummockResult<()>;

    /// Retrieve the current key.
    ///
    /// Note:
    /// - Before calling this function, make sure the iterator `is_valid`.
    /// - This function should be straightforward and return immediately.
    ///
    /// # Panics
    /// This function will panic if the iterator is invalid.
    // TODO: Add lifetime
    fn key(&self) -> &[u8];

    /// Retrieve the current value, decoded as [`HummockValue`].
    ///
    /// Note:
    /// - Before calling this function, make sure the iterator `is_valid`.
    /// - This function should be straightforward and return immediately.
    ///
    /// # Panics
    /// This function will panic if the iterator is invalid, or the value cannot be decoded into
    /// [`HummockValue`].
    // TODO: Add lifetime
    fn value(&self) -> HummockValue<&[u8]>;

    /// Indicate whether the iterator can be used.
    ///
    /// Note:
    /// - ONLY call `key`, `value`, and `next` if `is_valid` returns `true`.
    /// - This function should be straightforward and return immediately.
    fn is_valid(&self) -> bool;
}

pub struct SkiplistMemTable {
    // skiplist: SkipMap<Vec<u8>, HummockValue<Vec<u8>>>,
    skiplist: Arc<SkipMap<Vec<u8>, HummockValue<Vec<u8>>>>,
}

impl SkiplistMemTable {
    pub fn new() -> Self {
        Self {
            skiplist: Arc::new(SkipMap::new()),
        }
    }
}

impl MemTable for SkiplistMemTable {
    fn get(&self, key: &[u8]) -> HummockResult<Option<Vec<u8>>> {
        match self.skiplist.get(key) {
            Some(v) => Ok(v.value().clone().into_put_value()),
            None => Ok(None),
        }
    }

    fn get_iterator(&self, range: (Bound<Vec<u8>>, Bound<Vec<u8>>)) -> BoxedMemtableIterator {
        Box::new(SkiplistMemTableIterator::new(self.skiplist.range(range)))
    }

    fn put(
        &self,
        kv_pairs: impl Iterator<Item = (Vec<u8>, HummockValue<Vec<u8>>)>,
    ) -> HummockResult<()> {
        for (k, v) in kv_pairs {
            self.skiplist.insert(k, v);
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.skiplist.is_empty()
    }
}

pub struct SkiplistMemTableIterator<'a> {
    inner:
        map::Range<'a, Vec<u8>, (Bound<Vec<u8>>, Bound<Vec<u8>>), Vec<u8>, HummockValue<Vec<u8>>>,
    current: Option<map::Entry<'a, Vec<u8>, HummockValue<Vec<u8>>>>,
}

impl<'a> SkiplistMemTableIterator<'a> {
    fn new(
        inner: map::Range<
            'a,
            Vec<u8>,
            (Bound<Vec<u8>>, Bound<Vec<u8>>),
            Vec<u8>,
            HummockValue<Vec<u8>>,
        >,
    ) -> Self {
        let mut res = Self {
            inner,
            current: None,
        };
        res.next();
        res
    }
}

impl<'a> MemtableIterator for SkiplistMemTableIterator<'a> {
    fn next(&mut self) -> HummockResult<()> {
        self.current = self.inner.next();
        Ok(())
    }

    fn key(&self) -> &[u8] {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        match self.current.as_ref().unwrap().value() {
            HummockValue::Put(v) => HummockValue::Put(v.as_slice()),
            HummockValue::Delete => HummockValue::Delete,
        }
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }
}

pub struct MemTableIteratorBuilder<M: MemTable> {
    memtable: Arc<M>,
    epoch: u64,
    range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
}

impl<M: MemTable> MemTableIteratorBuilder<M> {
    pub fn new(memtable: Arc<M>, epoch: u64, range: (Bound<Vec<u8>>, Bound<Vec<u8>>)) -> Self {
        Self {
            memtable,
            epoch,
            range,
        }
    }
    pub fn build(&self) -> IteratorType {
        IteratorType::new_memtable_iterator(
            self.memtable.get_iterator(self.range.clone()),
            self.epoch,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};

    use super::SkiplistMemTable;
    use crate::hummock::memtable::MemTable;
    use crate::hummock::value::HummockValue;

    fn generate_random_bytes(len: usize) -> Vec<u8> {
        thread_rng().sample_iter(&Alphanumeric).take(len).collect()
    }

    #[tokio::test]
    async fn test_memtable() {
        // Generate random kv pairs with duplicate keys
        let memtable = Arc::new(SkiplistMemTable::new());
        let mut kv_pairs: Vec<(Vec<u8>, Vec<HummockValue<Vec<u8>>>)> = vec![];
        let mut rng = thread_rng();
        for _ in 0..1000 {
            let val =
                HummockValue::from(Some(generate_random_bytes(thread_rng().gen_range(1..50))));
            if rng.gen_bool(0.5) && kv_pairs.len() > 0 {
                let idx = rng.gen_range(0..kv_pairs.len());
                kv_pairs[idx].1.push(val);
            } else {
                let key = generate_random_bytes(thread_rng().gen_range(1..10));
                kv_pairs.push((key, vec![val]))
            }
        }

        // Concurrent put
        let mut handles = vec![];
        for (key, vals) in kv_pairs.clone() {
            let memtable = memtable.clone();
            let handle = tokio::spawn(async move {
                let batch: Vec<(Vec<u8>, HummockValue<Vec<u8>>)> =
                    vals.into_iter().map(|v| (key.clone(), v)).collect();
                memtable.put(batch.into_iter()).unwrap();
            });
            handles.push(handle);
        }

        for h in handles {
            h.await.unwrap();
        }

        // Concurrent read
        for (key, vals) in kv_pairs.clone() {
            let memtable = memtable.clone();
            tokio::spawn(async move {
                let latest_value = memtable.get(key.as_slice()).unwrap();
                assert_eq!(latest_value, vals.last().unwrap().clone().into_put_value());
            });
        }
    }
}
