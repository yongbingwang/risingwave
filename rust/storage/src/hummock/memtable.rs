use std::ops::Bound;

use async_trait::async_trait;
use crossbeam_skiplist::{map, SkipMap};

use super::iterator::{BoxedHummockIterator, HummockIterator};
use super::value::HummockValue;
use super::HummockResult;

pub trait MemTable {
    fn get(&self, key: &[u8]) -> HummockResult<Option<Vec<u8>>>;
    fn get_iterator<'a>(
        &'a self,
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> HummockResult<BoxedHummockIterator>;
    fn put(
        &self,
        kv_pairs: impl Iterator<Item = (Vec<u8>, HummockValue<Vec<u8>>)>,
    ) -> HummockResult<()>;
}

pub struct SkiplistMemTable {
    skiplist: SkipMap<Vec<u8>, HummockValue<Vec<u8>>>,
}

impl SkiplistMemTable {
    fn new() -> Self {
        Self {
            skiplist: SkipMap::new(),
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

    fn get_iterator(
        &self,
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> HummockResult<BoxedHummockIterator> {
        Ok(Box::new(SkiplistMemTableIterator::new(
            &self.skiplist,
            range,
        )))
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
}

pub struct SkiplistMemTableIterator<'a> {
    skiplist_ref: &'a SkipMap<Vec<u8>, HummockValue<Vec<u8>>>,
    range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    inner:
        map::Range<'a, Vec<u8>, (Bound<Vec<u8>>, Bound<Vec<u8>>), Vec<u8>, HummockValue<Vec<u8>>>,
    current:  Option<map::Entry<'a, Vec<u8>, HummockValue<Vec<u8>>>>
}

impl<'a> SkiplistMemTableIterator<'a> {
    fn new(
        skiplist_ref: &'a SkipMap<Vec<u8>, HummockValue<Vec<u8>>>,
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> Self {
        let inner = skiplist_ref.range(range.clone());
        Self {
            skiplist_ref,
            range,
            inner,
            current: None,
        }
    }
}

#[async_trait]
impl<'a> HummockIterator for SkiplistMemTableIterator<'a> {
    async fn next(&mut self) -> HummockResult<()> {
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

    async fn rewind(&mut self) -> HummockResult<()> {
        self.inner = self.skiplist_ref.range(self.range.clone());
        self.current = None;
        Ok(())
    }

    async fn seek(&mut self, _key: &[u8]) -> HummockResult<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};

    use crate::hummock::memtable::MemTable;
    use super::SkiplistMemTable;
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
