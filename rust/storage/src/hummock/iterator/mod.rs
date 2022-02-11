use super::key::split_key_epoch;
use super::memtable::BoxedMemtableIterator;
use super::{HummockResult, HummockValue};

mod concat;
pub use concat::*;
mod concat_inner;
mod reverse_concat;
pub use reverse_concat::*;
mod reverse_merge;
pub use reverse_merge::*;
mod reverse_user;
pub use reverse_user::*;
mod merge;
pub use merge::*;
mod merge_inner;
mod user;
pub use user::*;

#[cfg(test)]
pub(crate) mod test_utils;

use async_trait::async_trait;

/// `HummockIterator` defines the interface of all iterators, including `SSTableIterator`,
/// `MergeIterator`, `UserIterator` and `ConcatIterator`.
///
/// After create the iterator instance,
/// - if you want to iterate from the beginning, you need to then call its `rewind` method.
/// - if you want to iterate from some specific position, you need to then call its `seek` method.
///
/// Before calling
#[async_trait]
pub trait HummockIterator<'a>: Send + Sync {
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
    async fn next(&mut self) -> HummockResult<()>;

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

    /// Reset the position of the iterator.
    ///
    /// Note:
    /// - Do not decide whether the position is valid or not by checking the returned error of this
    ///   function. This function WON'T return an `Err` if invalid. You should check `is_valid`
    ///   before starting iteration.
    async fn rewind(&'a mut self) -> HummockResult<()>;

    /// Reset iterator and seek to the first position where the key >= provided key, or key <=
    /// provided key if this is a reverse iterator.
    ///
    /// Note:
    /// - Do not decide whether the position is valid or not by checking the returned error of this
    ///   function. This function WON'T return an `Err` if invalid. You should check `is_valid`
    ///   before starting iteration.
    async fn seek(&mut self, key: &[u8]) -> HummockResult<()>;
}

pub type BoxedHummockIterator<'a> = Box<dyn HummockIterator<'a>>;

pub enum IteratorType<'a> {
    SSTableIterator(BoxedHummockIterator<'a>),
    MemtableIterator {
        iter: BoxedMemtableIterator<'a>,
        epoch_part: [u8; 8],
    },
}

impl<'a> IteratorType<'a> {
    pub fn new_sstable_iterator(inner: BoxedHummockIterator<'a>) -> Self {
        Self::SSTableIterator(inner)
    }

    pub fn new_memtable_iterator(iter: BoxedMemtableIterator<'a>, epoch: u64) -> Self {
        let epoch_part = (u64::MAX - epoch).to_be_bytes();
        Self::MemtableIterator { iter, epoch_part }
    }

    pub fn is_sstable_iterator(&self) -> bool {
        match self {
            IteratorType::SSTableIterator(_) => true,
            IteratorType::MemtableIterator { .. } => false,
        }
    }

    pub fn to_hummock_iterator(&'a self) -> BoxedHummockIterator<'a> {
        match self {
            IteratorType::SSTableIterator(i) => *i,
            IteratorType::MemtableIterator { .. } => panic!(),
        }
    }

    /// Return key parts (`user_key`, `u64::MAX - epoch`) for the full key
    pub fn key_parts(&self) -> (&[u8], &[u8]) {
        match self {
            IteratorType::SSTableIterator(inner) => split_key_epoch(inner.key()),
            IteratorType::MemtableIterator { iter, epoch_part } => (iter.key(), epoch_part),
        }
    }

    pub async fn next(&mut self) -> HummockResult<()> {
        match self {
            IteratorType::SSTableIterator(inner) => inner.next().await,
            IteratorType::MemtableIterator { iter, .. } => iter.next(),
        }
    }

    pub fn key(&self) -> &[u8] {
        match self {
            IteratorType::SSTableIterator(inner) => inner.key(),
            IteratorType::MemtableIterator { iter, .. } => iter.key(),
        }
    }

    fn value(&self) -> HummockValue<&[u8]> {
        match self {
            IteratorType::SSTableIterator(inner) => inner.value(),
            IteratorType::MemtableIterator { iter, .. } => iter.value(),
        }
    }

    fn is_valid(&self) -> bool {
        match self {
            IteratorType::SSTableIterator(inner) => inner.is_valid(),
            IteratorType::MemtableIterator { iter, .. } => iter.is_valid(),
        }
    }
}


pub mod variants {
    pub const FORWARD: usize = 0;
    pub const BACKWARD: usize = 1;
}
