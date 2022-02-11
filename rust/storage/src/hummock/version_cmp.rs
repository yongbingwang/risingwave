use std::cmp;

use super::key::{split_key_epoch, user_key};

/// Compare two full keys first by their user keys, then by their versions (epochs).
pub struct VersionedComparator;

impl VersionedComparator {
    /// Suppose parameter as `full_key` = (`user_key`, `u64::MAX - epoch`), this function compare
    /// `&[u8]` as if compare tuple mentioned before.
    #[inline]
    pub fn compare_key(lhs: &[u8], rhs: &[u8]) -> cmp::Ordering {
        VersionedComparator::compare_key_parts(split_key_epoch(lhs), split_key_epoch(rhs))
    }

    #[inline]
    pub fn compare_key_parts(l_parts: (&[u8], &[u8]), r_parts: (&[u8], &[u8])) -> cmp::Ordering {
        l_parts
            .0
            .cmp(r_parts.0)
            .then_with(|| l_parts.1.cmp(r_parts.1))
    }

    #[inline]
    pub fn same_user_key(lhs: &[u8], rhs: &[u8]) -> bool {
        user_key(lhs) == user_key(rhs)
    }
}
