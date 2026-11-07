//! Shared optimistic-concurrency primitives for versioned control-plane records.

use crate::sync::Mutex;
use std::collections::BTreeMap;

/// Record carrying a monotonic optimistic-concurrency revision.
pub(crate) trait Revisioned {
    fn revision(&self) -> u64;
}

impl<V: Revisioned> Revisioned for Box<V> {
    fn revision(&self) -> u64 {
        (**self).revision()
    }
}

macro_rules! revisioned {
    ($($type:ty),+ $(,)?) => {$(
        impl Revisioned for $type {
            fn revision(&self) -> u64 {
                self.revision
            }
        }
    )+};
}

revisioned!(
    crate::GitCompatState,
    crate::LazyWorkspaceState,
    crate::MaterializationJournal,
    crate::MultiRootPublication,
    crate::OperationWindowSnapshot,
    crate::WorkspaceContext,
    crate::WorkspaceLineageRecord,
);

/// Revision stored under `key`, with zero meaning absent.
pub(crate) fn stored_revision<K: Ord, V: Revisioned>(records: &BTreeMap<K, V>, key: &K) -> u64 {
    records.get(key).map_or(0, Revisioned::revision)
}

/// Advances a record to its next revision, returning the expected one.
pub(crate) fn next_revision(revision: &mut u64) -> u64 {
    let expected = *revision;
    *revision = expected.saturating_add(1);
    expected
}

/// Process-local lock was poisoned by a panicking writer.
pub(crate) struct Poisoned;

/// Process-local revision-checked record map behind every memory adapter.
/// Lock order: a leaf lock. No other record lock, callback, or await is held
/// inside `locked`; callers receive owned values after releasing the guard.
// A B-tree node holds several values. Keep record bodies out of insertion and
// split frames even when a revisioned record contains a large pending action.
pub(crate) struct MemoryRecords<K, V>(pub(crate) Mutex<BTreeMap<K, Box<V>>>);

impl<K, V> Default for MemoryRecords<K, V> {
    fn default() -> Self {
        Self(Mutex::default())
    }
}

impl<K: Ord + Clone, V: Clone + Revisioned> MemoryRecords<K, V> {
    fn locked<T>(&self, action: impl FnOnce(&mut BTreeMap<K, Box<V>>) -> T) -> Result<T, Poisoned> {
        self.0
            .lock()
            .map(|mut records| action(&mut records))
            .map_err(|_| Poisoned)
    }

    pub(crate) fn load(&self, key: &K) -> Result<Option<V>, Poisoned> {
        self.locked(|records| records.get(key).map(|record| (**record).clone()))
    }

    pub(crate) fn keys(&self) -> Result<Vec<K>, Poisoned> {
        self.locked(|records| records.keys().cloned().collect())
    }

    /// Replaces `key` only at `expected` (zero means absent).
    pub(crate) fn compare_and_swap(
        &self,
        key: K,
        expected: u64,
        replacement: V,
    ) -> Result<bool, Poisoned> {
        self.locked(|records| {
            let matched = stored_revision(records, &key) == expected;
            if matched {
                match records.entry(key) {
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        **entry.get_mut() = replacement;
                    }
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(Box::new(replacement));
                    }
                }
            }
            matched
        })
    }

    /// Removes `key` only at `expected` (zero means absent).
    pub(crate) fn compare_and_delete(&self, key: &K, expected: u64) -> Result<bool, Poisoned> {
        self.locked(|records| {
            let matched = stored_revision(records, key) == expected;
            if matched {
                records.remove(key);
            }
            matched
        })
    }
}
