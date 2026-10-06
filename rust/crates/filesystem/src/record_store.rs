//! Shared optimistic-concurrency primitives for versioned control-plane records.

use std::collections::BTreeMap;
use std::sync::Mutex;

/// Bounded compare-and-swap retries before a coordinator reports contention.
pub(crate) const MAXIMUM_CAS_ATTEMPTS: u8 = 32;

/// Record carrying a monotonic optimistic-concurrency revision.
pub(crate) trait Revisioned {
    fn revision(&self) -> u64;
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
pub(crate) struct MemoryRecords<K, V>(pub(crate) Mutex<BTreeMap<K, V>>);

impl<K, V> Default for MemoryRecords<K, V> {
    fn default() -> Self {
        Self(Mutex::default())
    }
}

impl<K: Ord + Clone, V: Clone + Revisioned> MemoryRecords<K, V> {
    fn locked<T>(&self, action: impl FnOnce(&mut BTreeMap<K, V>) -> T) -> Result<T, Poisoned> {
        self.0
            .lock()
            .map(|mut records| action(&mut records))
            .map_err(|_| Poisoned)
    }

    pub(crate) fn load(&self, key: &K) -> Result<Option<V>, Poisoned> {
        self.locked(|records| records.get(key).cloned())
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
                records.insert(key, replacement);
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
