//! Shared optimistic-concurrency primitives for versioned control-plane records.

use crate::sync::Mutex;
use std::collections::{BTreeMap, btree_map::Entry};

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
/// Exhaustion leaves the revision unchanged and cannot reuse a write fence.
pub(crate) fn next_revision(revision: &mut u64) -> Option<u64> {
    let expected = *revision;
    *revision = expected.checked_add(1)?;
    Some(expected)
}

/// Process-local lock was poisoned by a panicking writer.
pub(crate) struct Poisoned;

/// Process-local revision-checked record map behind every memory adapter.
/// Mutations hold one leaf lock with no await. Removed and rejected values
/// retire after releasing the guard, before their allocation owners can lock.
pub(crate) struct MemoryRecords<K, V>(pub(crate) Mutex<BTreeMap<K, V>>);

impl<K, V> Default for MemoryRecords<K, V> {
    fn default() -> Self {
        Self(Mutex::default())
    }
}

impl<K: Copy + Ord, V: Revisioned> MemoryRecords<K, V> {
    fn locked<T>(&self, action: impl FnOnce(&mut BTreeMap<K, V>) -> T) -> Result<T, Poisoned> {
        let mut records = self.0.lock().map_err(|_| Poisoned)?;
        Ok(action(&mut records))
    }

    pub(crate) fn load(&self, key: &K) -> Result<Option<V>, Poisoned>
    where
        V: Clone,
    {
        self.locked(|records| records.get(key).cloned())
    }

    /// Replaces `key` only at `expected` (zero means absent).
    pub(crate) fn compare_and_swap(
        &self,
        key: K,
        expected: u64,
        replacement: V,
    ) -> Result<bool, Poisoned> {
        self.update(key, expected, Some(replacement))
    }

    /// Removes `key` only at `expected` (zero means absent).
    pub(crate) fn compare_and_delete(&self, key: &K, expected: u64) -> Result<bool, Poisoned> {
        self.update(*key, expected, None)
    }

    fn update(&self, key: K, expected: u64, replacement: Option<V>) -> Result<bool, Poisoned> {
        let (matched, retired) = self.locked(|records| match records.entry(key) {
            Entry::Occupied(mut entry) if entry.get().revision() == expected => {
                let retired = match replacement {
                    Some(value) => entry.insert(value),
                    None => entry.remove(),
                };
                (true, Some(retired))
            }
            Entry::Vacant(entry) if expected == 0 => {
                if let Some(value) = replacement {
                    entry.insert(value);
                }
                (true, None)
            }
            _ => (false, replacement),
        })?;
        drop(retired);
        Ok(matched)
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::sync::{Arc, TryLockError, Weak};

    // Exercise the production map with a real retained payload. Its destructor
    // observes lock ownership without waiting, so a broken edge fails directly.
    #[derive(Clone)]
    struct Record {
        revision: u64,
        records: Weak<MemoryRecords<u8, Self>>,
        _custody: crate::WorkspaceName,
    }

    impl Revisioned for Record {
        fn revision(&self) -> u64 {
            self.revision
        }
    }

    impl Drop for Record {
        fn drop(&mut self) {
            if let Some(records) = self.records.upgrade() {
                assert!(
                    !matches!(records.0.try_lock(), Err(TryLockError::WouldBlock)),
                    "record backing retired while its record lock was held"
                );
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn record_retirement_never_holds_the_record_lock(
            commands in proptest::collection::vec((0u8..4, any::<bool>(), any::<bool>()), 0..32)
        ) {
            let records = Arc::new(MemoryRecords::<u8, Record>::default());
            let cancellation = crate::CancellationToken::new();
            let make_record = |revision| Record {
                revision,
                records: Arc::downgrade(&records),
                _custody: crate::WorkspaceName::new(
                    "retained-record", &crate::performance::BOUNDARY_BUDGET, &cancellation,
                ).unwrap().value,
            };
            // Include replacement, rejection, removal and both absent branches
            // in every generated trace; the suffix varies their ordering/keys.
            let prefix = [
                (0, false, false), (0, false, false), (0, false, true),
                (0, true, false), (0, true, false), (0, true, true),
            ];
            for (key, delete, stale) in prefix.into_iter().chain(commands) {
                let revision = records.load(&key).ok().unwrap()
                    .as_ref().map_or(0, Revisioned::revision);
                let expected = revision + u64::from(stale);
                if delete {
                    records.compare_and_delete(&key, expected).ok().unwrap();
                } else {
                    records.compare_and_swap(key, expected, make_record(revision + 1)).ok().unwrap();
                }
            }
            // Even refusal must unlock the poisoned guard before destroying a
            // rejected request; no producer or destructor waits for this lock.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _guard = records.0.lock().unwrap();
                panic!("poison the record writer");
            }));
            prop_assert!(records.compare_and_swap(0, 0, make_record(1)).is_err());
        }
    }
}
