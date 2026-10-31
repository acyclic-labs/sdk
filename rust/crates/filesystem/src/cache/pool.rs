//! One bounded residency and fill pool, keyed by provider-owned namespaces.

use super::*;
use crate::sync::atomic::{AtomicU64, Ordering};

pub(super) struct CacheEntry {
    pub(super) bytes: OwnedBytes,
    pub(super) touched: u64,
}

pub(super) struct DecodedCacheEntry {
    pub(super) value: DecodedCacheValue,
    pub(super) touched: u64,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum RecencyKey {
    Canonical((u64, ObjectId)),
    Decoded((u64, DecodedCacheKey)),
}

// The namespace owns its entries, so unlinking it never visits another tenant.
pub(super) struct NamespaceIndex<K, V> {
    partitions: BTreeMap<u64, BTreeMap<K, V>>,
    len: usize,
}

impl<K, V> Default for NamespaceIndex<K, V> {
    fn default() -> Self {
        Self {
            partitions: BTreeMap::new(),
            len: 0,
        }
    }
}

impl<K: Copy + Ord, V> NamespaceIndex<K, V> {
    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn get(&self, key: &(u64, K)) -> Option<&V> {
        self.partitions.get(&key.0)?.get(&key.1)
    }

    pub(super) fn get_mut(&mut self, key: &(u64, K)) -> Option<&mut V> {
        self.partitions.get_mut(&key.0)?.get_mut(&key.1)
    }

    pub(super) fn insert(&mut self, key: (u64, K), value: V) {
        if self
            .partitions
            .entry(key.0)
            .or_default()
            .insert(key.1, value)
            .is_none()
        {
            self.len += 1;
        }
    }

    pub(super) fn remove(&mut self, key: &(u64, K)) -> Option<V> {
        let partition = self.partitions.get_mut(&key.0)?;
        let value = partition.remove(&key.1)?;
        self.len -= 1;
        if partition.is_empty() {
            self.partitions.remove(&key.0);
        }
        Some(value)
    }

    pub(super) fn first_key(&self) -> Option<(u64, K)> {
        let (namespace, entries) = self.partitions.first_key_value()?;
        Some((*namespace, *entries.first_key_value()?.0))
    }

    fn partition(&self, namespace: u64) -> impl Iterator<Item = (&K, &V)> {
        self.partitions
            .get(&namespace)
            .into_iter()
            .flat_map(|entries| entries.iter())
    }

    fn detach(&mut self, namespace: u64) -> BTreeMap<K, V> {
        let entries = self.partitions.remove(&namespace).unwrap_or_default();
        self.len -= entries.len();
        entries
    }
}

pub(super) struct DetachedEntries {
    canonical: BTreeMap<ObjectId, CacheEntry>,
    decoded: BTreeMap<DecodedCacheKey, DecodedCacheEntry>,
}

pub(super) struct CacheState {
    pub(super) entries: NamespaceIndex<ObjectId, CacheEntry>,
    pub(super) decoded: NamespaceIndex<DecodedCacheKey, DecodedCacheEntry>,
    pub(super) recency: BTreeSet<(u64, RecencyKey)>,
    pub(super) flights: BTreeMap<(u64, ObjectId), Arc<Flight>>,
    pub(super) resident_bytes: u64,
    pub(super) resident_canonical_bytes: u64,
    pub(super) resident_decoded_bytes: u64,
    pub(super) clock: u64,
    pub(super) next_namespace: u64,
    pub(super) hits: u64,
    pub(super) decoded_hits: u64,
    pub(super) misses: u64,
    pub(super) coalesced_reads: u64,
    pub(super) evictions: u64,
}

impl Default for CacheState {
    fn default() -> Self {
        Self {
            entries: NamespaceIndex::default(),
            decoded: NamespaceIndex::default(),
            recency: BTreeSet::new(),
            flights: BTreeMap::new(),
            resident_bytes: 0,
            resident_canonical_bytes: 0,
            resident_decoded_bytes: 0,
            clock: 1,
            next_namespace: 1,
            hits: 0,
            decoded_hits: 0,
            misses: 0,
            coalesced_reads: 0,
            evictions: 0,
        }
    }
}

pub(super) struct Shared {
    pub(super) options: ObjectCacheOptions,
    // Cache/view precedes any retained payload custody; never held over provider I/O.
    pub(super) state: Mutex<CacheState>,
}

pub(super) struct Namespace {
    pub(super) id: u64,
    pub(super) pool: Arc<Shared>,
    pub(super) epoch: AtomicU64,
}

impl Drop for Namespace {
    fn drop(&mut self) {
        let detached = {
            let mut state = self
                .pool
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            self.clear(&mut state)
        };
        drop(detached);
    }
}

impl Namespace {
    pub(super) fn fill_epoch(&self) -> u64 {
        self.epoch.load(Ordering::Relaxed)
    }

    pub(super) fn key<T>(&self, key: T) -> (u64, T) {
        (self.id, key)
    }

    pub(super) fn clear(
        &self,
        state: &mut CacheState,
    ) -> Result<DetachedEntries, ObjectStoreError> {
        let next_epoch = increment(self.fill_epoch())?;
        let canonical = state
            .entries
            .partition(self.id)
            .try_fold(0_u64, |total, (_, entry)| {
                total
                    .checked_add(u64::try_from(entry.bytes.len()).unwrap_or(u64::MAX))
                    .ok_or(ObjectStoreError::Corrupt)
            })?;
        let decoded = state
            .decoded
            .partition(self.id)
            .try_fold(0_u64, |total, (_, entry)| {
                total
                    .checked_add(entry.value.logical_bytes)
                    .ok_or(ObjectStoreError::Corrupt)
            })?;
        let canonical_remaining = state
            .resident_canonical_bytes
            .checked_sub(canonical)
            .ok_or(ObjectStoreError::Corrupt)?;
        let decoded_remaining = state
            .resident_decoded_bytes
            .checked_sub(decoded)
            .ok_or(ObjectStoreError::Corrupt)?;
        let remaining = state
            .resident_bytes
            .checked_sub(canonical)
            .and_then(|bytes| bytes.checked_sub(decoded))
            .ok_or(ObjectStoreError::Corrupt)?;
        let detached = DetachedEntries {
            canonical: state.entries.detach(self.id),
            decoded: state.decoded.detach(self.id),
        };
        for (key, entry) in &detached.canonical {
            state
                .recency
                .remove(&(entry.touched, RecencyKey::Canonical(self.key(*key))));
        }
        for (key, entry) in &detached.decoded {
            state
                .recency
                .remove(&(entry.touched, RecencyKey::Decoded(self.key(*key))));
        }
        state.resident_canonical_bytes = canonical_remaining;
        state.resident_decoded_bytes = decoded_remaining;
        state.resident_bytes = remaining;
        // Admission checks this exact namespace under the same pool lock.
        // A fill that started before clear may finish, but cannot repopulate it.
        self.epoch.store(next_epoch, Ordering::Relaxed);
        Ok(detached)
    }
}

#[cfg(all(test, not(loom)))]
#[path = "pool_tests.rs"]
mod tests;
