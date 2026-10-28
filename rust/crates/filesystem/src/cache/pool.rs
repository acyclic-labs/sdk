//! One bounded residency and fill pool, keyed by provider-owned namespaces.

use super::*;

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

pub(super) struct CacheState {
    pub(super) entries: BTreeMap<(u64, ObjectId), CacheEntry>,
    pub(super) decoded: BTreeMap<(u64, DecodedCacheKey), DecodedCacheEntry>,
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
            entries: BTreeMap::new(),
            decoded: BTreeMap::new(),
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

#[derive(Clone)]
pub(super) struct Namespace {
    pub(super) id: u64,
    pub(super) pool: Arc<Shared>,
}

impl Namespace {
    pub(super) fn key<T>(&self, key: T) -> (u64, T) {
        (self.id, key)
    }

    pub(super) fn clear(&self, state: &mut CacheState) -> Result<(), ObjectStoreError> {
        let canonical = state
            .entries
            .iter()
            .filter(|(key, _)| key.0 == self.id)
            .try_fold(0_u64, |total, (_, entry)| {
                total
                    .checked_add(u64::try_from(entry.bytes.len()).unwrap_or(u64::MAX))
                    .ok_or(ObjectStoreError::Corrupt)
            })?;
        let decoded = state
            .decoded
            .iter()
            .filter(|(key, _)| key.0 == self.id)
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
        state.entries.retain(|key, _| key.0 != self.id);
        state.decoded.retain(|key, _| key.0 != self.id);
        state.recency.retain(|(_, key)| match key {
            RecencyKey::Canonical(key) => key.0 != self.id,
            RecencyKey::Decoded(key) => key.0 != self.id,
        });
        state.resident_canonical_bytes = canonical_remaining;
        state.resident_decoded_bytes = decoded_remaining;
        state.resident_bytes = remaining;
        Ok(())
    }
}

#[cfg(all(test, not(loom)))]
#[path = "pool_tests.rs"]
mod tests;
