//! The physical key/value medium beneath the one blob engine.
use crate::{Observation, ObservationSource, Observed, ObservedVersion};
use acyclic_stream::OwnedBytes;
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::{Arc, PoisonError, RwLock};

/// A physical failure. `Ambiguous` means a write may or may not have landed.
#[derive(Debug, thiserror::Error)]
pub enum MediumError {
    /// The write's outcome is unknown; repeating the same create is safe.
    #[error("medium write outcome unknown: {0}")]
    Ambiguous(#[source] std::io::Error),
    /// The operation failed without effect.
    #[error("medium failure: {0}")]
    Failed(#[source] std::io::Error),
}

/// Immutable keys under one namespace. Every engine invariant is built from
/// create-if-absent, ranged reads, removal and lexical listing, so one
/// implementation of blob semantics serves memory, local disk and S3 alike.
/// Writes are atomic and immediately visible; [`Medium::flush`] makes them
/// durable, so a caller pays for durability once, where it commits.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Medium: acyclic_stream::ProviderPlatform + 'static {
    /// Creates `key` with `bytes` unless it exists. `Ok(false)` leaves the existing
    /// value untouched. A visible key always holds complete bytes.
    async fn create(&self, key: &str, bytes: OwnedBytes) -> Result<bool, MediumError>;
    /// Reads `range` of `key`, or `None` when it does not exist. A range past the
    /// end is an error; `None` for `range` reads everything.
    async fn read(
        &self,
        key: &str,
        range: Option<Range<u64>>,
    ) -> Result<Option<OwnedBytes>, MediumError>;
    /// Removes `key`, reporting whether it existed.
    async fn remove(&self, key: &str) -> Result<bool, MediumError>;
    /// Reads complete bytes and their original key-bound provider version.
    /// Absence supplies no conditional fact.
    async fn observe(&self, key: String) -> Result<Option<Observed<'_>>, MediumError>;
    /// Replaces the observed key only while its provider version still matches,
    /// atomically against every other write and removal. A successful write
    /// returns its own successor fact without another read. No durability
    /// barrier is added; a crash before flush leaves the old or new value whole.
    async fn replace<'a>(
        &'a self,
        observed: Observation<'a>,
        bytes: OwnedBytes,
    ) -> Result<Option<Observation<'a>>, MediumError>;
    /// Removes only the original observed key while its provider version matches.
    /// A stale version returns false; another source is refused before effect.
    async fn remove_if(&self, observed: Observation<'_>) -> Result<bool, MediumError>;
    /// Lists up to `limit` keys under `prefix` strictly after `after`, in
    /// ascending byte order, with their stored sizes.
    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, u64)>, MediumError>;
    /// Makes the current state of each of `keys`, present or removed, survive
    /// a crash, with the directory entries naming them. Until then a write is
    /// only visible. A medium durable on every write does nothing.
    async fn flush(&self, keys: &[String]) -> Result<(), MediumError>;
    /// Whether a write survives a crash only once flushed.
    fn volatile(&self) -> bool;
    /// Whether this medium opened over writes an owner that crashed may have
    /// left unflushed, and has not settled since: such a key can read at full
    /// length with damaged bytes. A recovered medium has made every key that
    /// survived durable, and stays recovered across clean closes until
    /// [`Medium::settle`].
    fn recovered(&self) -> bool;
    /// Records that the engine repaired what the crash left, every repair
    /// flushed: the next open trusts the medium, unless this owner crashes.
    fn settle(&self);
    /// Accounts for a `bytes` buffer the engine allocates on this medium's
    /// behalf; free unless the medium meters its caller.
    fn allocate(&self, _bytes: u64) -> Result<(), MediumError> {
        Ok(())
    }
    /// Withholds `operations` physical write attempts from the
    /// caller's budget, so the writes after it are never refused for budget;
    /// free unless the medium meters its caller.
    fn reserve_writes(&self, _operations: u64) -> Result<(), MediumError> {
        Ok(())
    }
}

/// The deterministic in-process medium, optionally bounded to a caller's
/// total byte budget.
#[derive(Clone, Default)]
pub struct MemoryMedium {
    shared: Arc<Shared>,
    budget: Option<u64>,
}

#[derive(Default)]
struct Shared {
    keys: RwLock<Keys>,
    source: ObservationSource,
}

#[derive(Default)]
struct Keys {
    keys: BTreeMap<String, Value>,
    sequence: u64,
    bytes: u64,
}

struct Value {
    bytes: OwnedBytes,
    sequence: u64,
}

impl Keys {
    fn next(&mut self) -> Result<u64, MediumError> {
        self.sequence = self.sequence.checked_add(1).ok_or_else(|| {
            MediumError::Failed(std::io::Error::other(
                "medium mutation sequence exhausted; recovery: open a new medium",
            ))
        })?;
        Ok(self.sequence)
    }
}

impl MemoryMedium {
    /// A medium that refuses writes taking its stored bytes past `budget`.
    #[must_use]
    pub fn bounded(budget: u64) -> Self {
        Self {
            shared: Arc::default(),
            budget: Some(budget),
        }
    }

    fn keys(&self) -> std::sync::RwLockReadGuard<'_, Keys> {
        self.shared
            .keys
            .read()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Keys> {
        self.shared
            .keys
            .write()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The stored total after `added` bytes replace `freed` ones, or the
    /// quota refusal; checked under the write lock so it is exact.
    fn admit(&self, stored: u64, added: usize, freed: usize) -> Result<u64, MediumError> {
        let total = stored - freed as u64 + added as u64;
        if self.budget.is_some_and(|budget| total > budget) {
            return Err(MediumError::Failed(
                std::io::ErrorKind::QuotaExceeded.into(),
            ));
        }
        Ok(total)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Medium for MemoryMedium {
    async fn create(&self, key: &str, bytes: OwnedBytes) -> Result<bool, MediumError> {
        let mut keys = self.write();
        if keys.keys.contains_key(key) {
            return Ok(false);
        }
        let total = self.admit(keys.bytes, bytes.len(), 0)?;
        let sequence = keys.next()?;
        keys.bytes = total;
        keys.keys.insert(key.to_owned(), Value { bytes, sequence });
        Ok(true)
    }

    async fn read(
        &self,
        key: &str,
        range: Option<Range<u64>>,
    ) -> Result<Option<OwnedBytes>, MediumError> {
        let keys = self.keys();
        let Some(value) = keys.keys.get(key) else {
            return Ok(None);
        };
        let bytes = &value.bytes;
        let Some(range) = range else {
            return Ok(Some(bytes.clone()));
        };
        let start = usize::try_from(range.start).ok();
        let end = usize::try_from(range.end).ok();
        match (start, end) {
            (Some(start), Some(end)) if start <= end && end <= bytes.len() => {
                Ok(Some(bytes.slice(start..end)))
            }
            _ => Err(MediumError::Failed(std::io::ErrorKind::InvalidInput.into())),
        }
    }

    async fn remove(&self, key: &str) -> Result<bool, MediumError> {
        let mut keys = self.write();
        let Some(bytes) = keys.keys.remove(key) else {
            return Ok(false);
        };
        keys.bytes -= bytes.bytes.len() as u64;
        Ok(true)
    }

    async fn observe(&self, key: String) -> Result<Option<Observed<'_>>, MediumError> {
        let keys = self.keys();
        Ok(keys.keys.get(&key).map(|value| Observed {
            bytes: value.bytes.clone(),
            observation: self
                .shared
                .source
                .issue(key, ObservedVersion::Sequence(value.sequence)),
        }))
    }

    async fn replace<'a>(
        &'a self,
        observed: Observation<'a>,
        bytes: OwnedBytes,
    ) -> Result<Option<Observation<'a>>, MediumError> {
        let ObservedVersion::Sequence(sequence) = self.shared.source.version(&observed)? else {
            return Err(MediumError::Failed(std::io::ErrorKind::InvalidInput.into()));
        };
        let mut locked = self.write();
        let Keys {
            keys,
            bytes: stored,
            sequence: counter,
        } = &mut *locked;
        let Some(current) = keys
            .get_mut(observed.key())
            .filter(|value| value.sequence == *sequence)
        else {
            return Ok(None);
        };
        let total = self.admit(*stored, bytes.len(), current.bytes.len())?;
        let sequence = counter.checked_add(1).ok_or_else(|| {
            MediumError::Failed(std::io::Error::other(
                "medium mutation sequence exhausted; recovery: open a new medium",
            ))
        })?;
        *counter = sequence;
        *stored = total;
        *current = Value { bytes, sequence };
        let key = observed.into_key();
        Ok(Some(
            self.shared
                .source
                .issue(key, ObservedVersion::Sequence(sequence)),
        ))
    }

    async fn remove_if(&self, observed: Observation<'_>) -> Result<bool, MediumError> {
        let ObservedVersion::Sequence(sequence) = self.shared.source.version(&observed)? else {
            return Err(MediumError::Failed(std::io::ErrorKind::InvalidInput.into()));
        };
        let mut keys = self.write();
        let Some(value) = keys
            .keys
            .get(observed.key())
            .filter(|value| value.sequence == *sequence)
        else {
            return Ok(false);
        };
        let length = value.bytes.len() as u64;
        keys.bytes -= length;
        keys.keys.remove(observed.key());
        Ok(true)
    }

    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, u64)>, MediumError> {
        use std::ops::Bound;
        let start = match after {
            Some(after) if after >= prefix => Bound::Excluded(after),
            _ => Bound::Included(prefix),
        };
        Ok(self
            .keys()
            .keys
            .range::<str, _>((start, Bound::Unbounded))
            .take_while(|(key, _)| key.starts_with(prefix))
            .take(limit)
            .map(|(key, value)| (key.clone(), value.bytes.len() as u64))
            .collect())
    }

    async fn flush(&self, _keys: &[String]) -> Result<(), MediumError> {
        Ok(())
    }

    fn volatile(&self) -> bool {
        false
    }

    fn recovered(&self) -> bool {
        false
    }

    fn settle(&self) {}
}
