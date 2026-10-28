//! The physical key/value medium beneath the one blob engine.
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
#[async_trait::async_trait]
pub trait Medium: Send + Sync + 'static {
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
    /// Replaces `key` with `bytes` only while it holds exactly `expected`,
    /// atomically against every other conditional write and removal.
    /// `Ok(false)` leaves the key untouched. Unflushed, a crash leaves the
    /// old value or the new one, whole: a committed key is rewritten in place.
    async fn replace(
        &self,
        key: &str,
        expected: &[u8],
        bytes: OwnedBytes,
    ) -> Result<bool, MediumError>;
    /// Removes `key` only while it holds exactly `expected`, atomically against
    /// every other conditional write and removal. The engine writes bytes
    /// unique to each write, so a stale remover never matches a later value.
    async fn remove_if(&self, key: &str, expected: &[u8]) -> Result<bool, MediumError>;
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
}

/// The deterministic in-process medium, optionally bounded to a caller's
/// total byte budget.
#[derive(Clone, Default)]
pub struct MemoryMedium {
    keys: Arc<RwLock<Keys>>,
    budget: Option<u64>,
}

#[derive(Default)]
struct Keys {
    keys: BTreeMap<String, OwnedBytes>,
    bytes: u64,
}

impl MemoryMedium {
    /// A medium that refuses writes taking its stored bytes past `budget`.
    #[must_use]
    pub fn bounded(budget: u64) -> Self {
        Self {
            keys: Arc::default(),
            budget: Some(budget),
        }
    }

    fn keys(&self) -> std::sync::RwLockReadGuard<'_, Keys> {
        self.keys.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Keys> {
        self.keys.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// The stored total after `added` bytes replace `freed` ones, or the
    /// quota refusal; checked under the write lock so it is exact.
    fn admit(&self, keys: &Keys, added: usize, freed: usize) -> Result<u64, MediumError> {
        let total = keys.bytes - freed as u64 + added as u64;
        if self.budget.is_some_and(|budget| total > budget) {
            return Err(MediumError::Failed(
                std::io::ErrorKind::QuotaExceeded.into(),
            ));
        }
        Ok(total)
    }
}

#[async_trait::async_trait]
impl Medium for MemoryMedium {
    async fn create(&self, key: &str, bytes: OwnedBytes) -> Result<bool, MediumError> {
        let mut keys = self.write();
        if keys.keys.contains_key(key) {
            return Ok(false);
        }
        keys.bytes = self.admit(&keys, bytes.len(), 0)?;
        keys.keys.insert(key.to_owned(), bytes);
        Ok(true)
    }

    async fn read(
        &self,
        key: &str,
        range: Option<Range<u64>>,
    ) -> Result<Option<OwnedBytes>, MediumError> {
        let keys = self.keys();
        let Some(bytes) = keys.keys.get(key) else {
            return Ok(None);
        };
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
        keys.bytes -= bytes.len() as u64;
        Ok(true)
    }

    async fn replace(
        &self,
        key: &str,
        expected: &[u8],
        bytes: OwnedBytes,
    ) -> Result<bool, MediumError> {
        let mut keys = self.write();
        if !keys
            .keys
            .get(key)
            .is_some_and(|current| current.as_ref() == expected)
        {
            return Ok(false);
        }
        keys.bytes = self.admit(&keys, bytes.len(), expected.len())?;
        keys.keys.insert(key.to_owned(), bytes);
        Ok(true)
    }

    async fn remove_if(&self, key: &str, expected: &[u8]) -> Result<bool, MediumError> {
        let mut keys = self.write();
        if keys
            .keys
            .get(key)
            .is_some_and(|current| current.as_ref() == expected)
        {
            keys.keys.remove(key);
            keys.bytes -= expected.len() as u64;
            return Ok(true);
        }
        Ok(false)
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
            .map(|(key, bytes)| (key.clone(), bytes.len() as u64))
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
