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
    /// `Ok(false)` leaves the key untouched.
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
    /// Accounts for a `bytes` buffer the engine allocates on this medium's
    /// behalf; free unless the medium meters its caller.
    fn allocate(&self, _bytes: u64) -> Result<(), MediumError> {
        Ok(())
    }
}

/// The deterministic in-process medium.
#[derive(Clone, Default)]
pub struct MemoryMedium {
    keys: Arc<RwLock<BTreeMap<String, OwnedBytes>>>,
}

impl MemoryMedium {
    fn keys(&self) -> std::sync::RwLockReadGuard<'_, BTreeMap<String, OwnedBytes>> {
        self.keys.read().unwrap_or_else(PoisonError::into_inner)
    }
}

#[async_trait::async_trait]
impl Medium for MemoryMedium {
    async fn create(&self, key: &str, bytes: OwnedBytes) -> Result<bool, MediumError> {
        let mut keys = self.keys.write().unwrap_or_else(PoisonError::into_inner);
        if keys.contains_key(key) {
            return Ok(false);
        }
        keys.insert(key.to_owned(), bytes);
        Ok(true)
    }

    async fn read(
        &self,
        key: &str,
        range: Option<Range<u64>>,
    ) -> Result<Option<OwnedBytes>, MediumError> {
        let keys = self.keys();
        let Some(bytes) = keys.get(key) else {
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
        Ok(self
            .keys
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(key)
            .is_some())
    }

    async fn replace(
        &self,
        key: &str,
        expected: &[u8],
        bytes: OwnedBytes,
    ) -> Result<bool, MediumError> {
        let mut keys = self.keys.write().unwrap_or_else(PoisonError::into_inner);
        match keys.get_mut(key) {
            Some(current) if current.as_ref() == expected => {
                *current = bytes;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn remove_if(&self, key: &str, expected: &[u8]) -> Result<bool, MediumError> {
        let mut keys = self.keys.write().unwrap_or_else(PoisonError::into_inner);
        if keys
            .get(key)
            .is_some_and(|current| current.as_ref() == expected)
        {
            keys.remove(key);
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
            .range::<str, _>((start, Bound::Unbounded))
            .take_while(|(key, _)| key.starts_with(prefix))
            .take(limit)
            .map(|(key, bytes)| (key.clone(), bytes.len() as u64))
            .collect())
    }
}
