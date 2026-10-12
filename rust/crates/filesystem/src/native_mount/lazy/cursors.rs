use super::*;

impl<T> CursorTable<T> {
    pub(super) fn new() -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
            next: AtomicU64::new(1),
        }
    }

    pub(super) fn remember(
        &self,
        generation: (u64, u64),
        cursor: T,
    ) -> Result<[u8; 8], MountSourceError> {
        let mut token = self.next.fetch_add(1, Ordering::Relaxed);
        if token == 0 {
            token = self.next.fetch_add(1, Ordering::Relaxed);
        }
        let mut entries = self.entries.lock().map_err(|_| MountSourceError::Stale)?;
        while entries.len() >= MAXIMUM_LAZY_DIRECTORY_CURSORS {
            let Some(oldest) = entries.first_key_value().map(|(&key, _)| key) else {
                break;
            };
            entries.remove(&oldest);
        }
        entries.insert(token, (generation, cursor));
        Ok(token.to_le_bytes())
    }

    pub(super) fn take(
        &self,
        generation: (u64, u64),
        cursor: Option<&[u8]>,
    ) -> Result<Option<T>, MountSourceError> {
        let Some(cursor) = cursor else {
            return Ok(None);
        };
        let bytes: [u8; 8] = cursor.try_into().map_err(|_| MountSourceError::Stale)?;
        let (recorded, cursor) = self
            .entries
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .remove(&u64::from_le_bytes(bytes))
            .ok_or(MountSourceError::Stale)?;
        if recorded != generation {
            return Err(MountSourceError::Stale);
        }
        Ok(Some(cursor))
    }

    pub(super) fn clear(&self) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.clear();
        }
    }
}
