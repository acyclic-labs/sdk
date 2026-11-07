use super::*;

impl<T> CursorTable<T> {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(CursorState {
                entries: BTreeMap::new(),
                next: 1,
            }),
        }
    }

    pub(super) fn remember(
        &self,
        generation: (u64, u64),
        cursor: T,
    ) -> Result<[u8; 8], MountSourceError> {
        let mut state = self.state.lock().map_err(|_| MountSourceError::Stale)?;
        let token = state.next;
        state.next = token.checked_add(1).ok_or(crate::WorkError::Overflow)?;
        let entries = &mut state.entries;
        while entries.len() >= MAXIMUM_LAZY_DIRECTORY_CURSORS {
            let Some(oldest) = entries.first_key_value().map(|(&key, _)| key) else {
                break;
            };
            entries.remove(&oldest);
        }
        entries.insert(token, CursorEntry { generation, cursor });
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
        let CursorEntry {
            generation: recorded,
            cursor,
        } = self
            .state
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .entries
            .remove(&u64::from_le_bytes(bytes))
            .ok_or(MountSourceError::Stale)?;
        if recorded != generation {
            return Err(MountSourceError::Stale);
        }
        Ok(Some(cursor))
    }

    pub(super) fn clear(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.entries.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exhausted_directory_tokens_preserve_retained_cursors() {
        let cursors = CursorTable::new();
        let original = cursors.remember((2, 1), "original").expect("first cursor");
        // Advance the actual issuer to its boundary without issuing 2^64 cursors.
        cursors.state.lock().expect("cursor state").next = u64::MAX - 1;
        let last = cursors.remember((3, 1), "last").expect("last cursor");
        let exhausted = [
            cursors.remember((2, 1), "replacement"),
            cursors.remember((2, 1), "replacement"),
        ];
        assert_eq!(
            cursors
                .take((2, 1), Some(&original))
                .expect("original cursor"),
            Some("original")
        );
        assert_eq!(
            cursors.take((3, 1), Some(&last)).expect("last cursor"),
            Some("last")
        );
        assert!(exhausted.into_iter().all(|result| matches!(
            result,
            Err(MountSourceError::Work(crate::WorkError::Overflow))
        )));
        cursors.clear();
        assert!(matches!(
            cursors.remember((2, 1), "after clear"),
            Err(MountSourceError::Work(crate::WorkError::Overflow))
        ));
    }
}
