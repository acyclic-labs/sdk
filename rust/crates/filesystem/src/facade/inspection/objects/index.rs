//! A disposable hash index with admission before every backing allocation.
use super::*;

type Slot = Option<(ObjectId, OwnedBytes)>;

#[derive(Default)]
pub(super) struct Entries {
    slots: Vec<Slot>,
    count: usize,
    _allocation: Option<crate::NativeAllocation>,
}

impl Entries {
    fn position(
        &self,
        id: ObjectId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<usize, ObjectStoreError> {
        let mut position = id
            .digest
            .as_bytes()
            .iter()
            .take(size_of::<usize>())
            .fold(0usize, |value, byte| {
                value.wrapping_mul(257) ^ usize::from(*byte)
            });
        for _ in 0..self.slots.len() {
            work.accounting()
                .charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })
                .map_err(crate::async_storage::object_error)?;
            position &= self.slots.len() - 1;
            let slot = self.slots.get(position).ok_or(ObjectStoreError::Corrupt)?;
            if slot.as_ref().is_none_or(|(key, _)| *key == id) {
                return Ok(position);
            }
            position = position.wrapping_add(1);
        }
        Err(ObjectStoreError::Corrupt)
    }

    pub(super) fn find(
        &self,
        id: ObjectId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<OwnedBytes>, ObjectStoreError> {
        if self.slots.is_empty() {
            return Ok(None);
        }
        let position = self.position(id, work)?;
        Ok(self
            .slots
            .get(position)
            .and_then(|slot| slot.as_ref().map(|(_, bytes)| bytes.clone())))
    }

    pub(super) fn insert(
        &mut self,
        object: HashedObject,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        let (id, bytes) = object.into_parts();
        if self.find(id, work)?.is_some() {
            return Ok(());
        }
        if self.count >= self.slots.len() / 2 {
            self.grow(work)?;
        }
        let position = self.position(id, work)?;
        *self
            .slots
            .get_mut(position)
            .ok_or(ObjectStoreError::Corrupt)? = Some((id, bytes));
        self.count += 1;
        Ok(())
    }

    fn grow(&mut self, work: &mut AuthorityWork<'_>) -> Result<(), ObjectStoreError> {
        let capacity = self
            .slots
            .len()
            .max(8)
            .checked_mul(2)
            .ok_or(WorkError::Overflow)?;
        let bytes = capacity
            .checked_mul(size_of::<Slot>())
            .ok_or(WorkError::Overflow)?;
        let original = work.original.ok_or(ObjectStoreError::NativeUnavailable)?;
        // Both tables coexist while entries move. Keep both claims until the
        // old backing storage has physically been released.
        let mut allocation = original.claim(bytes as u64, 1, WorkCounters::UNCHARGED)?;
        let mut slots = Vec::new();
        slots.try_reserve_exact(capacity).map_err(|cause| {
            ObjectStoreError::Native(crate::NativeTreeMaterializationError::AllocationFailed(
                cause,
            ))
        })?;
        allocation.reconcile_capacity((slots.capacity() * size_of::<Slot>()) as u64)?;
        slots.resize_with(capacity, || None);
        let mut replacement = Self {
            slots,
            count: self.count,
            _allocation: Some(allocation),
        };
        for (id, bytes) in self.slots.iter().flatten() {
            let position = replacement.position(*id, work)?;
            *replacement
                .slots
                .get_mut(position)
                .ok_or(ObjectStoreError::Corrupt)? = Some((*id, bytes.clone()));
        }
        std::mem::swap(self, &mut replacement);
        drop(replacement);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn growth_admits_both_tables_and_refusal_preserves_every_prior_object() {
        let cancellation = CancellationToken::new();
        let ledger = crate::NativeMaterializationWork::accounting_state_bytes();
        let budget = WorkBudget {
            peak_allocation_bytes: ledger + (32 * size_of::<Slot>()) as u64,
            ..WorkBudget::UNBOUNDED
        };
        let original = crate::NativeMaterializationWork::new(budget, &cancellation).unwrap();
        let mut work = AuthorityWork::native(&original, &cancellation);
        let mut entries = Entries::default();
        for bytes in b"abcdefgh".chunks(1) {
            entries
                .insert(
                    HashedObject::new(crate::ObjectKind::BlobChunk, OwnedBytes::from_static(bytes)),
                    &mut work,
                )
                .unwrap();
        }
        assert!(
            entries
                .insert(
                    HashedObject::new(crate::ObjectKind::BlobChunk, OwnedBytes::from_static(b"i")),
                    &mut work
                )
                .is_err(),
            "growth must admit the old and replacement tables while both are live"
        );
        for bytes in b"abcdefgh".chunks(1) {
            let object =
                HashedObject::new(crate::ObjectKind::BlobChunk, OwnedBytes::from_static(bytes));
            assert_eq!(
                entries
                    .find(object.object_id(), &mut work)
                    .unwrap()
                    .as_deref(),
                Some(bytes)
            );
        }
        original.receipt().verify(budget).unwrap();
        drop((entries, work));
        assert_eq!(
            original.live_allocation_bytes(),
            ledger,
            "dropping the disposable index must retire its complete backing allocation"
        );
    }
}
