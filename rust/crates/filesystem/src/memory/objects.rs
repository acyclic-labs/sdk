use super::*;
use crate::async_storage::{AsyncObjectStore, object_error};

/// Bounded in-memory immutable object store.
pub struct MemoryObjectStore {
    maximum_object_bytes: u64,
    pub(super) objects: RwLock<MemoryObjectState>,
}

pub(super) struct MemoryObjectState {
    pub(super) objects: Vec<(ObjectId, OwnedBytes)>,
    table_custody: KernelOwnedAllocation,
}

impl Default for MemoryObjectState {
    fn default() -> Self {
        Self {
            objects: Vec::new(),
            table_custody: KernelOwnedAllocation::Standalone,
        }
    }
}

impl MemoryObjectState {
    fn get(&self, id: &ObjectId) -> Option<&OwnedBytes> {
        self.objects
            .binary_search_by_key(id, |(id, _)| *id)
            .ok()
            .map(|index| &self.objects[index].1)
    }
}

impl Default for MemoryObjectStore {
    fn default() -> Self {
        Self {
            maximum_object_bytes: DEFAULT_MAX_OBJECT_BYTES,
            objects: RwLock::new(MemoryObjectState::default()),
        }
    }
}

impl MemoryObjectStore {
    /// Creates a bounded memory object store.
    pub fn new(maximum_object_bytes: u64) -> Result<Self, ObjectStoreError> {
        if maximum_object_bytes == 0 {
            return Err(ObjectStoreError::TooLarge {
                observed: 0,
                maximum: 0,
            });
        }
        Ok(Self {
            maximum_object_bytes,
            objects: RwLock::new(MemoryObjectState::default()),
        })
    }

    fn validate(
        &self,
        writes: &[ObjectWrite],
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        accounting.check()?;
        for write in writes {
            let hash = WorkCounters {
                bytes_hashed: (write.bytes.len() as u64)
                    .checked_add(OBJECT_DIGEST_ENVELOPE_BYTES)
                    .ok_or(WorkError::Overflow)?,
                ..WorkCounters::UNCHARGED
            };
            // Size admission also applies to already hashed objects in insert.
            if write.bytes.len() as u64 > self.maximum_object_bytes {
                return Err(ObjectStoreError::TooLarge {
                    observed: write.bytes.len() as u64,
                    maximum: self.maximum_object_bytes,
                }
                .into());
            }
            accounting.admit(hash)?;
            let digest = object_digest(write.object_id.kind, &write.bytes);
            accounting.record_completed(hash)?;
            if digest != write.object_id.digest {
                return Err(ObjectStoreError::DigestMismatch.into());
            }
        }
        Ok(())
    }

    fn insert(
        &self,
        writes: &[ObjectWrite],
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        if writes.is_empty() {
            return Err(ObjectStoreError::EmptyWriteBatch.into());
        }
        accounting.check()?;
        for write in writes {
            if write.bytes.len() as u64 > self.maximum_object_bytes {
                return Err(ObjectStoreError::TooLarge {
                    observed: write.bytes.len() as u64,
                    maximum: self.maximum_object_bytes,
                }
                .into());
            }
        }
        let mut state = self
            .objects
            .write()
            .map_err(|_| ObjectStoreError::Corrupt)?;
        let mut missing = 0_usize;
        let mut written = 0_u64;
        for (index, write) in writes.iter().enumerate() {
            accounting.charge(WorkCounters {
                object_probes: 1,
                ..WorkCounters::UNCHARGED
            })?;
            if let Some(existing) = state.get(&write.object_id) {
                if existing != &write.bytes {
                    return Err(ObjectStoreError::Corrupt.into());
                }
                continue;
            }
            let mut duplicate = false;
            for earlier in &writes[..index] {
                accounting.charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                if earlier.object_id == write.object_id {
                    if earlier.bytes != write.bytes {
                        return Err(ObjectStoreError::Corrupt.into());
                    }
                    duplicate = true;
                    break;
                }
            }
            if !duplicate {
                missing = missing.checked_add(1).ok_or(WorkError::Overflow)?;
                written = written
                    .checked_add(write.bytes.len() as u64)
                    .ok_or(WorkError::Overflow)?;
            }
        }
        let needed = state
            .objects
            .len()
            .checked_add(missing)
            .ok_or(WorkError::Overflow)?;
        let replacement = if needed > state.objects.capacity() {
            let capacity = needed.max(
                state
                    .objects
                    .capacity()
                    .checked_mul(2)
                    .ok_or(WorkError::Overflow)?,
            );
            Some(allocate(capacity, accounting, |error| {
                ObjectStoreError::MemoryTableAllocationFailed(error).into()
            })?)
        } else {
            None
        };
        let copy = WorkCounters {
            bytes_copied: if replacement.is_some() {
                state
                    .objects
                    .len()
                    .checked_mul(size_of::<(ObjectId, OwnedBytes)>())
                    .and_then(|bytes| u64::try_from(bytes).ok())
                    .ok_or(WorkError::Overflow)?
            } else {
                0
            },
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(WorkCounters {
            backend_write_operations: 1,
            object_bytes_written: written,
            ..copy
        })?;
        accounting.record_completed(WorkCounters {
            backend_write_operations: 1,
            ..WorkCounters::UNCHARGED
        })?;
        if let Some((mut table, custody)) = replacement {
            table.append(&mut state.objects);
            drop(std::mem::replace(&mut state.objects, table));
            state.table_custody = custody;
        }
        accounting.record_completed(copy)?;
        for entry in writes {
            if let Err(index) = state
                .objects
                .binary_search_by_key(&entry.object_id, |(id, _)| *id)
            {
                let shifted = state
                    .objects
                    .len()
                    .checked_sub(index)
                    .and_then(|count| count.checked_mul(size_of::<(ObjectId, OwnedBytes)>()))
                    .and_then(|bytes| u64::try_from(bytes).ok())
                    .ok_or(WorkError::Overflow)?;
                let delta = WorkCounters {
                    bytes_copied: shifted,
                    object_bytes_written: entry.bytes.len() as u64,
                    ..WorkCounters::UNCHARGED
                };
                accounting.admit(delta)?;
                state
                    .objects
                    .insert(index, (entry.object_id, entry.bytes.clone()));
                accounting.record_completed(delta)?;
            }
        }
        accounting.check()
    }

    fn read_value(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<ObjectRead, FsError> {
        accounting.charge(WorkCounters {
            object_probes: 1,
            backend_read_operations: 1,
            ..WorkCounters::UNCHARGED
        })?;
        let state = self.objects.read().map_err(|_| ObjectStoreError::Corrupt)?;
        Self::read_entry(&state, object_id, maximum_bytes, accounting)
    }

    fn read_entry(
        state: &MemoryObjectState,
        object_id: ObjectId,
        maximum_bytes: u64,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<ObjectRead, FsError> {
        let bytes = state.get(&object_id).ok_or(ObjectStoreError::Missing)?;
        let length = bytes.len() as u64;
        if length > maximum_bytes {
            return Err(ObjectStoreError::TooLarge {
                observed: length,
                maximum: maximum_bytes,
            }
            .into());
        }
        let read = WorkCounters {
            object_bytes_read: length,
            bytes_hashed: length
                .checked_add(OBJECT_DIGEST_ENVELOPE_BYTES)
                .ok_or(WorkError::Overflow)?,
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(read)?;
        let digest = object_digest(object_id.kind, bytes);
        accounting.record_completed(read)?;
        if digest != object_id.digest {
            return Err(ObjectStoreError::Corrupt.into());
        }
        Ok(ObjectRead {
            bytes: bytes.clone(),
            retention: ObjectReadRetention::Shared,
        })
    }
}

impl AsyncObjectStore for MemoryObjectStore {
    fn supports_decoded_cache(&self) -> bool {
        false
    }

    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        context: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.put_many(&[ObjectWrite { object_id, bytes }], context)
            .await
    }
    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        context: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        if writes.is_empty() {
            return Err(ObjectStoreError::EmptyWriteBatch);
        }
        let mut accounting = context.accounting();
        self.validate(writes, &mut accounting)
            .and_then(|()| self.insert(writes, &mut accounting))
            .map_err(object_error)
    }
    async fn put_hashed(
        &self,
        object: crate::storage::HashedObject,
        context: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        let (object_id, bytes) = object.into_parts();
        let writes = [ObjectWrite { object_id, bytes }];
        let native = context.is_native();
        let mut accounting = context.accounting();
        if !native {
            self.validate(&writes, &mut accounting)
                .map_err(object_error)?;
        }
        self.insert(&writes, &mut accounting).map_err(object_error)
    }
    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        if context.is_native() {
            return self
                .read_value(object_id, maximum_bytes, &mut context.accounting())
                .map_err(object_error);
        }
        let budget = context.read_budget()?;
        let mut operation = AuthorityWork::new(budget, context.cancellation);
        let result = self
            .read_value(object_id, maximum_bytes, &mut operation.accounting())
            .map_err(object_error);
        context.accept(operation.receipt(result))
    }
    async fn read_many(
        &self,
        requests: &[crate::storage::ObjectReadRequest],
        context: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        if requests.is_empty() {
            return Err(ObjectStoreError::EmptyReadBatch);
        }
        let mut accounting = context.accounting();
        accounting.check().map_err(object_error)?;
        let mut output = allocate(requests.len(), &mut accounting, |error| {
            ObjectStoreError::MemoryTableAllocationFailed(error).into()
        })
        .map_err(object_error)?;
        accounting
            .charge(WorkCounters {
                backend_read_operations: 1,
                ..WorkCounters::UNCHARGED
            })
            .map_err(object_error)?;
        let state = self.objects.read().map_err(|_| ObjectStoreError::Corrupt)?;
        for request in requests {
            accounting
                .charge(WorkCounters {
                    object_probes: 1,
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })
                .map_err(object_error)?;
            output.0.push(
                Self::read_entry(
                    &state,
                    request.object_id,
                    request.maximum_bytes,
                    &mut accounting,
                )
                .map_err(object_error)?,
            );
        }
        accounting
            .charge(WorkCounters {
                items_returned: output.0.len() as u64,
                ..WorkCounters::UNCHARGED
            })
            .map_err(object_error)?;
        accounting.check().map_err(object_error)?;
        Ok((output.0, allocation(output.1)))
    }
    async fn contains(
        &self,
        object_id: ObjectId,
        context: &mut AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        match self
            .read_value(object_id, u64::MAX, &mut context.accounting())
            .map_err(object_error)
        {
            Ok(_) => Ok(true),
            Err(ObjectStoreError::Missing) => Ok(false),
            Err(error) => Err(error),
        }
    }
    async fn flush_before_publish(
        &self,
        _scope: crate::async_storage::PublicationScope<'_>,
        context: &mut AuthorityWork<'_>,
    ) -> Result<crate::PublicationHold, ObjectStoreError> {
        if context.is_native() {
            context.accounting().check().map_err(object_error)?;
        }
        Ok(crate::PublicationHold::none())
    }
}
