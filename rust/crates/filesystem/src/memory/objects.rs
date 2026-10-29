use super::*;
use crate::async_storage::{AsyncObjectStore, object_error};

/// In-memory immutable objects with an optional caller-selected size policy.
#[derive(Default)]
pub struct MemoryObjectStore {
    maximum_object_bytes: Option<u64>,
    pub(super) objects: RwLock<MemoryObjectState>,
}

#[derive(Default)]
pub(super) struct MemoryObjectState {
    pub(super) objects:
        crate::kernel::allocation::fixed_index::FixedIndex<OwnedBytes, { ObjectId::ENCODED_LEN }>,
}

impl MemoryObjectState {
    fn get(
        &self,
        id: &ObjectId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<&OwnedBytes>, FsError> {
        self.objects.get(id.index_key(), accounting)
    }
}

impl MemoryObjectStore {
    /// Creates a memory object store with an optional positive byte policy.
    pub fn new(maximum_object_bytes: impl Into<Option<u64>>) -> Result<Self, ObjectStoreError> {
        let maximum_object_bytes = maximum_object_bytes.into();
        if maximum_object_bytes == Some(0) {
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
                bytes_hashed: (write.bytes.len() as u64),
                ..WorkCounters::UNCHARGED
            };
            // Size admission also applies to already hashed objects in insert.
            if let Some(maximum) = self
                .maximum_object_bytes
                .filter(|maximum| write.bytes.len() as u64 > *maximum)
            {
                return Err(ObjectStoreError::TooLarge {
                    observed: write.bytes.len() as u64,
                    maximum,
                }
                .into());
            }
            accounting.admit(hash)?;
            let digest = object_digest(&write.bytes);
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
            if let Some(maximum) = self
                .maximum_object_bytes
                .filter(|maximum| write.bytes.len() as u64 > *maximum)
            {
                return Err(ObjectStoreError::TooLarge {
                    observed: write.bytes.len() as u64,
                    maximum,
                }
                .into());
            }
        }
        let mut state = self
            .objects
            .write()
            .map_err(|_| ObjectStoreError::Corrupt)?;
        // The shared funded identity primitive verifies duplicates without
        // rescanning the batch. Temporary nodes retire before storage grows.
        let mut batch = if writes.len() > 1 {
            Some(crate::kernel::allocation::fixed_index::FixedIndex::<
                &OwnedBytes,
                { ObjectId::ENCODED_LEN },
            >::default())
        } else {
            None
        };
        let prepared = (|| {
            let mut written = 0_u64;
            for write in writes {
                accounting.charge(WorkCounters {
                    object_probes: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                if batch.is_some() {
                    if let Some(existing) = state.get(&write.object_id, accounting)? {
                        if existing != &write.bytes {
                            return Err(ObjectStoreError::Corrupt.into());
                        }
                        continue;
                    }
                }
                if let Some(batch) = &mut batch {
                    let mut inserted = false;
                    let previous = batch.get_or_try_insert_with(
                        write.object_id.index_key(),
                        accounting,
                        |_| {
                            inserted = true;
                            Ok(&write.bytes)
                        },
                    )?;
                    if *previous != &write.bytes {
                        return Err(ObjectStoreError::Corrupt.into());
                    }
                    if !inserted {
                        continue;
                    }
                }
                written = written
                    .checked_add(write.bytes.len() as u64)
                    .ok_or(WorkError::Overflow)?;
            }
            Ok::<_, FsError>(written)
        })();
        drop(batch);
        let written = prepared?;
        accounting.admit(WorkCounters {
            backend_write_operations: 1,
            object_bytes_written: if writes.len() > 1 { written } else { 0 },
            ..WorkCounters::UNCHARGED
        })?;
        accounting.record_completed(WorkCounters {
            backend_write_operations: 1,
            ..WorkCounters::UNCHARGED
        })?;
        for entry in writes {
            let stored = state.objects.get_or_try_insert_with(
                entry.object_id.index_key(),
                accounting,
                |accounting| {
                    let delta = WorkCounters {
                        object_bytes_written: entry.bytes.len() as u64,
                        ..WorkCounters::UNCHARGED
                    };
                    accounting.admit(delta)?;
                    let bytes = entry.bytes.clone();
                    accounting.record_completed(delta)?;
                    Ok(bytes)
                },
            )?;
            if stored != &entry.bytes {
                return Err(ObjectStoreError::Corrupt.into());
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
        let bytes = state
            .get(&object_id, accounting)?
            .ok_or(ObjectStoreError::Missing)?;
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
            bytes_hashed: length,
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(read)?;
        let digest = object_digest(bytes);
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
