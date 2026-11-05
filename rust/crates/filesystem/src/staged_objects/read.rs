use super::*;
use crate::async_storage::ReadRepresentation;
use crate::kernel::Owned;
use crate::kernel::allocation::KernelOwnedAllocation;

enum PlannedRead {
    Ready(ObjectRead),
    Private,
    Durable,
}

pub(super) fn resident_read(
    bytes: OwnedBytes,
    maximum_bytes: u64,
    work: &mut AuthorityWork<'_>,
) -> Result<ObjectRead, ObjectStoreError> {
    let length = byte_length(&bytes);
    if length > maximum_bytes {
        return Err(ObjectStoreError::TooLarge {
            observed: length,
            maximum: maximum_bytes,
        });
    }
    work.accounting()
        .charge(WorkCounters {
            object_bytes_read: length,
            ..WorkCounters::UNCHARGED
        })
        .map_err(crate::async_storage::object_error)?;
    Ok(ObjectRead {
        bytes,
        retention: ObjectReadRetention::Shared,
    })
}

impl<S: AsyncObjectStore> StagedObjects<S> {
    fn read_plan<R: ReadRepresentation>(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<Owned<Vec<PlannedRead>>>, ObjectStoreError> {
        let index = self.index();
        if index.objects.is_empty() {
            return Ok(None);
        }
        let mut plan: Option<Owned<Vec<PlannedRead>>> = None;
        for (position, request) in requests.iter().enumerate() {
            let found = if R::reads_plaintext(request.object_id) {
                index.lookup(request.object_id)
            } else {
                Lookup::Absent
            };
            if plan.is_none() {
                if matches!(found, Lookup::Absent) {
                    continue;
                }
                crate::async_storage::admit_read_batch(requests.len(), work)?;
                let mut entries = work.vec_owned(requests.len())?;
                entries.value.resize_with(position, || PlannedRead::Durable);
                plan = Some(entries);
            }
            let entry = match found {
                Lookup::Resident(bytes) => {
                    PlannedRead::Ready(resident_read(bytes, request.maximum_bytes, work)?)
                }
                // The offset is deliberately discarded. read_private acquires
                // the existing spill lease before looking it up again.
                Lookup::Spilled { .. } => PlannedRead::Private,
                Lookup::Absent => PlannedRead::Durable,
            };
            if let Some(plan) = &mut plan {
                plan.value.push(entry);
            }
        }
        Ok(plan)
    }

    pub(super) async fn read_batch<R: ReadRepresentation>(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        let mut scope = work.scope()?;
        let Some(plan) = self.read_plan::<R>(requests, &mut scope)? else {
            // The provider owns an entirely durable batch's original output.
            drop(scope);
            return R::read_many(&self.inner, requests, work).await;
        };
        let count = u64::try_from(requests.len()).map_err(|_| crate::WorkError::Overflow)?;
        let misses = plan
            .value
            .iter()
            .filter(|entry| matches!(entry, PlannedRead::Durable))
            .count();
        let mut durable_requests = scope.vec_owned(misses)?;
        for (entry, request) in plan.value.iter().zip(requests) {
            if matches!(entry, PlannedRead::Durable) {
                durable_requests.value.push(*request);
            }
        }
        let mut durable = if misses == 0 {
            None
        } else {
            let (reads, allocation) =
                R::read_many(&self.inner, &durable_requests.value, &mut scope).await?;
            // Keep the provider's physical vector funded through its iterator,
            // including errors and cancellation while private reads await.
            let reads = Owned::new(
                reads.into_iter(),
                allocation.map_or(
                    KernelOwnedAllocation::Standalone,
                    KernelOwnedAllocation::NativeOriginal,
                ),
                0,
            );
            if reads.value.len() != misses {
                return Err(ObjectStoreError::Corrupt);
            }
            Some(reads)
        };
        drop(durable_requests);
        let mut values = scope.vec_owned(requests.len())?;
        let mut plan = Owned::new(plan.value.into_iter(), plan.allocation, plan.bytes);
        for (entry, request) in plan.value.by_ref().zip(requests) {
            let read = match entry {
                PlannedRead::Ready(read) => read,
                PlannedRead::Private => {
                    in_frame(
                        scope.original,
                        self.read_private(request.object_id, request.maximum_bytes, &mut scope),
                    )
                    .await?
                }
                PlannedRead::Durable => durable
                    .as_mut()
                    .and_then(|reads| reads.value.next())
                    .ok_or(ObjectStoreError::Corrupt)?,
            };
            values.bytes = values
                .bytes
                .checked_add(read.retention.owned_bytes())
                .ok_or(crate::WorkError::Overflow)?;
            values.value.push(read);
        }
        drop(plan);
        drop(durable);
        scope.check()?;
        scope.charge(WorkCounters {
            items_returned: count,
            ..WorkCounters::UNCHARGED
        })?;
        Ok(crate::async_storage::authority_parts(
            scope
                .finish_owned(values)
                .map_err(crate::async_storage::object_error)?,
        ))
    }

    pub(super) async fn read_private(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        // The shared lock keeps a drain from reusing this spill offset.
        let spill_file = work
            .accounting()
            .wait_for(self.spill_io.read())
            .await
            .map_err(crate::async_storage::object_error)?;
        let (offset, length) = match self.lookup(object_id) {
            Lookup::Spilled { offset, length } => (offset, length),
            Lookup::Resident(bytes) => return resident_read(bytes, maximum_bytes, work),
            Lookup::Absent => {
                drop(spill_file);
                return self.inner.read(object_id, maximum_bytes, work).await;
            }
        };
        if length > maximum_bytes {
            return Err(ObjectStoreError::TooLarge {
                observed: length,
                maximum: maximum_bytes,
            });
        }
        let object = if let Some(native) = work.original {
            read_spilled_native(
                spill_file.as_ref().ok_or(ObjectStoreError::Corrupt)?,
                object_id,
                offset,
                length,
                native,
            )
            .await?
        } else {
            work.accounting()
                .charge(WorkCounters {
                    backend_read_operations: 1,
                    object_bytes_read: length,
                    bytes_hashed: length,
                    ..WorkCounters::UNCHARGED
                })
                .map_err(crate::async_storage::object_error)?;
            read_spilled(
                spill_file.as_ref().ok_or(ObjectStoreError::Corrupt)?,
                &[(object_id, offset, length)],
            )
            .await?
            .pop()
            .ok_or(ObjectStoreError::Corrupt)?
        };
        let bytes = object.bytes().clone();
        let mut index = self.index_mut();
        if index.resident_bytes.saturating_add(length) <= MAXIMUM_RESIDENT_BYTES
            && matches!(index.objects.get(&object_id), Some(&Staged::Spilled { offset: current, .. }) if current == offset)
        {
            index.insert_resident(object)?;
        }
        Ok(ObjectRead {
            bytes,
            retention: ObjectReadRetention::Shared,
        })
    }
}
