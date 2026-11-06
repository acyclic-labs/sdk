//! Preserve one provider batch across privately staged members.
use super::*;
use crate::kernel::{KernelOwnedAllocation, Owned};

impl<S: AsyncObjectStore> StagedObjects<S> {
    // The absent prefix can go directly to the provider without another lookup.
    // Keep the original resident alias when the first private member is found.
    fn first_private<const STORED: bool>(
        &self,
        requests: &[ObjectReadRequest],
    ) -> Option<(usize, Lookup)> {
        let index = self.index();
        requests.iter().enumerate().find_map(|(position, request)| {
            if STORED && request.object_id.storage.is_some() {
                return None;
            }
            match index.objects.get(&request.object_id)? {
                Staged::Resident {
                    object, referenced, ..
                } => {
                    referenced.store(true, Ordering::Relaxed);
                    Some((position, Lookup::Resident(object.bytes().clone())))
                }
                Staged::Spilled { offset, length } => Some((
                    position,
                    Lookup::Spilled {
                        offset: *offset,
                        length: *length,
                    },
                )),
            }
        })
    }

    pub(super) async fn read_partitioned<const STORED: bool>(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        let count = requests.len() as u64;
        work.admit(WorkCounters {
            items_examined: count,
            ..WorkCounters::UNCHARGED
        })?;
        work.charge(WorkCounters {
            items_examined: count,
            ..WorkCounters::UNCHARGED
        })?;
        let Some((first, lookup)) = self.first_private::<STORED>(requests) else {
            return if STORED {
                self.inner.read_many_stored(requests, work).await
            } else {
                self.inner.read_many(requests, work).await
            };
        };
        let mut scope = work.scope()?;
        let work = &mut *scope;
        work.admit(WorkCounters {
            items_returned: count,
            ..WorkCounters::UNCHARGED
        })?;
        // Slots retain private aliases and the exact input order. Provider
        // requests must be contiguous for the existing multi-get contract.
        let mut slots = work.vec_owned::<Option<ObjectRead>>(requests.len())?;
        let mut missing: Option<Owned<Vec<ObjectReadRequest>>> = None;
        let mut output = work.vec_owned::<ObjectRead>(requests.len())?;
        let mut first_lookup = Some(lookup);
        for (position, request) in requests.iter().enumerate() {
            let lookup = if position < first || (STORED && request.object_id.storage.is_some()) {
                Lookup::Absent
            } else if position == first {
                first_lookup.take().ok_or(ObjectStoreError::Corrupt)?
            } else {
                self.lookup(request.object_id)
            };
            let read = match lookup {
                Lookup::Absent => {
                    if missing.is_none() {
                        missing = Some(work.vec_owned(requests.len())?);
                    }
                    let missing = missing.as_mut().ok_or(ObjectStoreError::Corrupt)?;
                    work.accounting()
                        .perform(
                            WorkCounters {
                                bytes_copied: std::mem::size_of::<ObjectReadRequest>() as u64,
                                ..WorkCounters::UNCHARGED
                            },
                            || missing.value.push(*request),
                        )
                        .map_err(crate::async_storage::object_error)?;
                    None
                }
                Lookup::Resident(bytes) => Some(resident_read(bytes, request.maximum_bytes, work)?),
                Lookup::Spilled { .. } => {
                    // Revalidate the offset under the existing spill hold. A
                    // drain can retire it between classification and this read.
                    Some(
                        in_frame(
                            work.original,
                            self.read_private(request.object_id, request.maximum_bytes, work),
                        )
                        .await?,
                    )
                }
            };
            slots.value.push(read);
        }
        // Transfer original custody before moving any provider value. On every
        // exit the iterator dies before the output and its combined guard.
        let mut reads = if let Some(missing) = &missing {
            let (reads, allocation) = if STORED {
                self.inner.read_many_stored(&missing.value, work).await?
            } else {
                self.inner.read_many(&missing.value, work).await?
            };
            let header = (reads.capacity() * std::mem::size_of::<ObjectRead>()) as u64;
            let allocation = if work.is_native() {
                KernelOwnedAllocation::NativeOriginal(
                    allocation.ok_or(NativeTreeMaterializationError::MissingCustody)?,
                )
            } else {
                KernelOwnedAllocation::Standalone
            };
            let reads = Owned::new(reads, allocation, header)
                .transfer_to(
                    &mut output.allocation,
                    &mut output.bytes,
                    &mut work.accounting(),
                )
                .map_err(crate::async_storage::object_error)?;
            Some((reads.into_iter(), header))
        } else {
            None
        };
        for slot in &mut slots.value {
            let read = if let Some(read) = slot.take() {
                read
            } else {
                reads
                    .as_mut()
                    .and_then(|(reads, _)| reads.next())
                    .ok_or(ObjectStoreError::Corrupt)?
            };
            output.bytes = output
                .bytes
                .checked_add(read.retention.owned_bytes())
                .ok_or(crate::WorkError::Overflow)?;
            output.value.push(read);
        }
        if let Some((mut reads, header)) = reads {
            if reads.next().is_some() {
                return Err(ObjectStoreError::Corrupt);
            }
            drop(reads);
            work.accounting()
                .release_owned(&mut output.allocation, header)
                .map_err(crate::async_storage::object_error)?;
            output.bytes -= header;
        }
        slots
            .retire(&mut work.accounting())
            .map_err(crate::async_storage::object_error)?;
        if let Some(missing) = missing {
            missing
                .retire(&mut work.accounting())
                .map_err(crate::async_storage::object_error)?;
        }
        work.check()?;
        work.charge(WorkCounters {
            items_returned: count,
            ..WorkCounters::UNCHARGED
        })?;
        Ok(crate::async_storage::authority_parts(
            scope
                .finish_owned(output)
                .map_err(crate::async_storage::object_error)?,
        ))
    }
}
