use super::*;

pub(super) fn object_error(error: crate::FsError) -> ObjectStoreError {
    match error {
        crate::FsError::Object(error) => error,
        crate::FsError::Work(error) => ObjectStoreError::Work(error),
        crate::FsError::Cancelled(_) => ObjectStoreError::Cancelled,
        #[cfg(not(target_arch = "wasm32"))]
        crate::FsError::NativeKernel(error) => ObjectStoreError::Native(error),
        _ => ObjectStoreError::Io(std::io::ErrorKind::OutOfMemory.into()),
    }
}

pub(super) fn provider_remaining(
    accounting: &KernelAccounting<'_>,
    resident: u64,
) -> Result<WorkBudget, crate::FsError> {
    let mut remaining = accounting.remaining()?;
    remaining.peak_allocation_bytes = remaining
        .peak_allocation_bytes
        .checked_sub(resident)
        .ok_or(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            observed: resident,
            maximum: remaining.peak_allocation_bytes,
        })?;
    Ok(remaining)
}

pub(super) fn compose_backend(
    accounting: &mut KernelAccounting<'_>,
    backend: WorkCounters,
    resident: u64,
) -> Result<(), crate::FsError> {
    let (_, work, budget, _) = accounting
        .standalone_parts()
        .ok_or(crate::ObjectStoreError::Corrupt)?;
    *work = work.with_backend(backend, resident)?;
    work.verify(budget)?;
    Ok(())
}
impl<S: AsyncObjectStore> AsyncObjectStore for OperationReadCache<'_, S> {
    crate::async_storage::forward_object_store!(self => self.store;
        requires_child_first_publication,
        maximum_reference_bytes,
        prepare_hashed,
        supports_decoded_cache,
        put_stored,
        read_many_stored,
        read_stored,
        decoded_cache_lookup,
        put,
        contains,
        collection,
        collection_sweeps,
    );

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        if let Some((observation, _)) = context.read_observation {
            context.read_observation = if self
                .data_custody
                .is_some_and(|owner| std::ptr::eq(observation, owner))
            {
                None
            } else {
                Some((observation, 0))
            };
        }

        let cancellation = context.cancellation;
        #[cfg(not(target_arch = "wasm32"))]
        let mut accounting = match context.original {
            Some(work) => KernelAccounting::native(work, cancellation),
            None => KernelAccounting::standalone(context.read_budget()?, cancellation),
        };
        #[cfg(target_arch = "wasm32")]
        let mut accounting = KernelAccounting::standalone(context.read_budget()?, cancellation);
        let result = async {
            #[cfg(not(target_arch = "wasm32"))]
            if context.original.is_some() {
                accounting.check()?;
            }
            if let Some(value) = self.probe(object_id, &mut accounting)? {
                if usize_to_u64(value.bytes.len()) > maximum_bytes {
                    return Err(ObjectStoreError::TooLarge {
                        observed: usize_to_u64(value.bytes.len()),
                        maximum: maximum_bytes,
                    }
                    .into());
                }
                return Ok(value);
            }
            #[cfg(not(target_arch = "wasm32"))]
            if context.original.is_some() {
                let read = self.store.read(object_id, maximum_bytes, context).await?;
                self.admit(object_id, &read.bytes, 0, &mut accounting)?;
                return Ok(read);
            }
            let resident = self.extra_resident()?;
            let remaining = provider_remaining(&accounting, resident)?;
            let mut backend = crate::AuthorityWork::new(remaining, cancellation);
            let read = self
                .store
                .read(object_id, maximum_bytes, &mut backend)
                .await;
            let receipt = backend.receipt(read);
            let receipt = match receipt {
                Ok(receipt) => receipt,
                Err(failure) => {
                    if let Some(owner) = self.data_custody {
                        owner.record_completed_leaf_work(*failure.work)?;
                    }
                    compose_backend(&mut accounting, *failure.work, resident)?;
                    return Err(failure.error.into());
                }
            };
            if let Some(owner) = self.data_custody {
                owner.record_completed_leaf_work(receipt.work)?;
            }
            compose_backend(&mut accounting, receipt.work, resident)?;
            let owned = match receipt.value.retention {
                ObjectReadRetention::Shared => 0,
                ObjectReadRetention::Owned { logical_bytes } => logical_bytes,
            };
            // Resident backend aliases coexist with a new table during growth.
            let (allocations, work, _, _) = accounting
                .standalone_parts()
                .ok_or(crate::ObjectStoreError::Corrupt)?;
            allocations
                .record_owned_bytes(
                    resident.checked_add(owned).ok_or(WorkError::Overflow)?,
                    work,
                )
                .map_err(|error| error.into_work_or(crate::FsError::HandleAllocationFailed))?;
            let mut value = receipt.value;
            if self.admit(object_id, &value.bytes, owned, &mut accounting)? {
                value.retention = ObjectReadRetention::Shared;
            }
            Ok::<_, crate::FsError>(value)
        }
        .await;
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return result.map_err(object_error);
        }
        let work = accounting.spent();
        context.accept(
            result
                .map(|value| crate::OperationReceipt { value, work })
                .map_err(|error| ObjectFailure::new(object_error(error), work)),
        )
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        crate::async_storage::read_many_sequential(self, requests, context).await
    }
}
