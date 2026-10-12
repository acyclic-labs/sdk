use super::*;

/// Reads a batch sequentially when the backend has no multi-get primitive.
pub async fn read_many_sequential<S: AsyncObjectStore + ?Sized>(
    store: &S,
    requests: &[ObjectReadRequest],
    context: &mut AuthorityWork<'_>,
) -> Result<(Vec<ObjectRead>, AuthorityAllocation), crate::ObjectStoreError> {
    let cancellation = context.cancellation;
    let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
        context.work.remaining(context.budget)?,
        cancellation,
    );
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = context.original {
        accounting = crate::kernel::allocation::KernelAccounting::native(original, cancellation);
    }
    let result = async {
        let accounting = &mut accounting;
        accounting.check()?;
        if requests.is_empty() {
            return Err(crate::storage::ObjectStoreError::EmptyReadBatch.into());
        }
        let item_count =
            u64::try_from(requests.len()).map_err(|_| crate::performance::WorkError::Overflow)?;
        let requested_bytes = requests
            .len()
            .checked_mul(size_of::<ObjectRead>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(crate::performance::WorkError::Overflow)?;
        accounting.admit(crate::performance::WorkCounters {
            items_examined: item_count,
            items_returned: item_count,
            ..crate::performance::WorkCounters::UNCHARGED
        })?;
        accounting.charge(crate::performance::WorkCounters {
            items_examined: item_count,
            ..crate::performance::WorkCounters::UNCHARGED
        })?;
        let allocation = accounting.claim_owned(requested_bytes, 1)?;
        let mut values = (Vec::new(), allocation);
        values
            .0
            .try_reserve_exact(requests.len())
            .map_err(crate::storage::ObjectStoreError::MemoryTableAllocationFailed)?;
        let actual_bytes = values
            .0
            .capacity()
            .checked_mul(size_of::<ObjectRead>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(crate::performance::WorkError::Overflow)?;
        accounting.reconcile_owned(&mut values.1, requested_bytes, actual_bytes)?;
        for request in requests {
            accounting.check()?;
            #[cfg(not(target_arch = "wasm32"))]
            let mut read_work = match accounting.native_work() {
                Some(original) => AuthorityWork::native(original, cancellation),
                None => AuthorityWork::new(accounting.remaining()?, cancellation),
            };
            #[cfg(target_arch = "wasm32")]
            let mut read_work = AuthorityWork::new(accounting.remaining()?, cancellation);
            let read = store
                .read(request.object_id, request.maximum_bytes, &mut read_work)
                .await;
            #[cfg(not(target_arch = "wasm32"))]
            if read_work.original.is_some() {
                values.0.push(read?);
                continue;
            }
            let result = read_work.receipt(read);
            match result {
                Ok(receipt) => {
                    let retained = match receipt.value.retention {
                        crate::storage::ObjectReadRetention::Shared => 0,
                        crate::storage::ObjectReadRetention::Owned { logical_bytes } => {
                            logical_bytes
                        }
                    };
                    merge_completed_object_read(accounting, receipt.work, retained)?;
                    values.0.push(receipt.value);
                }
                Err(failure) => {
                    merge_completed_object_read(accounting, *failure.work, 0)?;
                    return Err(failure.error.into());
                }
            }
        }
        accounting.charge(crate::performance::WorkCounters {
            items_returned: item_count,
            ..crate::performance::WorkCounters::UNCHARGED
        })?;
        Ok(values)
    }
    .await
    .map_err(object_error)
    .and_then(|(values, allocation)| match allocation {
        #[cfg(not(target_arch = "wasm32"))]
        crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) => {
            native_read_batch(values, allocation)
                .map(|(values, allocation)| (values, Some(allocation)))
        }
        crate::kernel::allocation::KernelOwnedAllocation::Standalone => Ok((values, None)),
    });
    let work = accounting.spent();
    context.accept(
        result
            .map(|value| crate::OperationReceipt { value, work })
            .map_err(|error| crate::storage::ObjectFailure::new(error, work)),
    )
}

pub(crate) fn merge_completed_object_read(
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    completed: crate::performance::WorkCounters,
    retained: u64,
) -> Result<(), crate::FsError> {
    let (allocations, work, budget, _) = accounting
        .standalone_parts()
        .ok_or(crate::storage::ObjectStoreError::Corrupt)?;
    let peak = allocations
        .live_bytes()
        .checked_add(completed.peak_allocation_bytes)
        .ok_or(crate::performance::WorkError::Overflow)?;
    let mut combined = work.checked_add(completed)?;
    combined.peak_allocation_bytes = combined.peak_allocation_bytes.max(peak);
    *work = combined;
    allocations
        .record_owned_bytes(retained, work)
        .map_err(|error| error.into_work_or(crate::FsError::HandleAllocationFailed))?;
    work.verify(budget)?;
    Ok(())
}

pub(crate) fn object_error(error: crate::FsError) -> crate::storage::ObjectStoreError {
    match error {
        crate::FsError::Object(error) => error,
        crate::FsError::Work(error) => crate::storage::ObjectStoreError::Work(error),
        crate::FsError::Cancelled(_) => crate::storage::ObjectStoreError::Cancelled,
        #[cfg(not(target_arch = "wasm32"))]
        crate::FsError::NativeKernel(error) => crate::storage::ObjectStoreError::Native(error),
        crate::FsError::HandleAllocationFailed => {
            crate::storage::ObjectStoreError::SdkAllocationFailed
        }
        _ => crate::storage::ObjectStoreError::Corrupt,
    }
}
