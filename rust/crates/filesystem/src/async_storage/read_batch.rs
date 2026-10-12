use super::*;

/// Explicit sequential implementation for adapters whose physical backend has no multi-get primitive.
pub async fn read_many_sequential_async<S: AsyncObjectStore + ?Sized>(
    store: &S,
    requests: &[ObjectReadRequest],
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> ObjectResult<Vec<ObjectRead>> {
    let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
        budget, cancellation,
    );
    let result = read_many_sequential_engine(store, requests, &mut accounting, cancellation).await;
    let work = accounting.spent();
    match result {
        Ok(values) => Ok(crate::OperationReceipt { value: values.0, work }),
        Err(error) => Err(crate::storage::ObjectFailure::new(
            object_error(error), work,
        )),
    }
}

/// Explicit sequential native adapter using the caller's original work and allocation custody.
#[cfg(not(target_arch = "wasm32"))]
pub async fn read_many_sequential_with_native_work<S: AsyncObjectStore + ?Sized>(
    store: &S,
    requests: &[ObjectReadRequest],
    work: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<(Vec<ObjectRead>, crate::materializer::NativeAllocation), crate::storage::ObjectStoreError> {
    let mut accounting = crate::kernel::allocation::KernelAccounting::native(work, cancellation);
    let result = read_many_sequential_engine(store, requests, &mut accounting, cancellation)
        .await.map_err(object_error)?;
    match result.1 {
        crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) => {
            crate::async_storage::native_read_batch(result.0, allocation)
        }
        crate::kernel::allocation::KernelOwnedAllocation::Standalone => {
            drop(result.0);
            Err(crate::storage::ObjectStoreError::Corrupt)
        }
    }
}

pub(super) async fn read_many_sequential_engine<S: AsyncObjectStore + ?Sized>(
    store: &S,
    requests: &[ObjectReadRequest],
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(Vec<ObjectRead>, crate::kernel::allocation::KernelOwnedAllocation), crate::FsError> {
    accounting.check()?;
    if requests.is_empty() {
        return Err(crate::storage::ObjectStoreError::EmptyReadBatch.into());
    }
    let item_count = u64::try_from(requests.len())
        .map_err(|_| crate::performance::WorkError::Overflow)?;
    let requested_bytes = requests.len().checked_mul(size_of::<ObjectRead>())
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
    values.0.try_reserve_exact(requests.len())
        .map_err(crate::storage::ObjectStoreError::MemoryTableAllocationFailed)?;
    let actual_bytes = values.0.capacity().checked_mul(size_of::<ObjectRead>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(crate::performance::WorkError::Overflow)?;
    accounting.reconcile_owned(&mut values.1, requested_bytes, actual_bytes)?;
    for request in requests {
        accounting.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = accounting.native_work() {
            let read = store.read_with_native_work(
                request.object_id, request.maximum_bytes, work, cancellation,
            ).await?;
            values.0.push(read);
            continue;
        }
        let result = store.read(
            request.object_id, request.maximum_bytes, accounting.remaining()?, cancellation,
        ).await;
        match result {
            Ok(receipt) => {
                let retained = match receipt.value.retention {
                    crate::storage::ObjectReadRetention::Shared => 0,
                    crate::storage::ObjectReadRetention::Owned { logical_bytes } => logical_bytes,
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

pub(crate) fn merge_completed_object_read(
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    completed: crate::performance::WorkCounters,
    retained: u64,
) -> Result<(), crate::FsError> {
    let (allocations, work, budget, _) = accounting.standalone_parts()
        .ok_or(crate::storage::ObjectStoreError::Corrupt)?;
    let peak = allocations.live_bytes().checked_add(completed.peak_allocation_bytes)
        .ok_or(crate::performance::WorkError::Overflow)?;
    let mut combined = work.checked_add(completed)?;
    combined.peak_allocation_bytes = combined.peak_allocation_bytes.max(peak);
    *work = combined;
    allocations.record_owned_bytes(retained, work)
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
        crate::FsError::HandleAllocationFailed => crate::storage::ObjectStoreError::SdkAllocationFailed,
        _ => crate::storage::ObjectStoreError::Corrupt,
    }
}
