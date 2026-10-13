use super::*;

/// Reads a batch sequentially when the backend has no multi-get primitive.
pub async fn read_many_sequential<S: AsyncObjectStore + ?Sized>(
    store: &S,
    requests: &[ObjectReadRequest],
    context: &mut AuthorityWork<'_>,
) -> Result<(Vec<ObjectRead>, AuthorityAllocation), crate::ObjectStoreError> {
    let mut batch = AuthorityWork::new(
        context.work.remaining(context.budget)?,
        context.cancellation,
    );
    #[cfg(not(target_arch = "wasm32"))]
    {
        batch.original = context.original;
    }
    let result = async {
        batch.check().map_err(authority_object_error)?;
        if requests.is_empty() {
            return Err(crate::ObjectStoreError::EmptyReadBatch);
        }
        let count = u64::try_from(requests.len()).map_err(|_| crate::WorkError::Overflow)?;
        batch
            .admit(WorkCounters {
                items_examined: count,
                items_returned: count,
                ..WorkCounters::UNCHARGED
            })
            .map_err(authority_object_error)?;
        batch
            .charge(WorkCounters {
                items_examined: count,
                ..WorkCounters::UNCHARGED
            })
            .map_err(authority_object_error)?;
        let mut values = batch
            .vec_owned(requests.len())
            .map_err(authority_object_error)?;
        for request in requests {
            let mut read_work = batch.operation().map_err(authority_object_error)?;
            let read = store
                .read(request.object_id, request.maximum_bytes, &mut read_work)
                .await;
            if !batch.is_native() {
                batch.work = batch
                    .work
                    .with_backend(read_work.spent(), batch.allocations.live_bytes())?;
                let retained = match &read {
                    Ok(ObjectRead {
                        retention: crate::storage::ObjectReadRetention::Owned { logical_bytes },
                        ..
                    }) => *logical_bytes,
                    _ => 0,
                };
                batch
                    .allocations
                    .record_owned_bytes(retained, &mut batch.work)
                    .map_err(|error| {
                        error.into_work_or(crate::ObjectStoreError::SdkAllocationFailed)
                    })?;
                batch.work.verify(batch.budget)?;
            }
            values.0.push(read?);
        }
        batch.check().map_err(authority_object_error)?;
        batch
            .charge(WorkCounters {
                items_returned: count,
                ..WorkCounters::UNCHARGED
            })
            .map_err(authority_object_error)?;
        Ok(values)
    }
    .await;
    if !context.is_native() {
        context.work.try_add_assign(&batch.spent())?;
    }
    result
}

fn authority_object_error(error: crate::AuthorityStoreError) -> crate::ObjectStoreError {
    match error {
        crate::AuthorityStoreError::Work(error) => crate::ObjectStoreError::Work(error),
        crate::AuthorityStoreError::Cancelled => crate::ObjectStoreError::Cancelled,
        #[cfg(not(target_arch = "wasm32"))]
        crate::AuthorityStoreError::NativeOriginal(error) => crate::ObjectStoreError::Native(error),
        _ => crate::ObjectStoreError::SdkAllocationFailed,
    }
}

pub(crate) fn merge_completed_object_read(
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    completed: crate::performance::WorkCounters,
    retained: u64,
) -> Result<(), crate::FsError> {
    let (allocations, work, budget, _) = accounting
        .standalone_parts()
        .ok_or(crate::storage::ObjectStoreError::Corrupt)?;
    *work = work.with_backend(completed, allocations.live_bytes())?;
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
