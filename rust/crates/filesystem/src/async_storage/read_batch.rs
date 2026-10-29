use super::*;

/// Reads a batch sequentially when the backend has no multi-get primitive.
pub fn read_many_sequential<S: AsyncObjectStore + ?Sized>(
    store: &S,
    requests: &[ObjectReadRequest],
    context: &mut AuthorityWork<'_>,
) -> impl Future<Output = Result<(Vec<ObjectRead>, AuthorityAllocation), crate::ObjectStoreError>>
+ acyclic_stream::ProviderTask {
    read_many_representation::<false, S>(store, requests, context)
}

pub(crate) fn read_many_stored_sequential<S: AsyncObjectStore + ?Sized>(
    store: &S,
    requests: &[ObjectReadRequest],
    context: &mut AuthorityWork<'_>,
) -> impl Future<Output = Result<(Vec<ObjectRead>, AuthorityAllocation), crate::ObjectStoreError>>
+ acyclic_stream::ProviderTask {
    read_many_representation::<true, S>(store, requests, context)
}

async fn read_many_representation<const STORED: bool, S: AsyncObjectStore + ?Sized>(
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
        batch.check().map_err(crate::ObjectStoreError::from)?;
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
            .map_err(crate::ObjectStoreError::from)?;
        batch
            .charge(WorkCounters {
                items_examined: count,
                ..WorkCounters::UNCHARGED
            })
            .map_err(crate::ObjectStoreError::from)?;
        let mut values = batch
            .vec_owned(requests.len())
            .map_err(crate::ObjectStoreError::from)?;
        for request in requests {
            let mut read_work = batch.operation().map_err(crate::ObjectStoreError::from)?;
            let read = if STORED {
                store
                    .read_stored(request.object_id, request.maximum_bytes, &mut read_work)
                    .await
            } else {
                store
                    .read(request.object_id, request.maximum_bytes, &mut read_work)
                    .await
            };
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
            values.value.push(read?);
        }
        batch.check().map_err(crate::ObjectStoreError::from)?;
        batch
            .charge(WorkCounters {
                items_returned: count,
                ..WorkCounters::UNCHARGED
            })
            .map_err(crate::ObjectStoreError::from)?;
        Ok(values)
    }
    .await;
    if !context.is_native() {
        context.work.try_add_assign(&batch.spent())?;
    }
    result.map(authority_parts)
}

impl From<crate::AuthorityStoreError> for crate::ObjectStoreError {
    fn from(error: crate::AuthorityStoreError) -> Self {
        match error {
            crate::AuthorityStoreError::Work(error) => crate::ObjectStoreError::Work(error),
            crate::AuthorityStoreError::Cancelled => crate::ObjectStoreError::Cancelled,
            #[cfg(not(target_arch = "wasm32"))]
            crate::AuthorityStoreError::NativeOriginal(error) => {
                crate::ObjectStoreError::Native(error)
            }
            _ => crate::ObjectStoreError::SdkAllocationFailed,
        }
    }
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
