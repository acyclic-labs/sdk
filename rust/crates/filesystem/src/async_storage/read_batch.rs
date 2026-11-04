use super::*;

/// Reads a batch sequentially when the backend has no multi-get primitive.
pub fn read_many_sequential<S: AsyncObjectStore + ?Sized>(
    store: &S,
    requests: &[ObjectReadRequest],
    context: &mut AuthorityWork<'_>,
) -> impl Future<Output = Result<(Vec<ObjectRead>, AuthorityAllocation), crate::ObjectStoreError>>
+ acyclic_stream::ProviderTask {
    read_many_representation::<Decoded, S>(store, requests, context)
}

pub(crate) fn read_many_stored_sequential<S: AsyncObjectStore + ?Sized>(
    store: &S,
    requests: &[ObjectReadRequest],
    context: &mut AuthorityWork<'_>,
) -> impl Future<Output = Result<(Vec<ObjectRead>, AuthorityAllocation), crate::ObjectStoreError>>
+ acyclic_stream::ProviderTask {
    read_many_representation::<Stored, S>(store, requests, context)
}

// Select the provider future before forming the batch state machine. The
// decoded path must not also contain the stored representation's read path.
trait ReadRepresentation {
    fn read<S: AsyncObjectStore + ?Sized>(
        store: &S,
        request: &ObjectReadRequest,
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<ObjectRead, crate::ObjectStoreError>> + ProviderTask;
}
struct Decoded;
struct Stored;
impl ReadRepresentation for Decoded {
    fn read<S: AsyncObjectStore + ?Sized>(
        store: &S,
        request: &ObjectReadRequest,
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<ObjectRead, crate::ObjectStoreError>> + ProviderTask {
        store.read(request.object_id, request.maximum_bytes, context)
    }
}
impl ReadRepresentation for Stored {
    fn read<S: AsyncObjectStore + ?Sized>(
        store: &S,
        request: &ObjectReadRequest,
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<ObjectRead, crate::ObjectStoreError>> + ProviderTask {
        store.read_stored(request.object_id, request.maximum_bytes, context)
    }
}

async fn read_many_representation<R: ReadRepresentation, S: AsyncObjectStore + ?Sized>(
    store: &S,
    requests: &[ObjectReadRequest],
    context: &mut AuthorityWork<'_>,
) -> Result<(Vec<ObjectRead>, AuthorityAllocation), crate::ObjectStoreError> {
    let mut scope = context.scope()?;
    let batch = &mut *scope;
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
        values.value.push(R::read(store, request, batch).await?);
    }
    batch.check().map_err(crate::ObjectStoreError::from)?;
    batch
        .charge(WorkCounters {
            items_returned: count,
            ..WorkCounters::UNCHARGED
        })
        .map_err(crate::ObjectStoreError::from)?;
    Ok(authority_parts(values))
}

impl From<crate::AuthorityStoreError> for crate::ObjectStoreError {
    fn from(error: crate::AuthorityStoreError) -> Self {
        match error {
            crate::AuthorityStoreError::Work(error) => crate::ObjectStoreError::Work(error),
            crate::AuthorityStoreError::Cancelled => crate::ObjectStoreError::Cancelled,
            crate::AuthorityStoreError::AccountingHandoffActive => {
                crate::ObjectStoreError::AccountingHandoffActive
            }
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
        crate::FsError::Authority(error) => error.into(),
        crate::FsError::Work(error) => crate::storage::ObjectStoreError::Work(error),
        crate::FsError::Cancelled(_) => crate::storage::ObjectStoreError::Cancelled,
        crate::FsError::AccountingHandoffActive => {
            crate::storage::ObjectStoreError::AccountingHandoffActive
        }
        #[cfg(not(target_arch = "wasm32"))]
        crate::FsError::NativeKernel(error) => crate::storage::ObjectStoreError::Native(error),
        crate::FsError::HandleAllocationFailed => {
            crate::storage::ObjectStoreError::SdkAllocationFailed
        }
        _ => crate::storage::ObjectStoreError::Corrupt,
    }
}
