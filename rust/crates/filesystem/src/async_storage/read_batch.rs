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

// Select the provider future before forming the batch state machine. The
// decoded path must not also contain the stored representation's read path.
pub(crate) trait ReadRepresentation {
    fn reads_plaintext(id: ObjectId) -> bool;

    fn read_many<S: AsyncObjectStore + ?Sized>(
        store: &S,
        requests: &[ObjectReadRequest],
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<
        Output = Result<(Vec<ObjectRead>, AuthorityAllocation), crate::ObjectStoreError>,
    > + ProviderTask;

    fn read<S: AsyncObjectStore + ?Sized>(
        store: &S,
        request: &ObjectReadRequest,
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<ObjectRead, crate::ObjectStoreError>> + ProviderTask;
}
pub(crate) struct Decoded;
pub(crate) struct Stored;
impl ReadRepresentation for Decoded {
    fn reads_plaintext(_: ObjectId) -> bool {
        true
    }

    fn read_many<S: AsyncObjectStore + ?Sized>(
        store: &S,
        requests: &[ObjectReadRequest],
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<
        Output = Result<(Vec<ObjectRead>, AuthorityAllocation), crate::ObjectStoreError>,
    > + ProviderTask {
        store.read_many(requests, context)
    }

    fn read<S: AsyncObjectStore + ?Sized>(
        store: &S,
        request: &ObjectReadRequest,
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<ObjectRead, crate::ObjectStoreError>> + ProviderTask {
        store.read(request.object_id, request.maximum_bytes, context)
    }
}
impl ReadRepresentation for Stored {
    fn reads_plaintext(id: ObjectId) -> bool {
        id.storage.is_none()
    }

    fn read_many<S: AsyncObjectStore + ?Sized>(
        store: &S,
        requests: &[ObjectReadRequest],
        context: &mut AuthorityWork<'_>,
    ) -> impl Future<
        Output = Result<(Vec<ObjectRead>, AuthorityAllocation), crate::ObjectStoreError>,
    > + ProviderTask {
        store.read_many_stored(requests, context)
    }

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
    let count = admit_read_batch(requests.len(), batch)?;
    let mut values = batch
        .vec_owned(requests.len())
        .map_err(crate::ObjectStoreError::from)?;
    for request in requests {
        let read = R::read(store, request, batch).await?;
        values.bytes = values
            .bytes
            .checked_add(read.retention.owned_bytes())
            .ok_or(crate::WorkError::Overflow)?;
        values.value.push(read);
    }
    batch.check().map_err(crate::ObjectStoreError::from)?;
    batch
        .charge(WorkCounters {
            items_returned: count,
            ..WorkCounters::UNCHARGED
        })
        .map_err(crate::ObjectStoreError::from)?;
    Ok(authority_parts(
        scope.finish_owned(values).map_err(object_error)?,
    ))
}

pub(crate) fn admit_read_batch(
    length: usize,
    work: &mut AuthorityWork<'_>,
) -> Result<u64, crate::ObjectStoreError> {
    if length == 0 {
        return Err(crate::ObjectStoreError::EmptyReadBatch);
    }
    let count = u64::try_from(length).map_err(|_| crate::WorkError::Overflow)?;
    work.admit(WorkCounters {
        items_examined: count,
        items_returned: count,
        ..WorkCounters::UNCHARGED
    })?;
    work.charge(WorkCounters {
        items_examined: count,
        ..WorkCounters::UNCHARGED
    })?;
    Ok(count)
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
            error => {
                crate::ObjectStoreError::Filesystem(Box::new(crate::FsError::Authority(error)))
            }
        }
    }
}

/// Converts a filesystem failure at an object-store boundary. Object-store
/// categories map to their own variants; every other failure is kept whole,
/// so no cause is collapsed into another category.
pub(crate) fn object_error(error: crate::FsError) -> crate::storage::ObjectStoreError {
    use crate::storage::ObjectStoreError as O;
    match error {
        crate::FsError::Object(error) => error,
        crate::FsError::Authority(error) => error.into(),
        crate::FsError::Work(error) => O::Work(error),
        crate::FsError::Cancelled(_) => O::Cancelled,
        crate::FsError::AccountingHandoffActive => O::AccountingHandoffActive,
        #[cfg(not(target_arch = "wasm32"))]
        crate::FsError::NativeKernel(error) => O::Native(error),
        crate::FsError::HandleAllocationFailed => O::SdkAllocationFailed,
        crate::FsError::Decode(error) => O::Decode(error),
        error => O::Filesystem(Box::new(error)),
    }
}

impl From<crate::FsError> for crate::ObjectStoreError {
    fn from(error: crate::FsError) -> Self {
        object_error(error)
    }
}
