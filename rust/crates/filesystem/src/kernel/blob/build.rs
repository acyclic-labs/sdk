use super::*;

/// Hard bounds for streaming immutable-blob construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlobBuildOptions {
    /// Positive maximum bytes per authenticated content chunk.
    pub chunk_bytes: u32,
    /// At least two references per blob-index page.
    pub page_items: u32,
    /// Maximum canonical bytes in one authenticated blob-index page.
    pub page_bytes: u32,
    /// Maximum logical bytes consumed from the source.
    pub maximum_blob_bytes: u64,
}

/// Successful streaming blob construction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlobBuild {
    /// Authenticated blob-index root.
    pub root: ObjectId,
    /// Exact logical content length.
    pub logical_bytes: u64,
    /// Exact source, encoding, and backend work receipt.
    pub work: WorkCounters,
}

/// Streams a source into fixed-size authenticated chunks and a bounded index.
pub async fn build_blob<S: AsyncObjectStore, R: AsyncBlobSource>(
    store: &S,
    source: &mut R,
    options: BlobBuildOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<BlobBuild, BlobBuildFailure> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    build_blob_with_accounting(store, source, options, &mut accounting, cancellation)
        .await
        .map_err(|error| build_failed(blob_build_error(error), accounting.spent()))
}

pub(crate) async fn build_blob_with_accounting<S: AsyncObjectStore, R: AsyncBlobSource>(
    store: &S,
    source: &mut R,
    options: BlobBuildOptions,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<BlobBuild, FsError> {
    accounting.check()?;
    if options.chunk_bytes == 0
        || effective_page_width(options, store.maximum_reference_bytes()).is_none()
        || options.maximum_blob_bytes == 0
    {
        return Err(BlobBuildError::InvalidOptions.into());
    }
    let chunk_capacity =
        usize::try_from(options.chunk_bytes).map_err(|_| BlobBuildError::InvalidOptions)?;
    let mut batching = BlobBatchStore::new(store, accounting)?;
    let mut index = BlobIndexBuilder::new(options, store.maximum_reference_bytes());
    let result = async {
        let mut logical_bytes = 0_u64;
        loop {
            accounting.check()?;
            let detection_bytes = options
                .maximum_blob_bytes
                .saturating_sub(logical_bytes)
                .saturating_add(1);
            let maximum =
                chunk_capacity.min(usize::try_from(detection_bytes).unwrap_or(usize::MAX));
            let owned = read_owned_blob_source(source, maximum, accounting, cancellation).await?;
            let (chunk, filled, at_eof) = if let Some(chunk) = owned {
                let filled = chunk.bytes.len();
                // A short transferred chunk is not the producer's terminal event.
                (chunk, filled, filled == 0)
            } else {
                let (chunk, filled) =
                    read_blob_chunk(source, maximum, accounting, cancellation).await?;
                // Buffered reads stop short only after observing an explicit zero.
                (chunk, filled, filled < maximum)
            };
            if filled == 0 {
                drop_source_chunk(chunk, accounting)?;
                break;
            }
            logical_bytes = accept_owned_blob_chunk(
                &mut batching,
                &mut index,
                chunk,
                maximum,
                logical_bytes,
                options.maximum_blob_bytes,
                accounting,
                cancellation,
            )
            .await?;
            if at_eof {
                break;
            }
        }
        let root = index
            .finish(&mut batching, accounting, cancellation)
            .await?;
        batching.flush(accounting, cancellation).await?;
        Ok(BlobBuild {
            root,
            logical_bytes,
            work: accounting.spent(),
        })
    }
    .await;
    let index_cleanup = index.release(accounting);
    let batch_cleanup = batching.release(accounting);
    match result {
        Err(error) => Err(error),
        Ok(value) => {
            index_cleanup?;
            batch_cleanup?;
            Ok(value)
        }
    }
}

pub(super) fn blob_build_error(error: FsError) -> BlobBuildError {
    match error {
        FsError::BlobBuild(error) => error,
        FsError::Work(error) | FsError::Object(crate::ObjectStoreError::Work(error)) => {
            BlobBuildError::Work(error)
        }
        FsError::Cancelled(_) => BlobBuildError::Cancelled,
        FsError::Object(error) => BlobBuildError::Storage(error),
        FsError::Decode(error) => BlobBuildError::Encode(error),
        error => BlobBuildError::Context(Box::new(error)),
    }
}

pub(super) fn build_allocation_error(error: AllocationError) -> FsError {
    error.into_work_or(FsError::BlobBuild(BlobBuildError::AllocationFailed))
}

pub(super) fn vector_bytes<T>(capacity: usize) -> Result<u64, FsError> {
    capacity
        .checked_mul(size_of::<T>())
        .map(usize_to_u64)
        .ok_or_else(|| WorkError::Overflow.into())
}

pub(super) fn empty_blob_vec<T>() -> ReadOwned<Vec<T>> {
    ReadOwned {
        value: Vec::new(),
        custody: KernelOwnedAllocation::Standalone,
    }
}

pub(super) fn reserve_blob_vec<T>(
    owner: &mut ReadOwned<Vec<T>>,
    capacity: usize,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    if owner.value.capacity() >= capacity {
        return Ok(());
    }
    let before = vector_bytes::<T>(owner.value.capacity())?;
    let requested = vector_bytes::<T>(capacity)?;
    let delta = requested.checked_sub(before).ok_or(WorkError::Overflow)?;
    let mut admitted = accounting.claim_owned(delta, 1)?;
    #[cfg(not(target_arch = "wasm32"))]
    if matches!(owner.custody, KernelOwnedAllocation::Standalone)
        && matches!(admitted, KernelOwnedAllocation::NativeOriginal(_))
    {
        owner.custody = admitted;
    } else {
        owner.custody.absorb(&mut admitted)?;
    }
    #[cfg(target_arch = "wasm32")]
    owner.custody.absorb(&mut admitted)?;
    if let Err(error) = owner.value.try_reserve_exact(capacity - owner.value.len()) {
        let _ = accounting.reconcile_owned(&mut owner.custody, requested, before);
        return Err(BlobBuildError::ReserveFailed(error).into());
    }
    let actual = vector_bytes::<T>(owner.value.capacity())?;
    accounting.reconcile_owned(&mut owner.custody, requested, actual)
}

pub(super) fn release_blob_vec<T>(
    owner: ReadOwned<Vec<T>>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    let bytes = vector_bytes::<T>(owner.value.capacity())?;
    drop(owner.value);
    match owner.custody {
        KernelOwnedAllocation::Standalone => release_blob_bytes(bytes, accounting),
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(guard) => {
            drop(guard);
            Ok(())
        }
    }
}

pub(super) fn completed_blob_backend(
    measured: WorkCounters,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    match accounting {
        KernelAccounting::Standalone {
            allocations,
            work,
            budget,
            ..
        } => {
            *work = work.with_backend(measured, allocations.live_bytes())?;
            work.verify(*budget)?;
        }
        KernelAccounting::BorrowedStandalone {
            allocations,
            work,
            budget,
            ..
        } => {
            **work = work.with_backend(measured, allocations.live_bytes())?;
            work.verify(*budget)?;
        }
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { .. } => {
            return Err(BlobBuildError::NativeSourceUnavailable.into());
        }
    }
    Ok(())
}

pub(super) fn release_blob_bytes(
    bytes: u64,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    // Cleanup is unconditional, including after cancellation or a late denial.
    match accounting {
        KernelAccounting::Standalone { allocations, .. } => {
            allocations.release(bytes).map_err(build_allocation_error)
        }
        KernelAccounting::BorrowedStandalone { allocations, .. } => {
            allocations.release(bytes).map_err(build_allocation_error)
        }
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { .. } => Ok(()),
    }
}

pub(super) fn drop_source_chunk(
    chunk: OwnedSourceChunk,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    let bytes = chunk.logical_owned_bytes;
    drop(chunk);
    release_blob_bytes(bytes, accounting)
}

pub(super) fn hashed_blob_object(
    kind: ObjectKind,
    bytes: acyclic_stream::OwnedBytes,
    accounting: &mut KernelAccounting<'_>,
    _cancellation: &CancellationToken,
) -> Result<HashedObject, FsError> {
    accounting.check()?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = accounting.native_work() {
        return HashedObject::new_with_native_work(kind, bytes, original, _cancellation)
            .map_err(Into::into);
    }
    let hash = WorkCounters {
        bytes_hashed: usize_to_u64(bytes.len()),
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(hash)?;
    let object = HashedObject::new(kind, bytes);
    accounting.record_completed(hash)?;
    Ok(object)
}

#[allow(
    clippy::too_many_arguments,
    reason = "Store, bounds, accounting and cancellation are independent inputs to this operation."
)]
pub(super) async fn accept_owned_blob_chunk<S: AsyncObjectStore>(
    batching: &mut BlobBatchStore<'_, S>,
    index: &mut BlobIndexBuilder,
    chunk: OwnedSourceChunk,
    maximum: usize,
    logical_bytes: u64,
    maximum_blob_bytes: u64,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<u64, FsError> {
    let chunk_bytes = usize_to_u64(chunk.bytes.len());
    let end = logical_bytes
        .checked_add(chunk_bytes)
        .ok_or(BlobBuildError::TooLarge);
    let end = match end {
        Ok(end) if chunk.bytes.len() <= maximum && end <= maximum_blob_bytes => end,
        _ => {
            drop_source_chunk(chunk, accounting)?;
            return Err(BlobBuildError::TooLarge.into());
        }
    };
    let retained = chunk.logical_owned_bytes;
    let object =
        match hashed_blob_object(ObjectKind::BlobChunk, chunk.bytes, accounting, cancellation) {
            Ok(object) => object,
            Err(error) => {
                let _ = release_blob_bytes(retained, accounting);
                return Err(error);
            }
        };
    let chunk_id = batching
        .put(object, retained, accounting, cancellation)
        .await?;
    index
        .push_chunk(
            batching,
            BlobChunkRef {
                first_offset: logical_bytes,
                end_offset: end,
                chunk: chunk_id,
            },
            accounting,
            cancellation,
        )
        .await?;
    Ok(end)
}

pub(super) const BLOB_BATCH_OBJECTS: usize = 4;
const BLOB_BATCH_BYTES: u64 = 4 * 1024 * 1024;

pub(super) struct BlobBatchStore<'a, S> {
    inner: &'a S,
    writes: ReadOwned<Vec<HashedObject>>,
    bytes: u64,
}

impl<'a, S: AsyncObjectStore> BlobBatchStore<'a, S> {
    pub(super) fn new(
        inner: &'a S,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, FsError> {
        let mut writes = empty_blob_vec();
        if let Err(error) = reserve_blob_vec(&mut writes, BLOB_BATCH_OBJECTS, accounting) {
            let _ = release_blob_vec(writes, accounting);
            return Err(error);
        }
        Ok(Self {
            inner,
            writes,
            bytes: 0,
        })
    }

    pub(super) async fn flush(
        &mut self,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        if self.writes.value.is_empty() {
            return Ok(());
        }
        accounting.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            self.inner
                .put_many_hashed(
                    &self.writes.value,
                    &mut crate::AuthorityWork::native(original, cancellation),
                )
                .await?;
            self.writes.value.clear();
            self.bytes = 0;
            return Ok(());
        }
        let result = {
            let __object_args = (&self.writes.value, accounting.remaining()?, cancellation);
            let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
            let __object_result = self
                .inner
                .put_many_hashed(__object_args.0, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        };
        let measured = match &result {
            Ok(receipt) => receipt.work,
            Err(failure) => *failure.work,
        };
        completed_blob_backend(measured, accounting)?;
        result.map_err(|failure| FsError::from(failure.error))?;
        self.writes.value.clear();
        let bytes = std::mem::take(&mut self.bytes);
        release_blob_bytes(bytes, accounting)
    }

    pub(super) async fn put(
        &mut self,
        object: HashedObject,
        retained: u64,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, FsError> {
        let object =
            crate::async_storage::prepare_object(self.inner, object, accounting, cancellation)
                .await?;
        let id = object.object_id();
        self.bytes = self
            .bytes
            .checked_add(retained)
            .ok_or(WorkError::Overflow)?;
        self.writes.value.push(object);
        if self.writes.value.len() >= BLOB_BATCH_OBJECTS || self.bytes >= BLOB_BATCH_BYTES {
            self.flush(accounting, cancellation).await?;
        }
        Ok(id)
    }

    pub(super) fn release(self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        let bytes = self.bytes;
        release_blob_vec(self.writes, accounting)?;
        release_blob_bytes(bytes, accounting)
    }
}
