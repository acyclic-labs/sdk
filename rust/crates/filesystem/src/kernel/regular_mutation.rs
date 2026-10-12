//! Ordered regular-file mutation across canonical inline and sparse payloads.

use super::allocation::KernelAccounting;
use super::persistent_point::Owned;
use super::{
    BlobBuildError, BlobReadError, CanonicalDecodeError, DecodeLimits, Extent,
    ExtentCloneError, ExtentCloneRequest, ExtentCloneReceipt, ExtentKind, ExtentMutation,
    ExtentMutationError, ExtentPage, ExtentRangeRequest, ExtentReadError, FilePayload,
    InlineFileData, InlineFileDataError, MAXIMUM_INLINE_FILE_BYTES,
};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::foundation::usize_to_u64;
use crate::FsError;
use crate::model::VolumeConfig;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{ByteRange, ObjectId, ObjectKind, ObjectStoreError};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegularMutation {
    Write {
        offset: u64,
        length: u64,
        content: ObjectId,
        content_offset: u64,
    },
    Resize {
        logical_bytes: u64,
    },
    ZeroRange {
        offset: u64,
        length: u64,
        allocated: bool,
        extend: bool,
    },
    Preallocate {
        offset: u64,
        length: u64,
        keep_size: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegularMutationReceipt {
    pub(crate) payload: FilePayload,
    pub(crate) work: WorkCounters,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegularCloneReceipt {
    pub(crate) destination: FilePayload,
    pub(crate) work: WorkCounters,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RegularStorage {
    Inline(InlineFileData),
    Sparse {
        logical_bytes: u64,
        extents: ObjectId,
    },
}

impl RegularStorage {
    fn validate(payload: FilePayload) -> Result<Self, RegularMutationError> {
        match payload {
            FilePayload::InlineRegular(data) => Ok(Self::Inline(data)),
            FilePayload::Regular {
                logical_bytes,
                extents,
            } => Ok(Self::Sparse {
                logical_bytes,
                extents,
            }),
            FilePayload::Directory { .. }
            | FilePayload::SymbolicLink { .. }
            | FilePayload::Empty
            | FilePayload::Device { .. }
            | FilePayload::ReparsePoint { .. } => Err(RegularMutationError::NotRegular),
        }
    }

    fn logical_bytes(self) -> u64 {
        match self {
            Self::Inline(data) => usize_to_u64(data.as_bytes().len()),
            Self::Sparse { logical_bytes, .. } => logical_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PromotedRegularReceipt {
    logical_bytes: u64,
    extents: ObjectId,
}

pub(crate) type RegularMutationFailure = OperationFailure<RegularMutationError>;

/// Stable regular-file mutation failures surfaced by generation transactions.
#[derive(Debug, Error)]
pub enum RegularMutationError {
    /// The target record was not a regular file.
    #[error("content mutation requires a regular-file payload")]
    NotRegular,
    /// A requested byte interval or accounting value overflowed.
    #[error("regular-file mutation range overflowed")]
    RangeOverflow,
    /// A write referred to an object that was not a canonical blob.
    #[error("regular-file content object has the wrong kind")]
    WrongContentKind,
    /// Inline payload construction failed.
    #[error(transparent)]
    Inline(#[from] InlineFileDataError),
    /// Inline-to-sparse blob construction failed.
    #[error(transparent)]
    BlobBuild(#[from] BlobBuildError),
    /// A bounded source-blob range read failed.
    #[error(transparent)]
    BlobRead(#[from] BlobReadError),
    /// Sparse extent path-copy failed.
    #[error(transparent)]
    Extent(#[from] ExtentMutationError),
    /// Sparse range planning for inline demotion failed.
    #[error(transparent)]
    Range(#[from] ExtentReadError),
    /// Sparse source planning or destination range-clone failed.
    #[error(transparent)]
    Clone(#[from] ExtentCloneError),
    /// The requested source interval extends beyond the source file.
    #[error("regular-file clone source range is outside the source file")]
    SourceRangeOutOfBounds,
    /// A dense volume operation would introduce sparse extent semantics.
    #[error("regular-file operation requires sparse volume semantics")]
    SparseSemanticsDisabled,
    /// Keep-size allocation beyond EOF cannot be represented portably.
    #[error("keep-size preallocation beyond EOF requires a physical backend capability")]
    KeepSizeBeyondEofUnsupported,
    /// Canonical extent encoding or validation failed.
    #[error(transparent)]
    Codec(#[from] CanonicalDecodeError),
    /// Immutable-object storage failed.
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    /// Bounded scratch allocation failed.
    #[error("regular-file mutation allocation failed")]
    AllocationFailed,
    /// Exact work accounting overflowed or exceeded its admitted budget.
    #[error(transparent)]
    Work(#[from] WorkError),
    /// A shared kernel boundary returned another original typed filesystem error.
    #[error(transparent)]
    Context(Box<FsError>),
}

pub(crate) async fn apply_regular_mutation_async<S: AsyncObjectStore>(
    store: &S, payload: FilePayload, mutation: RegularMutation, config: VolumeConfig,
    budget: WorkBudget, cancellation: &CancellationToken,
) -> Result<RegularMutationReceipt, RegularMutationFailure> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    apply_regular_mutation_with_accounting(store, payload, mutation, config, &mut accounting, cancellation)
        .await.map_err(|error| OperationFailure::new(project_regular_error(error), accounting.spent()))
}

pub(crate) async fn apply_regular_mutation_with_accounting<S: AsyncObjectStore>(
    store: &S, payload: FilePayload, mutation: RegularMutation, config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<RegularMutationReceipt, FsError> {
    accounting.check()?;
    validate_mutation(mutation).map_err(regular_error)?;
    if !config.sparse_files {
        let current = regular_logical_bytes(payload).map_err(regular_error)?;
        let requires_sparse = match mutation {
            RegularMutation::Write { offset, .. } => offset > current,
            RegularMutation::Resize { logical_bytes } => logical_bytes > current,
            RegularMutation::ZeroRange { .. } | RegularMutation::Preallocate { .. } => true,
        };
        if requires_sparse { return Err(regular_error(RegularMutationError::SparseSemanticsDisabled)); }
    }
    let payload = if let FilePayload::InlineRegular(data) = payload {
        if let Some(payload) = try_inline(store, data, mutation, config, accounting, cancellation).await? {
            return Ok(RegularMutationReceipt { payload, work: accounting.spent() });
        }
        let promoted = promote_inline(store, data, config, accounting, cancellation).await?;
        FilePayload::Regular { logical_bytes: promoted.logical_bytes, extents: promoted.extents }
    } else { payload };
    apply_sparse(store, payload, mutation, config, accounting, cancellation).await
}

pub(crate) async fn apply_regular_clone_async<S: AsyncObjectStore>(
    store: &S, source: FilePayload, source_offset: u64, destination: FilePayload,
    destination_offset: u64, length: u64, config: VolumeConfig, budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<RegularCloneReceipt, RegularMutationFailure> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    apply_regular_clone_with_accounting(
        store, source, source_offset, destination, destination_offset, length, config,
        &mut accounting, cancellation,
    ).await.map_err(|error| OperationFailure::new(project_regular_error(error), accounting.spent()))
}

pub(crate) async fn apply_regular_clone_with_accounting<S: AsyncObjectStore>(
    store: &S, source: FilePayload, source_offset: u64, destination: FilePayload,
    destination_offset: u64, length: u64, config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<RegularCloneReceipt, FsError> {
    accounting.check()?;
    if !config.sparse_files { return Err(regular_error(RegularMutationError::SparseSemanticsDisabled)); }
    let source = RegularStorage::validate(source).map_err(regular_error)?;
    let destination = RegularStorage::validate(destination).map_err(regular_error)?;
    let source_end = source_offset.checked_add(length).ok_or_else(|| regular_error(RegularMutationError::RangeOverflow))?;
    let destination_end = destination_offset.checked_add(length).ok_or_else(|| regular_error(RegularMutationError::RangeOverflow))?;
    if length == 0 || source_end > source.logical_bytes() {
        return Err(regular_error(RegularMutationError::SourceRangeOutOfBounds));
    }
    if let (RegularStorage::Inline(source_data), RegularStorage::Inline(destination_data)) = (source, destination)
        && destination_offset <= destination.logical_bytes()
        && destination_end <= usize_to_u64(MAXIMUM_INLINE_FILE_BYTES)
    {
        charge_copy(accounting, length)?;
        let range = usize::try_from(source_offset).map_err(|_| WorkError::Overflow)?
            ..usize::try_from(source_end).map_err(|_| WorkError::Overflow)?;
        let bytes = source_data.as_bytes().get(range).ok_or(WorkError::Overflow)?;
        let data = destination_data.replace_range(
            usize::try_from(destination_offset).map_err(|_| WorkError::Overflow)?, bytes,
            usize::try_from(destination.logical_bytes().max(destination_end)).map_err(|_| WorkError::Overflow)?,
        )?;
        return Ok(RegularCloneReceipt { destination: FilePayload::InlineRegular(data), work: accounting.spent() });
    }
    let equal = source == destination;
    let (source_logical_bytes, source_root) = sparse_storage(store, source, config, accounting, cancellation).await?;
    let (destination_logical_bytes, destination_root) = if equal {
        (source_logical_bytes, source_root)
    } else { sparse_storage(store, destination, config, accounting, cancellation).await? };
    let receipt = clone_extent_range_with_accounting(store, ExtentCloneRequest {
        source_root, source_logical_bytes, source_range: ByteRange { offset: source_offset, length },
        destination_root, destination_logical_bytes, destination_offset,
        maximum_spans: config.limits.maximum_mutations_per_batch,
        maximum_mutations: config.limits.maximum_mutations_per_batch,
    }, DecodeLimits::for_volume(config), accounting, cancellation).await?;
    let payload = if receipt.logical_bytes <= usize_to_u64(MAXIMUM_INLINE_FILE_BYTES) {
        try_demote_sparse_async(store, receipt.root, receipt.logical_bytes, receipt.logical_bytes,
            config, accounting, cancellation).await?
            .map(FilePayload::InlineRegular)
    } else { None };
    Ok(RegularCloneReceipt {
        destination: payload.unwrap_or(FilePayload::Regular { logical_bytes: receipt.logical_bytes, extents: receipt.root }),
        work: accounting.spent(),
    })
}

pub(crate) async fn clone_extent_range_with_accounting<S: AsyncObjectStore>(
    store: &S, request: ExtentCloneRequest, limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<ExtentCloneReceipt, FsError> {
    accounting.check()?;
    super::extent_clone::validate_request(request, limits)
        .map_err(|error| regular_error(RegularMutationError::Clone(error)))?;
    let (plan, allocation) = super::range::plan_extent_range_with_accounting(store, ExtentRangeRequest {
        root: request.source_root, file_size: request.source_logical_bytes, range: request.source_range,
        maximum_spans: request.maximum_spans, limits, budget: WorkBudget::UNBOUNDED,
    }, accounting, cancellation).await.map_err(|error| clone_stage_error(error, CloneStage::Source))?;
    let bytes = plan.retained_allocation_bytes;
    let plan = Owned::new(plan, allocation, bytes);
    accounting.charge(WorkCounters { items_examined: usize_to_u64(plan.value.spans.len()), ..WorkCounters::UNCHARGED })
        .map_err(|error| clone_stage_error(error, CloneStage::Compile))?;
    let mut mutations = Owned::<Vec<ExtentMutation>>::allocate(plan.value.spans.len(), accounting)
        .map_err(|error| clone_stage_error(error, CloneStage::Compile))?;
    for span in &plan.value.spans {
        let relative = span.offset.checked_sub(request.source_range.offset).ok_or(WorkError::Overflow)?;
        mutations.value.push(ExtentMutation::Replace {
            offset: request.destination_offset.checked_add(relative).ok_or(WorkError::Overflow)?,
            length: span.length, kind: span.kind.clone(), extend: true,
        });
    }
    plan.retire(accounting)?;
    let receipt = super::extent_mutation::apply_extent_mutations_with_accounting(
        store, request.destination_root, request.destination_logical_bytes, &mutations.value,
        request.maximum_mutations, limits, accounting, cancellation,
    ).await.map_err(|error| clone_stage_error(error, CloneStage::Destination));
    mutations.retire(accounting)?;
    let receipt = receipt?;
    Ok(ExtentCloneReceipt { root: receipt.root, logical_bytes: receipt.logical_bytes, work: accounting.spent() })
}

fn regular_logical_bytes(payload: FilePayload) -> Result<u64, RegularMutationError> {
    RegularStorage::validate(payload).map(RegularStorage::logical_bytes)
}

fn validate_mutation(mutation: RegularMutation) -> Result<(), RegularMutationError> {
    match mutation {
        RegularMutation::Write { offset, length, content, content_offset } => {
            if content.kind != ObjectKind::Blob { return Err(RegularMutationError::WrongContentKind); }
            if length == 0 || offset.checked_add(length).is_none() || content_offset.checked_add(length).is_none() {
                return Err(RegularMutationError::RangeOverflow);
            }
        }
        RegularMutation::ZeroRange { offset, length, .. } | RegularMutation::Preallocate { offset, length, .. } => {
            if length == 0 || offset.checked_add(length).is_none() { return Err(RegularMutationError::RangeOverflow); }
        }
        RegularMutation::Resize { .. } => {}
    }
    Ok(())
}

async fn try_inline<S: AsyncObjectStore>(
    store: &S, data: InlineFileData, mutation: RegularMutation, config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<Option<FilePayload>, FsError> {
    let current = usize_to_u64(data.as_bytes().len());
    let data = match mutation {
        RegularMutation::Resize { logical_bytes } => {
            if logical_bytes > current { return Ok(None); }
            data.truncate(usize::try_from(logical_bytes).map_err(|_| WorkError::Overflow)?)?
        }
        RegularMutation::ZeroRange { offset, length, allocated, extend } => {
            let end = offset.checked_add(length).ok_or_else(|| regular_error(RegularMutationError::RangeOverflow))?;
            if !extend && offset >= current { return Ok(Some(FilePayload::InlineRegular(data))); }
            if !allocated || (extend && offset > current) { return Ok(None); }
            let logical_bytes = if extend { end.max(current) } else { current };
            if logical_bytes > usize_to_u64(MAXIMUM_INLINE_FILE_BYTES) { return Ok(None); }
            let copied = end.min(logical_bytes).checked_sub(offset).ok_or(WorkError::Overflow)?;
            charge_copy(accounting, copied)?;
            let zeros = [0_u8; MAXIMUM_INLINE_FILE_BYTES];
            data.replace_range(
                usize::try_from(offset).map_err(|_| WorkError::Overflow)?,
                zeros.get(..usize::try_from(copied).map_err(|_| WorkError::Overflow)?).ok_or(WorkError::Overflow)?,
                usize::try_from(logical_bytes).map_err(|_| WorkError::Overflow)?,
            )?
        }
        RegularMutation::Preallocate { offset, length, keep_size } => {
            let end = offset.checked_add(length).ok_or_else(|| regular_error(RegularMutationError::RangeOverflow))?;
            if keep_size && end > current { return Err(regular_error(RegularMutationError::KeepSizeBeyondEofUnsupported)); }
            if end > current { return Ok(None); }
            data
        }
        RegularMutation::Write { offset, length, content, content_offset } => {
            let end = offset.checked_add(length).ok_or_else(|| regular_error(RegularMutationError::RangeOverflow))?;
            if offset > current || end > usize_to_u64(MAXIMUM_INLINE_FILE_BYTES) { return Ok(None); }
            // Admit both the blob output and the inline replacement before source access.
            accounting.admit(WorkCounters {
                bytes_copied: length.checked_mul(2).ok_or(WorkError::Overflow)?,
                ..WorkCounters::UNCHARGED
            })?;
            let read = super::blob::read_blob_range_for_copy_with_accounting(
                store, content, ByteRange { offset: content_offset, length },
                DecodeLimits::for_volume(config), accounting, cancellation,
            ).await?;
            charge_copy(accounting, length)?;
            let result = data.replace_range(
                usize::try_from(offset).map_err(|_| WorkError::Overflow)?, &read.bytes,
                usize::try_from(current.max(end)).map_err(|_| WorkError::Overflow)?,
            );
            let bytes = read.logical_owned_bytes;
            drop(read);
            super::file_read::release_standalone_read_bytes(bytes, accounting)?;
            result?
        }
    };
    Ok(Some(FilePayload::InlineRegular(data)))
}

async fn sparse_storage<S: AsyncObjectStore>(
    store: &S, storage: RegularStorage, config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<(u64, ObjectId), FsError> {
    match storage {
        RegularStorage::Sparse { logical_bytes, extents } => Ok((logical_bytes, extents)),
        RegularStorage::Inline(data) => {
            let promoted = promote_inline(store, data, config, accounting, cancellation).await?;
            Ok((promoted.logical_bytes, promoted.extents))
        }
    }
}

async fn promote_inline<S: AsyncObjectStore>(
    store: &S, data: InlineFileData, config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<PromotedRegularReceipt, FsError> {
    let logical_bytes = usize_to_u64(data.as_bytes().len());
    if logical_bytes != 0 {
        if config.limits.maximum_directory_page_entries < 2 || config.limits.maximum_object_bytes < 31 + 2 * 48 {
            return Err(BlobBuildError::InvalidOptions.into());
        }
        accounting.admit(WorkCounters { source_bytes_read: logical_bytes, ..WorkCounters::UNCHARGED })?;
    }
    let mut extents = Owned::<Vec<Extent>>::allocate(usize::from(logical_bytes != 0), accounting)?;
    if logical_bytes != 0 {
        // The ordinary promotion's chunk bound is MAXIMUM_INLINE_FILE_BYTES:
        // exactly one immutable chunk and one canonical leaf index are produced.
        accounting.charge(WorkCounters { source_bytes_read: logical_bytes, ..WorkCounters::UNCHARGED })?;
        let mut chunk = Owned::<Vec<u8>>::allocate(data.as_bytes().len(), accounting)?;
        chunk.value.extend_from_slice(data.as_bytes());
        let chunk = super::extent_mutation::put_encoded_with_accounting(
            store, ObjectKind::BlobChunk, chunk, accounting, cancellation,
        ).await?;
        accounting.admit(WorkCounters { page_writes: 1, ..WorkCounters::UNCHARGED })?;
        let mut items = Owned::<Vec<super::BlobChunkRef>>::allocate(1, accounting)?;
        items.value.push(super::BlobChunkRef { first_offset: 0, end_offset: logical_bytes, chunk });
        let Owned { value, allocation, bytes } = items;
        let page = Owned::new(super::BlobPage { first_offset: 0, end_offset: logical_bytes, node: super::BlobNode::Leaf(value) }, allocation, bytes);
        let encoded_result = (|| {
            let length = usize_to_u64(super::blob::blob_page_encoded_length(
                &page.value, config.limits.maximum_directory_page_entries,
            )?);
            if length > config.limits.maximum_object_bytes {
                return Err(CanonicalDecodeError::ObjectTooLarge {
                    observed: length, maximum: config.limits.maximum_object_bytes,
                }.into());
            }
            super::codec::encode_owned(accounting, |observe, reserve_error| {
                super::blob::encode_blob_page(
                    &page.value, config.limits.maximum_directory_page_entries,
                    observe, reserve_error,
                )
            })
        })();
        page.retire(accounting)?;
        let (value, allocation) = encoded_result?;
        let bytes = usize_to_u64(value.capacity());
        let encoded = Owned::new(value, allocation, bytes);
        let blob = super::extent_mutation::put_encoded_with_accounting(
            store, ObjectKind::Blob, encoded, accounting, cancellation,
        ).await?;
        extents.value.push(Extent { offset: 0, length: logical_bytes,
            kind: ExtentKind::Content { object: blob, object_offset: 0 } });
    }
    let Owned { value, allocation, bytes } = extents;
    let page = Owned::new(ExtentPage::Leaf(value), allocation, bytes);
    let result = super::extent_mutation::put_extent_page_with_accounting(
        store, &page.value, DecodeLimits::for_volume(config), accounting, cancellation,
    ).await;
    page.retire(accounting)?;
    Ok(PromotedRegularReceipt { logical_bytes, extents: result? })
}

async fn apply_sparse<S: AsyncObjectStore>(
    store: &S, payload: FilePayload, mutation: RegularMutation, config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<RegularMutationReceipt, FsError> {
    let FilePayload::Regular { logical_bytes, extents } = payload else {
        return Err(regular_error(RegularMutationError::NotRegular));
    };
    if let RegularMutation::ZeroRange { offset, extend: false, .. } = mutation
        && offset >= logical_bytes {
        return Ok(RegularMutationReceipt { payload, work: accounting.spent() });
    }
    let mut demotion_known_impossible = false;
    if let RegularMutation::Resize { logical_bytes: target } = mutation
        && target <= usize_to_u64(MAXIMUM_INLINE_FILE_BYTES) && target <= logical_bytes {
        if let Some(data) = try_demote_sparse_async(store, extents, logical_bytes, target, config, accounting, cancellation).await? {
            return Ok(RegularMutationReceipt { payload: FilePayload::InlineRegular(data), work: accounting.spent() });
        }
        demotion_known_impossible = true;
    }
    let extent_mutation = match mutation {
        RegularMutation::Write { offset, length, content, content_offset } => ExtentMutation::Replace {
            offset, length, kind: ExtentKind::Content { object: content, object_offset: content_offset }, extend: true,
        },
        RegularMutation::Resize { logical_bytes } => ExtentMutation::Resize { logical_bytes },
        RegularMutation::ZeroRange { offset, length, allocated, extend } => ExtentMutation::Replace {
            offset, length: if extend { length } else { length.min(logical_bytes - offset) },
            kind: if allocated { ExtentKind::AllocatedZero } else { ExtentKind::Hole }, extend,
        },
        RegularMutation::Preallocate { offset, length, keep_size } => {
            return apply_sparse_preallocation(store, logical_bytes, extents, offset, length, keep_size, config, accounting, cancellation).await;
        }
    };
    let receipt = super::extent_mutation::apply_extent_mutations_with_accounting(
        store, extents, logical_bytes, &[extent_mutation], 1, DecodeLimits::for_volume(config),
        accounting, cancellation,
    ).await?;
    if !demotion_known_impossible && receipt.logical_bytes <= usize_to_u64(MAXIMUM_INLINE_FILE_BYTES)
        && let Some(data) = try_demote_sparse_async(store, receipt.root, receipt.logical_bytes, receipt.logical_bytes,
            config, accounting, cancellation).await? {
        return Ok(RegularMutationReceipt { payload: FilePayload::InlineRegular(data), work: accounting.spent() });
    }
    Ok(RegularMutationReceipt {
        payload: FilePayload::Regular { logical_bytes: receipt.logical_bytes, extents: receipt.root },
        work: accounting.spent(),
    })
}

async fn apply_sparse_preallocation<S: AsyncObjectStore>(
    store: &S, logical_bytes: u64, extents: ObjectId, offset: u64, length: u64, keep_size: bool,
    config: VolumeConfig, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<RegularMutationReceipt, FsError> {
    let end = offset.checked_add(length).ok_or_else(|| regular_error(RegularMutationError::RangeOverflow))?;
    if keep_size && end > logical_bytes { return Err(regular_error(RegularMutationError::KeepSizeBeyondEofUnsupported)); }
    let existing_end = end.min(logical_bytes);
    let existing_length = existing_end.saturating_sub(offset.min(existing_end));
    let spans = if existing_length == 0 { Owned::allocate(0, accounting)? } else {
        let (plan, allocation) = super::range::plan_extent_range_with_accounting(store, ExtentRangeRequest {
            root: extents, file_size: logical_bytes, range: ByteRange { offset, length: existing_length },
            maximum_spans: config.limits.maximum_mutations_per_batch, limits: DecodeLimits::for_volume(config),
            budget: WorkBudget::UNBOUNDED,
        }, accounting, cancellation).await?;
        Owned::new(plan.spans, allocation, plan.retained_allocation_bytes)
    };
    accounting.charge(WorkCounters { items_examined: usize_to_u64(spans.value.len()), ..WorkCounters::UNCHARGED })?;
    let holes = spans.value.iter().filter(|span| matches!(span.kind, ExtentKind::Hole)).count();
    let grows = end > logical_bytes;
    let extension_offset = logical_bytes.max(offset);
    let merge = grows && spans.value.last().is_some_and(|span|
        matches!(span.kind, ExtentKind::Hole) && span.offset.checked_add(span.length) == Some(extension_offset));
    let count = holes.checked_add(usize::from(grows)).and_then(|count| count.checked_sub(usize::from(merge))).ok_or(WorkError::Overflow)?;
    if u32::try_from(count).unwrap_or(u32::MAX) > config.limits.maximum_mutations_per_batch {
        return Err(regular_error(RegularMutationError::Extent(ExtentMutationError::TooManyMutations)));
    }
    let mut mutations = Owned::<Vec<ExtentMutation>>::allocate(count, accounting)?;
    for span in spans.value.iter().filter(|span| matches!(span.kind, ExtentKind::Hole)) {
        let extends = merge && span.offset.checked_add(span.length) == Some(extension_offset);
        mutations.value.push(ExtentMutation::Replace {
            offset: span.offset, length: if extends { end - span.offset } else { span.length },
            kind: ExtentKind::AllocatedZero, extend: extends,
        });
    }
    if grows && !merge {
        mutations.value.push(ExtentMutation::Replace { offset: extension_offset,
            length: end - extension_offset, kind: ExtentKind::AllocatedZero, extend: true });
    }
    spans.retire(accounting)?;
    let result = if mutations.value.is_empty() {
        Ok(FilePayload::Regular { logical_bytes, extents })
    } else {
        super::extent_mutation::apply_extent_mutations_with_accounting(
            store, extents, logical_bytes, &mutations.value, config.limits.maximum_mutations_per_batch,
            DecodeLimits::for_volume(config), accounting, cancellation,
        ).await.map(|receipt| FilePayload::Regular { logical_bytes: receipt.logical_bytes, extents: receipt.root })
    };
    mutations.retire(accounting)?;
    Ok(RegularMutationReceipt { payload: result?, work: accounting.spent() })
}

async fn try_demote_sparse_async<S: AsyncObjectStore>(
    store: &S, root: ObjectId, file_size: u64, target_size: u64, config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<Option<InlineFileData>, FsError> {
    if target_size == 0 { return Ok(Some(InlineFileData::new(&[])?)); }
    let (plan, allocation) = super::range::plan_extent_range_with_accounting(store, ExtentRangeRequest {
        root, file_size, range: ByteRange { offset: 0, length: target_size },
        maximum_spans: config.limits.maximum_mutations_per_batch,
        limits: DecodeLimits::for_volume(config), budget: WorkBudget::UNBOUNDED,
    }, accounting, cancellation).await?;
    let bytes = plan.retained_allocation_bytes;
    let plan = Owned::new(plan, allocation, bytes);
    if plan.value.spans.iter().any(|span| !matches!(span.kind, ExtentKind::Content { .. })) {
        plan.retire(accounting)?;
        return Ok(None);
    }
    let target = usize::try_from(target_size).map_err(|_| WorkError::Overflow)?;
    let mut data = InlineFileData::new(&[])?;
    for span in &plan.value.spans {
        let ExtentKind::Content { object, object_offset } = span.kind else { unreachable!("content-only plan"); };
        accounting.admit(WorkCounters {
            bytes_copied: span.length.checked_mul(2).ok_or(WorkError::Overflow)?,
            ..WorkCounters::UNCHARGED
        })?;
        let read = super::blob::read_blob_range_for_copy_with_accounting(
            store, object, ByteRange { offset: object_offset, length: span.length },
            DecodeLimits::for_volume(config), accounting, cancellation,
        ).await?;
        charge_copy(accounting, span.length)?;
        let copied = data.replace_range(usize::try_from(span.offset).map_err(|_| WorkError::Overflow)?, &read.bytes, target);
        let bytes = read.logical_owned_bytes;
        drop(read);
        super::file_read::release_standalone_read_bytes(bytes, accounting)?;
        data = copied?;
    }
    plan.retire(accounting)?;
    Ok(Some(data))
}

fn charge_copy(accounting: &mut KernelAccounting<'_>, bytes: u64) -> Result<(), FsError> {
    accounting.charge(WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED })
}

fn regular_error(error: RegularMutationError) -> FsError { FsError::DetachedMutation(error) }

pub(super) fn project_regular_error(error: FsError) -> RegularMutationError {
    match error {
        FsError::DetachedMutation(error) => error,
        FsError::Work(error) => error.into(),
        FsError::Decode(error) => error.into(),
        FsError::BlobBuild(error) => error.into(),
        FsError::BlobRead(error) => error.into(),
        FsError::InlineFile(error) => error.into(),
        FsError::FileRead(super::FileRangeReadError::Extent(error)) => error.into(),
        FsError::FileRead(super::FileRangeReadError::Blob(error)) => error.into(),
        #[cfg(not(target_arch = "wasm32"))]
        FsError::NativeKernel(error) => ObjectStoreError::Native(error).into(),
        FsError::Cancelled(_) => ObjectStoreError::Cancelled.into(),
        error => RegularMutationError::Context(Box::new(error)),
    }
}

pub(super) fn project_clone_error(error: FsError) -> ExtentCloneError {
    match error {
        FsError::DetachedMutation(RegularMutationError::Clone(error)) => error,
        FsError::DetachedMutation(RegularMutationError::Extent(error)) => error.into(),
        FsError::DetachedMutation(RegularMutationError::Range(error))
        | FsError::FileRead(super::FileRangeReadError::Extent(error)) => error.into(),
        FsError::Work(error) => error.into(),
        FsError::Decode(CanonicalDecodeError::AllocationFailed) => ExtentCloneError::AllocationFailed,
        FsError::Decode(error) => ExtentMutationError::Decode(error).into(),
        FsError::Cancelled(_) => ExtentMutationError::Storage(ObjectStoreError::Cancelled).into(),
        FsError::BlobRead(BlobReadError::Storage(error)) => ExtentMutationError::Storage(error).into(),
        #[cfg(not(target_arch = "wasm32"))]
        FsError::NativeKernel(error) => ExtentMutationError::Storage(ObjectStoreError::Native(error)).into(),
        error => ExtentMutationError::Context(Box::new(error)).into(),
    }
}

enum CloneStage { Source, Compile, Destination }

fn clone_stage_error(error: FsError, stage: CloneStage) -> FsError {
    #[cfg(not(target_arch = "wasm32"))]
    if matches!(&error, FsError::NativeKernel(_)) { return error; }
    let error = match stage {
        CloneStage::Compile => match error {
            FsError::Work(error) => error.into(),
            FsError::Decode(CanonicalDecodeError::AllocationFailed) => ExtentCloneError::AllocationFailed,
            FsError::DetachedMutation(RegularMutationError::Clone(error)) => error,
            error => return error,
        },
        CloneStage::Destination => {
            let error = match error {
                FsError::DetachedMutation(RegularMutationError::Extent(error)) => error,
                FsError::Work(error) => error.into(),
                FsError::Decode(error) => error.into(),
                FsError::BlobRead(BlobReadError::Storage(error)) => error.into(),
                FsError::FileRead(super::FileRangeReadError::Extent(ExtentReadError::Decode(error))) => error.into(),
                FsError::FileRead(super::FileRangeReadError::Extent(ExtentReadError::Work(error))) => error.into(),
                FsError::FileRead(super::FileRangeReadError::Extent(ExtentReadError::Storage(error))) => error.into(),
                FsError::Cancelled(_) => ObjectStoreError::Cancelled.into(),
                error => return error,
            };
            ExtentCloneError::Destination(error)
        },
        CloneStage::Source => {
            let error = match error {
                FsError::FileRead(super::FileRangeReadError::Extent(error)) => error,
                FsError::Work(error) => error.into(),
                FsError::Decode(error) => error.into(),
                FsError::BlobRead(BlobReadError::Storage(error)) => error.into(),
                FsError::Cancelled(_) => ObjectStoreError::Cancelled.into(),
                error => return error,
            };
            ExtentCloneError::Source(error)
        }
    };
    regular_error(RegularMutationError::Clone(error))
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/regular_mutation.rs"]
mod tests;
