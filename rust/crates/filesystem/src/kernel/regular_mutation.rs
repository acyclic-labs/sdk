//! Ordered regular-file mutation across canonical inline and sparse payloads.

use super::allocation::{AllocationError, AllocationLedger};
use super::extent::extent_page_encoded_length;
use super::{
    BlobBuildError, BlobBuildOptions, BlobReadError, CanonicalDecodeError, DecodeLimits, Extent,
    ExtentCloneError, ExtentCloneRequest, ExtentKind, ExtentMutation, ExtentMutationError,
    ExtentMutationOptions, ExtentPage, ExtentRangeRequest, ExtentReadError, FilePayload,
    InlineFileData, InlineFileDataError, MAXIMUM_INLINE_FILE_BYTES, apply_extent_mutations_async,
    build_blob_async, clone_extent_range_async, encode_extent_page, plan_extent_range_async,
    read_blob_range_async,
};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::model::VolumeConfig;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{
    ByteRange, OBJECT_DIGEST_ENVELOPE_BYTES, ObjectId, ObjectKind, ObjectStoreError, object_digest,
};
use bytes::Bytes;
use std::io::Cursor;
use std::mem::size_of;
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
    fn validate(payload: FilePayload) -> Result<Self, RegularMutationFailure> {
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
            | FilePayload::ReparsePoint { .. } => Err(OperationFailure::before_work(
                RegularMutationError::NotRegular,
            )),
        }
    }

    fn logical_bytes(self) -> u64 {
        match self {
            Self::Inline(data) => u64::try_from(data.as_bytes().len()).unwrap_or(u64::MAX),
            Self::Sparse { logical_bytes, .. } => logical_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PromotedRegularReceipt {
    logical_bytes: u64,
    extents: ObjectId,
    work: WorkCounters,
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
}

pub(crate) async fn apply_regular_mutation_async<S: AsyncObjectStore>(
    store: &S,
    payload: FilePayload,
    mutation: RegularMutation,
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<RegularMutationReceipt, RegularMutationFailure> {
    cancellation
        .check()
        .map_err(|_| OperationFailure::before_work(cancelled()))?;
    validate_mutation(mutation)?;
    if !config.sparse_files {
        let current = regular_logical_bytes(payload)?;
        let requires_sparse = match mutation {
            RegularMutation::Write { offset, .. } => offset > current,
            RegularMutation::Resize { logical_bytes } => logical_bytes > current,
            RegularMutation::ZeroRange { .. } | RegularMutation::Preallocate { .. } => true,
        };
        if requires_sparse {
            return Err(OperationFailure::before_work(
                RegularMutationError::SparseSemanticsDisabled,
            ));
        }
    }
    if let FilePayload::InlineRegular(data) = payload {
        if let Some(receipt) =
            try_inline(store, data, mutation, config, budget, cancellation).await?
        {
            return Ok(receipt);
        }
        let promoted = promote_inline(store, data, config, budget, cancellation).await?;
        return apply_sparse(
            store,
            FilePayload::Regular {
                logical_bytes: promoted.logical_bytes,
                extents: promoted.extents,
            },
            mutation,
            config,
            budget,
            promoted.work,
            cancellation,
        )
        .await;
    }
    apply_sparse(
        store,
        payload,
        mutation,
        config,
        budget,
        WorkCounters::default(),
        cancellation,
    )
    .await
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(crate) async fn apply_regular_clone_async<S: AsyncObjectStore>(
    store: &S,
    source: FilePayload,
    source_offset: u64,
    destination: FilePayload,
    destination_offset: u64,
    length: u64,
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<RegularCloneReceipt, RegularMutationFailure> {
    cancellation
        .check()
        .map_err(|_| OperationFailure::before_work(cancelled()))?;
    if !config.sparse_files {
        return Err(OperationFailure::before_work(
            RegularMutationError::SparseSemanticsDisabled,
        ));
    }
    let source = RegularStorage::validate(source)?;
    let destination = RegularStorage::validate(destination)?;
    let source_bytes = source.logical_bytes();
    let destination_bytes = destination.logical_bytes();
    let source_end = source_offset
        .checked_add(length)
        .ok_or_else(|| OperationFailure::before_work(RegularMutationError::RangeOverflow))?;
    let destination_end = destination_offset
        .checked_add(length)
        .ok_or_else(|| OperationFailure::before_work(RegularMutationError::RangeOverflow))?;
    if length == 0 || source_end > source_bytes {
        return Err(OperationFailure::before_work(
            RegularMutationError::SourceRangeOutOfBounds,
        ));
    }

    if let (RegularStorage::Inline(source_data), RegularStorage::Inline(destination_data)) =
        (source, destination)
        && destination_offset <= destination_bytes
        && destination_end <= u64::try_from(MAXIMUM_INLINE_FILE_BYTES).unwrap_or(u64::MAX)
    {
        let source_start = usize::try_from(source_offset)
            .map_err(|_| OperationFailure::before_work(RegularMutationError::RangeOverflow))?;
        let source_limit = usize::try_from(source_end)
            .map_err(|_| OperationFailure::before_work(RegularMutationError::RangeOverflow))?;
        let work = WorkCounters {
            bytes_copied: length,
            ..WorkCounters::default()
        };
        work.verify(budget)
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        let destination = destination_data
            .replace_range(
                usize::try_from(destination_offset).map_err(|_| {
                    OperationFailure::before_work(RegularMutationError::RangeOverflow)
                })?,
                &source_data.as_bytes()[source_start..source_limit],
                usize::try_from(destination_bytes.max(destination_end)).map_err(|_| {
                    OperationFailure::before_work(RegularMutationError::RangeOverflow)
                })?,
            )
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        return Ok(RegularCloneReceipt {
            destination: FilePayload::InlineRegular(destination),
            work,
        });
    }

    let equal_payloads = source == destination;
    let mut work = WorkCounters::default();
    let (source_logical_bytes, source_root) = match source {
        RegularStorage::Inline(data) => {
            let receipt =
                promote_inline(store, data, config, remaining(work, budget)?, cancellation)
                    .await
                    .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
            work = add(work, receipt.work)?;
            (receipt.logical_bytes, receipt.extents)
        }
        RegularStorage::Sparse {
            logical_bytes,
            extents,
        } => (logical_bytes, extents),
    };
    let (destination_logical_bytes, destination_root) = if equal_payloads {
        (source_logical_bytes, source_root)
    } else {
        match destination {
            RegularStorage::Inline(data) => {
                let receipt =
                    promote_inline(store, data, config, remaining(work, budget)?, cancellation)
                        .await
                        .map_err(|failure| {
                            failure.map_with_prior_work(work, std::convert::identity)
                        })?;
                work = add(work, receipt.work)?;
                (receipt.logical_bytes, receipt.extents)
            }
            RegularStorage::Sparse {
                logical_bytes,
                extents,
            } => (logical_bytes, extents),
        }
    };
    let receipt = clone_extent_range_async(
        store,
        ExtentCloneRequest {
            source_root,
            source_logical_bytes,
            source_range: ByteRange {
                offset: source_offset,
                length,
            },
            destination_root,
            destination_logical_bytes,
            destination_offset,
            maximum_spans: config.limits.maximum_mutations_per_batch,
            maximum_mutations: config.limits.maximum_mutations_per_batch,
        },
        decode_limits(config),
        remaining(work, budget)?,
        cancellation,
    )
    .await
    .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
    work = add(work, receipt.work)?;
    if receipt.logical_bytes <= u64::try_from(MAXIMUM_INLINE_FILE_BYTES).unwrap_or(u64::MAX) {
        let attempt = try_demote_sparse_async(
            store,
            receipt.root,
            receipt.logical_bytes,
            receipt.logical_bytes,
            config,
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        work = add(work, attempt.work)?;
        if let Some(data) = attempt.data {
            return Ok(RegularCloneReceipt {
                destination: FilePayload::InlineRegular(data),
                work,
            });
        }
    }
    Ok(RegularCloneReceipt {
        destination: FilePayload::Regular {
            logical_bytes: receipt.logical_bytes,
            extents: receipt.root,
        },
        work,
    })
}

fn regular_logical_bytes(payload: FilePayload) -> Result<u64, RegularMutationFailure> {
    match payload {
        FilePayload::InlineRegular(data) => {
            Ok(crate::foundation::usize_to_u64(data.as_bytes().len()))
        }
        FilePayload::Regular { logical_bytes, .. } => Ok(logical_bytes),
        FilePayload::Directory { .. }
        | FilePayload::SymbolicLink { .. }
        | FilePayload::Empty
        | FilePayload::Device { .. }
        | FilePayload::ReparsePoint { .. } => Err(OperationFailure::before_work(
            RegularMutationError::NotRegular,
        )),
    }
}

fn validate_mutation(mutation: RegularMutation) -> Result<(), RegularMutationFailure> {
    match mutation {
        RegularMutation::Write {
            offset,
            length,
            content,
            content_offset,
        } => {
            if content.kind != ObjectKind::Blob {
                return Err(OperationFailure::before_work(
                    RegularMutationError::WrongContentKind,
                ));
            }
            if length == 0
                || offset.checked_add(length).is_none()
                || content_offset.checked_add(length).is_none()
            {
                return Err(OperationFailure::before_work(
                    RegularMutationError::RangeOverflow,
                ));
            }
        }
        RegularMutation::ZeroRange { offset, length, .. }
        | RegularMutation::Preallocate { offset, length, .. } => {
            if length == 0 || offset.checked_add(length).is_none() {
                return Err(OperationFailure::before_work(
                    RegularMutationError::RangeOverflow,
                ));
            }
        }
        RegularMutation::Resize { .. } => {}
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn try_inline<S: AsyncObjectStore>(
    store: &S,
    data: InlineFileData,
    mutation: RegularMutation,
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<Option<RegularMutationReceipt>, RegularMutationFailure> {
    match mutation {
        RegularMutation::Resize { logical_bytes } => {
            let current = u64::try_from(data.as_bytes().len()).unwrap_or(u64::MAX);
            if logical_bytes > current {
                return Ok(None);
            }
            let length = usize::try_from(logical_bytes)
                .map_err(|_| OperationFailure::before_work(RegularMutationError::RangeOverflow))?;
            Ok(Some(RegularMutationReceipt {
                payload: FilePayload::InlineRegular(
                    data.truncate(length)
                        .map_err(|error| OperationFailure::before_work(error.into()))?,
                ),
                work: WorkCounters::default(),
            }))
        }
        RegularMutation::ZeroRange {
            offset,
            length,
            allocated,
            extend,
        } => {
            let current = u64::try_from(data.as_bytes().len()).unwrap_or(u64::MAX);
            let requested_end = offset.checked_add(length).ok_or_else(|| {
                OperationFailure::before_work(RegularMutationError::RangeOverflow)
            })?;
            if !extend && offset >= current {
                return Ok(Some(RegularMutationReceipt {
                    payload: FilePayload::InlineRegular(data),
                    work: WorkCounters::default(),
                }));
            }
            if !allocated {
                return Ok(None);
            }
            if extend && offset > current {
                return Ok(None);
            }
            let logical_bytes = if extend {
                requested_end.max(current)
            } else {
                current
            };
            if logical_bytes > u64::try_from(MAXIMUM_INLINE_FILE_BYTES).unwrap_or(u64::MAX) {
                return Ok(None);
            }
            let end = requested_end.min(logical_bytes);
            let copied = end.checked_sub(offset).ok_or_else(|| {
                OperationFailure::before_work(RegularMutationError::RangeOverflow)
            })?;
            let work = WorkCounters {
                bytes_copied: copied,
                ..WorkCounters::default()
            };
            work.verify(budget)
                .map_err(|error| OperationFailure::before_work(error.into()))?;
            let zeros = [0_u8; MAXIMUM_INLINE_FILE_BYTES];
            let replacement = &zeros[..usize::try_from(copied)
                .map_err(|_| OperationFailure::before_work(RegularMutationError::RangeOverflow))?];
            let payload = data
                .replace_range(
                    usize::try_from(offset).map_err(|_| {
                        OperationFailure::before_work(RegularMutationError::RangeOverflow)
                    })?,
                    replacement,
                    usize::try_from(logical_bytes).map_err(|_| {
                        OperationFailure::before_work(RegularMutationError::RangeOverflow)
                    })?,
                )
                .map_err(|error| OperationFailure::new(error.into(), work))?;
            Ok(Some(RegularMutationReceipt {
                payload: FilePayload::InlineRegular(payload),
                work,
            }))
        }
        RegularMutation::Preallocate {
            offset,
            length,
            keep_size,
        } => {
            let current = u64::try_from(data.as_bytes().len()).unwrap_or(u64::MAX);
            let end = offset.checked_add(length).ok_or_else(|| {
                OperationFailure::before_work(RegularMutationError::RangeOverflow)
            })?;
            if keep_size && end > current {
                return Err(OperationFailure::before_work(
                    RegularMutationError::KeepSizeBeyondEofUnsupported,
                ));
            }
            if end <= current {
                return Ok(Some(RegularMutationReceipt {
                    payload: FilePayload::InlineRegular(data),
                    work: WorkCounters::default(),
                }));
            }
            Ok(None)
        }
        RegularMutation::Write {
            offset,
            length,
            content,
            content_offset,
        } => {
            let current = u64::try_from(data.as_bytes().len()).unwrap_or(u64::MAX);
            let end = offset.checked_add(length).ok_or_else(|| {
                OperationFailure::before_work(RegularMutationError::RangeOverflow)
            })?;
            if offset > current
                || end > u64::try_from(MAXIMUM_INLINE_FILE_BYTES).unwrap_or(u64::MAX)
            {
                return Ok(None);
            }
            let read = read_blob_range_async(
                store,
                content,
                ByteRange {
                    offset: content_offset,
                    length,
                },
                decode_limits(config),
                budget,
                cancellation,
            )
            .await
            .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
            let mut work = read.work;
            let copy = WorkCounters {
                bytes_copied: length,
                ..WorkCounters::default()
            };
            work = add(work, copy)?;
            work.verify(budget)
                .map_err(|error| OperationFailure::new(error.into(), read.work))?;
            let logical_bytes = current.max(end);
            let payload = data
                .replace_range(
                    usize::try_from(offset).map_err(|_| {
                        OperationFailure::new(RegularMutationError::RangeOverflow, work)
                    })?,
                    &read.bytes,
                    usize::try_from(logical_bytes).map_err(|_| {
                        OperationFailure::new(RegularMutationError::RangeOverflow, work)
                    })?,
                )
                .map_err(|error| OperationFailure::new(error.into(), work))?;
            Ok(Some(RegularMutationReceipt {
                payload: FilePayload::InlineRegular(payload),
                work,
            }))
        }
    }
}

async fn promote_inline<S: AsyncObjectStore>(
    store: &S,
    data: InlineFileData,
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<PromotedRegularReceipt, RegularMutationFailure> {
    let logical_bytes = u64::try_from(data.as_bytes().len()).unwrap_or(u64::MAX);
    let mut allocations = AllocationLedger::default();
    let (extents, mut work, extent_bytes) = if logical_bytes == 0 {
        (Vec::new(), WorkCounters::default(), 0)
    } else {
        let mut source = Cursor::new(data.as_bytes());
        let blob = build_blob_async(
            store,
            &mut source,
            BlobBuildOptions {
                chunk_bytes: u32::try_from(MAXIMUM_INLINE_FILE_BYTES).unwrap_or(u32::MAX),
                page_items: config.limits.maximum_directory_page_entries,
                page_bytes: u32::try_from(config.limits.maximum_object_bytes).unwrap_or(u32::MAX),
                maximum_blob_bytes: logical_bytes,
            },
            budget,
            cancellation,
        )
        .await
        .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
        let mut work = blob.work;
        let extent_bytes = allocations
            .claim_elements::<Extent>(1, &mut work, budget)
            .map_err(|error| allocation_error(error, work))?;
        let mut extents = Vec::new();
        if extents.try_reserve_exact(1).is_err() {
            allocations
                .release(extent_bytes)
                .map_err(|error| allocation_error(error, work))?;
            return Err(OperationFailure::new(
                RegularMutationError::AllocationFailed,
                work,
            ));
        }
        extents.push(Extent {
            offset: 0,
            length: logical_bytes,
            kind: ExtentKind::Content {
                object: blob.root,
                object_offset: 0,
            },
        });
        (extents, work, extent_bytes)
    };
    let page = ExtentPage::Leaf(extents);
    let mut page_budget = remaining(work, budget)?;
    page_budget.peak_allocation_bytes = page_budget
        .peak_allocation_bytes
        .checked_sub(allocations.live_bytes())
        .ok_or_else(|| OperationFailure::new(RegularMutationError::RangeOverflow, work))?;
    let written = put_extent_page_async(
        store,
        &page,
        decode_limits(config),
        page_budget,
        cancellation,
    )
    .await
    .map_err(|failure| {
        simultaneous_failure(work, *failure.work, allocations.live_bytes(), failure.error)
    })?;
    work = simultaneous(work, written.work, allocations.live_bytes(), budget)?;
    allocations
        .release(extent_bytes)
        .map_err(|error| allocation_error(error, work))?;
    Ok(PromotedRegularReceipt {
        logical_bytes,
        extents: written.object,
        work,
    })
}

#[allow(clippy::too_many_lines)]
async fn apply_sparse<S: AsyncObjectStore>(
    store: &S,
    payload: FilePayload,
    mutation: RegularMutation,
    config: VolumeConfig,
    budget: WorkBudget,
    prior: WorkCounters,
    cancellation: &CancellationToken,
) -> Result<RegularMutationReceipt, RegularMutationFailure> {
    let FilePayload::Regular {
        logical_bytes,
        extents,
    } = payload
    else {
        return Err(OperationFailure::new(
            RegularMutationError::NotRegular,
            prior,
        ));
    };
    let mut prior = prior;
    let mut demotion_known_impossible = false;
    if let RegularMutation::ZeroRange {
        offset,
        extend: false,
        ..
    } = mutation
        && offset >= logical_bytes
    {
        return Ok(RegularMutationReceipt {
            payload,
            work: prior,
        });
    }
    if let RegularMutation::Resize {
        logical_bytes: target,
    } = mutation
        && target <= u64::try_from(MAXIMUM_INLINE_FILE_BYTES).unwrap_or(u64::MAX)
        && target <= logical_bytes
    {
        let attempt = try_demote_sparse_async(
            store,
            extents,
            logical_bytes,
            target,
            config,
            remaining(prior, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(prior, std::convert::identity))?;
        prior = add(prior, attempt.work)?;
        if let Some(data) = attempt.data {
            return Ok(RegularMutationReceipt {
                payload: FilePayload::InlineRegular(data),
                work: prior,
            });
        }
        demotion_known_impossible = true;
    }
    let extent_mutation = match mutation {
        RegularMutation::Write {
            offset,
            length,
            content,
            content_offset,
        } => ExtentMutation::Replace {
            offset,
            length,
            kind: ExtentKind::Content {
                object: content,
                object_offset: content_offset,
            },
            extend: true,
        },
        RegularMutation::Resize { logical_bytes } => ExtentMutation::Resize { logical_bytes },
        RegularMutation::ZeroRange {
            offset,
            mut length,
            allocated,
            extend,
        } => {
            if !extend {
                length = length.min(logical_bytes - offset);
            }
            ExtentMutation::Replace {
                offset,
                length,
                kind: if allocated {
                    ExtentKind::AllocatedZero
                } else {
                    ExtentKind::Hole
                },
                extend,
            }
        }
        RegularMutation::Preallocate {
            offset,
            length,
            keep_size,
        } => {
            return apply_sparse_preallocation(
                store,
                logical_bytes,
                extents,
                offset,
                length,
                keep_size,
                config,
                budget,
                prior,
                cancellation,
            )
            .await;
        }
    };
    let receipt = apply_extent_mutations_async(
        store,
        extents,
        logical_bytes,
        &[extent_mutation],
        ExtentMutationOptions {
            maximum_mutations: 1,
            limits: decode_limits(config),
            budget: remaining(prior, budget)?,
        },
        cancellation,
    )
    .await
    .map_err(|failure| failure.map_with_prior_work(prior, Into::into))?;
    let work = add(prior, receipt.work)?;
    if !demotion_known_impossible
        && receipt.logical_bytes <= u64::try_from(MAXIMUM_INLINE_FILE_BYTES).unwrap_or(u64::MAX)
    {
        let attempt = try_demote_sparse_async(
            store,
            receipt.root,
            receipt.logical_bytes,
            receipt.logical_bytes,
            config,
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, std::convert::identity))?;
        let work = add(work, attempt.work)?;
        if let Some(data) = attempt.data {
            return Ok(RegularMutationReceipt {
                payload: FilePayload::InlineRegular(data),
                work,
            });
        }
        return Ok(RegularMutationReceipt {
            payload: FilePayload::Regular {
                logical_bytes: receipt.logical_bytes,
                extents: receipt.root,
            },
            work,
        });
    }
    Ok(RegularMutationReceipt {
        payload: FilePayload::Regular {
            logical_bytes: receipt.logical_bytes,
            extents: receipt.root,
        },
        work,
    })
}

struct InlineDemotionAttempt {
    data: Option<InlineFileData>,
    work: WorkCounters,
}

struct SparsePreallocationPlan {
    mutations: Vec<ExtentMutation>,
    mutation_bytes: u64,
    work: WorkCounters,
}

#[allow(clippy::too_many_arguments)]
async fn apply_sparse_preallocation<S: AsyncObjectStore>(
    store: &S,
    logical_bytes: u64,
    extents: ObjectId,
    offset: u64,
    length: u64,
    keep_size: bool,
    config: VolumeConfig,
    budget: WorkBudget,
    prior: WorkCounters,
    cancellation: &CancellationToken,
) -> Result<RegularMutationReceipt, RegularMutationFailure> {
    let end = offset
        .checked_add(length)
        .ok_or_else(|| OperationFailure::new(RegularMutationError::RangeOverflow, prior))?;
    if keep_size && end > logical_bytes {
        return Err(OperationFailure::new(
            RegularMutationError::KeepSizeBeyondEofUnsupported,
            prior,
        ));
    }
    let plan = plan_sparse_preallocation(
        store,
        extents,
        logical_bytes,
        offset,
        end,
        config,
        budget,
        prior,
        cancellation,
    )
    .await?;
    if plan.mutations.is_empty() {
        return Ok(RegularMutationReceipt {
            payload: FilePayload::Regular {
                logical_bytes,
                extents,
            },
            work: plan.work,
        });
    }
    let mut nested_budget = remaining(plan.work, budget)?;
    nested_budget.peak_allocation_bytes = nested_budget
        .peak_allocation_bytes
        .checked_sub(plan.mutation_bytes)
        .ok_or_else(|| OperationFailure::new(RegularMutationError::RangeOverflow, plan.work))?;
    let receipt = apply_extent_mutations_async(
        store,
        extents,
        logical_bytes,
        &plan.mutations,
        ExtentMutationOptions {
            maximum_mutations: config.limits.maximum_mutations_per_batch,
            limits: decode_limits(config),
            budget: nested_budget,
        },
        cancellation,
    )
    .await
    .map_err(|failure| {
        simultaneous_failure(
            plan.work,
            *failure.work,
            plan.mutation_bytes,
            failure.error.into(),
        )
    })?;
    let work = simultaneous(plan.work, receipt.work, plan.mutation_bytes, budget)?;
    Ok(RegularMutationReceipt {
        payload: FilePayload::Regular {
            logical_bytes: receipt.logical_bytes,
            extents: receipt.root,
        },
        work,
    })
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
async fn plan_sparse_preallocation<S: AsyncObjectStore>(
    store: &S,
    extents: ObjectId,
    logical_bytes: u64,
    offset: u64,
    end: u64,
    config: VolumeConfig,
    budget: WorkBudget,
    prior: WorkCounters,
    cancellation: &CancellationToken,
) -> Result<SparsePreallocationPlan, RegularMutationFailure> {
    let existing_end = end.min(logical_bytes);
    let existing_length = existing_end.saturating_sub(offset.min(existing_end));
    let (spans, retained, mut work) = if existing_length == 0 {
        (Vec::new(), 0, prior)
    } else {
        let plan = plan_extent_range_async(
            store,
            ExtentRangeRequest {
                root: extents,
                file_size: logical_bytes,
                range: ByteRange {
                    offset,
                    length: existing_length,
                },
                maximum_spans: config.limits.maximum_mutations_per_batch,
                limits: decode_limits(config),
                budget: remaining(prior, budget)?,
            },
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(prior, Into::into))?;
        let work = add(prior, plan.work)?;
        (plan.spans, plan.retained_allocation_bytes, work)
    };
    let scan = WorkCounters {
        items_examined: u64::try_from(spans.len()).unwrap_or(u64::MAX),
        ..WorkCounters::default()
    };
    let scanned = work
        .checked_add(scan)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    scanned
        .verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    work = scanned;
    let hole_count = spans
        .iter()
        .filter(|span| matches!(span.kind, ExtentKind::Hole))
        .count();
    let grows_file = end > logical_bytes;
    let extension_offset = logical_bytes.max(offset);
    let merge_extension = grows_file
        && spans.last().is_some_and(|span| {
            matches!(span.kind, ExtentKind::Hole)
                && span.offset.checked_add(span.length) == Some(extension_offset)
        });
    let mutation_count = hole_count
        .checked_add(usize::from(grows_file))
        .and_then(|count| count.checked_sub(usize::from(merge_extension)))
        .ok_or_else(|| OperationFailure::new(RegularMutationError::RangeOverflow, work))?;
    if u32::try_from(mutation_count).unwrap_or(u32::MAX) > config.limits.maximum_mutations_per_batch
    {
        return Err(OperationFailure::new(
            RegularMutationError::Extent(ExtentMutationError::TooManyMutations),
            work,
        ));
    }
    if mutation_count == 0 {
        return Ok(SparsePreallocationPlan {
            mutations: Vec::new(),
            mutation_bytes: 0,
            work,
        });
    }
    let mutation_bytes = mutation_count
        .checked_mul(size_of::<ExtentMutation>())
        .map(crate::foundation::usize_to_u64)
        .ok_or_else(|| OperationFailure::new(RegularMutationError::RangeOverflow, work))?;
    let simultaneous_bytes = retained
        .checked_add(mutation_bytes)
        .ok_or_else(|| OperationFailure::new(RegularMutationError::RangeOverflow, work))?;
    let allocation_attempt = work
        .checked_add(WorkCounters {
            allocation_operations: 1,
            ..WorkCounters::default()
        })
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    let mut admitted = allocation_attempt;
    admitted.peak_allocation_bytes = admitted.peak_allocation_bytes.max(simultaneous_bytes);
    admitted
        .verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    let mut mutations = Vec::new();
    if mutations.try_reserve_exact(mutation_count).is_err() {
        return Err(OperationFailure::new(
            RegularMutationError::AllocationFailed,
            allocation_attempt,
        ));
    }
    work = admitted;
    mutations.extend(
        spans
            .iter()
            .filter(|span| matches!(span.kind, ExtentKind::Hole))
            .map(|span| ExtentMutation::Replace {
                offset: span.offset,
                length: if merge_extension
                    && span.offset.checked_add(span.length) == Some(extension_offset)
                {
                    end - span.offset
                } else {
                    span.length
                },
                kind: ExtentKind::AllocatedZero,
                extend: merge_extension
                    && span.offset.checked_add(span.length) == Some(extension_offset),
            }),
    );
    if grows_file && !merge_extension {
        mutations.push(ExtentMutation::Replace {
            offset: extension_offset,
            length: end - extension_offset,
            kind: ExtentKind::AllocatedZero,
            extend: true,
        });
    }
    drop(spans);
    Ok(SparsePreallocationPlan {
        mutations,
        mutation_bytes,
        work,
    })
}

#[allow(clippy::too_many_arguments)]
async fn try_demote_sparse_async<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    file_size: u64,
    target_size: u64,
    config: VolumeConfig,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<InlineDemotionAttempt, RegularMutationFailure> {
    if target_size == 0 {
        return Ok(InlineDemotionAttempt {
            data: Some(
                InlineFileData::new(&[])
                    .map_err(|error| OperationFailure::before_work(error.into()))?,
            ),
            work: WorkCounters::default(),
        });
    }
    let plan = plan_extent_range_async(
        store,
        ExtentRangeRequest {
            root,
            file_size,
            range: ByteRange {
                offset: 0,
                length: target_size,
            },
            maximum_spans: config.limits.maximum_mutations_per_batch,
            limits: decode_limits(config),
            budget,
        },
        cancellation,
    )
    .await
    .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
    let retained = plan.retained_allocation_bytes;
    let mut work = plan.work;
    if plan
        .spans
        .iter()
        .any(|span| !matches!(span.kind, ExtentKind::Content { .. }))
    {
        return Ok(InlineDemotionAttempt { data: None, work });
    }
    let target = usize::try_from(target_size)
        .map_err(|_| OperationFailure::new(RegularMutationError::RangeOverflow, work))?;
    let mut data =
        InlineFileData::new(&[]).map_err(|error| OperationFailure::new(error.into(), work))?;
    for span in &plan.spans {
        let ExtentKind::Content {
            object,
            object_offset,
        } = span.kind
        else {
            return Ok(InlineDemotionAttempt { data: None, work });
        };
        let mut nested_budget = remaining(work, budget)?;
        nested_budget.peak_allocation_bytes = nested_budget
            .peak_allocation_bytes
            .checked_sub(retained)
            .ok_or_else(|| OperationFailure::new(RegularMutationError::RangeOverflow, work))?;
        let read = read_blob_range_async(
            store,
            object,
            ByteRange {
                offset: object_offset,
                length: span.length,
            },
            decode_limits(config),
            nested_budget,
            cancellation,
        )
        .await
        .map_err(|failure| {
            simultaneous_failure(work, *failure.work, retained, failure.error.into())
        })?;
        work = simultaneous(work, read.work, retained, budget)?;
        let start = usize::try_from(span.offset)
            .map_err(|_| OperationFailure::new(RegularMutationError::RangeOverflow, work))?;
        data = data
            .replace_range(start, &read.bytes, target)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        work = add(
            work,
            WorkCounters {
                bytes_copied: span.length,
                ..WorkCounters::default()
            },
        )?;
        work.verify(budget)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
    }
    Ok(InlineDemotionAttempt {
        data: Some(data),
        work,
    })
}

struct ExtentPageWrite {
    object: ObjectId,
    work: WorkCounters,
}

async fn put_extent_page_async<S: AsyncObjectStore>(
    store: &S,
    page: &ExtentPage,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<ExtentPageWrite, RegularMutationFailure> {
    let encoded_length = extent_page_encoded_length(page, limits.maximum_page_items)
        .map_err(|error| OperationFailure::before_work(error.into()))?;
    let encoded_bytes = crate::foundation::usize_to_u64(encoded_length);
    if encoded_bytes > limits.maximum_page_object_bytes() {
        return Err(OperationFailure::before_work(
            CanonicalDecodeError::ObjectTooLarge {
                observed: encoded_bytes,
                maximum: limits.maximum_page_object_bytes(),
            }
            .into(),
        ));
    }
    let mut work = WorkCounters {
        allocation_operations: u64::from(encoded_bytes != 0),
        peak_allocation_bytes: encoded_bytes,
        ..WorkCounters::default()
    };
    work.verify(budget)
        .map_err(|error| OperationFailure::before_work(error.into()))?;
    let encoded = encode_extent_page(page, limits.maximum_page_items)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    if u64::try_from(encoded.capacity()).unwrap_or(u64::MAX) != encoded_bytes {
        return Err(OperationFailure::new(
            RegularMutationError::AllocationFailed,
            work,
        ));
    }
    work = add(
        work,
        WorkCounters {
            bytes_encoded: encoded_bytes,
            ..WorkCounters::default()
        },
    )?;
    let hashed = work
        .checked_add(WorkCounters {
            bytes_hashed: encoded_bytes
                .checked_add(OBJECT_DIGEST_ENVELOPE_BYTES)
                .ok_or_else(|| OperationFailure::new(RegularMutationError::RangeOverflow, work))?,
            page_writes: 1,
            ..WorkCounters::default()
        })
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    hashed
        .verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    let object = ObjectId {
        kind: ObjectKind::ExtentPage,
        digest: object_digest(ObjectKind::ExtentPage, &encoded),
    };
    let mut backend_budget = remaining(hashed, budget)?;
    backend_budget.peak_allocation_bytes = backend_budget
        .peak_allocation_bytes
        .checked_sub(encoded_bytes)
        .ok_or_else(|| OperationFailure::new(RegularMutationError::RangeOverflow, hashed))?;
    let receipt = store
        .put(object, Bytes::from(encoded), backend_budget, cancellation)
        .await
        .map_err(|failure| {
            simultaneous_failure(hashed, *failure.work, encoded_bytes, failure.error.into())
        })?;
    Ok(ExtentPageWrite {
        object,
        work: simultaneous(hashed, receipt.work, encoded_bytes, budget)?,
    })
}

fn decode_limits(config: VolumeConfig) -> DecodeLimits {
    DecodeLimits {
        maximum_object_bytes: config.limits.maximum_object_bytes,
        maximum_name_bytes: config.limits.maximum_component_bytes,
        maximum_page_items: config.limits.maximum_directory_page_entries,
        maximum_page_bytes: u32::try_from(config.limits.maximum_object_bytes).unwrap_or(u32::MAX),
        maximum_page_height: config.limits.maximum_page_height,
        maximum_visited_pages: u32::try_from(config.limits.maximum_objects_per_generation)
            .unwrap_or(u32::MAX),
    }
}

fn add(prior: WorkCounters, next: WorkCounters) -> Result<WorkCounters, RegularMutationFailure> {
    prior
        .checked_add(next)
        .map_err(|error| OperationFailure::new(error.into(), prior))
}

fn remaining(work: WorkCounters, budget: WorkBudget) -> Result<WorkBudget, RegularMutationFailure> {
    work.remaining(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))
}

fn simultaneous(
    prior: WorkCounters,
    mut nested: WorkCounters,
    live_bytes: u64,
    budget: WorkBudget,
) -> Result<WorkCounters, RegularMutationFailure> {
    let peak = live_bytes
        .checked_add(nested.peak_allocation_bytes)
        .ok_or_else(|| OperationFailure::new(RegularMutationError::RangeOverflow, prior))?;
    nested.peak_allocation_bytes = 0;
    let mut work = add(prior, nested)?;
    work.peak_allocation_bytes = work.peak_allocation_bytes.max(peak);
    work.verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    Ok(work)
}

fn simultaneous_failure(
    prior: WorkCounters,
    mut nested: WorkCounters,
    live_bytes: u64,
    error: RegularMutationError,
) -> RegularMutationFailure {
    let Some(peak) = live_bytes.checked_add(nested.peak_allocation_bytes) else {
        return OperationFailure::new(RegularMutationError::RangeOverflow, prior);
    };
    nested.peak_allocation_bytes = 0;
    let Ok(mut work) = prior.checked_add(nested) else {
        return OperationFailure::new(RegularMutationError::Work(WorkError::Overflow), prior);
    };
    work.peak_allocation_bytes = work.peak_allocation_bytes.max(peak);
    OperationFailure::new(error, work)
}

fn cancelled() -> RegularMutationError {
    RegularMutationError::Storage(ObjectStoreError::Cancelled)
}

fn allocation_error(error: AllocationError, work: WorkCounters) -> RegularMutationFailure {
    match error {
        AllocationError::Work(error) => OperationFailure::new(error.into(), work),
        AllocationError::Overflow | AllocationError::ReleaseInvariant => {
            OperationFailure::new(RegularMutationError::RangeOverflow, work)
        }
        AllocationError::InvalidCapacity
        | AllocationError::CapacityExceeded
        | AllocationError::AllocationFailed => {
            OperationFailure::new(RegularMutationError::AllocationFailed, work)
        }
    }
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/regular_mutation.rs"]
mod tests;
