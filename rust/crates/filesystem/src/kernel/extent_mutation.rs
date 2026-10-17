//! Sparse persistent extent mutation without file-body materialization.

use super::allocation::{AllocationError, KernelAccounting, VisitedObjectSet};
#[cfg(test)]
use super::extent::extent_page_encoded_length;
use super::persistent_point::{Owned, retire_allocation};
use super::search::{binary_search_with_accounting, partition_point_with_accounting};
use super::{CanonicalDecodeError, DecodeLimits, Extent, ExtentChild, ExtentKind, ExtentPage};
use crate::FsError;
use crate::cancellation::CancellationToken;
use crate::foundation::usize_to_u64;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{
    HashedObject, OBJECT_DIGEST_ENVELOPE_BYTES, ObjectId, ObjectKind, ObjectStoreError,
};
use std::mem::size_of;
use thiserror::Error;

/// One ordered logical-file extent mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExtentMutation {
    /// Replaces one byte range with content, a hole, or allocated zeros.
    Replace {
        /// Inclusive logical destination offset.
        offset: u64,
        /// Positive byte count.
        length: u64,
        /// Replacement representation. Content offset corresponds to `offset`.
        kind: ExtentKind,
        /// Whether this operation may extend logical file length.
        extend: bool,
    },
    /// Truncates or extends; growth is always represented by holes.
    Resize {
        /// New exact logical byte length.
        logical_bytes: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Patch {
    offset: u64,
    end: u64,
    kind: ExtentKind,
}

struct CompiledPatchPlan {
    patches: Owned<Vec<Patch>>,
    final_size: u64,
}

struct NodeRequest {
    page: ObjectId,
    original_first: u64,
    original_end: u64,
    output_end: u64,
    height: u16,
}

struct InternalFrame {
    original: ObjectId,
    children: Owned<Vec<ExtentChild>>,
    original_first: u64,
    original_end: u64,
    output_end: u64,
    height: u16,
    next_child: usize,
    rewritten: Owned<Vec<ExtentChild>>,
}

enum EnteredNode {
    Complete(Owned<Vec<ExtentChild>>),
    Internal(InternalFrame),
}

/// New extent root, logical size, and exact path-copy evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtentMutationReceipt {
    /// Candidate immutable extent-tree root.
    pub root: ObjectId,
    /// Final exact logical file length.
    pub logical_bytes: u64,
    /// Exact page, encoding, hashing, and backend work.
    pub work: WorkCounters,
}

/// Extent mutation failure retaining spent work and safe orphan writes.
pub type ExtentMutationFailure = OperationFailure<ExtentMutationError>;

struct Context<'a, 'b, S> {
    store: &'a S,
    limits: DecodeLimits,
    accounting: &'a mut KernelAccounting<'b>,
    visited: Owned<VisitedObjectSet>,
    patches: Owned<Vec<Patch>>,
    maximum_seen_height: u16,
    cancellation: &'a CancellationToken,
}

/// Applies ordered extent mutations by rewriting only intersecting frontiers.
#[allow(
    clippy::too_many_arguments,
    reason = "Store, bounds, accounting and cancellation are independent inputs to this operation."
)]
pub async fn apply_extent_mutations<S: crate::AsyncObjectStore>(
    store: &S,
    root: (ObjectId, u64),
    mutations: &[ExtentMutation],
    maximum_mutations: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<ExtentMutationReceipt, ExtentMutationFailure> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    apply_extent_mutations_with_accounting(
        store,
        root,
        mutations,
        maximum_mutations,
        limits,
        &mut accounting,
        cancellation,
    )
    .await
    .map_err(|error| OperationFailure::new(project_extent_error(error), accounting.spent()))
}

#[allow(
    clippy::too_many_arguments,
    reason = "Store, bounds, accounting and cancellation are independent inputs to this operation."
)]
pub(crate) async fn apply_extent_mutations_with_accounting<S: crate::AsyncObjectStore>(
    store: &S,
    root: (ObjectId, u64),
    mutations: &[ExtentMutation],
    maximum_mutations: u32,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<ExtentMutationReceipt, FsError> {
    let (root, old_logical_bytes) = root;
    accounting.check()?;
    validate_request(root, mutations, maximum_mutations, limits).map_err(extent_error)?;
    let plan = compile_patch_plan(old_logical_bytes, mutations, accounting)?;
    let final_size = plan.final_size;
    let maximum = usize::try_from(limits.maximum_visited_pages)
        .map_err(|_| extent_error(ExtentMutationError::InvalidLimits))?;
    let visited = VisitedObjectSet::new_with_accounting(maximum, accounting)?;
    let mut context = Context {
        store,
        limits,
        accounting,
        visited,
        patches: plan.patches,
        maximum_seen_height: 0,
        cancellation,
    };
    let result = async {
        let summaries = context
            .rewrite(root, 0, old_logical_bytes, final_size)
            .await?;
        context.finish_root(summaries).await
    }
    .await;
    let Context {
        visited,
        patches,
        accounting,
        ..
    } = context;
    visited.retire(accounting)?;
    patches.retire(accounting)?;
    Ok(ExtentMutationReceipt {
        root: result?,
        logical_bytes: final_size,
        work: accounting.spent(),
    })
}

fn validate_request(
    root: ObjectId,
    mutations: &[ExtentMutation],
    maximum_mutations: u32,
    limits: DecodeLimits,
) -> Result<(), ExtentMutationError> {
    let error = if root.kind != ObjectKind::ExtentPage {
        Some(ExtentMutationError::WrongRootKind)
    } else if mutations.is_empty() {
        Some(ExtentMutationError::Empty)
    } else if maximum_mutations == 0
        || usize::try_from(maximum_mutations).map_or(true, |maximum| mutations.len() > maximum)
    {
        Some(ExtentMutationError::TooManyMutations)
    } else if !limits.page_limits_valid(2) {
        Some(ExtentMutationError::InvalidLimits)
    } else {
        None
    };
    match error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn compile_patch_plan(
    old_size: u64,
    mutations: &[ExtentMutation],
    accounting: &mut KernelAccounting<'_>,
) -> Result<CompiledPatchPlan, FsError> {
    let (final_size, raw_count) = admit_patch_plan(old_size, mutations, accounting)?;
    let mut raw = Owned::<Vec<Patch>>::allocate(raw_count, accounting)?;
    let mut size = old_size;
    for mutation in mutations {
        charge_items(accounting, 1)?;
        match mutation {
            ExtentMutation::Replace {
                offset,
                length,
                kind,
                ..
            } => {
                let end = offset.checked_add(*length).ok_or(WorkError::Overflow)?;
                if *offset > size {
                    raw.value.push(Patch {
                        offset: size,
                        end: *offset,
                        kind: ExtentKind::Hole,
                    });
                }
                raw.value.push(Patch {
                    offset: *offset,
                    end,
                    kind: kind.clone(),
                });
                size = size.max(end);
            }
            ExtentMutation::Resize { logical_bytes } => {
                if *logical_bytes != size {
                    raw.value.push(Patch {
                        offset: size.min(*logical_bytes),
                        end: size.max(*logical_bytes),
                        kind: ExtentKind::Hole,
                    });
                    size = *logical_bytes;
                }
            }
        }
    }
    let patches = normalize_raw_patches(&raw.value, final_size, accounting, sort_patch_coordinates);
    raw.retire(accounting)?;
    Ok(CompiledPatchPlan {
        patches: patches?,
        final_size,
    })
}

mod patches;
use patches::*;

fn extent_error(error: ExtentMutationError) -> FsError {
    FsError::DetachedMutation(super::RegularMutationError::Extent(error))
}

fn extent_allocation_error(error: AllocationError) -> FsError {
    error.into_work_or(extent_error(ExtentMutationError::AllocationFailed))
}

pub(super) fn project_extent_error(error: FsError) -> ExtentMutationError {
    match error {
        FsError::DetachedMutation(super::RegularMutationError::Extent(error)) => error,
        FsError::Work(error) => error.into(),
        FsError::Decode(error) => error.into(),
        FsError::BlobRead(super::BlobReadError::Storage(error)) => error.into(),
        FsError::FileRead(super::FileRangeReadError::Extent(super::ExtentReadError::Decode(
            error,
        ))) => error.into(),
        FsError::FileRead(super::FileRangeReadError::Extent(super::ExtentReadError::Work(
            error,
        ))) => error.into(),
        FsError::FileRead(super::FileRangeReadError::Extent(super::ExtentReadError::Storage(
            error,
        ))) => error.into(),
        #[cfg(not(target_arch = "wasm32"))]
        FsError::NativeKernel(error) => ObjectStoreError::Native(error).into(),
        FsError::Cancelled(_) => ObjectStoreError::Cancelled.into(),
        error => ExtentMutationError::Context(Box::new(error)),
    }
}

fn patch_continues(left: &Patch, right: &Patch) -> Result<bool, ExtentMutationError> {
    if left.end != right.offset {
        return Ok(false);
    }
    let left_extent = Extent {
        offset: left.offset,
        length: left.end - left.offset,
        kind: left.kind.clone(),
    };
    let right_extent = Extent {
        offset: right.offset,
        length: right.end - right.offset,
        kind: right.kind.clone(),
    };
    equivalent_continuation(&left_extent, &right_extent)
}

fn validate_kind(kind: &ExtentKind, length: u64) -> Result<(), ExtentMutationError> {
    if let ExtentKind::Content {
        object,
        object_offset,
    } = kind
    {
        if object.kind != ObjectKind::Blob {
            return Err(ExtentMutationError::WrongContentKind);
        }
        object_offset
            .checked_add(length)
            .ok_or(ExtentMutationError::RangeOverflow)?;
    }
    Ok(())
}

fn extent_leaf_chunk_end(
    extents: &[Extent],
    start: usize,
    limits: DecodeLimits,
) -> Result<(usize, u64), ExtentMutationError> {
    extent_chunk_end(extents, start, limits, |extent| match extent.kind {
        ExtentKind::Hole | ExtentKind::AllocatedZero => 8 + 8 + 1,
        ExtentKind::Content { .. } => 8 + 8 + 1 + 32 + 8,
    })
}

fn extent_internal_chunk_end(
    children: &[ExtentChild],
    start: usize,
    limits: DecodeLimits,
) -> Result<(usize, u64), ExtentMutationError> {
    extent_chunk_end(children, start, limits, |_| 8 + 8 + 32)
}

fn extent_chunk_end<T>(
    items: &[T],
    start: usize,
    limits: DecodeLimits,
    encoded_length: impl Fn(&T) -> usize,
) -> Result<(usize, u64), ExtentMutationError> {
    const PAGE_HEADER_BYTES: usize = 8 + 2 + 1 + 4;
    let maximum_items = usize::try_from(limits.maximum_page_items)
        .map_err(|_| ExtentMutationError::InvalidLimits)?;
    let maximum_bytes = usize::try_from(limits.maximum_page_bytes)
        .map_err(|_| ExtentMutationError::InvalidLimits)?;
    let mut end = start;
    let mut bytes = PAGE_HEADER_BYTES;
    let mut examined = 0_u64;
    while end < items.len() && end - start < maximum_items {
        examined = examined.saturating_add(1);
        #[allow(
            clippy::indexing_slicing,
            reason = "the loop guard `end < items.len()` directly establishes `end` is in bounds \
                      at this point in the loop body"
        )]
        let next = bytes
            .checked_add(encoded_length(&items[end]))
            .ok_or(ExtentMutationError::PageItemTooLarge)?;
        if next > maximum_bytes {
            if end == start {
                return Err(ExtentMutationError::PageItemTooLarge);
            }
            break;
        }
        bytes = next;
        end += 1;
    }
    if end == start {
        return Err(ExtentMutationError::PageItemTooLarge);
    }
    Ok((end, examined))
}

mod context;

pub(crate) async fn put_extent_page_with_accounting<S: crate::AsyncObjectStore>(
    store: &S,
    page: &ExtentPage,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<ObjectId, FsError> {
    accounting.check()?;
    let length = usize_to_u64(super::extent::extent_page_encoded_length(
        page,
        limits.maximum_page_items,
    )?);
    if length > limits.maximum_page_object_bytes() {
        return Err(CanonicalDecodeError::ObjectTooLarge {
            observed: length,
            maximum: limits.maximum_page_object_bytes(),
        }
        .into());
    }
    accounting.admit(WorkCounters {
        page_writes: 1,
        ..WorkCounters::UNCHARGED
    })?;
    let (value, allocation) = super::codec::encode_owned(accounting, |observe, reserve_error| {
        super::extent::encode_extent_page(page, limits.maximum_page_items, observe, reserve_error)
    })?;
    let bytes = usize_to_u64(value.capacity());
    let encoded = Owned::new(value, allocation, bytes);
    put_encoded_with_accounting(
        store,
        ObjectKind::ExtentPage,
        encoded,
        accounting,
        cancellation,
    )
    .await
}

pub(crate) async fn put_encoded_with_accounting<S: crate::AsyncObjectStore>(
    store: &S,
    kind: ObjectKind,
    encoded: Owned<Vec<u8>>,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<ObjectId, FsError> {
    accounting.check()?;
    accounting.admit(WorkCounters {
        page_writes: u64::from(kind != ObjectKind::BlobChunk),
        ..WorkCounters::UNCHARGED
    })?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = accounting.native_work() {
        let mut encoded = encoded;
        let allocation = match &mut encoded.allocation {
            super::allocation::KernelOwnedAllocation::NativeOriginal(allocation) => allocation,
            super::allocation::KernelOwnedAllocation::Standalone => {
                return Err(extent_error(ExtentMutationError::AllocationFailed));
            }
        };
        let hashed = HashedObject::from_native_vec(
            kind,
            &mut encoded.value,
            allocation,
            original,
            cancellation,
        )?;
        let object = hashed.object_id();
        accounting.charge(WorkCounters {
            page_writes: u64::from(kind != ObjectKind::BlobChunk),
            ..WorkCounters::UNCHARGED
        })?;
        {
            let __object_args = (hashed, original, cancellation);
            let mut __object_work = crate::AuthorityWork::native(__object_args.1, __object_args.2);

            store.put_hashed(__object_args.0, &mut __object_work).await
        }
        .map_err(super::file_read::read_store_error)?;
        return Ok(object);
    }
    let hashing = WorkCounters {
        bytes_hashed: usize_to_u64(encoded.value.len())
            .checked_add(OBJECT_DIGEST_ENVELOPE_BYTES)
            .ok_or(WorkError::Overflow)?,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(hashing)?;
    let Owned {
        value,
        allocation,
        bytes,
    } = encoded;
    let hashed = HashedObject::new(kind, value.into());
    accounting.charge(hashing)?;
    accounting.charge(WorkCounters {
        page_writes: u64::from(kind != ObjectKind::BlobChunk),
        ..WorkCounters::UNCHARGED
    })?;
    let object = hashed.object_id();
    let receipt = {
        let __object_args = (hashed, accounting.remaining()?, cancellation);
        let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = store.put_hashed(__object_args.0, &mut __object_work).await;
        __object_work.receipt(__object_result)
    };
    let spent = match &receipt {
        Ok(receipt) => receipt.work,
        Err(failure) => *failure.work,
    };
    super::file_read::complete_standalone_backend(accounting, spent, 0)?;
    retire_allocation(allocation, bytes, accounting)?;
    receipt.map_err(|failure| super::file_read::read_store_error(failure.error))?;
    Ok(object)
}

fn clip_and_grow(
    extents: &[Extent],
    output_end: u64,
    original_end: u64,
    result: &mut Vec<Extent>,
) -> Result<(), ExtentMutationError> {
    for mut extent in extents.iter().cloned() {
        if extent.offset >= output_end {
            break;
        }
        let end = extent_end(&extent)?.min(output_end);
        extent.length = end - extent.offset;
        result.push(extent);
    }
    if output_end > original_end {
        result.push(Extent {
            offset: original_end,
            length: output_end - original_end,
            kind: ExtentKind::Hole,
        });
    }
    Ok(())
}

fn apply_patches_linear(
    extents: &[Extent],
    patches: &[Patch],
    result: &mut Vec<Extent>,
) -> Result<u64, ExtentMutationError> {
    let mut patch_index = 0;
    let mut examined = 0_u64;
    for extent in extents {
        examined = examined.saturating_add(1);
        let extent_end = extent_end(extent)?;
        let mut cursor = extent.offset;
        while patches
            .get(patch_index)
            .is_some_and(|patch| patch.end <= cursor)
        {
            patch_index += 1;
            examined = examined.saturating_add(1);
        }
        while let Some(patch) = patches.get(patch_index) {
            examined = examined.saturating_add(1);
            if patch.offset >= extent_end {
                break;
            }
            if cursor < patch.offset {
                let unchanged_end = patch.offset.min(extent_end);
                result.push(Extent {
                    offset: cursor,
                    length: unchanged_end - cursor,
                    kind: offset_kind(&extent.kind, cursor - extent.offset)?,
                });
                cursor = unchanged_end;
            }
            let replacement_start = cursor.max(patch.offset);
            let replacement_end = extent_end.min(patch.end);
            if replacement_start < replacement_end {
                result.push(Extent {
                    offset: replacement_start,
                    length: replacement_end - replacement_start,
                    kind: offset_kind(&patch.kind, replacement_start - patch.offset)?,
                });
                cursor = replacement_end;
            }
            if patch.end <= cursor {
                patch_index += 1;
            } else {
                break;
            }
        }
        if cursor < extent_end {
            result.push(Extent {
                offset: cursor,
                length: extent_end - cursor,
                kind: offset_kind(&extent.kind, cursor - extent.offset)?,
            });
        }
    }
    Ok(examined)
}

fn coalesce(extents: &mut Vec<Extent>) -> Result<(), ExtentMutationError> {
    let mut output = 0_usize;
    for index in 0..extents.len() {
        let extent = extents
            .get(index)
            .ok_or(ExtentMutationError::PatchInvariant)?
            .clone();
        if output != 0 {
            let previous = extents
                .get_mut(output - 1)
                .ok_or(ExtentMutationError::PatchInvariant)?;
            if equivalent_continuation(previous, &extent)? {
                previous.length = previous
                    .length
                    .checked_add(extent.length)
                    .ok_or(ExtentMutationError::RangeOverflow)?;
                continue;
            }
        }
        *extents
            .get_mut(output)
            .ok_or(ExtentMutationError::PatchInvariant)? = extent;
        output += 1;
    }
    extents.truncate(output);
    Ok(())
}

fn equivalent_continuation(left: &Extent, right: &Extent) -> Result<bool, ExtentMutationError> {
    if extent_end(left)? != right.offset {
        return Ok(false);
    }
    Ok(match (&left.kind, &right.kind) {
        (ExtentKind::Hole, ExtentKind::Hole)
        | (ExtentKind::AllocatedZero, ExtentKind::AllocatedZero) => true,
        (
            ExtentKind::Content {
                object: left_object,
                object_offset: left_offset,
            },
            ExtentKind::Content {
                object: right_object,
                object_offset: right_offset,
            },
        ) => {
            left_object == right_object
                && left_offset.checked_add(left.length) == Some(*right_offset)
        }
        _ => false,
    })
}

fn offset_kind(kind: &ExtentKind, delta: u64) -> Result<ExtentKind, ExtentMutationError> {
    match kind {
        ExtentKind::Content {
            object,
            object_offset,
        } => Ok(ExtentKind::Content {
            object: *object,
            object_offset: object_offset
                .checked_add(delta)
                .ok_or(ExtentMutationError::RangeOverflow)?,
        }),
        value => Ok(value.clone()),
    }
}

fn validate_leaf(
    extents: &[Extent],
    expected_first: u64,
    expected_end: u64,
) -> Result<(), ExtentMutationError> {
    if expected_first == expected_end && extents.is_empty() {
        return Ok(());
    }
    if extents.first().map(|extent| extent.offset) != Some(expected_first)
        || extents.last().map(extent_end).transpose()? != Some(expected_end)
    {
        return Err(ExtentMutationError::ChildBoundsMismatch);
    }
    Ok(())
}

fn validate_children(
    children: &[ExtentChild],
    expected_first: u64,
    expected_end: u64,
) -> Result<(), ExtentMutationError> {
    if children.first().map(|child| child.first_offset) != Some(expected_first)
        || children.last().map(|child| child.end_offset) != Some(expected_end)
    {
        return Err(ExtentMutationError::ChildBoundsMismatch);
    }
    Ok(())
}

fn overlapping_patch_range(
    patches: &[Patch],
    start: u64,
    end: u64,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(usize, usize), FsError> {
    let first = partition_point_with_accounting(patches, accounting, |patch| patch.end <= start)?;
    let after_last =
        partition_point_with_accounting(patches, accounting, |patch| patch.offset < end)?;
    Ok((first, after_last.max(first)))
}

fn extent_end(extent: &Extent) -> Result<u64, ExtentMutationError> {
    extent
        .offset
        .checked_add(extent.length)
        .ok_or(ExtentMutationError::RangeOverflow)
}

/// Persistent sparse extent mutation failures.
#[derive(Debug, Error)]
pub enum ExtentMutationError {
    /// Batch contains no mutations.
    #[error("extent mutation batch is empty")]
    Empty,
    /// Batch exceeds its admitted bound.
    #[error("extent mutation count exceeds its bound")]
    TooManyMutations,
    /// Root has the wrong object kind.
    #[error("extent mutation root has the wrong kind")]
    WrongRootKind,
    /// Page width/height cannot support mutation.
    #[error("extent mutation limits are invalid")]
    InvalidLimits,
    /// One extent or required pair of child routes cannot fit in a page.
    #[error("extent mutation item exceeds its page byte bound")]
    PageItemTooLarge,
    /// Replacement range must be non-empty.
    #[error("extent mutation range is empty")]
    EmptyRange,
    /// Range arithmetic overflowed.
    #[error("extent mutation range overflowed")]
    RangeOverflow,
    /// Non-extending replacement exceeds current logical file length.
    #[error("extent mutation exceeds logical file length")]
    OutsideFile,
    /// Content replacement references another object class.
    #[error("extent mutation content is not a blob")]
    WrongContentKind,
    /// Internal normalized-patch map was inconsistent.
    #[error("extent mutation normalized patch map is inconsistent")]
    PatchInvariant,
    /// Fallible fixed-capacity scratch allocation was unavailable.
    #[error("extent mutation scratch allocation failed")]
    AllocationFailed,
    /// Traversal or growth exceeds the page-height bound.
    #[error("extent mutation exceeds its height bound")]
    HeightExceeded,
    /// Authenticated page graph cycles or aliases.
    #[error("extent mutation encountered a cycle or alias")]
    CycleOrAlias,
    /// Parent range bounds differ from child content.
    #[error("extent mutation child bounds mismatch")]
    ChildBoundsMismatch,
    /// Immutable object storage failed.
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    /// Canonical page encoding/decoding failed.
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
    /// Exact work exceeded or overflowed its budget.
    #[error(transparent)]
    Work(#[from] WorkError),
    /// A shared kernel boundary returned another original typed filesystem error.
    #[error(transparent)]
    Context(Box<FsError>),
}

impl From<AllocationError> for ExtentMutationError {
    fn from(error: AllocationError) -> Self {
        error.into_work_or(Self::AllocationFailed)
    }
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/extent_mutation.rs"]
mod tests;
