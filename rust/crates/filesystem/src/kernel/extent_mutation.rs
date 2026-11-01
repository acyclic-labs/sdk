//! Sparse persistent extent mutation without file-body materialization.

use super::allocation::{AllocationError, KernelAccounting, KernelOwnedAllocation};
use super::codec::DecodedPageShape;
use super::persistent_btree::{self, Format, Page, PageRef};
use super::persistent_point::{Owned, retire_allocation};
use super::search::binary_search_with_accounting;
use super::types::ExtentItems;
use super::{CanonicalDecodeError, DecodeLimits, Extent, ExtentKind, ExtentPage};
use crate::FsError;
use crate::cancellation::CancellationToken;
use crate::foundation::usize_to_u64;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{HashedObject, ObjectId, ObjectKind, ObjectStoreError};
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

/// Applies ordered extent mutations on the shared B+tree engine.
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
    let truncated = final_size < old_logical_bytes;
    let mut edits = Owned::<Vec<ExtentEdit>>::allocate(
        plan.patches
            .value
            .len()
            .checked_add(usize::from(truncated))
            .ok_or(WorkError::Overflow)?,
        accounting,
    )?;
    for patch in &plan.patches.value {
        edits.value.push(ExtentEdit {
            route: patch.offset.saturating_sub(1),
            change: Change::Write(patch.clone()),
        });
    }
    if truncated {
        edits.value.push(ExtentEdit {
            route: final_size.saturating_sub(1),
            change: Change::Truncate(final_size),
        });
    }
    plan.patches.retire(accounting)?;
    let result = if edits.value.is_empty() {
        Ok(root)
    } else if final_size == 0 {
        // Nothing survives, so no old page needs reading.
        put_extent_page_with_accounting(
            store,
            &ExtentPage::Leaf(Vec::new()),
            limits,
            accounting,
            cancellation,
        )
        .await
    } else {
        persistent_btree::apply_with_accounting::<S, ExtentFormat, ExtentEdit>(
            store,
            (root, Some(&0), Some(&old_logical_bytes)),
            &edits.value,
            u32::MAX,
            limits,
            accounting,
            cancellation,
        )
        .await
        .map(|receipt| receipt.value)
    };
    edits.retire(accounting)?;
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

/// The shared engine's view of extent pages: leaves keyed by offset.
pub(crate) struct ExtentFormat;

impl acyclic_collections::page::Format for ExtentFormat {
    type Key = u64;
    type Value = Extent;
    type Reference = ObjectId;
    fn key(value: &Extent) -> &u64 {
        &value.offset
    }
}

impl Format for ExtentFormat {
    fn kind() -> ObjectKind {
        ObjectKind::ExtentPage
    }

    fn key_hash(key: &u64) -> u64 {
        persistent_btree::stable_hash(&key.to_le_bytes())
    }

    fn decode<E: From<CanonicalDecodeError>>(
        bytes: &[u8],
        limits: DecodeLimits,
        observe: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Page<Self>, E> {
        Ok(
            match super::extent::decode_extent_page(bytes, limits, observe)? {
                ExtentPage::Leaf(extents) => Page::Leaf(extents),
                ExtentPage::Internal(children) => Page::Internal(children),
            },
        )
    }

    fn decode_shape(
        bytes: &[u8],
        limits: DecodeLimits,
    ) -> Result<DecodedPageShape, CanonicalDecodeError> {
        super::extent::decode_extent_page_shape(bytes, limits)
    }

    fn encode_with_accounting(
        page: &PageRef<'_, Self>,
        maximum_items: u32,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(Vec<u8>, KernelOwnedAllocation), FsError> {
        let items = match page {
            PageRef::Leaf(extents) => ExtentItems::Leaf(extents),
            PageRef::Internal(children) => ExtentItems::Internal(children),
        };
        super::codec::encode_owned(accounting, |observe, reserve_error| {
            super::extent::encode_extent_items(items, maximum_items, observe, reserve_error)
        })
    }

    fn leaf_item_encoded_length(value: &Extent) -> Result<usize, CanonicalDecodeError> {
        Ok(super::extent::extent_encoded_length(value))
    }

    fn internal_item_encoded_length(
        _key: &u64,
        page: ObjectId,
    ) -> Result<usize, CanonicalDecodeError> {
        Ok(super::extent::extent_child_encoded_length(page))
    }

    fn key_nested_bytes(_key: &u64) -> u64 {
        0
    }

    fn value_nested_bytes(_value: &Extent) -> u64 {
        0
    }

    fn key_owned_bytes(_key: &u64) -> u64 {
        0
    }

    fn value_owned_bytes(_value: &Extent) -> u64 {
        0
    }

    fn try_clone_key<E: From<CanonicalDecodeError>>(
        key: &u64,
        _maximum_bytes: u32,
        _steps: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
    ) -> Result<u64, E> {
        Ok(*key)
    }

    fn try_clone_value<E: From<CanonicalDecodeError>>(
        value: &Extent,
        _maximum_bytes: u32,
        _steps: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Extent, E> {
        Ok(value.clone())
    }
}

/// One normalized range edit, routed from the byte before it so that the
/// extent it may join on the left is rewritten with it.
pub(crate) struct ExtentEdit {
    route: u64,
    change: Change,
}

enum Change {
    /// Replaces `[offset, end)`, reaching the extent at `end` it may join.
    Write(Patch),
    /// Removes every byte at or after this logical length.
    Truncate(u64),
}

impl persistent_btree::Mutation<ExtentFormat> for ExtentEdit {
    type Error = ExtentMutationError;

    const RANGED: bool = true;

    fn key(&self) -> &u64 {
        &self.route
    }

    fn last(&self) -> Option<&u64> {
        match &self.change {
            Change::Write(patch) => Some(&patch.end),
            Change::Truncate(_) => None,
        }
    }

    fn removes_from(&self) -> Option<&u64> {
        match &self.change {
            Change::Write(_) => None,
            Change::Truncate(size) => Some(size),
        }
    }

    fn changes_cardinality(&self) -> bool {
        true
    }

    fn replacement<'a>(
        &'a self,
        _current: Option<&Extent>,
    ) -> Result<Option<&'a Extent>, Self::Error> {
        Err(ExtentMutationError::PatchInvariant)
    }

    fn map_error(error: persistent_btree::Error<Self::Error>) -> FsError {
        use persistent_btree::Error;
        extent_error(match error {
            Error::Empty => ExtentMutationError::Empty,
            Error::TooManyMutations => ExtentMutationError::TooManyMutations,
            Error::WrongRootKind => ExtentMutationError::WrongRootKind,
            Error::InvalidLimits => ExtentMutationError::InvalidLimits,
            Error::PageItemTooLarge => ExtentMutationError::PageItemTooLarge,
            Error::HeightExceeded => ExtentMutationError::HeightExceeded,
            Error::CycleOrAlias => ExtentMutationError::CycleOrAlias,
            Error::ChildBoundsMismatch => ExtentMutationError::ChildBoundsMismatch,
            Error::MutationContract => ExtentMutationError::PatchInvariant,
            Error::AllocationFailed => ExtentMutationError::AllocationFailed,
            Error::Allocation(error) => return extent_allocation_error(error),
            Error::Work(error) => return error.into(),
            Error::Semantic(error) => error,
            Error::Storage(error) => error.into(),
            Error::Decode(error) => error.into(),
        })
    }

    fn rewrite_range(
        carried: &[Extent],
        entries: &[Extent],
        edits: &[(usize, &Self)],
        (lower, upper, expected_end): (Option<&u64>, Option<&u64>, Option<&u64>),
        output: &mut Vec<Extent>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        let first = lower.copied().unwrap_or(0);
        charge_items(accounting, 2)?;
        let end = match (entries.first(), entries.last()) {
            (Some(head), Some(tail)) => {
                let end = extent_end(tail).map_err(extent_error)?;
                if head.offset != first || upper.is_some_and(|upper| *upper != end) {
                    return Err(extent_error(ExtentMutationError::ChildBoundsMismatch));
                }
                end
            }
            _ if lower.is_some() || upper.is_some() => {
                return Err(extent_error(ExtentMutationError::ChildBoundsMismatch));
            }
            _ => first,
        };
        if upper.is_none() && expected_end.is_some_and(|expected| *expected != end) {
            return Err(extent_error(ExtentMutationError::ChildBoundsMismatch));
        }
        for extent in carried {
            charge_items(accounting, 1)?;
            push_joined(output, extent.clone()).map_err(extent_error)?;
        }
        let mut next_edit = 0;
        let mut write = next_write(edits, &mut next_edit, first, upper.copied(), accounting)?;
        for extent in entries {
            charge_items(accounting, 1)?;
            let extent_end = extent_end(extent).map_err(extent_error)?;
            let mut cursor = extent.offset;
            while cursor < extent_end {
                charge_items(accounting, 1)?;
                while write.is_some_and(|patch| patch.end <= cursor) {
                    write = next_write(edits, &mut next_edit, first, upper.copied(), accounting)?;
                }
                let piece_end = match write {
                    Some(patch) if patch.offset <= cursor => {
                        let piece_end = patch.end.min(extent_end);
                        push_joined(
                            output,
                            piece(&patch.kind, patch.offset, cursor, piece_end)
                                .map_err(extent_error)?,
                        )
                        .map_err(extent_error)?;
                        cursor = piece_end;
                        continue;
                    }
                    Some(patch) if patch.offset < extent_end => patch.offset,
                    _ => extent_end,
                };
                push_joined(
                    output,
                    piece(&extent.kind, extent.offset, cursor, piece_end).map_err(extent_error)?,
                )
                .map_err(extent_error)?;
                cursor = piece_end;
            }
        }
        // Only the last leaf grows; the patch plan leaves no gap past its end.
        if upper.is_none() {
            let mut cursor = end;
            while let Some(patch) = write {
                charge_items(accounting, 1)?;
                write = next_write(edits, &mut next_edit, first, upper.copied(), accounting)?;
                if patch.end <= cursor {
                    continue;
                }
                if patch.offset > cursor {
                    return Err(extent_error(ExtentMutationError::PatchInvariant));
                }
                push_joined(
                    output,
                    piece(&patch.kind, patch.offset, cursor, patch.end).map_err(extent_error)?,
                )
                .map_err(extent_error)?;
                cursor = patch.end;
            }
        }
        for (_, edit) in edits {
            charge_items(accounting, 1)?;
            if let Change::Truncate(size) = edit.change {
                charge_items(accounting, 1)?;
                while output.last().is_some_and(|extent| extent.offset >= size) {
                    charge_items(accounting, 1)?;
                    output.pop();
                }
                if let Some(last) = output.last_mut()
                    && extent_end(last).map_err(extent_error)? > size
                {
                    last.length = size - last.offset;
                }
            }
        }
        Ok(())
    }
}

/// Advances the original sorted edit slice, admitting each inspection before it
/// can be filtered or projected. No per-leaf patch container is allocated.
fn next_write<'a>(
    edits: &[(usize, &'a ExtentEdit)],
    next: &mut usize,
    first: u64,
    upper: Option<u64>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Option<&'a Patch>, FsError> {
    while let Some((_, edit)) = edits.get(*next) {
        charge_items(accounting, 1)?;
        *next += 1;
        if let Change::Write(patch) = &edit.change
            && patch.end > first
            && upper.is_none_or(|upper| patch.offset < upper)
        {
            return Ok(Some(patch));
        }
    }
    Ok(None)
}

/// The part `[start, end)` of a run of `kind` that begins at `base`.
fn piece(
    kind: &ExtentKind,
    base: u64,
    start: u64,
    end: u64,
) -> Result<Extent, ExtentMutationError> {
    Ok(Extent {
        offset: start,
        length: end - start,
        kind: offset_kind(kind, start - base)?,
    })
}

/// Appends `extent`, joining it to the last extent when it continues it, so
/// the extent list stays maximal and therefore canonical. Never grows past
/// the room the caller admitted.
pub(super) fn push_joined(
    output: &mut Vec<Extent>,
    extent: Extent,
) -> Result<(), ExtentMutationError> {
    if let Some(previous) = output.last_mut()
        && equivalent_continuation(previous, &extent)?
    {
        previous.length = previous
            .length
            .checked_add(extent.length)
            .ok_or(ExtentMutationError::RangeOverflow)?;
        return Ok(());
    }
    if output.len() == output.capacity() {
        return Err(ExtentMutationError::PatchInvariant);
    }
    output.push(extent);
    Ok(())
}

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
    if length > limits.maximum_object_bytes {
        return Err(CanonicalDecodeError::ObjectTooLarge {
            observed: length,
            maximum: limits.maximum_object_bytes,
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
        let hashed = crate::async_storage::prepare_object(store, hashed, accounting).await?;
        let object = hashed.object_id();
        accounting.charge(WorkCounters {
            page_writes: u64::from(kind != ObjectKind::BlobChunk),
            ..WorkCounters::UNCHARGED
        })?;
        {
            let mut work = crate::AuthorityWork::native(original, cancellation);
            store.put_hashed(hashed, &mut work).await
        }
        .map_err(super::file_read::read_store_error)?;
        return Ok(object);
    }
    let hashing = WorkCounters {
        bytes_hashed: usize_to_u64(encoded.value.len()),
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
    let hashed = crate::async_storage::prepare_object(store, hashed, accounting).await?;
    let object = hashed.object_id();
    let receipt = {
        let mut object_work = crate::AuthorityWork::new(accounting.remaining()?, cancellation);
        let result = store.put_hashed(hashed, &mut object_work).await;
        object_work.receipt(result)
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

#[cfg(test)]
#[cfg(feature = "memory")]
#[path = "tests/extent_mutation.rs"]
mod tests;
