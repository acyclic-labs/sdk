//! Sparse persistent extent mutation without file-body materialization.

use super::allocation::{AllocationError, KernelAccounting, VisitedObjectSet};
#[cfg(test)]
use super::extent::extent_page_encoded_length;
use super::persistent_point::{Owned, retire_allocation};
use super::search::{binary_search_with_accounting, partition_point_with_accounting};
use super::{
    CanonicalDecodeError, DecodeLimits, Extent, ExtentChild, ExtentKind, ExtentPage,
};
use crate::cancellation::CancellationToken;
use crate::foundation::usize_to_u64;
use crate::FsError;
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

#[derive(Clone, Debug)]
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
pub async fn apply_extent_mutations<S: crate::AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    old_logical_bytes: u64,
    mutations: &[ExtentMutation],
    maximum_mutations: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<ExtentMutationReceipt, ExtentMutationFailure> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    apply_extent_mutations_with_accounting(
        store, root, old_logical_bytes, mutations, maximum_mutations,
        limits, &mut accounting, cancellation,
    ).await.map_err(|error| OperationFailure::new(project_extent_error(error), accounting.spent()))
}

pub(crate) async fn apply_extent_mutations_with_accounting<S: crate::AsyncObjectStore>(
    store: &S, root: ObjectId, old_logical_bytes: u64, mutations: &[ExtentMutation],
    maximum_mutations: u32, limits: DecodeLimits, accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<ExtentMutationReceipt, FsError> {
    accounting.check()?;
    validate_request(root, mutations, maximum_mutations, limits).map_err(extent_error)?;
    let plan = compile_patch_plan(old_logical_bytes, mutations, accounting)?;
    let final_size = plan.final_size;
    let maximum = usize::try_from(limits.maximum_visited_pages)
        .map_err(|_| extent_error(ExtentMutationError::InvalidLimits))?;
    let visited = VisitedObjectSet::new_with_accounting(maximum, accounting)?;
    let mut context = Context {
        store, limits, accounting, visited,
        patches: plan.patches, maximum_seen_height: 0, cancellation,
    };
    let result = async {
        let summaries = context.rewrite(root, 0, old_logical_bytes, final_size).await?;
        context.finish_root(summaries).await
    }.await;
    let Context { visited, patches, accounting, .. } = context;
    visited.retire(accounting)?;
    patches.retire(accounting)?;
    Ok(ExtentMutationReceipt {
        root: result?, logical_bytes: final_size, work: accounting.spent(),
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
    old_size: u64, mutations: &[ExtentMutation], accounting: &mut KernelAccounting<'_>,
) -> Result<CompiledPatchPlan, FsError> {
    let (final_size, raw_count) = admit_patch_plan(old_size, mutations, accounting)?;
    let mut raw = Owned::<Vec<Patch>>::allocate(raw_count, accounting)?;
    let mut size = old_size;
    for mutation in mutations {
        charge_items(accounting, 1)?;
        match mutation {
            ExtentMutation::Replace { offset, length, kind, .. } => {
                let end = offset.checked_add(*length).ok_or(WorkError::Overflow)?;
                if *offset > size {
                    raw.value.push(Patch { offset: size, end: *offset, kind: ExtentKind::Hole });
                }
                raw.value.push(Patch { offset: *offset, end, kind: kind.clone() });
                size = size.max(end);
            }
            ExtentMutation::Resize { logical_bytes } => {
                if *logical_bytes != size {
                    raw.value.push(Patch {
                        offset: size.min(*logical_bytes), end: size.max(*logical_bytes),
                        kind: ExtentKind::Hole,
                    });
                    size = *logical_bytes;
                }
            }
        }
    }
    let patches = normalize_raw_patches(&raw.value, final_size, accounting);
    raw.retire(accounting)?;
    Ok(CompiledPatchPlan { patches: patches?, final_size })
}

fn admit_patch_plan(
    old_size: u64, mutations: &[ExtentMutation], accounting: &mut KernelAccounting<'_>,
) -> Result<(u64, usize), FsError> {
    let mut size = old_size;
    let mut raw_count = 0_usize;
    for mutation in mutations {
        charge_items(accounting, 1)?;
        match mutation {
            ExtentMutation::Replace { offset, length, kind, extend } => {
                if *length == 0 { return Err(extent_error(ExtentMutationError::EmptyRange)); }
                validate_kind(kind, *length).map_err(extent_error)?;
                let end = offset.checked_add(*length)
                    .ok_or_else(|| extent_error(ExtentMutationError::RangeOverflow))?;
                if end > size && !extend { return Err(extent_error(ExtentMutationError::OutsideFile)); }
                raw_count = raw_count.checked_add(usize::from(*offset > size) + 1)
                    .ok_or(WorkError::Overflow)?;
                size = size.max(end);
            }
            ExtentMutation::Resize { logical_bytes } => {
                raw_count = raw_count.checked_add(usize::from(*logical_bytes != size))
                    .ok_or(WorkError::Overflow)?;
                size = *logical_bytes;
            }
        }
    }
    Ok((size, raw_count))
}

fn charge_items(accounting: &mut KernelAccounting<'_>, count: u64) -> Result<(), FsError> {
    accounting.charge(WorkCounters { items_examined: count, ..WorkCounters::UNCHARGED })
}

fn normalize_raw_patches(
    raw: &[Patch], final_size: u64, accounting: &mut KernelAccounting<'_>,
) -> Result<Owned<Vec<Patch>>, FsError> {
    let capacity = raw.len().checked_mul(2).ok_or(WorkError::Overflow)?;
    let mut coordinates = Owned::<Vec<u64>>::allocate(capacity, accounting)?;
    for patch in raw {
        charge_items(accounting, 1)?;
        let start = patch.offset.min(final_size);
        let end = patch.end.min(final_size);
        if start < end { coordinates.value.extend([start, end]); }
    }
    if coordinates.value.is_empty() {
        coordinates.retire(accounting)?;
        return Owned::allocate(0, accounting);
    }
    let mut scratch = Owned::<Vec<u64>>::allocate(coordinates.value.len(), accounting)?;
    let sorted = radix_sort(&mut coordinates, &mut scratch, accounting);
    scratch.retire(accounting)?;
    sorted?;
    charge_items(accounting, usize_to_u64(coordinates.value.len()))?;
    coordinates.value.dedup();
    let intervals = coordinates.value.len().saturating_sub(1);
    let mut assignments = Owned::<Vec<Option<usize>>>::allocate(intervals, accounting)?;
    charge_items(accounting, usize_to_u64(intervals))?;
    assignments.value.resize(intervals, None);
    let mut parents = Owned::<Vec<usize>>::allocate(intervals + 1, accounting)?;
    charge_items(accounting, usize_to_u64(intervals + 1))?;
    parents.value.extend(0..=intervals);
    let result = (|| {
        for (patch_index, patch) in raw.iter().enumerate().rev() {
            charge_items(accounting, 1)?;
            let start = patch.offset.min(final_size);
            let end = patch.end.min(final_size);
            if start >= end { continue; }
            let left = coordinate_index(&coordinates.value, start, accounting)?;
            let right = coordinate_index(&coordinates.value, end, accounting)?;
            let mut interval = find_next(&mut parents.value, left, accounting)?;
            while interval < right {
                charge_items(accounting, 1)?;
                *assignments.value.get_mut(interval).ok_or(WorkError::Overflow)? = Some(patch_index);
                let successor = find_next(&mut parents.value, interval + 1, accounting)?;
                *parents.value.get_mut(interval).ok_or(WorkError::Overflow)? = successor;
                interval = successor;
            }
        }
        let mut result = Owned::<Vec<Patch>>::allocate(intervals, accounting)?;
        for (interval, assignment) in assignments.value.iter().enumerate() {
            charge_items(accounting, 1)?;
            let Some(patch_index) = assignment else { continue; };
            let source = raw.get(*patch_index).ok_or(WorkError::Overflow)?;
            let offset = *coordinates.value.get(interval).ok_or(WorkError::Overflow)?;
            let end = *coordinates.value.get(interval + 1).ok_or(WorkError::Overflow)?;
            let candidate = Patch {
                offset, end,
                kind: offset_kind(&source.kind, offset - source.offset).map_err(extent_error)?,
            };
            if let Some(previous) = result.value.last_mut()
                && patch_continues(previous, &candidate).map_err(extent_error)?
            { previous.end = candidate.end; }
            else { result.value.push(candidate); }
        }
        Ok(result)
    })();
    parents.retire(accounting)?;
    assignments.retire(accounting)?;
    coordinates.retire(accounting)?;
    result
}

fn radix_sort(
    values: &mut Owned<Vec<u64>>, scratch: &mut Owned<Vec<u64>>, accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    scratch.value.resize(values.value.len(), 0);
    for byte in 0..8_u32 {
        accounting.charge(WorkCounters {
            items_examined: usize_to_u64(values.value.len()).checked_mul(2).ok_or(WorkError::Overflow)?,
            bytes_copied: usize_to_u64(values.value.len().checked_mul(size_of::<u64>()).ok_or(WorkError::Overflow)?),
            ..WorkCounters::UNCHARGED
        })?;
        let shift = byte * 8;
        let mut counts = [0_usize; 256];
        for value in values.value.iter() {
            let count = counts.get_mut(usize::from(((value >> shift) & 0xff) as u8)).ok_or(WorkError::Overflow)?;
            *count += 1;
        }
        let mut offset = 0;
        for count in &mut counts {
            let current = *count;
            *count = offset;
            offset += current;
        }
        for value in values.value.iter() {
            let count = counts.get_mut(usize::from(((value >> shift) & 0xff) as u8)).ok_or(WorkError::Overflow)?;
            *scratch.value.get_mut(*count).ok_or(WorkError::Overflow)? = *value;
            *count += 1;
        }
        // Move physical data and its exact original custody together on every pass.
        std::mem::swap(values, scratch);
    }
    Ok(())
}

fn coordinate_index(
    values: &[u64], target: u64, accounting: &mut KernelAccounting<'_>,
) -> Result<usize, FsError> {
    let found = binary_search_with_accounting(values, accounting, |value| value.cmp(&target))?;
    found.map_err(|_| extent_error(ExtentMutationError::PatchInvariant))
}

fn find_next(
    parents: &mut [usize], start: usize, accounting: &mut KernelAccounting<'_>,
) -> Result<usize, FsError> {
    let mut root = start;
    while *parents.get(root).ok_or(WorkError::Overflow)? != root {
        charge_items(accounting, 1)?;
        root = *parents.get(root).ok_or(WorkError::Overflow)?;
    }
    let mut current = start;
    while *parents.get(current).ok_or(WorkError::Overflow)? != current {
        charge_items(accounting, 1)?;
        let next = *parents.get(current).ok_or(WorkError::Overflow)?;
        *parents.get_mut(current).ok_or(WorkError::Overflow)? = root;
        current = next;
    }
    Ok(root)
}

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
        FsError::FileRead(super::FileRangeReadError::Extent(super::ExtentReadError::Decode(error))) => error.into(),
        FsError::FileRead(super::FileRangeReadError::Extent(super::ExtentReadError::Work(error))) => error.into(),
        FsError::FileRead(super::FileRangeReadError::Extent(super::ExtentReadError::Storage(error))) => error.into(),
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

impl<S: crate::AsyncObjectStore> Context<'_, '_, S> {
    async fn rewrite(
        &mut self, page: ObjectId, original_first: u64, original_end: u64, output_end: u64,
    ) -> Result<Owned<Vec<ExtentChild>>, FsError> {
        let mut stack = Owned::<Vec<InternalFrame>>::allocate(0, self.accounting)?;
        let result = async {
            let mut request = NodeRequest { page, original_first, original_end, output_end, height: 1 };
            'traverse: loop {
                let mut result = match self.enter_node(request).await? {
                    EnteredNode::Complete(result) => result,
                    EnteredNode::Internal(mut frame) => {
                        if let Some(next) = self.advance_frame(&mut frame)? {
                            stack.ensure_for_push(usize::from(self.limits.maximum_page_height), self.accounting)?;
                            stack.value.push(frame);
                            request = next;
                            continue;
                        }
                        self.finish_frame(frame).await?
                    }
                };
                loop {
                    let Some(mut frame) = stack.value.pop() else { return Ok(result); };
                    for summary in result.value.drain(..) {
                        frame.rewritten.ensure_for_push(usize::MAX, self.accounting)?;
                        frame.rewritten.value.push(summary);
                    }
                    result.retire(self.accounting)?;
                    if let Some(next) = self.advance_frame(&mut frame)? {
                        stack.ensure_for_push(usize::from(self.limits.maximum_page_height), self.accounting)?;
                        stack.value.push(frame);
                        request = next;
                        continue 'traverse;
                    }
                    result = self.finish_frame(frame).await?;
                }
            }
        }.await;
        stack.retire(self.accounting)?;
        result
    }

    async fn enter_node(&mut self, request: NodeRequest) -> Result<EnteredNode, FsError> {
        if request.height > self.limits.maximum_page_height {
            return Err(extent_error(ExtentMutationError::HeightExceeded));
        }
        if request.output_end <= request.original_first {
            return Ok(EnteredNode::Complete(Owned::allocate(0, self.accounting)?));
        }
        self.maximum_seen_height = self.maximum_seen_height.max(request.height);
        let visited = VisitedObjectSet::insert_with_accounting(
            &mut self.visited, request.page, self.accounting,
        )?;
        if !visited.inserted { return Err(extent_error(ExtentMutationError::CycleOrAlias)); }
        let decoded = self.read_page(request.page).await?;
        let Owned { value: page, allocation, bytes } = decoded;
        match page {
            ExtentPage::Leaf(extents) => {
                let extents = Owned::new(extents, allocation, bytes);
                Ok(EnteredNode::Complete(self.rewrite_leaf(
                    request.page, extents, request.original_first, request.original_end, request.output_end,
                ).await?))
            }
            ExtentPage::Internal(children) => {
                let children = Owned::new(children, allocation, bytes);
                validate_children(&children.value, request.original_first, request.original_end).map_err(extent_error)?;
                Ok(EnteredNode::Internal(InternalFrame {
                    original: request.page, children, original_first: request.original_first,
                    original_end: request.original_end, output_end: request.output_end,
                    height: request.height, next_child: 0,
                    rewritten: Owned::allocate(0, self.accounting)?,
                }))
            }
        }
    }

    fn advance_frame(&mut self, frame: &mut InternalFrame) -> Result<Option<NodeRequest>, FsError> {
        while frame.next_child < frame.children.value.len() {
            let index = frame.next_child;
            frame.next_child += 1;
            let child = frame.children.value.get(index).ok_or(WorkError::Overflow)?;
            if child.first_offset >= frame.output_end {
                self.charge_items(1)?;
                frame.next_child = frame.children.value.len();
                break;
            }
            let mut child_output_end = child.end_offset.min(frame.output_end);
            if index + 1 == frame.children.value.len() && frame.output_end > frame.original_end {
                child_output_end = frame.output_end;
            }
            let (first, last) = overlapping_patch_range(
                &self.patches.value, child.first_offset, child_output_end, self.accounting,
            )?;
            self.charge_items(1)?;
            let patch_touched = first < last;
            if child_output_end != child.end_offset || patch_touched {
                return Ok(Some(NodeRequest {
                    page: child.page, original_first: child.first_offset, original_end: child.end_offset,
                    output_end: child_output_end,
                    height: frame.height.checked_add(1).ok_or_else(|| extent_error(ExtentMutationError::HeightExceeded))?,
                }));
            }
            frame.rewritten.ensure_for_push(usize::MAX, self.accounting)?;
            frame.rewritten.value.push(child.clone());
        }
        Ok(None)
    }

    async fn finish_frame(&mut self, frame: InternalFrame) -> Result<Owned<Vec<ExtentChild>>, FsError> {
        let InternalFrame { children, mut rewritten, original_first, original_end, original, height, .. } = frame;
        if rewritten.value.is_empty() || rewritten.value == children.value {
            if !rewritten.value.is_empty() {
                rewritten.value.clear();
                rewritten.value.push(ExtentChild { first_offset: original_first, end_offset: original_end, page: original });
            }
            children.retire(self.accounting)?;
            return Ok(rewritten);
        }
        if height == 1 && rewritten.value.len() == 1 {
            children.retire(self.accounting)?;
            return Ok(rewritten);
        }
        let result = self.write_internal_chunks(&rewritten.value).await;
        rewritten.retire(self.accounting)?;
        children.retire(self.accounting)?;
        result
    }

    async fn rewrite_leaf(
        &mut self, original: ObjectId, extents: Owned<Vec<Extent>>,
        original_first: u64, original_end: u64, output_end: u64,
    ) -> Result<Owned<Vec<ExtentChild>>, FsError> {
        validate_leaf(&extents.value, original_first, original_end).map_err(extent_error)?;
        let mut clipped = Owned::<Vec<Extent>>::allocate(
            extents.value.len().checked_add(usize::from(output_end > original_end)).ok_or(WorkError::Overflow)?,
            self.accounting,
        )?;
        clip_and_grow(&extents.value, output_end, original_end, &mut clipped.value).map_err(extent_error)?;
        let (first, last) = overlapping_patch_range(
            &self.patches.value, original_first, output_end, self.accounting,
        )?;
        let patches = self.patches.value.get(first..last).ok_or(WorkError::Overflow)?;
        let capacity = clipped.value.len().checked_add(patches.len().checked_mul(2).ok_or(WorkError::Overflow)?)
            .ok_or(WorkError::Overflow)?;
        let mut rewritten = Owned::<Vec<Extent>>::allocate(capacity, self.accounting)?;
        self.accounting.admit(WorkCounters {
            items_examined: usize_to_u64(clipped.value.len())
                .checked_add(usize_to_u64(patches.len()).checked_mul(3).ok_or(WorkError::Overflow)?)
                .ok_or(WorkError::Overflow)?,
            ..WorkCounters::UNCHARGED
        })?;
        let examined = apply_patches_linear(&clipped.value, patches, &mut rewritten.value).map_err(extent_error)?;
        self.charge_items(examined)?;
        coalesce(&mut rewritten.value).map_err(extent_error)?;
        let result = if rewritten.value == extents.value {
            let mut result = Owned::<Vec<ExtentChild>>::allocate(1, self.accounting)?;
            result.value.push(ExtentChild { first_offset: original_first, end_offset: original_end, page: original });
            Ok(result)
        } else {
            self.write_leaf_chunks(&rewritten.value).await
        };
        rewritten.retire(self.accounting)?;
        clipped.retire(self.accounting)?;
        extents.retire(self.accounting)?;
        result
    }

    async fn write_leaf_chunks(&mut self, extents: &[Extent]) -> Result<Owned<Vec<ExtentChild>>, FsError> {
        let mut result = Owned::<Vec<ExtentChild>>::allocate(0, self.accounting)?;
        let mut start = 0;
        while start < extents.len() {
            let (end_index, examined) = extent_leaf_chunk_end(extents, start, self.limits).map_err(extent_error)?;
            self.charge_items(examined)?;
            let chunk = extents.get(start..end_index).ok_or(WorkError::Overflow)?;
            let first = chunk.first().ok_or(WorkError::Overflow)?.offset;
            let end = extent_end(chunk.last().ok_or(WorkError::Overflow)?).map_err(extent_error)?;
            let mut items = Owned::<Vec<Extent>>::allocate(chunk.len(), self.accounting)?;
            items.value.extend_from_slice(chunk);
            let Owned { value, allocation, bytes } = items;
            let page = Owned::new(ExtentPage::Leaf(value), allocation, bytes);
            let written = self.write_page(&page.value).await;
            page.retire(self.accounting)?;
            result.ensure_for_push(usize::MAX, self.accounting)?;
            result.value.push(ExtentChild { first_offset: first, end_offset: end, page: written? });
            start = end_index;
        }
        Ok(result)
    }

    async fn write_internal_chunks(&mut self, children: &[ExtentChild]) -> Result<Owned<Vec<ExtentChild>>, FsError> {
        let mut cursor = 0;
        let mut chunks = 0;
        while cursor < children.len() {
            let (end, examined) = extent_internal_chunk_end(children, cursor, self.limits).map_err(extent_error)?;
            self.charge_items(examined)?;
            chunks += 1;
            cursor = end;
        }
        if children.len() > 1 && chunks >= children.len() {
            return Err(extent_error(ExtentMutationError::PageItemTooLarge));
        }
        let mut result = Owned::<Vec<ExtentChild>>::allocate(chunks, self.accounting)?;
        let mut start = 0;
        while start < children.len() {
            let (end_index, examined) = extent_internal_chunk_end(children, start, self.limits).map_err(extent_error)?;
            self.charge_items(examined)?;
            let chunk = children.get(start..end_index).ok_or(WorkError::Overflow)?;
            let first = chunk.first().ok_or(WorkError::Overflow)?.first_offset;
            let end = chunk.last().ok_or(WorkError::Overflow)?.end_offset;
            let mut items = Owned::<Vec<ExtentChild>>::allocate(chunk.len(), self.accounting)?;
            items.value.extend_from_slice(chunk);
            let Owned { value, allocation, bytes } = items;
            let page = Owned::new(ExtentPage::Internal(value), allocation, bytes);
            let written = self.write_page(&page.value).await;
            page.retire(self.accounting)?;
            result.value.push(ExtentChild { first_offset: first, end_offset: end, page: written? });
            start = end_index;
        }
        Ok(result)
    }

    async fn finish_root(&mut self, mut summaries: Owned<Vec<ExtentChild>>) -> Result<ObjectId, FsError> {
        if summaries.value.is_empty() {
            summaries.retire(self.accounting)?;
            return self.write_page(&ExtentPage::Leaf(Vec::new())).await;
        }
        let mut height = self.maximum_seen_height;
        while summaries.value.len() > 1 {
            height = height.checked_add(1).ok_or_else(|| extent_error(ExtentMutationError::HeightExceeded))?;
            if height > self.limits.maximum_page_height {
                return Err(extent_error(ExtentMutationError::HeightExceeded));
            }
            let next = self.write_internal_chunks(&summaries.value).await;
            summaries.retire(self.accounting)?;
            summaries = next?;
        }
        let root = summaries.value.first().ok_or(WorkError::Overflow)?.page;
        summaries.retire(self.accounting)?;
        Ok(root)
    }

    async fn read_page(&mut self, page: ObjectId) -> Result<Owned<ExtentPage>, FsError> {
        self.accounting.charge(WorkCounters { page_reads: 1, ..WorkCounters::UNCHARGED })?;
        let (read, retained) = super::file_read::read_object_with_accounting(
            self.store, page, self.limits.maximum_page_object_bytes(), self.accounting, self.cancellation,
        ).await?;
        let before = match &self.accounting {
            KernelAccounting::Standalone { allocations, .. } => Some(allocations.live_bytes()),
            KernelAccounting::BorrowedStandalone { allocations, .. } => Some(allocations.live_bytes()),
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { .. } => None,
        };
        let decoded = super::extent::decode_extent_page_with_accounting(&read, self.limits, self.accounting);
        if decoded.is_err() && let Some(before) = before {
            let ledger = match &mut self.accounting {
                KernelAccounting::Standalone { allocations, .. } => &mut *allocations,
                KernelAccounting::BorrowedStandalone { allocations, .. } => &mut **allocations,
                #[cfg(not(target_arch = "wasm32"))]
                KernelAccounting::NativeOriginal { .. } => unreachable!("standalone decoder custody"),
            };
            ledger.release(ledger.live_bytes().checked_sub(before).ok_or(WorkError::Overflow)?)
                .map_err(extent_allocation_error)?;
        }
        drop(read);
        super::file_read::release_standalone_read_bytes(retained, self.accounting)?;
        let (page, bytes, allocation) = decoded?;
        Ok(Owned::new(page, allocation, bytes))
    }

    fn charge_items(&mut self, count: u64) -> Result<(), FsError> {
        charge_items(self.accounting, count)
    }

    async fn write_page(&mut self, page: &ExtentPage) -> Result<ObjectId, FsError> {
        put_extent_page_with_accounting(self.store, page, self.limits, self.accounting, self.cancellation).await
    }
}

pub(crate) async fn put_extent_page_with_accounting<S: crate::AsyncObjectStore>(
    store: &S, page: &ExtentPage, limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<ObjectId, FsError> {
    accounting.check()?;
    let length = usize_to_u64(super::extent::extent_page_encoded_length(page, limits.maximum_page_items)?);
    if length > limits.maximum_page_object_bytes() {
        return Err(CanonicalDecodeError::ObjectTooLarge {
            observed: length, maximum: limits.maximum_page_object_bytes(),
        }.into());
    }
    accounting.admit(WorkCounters { page_writes: 1, ..WorkCounters::UNCHARGED })?;
    let (value, allocation) = super::codec::encode_owned(accounting, |observe, reserve_error| {
        super::extent::encode_extent_page(page, limits.maximum_page_items, observe, reserve_error)
    })?;
    let bytes = usize_to_u64(value.capacity());
    let encoded = Owned::new(value, allocation, bytes);
    put_encoded_with_accounting(store, ObjectKind::ExtentPage, encoded, accounting, cancellation).await
}

pub(crate) async fn put_encoded_with_accounting<S: crate::AsyncObjectStore>(
    store: &S, kind: ObjectKind, encoded: Owned<Vec<u8>>,
    accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<ObjectId, FsError> {
    accounting.check()?;
    accounting.admit(WorkCounters {
        page_writes: u64::from(kind != ObjectKind::BlobChunk), ..WorkCounters::UNCHARGED
    })?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = accounting.native_work() {
        let mut encoded = encoded;
        let allocation = match &mut encoded.allocation {
            super::allocation::KernelOwnedAllocation::NativeOriginal(allocation) => allocation,
            super::allocation::KernelOwnedAllocation::Standalone => return Err(extent_error(ExtentMutationError::AllocationFailed)),
        };
        let hashed = HashedObject::from_native_vec(kind, &mut encoded.value, allocation, original, cancellation)?;
        let object = hashed.object_id();
        accounting.charge(WorkCounters { page_writes: u64::from(kind != ObjectKind::BlobChunk), ..WorkCounters::UNCHARGED })?;
        store.put_hashed_with_native_work(hashed, original, cancellation).await
            .map_err(super::file_read::read_store_error)?;
        return Ok(object);
    }
    let hashing = WorkCounters {
        bytes_hashed: usize_to_u64(encoded.value.len()).checked_add(OBJECT_DIGEST_ENVELOPE_BYTES).ok_or(WorkError::Overflow)?,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(hashing)?;
    let Owned { value, allocation, bytes } = encoded;
    let hashed = HashedObject::new(kind, value.into());
    accounting.charge(hashing)?;
    accounting.charge(WorkCounters { page_writes: u64::from(kind != ObjectKind::BlobChunk), ..WorkCounters::UNCHARGED })?;
    let object = hashed.object_id();
    let receipt = store.put_hashed(hashed, accounting.remaining()?, cancellation).await;
    let spent = match &receipt { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
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
        let extent_end = extent_end(&extent)?;
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
        let extent = extents.get(index).ok_or(ExtentMutationError::PatchInvariant)?.clone();
        if output != 0 {
            let previous = extents.get_mut(output - 1).ok_or(ExtentMutationError::PatchInvariant)?;
            if equivalent_continuation(previous, &extent)? {
                previous.length = previous.length.checked_add(extent.length)
                    .ok_or(ExtentMutationError::RangeOverflow)?;
                continue;
            }
        }
        *extents.get_mut(output).ok_or(ExtentMutationError::PatchInvariant)? = extent;
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
    patches: &[Patch], start: u64, end: u64, accounting: &mut KernelAccounting<'_>,
) -> Result<(usize, usize), FsError> {
    let first = partition_point_with_accounting(patches, accounting, |patch| patch.end <= start)?;
    let after_last = partition_point_with_accounting(patches, accounting, |patch| patch.offset < end)?;
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
