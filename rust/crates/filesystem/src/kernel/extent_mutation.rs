//! Sparse persistent extent mutation without file-body materialization.

use super::allocation::{AllocationError, AllocationLedger, VisitedObjectSet};
use super::codec::DecodedPageKind;
use super::extent::{extent_page_decode_shape, extent_page_encoded_length};
use super::{
    CanonicalDecodeError, DecodeLimits, Extent, ExtentChild, ExtentKind, ExtentPage,
    decode_extent_page, encode_extent_page,
};
use crate::cancellation::CancellationToken;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{
    OBJECT_DIGEST_ENVELOPE_BYTES, ObjectId, ObjectKind, ObjectReadRetention, ObjectStoreError,
    object_digest,
};
use bytes::Bytes;
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
    patches: Vec<Patch>,
    final_size: u64,
    work: WorkCounters,
    live_allocation_bytes: u64,
}

#[derive(Clone, Debug)]
struct Summary {
    first: u64,
    end: u64,
    page: ObjectId,
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
    children: Vec<ExtentChild>,
    original_first: u64,
    original_end: u64,
    output_end: u64,
    height: u16,
    next_child: usize,
    rewritten: Vec<Summary>,
    logical_bytes: u64,
}

enum EnteredNode {
    Complete(Vec<Summary>),
    Internal(InternalFrame),
}

struct OwnedExtentPage {
    page: ExtentPage,
    logical_bytes: u64,
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

/// Admission bounds shared by asynchronous extent mutation operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExtentMutationOptions {
    /// Maximum accepted mutations in this ordered batch.
    pub maximum_mutations: u32,
    /// Canonical decode and authenticated-tree bounds.
    pub limits: DecodeLimits,
    /// Exact physical work ceiling.
    pub budget: WorkBudget,
}

/// Extent mutation failure retaining spent work and safe orphan writes.
pub type ExtentMutationFailure = OperationFailure<ExtentMutationError>;

struct Context<'a, S> {
    store: &'a S,
    limits: DecodeLimits,
    budget: WorkBudget,
    work: WorkCounters,
    allocations: AllocationLedger,
    visited: VisitedObjectSet,
    patches: Vec<Patch>,
    maximum_seen_height: u16,
    cancellation: &'a CancellationToken,
}

/// Applies ordered extent mutations by rewriting only intersecting frontiers.
///
/// Shrink operations discard whole later subtrees from parent bounds without
/// reading them. Growth follows only the final frontier and appends holes.
/// Content blobs are referenced, never read or materialized. Equivalent
/// adjacent spans are coalesced after ordered patch replay.
///
/// # Errors
///
/// Rejects malformed/excessive ranges and unsupported extension before backend
/// access, then fails closed on routing corruption, cycles, storage failure, or
/// work outside the admitted budget.
pub fn apply_extent_mutations<S: crate::ImmediateObjectStore>(
    store: &S,
    root: ObjectId,
    old_logical_bytes: u64,
    mutations: &[ExtentMutation],
    maximum_mutations: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
) -> Result<ExtentMutationReceipt, ExtentMutationFailure> {
    let cancellation = CancellationToken::new();
    crate::async_storage::poll_immediate(apply_extent_mutations_async(
        store,
        root,
        old_logical_bytes,
        mutations,
        ExtentMutationOptions {
            maximum_mutations,
            limits,
            budget,
        },
        &cancellation,
    ))
}

/// Asynchronously applies ordered extent mutations without blocking an executor.
///
/// The sync and async APIs execute this same iterative path-copy state machine.
/// Cancellation is checked by every backend operation and cannot publish a
/// mutable object because all writes are immutable and content addressed.
///
/// # Errors
///
/// Returns the same bounded, receipt-bearing failures as
/// [`apply_extent_mutations`], plus backend cancellation.
pub async fn apply_extent_mutations_async<S: crate::AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    old_logical_bytes: u64,
    mutations: &[ExtentMutation],
    options: ExtentMutationOptions,
    cancellation: &CancellationToken,
) -> Result<ExtentMutationReceipt, ExtentMutationFailure> {
    cancellation.check().map_err(|_| {
        OperationFailure::before_work(ExtentMutationError::Storage(ObjectStoreError::Cancelled))
    })?;
    let ExtentMutationOptions {
        maximum_mutations,
        limits,
        budget,
    } = options;
    validate_request(root, mutations, maximum_mutations, limits)?;
    let plan = compile_patch_plan(old_logical_bytes, mutations, budget)?;
    let final_size = plan.final_size;
    let mut allocations = AllocationLedger::default();
    let mut work = plan.work;
    allocations
        .claim_bytes(plan.live_allocation_bytes, 0, &mut work, budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    let maximum_visited = usize::try_from(limits.maximum_visited_pages)
        .map_err(|_| OperationFailure::new(ExtentMutationError::InvalidLimits, work))?;
    let visited = VisitedObjectSet::new(maximum_visited, &mut allocations, &mut work, budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    let mut context = Context {
        store,
        limits,
        budget,
        work,
        allocations,
        visited,
        patches: plan.patches,
        maximum_seen_height: 0,
        cancellation,
    };
    let summaries = context
        .rewrite(root, 0, old_logical_bytes, final_size)
        .await
        .map_err(|error| OperationFailure::new(error, context.work))?;
    let new_root = context
        .finish_root(summaries)
        .await
        .map_err(|error| OperationFailure::new(error, context.work))?;
    context
        .visited
        .release(&mut context.allocations)
        .map_err(|error| OperationFailure::new(error.into(), context.work))?;
    context
        .allocations
        .release(plan.live_allocation_bytes)
        .map_err(|error| OperationFailure::new(error.into(), context.work))?;
    Ok(ExtentMutationReceipt {
        root: new_root,
        logical_bytes: final_size,
        work: context.work,
    })
}

fn validate_request(
    root: ObjectId,
    mutations: &[ExtentMutation],
    maximum_mutations: u32,
    limits: DecodeLimits,
) -> Result<(), ExtentMutationFailure> {
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
        Some(error) => Err(OperationFailure::before_work(error)),
        None => Ok(()),
    }
}

fn compile_patch_plan(
    old_size: u64,
    mutations: &[ExtentMutation],
    budget: WorkBudget,
) -> Result<CompiledPatchPlan, ExtentMutationFailure> {
    let (final_size, raw_count, mut work) = admit_patch_plan(old_size, mutations, budget)?;
    if raw_count == 0 {
        return Ok(CompiledPatchPlan {
            patches: Vec::new(),
            final_size,
            work,
            live_allocation_bytes: 0,
        });
    }
    reserve_compile_scratch(raw_count, work, budget)?;
    let mut live_bytes = 0_u64;
    let mut raw = allocate_vec::<Patch>(raw_count, &mut work, budget, &mut live_bytes)?;
    let mut size = old_size;
    for mutation in mutations {
        charge_items(&mut work, budget, 1)?;
        match mutation {
            ExtentMutation::Replace {
                offset,
                length,
                kind,
                extend: _,
            } => {
                let end = offset.checked_add(*length).ok_or_else(|| {
                    OperationFailure::new(ExtentMutationError::RangeOverflow, work)
                })?;
                if *offset > size {
                    raw.push(Patch {
                        offset: size,
                        end: *offset,
                        kind: ExtentKind::Hole,
                    });
                }
                raw.push(Patch {
                    offset: *offset,
                    end,
                    kind: kind.clone(),
                });
                size = size.max(end);
            }
            ExtentMutation::Resize { logical_bytes } => {
                if *logical_bytes != size {
                    let start = size.min(*logical_bytes);
                    let end = size.max(*logical_bytes);
                    raw.push(Patch {
                        offset: start,
                        end,
                        kind: ExtentKind::Hole,
                    });
                    size = *logical_bytes;
                }
            }
        }
    }
    let patches = normalize_raw_patches(&raw, final_size, &mut work, budget, &mut live_bytes)?;
    let live_allocation_bytes = bytes_for::<Patch>(patches.capacity(), work)?;
    Ok(CompiledPatchPlan {
        patches,
        final_size,
        work,
        live_allocation_bytes,
    })
}

fn admit_patch_plan(
    old_size: u64,
    mutations: &[ExtentMutation],
    budget: WorkBudget,
) -> Result<(u64, usize, WorkCounters), ExtentMutationFailure> {
    let mut size = old_size;
    let mut raw_count = 0_usize;
    let mut work = WorkCounters::default();
    for mutation in mutations {
        charge_items(&mut work, budget, 1)?;
        match mutation {
            ExtentMutation::Replace {
                offset,
                length,
                kind,
                extend,
            } => {
                if *length == 0 {
                    return Err(OperationFailure::new(ExtentMutationError::EmptyRange, work));
                }
                validate_kind(kind, *length).map_err(|error| OperationFailure::new(error, work))?;
                let end = offset.checked_add(*length).ok_or_else(|| {
                    OperationFailure::new(ExtentMutationError::RangeOverflow, work)
                })?;
                if end > size && !extend {
                    return Err(OperationFailure::new(
                        ExtentMutationError::OutsideFile,
                        work,
                    ));
                }
                raw_count = raw_count
                    .checked_add(usize::from(*offset > size) + 1)
                    .ok_or_else(|| {
                        OperationFailure::new(ExtentMutationError::RangeOverflow, work)
                    })?;
                size = size.max(end);
            }
            ExtentMutation::Resize { logical_bytes } => {
                raw_count = raw_count
                    .checked_add(usize::from(*logical_bytes != size))
                    .ok_or_else(|| {
                        OperationFailure::new(ExtentMutationError::RangeOverflow, work)
                    })?;
                size = *logical_bytes;
            }
        }
    }
    Ok((size, raw_count, work))
}

fn reserve_compile_scratch(
    raw_count: usize,
    work: WorkCounters,
    budget: WorkBudget,
) -> Result<(), ExtentMutationFailure> {
    let coordinates = raw_count
        .checked_mul(2)
        .ok_or_else(|| OperationFailure::new(ExtentMutationError::RangeOverflow, work))?;
    let intervals = coordinates.saturating_sub(1);
    let capacities = [
        bytes_for::<Patch>(raw_count, work)?,
        bytes_for::<u64>(coordinates, work)?,
        bytes_for::<u64>(coordinates, work)?,
        bytes_for::<Option<usize>>(intervals, work)?,
        bytes_for::<usize>(intervals + 1, work)?,
        bytes_for::<Patch>(intervals, work)?,
    ];
    let bytes = capacities.into_iter().try_fold(0_u64, |total, bytes| {
        total
            .checked_add(bytes)
            .ok_or_else(|| OperationFailure::new(ExtentMutationError::RangeOverflow, work))
    })?;
    let mut reserved = work
        .checked_add(WorkCounters {
            allocation_operations: 6,
            ..WorkCounters::default()
        })
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    reserved.peak_allocation_bytes = reserved.peak_allocation_bytes.max(bytes);
    reserved
        .verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))
}

fn bytes_for<T>(count: usize, work: WorkCounters) -> Result<u64, ExtentMutationFailure> {
    count
        .checked_mul(size_of::<T>())
        .map(crate::foundation::usize_to_u64)
        .ok_or_else(|| OperationFailure::new(ExtentMutationError::RangeOverflow, work))
}

fn allocate_vec<T>(
    capacity: usize,
    work: &mut WorkCounters,
    budget: WorkBudget,
    live_bytes: &mut u64,
) -> Result<Vec<T>, ExtentMutationFailure> {
    let requested = bytes_for::<T>(capacity, *work)?;
    let next_live = live_bytes
        .checked_add(requested)
        .ok_or_else(|| OperationFailure::new(ExtentMutationError::RangeOverflow, *work))?;
    let mut prospective = work
        .checked_add(WorkCounters {
            allocation_operations: u64::from(capacity != 0),
            ..WorkCounters::default()
        })
        .map_err(|error| OperationFailure::new(error.into(), *work))?;
    prospective.peak_allocation_bytes = prospective.peak_allocation_bytes.max(next_live);
    prospective
        .verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), *work))?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(capacity)
        .map_err(|_| OperationFailure::new(ExtentMutationError::AllocationFailed, prospective))?;
    let actual = bytes_for::<T>(values.capacity(), prospective)?;
    *live_bytes = live_bytes
        .checked_add(actual)
        .ok_or_else(|| OperationFailure::new(ExtentMutationError::RangeOverflow, prospective))?;
    prospective.peak_allocation_bytes = prospective.peak_allocation_bytes.max(*live_bytes);
    prospective
        .verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), prospective))?;
    *work = prospective;
    Ok(values)
}

fn charge_items(
    work: &mut WorkCounters,
    budget: WorkBudget,
    count: u64,
) -> Result<(), ExtentMutationFailure> {
    let prospective = work
        .checked_add(WorkCounters {
            items_examined: count,
            ..WorkCounters::default()
        })
        .map_err(|error| OperationFailure::new(error.into(), *work))?;
    prospective
        .verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), *work))?;
    *work = prospective;
    Ok(())
}

fn charge_copied_bytes(
    work: &mut WorkCounters,
    budget: WorkBudget,
    bytes: u64,
) -> Result<(), ExtentMutationFailure> {
    let prospective = work
        .checked_add(WorkCounters {
            bytes_copied: bytes,
            ..WorkCounters::default()
        })
        .map_err(|error| OperationFailure::new(error.into(), *work))?;
    prospective
        .verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), *work))?;
    *work = prospective;
    Ok(())
}

fn normalize_raw_patches(
    raw: &[Patch],
    final_size: u64,
    work: &mut WorkCounters,
    budget: WorkBudget,
    live_bytes: &mut u64,
) -> Result<Vec<Patch>, ExtentMutationFailure> {
    let coordinate_capacity = raw
        .len()
        .checked_mul(2)
        .ok_or_else(|| OperationFailure::new(ExtentMutationError::RangeOverflow, *work))?;
    let mut coordinates = allocate_vec::<u64>(coordinate_capacity, work, budget, live_bytes)?;
    for patch in raw {
        charge_items(work, budget, 1)?;
        let start = patch.offset.min(final_size);
        let end = patch.end.min(final_size);
        if start < end {
            coordinates.push(start);
            coordinates.push(end);
        }
    }
    if coordinates.is_empty() {
        return Ok(Vec::new());
    }
    let mut scratch = allocate_vec::<u64>(coordinates.len(), work, budget, live_bytes)?;
    radix_sort(&mut coordinates, &mut scratch, work, budget)?;
    deduplicate_coordinates(&mut coordinates, work, budget)?;
    let interval_count = coordinates.len().saturating_sub(1);
    let mut assignments = allocate_vec::<Option<usize>>(interval_count, work, budget, live_bytes)?;
    charge_items(
        work,
        budget,
        u64::try_from(interval_count).unwrap_or(u64::MAX),
    )?;
    assignments.resize(interval_count, None);
    let mut parents = allocate_vec::<usize>(interval_count + 1, work, budget, live_bytes)?;
    charge_items(
        work,
        budget,
        u64::try_from(interval_count + 1).unwrap_or(u64::MAX),
    )?;
    parents.extend(0..=interval_count);
    for (patch_index, patch) in raw.iter().enumerate().rev() {
        charge_items(work, budget, 1)?;
        let start = patch.offset.min(final_size);
        let end = patch.end.min(final_size);
        if start >= end {
            continue;
        }
        let left = coordinate_index(&coordinates, start, work, budget)?;
        let right = coordinate_index(&coordinates, end, work, budget)?;
        let mut interval = find_next(&mut parents, left, work, budget)?;
        while interval < right {
            charge_items(work, budget, 1)?;
            assignments[interval] = Some(patch_index);
            let successor = find_next(&mut parents, interval + 1, work, budget)?;
            parents[interval] = successor;
            interval = successor;
        }
    }
    let mut result = allocate_vec::<Patch>(interval_count, work, budget, live_bytes)?;
    for (interval, assignment) in assignments.into_iter().enumerate() {
        charge_items(work, budget, 1)?;
        let Some(patch_index) = assignment else {
            continue;
        };
        let source = &raw[patch_index];
        let candidate = Patch {
            offset: coordinates[interval],
            end: coordinates[interval + 1],
            kind: offset_kind(&source.kind, coordinates[interval] - source.offset)
                .map_err(|error| OperationFailure::new(error, *work))?,
        };
        if let Some(previous) = result.last_mut()
            && patch_continues(previous, &candidate)
                .map_err(|error| OperationFailure::new(error, *work))?
        {
            previous.end = candidate.end;
        } else {
            result.push(candidate);
        }
    }
    Ok(result)
}

fn radix_sort(
    values: &mut Vec<u64>,
    scratch: &mut Vec<u64>,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<(), ExtentMutationFailure> {
    scratch.resize(values.len(), 0);
    for byte in 0..8_u32 {
        charge_items(
            work,
            budget,
            u64::try_from(values.len())
                .unwrap_or(u64::MAX)
                .saturating_mul(2),
        )?;
        charge_copied_bytes(work, budget, bytes_for::<u64>(values.len(), *work)?)?;
        let shift = byte * 8;
        let mut counts = [0_usize; 256];
        for value in values.iter() {
            counts[usize::from(((value >> shift) & 0xff) as u8)] += 1;
        }
        let mut offset = 0_usize;
        for count in &mut counts {
            let current = *count;
            *count = offset;
            offset += current;
        }
        for value in values.iter() {
            let bucket = usize::from(((value >> shift) & 0xff) as u8);
            scratch[counts[bucket]] = *value;
            counts[bucket] += 1;
        }
        std::mem::swap(values, scratch);
    }
    Ok(())
}

fn deduplicate_coordinates(
    values: &mut Vec<u64>,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<(), ExtentMutationFailure> {
    charge_items(
        work,
        budget,
        u64::try_from(values.len()).unwrap_or(u64::MAX),
    )?;
    let mut write = 1_usize;
    for read in 1..values.len() {
        if values[read] != values[write - 1] {
            values[write] = values[read];
            write += 1;
        }
    }
    values.truncate(write);
    Ok(())
}

fn coordinate_index(
    values: &[u64],
    target: u64,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<usize, ExtentMutationFailure> {
    let mut left = 0_usize;
    let mut right = values.len();
    while left < right {
        charge_items(work, budget, 1)?;
        let middle = left + (right - left) / 2;
        match values[middle].cmp(&target) {
            std::cmp::Ordering::Less => left = middle + 1,
            std::cmp::Ordering::Greater => right = middle,
            std::cmp::Ordering::Equal => return Ok(middle),
        }
    }
    Err(OperationFailure::new(
        ExtentMutationError::PatchInvariant,
        *work,
    ))
}

fn find_next(
    parents: &mut [usize],
    start: usize,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<usize, ExtentMutationFailure> {
    let mut root = start;
    while parents[root] != root {
        charge_items(work, budget, 1)?;
        root = parents[root];
    }
    let mut current = start;
    while parents[current] != current {
        charge_items(work, budget, 1)?;
        let next = parents[current];
        parents[current] = root;
        current = next;
    }
    Ok(root)
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
    children: &[Summary],
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

impl<S: crate::AsyncObjectStore> Context<'_, S> {
    async fn rewrite(
        &mut self,
        page_id: ObjectId,
        original_first: u64,
        original_end: u64,
        output_end: u64,
    ) -> Result<Vec<Summary>, ExtentMutationError> {
        let mut stack = Vec::new();
        let mut request = NodeRequest {
            page: page_id,
            original_first,
            original_end,
            output_end,
            height: 1,
        };
        'traverse: loop {
            let entered = self.enter_node(request).await?;
            let mut result = match entered {
                EnteredNode::Complete(result) => result,
                EnteredNode::Internal(mut frame) => {
                    if let Some(next) = self.advance_frame(&mut frame)? {
                        stack.push(frame);
                        request = next;
                        continue;
                    }
                    self.finish_frame(frame).await?
                }
            };
            loop {
                let Some(mut frame) = stack.pop() else {
                    return Ok(result);
                };
                frame.rewritten.extend(result);
                if let Some(next) = self.advance_frame(&mut frame)? {
                    stack.push(frame);
                    request = next;
                    continue 'traverse;
                }
                result = self.finish_frame(frame).await?;
            }
        }
    }

    async fn enter_node(
        &mut self,
        request: NodeRequest,
    ) -> Result<EnteredNode, ExtentMutationError> {
        if request.height > self.limits.maximum_page_height {
            return Err(ExtentMutationError::HeightExceeded);
        }
        if request.output_end <= request.original_first {
            return Ok(EnteredNode::Complete(Vec::new()));
        }
        self.maximum_seen_height = self.maximum_seen_height.max(request.height);
        let visited = self.visited.insert(
            request.page,
            &mut self.allocations,
            &mut self.work,
            self.budget,
        )?;
        if !visited.inserted {
            return Err(ExtentMutationError::CycleOrAlias);
        }
        let decoded = self.read_page(request.page).await?;
        match decoded.page {
            ExtentPage::Leaf(extents) => {
                let result = self
                    .rewrite_leaf(
                        request.page,
                        extents,
                        request.original_first,
                        request.original_end,
                        request.output_end,
                    )
                    .await;
                self.allocations.release(decoded.logical_bytes)?;
                Ok(EnteredNode::Complete(result?))
            }
            ExtentPage::Internal(children) => {
                validate_children(&children, request.original_first, request.original_end)?;
                Ok(EnteredNode::Internal(InternalFrame {
                    original: request.page,
                    children,
                    original_first: request.original_first,
                    original_end: request.original_end,
                    output_end: request.output_end,
                    height: request.height,
                    next_child: 0,
                    rewritten: Vec::new(),
                    logical_bytes: decoded.logical_bytes,
                }))
            }
        }
    }

    fn advance_frame(
        &mut self,
        frame: &mut InternalFrame,
    ) -> Result<Option<NodeRequest>, ExtentMutationError> {
        while frame.next_child < frame.children.len() {
            let index = frame.next_child;
            frame.next_child += 1;
            let child = &frame.children[index];
            if child.first_offset >= frame.output_end {
                self.charge_items(1)?;
                frame.next_child = frame.children.len();
                break;
            }
            let mut child_output_end = child.end_offset.min(frame.output_end);
            if index + 1 == frame.children.len() && frame.output_end > frame.original_end {
                child_output_end = frame.output_end;
            }
            let (patch_touched, comparisons) =
                has_overlapping_patch(&self.patches, child.first_offset, child_output_end);
            self.charge_items(comparisons.saturating_add(1))?;
            let touched = child_output_end != child.end_offset || patch_touched;
            if touched {
                return Ok(Some(NodeRequest {
                    page: child.page,
                    original_first: child.first_offset,
                    original_end: child.end_offset,
                    output_end: child_output_end,
                    height: frame
                        .height
                        .checked_add(1)
                        .ok_or(ExtentMutationError::HeightExceeded)?,
                }));
            }
            frame.rewritten.push(Summary {
                first: child.first_offset,
                end: child.end_offset,
                page: child.page,
            });
        }
        Ok(None)
    }

    async fn finish_frame(
        &mut self,
        frame: InternalFrame,
    ) -> Result<Vec<Summary>, ExtentMutationError> {
        let result = if frame.rewritten.is_empty() {
            Ok(Vec::new())
        } else if unchanged(&frame.rewritten, &frame.children) {
            Ok(vec![Summary {
                first: frame.original_first,
                end: frame.original_end,
                page: frame.original,
            }])
        } else if frame.height == 1 && frame.rewritten.len() == 1 {
            Ok(frame.rewritten)
        } else {
            self.write_internal_chunks(&frame.rewritten).await
        };
        self.allocations.release(frame.logical_bytes)?;
        result
    }

    async fn rewrite_leaf(
        &mut self,
        original: ObjectId,
        extents: Vec<Extent>,
        original_first: u64,
        original_end: u64,
        output_end: u64,
    ) -> Result<Vec<Summary>, ExtentMutationError> {
        validate_leaf(&extents, original_first, original_end)?;
        if output_end <= original_first {
            return Ok(Vec::new());
        }
        let original_extents = extents.clone();
        let mut rewritten = clip_and_grow(extents, output_end, original_end)?;
        let (first_patch, after_last_patch, comparisons) =
            overlapping_patch_range(&self.patches, original_first, output_end);
        self.work = self.work.checked_add(WorkCounters {
            items_examined: comparisons,
            ..WorkCounters::default()
        })?;
        self.work.verify(self.budget)?;
        let (next, examined) =
            apply_patches_linear(rewritten, &self.patches[first_patch..after_last_patch])?;
        rewritten = next;
        self.work = self.work.checked_add(WorkCounters {
            items_examined: examined,
            ..WorkCounters::default()
        })?;
        self.work.verify(self.budget)?;
        coalesce(&mut rewritten)?;
        if rewritten == original_extents {
            return Ok(vec![Summary {
                first: original_first,
                end: original_end,
                page: original,
            }]);
        }
        self.write_leaf_chunks(&rewritten).await
    }

    async fn write_leaf_chunks(
        &mut self,
        extents: &[Extent],
    ) -> Result<Vec<Summary>, ExtentMutationError> {
        let mut result = Vec::new();
        let mut start = 0_usize;
        while start < extents.len() {
            let (end_index, examined) = extent_leaf_chunk_end(extents, start, self.limits)?;
            self.charge_items(examined)?;
            let chunk = &extents[start..end_index];
            let first = chunk[0].offset;
            let end = extent_end(&chunk[chunk.len() - 1])?;
            result.push(Summary {
                first,
                end,
                page: self.write_page(&ExtentPage::Leaf(chunk.to_vec())).await?,
            });
            start = end_index;
        }
        Ok(result)
    }

    async fn write_internal_chunks(
        &mut self,
        children: &[Summary],
    ) -> Result<Vec<Summary>, ExtentMutationError> {
        let mut cursor = 0_usize;
        let mut chunks = 0_usize;
        while cursor < children.len() {
            let (end, examined) = extent_internal_chunk_end(children, cursor, self.limits)?;
            self.charge_items(examined)?;
            chunks += 1;
            cursor = end;
        }
        if children.len() > 1 && chunks >= children.len() {
            return Err(ExtentMutationError::PageItemTooLarge);
        }
        let mut result = Vec::new();
        let mut start = 0_usize;
        while start < children.len() {
            let (end_index, examined) = extent_internal_chunk_end(children, start, self.limits)?;
            self.charge_items(examined)?;
            let chunk = &children[start..end_index];
            let first = chunk[0].first;
            let end = chunk[chunk.len() - 1].end;
            let page = ExtentPage::Internal(
                chunk
                    .iter()
                    .map(|child| ExtentChild {
                        first_offset: child.first,
                        end_offset: child.end,
                        page: child.page,
                    })
                    .collect(),
            );
            result.push(Summary {
                first,
                end,
                page: self.write_page(&page).await?,
            });
            start = end_index;
        }
        Ok(result)
    }

    async fn finish_root(
        &mut self,
        mut summaries: Vec<Summary>,
    ) -> Result<ObjectId, ExtentMutationError> {
        if summaries.is_empty() {
            return self.write_page(&ExtentPage::Leaf(Vec::new())).await;
        }
        let mut height = self.maximum_seen_height;
        while summaries.len() > 1 {
            height = height
                .checked_add(1)
                .ok_or(ExtentMutationError::HeightExceeded)?;
            if height > self.limits.maximum_page_height {
                return Err(ExtentMutationError::HeightExceeded);
            }
            summaries = self.write_internal_chunks(&summaries).await?;
        }
        Ok(summaries[0].page)
    }

    async fn read_page(&mut self, page: ObjectId) -> Result<OwnedExtentPage, ExtentMutationError> {
        let prospective = self.work.checked_add(WorkCounters {
            page_reads: 1,
            ..WorkCounters::default()
        })?;
        let mut remaining = prospective.remaining(self.budget)?;
        remaining.peak_allocation_bytes = self
            .budget
            .peak_allocation_bytes
            .checked_sub(self.allocations.live_bytes())
            .ok_or(WorkError::Overflow)?;
        let receipt = match crate::AsyncObjectStore::read(
            self.store,
            page,
            self.limits.maximum_page_object_bytes(),
            remaining,
            self.cancellation,
        )
        .await
        {
            Ok(receipt) => receipt,
            Err(failure) => {
                self.work =
                    merge_backend_work(prospective, *failure.work, self.allocations.live_bytes())?;
                return Err(ExtentMutationError::Storage(failure.error));
            }
        };
        self.work = merge_backend_work(prospective, receipt.work, self.allocations.live_bytes())?;
        self.work.verify(self.budget)?;
        let retained_bytes = match receipt.value.retention {
            ObjectReadRetention::Shared => 0,
            ObjectReadRetention::Owned { logical_bytes } => logical_bytes,
        };
        self.allocations
            .claim_bytes(retained_bytes, 0, &mut self.work, self.budget)?;
        let shape = extent_page_decode_shape(&receipt.value, self.limits)?;
        self.charge_items(u64::try_from(shape.items).unwrap_or(u64::MAX))?;
        let item_bytes = match shape.kind {
            DecodedPageKind::Leaf => size_of::<Extent>(),
            DecodedPageKind::Internal => size_of::<ExtentChild>(),
        };
        let logical_bytes = shape
            .items
            .checked_mul(item_bytes)
            .map(crate::foundation::usize_to_u64)
            .ok_or(ExtentMutationError::AllocationFailed)?;
        self.allocations.claim_bytes(
            logical_bytes,
            u64::from(logical_bytes != 0),
            &mut self.work,
            self.budget,
        )?;
        let decoded = match decode_extent_page(&receipt.value, self.limits) {
            Ok(decoded) => decoded,
            Err(error) => {
                self.allocations.release(logical_bytes)?;
                return Err(ExtentMutationError::Decode(error));
            }
        };
        self.allocations.release(retained_bytes)?;
        Ok(OwnedExtentPage {
            page: decoded,
            logical_bytes,
        })
    }

    fn charge_items(&mut self, count: u64) -> Result<(), ExtentMutationError> {
        let prospective = self.work.checked_add(WorkCounters {
            items_examined: count,
            ..WorkCounters::default()
        })?;
        prospective.verify(self.budget)?;
        self.work = prospective;
        Ok(())
    }

    async fn write_page(&mut self, page: &ExtentPage) -> Result<ObjectId, ExtentMutationError> {
        let encoded_length = extent_page_encoded_length(page, self.limits.maximum_page_items)?;
        let encoded_bytes = crate::foundation::usize_to_u64(encoded_length);
        let maximum_page_bytes = self.limits.maximum_page_object_bytes();
        if encoded_bytes > maximum_page_bytes {
            return Err(ExtentMutationError::Decode(
                CanonicalDecodeError::ObjectTooLarge {
                    observed: encoded_bytes,
                    maximum: maximum_page_bytes,
                },
            ));
        }
        self.work
            .checked_add(WorkCounters {
                bytes_encoded: encoded_bytes,
                ..WorkCounters::default()
            })?
            .verify(self.budget)?;
        self.allocations.claim_bytes(
            encoded_bytes,
            u64::from(encoded_bytes != 0),
            &mut self.work,
            self.budget,
        )?;
        let encoded = encode_extent_page(page, self.limits.maximum_page_items)?;
        if u64::try_from(encoded.capacity()).unwrap_or(u64::MAX) != encoded_bytes {
            return Err(ExtentMutationError::AllocationFailed);
        }
        self.work = self.work.checked_add(WorkCounters {
            bytes_encoded: encoded_bytes,
            ..WorkCounters::default()
        })?;
        self.work.verify(self.budget)?;
        let hashed_work = self.work.checked_add(WorkCounters {
            bytes_hashed: encoded_bytes
                .checked_add(OBJECT_DIGEST_ENVELOPE_BYTES)
                .ok_or(ExtentMutationError::RangeOverflow)?,
            ..WorkCounters::default()
        })?;
        hashed_work.verify(self.budget)?;
        let object = ObjectId {
            kind: ObjectKind::ExtentPage,
            digest: object_digest(ObjectKind::ExtentPage, &encoded),
        };
        self.work = hashed_work;
        let prospective = self.work.checked_add(WorkCounters {
            page_writes: 1,
            ..WorkCounters::default()
        })?;
        let remaining = prospective.remaining(self.budget)?;
        let receipt = match crate::AsyncObjectStore::put(
            self.store,
            object,
            Bytes::from(encoded),
            remaining,
            self.cancellation,
        )
        .await
        {
            Ok(receipt) => receipt,
            Err(failure) => {
                self.allocations.release(encoded_bytes)?;
                self.work =
                    merge_backend_work(prospective, *failure.work, self.allocations.live_bytes())?;
                return Err(ExtentMutationError::Storage(failure.error));
            }
        };
        self.work = prospective.checked_add(receipt.work)?;
        self.work.verify(self.budget)?;
        self.allocations.release(encoded_bytes)?;
        Ok(object)
    }
}

fn merge_backend_work(
    prior: WorkCounters,
    mut backend: WorkCounters,
    live_bytes: u64,
) -> Result<WorkCounters, WorkError> {
    let simultaneous_peak = live_bytes
        .checked_add(backend.peak_allocation_bytes)
        .ok_or(WorkError::Overflow)?;
    backend.peak_allocation_bytes = 0;
    let mut merged = prior.checked_add(backend)?;
    merged.peak_allocation_bytes = merged.peak_allocation_bytes.max(simultaneous_peak);
    Ok(merged)
}

fn clip_and_grow(
    extents: Vec<Extent>,
    output_end: u64,
    original_end: u64,
) -> Result<Vec<Extent>, ExtentMutationError> {
    let mut result = Vec::new();
    for mut extent in extents {
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
    Ok(result)
}

fn apply_patches_linear(
    extents: Vec<Extent>,
    patches: &[Patch],
) -> Result<(Vec<Extent>, u64), ExtentMutationError> {
    let capacity = extents
        .len()
        .checked_add(patches.len().saturating_mul(2))
        .ok_or(ExtentMutationError::RangeOverflow)?;
    let mut result = Vec::with_capacity(capacity);
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
    Ok((result, examined))
}

fn coalesce(extents: &mut Vec<Extent>) -> Result<(), ExtentMutationError> {
    let mut result: Vec<Extent> = Vec::with_capacity(extents.len());
    for extent in extents.drain(..) {
        if let Some(previous) = result.last_mut()
            && equivalent_continuation(previous, &extent)?
        {
            previous.length = previous
                .length
                .checked_add(extent.length)
                .ok_or(ExtentMutationError::RangeOverflow)?;
            continue;
        }
        result.push(extent);
    }
    *extents = result;
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

fn unchanged(summaries: &[Summary], children: &[ExtentChild]) -> bool {
    summaries.len() == children.len()
        && summaries.iter().zip(children).all(|(left, right)| {
            left.first == right.first_offset
                && left.end == right.end_offset
                && left.page == right.page
        })
}

fn overlapping_patch_range(patches: &[Patch], start: u64, end: u64) -> (usize, usize, u64) {
    let (first, first_comparisons) = lower_bound(patches, |patch| patch.end <= start);
    let (after_last, last_comparisons) = lower_bound(patches, |patch| patch.offset < end);
    (
        first,
        after_last.max(first),
        first_comparisons.saturating_add(last_comparisons),
    )
}

fn has_overlapping_patch(patches: &[Patch], start: u64, end: u64) -> (bool, u64) {
    let (first, after_last, comparisons) = overlapping_patch_range(patches, start, end);
    (first < after_last, comparisons)
}

fn lower_bound(patches: &[Patch], before: impl Fn(&Patch) -> bool) -> (usize, u64) {
    let mut left = 0;
    let mut right = patches.len();
    let mut comparisons = 0_u64;
    while left < right {
        comparisons = comparisons.saturating_add(1);
        let middle = left + (right - left) / 2;
        if before(&patches[middle]) {
            left = middle + 1;
        } else {
            right = middle;
        }
    }
    (left, comparisons)
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
}

impl From<AllocationError> for ExtentMutationError {
    fn from(error: AllocationError) -> Self {
        match error {
            AllocationError::Work(error) => Self::Work(error),
            AllocationError::Overflow | AllocationError::ReleaseInvariant => {
                Self::Work(WorkError::Overflow)
            }
            AllocationError::InvalidCapacity
            | AllocationError::CapacityExceeded
            | AllocationError::AllocationFailed => Self::AllocationFailed,
        }
    }
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/extent_mutation.rs"]
mod tests;
