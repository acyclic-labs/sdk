//! Bounded authenticated named-attribute lookup.

use crate::kernel::Child;
use super::allocation::{AllocationError, AllocationLedger, KernelAccounting, KernelOwnedAllocation, VisitedObjectSet};
use super::attribute::attribute_page_decode_shape;
use super::attribute_mutation::AttributeFormat;
use super::codec::DecodedPageKind;
use super::persistent_batch;
use super::persistent_btree::within_bounds;
use super::persistent_point::{Owned, retire_allocation, split_allocation};
use super::search::{counted_binary_search, counted_partition_point};
use super::{
    AttributeEntry, AttributeName, AttributePage, CanonicalDecodeError,
    DecodeLimits, decode_attribute_page,
};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::foundation::usize_to_u64;
use crate::performance::{OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError};
use crate::storage::{ObjectId, ObjectKind, ObjectReadRetention, ObjectStoreError};
use std::mem::size_of;
use thiserror::Error;

/// Looks up named attributes while reading each distinct frontier once.
pub async fn lookup_attributes<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    names: &[AttributeName],
    maximum_queries: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<Vec<Option<AttributeEntry>>>, AttributeLookupFailure> {
    persistent_batch::lookup_async::<S, AttributeFormat>(
        store,
        root,
        names,
        maximum_queries,
        limits,
        budget,
        cancellation,
    )
    .await
    .map_err(map_batch_failure)
}

fn map_batch_failure(failure: persistent_batch::Failure) -> AttributeLookupFailure {
    OperationFailure::new(map_batch_error(failure.error), *failure.work)
}

fn map_batch_error(error: persistent_batch::Error) -> AttributeLookupError {
    match error {
        persistent_batch::Error::Cancelled => AttributeLookupError::Cancelled,
        persistent_batch::Error::Empty => AttributeLookupError::EmptyBatch,
        persistent_batch::Error::TooManyQueries => AttributeLookupError::TooManyQueries,
        persistent_batch::Error::WrongRootKind => AttributeLookupError::WrongRootKind,
        persistent_batch::Error::InvalidLimits => AttributeLookupError::InvalidLimits,
        persistent_batch::Error::HeightExceeded => AttributeLookupError::HeightExceeded,
        persistent_batch::Error::CycleOrAlias => AttributeLookupError::CycleOrAlias,
        persistent_batch::Error::ChildBoundsMismatch => AttributeLookupError::ChildBoundsMismatch,
        persistent_batch::Error::InvalidRouting => AttributeLookupError::InvalidRouting,
        persistent_batch::Error::AllocationFailed => AttributeLookupError::AllocationFailed,
        persistent_batch::Error::Storage(ObjectStoreError::Cancelled) => {
            AttributeLookupError::Cancelled
        }
        persistent_batch::Error::Storage(error) => error.into(),
        persistent_batch::Error::Decode(error) => error.into(),
        persistent_batch::Error::Work(error) => error.into(),
    }
}

/// Named-attribute lookup failure retaining all exact spent work.
pub type AttributeLookupFailure = OperationFailure<AttributeLookupError>;

/// Looks up one exact named attribute through one authenticated frontier.
pub async fn lookup_attribute<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    name: &AttributeName,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<Option<AttributeEntry>>, AttributeLookupFailure> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    lookup_attribute_async_with_accounting(store, root, name, limits, &mut accounting, cancellation)
        .await
        .map(|(lookup, _)| lookup)
        .map_err(|error| failed(project_scalar_error(error), accounting.spent()))
}

pub(crate) async fn lookup_attribute_async_with_accounting<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    name: &AttributeName,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(OperationReceipt<Option<AttributeEntry>>, KernelOwnedAllocation), crate::FsError> {
    cancellation.check()?;
    accounting.check()?;
    if root.kind != ObjectKind::AttributePage {
        return Err(AttributeLookupError::WrongRootKind.into());
    }
    if !limits.page_limits_valid(1) {
        return Err(AttributeLookupError::InvalidLimits.into());
    }
    let maximum_visited = usize::from(limits.maximum_page_height).min(
        usize::try_from(limits.maximum_visited_pages)
            .map_err(|_| AttributeLookupError::InvalidLimits)?,
    );
    let mut visited = VisitedObjectSet::new_with_accounting(maximum_visited, accounting)
        .map_err(map_visited_error)?;
    let mut page = root;
    let mut routing = RoutingState::default();
    let result: Result<(Option<AttributeEntry>, KernelOwnedAllocation), crate::FsError> = async {
        for _ in 0..limits.maximum_page_height {
            let inserted = VisitedObjectSet::insert_with_accounting(&mut visited, page, accounting)
                .map_err(map_visited_error)?;
            if !inserted.inserted {
                return Err(AttributeLookupError::CycleOrAlias.into());
            }
            let decoded = read_page(store, page, limits, cancellation, accounting).await?;
            let DecodedAttributePage { page: decoded_page, mut allocation, logical_bytes } = decoded;
            match decoded_page {
                AttributePage::Leaf(mut entries) => {
                    let step = (|| -> Result<_, crate::FsError> {
                        let first = entries.first().map(|entry| &entry.name);
                        let last = entries.last().map(|entry| &entry.name);
                        if !within_bounds(first, last, routing.lower.as_ref().map(|bound| &bound.value), routing.upper.as_ref().map(|bound| &bound.value)) {
                            return Err(AttributeLookupError::ChildBoundsMismatch.into());
                        }
                        let position = search_entries_with_accounting(&entries, name, accounting)?;
                        accounting.charge(WorkCounters { items_returned: u64::from(position.is_ok()), ..WorkCounters::default() })?;
                        let entry_bytes = position.ok().map_or(0, |index| {
                            entries.get(index).map_or(0, |entry| owned_name_bytes(&entry.name, &allocation))
                        });
                        let selected_allocation = split_allocation(&mut allocation, entry_bytes)?;
                        let entry = position.ok().map(|index| entries.swap_remove(index));
                        Ok((entry, selected_allocation, entry_bytes))
                    })();
                    drop(entries);
                    let released = logical_bytes.checked_sub(step.as_ref().map_or(0, |(_, _, bytes)| *bytes))
                        .ok_or(WorkError::Overflow)?;
                    retire_allocation(allocation, released, accounting)?;
                    let (entry, selected_allocation, _) = step?;
                    return Ok((entry, selected_allocation));
                }
                AttributePage::Internal(children) => {
                    page = advance_internal(children, name, &mut routing, logical_bytes, allocation, accounting)?;
                }
            }
        }
        Err(AttributeLookupError::HeightExceeded.into())
    }.await;
    routing.retire(accounting)?;
    visited.retire(accounting).map_err(map_visited_error)?;
    result.map(|(value, allocation)| (OperationReceipt { value, work: accounting.spent() }, allocation))
}

#[derive(Default)]
struct RoutingState {
    lower: Option<Owned<AttributeName>>,
    upper: Option<Owned<AttributeName>>,
}

impl RoutingState {
    fn retire(self, accounting: &mut KernelAccounting<'_>) -> Result<(), crate::FsError> {
        if let Some(lower) = self.lower { lower.retire(accounting)?; }
        if let Some(upper) = self.upper { upper.retire(accounting)?; }
        Ok(())
    }
}

fn advance_internal(
    children: Vec<Child<AttributeName>>,
    name: &AttributeName,
    routing: &mut RoutingState,
    decoded_bytes: u64,
    mut allocation: KernelOwnedAllocation,
    accounting: &mut KernelAccounting<'_>,
) -> Result<ObjectId, crate::FsError> {
    let first = children.first().map(|child| &child.first);
    let last = children.last().map(|child| &child.first);
    let step = (|| -> Result<_, crate::FsError> {
        if !within_bounds(first, last, routing.lower.as_ref().map(|bound| &bound.value), routing.upper.as_ref().map(|bound| &bound.value)) {
            return Err(AttributeLookupError::ChildBoundsMismatch.into());
        }
        let partition = upper_bound_children_with_accounting(&children, name, accounting)?;
        let selected = partition.saturating_sub(1);
        let child = children.get(selected).ok_or(AttributeLookupError::InvalidRouting)?;
        let lower_bytes = routing_name_bytes(&child.first, &allocation);
        let upper_bytes = children.get(selected + 1)
            .map(|next| routing_name_bytes(&next.first, &allocation));
        let retained = lower_bytes.checked_add(upper_bytes.unwrap_or(0)).ok_or(WorkError::Overflow)?;
        decoded_bytes.checked_sub(retained).ok_or(WorkError::Overflow)?;
        Ok((selected, lower_bytes, upper_bytes, retained))
    })();
    let (selected, lower_bytes, upper_bytes, retained) = match step {
        Ok(step) => step,
        Err(error) => {
            drop(children);
            retire_allocation(allocation, decoded_bytes, accounting)?;
            return Err(error);
        }
    };
    let mut selected_children = children.into_iter().skip(selected);
    let child = selected_children.next().ok_or(AttributeLookupError::InvalidRouting)?;
    let next_upper = selected_children.next().map(|next| next.first);
    drop(selected_children);
    let next_page = child.page;
    let mut lower = Owned::new(child.first, KernelOwnedAllocation::Standalone, lower_bytes);
    let mut upper = next_upper.zip(upper_bytes)
        .map(|(name, bytes)| Owned::new(name, KernelOwnedAllocation::Standalone, bytes));
    lower.allocation = split_allocation(&mut allocation, lower_bytes)?;
    if let Some(upper) = &mut upper {
        upper.allocation = split_allocation(&mut allocation, upper.bytes)?;
    }
    if let Some(previous) = routing.lower.replace(lower) { previous.retire(accounting)?; }
    if let Some(upper) = upper {
        if let Some(previous) = routing.upper.replace(upper) { previous.retire(accounting)?; }
    }
    retire_allocation(allocation, decoded_bytes.checked_sub(retained).ok_or(WorkError::Overflow)?, accounting)?;
    Ok(next_page)
}

fn owned_name_bytes(name: &AttributeName, allocation: &KernelOwnedAllocation) -> u64 {
    match allocation {
        KernelOwnedAllocation::Standalone => 0,
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(_) => usize_to_u64(name.retained_bytes()),
    }
}

fn routing_name_bytes(name: &AttributeName, allocation: &KernelOwnedAllocation) -> u64 {
    match allocation {
        KernelOwnedAllocation::Standalone => usize_to_u64(name.as_bytes().len()),
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(_) => usize_to_u64(name.retained_bytes()),
    }
}

struct DecodedAttributePage {
    page: AttributePage,
    allocation: KernelOwnedAllocation,
    logical_bytes: u64,
}

async fn read_page<S: AsyncObjectStore>(
    store: &S,
    page: ObjectId,
    limits: DecodeLimits,
    cancellation: &CancellationToken,
    accounting: &mut KernelAccounting<'_>,
) -> Result<DecodedAttributePage, crate::FsError> {
    match accounting {
        KernelAccounting::Standalone { allocations, work, budget, .. } => {
            read_page_standalone(store, page, limits, *budget, cancellation, allocations, work)
                .await.map_err(|failure| { *work = *failure.work; failure.error.into() })
        }
        KernelAccounting::BorrowedStandalone { allocations, work, budget, .. } => {
            read_page_standalone(store, page, limits, *budget, cancellation, allocations, work)
                .await.map_err(|failure| { **work = *failure.work; failure.error.into() })
        }
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { .. } => {
            cancellation.check()?;
            accounting.charge(WorkCounters { page_reads: 1, ..WorkCounters::default() })?;
            let original = accounting.native_work().expect("native accounting retains its original work");
            let read = store.read_with_native_work(page, limits.maximum_page_object_bytes(), original, cancellation)
                .await.map_err(|error| match error {
                    ObjectStoreError::Native(error) => crate::FsError::NativeKernel(error),
                    error => crate::FsError::AttributeLookup(map_storage(error)),
                })?;
            let mut scope = super::file_read::OriginalReadAccounting::new(accounting)?;
            let decoded = super::attribute::decode_attribute_page(&read, limits, &mut |step| -> Result<(), crate::FsError> { (&mut |step| scope.observe(step)
                    .map_err(super::file_read::ReadDecodeError::into_original_fs_error))(step) });
            let allocation = scope.into_allocation();
            drop(read);
            match decoded {
                Ok(page) => {
                    let logical_bytes = match &allocation {
                        KernelOwnedAllocation::NativeOriginal(allocation) => allocation.bytes(),
                        KernelOwnedAllocation::Standalone => unreachable!("original decoder retains original custody"),
                    };
                    Ok(DecodedAttributePage { page, allocation, logical_bytes })
                }
                Err(error) => { drop(allocation); Err(error) }
            }
        }
    }
}

async fn read_page_standalone<S: AsyncObjectStore>(
    store: &S,
    page: ObjectId,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
) -> Result<DecodedAttributePage, AttributeLookupFailure> {
    let prospective = work
        .checked_add(WorkCounters {
            page_reads: 1,
            ..WorkCounters::default()
        })
        .map_err(|error| failed(error.into(), *work))?;
    let mut remaining = prospective
        .remaining(budget)
        .map_err(|error| failed(error.into(), *work))?;
    remaining.peak_allocation_bytes = budget
        .peak_allocation_bytes
        .checked_sub(allocations.live_bytes())
        .ok_or_else(|| failed(AttributeLookupError::Work(WorkError::Overflow), *work))?;
    let receipt = AsyncObjectStore::read(
        store,
        page,
        limits.maximum_page_object_bytes(),
        remaining,
        cancellation,
    )
    .await
    .map_err(|failure| {
        match prospective.with_backend(*failure.work, allocations.live_bytes()) {
            Ok(spent) => failed(map_storage(failure.error), spent),
            Err(error) => failed(error.into(), prospective),
        }
    })?;
    *work = prospective
        .with_backend(receipt.work, allocations.live_bytes())
        .map_err(|error| failed(error.into(), prospective))?;
    work.verify(budget)
        .map_err(|error| failed(error.into(), *work))?;

    let retained_bytes = match receipt.value.retention {
        ObjectReadRetention::Shared => 0,
        ObjectReadRetention::Owned { logical_bytes } => logical_bytes,
    };
    allocations
        .claim_bytes(retained_bytes, 0, work, budget)
        .map_err(|error| failed(map_allocation(error), *work))?;
    let shape = attribute_page_decode_shape(&receipt.value, limits)
        .map_err(|error| failed(error.into(), *work))?;
    charge_items(work, usize_to_u64(shape.items), budget)?;
    let decoded_work = work
        .checked_add(WorkCounters {
            bytes_copied: shape.nested_bytes,
            ..WorkCounters::default()
        })
        .map_err(|error| failed(error.into(), *work))?;
    decoded_work
        .verify(budget)
        .map_err(|error| failed(error.into(), *work))?;
    *work = decoded_work;
    let container_bytes = match shape.kind {
        DecodedPageKind::Leaf => shape.items.checked_mul(size_of::<AttributeEntry>()),
        DecodedPageKind::Internal => shape.items.checked_mul(size_of::<Child<AttributeName>>()),
    }
    .map(usize_to_u64)
    .ok_or_else(|| failed(AttributeLookupError::AllocationFailed, *work))?;
    let logical_bytes = container_bytes
        .checked_add(shape.nested_bytes)
        .ok_or_else(|| failed(AttributeLookupError::AllocationFailed, *work))?;
    allocations
        .claim_bytes(logical_bytes, u64::from(logical_bytes != 0), work, budget)
        .map_err(|error| failed(map_allocation(error), *work))?;
    let page = decode_attribute_page(&receipt.value, limits, &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))
        .map_err(|error| failed(error.into(), *work))?;
    allocations
        .release(retained_bytes)
        .map_err(|error| failed(map_allocation(error), *work))?;
    Ok(DecodedAttributePage {
        page,
        logical_bytes,
        allocation: KernelOwnedAllocation::Standalone,
    })
}

fn project_scalar_error(error: crate::FsError) -> AttributeLookupError {
    match error {
        crate::FsError::AttributeLookup(error) => error,
        crate::FsError::Cancelled(_) => AttributeLookupError::Cancelled,
        crate::FsError::Work(error) => AttributeLookupError::Work(error),
        crate::FsError::Object(error) => map_storage(error),
        crate::FsError::Decode(error) => AttributeLookupError::Decode(error),
        #[cfg(not(target_arch = "wasm32"))]
        crate::FsError::NativeKernel(error) => AttributeLookupError::Storage(ObjectStoreError::Native(error)),
        _ => unreachable!("scalar attribute leaves return only typed attribute, object, decode, work or cancellation errors"),
    }
}

fn map_visited_error(error: crate::FsError) -> crate::FsError {
    match error {
        crate::FsError::PendingMutationAllocationFailed => AttributeLookupError::AllocationFailed.into(),
        error => error,
    }
}

fn search_entries(entries: &[AttributeEntry], name: &AttributeName) -> (Result<usize, usize>, u64) {
    counted_binary_search(entries, |entry| entry.name.cmp(name))
}

fn upper_bound_children(children: &[Child<AttributeName>], name: &AttributeName) -> (usize, u64) {
    counted_partition_point(children, |child| child.first <= *name)
}

fn search_entries_with_accounting(
    entries: &[AttributeEntry], name: &AttributeName, accounting: &mut KernelAccounting<'_>,
) -> Result<Result<usize, usize>, crate::FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    if accounting.native_work().is_some() {
        return super::search::binary_search_with_accounting(entries, accounting, |entry| entry.name.cmp(name));
    }
    let (position, comparisons) = search_entries(entries, name);
    accounting.charge(WorkCounters { items_examined: comparisons, ..WorkCounters::UNCHARGED })?;
    Ok(position)
}

fn upper_bound_children_with_accounting(
    children: &[Child<AttributeName>], name: &AttributeName, accounting: &mut KernelAccounting<'_>,
) -> Result<usize, crate::FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    if accounting.native_work().is_some() {
        return super::search::partition_point_with_accounting(children, accounting, |child| child.first <= *name);
    }
    let (partition, comparisons) = upper_bound_children(children, name);
    accounting.charge(WorkCounters { items_examined: comparisons, ..WorkCounters::UNCHARGED })?;
    Ok(partition)
}

fn charge_items(
    work: &mut WorkCounters,
    count: u64,
    budget: WorkBudget,
) -> Result<(), AttributeLookupFailure> {
    work.charge_items(count, &budget)
        .map_err(|error| failed(error.into(), *work))
}

fn map_allocation(error: AllocationError) -> AttributeLookupError {
    match error {
        AllocationError::Work(error) => AttributeLookupError::Work(error),
        AllocationError::Overflow
        | AllocationError::ReleaseInvariant
        | AllocationError::InvalidCapacity
        | AllocationError::CapacityExceeded
        | AllocationError::AllocationFailed => AttributeLookupError::AllocationFailed,
    }
}

fn map_storage(error: ObjectStoreError) -> AttributeLookupError {
    match error {
        ObjectStoreError::Cancelled => AttributeLookupError::Cancelled,
        error => AttributeLookupError::Storage(error),
    }
}

fn failed(error: AttributeLookupError, work: WorkCounters) -> AttributeLookupFailure {
    OperationFailure::new(error, work)
}

/// Authenticated named-attribute lookup failures.
#[derive(Debug, Error)]
pub enum AttributeLookupError {
    /// Cancellation occurred before a storage boundary.
    #[error("attribute lookup was cancelled")]
    Cancelled,
    /// Batch lookup requires at least one name.
    #[error("attribute lookup batch is empty")]
    EmptyBatch,
    /// Batch lookup exceeds its explicit query limit.
    #[error("attribute lookup batch exceeds its admitted query bound")]
    TooManyQueries,
    /// Root is not an attribute page.
    #[error("attribute lookup root is not an attribute page")]
    WrongRootKind,
    /// Traversal/page limits are invalid.
    #[error("attribute lookup limits are invalid")]
    InvalidLimits,
    /// Traversal exceeded the admitted height.
    #[error("attribute tree exceeds its admitted height")]
    HeightExceeded,
    /// Authenticated graph cycles or aliases.
    #[error("attribute tree contains a cycle or alias")]
    CycleOrAlias,
    /// A parent route does not match child bounds.
    #[error("attribute child bounds mismatch")]
    ChildBoundsMismatch,
    /// No child can route the requested name.
    #[error("attribute tree routing is invalid")]
    InvalidRouting,
    /// Bounded scratch allocation failed.
    #[error("attribute lookup scratch allocation failed")]
    AllocationFailed,
    /// Immutable object storage failed.
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    /// Canonical attribute page failed decoding.
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
    /// Work accounting overflowed or exceeded the admitted budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/attribute_access.rs"]
mod tests;
