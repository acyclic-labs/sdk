//! Bounded authenticated named-attribute lookup.

use super::allocation::{AllocationError, AllocationLedger, VisitedObjectSet};
use super::attribute::attribute_page_decode_shape;
use super::attribute_mutation::AttributeFormat;
use super::codec::DecodedPageKind;
use super::persistent_batch;
use super::{
    AttributeChild, AttributeEntry, AttributeName, AttributePage, CanonicalDecodeError,
    DecodeLimits, decode_attribute_page,
};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{ObjectId, ObjectKind, ObjectReadRetention, ObjectStoreError};
use std::mem::size_of;
use thiserror::Error;

/// Exact authenticated named-attribute lookup result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttributeLookup {
    /// Matching complete entry, or authenticated absence.
    pub entry: Option<AttributeEntry>,
    /// Exact backend, traversal, and logical allocation work.
    pub work: WorkCounters,
}

/// Original-order named attributes from one shared authenticated frontier batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttributeBatchLookup {
    /// One explicit present/absent result per requested attribute name.
    pub entries: Vec<Option<AttributeEntry>>,
    /// Exact shared traversal, backend, copy, and allocation work.
    pub work: WorkCounters,
}

/// Looks up named attributes while reading each distinct frontier once.
///
/// # Errors
///
/// Rejects empty/oversized batches and fails closed on malformed routing,
/// cycles, storage failure, decoding, cancellation, or unadmitted work.
pub fn lookup_attributes<S: crate::ImmediateObjectStore>(
    store: &S,
    root: ObjectId,
    names: &[AttributeName],
    maximum_queries: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
) -> Result<AttributeBatchLookup, AttributeLookupFailure> {
    persistent_batch::lookup::<S, AttributeFormat>(
        store,
        root,
        names,
        maximum_queries,
        limits,
        budget,
    )
    .map(to_batch)
    .map_err(map_batch_failure)
}

/// Asynchronously executes the same batch as [`lookup_attributes`].
///
/// # Errors
///
/// Returns identical typed failures, including cooperative cancellation.
pub async fn lookup_attributes_async<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    names: &[AttributeName],
    maximum_queries: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<AttributeBatchLookup, AttributeLookupFailure> {
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
    .map(to_batch)
    .map_err(map_batch_failure)
}

fn to_batch(receipt: persistent_batch::Receipt<AttributeEntry>) -> AttributeBatchLookup {
    AttributeBatchLookup {
        entries: receipt.values,
        work: receipt.work,
    }
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
///
/// # Errors
///
/// Fails closed on malformed routing, cycles, bounds, storage failures,
/// cancellation, malformed canonical pages, or unadmitted work.
pub fn lookup_attribute<S: crate::ImmediateObjectStore>(
    store: &S,
    root: ObjectId,
    name: &AttributeName,
    limits: DecodeLimits,
    budget: WorkBudget,
) -> Result<AttributeLookup, AttributeLookupFailure> {
    crate::async_storage::poll_immediate(lookup_attribute_async(
        store,
        root,
        name,
        limits,
        budget,
        &CancellationToken::new(),
    ))
}

/// Asynchronously executes the same attribute frontier as [`lookup_attribute`].
///
/// # Errors
///
/// Returns the same typed failures, including cancellation before each backend
/// boundary.
pub async fn lookup_attribute_async<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    name: &AttributeName,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<AttributeLookup, AttributeLookupFailure> {
    if cancellation.is_cancelled() {
        return Err(OperationFailure::before_work(
            AttributeLookupError::Cancelled,
        ));
    }
    if root.kind != ObjectKind::AttributePage {
        return Err(OperationFailure::before_work(
            AttributeLookupError::WrongRootKind,
        ));
    }
    if !limits.page_limits_valid(1) {
        return Err(OperationFailure::before_work(
            AttributeLookupError::InvalidLimits,
        ));
    }

    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let mut visited = VisitedObjectSet::new(
        usize::from(limits.maximum_page_height).min(
            usize::try_from(limits.maximum_visited_pages)
                .map_err(|_| OperationFailure::before_work(AttributeLookupError::InvalidLimits))?,
        ),
        &mut allocations,
        &mut work,
        budget,
    )
    .map_err(|error| failed(map_allocation(error), work))?;
    let mut page = root;
    let mut routing = RoutingState::default();

    for _ in 0..limits.maximum_page_height {
        let inserted = visited
            .insert(page, &mut allocations, &mut work, budget)
            .map_err(|error| failed(map_allocation(error), work))?;
        if !inserted.inserted {
            return Err(failed(AttributeLookupError::CycleOrAlias, work));
        }

        let decoded = read_page(
            store,
            page,
            limits,
            budget,
            cancellation,
            &mut allocations,
            &mut work,
        )
        .await?;
        let decoded_bytes = decoded.logical_bytes;

        match decoded.page {
            AttributePage::Leaf(mut entries) => {
                validate_leaf(&entries, routing.lower.as_ref(), routing.upper.as_ref())
                    .map_err(|error| failed(error, work))?;
                let (position, comparisons) = search_entries(&entries, name);
                charge_items(&mut work, comparisons, budget)?;
                let entry = position.ok().map(|index| entries.swap_remove(index));
                work = work
                    .checked_add(WorkCounters {
                        items_returned: u64::from(entry.is_some()),
                        ..WorkCounters::default()
                    })
                    .map_err(|error| failed(error.into(), work))?;
                work.verify(budget)
                    .map_err(|error| failed(error.into(), work))?;
                allocations
                    .release(routing.logical_bytes)
                    .and_then(|()| allocations.release(decoded_bytes))
                    .map_err(|error| failed(map_allocation(error), work))?;
                visited
                    .release(&mut allocations)
                    .map_err(|error| failed(map_allocation(error), work))?;
                return Ok(AttributeLookup { entry, work });
            }
            AttributePage::Internal(children) => {
                page = advance_internal(
                    children,
                    name,
                    &mut routing,
                    decoded_bytes,
                    &mut allocations,
                    &mut work,
                    budget,
                )?;
            }
        }
    }
    Err(failed(AttributeLookupError::HeightExceeded, work))
}

#[derive(Default)]
struct RoutingState {
    lower: Option<AttributeName>,
    upper: Option<AttributeName>,
    logical_bytes: u64,
}

fn advance_internal(
    children: Vec<AttributeChild>,
    name: &AttributeName,
    routing: &mut RoutingState,
    decoded_bytes: u64,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<ObjectId, AttributeLookupFailure> {
    validate_children(&children, routing.lower.as_ref(), routing.upper.as_ref())
        .map_err(|error| failed(error, *work))?;
    let (partition, comparisons) = upper_bound_children(&children, name);
    charge_items(work, comparisons, budget)?;
    let selected = partition.saturating_sub(1);
    let mut selected_children = children.into_iter().skip(selected);
    let child = selected_children
        .next()
        .ok_or_else(|| failed(AttributeLookupError::InvalidRouting, *work))?;
    let next_upper = selected_children.next().map(|next| next.first_name);
    let inherited_upper = routing.upper.take();
    let old_lower_bytes = nested_name_bytes(routing.lower.as_ref());
    let inherited_upper_bytes = nested_name_bytes(inherited_upper.as_ref());
    let new_lower_bytes = nested_name_bytes(Some(&child.first_name));
    let decoded_retained = nested_name_bytes(next_upper.as_ref())
        .checked_add(new_lower_bytes)
        .ok_or_else(|| failed(AttributeLookupError::AllocationFailed, *work))?;
    allocations
        .release(old_lower_bytes)
        .and_then(|()| {
            if next_upper.is_some() {
                allocations.release(inherited_upper_bytes)
            } else {
                Ok(())
            }
        })
        .and_then(|()| {
            decoded_bytes
                .checked_sub(decoded_retained)
                .ok_or(AllocationError::ReleaseInvariant)
                .and_then(|released| allocations.release(released))
        })
        .map_err(|error| failed(map_allocation(error), *work))?;
    routing.lower = Some(child.first_name);
    routing.upper = next_upper.or(inherited_upper);
    routing.logical_bytes = new_lower_bytes
        .checked_add(nested_name_bytes(routing.upper.as_ref()))
        .ok_or_else(|| failed(AttributeLookupError::AllocationFailed, *work))?;
    Ok(child.page)
}

fn nested_name_bytes(name: Option<&AttributeName>) -> u64 {
    name.map_or(0, |bound| {
        u64::try_from(bound.as_bytes().len()).unwrap_or(u64::MAX)
    })
}

struct DecodedAttributePage {
    page: AttributePage,
    logical_bytes: u64,
}

async fn read_page<S: AsyncObjectStore>(
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
        match merge_backend_work(prospective, *failure.work, allocations.live_bytes()) {
            Ok(spent) => failed(map_storage(failure.error), spent),
            Err(error) => failed(error.into(), prospective),
        }
    })?;
    *work = merge_backend_work(prospective, receipt.work, allocations.live_bytes())
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
    charge_items(work, u64::try_from(shape.items).unwrap_or(u64::MAX), budget)?;
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
        DecodedPageKind::Internal => shape.items.checked_mul(size_of::<AttributeChild>()),
    }
    .map(crate::foundation::usize_to_u64)
    .ok_or_else(|| failed(AttributeLookupError::AllocationFailed, *work))?;
    let logical_bytes = container_bytes
        .checked_add(shape.nested_bytes)
        .ok_or_else(|| failed(AttributeLookupError::AllocationFailed, *work))?;
    allocations
        .claim_bytes(logical_bytes, u64::from(logical_bytes != 0), work, budget)
        .map_err(|error| failed(map_allocation(error), *work))?;
    let page = decode_attribute_page(&receipt.value, limits)
        .map_err(|error| failed(error.into(), *work))?;
    allocations
        .release(retained_bytes)
        .map_err(|error| failed(map_allocation(error), *work))?;
    Ok(DecodedAttributePage {
        page,
        logical_bytes,
    })
}

fn search_entries(entries: &[AttributeEntry], name: &AttributeName) -> (Result<usize, usize>, u64) {
    let mut left = 0_usize;
    let mut right = entries.len();
    let mut comparisons = 0_u64;
    while left < right {
        comparisons = comparisons.saturating_add(1);
        let middle = left + (right - left) / 2;
        match entries[middle].name.cmp(name) {
            std::cmp::Ordering::Less => left = middle + 1,
            std::cmp::Ordering::Greater => right = middle,
            std::cmp::Ordering::Equal => return (Ok(middle), comparisons),
        }
    }
    (Err(left), comparisons)
}

fn upper_bound_children(children: &[AttributeChild], name: &AttributeName) -> (usize, u64) {
    let mut left = 0_usize;
    let mut right = children.len();
    let mut comparisons = 0_u64;
    while left < right {
        comparisons = comparisons.saturating_add(1);
        let middle = left + (right - left) / 2;
        if children[middle].first_name <= *name {
            left = middle + 1;
        } else {
            right = middle;
        }
    }
    (left, comparisons)
}

fn validate_leaf(
    entries: &[AttributeEntry],
    lower: Option<&AttributeName>,
    upper: Option<&AttributeName>,
) -> Result<(), AttributeLookupError> {
    if lower.is_some() && entries.first().map(|entry| &entry.name) != lower {
        return Err(AttributeLookupError::ChildBoundsMismatch);
    }
    if let Some(upper) = upper
        && entries.last().is_some_and(|entry| entry.name >= *upper)
    {
        return Err(AttributeLookupError::ChildBoundsMismatch);
    }
    Ok(())
}

fn validate_children(
    children: &[AttributeChild],
    lower: Option<&AttributeName>,
    upper: Option<&AttributeName>,
) -> Result<(), AttributeLookupError> {
    if lower.is_some() && children.first().map(|child| &child.first_name) != lower {
        return Err(AttributeLookupError::ChildBoundsMismatch);
    }
    if let Some(upper) = upper
        && children
            .last()
            .is_some_and(|child| child.first_name >= *upper)
    {
        return Err(AttributeLookupError::ChildBoundsMismatch);
    }
    Ok(())
}

fn charge_items(
    work: &mut WorkCounters,
    count: u64,
    budget: WorkBudget,
) -> Result<(), AttributeLookupFailure> {
    let prospective = work
        .checked_add(WorkCounters {
            items_examined: count,
            ..WorkCounters::default()
        })
        .map_err(|error| failed(error.into(), *work))?;
    prospective
        .verify(budget)
        .map_err(|error| failed(error.into(), *work))?;
    *work = prospective;
    Ok(())
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
