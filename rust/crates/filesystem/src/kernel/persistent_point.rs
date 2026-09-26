//! Single-key authenticated lookup through a persistent B+tree that shares
//! decoded pages with every other kernel reader.

use super::DecodeLimits;
use super::persistent_batch::Error;
use super::persistent_btree::{Child, Format, Page};
use super::persistent_io;
use crate::async_storage::{AsyncObjectStore, DecodedCacheKey};
use crate::cancellation::CancellationToken;
use crate::heap_future::in_heap;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters};
use crate::storage::{ObjectId, ObjectStoreError};
use std::collections::HashSet;
use std::sync::Arc;

/// Exact value for one key, or its authenticated absence, and the work spent.
pub(crate) struct Receipt<V> {
    pub(crate) value: Option<V>,
    pub(crate) work: WorkCounters,
}

pub(crate) type Failure = OperationFailure<Error>;

/// Looks `key` up from `root` one page per level.
///
/// A page already decoded by any reader of `store` is used without reading
/// or decoding it again; a page read here is offered to the decoded cache.
/// Each level charges one page read plus the backend's own work for a page
/// it reads, verifies its routing bounds against the parent, and rejects
/// cycles and excess height.
pub(crate) async fn lookup_async<S, F>(
    store: &S,
    root: ObjectId,
    key: &F::Key,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<Receipt<F::Value>, Failure>
where
    S: AsyncObjectStore,
    F: Format,
{
    if root.kind != F::kind() {
        return Err(OperationFailure::before_work(Error::WrongRootKind));
    }
    if !limits.page_limits_valid(1) {
        return Err(OperationFailure::before_work(Error::InvalidLimits));
    }
    let mut work = WorkCounters::default();
    let mut visited = HashSet::new();
    let mut page = root;
    let mut lower: Option<F::Key> = None;
    let mut upper: Option<F::Key> = None;
    loop {
        if cancellation.is_cancelled() {
            return Err(OperationFailure::new(Error::Cancelled, work));
        }
        if visited.len() >= usize::from(limits.maximum_page_height) {
            return Err(OperationFailure::new(Error::HeightExceeded, work));
        }
        if !visited.insert(page) {
            return Err(OperationFailure::new(Error::CycleOrAlias, work));
        }
        // Charging the page read in place is exactly `checked_add` then
        // `remaining`'s verification, and leaves `work` unchanged on failure.
        work.charge(&PAGE_READ, &budget)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        let key_for_cache = DecodedCacheKey::new::<Page<F>>(page, limits);
        let cached = store
            .decoded_cache_get(key_for_cache)
            .map_err(|error| OperationFailure::new(Error::Storage(error), work))?;
        let decoded = if let Some(cached) = cached {
            cached.value.downcast::<Page<F>>().map_err(|_| {
                OperationFailure::new(Error::Storage(ObjectStoreError::Corrupt), work)
            })?
        } else {
            // A cache miss awaits the backend in its own heap frame, so a
            // lookup the decoded cache answers, which is nearly every one,
            // never builds or moves that future.
            let (decoded, spent) = in_heap(|| {
                read_uncached::<S, F>(
                    store,
                    page,
                    key_for_cache,
                    limits,
                    work,
                    budget,
                    cancellation,
                )
            })
            .await?;
            work = spent;
            decoded
        };
        match &*decoded {
            Page::Leaf(values) => {
                validate_leaf::<F>(values, lower.as_ref(), upper.as_ref())
                    .map_err(|error| OperationFailure::new(error, work))?;
                let value = values
                    .binary_search_by(|value| F::key(value).cmp(key))
                    .ok()
                    .and_then(|index| values.get(index).cloned());
                return Ok(Receipt { value, work });
            }
            Page::Internal(children) => {
                validate_children::<F>(children, lower.as_ref(), upper.as_ref())
                    .map_err(|error| OperationFailure::new(error, work))?;
                let selected = children
                    .partition_point(|child| child.first <= *key)
                    .saturating_sub(1);
                let child = children
                    .get(selected)
                    .ok_or_else(|| OperationFailure::new(Error::InvalidRouting, work))?;
                lower = Some(child.first.clone());
                if let Some(next) = children.get(selected + 1) {
                    upper = Some(next.first.clone());
                }
                page = child.page;
            }
        }
    }
}

/// One authenticated page read, as charged before any backend work.
const PAGE_READ: WorkCounters = WorkCounters {
    page_reads: 1,
    ..WorkCounters::UNCHARGED
};

/// One page the decoded cache did not hold, read, decoded, and offered to
/// the cache, with the work spent: `prospective` (which already charges the
/// page read) plus the backend's work.
async fn read_uncached<S, F>(
    store: &S,
    page: ObjectId,
    key: DecodedCacheKey,
    limits: DecodeLimits,
    prospective: WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(Arc<Page<F>>, WorkCounters), Failure>
where
    S: AsyncObjectStore,
    F: Format,
{
    let storage =
        |error: ObjectStoreError, work| OperationFailure::new(Error::Storage(error), work);
    let remaining = prospective
        .remaining(budget)
        .map_err(|error| OperationFailure::new(error.into(), prospective))?;
    let receipt = store
        .read(
            page,
            limits.maximum_page_object_bytes(),
            remaining,
            cancellation,
        )
        .await
        .map_err(|failure| match prospective.checked_add(*failure.work) {
            Ok(spent) => storage(failure.error, spent),
            Err(error) => OperationFailure::new(error.into(), prospective),
        })?;
    let work = prospective
        .checked_add(receipt.work)
        .map_err(|error| OperationFailure::new(error.into(), prospective))?;
    work.verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    let decoded = persistent_io::decoded_cache_value::<F>(&receipt.value.bytes, limits)
        .map_err(|error| OperationFailure::new(Error::Decode(error), work))?;
    let decoded = match store
        .decoded_cache_admit(key, decoded)
        .map_err(|error| storage(error, work))?
    {
        crate::async_storage::DecodedCacheAdmission::Shared(value)
        | crate::async_storage::DecodedCacheAdmission::Uncached(value) => value,
    };
    let decoded = decoded
        .value
        .downcast::<Page<F>>()
        .map_err(|_| storage(ObjectStoreError::Corrupt, work))?;
    Ok((decoded, work))
}

/// Rejects a leaf whose first key differs from its parent's routing key or
/// whose last key reaches the next sibling's.
pub(crate) fn validate_leaf<F: Format>(
    values: &[F::Value],
    lower: Option<&F::Key>,
    upper: Option<&F::Key>,
) -> Result<(), Error> {
    if lower.is_some() && values.first().map(F::key) != lower {
        return Err(Error::ChildBoundsMismatch);
    }
    if let Some(upper) = upper
        && values.last().is_some_and(|value| F::key(value) >= upper)
    {
        return Err(Error::ChildBoundsMismatch);
    }
    Ok(())
}

/// Rejects an internal page whose routing keys disagree with its parent's.
pub(crate) fn validate_children<F: Format>(
    children: &[Child<F::Key>],
    lower: Option<&F::Key>,
    upper: Option<&F::Key>,
) -> Result<(), Error> {
    if lower.is_some() && children.first().map(|child| &child.first) != lower {
        return Err(Error::ChildBoundsMismatch);
    }
    if let Some(upper) = upper
        && children.last().is_some_and(|child| child.first >= *upper)
    {
        return Err(Error::ChildBoundsMismatch);
    }
    Ok(())
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/persistent_point.rs"]
mod tests;
