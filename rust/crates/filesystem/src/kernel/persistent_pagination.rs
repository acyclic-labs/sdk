//! Shared bounded cursor pagination for authenticated persistent B+trees.

use super::allocation::{AllocationError, AllocationLedger, LogicalVecCapacity, VisitedObjectSet};
use super::persistent_btree::{Child, Format, Page};
use super::persistent_io::{self, OwnedPage};
use super::{CanonicalDecodeError, DecodeLimits};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{ObjectId, ObjectReadRequest, ObjectStoreError};
use std::marker::PhantomData;
use std::mem::size_of;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Receipt<V> {
    pub(crate) values: Vec<V>,
    pub(crate) has_more: bool,
    pub(crate) next_request: Option<ObjectReadRequest>,
    pub(crate) work: WorkCounters,
}

pub(crate) type Failure = OperationFailure<Error>;

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("persistent B+tree pagination was cancelled")]
    Cancelled,
    #[error("persistent B+tree page limit must be non-zero")]
    ZeroLimit,
    #[error("persistent B+tree page limit cannot be represented")]
    LimitOverflow,
    #[error("persistent B+tree root has the wrong object kind")]
    WrongRootKind,
    #[error("persistent B+tree limits are invalid")]
    InvalidLimits,
    #[error("persistent B+tree exceeds its admitted height")]
    HeightExceeded,
    #[error("persistent B+tree contains a cycle or alias")]
    CycleOrAlias,
    #[error("persistent B+tree child bounds do not match its page")]
    ChildBoundsMismatch,
    #[error("persistent B+tree traversal state is invalid")]
    TraversalState,
    #[error("persistent B+tree allocation failed")]
    AllocationFailed,
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
    #[error(transparent)]
    Work(#[from] WorkError),
}

struct Pending<K> {
    page: ObjectId,
    lower: Option<K>,
    upper: Option<K>,
    height: u16,
    nested_bytes: u64,
}

pub(crate) fn paginate<S, F>(
    store: &S,
    root: ObjectId,
    after: Option<&F::Key>,
    maximum_values: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
) -> Result<Receipt<F::Value>, Failure>
where
    S: crate::ImmediateObjectStore,
    F: Format,
{
    crate::async_storage::poll_immediate(paginate_async::<S, F>(
        store,
        root,
        after,
        maximum_values,
        limits,
        budget,
        &CancellationToken::new(),
    ))
}

pub(crate) async fn paginate_async<S, F>(
    store: &S,
    root: ObjectId,
    after: Option<&F::Key>,
    maximum_values: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<Receipt<F::Value>, Failure>
where
    S: AsyncObjectStore,
    F: Format,
{
    if cancellation.is_cancelled() {
        return Err(OperationFailure::before_work(Error::Cancelled));
    }
    let mut machine = Machine::<F>::new(root, after, maximum_values, limits, budget)?;
    while machine.values.len() < machine.target && !machine.pending.is_empty() {
        if cancellation.is_cancelled() {
            return Err(failed(Error::Cancelled, machine.work));
        }
        let pending = machine.pop_pending()?;
        machine.visit(pending.page)?;
        let decoded = machine.read_page(store, pending.page, cancellation).await?;
        machine.accept(decoded, pending)?;
    }
    machine.finish()
}

struct Machine<'a, F: Format> {
    after: Option<&'a F::Key>,
    maximum_values: usize,
    target: usize,
    maximum_traversal_pages: usize,
    limits: DecodeLimits,
    budget: WorkBudget,
    pending: Vec<Pending<F::Key>>,
    pending_capacity: LogicalVecCapacity,
    visited: VisitedObjectSet,
    values: Vec<F::Value>,
    values_container_bytes: u64,
    values_nested_bytes: u64,
    successor_page: Option<ObjectId>,
    allocations: AllocationLedger,
    work: WorkCounters,
    format: PhantomData<F>,
}

impl<'a, F: Format> Machine<'a, F> {
    fn new(
        root: ObjectId,
        after: Option<&'a F::Key>,
        maximum_values: u32,
        limits: DecodeLimits,
        budget: WorkBudget,
    ) -> Result<Self, Failure> {
        if root.kind != F::kind() {
            return Err(OperationFailure::before_work(Error::WrongRootKind));
        }
        if maximum_values == 0 {
            return Err(OperationFailure::before_work(Error::ZeroLimit));
        }
        if !limits.page_limits_valid(1) {
            return Err(OperationFailure::before_work(Error::InvalidLimits));
        }
        let maximum_values = usize::try_from(maximum_values)
            .map_err(|_| OperationFailure::before_work(Error::LimitOverflow))?;
        let target = maximum_values
            .checked_add(1)
            .ok_or_else(|| OperationFailure::before_work(Error::LimitOverflow))?;
        let configured_maximum = usize::try_from(limits.maximum_visited_pages)
            .map_err(|_| OperationFailure::before_work(Error::InvalidLimits))?;
        // Every valid non-root child contains at least one value. At most one
        // cursor-selected subtree can be exhausted before the requested values,
        // and each retained value/witness traverses no more than one frontier
        // per level. Size traversal state from that operation-local proof, not
        // from the volume-wide object-count ceiling.
        let maximum_traversal_pages = target
            .checked_add(1)
            .and_then(|value| value.checked_mul(usize::from(limits.maximum_page_height)))
            .ok_or_else(|| OperationFailure::before_work(Error::LimitOverflow))?
            .min(configured_maximum);
        let maximum_nested = u64::try_from(target)
            .ok()
            .and_then(|count| count.checked_mul(u64::from(limits.maximum_name_bytes)))
            .ok_or_else(|| OperationFailure::before_work(Error::LimitOverflow))?;
        let maximum_container = u64::try_from(target)
            .ok()
            .and_then(|count| {
                count.checked_mul(u64::try_from(size_of::<F::Value>()).unwrap_or(u64::MAX))
            })
            .ok_or_else(|| OperationFailure::before_work(Error::LimitOverflow))?;
        WorkCounters {
            items_returned: u64::try_from(maximum_values).unwrap_or(u64::MAX),
            peak_allocation_bytes: maximum_container.saturating_add(maximum_nested),
            ..WorkCounters::default()
        }
        .verify(budget)
        .map_err(|error| OperationFailure::before_work(error.into()))?;

        let mut allocations = AllocationLedger::default();
        let mut work = WorkCounters::default();
        let values_container_bytes = allocations
            .claim_elements::<F::Value>(target, &mut work, budget)
            .map_err(|error| failed(map_allocation(error), work))?;
        let mut values = Vec::new();
        if values.try_reserve_exact(target).is_err() {
            return Err(failed(Error::AllocationFailed, work));
        }
        let visited =
            VisitedObjectSet::new(maximum_traversal_pages, &mut allocations, &mut work, budget)
                .map_err(|error| failed(map_allocation(error), work))?;
        let mut machine = Self {
            after,
            maximum_values,
            target,
            maximum_traversal_pages,
            limits,
            budget,
            pending: Vec::new(),
            pending_capacity: LogicalVecCapacity::default(),
            visited,
            values,
            values_container_bytes,
            values_nested_bytes: 0,
            successor_page: None,
            allocations,
            work,
            format: PhantomData,
        };
        machine.push_pending(Pending {
            page: root,
            lower: None,
            upper: None,
            height: 1,
            nested_bytes: 0,
        })?;
        Ok(machine)
    }

    fn push_pending(&mut self, pending: Pending<F::Key>) -> Result<(), Failure> {
        self.pending_capacity
            .ensure_for_push(
                &mut self.pending,
                self.maximum_traversal_pages,
                &mut self.allocations,
                &mut self.work,
                self.budget,
            )
            .map_err(|error| failed(map_allocation(error), self.work))?;
        self.pending.push(pending);
        Ok(())
    }

    fn pop_pending(&mut self) -> Result<Pending<F::Key>, Failure> {
        let pending = self
            .pending
            .pop()
            .ok_or_else(|| failed(Error::TraversalState, self.work))?;
        if pending.height > self.limits.maximum_page_height {
            return Err(failed(Error::HeightExceeded, self.work));
        }
        Ok(pending)
    }

    fn visit(&mut self, page: ObjectId) -> Result<(), Failure> {
        let inserted = self
            .visited
            .insert(page, &mut self.allocations, &mut self.work, self.budget)
            .map_err(|error| failed(map_allocation(error), self.work))?;
        if !inserted.inserted {
            return Err(failed(Error::CycleOrAlias, self.work));
        }
        Ok(())
    }

    async fn read_page<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        page: ObjectId,
        cancellation: &CancellationToken,
    ) -> Result<OwnedPage<F>, Failure> {
        persistent_io::read_page::<S, F>(
            store,
            page,
            self.limits,
            self.budget,
            cancellation,
            &mut self.allocations,
            &mut self.work,
        )
        .await
        .map_err(|error| failed(map_io(error), self.work))
    }

    fn accept(
        &mut self,
        decoded: OwnedPage<F>,
        mut pending: Pending<F::Key>,
    ) -> Result<(), Failure> {
        match decoded.page {
            persistent_io::PageLease::Owned(Page::Leaf(values)) => {
                self.accept_leaf(values, decoded.logical_bytes, &pending)?;
            }
            persistent_io::PageLease::Owned(Page::Internal(children)) => {
                self.accept_internal(children, decoded.logical_bytes, &mut pending)?;
            }
            persistent_io::PageLease::Shared { page, .. } => match page.as_ref() {
                Page::Leaf(values) => self.accept_shared_leaf(values, &pending)?,
                Page::Internal(children) => {
                    self.accept_shared_internal(children, &mut pending)?;
                }
            },
        }
        self.allocations
            .release(pending.nested_bytes)
            .map_err(|error| failed(map_allocation(error), self.work))?;
        self.work
            .verify(self.budget)
            .map_err(|error| failed(error.into(), self.work))
    }

    fn accept_shared_leaf(
        &mut self,
        values: &[F::Value],
        pending: &Pending<F::Key>,
    ) -> Result<(), Failure> {
        validate_values::<F>(values, pending.lower.as_ref(), pending.upper.as_ref())
            .map_err(|error| failed(error, self.work))?;
        let (start, comparisons) = self
            .after
            .map_or((0, 0), |cursor| upper_bound_values::<F>(values, cursor));
        charge_items(&mut self.work, comparisons, self.budget)?;
        for value in values.iter().skip(start) {
            if self.values.len() == self.target {
                break;
            }
            charge_items(&mut self.work, 1, self.budget)?;
            let nested = F::value_nested_bytes(value);
            let cloned = persistent_io::clone_value::<F>(
                value,
                self.limits.maximum_name_bytes,
                self.budget,
                &mut self.allocations,
                &mut self.work,
            )
            .map_err(|error| failed(map_io(error), self.work))?;
            self.values_nested_bytes = self
                .values_nested_bytes
                .checked_add(nested)
                .ok_or_else(|| failed(Error::AllocationFailed, self.work))?;
            self.values.push(cloned);
        }
        Ok(())
    }

    fn accept_shared_internal(
        &mut self,
        children: &[Child<F::Key>],
        pending: &mut Pending<F::Key>,
    ) -> Result<(), Failure> {
        validate_children::<F>(children, pending.lower.as_ref(), pending.upper.as_ref())
            .map_err(|error| failed(error, self.work))?;
        let (start, comparisons) = self
            .after
            .map_or((0, 0), |cursor| upper_bound_children(children, cursor));
        charge_items(&mut self.work, comparisons, self.budget)?;
        let next_height = pending
            .height
            .checked_add(1)
            .ok_or_else(|| failed(Error::HeightExceeded, self.work))?;
        if next_height > self.limits.maximum_page_height {
            return Err(failed(Error::HeightExceeded, self.work));
        }
        let remaining = self.target.saturating_sub(self.values.len());
        let end = start
            .saturating_add(remaining.saturating_add(1))
            .min(children.len());
        if end < children.len() {
            self.successor_page = Some(children[end].page);
        }
        let mut next_lower: Option<F::Key> = None;
        let mut transferred_upper = false;
        for index in (start..end).rev() {
            let child = &children[index];
            let lower = self.clone_key(&child.first)?;
            let lower_bytes = F::key_nested_bytes(&lower);
            let upper = if next_lower.is_some() {
                next_lower.take()
            } else {
                transferred_upper = pending.upper.is_some();
                pending.upper.take()
            };
            let upper_bytes = upper.as_ref().map_or(0, F::key_nested_bytes);
            let previous = if index == start {
                None
            } else {
                Some(self.clone_key(&child.first)?)
            };
            self.push_pending(Pending {
                page: child.page,
                lower: Some(lower),
                upper,
                height: next_height,
                nested_bytes: lower_bytes
                    .checked_add(upper_bytes)
                    .ok_or_else(|| failed(Error::AllocationFailed, self.work))?,
            })?;
            next_lower = previous;
        }
        let inherited = if transferred_upper {
            pending
                .nested_bytes
                .checked_sub(pending.lower.as_ref().map_or(0, F::key_nested_bytes))
                .ok_or_else(|| failed(Error::TraversalState, self.work))?
        } else {
            0
        };
        pending.nested_bytes = pending
            .nested_bytes
            .checked_sub(inherited)
            .ok_or_else(|| failed(Error::TraversalState, self.work))?;
        Ok(())
    }

    fn accept_leaf(
        &mut self,
        values: Vec<F::Value>,
        decoded_bytes: u64,
        pending: &Pending<F::Key>,
    ) -> Result<(), Failure> {
        validate_values::<F>(&values, pending.lower.as_ref(), pending.upper.as_ref())
            .map_err(|error| failed(error, self.work))?;
        let (start, comparisons) = self
            .after
            .map_or((0, 0), |cursor| upper_bound_values::<F>(&values, cursor));
        charge_items(&mut self.work, comparisons, self.budget)?;
        let mut retained_nested = 0_u64;
        for value in values.into_iter().skip(start) {
            if self.values.len() == self.target {
                break;
            }
            charge_items(&mut self.work, 1, self.budget)?;
            retained_nested = retained_nested
                .checked_add(F::value_nested_bytes(&value))
                .ok_or_else(|| failed(Error::AllocationFailed, self.work))?;
            self.values.push(value);
        }
        self.values_nested_bytes = self
            .values_nested_bytes
            .checked_add(retained_nested)
            .ok_or_else(|| failed(Error::AllocationFailed, self.work))?;
        self.allocations
            .release(
                decoded_bytes
                    .checked_sub(retained_nested)
                    .ok_or_else(|| failed(Error::TraversalState, self.work))?,
            )
            .map_err(|error| failed(map_allocation(error), self.work))
    }

    fn accept_internal(
        &mut self,
        children: Vec<Child<F::Key>>,
        decoded_bytes: u64,
        pending: &mut Pending<F::Key>,
    ) -> Result<(), Failure> {
        validate_children::<F>(&children, pending.lower.as_ref(), pending.upper.as_ref())
            .map_err(|error| failed(error, self.work))?;
        let (start, comparisons) = self
            .after
            .map_or((0, 0), |cursor| upper_bound_children(&children, cursor));
        charge_items(&mut self.work, comparisons, self.budget)?;
        let next_height = pending
            .height
            .checked_add(1)
            .ok_or_else(|| failed(Error::HeightExceeded, self.work))?;
        if next_height > self.limits.maximum_page_height {
            return Err(failed(Error::HeightExceeded, self.work));
        }
        // A valid routed child contains at least one value. The selected child
        // may contain no value after the caller's cursor, so retain one extra
        // successor beyond the remaining output witness. Routing-state growth
        // is therefore proportional to the requested page, not parent fanout.
        let remaining = self.target.saturating_sub(self.values.len());
        let retained_children = remaining.saturating_add(1);
        let mut children = children;
        let retained_end = start.saturating_add(retained_children).min(children.len());
        if retained_end < children.len() {
            self.successor_page = Some(children[retained_end].page);
        }
        children.truncate(retained_end);
        let mut tail = children.drain(start..);
        let mut next_lower: Option<F::Key> = None;
        let mut moved_nested = 0_u64;
        let mut transferred_upper = false;
        while let Some(child) = tail.next_back() {
            let lower_bytes = F::key_nested_bytes(&child.first);
            let upper = if next_lower.is_some() {
                next_lower.take()
            } else {
                transferred_upper = pending.upper.is_some();
                pending.upper.take()
            };
            let upper_bytes = upper.as_ref().map_or(0, F::key_nested_bytes);
            moved_nested = moved_nested
                .checked_add(lower_bytes)
                .ok_or_else(|| failed(Error::AllocationFailed, self.work))?;
            let previous = if tail.len() == 0 {
                None
            } else {
                Some(self.clone_key(&child.first)?)
            };
            self.push_pending(Pending {
                page: child.page,
                lower: Some(child.first),
                upper,
                height: next_height,
                nested_bytes: lower_bytes
                    .checked_add(upper_bytes)
                    .ok_or_else(|| failed(Error::AllocationFailed, self.work))?,
            })?;
            next_lower = previous;
        }
        let inherited = if transferred_upper {
            pending
                .nested_bytes
                .checked_sub(pending.lower.as_ref().map_or(0, F::key_nested_bytes))
                .ok_or_else(|| failed(Error::TraversalState, self.work))?
        } else {
            0
        };
        pending.nested_bytes = pending
            .nested_bytes
            .checked_sub(inherited)
            .ok_or_else(|| failed(Error::TraversalState, self.work))?;
        self.allocations
            .release(
                decoded_bytes
                    .checked_sub(moved_nested)
                    .ok_or_else(|| failed(Error::TraversalState, self.work))?,
            )
            .map_err(|error| failed(map_allocation(error), self.work))
    }

    fn clone_key(&mut self, key: &F::Key) -> Result<F::Key, Failure> {
        persistent_io::clone_key::<F>(
            key,
            self.limits.maximum_name_bytes,
            self.budget,
            &mut self.allocations,
            &mut self.work,
        )
        .map_err(|error| failed(map_io(error), self.work))
    }

    fn finish(mut self) -> Result<Receipt<F::Value>, Failure> {
        let has_more = self.values.len() > self.maximum_values;
        let next_request = has_more
            .then(|| {
                self.pending
                    .last()
                    .map(|pending| pending.page)
                    .or(self.successor_page)
            })
            .flatten()
            .map(|object_id| ObjectReadRequest {
                object_id,
                maximum_bytes: self.limits.maximum_page_object_bytes(),
            });
        if has_more {
            let removed = self
                .values
                .pop()
                .ok_or_else(|| failed(Error::TraversalState, self.work))?;
            let bytes = F::value_nested_bytes(&removed);
            self.values_nested_bytes = self
                .values_nested_bytes
                .checked_sub(bytes)
                .ok_or_else(|| failed(Error::TraversalState, self.work))?;
            self.allocations
                .release(bytes)
                .map_err(|error| failed(map_allocation(error), self.work))?;
        }
        self.work.items_returned = u64::try_from(self.values.len()).unwrap_or(u64::MAX);
        self.work
            .verify(self.budget)
            .map_err(|error| failed(error.into(), self.work))?;
        for pending in self.pending.drain(..) {
            self.allocations
                .release(pending.nested_bytes)
                .map_err(|error| failed(map_allocation(error), self.work))?;
        }
        self.allocations
            .release(self.values_nested_bytes)
            .and_then(|()| self.allocations.release(self.values_container_bytes))
            .and_then(|()| self.visited.release(&mut self.allocations))
            .and_then(|()| {
                self.allocations
                    .release(self.pending_capacity.logical_bytes())
            })
            .map_err(|error| failed(map_allocation(error), self.work))?;
        if self.allocations.live_bytes() != 0 {
            return Err(failed(Error::TraversalState, self.work));
        }
        Ok(Receipt {
            values: self.values,
            has_more,
            next_request,
            work: self.work,
        })
    }
}

fn validate_values<F: Format>(
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

fn validate_children<F: Format>(
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

fn upper_bound_values<F: Format>(values: &[F::Value], cursor: &F::Key) -> (usize, u64) {
    let mut left = 0;
    let mut right = values.len();
    let mut comparisons = 0_u64;
    while left < right {
        comparisons = comparisons.saturating_add(1);
        let middle = left + (right - left) / 2;
        if F::key(&values[middle]) <= cursor {
            left = middle + 1;
        } else {
            right = middle;
        }
    }
    (left, comparisons)
}

fn upper_bound_children<K: Ord>(children: &[Child<K>], cursor: &K) -> (usize, u64) {
    let mut left = 0;
    let mut right = children.len();
    let mut comparisons = 0_u64;
    while left < right {
        comparisons = comparisons.saturating_add(1);
        let middle = left + (right - left) / 2;
        if children[middle].first <= *cursor {
            left = middle + 1;
        } else {
            right = middle;
        }
    }
    (left.saturating_sub(1), comparisons)
}

fn charge_items(work: &mut WorkCounters, count: u64, budget: WorkBudget) -> Result<(), Failure> {
    *work = work
        .checked_add(WorkCounters {
            items_examined: count,
            ..WorkCounters::default()
        })
        .map_err(|error| failed(error.into(), *work))?;
    work.verify(budget)
        .map_err(|error| failed(error.into(), *work))
}

fn map_allocation(error: AllocationError) -> Error {
    match error {
        AllocationError::Work(error) => error.into(),
        _ => Error::AllocationFailed,
    }
}

fn map_io(error: persistent_io::Error) -> Error {
    match error {
        persistent_io::Error::AllocationFailed => Error::AllocationFailed,
        persistent_io::Error::Allocation(error) => map_allocation(error),
        persistent_io::Error::Storage(error) => error.into(),
        persistent_io::Error::Decode(error) => error.into(),
        persistent_io::Error::Work(error) => error.into(),
    }
}

fn failed(error: Error, work: WorkCounters) -> Failure {
    OperationFailure::new(error, work)
}

#[cfg(test)]
#[path = "tests/persistent_pagination.rs"]
mod tests;
