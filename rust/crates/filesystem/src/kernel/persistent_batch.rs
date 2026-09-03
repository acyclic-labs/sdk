//! Shared-frontier batch lookup for authenticated persistent B+trees.

use super::allocation::{AllocationError, AllocationLedger, LogicalVecCapacity, VisitedObjectSet};
use super::persistent_btree::{Child, Format, Page};
use super::persistent_io;
use super::{CanonicalDecodeError, DecodeLimits};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{ObjectId, ObjectStoreError};
use std::marker::PhantomData;
use std::ops::Range;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Receipt<V> {
    pub(crate) values: Vec<Option<V>>,
    pub(crate) work: WorkCounters,
}

pub(crate) type Failure = OperationFailure<Error>;

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("batch lookup was cancelled")]
    Cancelled,
    #[error("batch lookup is empty")]
    Empty,
    #[error("batch lookup exceeds its admitted query bound")]
    TooManyQueries,
    #[error("batch lookup root has the wrong object kind")]
    WrongRootKind,
    #[error("batch lookup limits are invalid")]
    InvalidLimits,
    #[error("batch lookup tree exceeds its admitted height")]
    HeightExceeded,
    #[error("batch lookup tree contains a cycle or alias")]
    CycleOrAlias,
    #[error("batch lookup child bounds mismatch")]
    ChildBoundsMismatch,
    #[error("batch lookup routing is invalid")]
    InvalidRouting,
    #[error("batch lookup allocation failed")]
    AllocationFailed,
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
    #[error(transparent)]
    Work(#[from] WorkError),
}

struct IndexedKey<'a, K> {
    ordinal: usize,
    key: &'a K,
}

struct Request<K> {
    page: ObjectId,
    lower: Option<K>,
    upper: Option<K>,
    queries: Range<usize>,
    height: u16,
    nested_bytes: u64,
}

struct Match {
    entry: usize,
    queries: Range<usize>,
}

pub(crate) fn lookup<S, F>(
    store: &S,
    root: ObjectId,
    keys: &[F::Key],
    maximum_queries: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
) -> Result<Receipt<F::Value>, Failure>
where
    S: crate::ImmediateObjectStore,
    F: Format,
{
    crate::async_storage::poll_immediate(lookup_async::<S, F>(
        store,
        root,
        keys,
        maximum_queries,
        limits,
        budget,
        &CancellationToken::new(),
    ))
}

pub(crate) async fn lookup_async<S, F>(
    store: &S,
    root: ObjectId,
    keys: &[F::Key],
    maximum_queries: u32,
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
    let mut machine = Machine::<F>::new(root, keys, maximum_queries, limits, budget)?;
    while !machine.requests.is_empty() {
        if cancellation.is_cancelled() {
            return Err(failed(Error::Cancelled, machine.work));
        }
        let frontier_count = machine.requests.len();
        let height = machine
            .requests
            .last()
            .map(|request| request.height)
            .ok_or_else(|| failed(Error::InvalidRouting, machine.work))?;
        if machine
            .requests
            .iter()
            .any(|request| request.height != height)
        {
            return Err(failed(Error::InvalidRouting, machine.work));
        }
        for index in (0..frontier_count).rev() {
            machine.visit(machine.requests[index].page)?;
        }
        let mut pages = persistent_io::read_pages::<S, F, _>(
            store,
            machine.requests[..frontier_count]
                .iter()
                .rev()
                .map(|request| request.page),
            limits,
            budget,
            cancellation,
            &mut machine.allocations,
            &mut machine.work,
        )
        .await
        .map_err(|error| failed(map_io(error), machine.work))?;
        for index in (0..frontier_count).rev() {
            let decoded = pages
                .next(
                    limits,
                    budget,
                    cancellation,
                    &mut machine.allocations,
                    &mut machine.work,
                )
                .map_err(|error| failed(map_io(error), machine.work))?;
            let request = machine.requests.swap_remove(index);
            if let Err(failure) = machine.accept(request, decoded) {
                let page_cleanup = pages.discard(&mut machine.allocations);
                let cleanup = machine.abort_cleanup();
                page_cleanup.map_err(|error| failed(map_io(error), *failure.work))?;
                cleanup?;
                return Err(failure);
            }
        }
        pages
            .finish(&mut machine.allocations)
            .map_err(|error| failed(map_io(error), machine.work))?;
    }
    machine.finish()
}

struct Machine<'a, F: Format> {
    ordered: Vec<IndexedKey<'a, F::Key>>,
    ordered_bytes: u64,
    values: Vec<Option<F::Value>>,
    value_container_bytes: u64,
    value_nested_bytes: u64,
    requests: Vec<Request<F::Key>>,
    request_capacity: LogicalVecCapacity,
    matches: Vec<Match>,
    match_bytes: u64,
    visited: VisitedObjectSet,
    limits: DecodeLimits,
    budget: WorkBudget,
    allocations: AllocationLedger,
    work: WorkCounters,
    format: PhantomData<F>,
}

impl<'a, F: Format> Machine<'a, F> {
    fn new(
        root: ObjectId,
        keys: &'a [F::Key],
        maximum_queries: u32,
        limits: DecodeLimits,
        budget: WorkBudget,
    ) -> Result<Self, Failure> {
        if root.kind != F::kind() {
            return Err(OperationFailure::before_work(Error::WrongRootKind));
        }
        if keys.is_empty() {
            return Err(OperationFailure::before_work(Error::Empty));
        }
        if maximum_queries == 0 || u32::try_from(keys.len()).unwrap_or(u32::MAX) > maximum_queries {
            return Err(OperationFailure::before_work(Error::TooManyQueries));
        }
        if !limits.page_limits_valid(1) {
            return Err(OperationFailure::before_work(Error::InvalidLimits));
        }
        let maximum_visited = keys
            .len()
            .checked_mul(usize::from(limits.maximum_page_height))
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| OperationFailure::before_work(Error::InvalidLimits))?
            .min(
                usize::try_from(limits.maximum_visited_pages)
                    .map_err(|_| OperationFailure::before_work(Error::InvalidLimits))?,
            );
        let mut allocations = AllocationLedger::default();
        let mut work = WorkCounters::default();
        let ordered_bytes = allocations
            .claim_elements::<IndexedKey<'_, F::Key>>(keys.len(), &mut work, budget)
            .map_err(|error| failed(map_allocation(error), work))?;
        let mut ordered = Vec::new();
        if ordered.try_reserve_exact(keys.len()).is_err() {
            return Err(failed(Error::AllocationFailed, work));
        }
        ordered.extend(
            keys.iter()
                .enumerate()
                .map(|(ordinal, key)| IndexedKey { ordinal, key }),
        );
        sort_indexed(&mut ordered, &mut work, budget).map_err(|error| failed(error, work))?;

        let value_container_bytes = allocations
            .claim_elements::<Option<F::Value>>(keys.len(), &mut work, budget)
            .map_err(|error| failed(map_allocation(error), work))?;
        let mut values = Vec::new();
        if values.try_reserve_exact(keys.len()).is_err() {
            return Err(failed(Error::AllocationFailed, work));
        }
        values.extend(std::iter::repeat_with(|| None).take(keys.len()));

        let match_bytes = allocations
            .claim_elements::<Match>(keys.len(), &mut work, budget)
            .map_err(|error| failed(map_allocation(error), work))?;
        let mut matches = Vec::new();
        if matches.try_reserve_exact(keys.len()).is_err() {
            return Err(failed(Error::AllocationFailed, work));
        }
        let visited = VisitedObjectSet::new(maximum_visited, &mut allocations, &mut work, budget)
            .map_err(|error| failed(map_allocation(error), work))?;
        let mut machine = Self {
            ordered,
            ordered_bytes,
            values,
            value_container_bytes,
            value_nested_bytes: 0,
            requests: Vec::new(),
            request_capacity: LogicalVecCapacity::default(),
            matches,
            match_bytes,
            visited,
            limits,
            budget,
            allocations,
            work,
            format: PhantomData,
        };
        machine.push_request(Request {
            page: root,
            lower: None,
            upper: None,
            queries: 0..keys.len(),
            height: 1,
            nested_bytes: 0,
        })?;
        Ok(machine)
    }

    fn push_request(&mut self, request: Request<F::Key>) -> Result<(), Failure> {
        let maximum = usize::try_from(self.limits.maximum_visited_pages)
            .map_err(|_| failed(Error::InvalidLimits, self.work))?;
        self.request_capacity
            .ensure_for_push(
                &mut self.requests,
                maximum,
                &mut self.allocations,
                &mut self.work,
                self.budget,
            )
            .map_err(|error| failed(map_allocation(error), self.work))?;
        self.requests.push(request);
        Ok(())
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

    fn accept(
        &mut self,
        mut request: Request<F::Key>,
        decoded: persistent_io::OwnedPage<F>,
    ) -> Result<(), Failure> {
        let (page, decoded_bytes) = decoded
            .into_owned(self.budget, &mut self.allocations, &mut self.work)
            .map_err(|error| failed(map_io(error), self.work))?;
        let accepted = match page {
            Page::Leaf(entries) => self.accept_leaf(&request, entries, decoded_bytes),
            Page::Internal(children) => {
                self.accept_internal(&mut request, &children, decoded_bytes)
            }
        };
        let request_bytes = request.nested_bytes;
        drop(request);
        if let Err(failure) = accepted {
            self.allocations
                .release(decoded_bytes)
                .and_then(|()| self.allocations.release(request_bytes))
                .map_err(|error| failed(map_allocation(error), self.work))?;
            return Err(failure);
        }
        self.allocations
            .release(request_bytes)
            .map_err(|error| failed(map_allocation(error), self.work))?;
        Ok(())
    }

    fn accept_leaf(
        &mut self,
        request: &Request<F::Key>,
        mut entries: Vec<F::Value>,
        decoded_bytes: u64,
    ) -> Result<(), Failure> {
        validate_values::<F>(&entries, request.lower.as_ref(), request.upper.as_ref())
            .map_err(|error| failed(error, self.work))?;
        self.matches.clear();
        let mut query = request.queries.start;
        while query < request.queries.end {
            let (group_end, group_comparisons) =
                equal_group(&self.ordered, query, request.queries.end);
            self.charge(group_comparisons)?;
            let (found, comparisons) = search::<F>(&entries, self.ordered[query].key);
            self.charge(comparisons)?;
            if let Ok(entry) = found {
                self.matches.push(Match {
                    entry,
                    queries: query..group_end,
                });
            }
            query = group_end;
        }
        let mut moved_nested = 0_u64;
        while let Some(found) = self.matches.pop() {
            let value = entries.swap_remove(found.entry);
            let nested = F::value_nested_bytes(&value);
            moved_nested = moved_nested
                .checked_add(nested)
                .ok_or_else(|| failed(Error::AllocationFailed, self.work))?;
            let mut queries = found.queries;
            let first = queries
                .next()
                .ok_or_else(|| failed(Error::InvalidRouting, self.work))?;
            for query in queries {
                self.clone_result(query, &value, nested)?;
            }
            let ordinal = self.ordered[first].ordinal;
            self.values[ordinal] = Some(value);
        }
        self.value_nested_bytes = self
            .value_nested_bytes
            .checked_add(moved_nested)
            .ok_or_else(|| failed(Error::AllocationFailed, self.work))?;
        self.allocations
            .release(
                decoded_bytes
                    .checked_sub(moved_nested)
                    .ok_or_else(|| failed(Error::InvalidRouting, self.work))?,
            )
            .map_err(|error| failed(map_allocation(error), self.work))
    }

    fn clone_result(&mut self, query: usize, value: &F::Value, nested: u64) -> Result<(), Failure> {
        let next_nested = self
            .value_nested_bytes
            .checked_add(nested)
            .ok_or_else(|| failed(Error::AllocationFailed, self.work))?;
        let cloned = persistent_io::clone_value::<F>(
            value,
            self.limits.maximum_name_bytes,
            self.budget,
            &mut self.allocations,
            &mut self.work,
        )
        .map_err(|error| failed(map_io(error), self.work))?;
        self.value_nested_bytes = next_nested;
        self.values[self.ordered[query].ordinal] = Some(cloned);
        Ok(())
    }

    fn accept_internal(
        &mut self,
        request: &mut Request<F::Key>,
        children: &[Child<F::Key>],
        decoded_bytes: u64,
    ) -> Result<(), Failure> {
        validate_children::<F>(children, request.lower.as_ref(), request.upper.as_ref())
            .map_err(|error| failed(error, self.work))?;
        let next_height = request
            .height
            .checked_add(1)
            .ok_or_else(|| failed(Error::HeightExceeded, self.work))?;
        admit_height(next_height, self.limits.maximum_page_height)
            .map_err(|error| failed(error, self.work))?;
        let mut query = request.queries.start;
        while query < request.queries.end {
            let (child_index, comparisons) = route(children, self.ordered[query].key);
            self.charge(comparisons)?;
            let child = children
                .get(child_index)
                .ok_or_else(|| failed(Error::InvalidRouting, self.work))?;
            let upper_ref = children
                .get(child_index + 1)
                .map(|next| &next.first)
                .or(request.upper.as_ref());
            let mut end = next_index(query).map_err(|error| failed(error, self.work))?;
            while end < request.queries.end {
                self.charge(1)?;
                if upper_ref.is_some_and(|upper| self.ordered[end].key >= upper) {
                    break;
                }
                end += 1;
            }
            let lower = self.clone_key(&child.first)?;
            let upper = upper_ref.map(|key| self.clone_key(key)).transpose()?;
            let nested_bytes = F::key_nested_bytes(&lower)
                .checked_add(upper.as_ref().map_or(0, F::key_nested_bytes))
                .ok_or_else(|| failed(Error::AllocationFailed, self.work))?;
            self.push_request(Request {
                page: child.page,
                lower: Some(lower),
                upper,
                queries: query..end,
                height: next_height,
                nested_bytes,
            })?;
            query = end;
        }
        self.allocations
            .release(decoded_bytes)
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

    fn charge(&mut self, count: u64) -> Result<(), Failure> {
        self.work = charge_work(self.work, count, self.budget)
            .map_err(|error| failed(error.into(), self.work))?;
        Ok(())
    }

    fn abort_cleanup(mut self) -> Result<(), Failure> {
        let pending_nested = self.requests.iter().try_fold(0_u64, |total, request| {
            total
                .checked_add(request.nested_bytes)
                .ok_or_else(|| failed(Error::AllocationFailed, self.work))
        })?;
        self.requests.clear();
        self.values.clear();
        self.ordered.clear();
        self.matches.clear();
        self.allocations
            .release(pending_nested)
            .and_then(|()| self.allocations.release(self.value_nested_bytes))
            .and_then(|()| self.allocations.release(self.value_container_bytes))
            .and_then(|()| self.allocations.release(self.ordered_bytes))
            .and_then(|()| self.allocations.release(self.match_bytes))
            .and_then(|()| self.visited.release(&mut self.allocations))
            .and_then(|()| {
                self.allocations
                    .release(self.request_capacity.logical_bytes())
            })
            .map_err(|error| failed(map_allocation(error), self.work))?;
        if self.allocations.live_bytes() != 0 {
            return Err(failed(Error::InvalidRouting, self.work));
        }
        Ok(())
    }

    fn finish(mut self) -> Result<Receipt<F::Value>, Failure> {
        self.work.items_returned = u64::try_from(self.values.len()).unwrap_or(u64::MAX);
        self.work
            .verify(self.budget)
            .map_err(|error| failed(error.into(), self.work))?;
        self.allocations
            .release(self.value_nested_bytes)
            .and_then(|()| self.allocations.release(self.value_container_bytes))
            .and_then(|()| self.allocations.release(self.ordered_bytes))
            .and_then(|()| self.allocations.release(self.match_bytes))
            .and_then(|()| self.visited.release(&mut self.allocations))
            .and_then(|()| {
                self.allocations
                    .release(self.request_capacity.logical_bytes())
            })
            .map_err(|error| failed(map_allocation(error), self.work))?;
        if self.allocations.live_bytes() != 0 {
            return Err(failed(Error::InvalidRouting, self.work));
        }
        Ok(Receipt {
            values: self.values,
            work: self.work,
        })
    }
}

fn sort_indexed<K: Ord>(
    values: &mut [IndexedKey<'_, K>],
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<(), Error> {
    let count = values.len();
    if count < 2 {
        return Ok(());
    }
    let scan_bound = crate::foundation::usize_to_u64(count - 1);
    work.checked_add(WorkCounters {
        items_examined: scan_bound,
        ..WorkCounters::default()
    })?
    .verify(budget)?;
    let mut scan_comparisons = 0_u64;
    let mut ordered = true;
    for adjacent in values.windows(2) {
        scan_comparisons = scan_comparisons
            .checked_add(1)
            .ok_or(Error::Work(WorkError::Overflow))?;
        if indexed_order(&adjacent[0], &adjacent[1]).is_gt() {
            ordered = false;
            break;
        }
    }
    *work = work.checked_add(WorkCounters {
        items_examined: scan_comparisons,
        ..WorkCounters::default()
    })?;
    if ordered {
        return Ok(());
    }
    let bound = sort_admission_bound(count)?;
    work.checked_add(WorkCounters {
        items_examined: bound,
        ..WorkCounters::default()
    })?
    .verify(budget)?;
    let mut comparisons = 0_u64;
    for start in (0..heap_parent_count(count)).rev() {
        sift_down(values, start, count, &mut comparisons)?;
    }
    for end in (1..count).rev() {
        values.swap(0, end);
        sift_down(values, 0, end, &mut comparisons)?;
    }
    *work = work.checked_add(WorkCounters {
        items_examined: comparisons,
        ..WorkCounters::default()
    })?;
    Ok(())
}

fn admit_height(next_height: u16, maximum_height: u16) -> Result<(), Error> {
    if next_height > maximum_height {
        Err(Error::HeightExceeded)
    } else {
        Ok(())
    }
}

fn next_index(index: usize) -> Result<usize, Error> {
    index.checked_add(1).ok_or(Error::InvalidRouting)
}

fn charge_work(
    work: WorkCounters,
    count: u64,
    budget: WorkBudget,
) -> Result<WorkCounters, WorkError> {
    let charged = work.checked_add(WorkCounters {
        items_examined: count,
        ..WorkCounters::default()
    })?;
    charged.verify(budget)?;
    Ok(charged)
}

fn sort_admission_bound(count: usize) -> Result<u64, Error> {
    let levels = usize::BITS - count.leading_zeros();
    u64::try_from(count)
        .ok()
        .and_then(|value| value.checked_mul(u64::from(levels)))
        .and_then(|value| value.checked_mul(3))
        .ok_or(Error::Work(WorkError::Overflow))
}

const fn heap_parent_count(count: usize) -> usize {
    count >> 1
}

fn sift_down<K: Ord>(
    values: &mut [IndexedKey<'_, K>],
    mut root: usize,
    end: usize,
    comparisons: &mut u64,
) -> Result<(), Error> {
    loop {
        let Some(left) = root
            .checked_mul(2)
            .and_then(|value| value.checked_add(1))
            .filter(|left| *left < end)
        else {
            return Ok(());
        };
        let right = left + 1;
        let mut greater = left;
        if right < end {
            *comparisons = comparisons
                .checked_add(1)
                .ok_or(Error::Work(WorkError::Overflow))?;
            if indexed_order(&values[left], &values[right]).is_lt() {
                greater = right;
            }
        }
        *comparisons = comparisons
            .checked_add(1)
            .ok_or(Error::Work(WorkError::Overflow))?;
        if !indexed_order(&values[root], &values[greater]).is_lt() {
            return Ok(());
        }
        values.swap(root, greater);
        root = greater;
    }
}

fn indexed_order<K: Ord>(
    left: &IndexedKey<'_, K>,
    right: &IndexedKey<'_, K>,
) -> std::cmp::Ordering {
    left.key
        .cmp(right.key)
        .then(left.ordinal.cmp(&right.ordinal))
}

fn equal_group<K: Eq>(values: &[IndexedKey<'_, K>], start: usize, end: usize) -> (usize, u64) {
    let mut cursor = start + 1;
    let mut comparisons = 0_u64;
    while cursor < end {
        comparisons = comparisons.saturating_add(1);
        if values[cursor].key != values[start].key {
            break;
        }
        cursor += 1;
    }
    (cursor, comparisons)
}

fn search<F: Format>(values: &[F::Value], key: &F::Key) -> (Result<usize, usize>, u64) {
    let mut left = 0;
    let mut right = values.len();
    let mut comparisons = 0_u64;
    while left < right {
        comparisons = comparisons.saturating_add(1);
        let middle = left + (right - left) / 2;
        match F::key(&values[middle]).cmp(key) {
            std::cmp::Ordering::Less => left = middle + 1,
            std::cmp::Ordering::Greater => right = middle,
            std::cmp::Ordering::Equal => return (Ok(middle), comparisons),
        }
    }
    (Err(left), comparisons)
}

fn route<K: Ord>(children: &[Child<K>], key: &K) -> (usize, u64) {
    let mut left = 0;
    let mut right = children.len();
    let mut comparisons = 0_u64;
    while left < right {
        comparisons = comparisons.saturating_add(1);
        let middle = left + (right - left) / 2;
        if children[middle].first <= *key {
            left = middle + 1;
        } else {
            right = middle;
        }
    }
    (left.saturating_sub(1), comparisons)
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
#[path = "tests/persistent_batch.rs"]
mod tests;
