//! Shared-frontier batch lookup for authenticated persistent B+trees.

use super::allocation::{KernelAccounting, KernelOwnedAllocation, VisitedObjectSet};
use super::persistent_point::{Owned, query_error, project_failure, split_allocation};
use super::persistent_btree::{Child, Format, Page, children_within, leaf_within};
use super::persistent_io;
use super::search::{binary_search_with_accounting, partition_point_with_accounting};
use super::{CanonicalDecodeError, DecodeLimits};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::foundation::usize_to_u64;
use crate::performance::{OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError};
use crate::storage::{ObjectId, ObjectStoreError};
use std::ops::Range;
use thiserror::Error;

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
    lower: Option<Owned<K>>,
    upper: Option<Owned<K>>,
    queries: Range<usize>,
    height: u16,
}

struct Match {
    entry: usize,
    queries: Range<usize>,
}

pub(crate) async fn lookup_async<S, F>(
    store: &S, root: ObjectId, keys: &[F::Key], maximum_queries: u32,
    limits: DecodeLimits, budget: WorkBudget, cancellation: &CancellationToken,
) -> Result<OperationReceipt<Vec<Option<F::Value>>>, Failure>
where S: AsyncObjectStore, F: Format {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    let result = lookup_async_with_accounting::<S, F>(
        store, root, keys, maximum_queries, limits, &mut accounting, cancellation, None,
    ).await;
    let work = accounting.spent();
    result.map(|(mut receipt, _)| { receipt.work = work; receipt })
        .map_err(|error| failed(project_failure(error), work))
}

pub(crate) async fn lookup_async_with_accounting<S, F>(
    store: &S, root: ObjectId, keys: &[F::Key], maximum_queries: u32,
    limits: DecodeLimits, accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken, data_custody: Option<&super::ProofDataCustody>,
) -> Result<(OperationReceipt<Vec<Option<F::Value>>>, KernelOwnedAllocation), crate::FsError>
where S: AsyncObjectStore, F: Format {
    accounting.check()?;
    let mut machine = Machine::<F>::new(root, keys, maximum_queries, limits, accounting)?;
    let result = async {
        while !machine.requests.value.is_empty() {
            cancellation.check()?;
            accounting.check()?;
            let frontier_count = machine.requests.value.len();
            let height = machine.requests.value.last()
                .ok_or_else(|| query_error(Error::InvalidRouting))?.height;
            if machine.requests.value.iter().any(|request| request.height != height) {
                return Err(query_error(Error::InvalidRouting));
            }
            for index in (0..frontier_count).rev() {
                let page = machine.requests.value.get(index)
                    .ok_or_else(|| query_error(Error::InvalidRouting))?.page;
                machine.visit(page, accounting)?;
            }
            let mut pages = persistent_io::read_pages_with_accounting::<S, F, _>(
                store, machine.requests.value.iter().rev().map(|request| request.page),
                limits, accounting, cancellation, data_custody,
            ).await?;
            for index in (0..frontier_count).rev() {
                let (decoded, allocation) = match pages.next_with_accounting(limits, accounting, cancellation) {
                    Ok(page) => page,
                    Err(error) => { pages.discard_with_accounting(accounting, cancellation)?; return Err(error); }
                };
                let request = machine.requests.value.swap_remove(index);
                if let Err(error) = machine.accept(request, decoded, allocation, accounting) {
                    pages.discard_with_accounting(accounting, cancellation)?;
                    return Err(error);
                }
            }
            pages.finish_with_accounting(accounting, cancellation)?;
        }
        Ok(())
    }.await;
    match result {
        Ok(()) => machine.finish(accounting),
        Err(error) => { machine.abort_cleanup(accounting)?; Err(error) }
    }
}

struct Machine<'a, F: Format> {
    ordered: Owned<Vec<IndexedKey<'a, F::Key>>>,
    values: Owned<Vec<Option<F::Value>>>,
    requests: Owned<Vec<Request<F::Key>>>,
    matches: Owned<Vec<Match>>,
    visited: Owned<VisitedObjectSet>,
    limits: DecodeLimits,
    maximum_visited: usize,
}

impl<'a, F: Format> Machine<'a, F> {
    fn new(
        root: ObjectId, keys: &'a [F::Key], maximum_queries: u32,
        limits: DecodeLimits, accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, crate::FsError> {
        if root.kind != F::kind() { return Err(query_error(Error::WrongRootKind)); }
        if keys.is_empty() { return Err(query_error(Error::Empty)); }
        if maximum_queries == 0 || u32::try_from(keys.len()).unwrap_or(u32::MAX) > maximum_queries {
            return Err(query_error(Error::TooManyQueries));
        }
        if !limits.page_limits_valid(1) { return Err(query_error(Error::InvalidLimits)); }
        let maximum_visited = keys.len().checked_mul(usize::from(limits.maximum_page_height))
            .and_then(|count| count.checked_add(1))
            .ok_or_else(|| query_error(Error::InvalidLimits))?
            .min(usize::try_from(limits.maximum_visited_pages).map_err(|_| query_error(Error::InvalidLimits))?);
        let requests = Owned::allocate(0, accounting)?;
        let mut ordered = Owned::allocate(keys.len(), accounting)?;
        let initialized: Result<(), crate::FsError> = (|| {
            let copied = WorkCounters {
                bytes_copied: usize_to_u64(std::mem::size_of::<IndexedKey<'_, F::Key>>()),
                ..WorkCounters::UNCHARGED
            };
            for (ordinal, key) in keys.iter().enumerate() {
                accounting.admit(copied)?;
                ordered.value.push(IndexedKey { ordinal, key });
                accounting.record_completed(copied)?;
            }
            Ok(())
        })();
        if let Err(error) = initialized { ordered.retire(accounting)?; return Err(error); }
        let sorted = sort_indexed(&mut ordered.value, accounting);
        if let Err(error) = sorted { ordered.retire(accounting)?; return Err(error); }
        let values = match Owned::allocate(keys.len(), accounting) {
            Ok(mut values) => { values.value.extend(std::iter::repeat_with(|| None).take(keys.len())); values }
            Err(error) => { ordered.retire(accounting)?; return Err(error); }
        };
        let matches = match Owned::allocate(keys.len(), accounting) {
            Ok(matches) => matches,
            Err(error) => { values.retire(accounting)?; ordered.retire(accounting)?; return Err(error); }
        };
        let visited = VisitedObjectSet::new_with_accounting(maximum_visited, accounting);
        let visited = match visited {
            Ok(visited) => visited,
            Err(error) => { matches.retire(accounting)?; values.retire(accounting)?; ordered.retire(accounting)?; return Err(error); }
        };
        let mut machine = Self {
            ordered, values, matches, visited,
            requests, limits, maximum_visited,
        };
        let initial = Request {
            page: root, lower: None, upper: None, queries: 0..keys.len(), height: 1,
        };
        if let Err(error) = machine.push_request(initial, accounting) {
            machine.abort_cleanup(accounting)?;
            return Err(error);
        }
        Ok(machine)
    }

    fn push_request(&mut self, request: Request<F::Key>, accounting: &mut KernelAccounting<'_>) -> Result<(), crate::FsError> {
        if let Err(error) = self.requests.ensure_for_push(self.maximum_visited, accounting) {
            Self::retire_request(request, accounting)?;
            return Err(error);
        }
        let copied = WorkCounters {
            bytes_copied: usize_to_u64(std::mem::size_of::<Request<F::Key>>()),
            ..WorkCounters::UNCHARGED
        };
        if let Err(error) = accounting.admit(copied) {
            Self::retire_request(request, accounting)?;
            return Err(error);
        }
        self.requests.value.push(request);
        accounting.record_completed(copied)
    }

    fn visit(&mut self, page: ObjectId, accounting: &mut KernelAccounting<'_>) -> Result<(), crate::FsError> {
        let inserted = VisitedObjectSet::insert_with_accounting(&mut self.visited, page, accounting)?;
        if !inserted.inserted { return Err(query_error(Error::CycleOrAlias)); }
        Ok(())
    }

    fn accept(
        &mut self, request: Request<F::Key>, decoded: persistent_io::OwnedPage<F>,
        allocation: KernelOwnedAllocation, accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        let converted = decoded.into_owned_with_accounting(allocation, accounting);
        let ((page, bytes), allocation) = match converted {
            Ok(page) => page,
            Err(error) => { Self::retire_request(request, accounting)?; return Err(error); }
        };
        let mut page = Owned::new(page, allocation, bytes);
        let accepted = match &mut page.value {
            Page::Leaf(entries) => self.accept_leaf(&request, entries, &mut page.allocation, &mut page.bytes, accounting),
            Page::Internal(children) => self.accept_internal(&request, children, accounting),
        };
        page.retire(accounting)?;
        Self::retire_request(request, accounting)?;
        accepted
    }

    fn accept_leaf(
        &mut self, request: &Request<F::Key>, entries: &mut Vec<F::Value>,
        page_allocation: &mut KernelOwnedAllocation, page_bytes: &mut u64,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        if !leaf_within::<F>(entries, request.lower.as_ref().map(|key| &key.value), request.upper.as_ref().map(|key| &key.value)) {
            return Err(query_error(Error::ChildBoundsMismatch));
        }
        self.matches.value.clear();
        let mut query = request.queries.start;
        while query < request.queries.end {
            let group_end = equal_group(&self.ordered.value, query, request.queries.end, accounting)?;
            let key = self.ordered.value.get(query).ok_or_else(|| query_error(Error::InvalidRouting))?.key;
            let found = binary_search_with_accounting(entries, accounting, |value| F::key(value).cmp(key))?;
            if let Ok(entry) = found {
                let copied = WorkCounters { bytes_copied: usize_to_u64(std::mem::size_of::<Match>()), ..WorkCounters::UNCHARGED };
                accounting.admit(copied)?;
                self.matches.value.push(Match { entry, queries: query..group_end });
                accounting.record_completed(copied)?;
            }
            query = group_end;
        }
        while let Some(found) = self.matches.value.pop() {
            let copied = WorkCounters {
                bytes_copied: usize_to_u64(std::mem::size_of::<F::Value>() * (1 + usize::from(found.entry + 1 != entries.len()))),
                ..WorkCounters::UNCHARGED
            };
            accounting.admit(copied)?;
            let value = entries.swap_remove(found.entry);
            let nested = F::value_owned_bytes(&value);
            let allocation = split_allocation(page_allocation, nested)?;
            *page_bytes = page_bytes.checked_sub(nested).ok_or(crate::WorkError::Overflow)?;
            let value = Owned::new(value, allocation, nested);
            if let Err(error) = accounting.record_completed(copied) { value.retire(accounting)?; return Err(error); }
            let transferred = (|| {
                let mut queries = found.queries;
                let first = queries.next().ok_or_else(|| query_error(Error::InvalidRouting))?;
                for query in queries { self.clone_result(query, &value.value, accounting)?; }
                let ordinal = self.ordered.value.get(first).ok_or_else(|| query_error(Error::InvalidRouting))?.ordinal;
                self.values.value.get_mut(ordinal).ok_or_else(|| query_error(Error::InvalidRouting))?;
                accounting.admit(WorkCounters { bytes_copied: usize_to_u64(std::mem::size_of::<F::Value>()), ..WorkCounters::UNCHARGED })?;
                Ok(ordinal)
            })();
            match transferred {
                Ok(ordinal) => {
                    let Owned { value, mut allocation, bytes } = value;
                    self.values.allocation.absorb(&mut allocation)?;
                    self.values.bytes = self.values.bytes.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
                    *self.values.value.get_mut(ordinal).ok_or_else(|| query_error(Error::InvalidRouting))? = Some(value);
                    accounting.record_completed(WorkCounters { bytes_copied: usize_to_u64(std::mem::size_of::<F::Value>()), ..WorkCounters::UNCHARGED })?;
                }
                Err(error) => { value.retire(accounting)?; return Err(error); }
            }
        }
        Ok(())
    }

    fn clone_result(&mut self, query: usize, value: &F::Value, accounting: &mut KernelAccounting<'_>) -> Result<(), crate::FsError> {
        let ordinal = self.ordered.value.get(query).ok_or_else(|| query_error(Error::InvalidRouting))?.ordinal;
        let destination = self.values.value.get_mut(ordinal).ok_or_else(|| query_error(Error::InvalidRouting))?;
        let (cloned, allocation) = persistent_io::clone_value_with_accounting::<F>(value, self.limits.maximum_name_bytes, accounting)?;
        let bytes = F::value_owned_bytes(&cloned);
        let cloned = Owned::new(cloned, allocation, bytes);
        let copied = WorkCounters { bytes_copied: usize_to_u64(std::mem::size_of::<F::Value>()), ..WorkCounters::UNCHARGED };
        if let Err(error) = accounting.admit(copied) { cloned.retire(accounting)?; return Err(error); }
        let Owned { value: cloned, mut allocation, .. } = cloned;
        self.values.allocation.absorb(&mut allocation)?;
        self.values.bytes = self.values.bytes.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
        *destination = Some(cloned);
        accounting.record_completed(copied)?;
        Ok(())
    }

    fn accept_internal(&mut self, request: &Request<F::Key>, children: &[Child<F::Key>], accounting: &mut KernelAccounting<'_>) -> Result<(), crate::FsError> {
        if !children_within(children, request.lower.as_ref().map(|key| &key.value), request.upper.as_ref().map(|key| &key.value)) {
            return Err(query_error(Error::ChildBoundsMismatch));
        }
        let next_height = request.height.checked_add(1).ok_or_else(|| query_error(Error::HeightExceeded))?;
        admit_height(next_height, self.limits.maximum_page_height).map_err(query_error)?;
        let mut query = request.queries.start;
        while query < request.queries.end {
            let key = self.ordered.value.get(query).ok_or_else(|| query_error(Error::InvalidRouting))?.key;
            let child_index = partition_point_with_accounting(children, accounting, |child| child.first <= *key)?.saturating_sub(1);
            let child = children.get(child_index).ok_or_else(|| query_error(Error::InvalidRouting))?;
            let upper_ref = children.get(child_index + 1).map(|next| &next.first).or(request.upper.as_ref().map(|key| &key.value));
            let mut end = next_index(query).map_err(query_error)?;
            while end < request.queries.end {
                let delta = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
                accounting.admit(delta)?;
                let past_upper = upper_ref.is_some_and(|upper| self.ordered.value.get(end).is_some_and(|query| query.key >= upper));
                accounting.record_completed(delta)?;
                if past_upper { break; }
                end += 1;
            }
            let (lower, lower_allocation) = persistent_io::clone_key_with_accounting::<F>(&child.first, self.limits.maximum_name_bytes, accounting)?;
            let lower_bytes = F::key_owned_bytes(&lower);
            let lower = Owned::new(lower, lower_allocation, lower_bytes);
            let upper = match upper_ref {
                Some(key) => persistent_io::clone_key_with_accounting::<F>(key, self.limits.maximum_name_bytes, accounting)
                    .map(|(key, allocation)| {
                        let bytes = F::key_owned_bytes(&key);
                        Some(Owned::new(key, allocation, bytes))
                    }),
                None => Ok(None),
            };
            let upper = match upper {
                Ok(upper) => upper,
                Err(error) => { lower.retire(accounting)?; return Err(error); }
            };
            self.push_request(Request {
                page: child.page, lower: Some(lower), upper, queries: query..end, height: next_height,
            }, accounting)?;
            query = end;
        }
        Ok(())
    }

    fn retire_request(request: Request<F::Key>, accounting: &mut KernelAccounting<'_>) -> Result<(), crate::FsError> {
        if let Some(lower) = request.lower { lower.retire(accounting)?; }
        if let Some(upper) = request.upper { upper.retire(accounting)?; }
        Ok(())
    }

    fn cleanup_scratch(self, accounting: &mut KernelAccounting<'_>) -> Result<Owned<Vec<Option<F::Value>>>, crate::FsError> {
        let Self { ordered, values, mut requests, matches, visited, .. } = self;
        let mut failure = None;
        for request in requests.value.drain(..) {
            if let Err(error) = Self::retire_request(request, accounting) { failure.get_or_insert(error); }
        }
        if let Err(error) = requests.retire(accounting) { failure.get_or_insert(error); }
        if let Err(error) = matches.retire(accounting) { failure.get_or_insert(error); }
        if let Err(error) = ordered.retire(accounting) { failure.get_or_insert(error); }
        if let Err(error) = visited.retire(accounting) { failure.get_or_insert(error); }
        match failure {
            Some(error) => { values.retire(accounting)?; Err(error) }
            None => Ok(values),
        }
    }

    fn abort_cleanup(self, accounting: &mut KernelAccounting<'_>) -> Result<(), crate::FsError> {
        self.cleanup_scratch(accounting)?.retire(accounting)
    }

    fn finish(self, accounting: &mut KernelAccounting<'_>) -> Result<(OperationReceipt<Vec<Option<F::Value>>>, KernelOwnedAllocation), crate::FsError> {
        let returned = accounting.charge(WorkCounters { items_returned: usize_to_u64(self.values.value.len()), ..WorkCounters::UNCHARGED });
        if let Err(error) = returned { self.abort_cleanup(accounting)?; return Err(error); }
        let values = self.cleanup_scratch(accounting)?;
        let Owned { value: values, allocation, .. } = values;
        let work = accounting.spent();
        Ok((OperationReceipt { value: values, work }, allocation))
    }
}

fn sort_indexed<K: Ord>(
    values: &mut [IndexedKey<'_, K>],
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), crate::FsError> {
    let count = values.len();
    if count < 2 { return Ok(()); }
    accounting.admit(WorkCounters { items_examined: usize_to_u64(count - 1), ..WorkCounters::UNCHARGED })?;
    let item = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
    let mut ordered = true;
    for adjacent in values.windows(2) {
        let [left, right] = adjacent else { unreachable!("windows(2) yields two elements"); };
        accounting.admit(item)?;
        let comparison = indexed_order(left, right);
        accounting.record_completed(item)?;
        if comparison.is_gt() { ordered = false; break; }
    }
    if ordered { return Ok(()); }
    accounting.admit(WorkCounters {
        items_examined: sort_admission_bound(count).map_err(query_error)?,
        ..WorkCounters::UNCHARGED
    })?;
    for start in (0..count / 2).rev() {
        sift_down(values, start, count, accounting)?;
    }
    for end in (1..count).rev() {
        swap_indexed(values, 0, end, accounting)?;
        sift_down(values, 0, end, accounting)?;
    }
    Ok(())
}

fn swap_indexed<K>(
    values: &mut [IndexedKey<'_, K>], left: usize, right: usize,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), crate::FsError> {
    let bytes = usize_to_u64(std::mem::size_of::<IndexedKey<'_, K>>() * 2);
    let delta = WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED };
    accounting.admit(delta)?;
    values.swap(left, right);
    accounting.record_completed(delta)
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

fn sort_admission_bound(count: usize) -> Result<u64, Error> {
    let levels = usize::BITS - count.leading_zeros();
    usize_to_u64(count)
        .checked_mul(u64::from(levels))
        .and_then(|value| value.checked_mul(3))
        .ok_or(Error::Work(WorkError::Overflow))
}

#[allow(
    clippy::indexing_slicing,
    reason = "heap indices stay below end <= values.len()"
)]
fn sift_down<K: Ord>(
    values: &mut [IndexedKey<'_, K>],
    mut root: usize,
    end: usize,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), crate::FsError> {
    let item = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
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
            accounting.admit(item)?;
            let comparison = indexed_order(&values[left], &values[right]);
            accounting.record_completed(item)?;
            if comparison.is_lt() {
                greater = right;
            }
        }
        accounting.admit(item)?;
        let comparison = indexed_order(&values[root], &values[greater]);
        accounting.record_completed(item)?;
        if !comparison.is_lt() {
            return Ok(());
        }
        swap_indexed(values, root, greater, accounting)?;
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

#[allow(
    clippy::indexing_slicing,
    reason = "start and cursor stay below end <= values.len()"
)]
fn equal_group<K: Eq>(
    values: &[IndexedKey<'_, K>], start: usize, end: usize,
    accounting: &mut KernelAccounting<'_>,
) -> Result<usize, crate::FsError> {
    let mut cursor = start + 1;
    let item = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
    while cursor < end {
        accounting.admit(item)?;
        let equal = values[cursor].key == values[start].key;
        accounting.record_completed(item)?;
        if !equal {
            break;
        }
        cursor += 1;
    }
    Ok(cursor)
}

fn failed(error: Error, work: WorkCounters) -> Failure {
    OperationFailure::new(error, work)
}

#[cfg(test)]
#[path = "tests/persistent_batch.rs"]
mod tests;
