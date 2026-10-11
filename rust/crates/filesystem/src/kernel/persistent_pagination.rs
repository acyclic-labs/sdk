//! Shared bounded cursor pagination with field-owned original native custody.

#[cfg(test)]
use super::allocation::AllocationError;
use super::allocation::{AllocationLedger, KernelAccounting, KernelOwnedAllocation, VisitedObjectSet};
use super::persistent_btree::{Child, Format, Page, children_within, leaf_within};
use super::persistent_io::{self, OwnedPage};
use super::persistent_point::{Owned, retire_allocation, split_allocation};
#[cfg(test)]
use super::search::counted_partition_point;
use super::search::partition_point_with_accounting;
use super::{CanonicalDecodeError, DecodeLimits};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::foundation::usize_to_u64;
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

// Bounds are destroyed before their individual original-invocation tags.
struct PendingState<K> {
    pending: Pending<K>,
    lower_allocation: Option<KernelOwnedAllocation>,
    upper_allocation: Option<KernelOwnedAllocation>,
}

impl<K> std::ops::Deref for PendingState<K> {
    type Target = Pending<K>;
    fn deref(&self) -> &Self::Target { &self.pending }
}

impl<K> std::ops::DerefMut for PendingState<K> {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.pending }
}

#[cfg(test)]
impl<K> From<Pending<K>> for PendingState<K> {
    fn from(pending: Pending<K>) -> Self {
        let lower_allocation = pending.lower.as_ref().map(|_| KernelOwnedAllocation::Standalone);
        let upper_allocation = pending.upper.as_ref().map(|_| KernelOwnedAllocation::Standalone);
        Self { pending, lower_allocation, upper_allocation }
    }
}

impl<K> PendingState<K> {
    fn retire(self, accounting: &mut KernelAccounting<'_>, nested: impl Fn(&K) -> u64) -> Result<(), crate::FsError> {
        let Self { pending, lower_allocation, upper_allocation } = self;
        let lower_bytes = pending.lower.as_ref().map_or(0, &nested);
        let upper_bytes = pending.upper.as_ref().map_or(0, nested);
        drop(pending);
        if let Some(allocation) = lower_allocation { retire_allocation(allocation, lower_bytes, accounting)?; }
        if let Some(allocation) = upper_allocation { retire_allocation(allocation, upper_bytes, accounting)?; }
        Ok(())
    }
}

pub(crate) fn paginate<S, F>(
    store: &S, root: ObjectId, after: Option<&F::Key>, maximum_values: u32,
    limits: DecodeLimits, budget: WorkBudget,
) -> Result<Receipt<F::Value>, Failure>
where S: crate::ImmediateObjectStore, F: Format {
    crate::async_storage::poll_immediate(paginate_async::<S, F>(
        store, root, after, maximum_values, limits, budget, &CancellationToken::new(), None,
    ))
}

pub(crate) async fn paginate_async<S, F>(
    store: &S, root: ObjectId, after: Option<&F::Key>, maximum_values: u32,
    limits: DecodeLimits, budget: WorkBudget, cancellation: &CancellationToken,
    data_custody: Option<&super::ProofDataCustody>,
) -> Result<Receipt<F::Value>, Failure>
where S: AsyncObjectStore, F: Format {
    paginate_async_with_bound::<S, F>(
        store, root, after, false, maximum_values, limits, budget, cancellation, data_custody,
    ).await
}

pub(crate) async fn paginate_async_at_or_after<S, F>(
    store: &S, root: ObjectId, at: &F::Key, maximum_values: u32,
    limits: DecodeLimits, budget: WorkBudget, cancellation: &CancellationToken,
    data_custody: Option<&super::ProofDataCustody>,
) -> Result<Receipt<F::Value>, Failure>
where S: AsyncObjectStore, F: Format {
    paginate_async_with_bound::<S, F>(
        store, root, Some(at), true, maximum_values, limits, budget, cancellation, data_custody,
    ).await
}

#[allow(clippy::too_many_arguments, reason = "independent authenticated cursor inputs")]
async fn paginate_async_with_bound<S, F>(
    store: &S, root: ObjectId, after: Option<&F::Key>, inclusive: bool,
    maximum_values: u32, limits: DecodeLimits, budget: WorkBudget,
    cancellation: &CancellationToken, data_custody: Option<&super::ProofDataCustody>,
) -> Result<Receipt<F::Value>, Failure>
where S: AsyncObjectStore, F: Format {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    let result = paginate_with_bound_and_accounting::<S, F>(
        store, root, after, inclusive, maximum_values, limits, &mut accounting, cancellation, data_custody,
    ).await;
    result.map(|(receipt, _)| receipt)
        .map_err(|error| failed(project_failure(error), accounting.spent()))
}

/// Both projections execute the actual cursor machine on this accounting.
pub(crate) async fn paginate_async_with_accounting<S, F>(
    store: &S, root: ObjectId, after: Option<&F::Key>, maximum_values: u32,
    limits: DecodeLimits, accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken, data_custody: Option<&super::ProofDataCustody>,
) -> Result<(Receipt<F::Value>, KernelOwnedAllocation), crate::FsError>
where S: AsyncObjectStore, F: Format {
    paginate_with_bound_and_accounting::<S, F>(
        store, root, after, false, maximum_values, limits, accounting, cancellation, data_custody,
    ).await
}

pub(crate) async fn paginate_async_at_or_after_with_accounting<S, F>(
    store: &S, root: ObjectId, at: &F::Key, maximum_values: u32,
    limits: DecodeLimits, accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken, data_custody: Option<&super::ProofDataCustody>,
) -> Result<(Receipt<F::Value>, KernelOwnedAllocation), crate::FsError>
where S: AsyncObjectStore, F: Format {
    paginate_with_bound_and_accounting::<S, F>(
        store, root, Some(at), true, maximum_values, limits, accounting, cancellation, data_custody,
    ).await
}

#[allow(clippy::too_many_arguments, reason = "independent authenticated cursor inputs")]
async fn paginate_with_bound_and_accounting<S, F>(
    store: &S, root: ObjectId, after: Option<&F::Key>, inclusive: bool,
    maximum_values: u32, limits: DecodeLimits, accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken, data_custody: Option<&super::ProofDataCustody>,
) -> Result<(Receipt<F::Value>, KernelOwnedAllocation), crate::FsError>
where S: AsyncObjectStore, F: Format {
    accounting.check()?;
    let borrowed = match accounting {
        KernelAccounting::Standalone { allocations, work, budget, cancellation } => {
            KernelAccounting::borrowed_standalone(allocations, work, *budget, cancellation)
        }
        KernelAccounting::BorrowedStandalone { allocations, work, budget, cancellation } => {
            KernelAccounting::borrowed_standalone(allocations, work, *budget, cancellation)
        }
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { work, cancellation } => KernelAccounting::native(work, cancellation),
    };
    let mut machine = Machine::<F>::new_with_accounting(
        root, after, inclusive, maximum_values, limits, borrowed, data_custody,
    ).map_err(|failure| query_error(failure.error))?;
    let result: Result<(), Failure> = async {
        while machine.values.len() < machine.target && !machine.pending.value.is_empty() {
            cancellation.check().map_err(|error| machine.failure(error.into()))?;
            let mut pending = Some(machine.pop_pending()?);
            let page = pending.as_ref().ok_or_else(|| failed(Error::TraversalState, machine.accounting.spent()))?.page;
            let step = async {
                machine.visit(page)?;
                let (decoded, allocation) = persistent_io::read_page_with_accounting::<S, F>(
                    store, page, limits, &mut machine.accounting, cancellation, data_custody,
                ).await.map_err(|error| machine.failure(error))?;
                let pending = pending.take().ok_or_else(|| failed(Error::TraversalState, machine.accounting.spent()))?;
                machine.accept(decoded, allocation, pending)
            }.await;
            if let Some(pending) = pending {
                pending.retire(&mut machine.accounting, F::key_owned_bytes).map_err(|error| machine.failure(error))?;
            }
            step?;
        }
        Ok(())
    }.await;
    match result {
        Ok(()) => machine.finish().map_err(|failure| query_error(failure.error)),
        Err(failure) => {
            machine.retire().map_err(query_error)?;
            Err(query_error(failure.error))
        }
    }
}

struct Machine<'a, F: Format> {
    after: Option<&'a F::Key>,
    inclusive: bool,
    maximum_values: usize,
    target: usize,
    maximum_traversal_pages: usize,
    limits: DecodeLimits,
    pending: Owned<Vec<PendingState<F::Key>>>,
    visited: Option<Owned<VisitedObjectSet>>,
    values: Vec<F::Value>,
    values_allocation: KernelOwnedAllocation,
    values_container_bytes: u64,
    values_nested_bytes: u64,
    successor_page: Option<ObjectId>,
    work: WorkCounters,
    format: PhantomData<F>,
    data_custody: Option<&'a super::ProofDataCustody>,
    accounting: KernelAccounting<'a>,
}

impl<'a, F: Format> Machine<'a, F> {
    #[cfg(test)]
    fn new(
        root: ObjectId, after: Option<&'a F::Key>, maximum_values: u32,
        limits: DecodeLimits, budget: WorkBudget,
    ) -> Result<Self, Failure> {
        static CANCELLATION: std::sync::LazyLock<CancellationToken> = std::sync::LazyLock::new(CancellationToken::new);
        Self::new_with_accounting(root, after, false, maximum_values, limits,
            KernelAccounting::standalone(budget, &CANCELLATION), None)
    }

    fn new_with_accounting(
        root: ObjectId, after: Option<&'a F::Key>, inclusive: bool,
        maximum_values: u32, limits: DecodeLimits, mut accounting: KernelAccounting<'a>,
        data_custody: Option<&'a super::ProofDataCustody>,
    ) -> Result<Self, Failure> {
        let fail = |error| failed(error, accounting.spent());
        if root.kind != F::kind() { return Err(fail(Error::WrongRootKind)); }
        if maximum_values == 0 { return Err(fail(Error::ZeroLimit)); }
        if !limits.page_limits_valid(1) { return Err(fail(Error::InvalidLimits)); }
        if let Some(observed) = after.map(F::key_nested_bytes)
            .filter(|&bytes| bytes > u64::from(limits.maximum_name_bytes)) {
            return Err(fail(Error::Decode(CanonicalDecodeError::FieldTooLarge {
                observed: u32::try_from(observed).unwrap_or(u32::MAX), maximum: limits.maximum_name_bytes,
            })));
        }
        let maximum_values = usize::try_from(maximum_values).map_err(|_| fail(Error::LimitOverflow))?;
        let target = maximum_values.checked_add(1).ok_or_else(|| fail(Error::LimitOverflow))?;
        let configured_maximum = usize::try_from(limits.maximum_visited_pages)
            .map_err(|_| fail(Error::InvalidLimits))?;
        // One cursor-exhausted subtree plus each requested value/witness frontier.
        let maximum_traversal_pages = target.checked_add(1)
            .and_then(|value| value.checked_mul(usize::from(limits.maximum_page_height)))
            .ok_or_else(|| fail(Error::LimitOverflow))?.min(configured_maximum);
        accounting.admit(WorkCounters {
            items_returned: usize_to_u64(maximum_values), ..WorkCounters::default()
        }).map_err(|error| failed(project_failure(error), accounting.spent()))?;
        let prefix = begin_custody_leaf(&mut accounting, data_custody)
            .map_err(|error| failed(project_failure(error), accounting.spent()))?;
        let values_result = Owned::<Vec<F::Value>>::allocate(target, &mut accounting);
        let completed = finish_custody_leaf(&mut accounting, data_custody, prefix);
        let values = values_result.map_err(|error| failed(project_failure(error), accounting.spent()))?;
        if let Err(error) = completed {
            values.retire(&mut accounting).map_err(|error| failed(project_failure(error), accounting.spent()))?;
            return Err(failed(project_failure(error), accounting.spent()));
        }
        let prefix = match begin_custody_leaf(&mut accounting, data_custody) {
            Ok(prefix) => prefix,
            Err(error) => {
                values.retire(&mut accounting).map_err(|error| failed(project_failure(error), accounting.spent()))?;
                return Err(failed(project_failure(error), accounting.spent()));
            }
        };
        let visited_result = VisitedObjectSet::new_with_accounting(maximum_traversal_pages, &mut accounting);
        let completed = finish_custody_leaf(&mut accounting, data_custody, prefix);
        let visited = match visited_result {
            Ok(result) => result,
            Err(error) => {
                values.retire(&mut accounting).map_err(|error| failed(project_failure(error), accounting.spent()))?;
                return Err(failed(project_failure(error), accounting.spent()));
            }
        };
        if let Err(error) = completed {
            visited.retire(&mut accounting)
                .map_err(|error| failed(project_failure(error), accounting.spent()))?;
            values.retire(&mut accounting).map_err(|error| failed(project_failure(error), accounting.spent()))?;
            return Err(failed(project_failure(error), accounting.spent()));
        }
        let pending_result = accounting.claim_owned(0, 0).map(|allocation| (Vec::new(), allocation));
        let (pending, pending_allocation) = match pending_result {
            Ok(result) => result,
            Err(error) => {
                visited.retire(&mut accounting)
                    .map_err(|error| failed(project_failure(error), accounting.spent()))?;
                values.retire(&mut accounting).map_err(|error| failed(project_failure(error), accounting.spent()))?;
                return Err(failed(project_failure(error), accounting.spent()));
            }
        };
        let mut machine = Self {
            after, inclusive, maximum_values, target, maximum_traversal_pages, limits,
            pending: Owned::new(pending, pending_allocation, 0),
            visited: Some(visited),
            values: values.value, values_allocation: values.allocation,
            values_container_bytes: values.bytes, values_nested_bytes: 0,
            successor_page: None, work: accounting.spent(), format: PhantomData,
            data_custody, accounting,
        };
        if let Err(failure) = machine.push_pending(PendingState {
            pending: Pending { page: root, lower: None, upper: None, height: 1, nested_bytes: 0 },
            lower_allocation: None, upper_allocation: None,
        }) {
            machine.retire().map_err(|error| failed(error, *failure.work))?;
            return Err(failure);
        }
        Ok(machine)
    }

    fn failure(&self, error: crate::FsError) -> Failure {
        failed(project_failure(error), self.accounting.spent())
    }

    fn push_pending(&mut self, pending: impl Into<PendingState<F::Key>>) -> Result<(), Failure> {
        let pending = pending.into();
        let prefix = begin_custody_leaf(&mut self.accounting, self.data_custody);
        let result = match prefix {
            Ok(prefix) => {
                let result = self.pending.ensure_for_push(self.maximum_traversal_pages, &mut self.accounting);
                let completed = finish_custody_leaf(&mut self.accounting, self.data_custody, prefix);
                result.and(completed)
            }
            Err(error) => Err(error),
        };
        self.work = self.accounting.spent();
        if let Err(error) = result {
            pending.retire(&mut self.accounting, F::key_owned_bytes).map_err(|error| self.failure(error))?;
            return Err(self.failure(error));
        }
        let copied = WorkCounters { bytes_copied: usize_to_u64(size_of::<PendingState<F::Key>>()), ..WorkCounters::default() };
        let prefix = match begin_custody_leaf(&mut self.accounting, self.data_custody) {
            Ok(prefix) => prefix,
            Err(error) => {
                pending.retire(&mut self.accounting, F::key_owned_bytes).map_err(|error| self.failure(error))?;
                return Err(self.failure(error));
            }
        };
        if let Err(error) = self.accounting.admit(copied) {
            let completed = finish_custody_leaf(&mut self.accounting, self.data_custody, prefix);
            pending.retire(&mut self.accounting, F::key_owned_bytes).map_err(|error| self.failure(error))?;
            completed.map_err(|error| self.failure(error))?;
            return Err(self.failure(error));
        }
        self.pending.value.push(pending);
        let recorded = self.accounting.record_completed(copied);
        let completed = finish_custody_leaf(&mut self.accounting, self.data_custody, prefix);
        self.work = self.accounting.spent();
        recorded.and(completed).map_err(|error| self.failure(error))
    }

    fn pop_pending(&mut self) -> Result<PendingState<F::Key>, Failure> {
        let pending = self.pending.value.pop().ok_or_else(|| failed(Error::TraversalState, self.accounting.spent()))?;
        if pending.height > self.limits.maximum_page_height {
            pending.retire(&mut self.accounting, F::key_owned_bytes).map_err(|error| self.failure(error))?;
            return Err(failed(Error::HeightExceeded, self.accounting.spent()));
        }
        Ok(pending)
    }

    fn visit(&mut self, page: ObjectId) -> Result<(), Failure> {
        let prefix = begin_custody_leaf(&mut self.accounting, self.data_custody).map_err(|error| self.failure(error))?;
        let visited = self.visited.as_mut().ok_or_else(|| failed(Error::TraversalState, self.accounting.spent()))?;
        let result = VisitedObjectSet::insert_with_accounting(visited, page, &mut self.accounting);
        let completed = finish_custody_leaf(&mut self.accounting, self.data_custody, prefix);
        self.work = self.accounting.spent();
        let inserted = result.map_err(|error| failed(project_failure(error), self.work))?;
        completed.map_err(|error| failed(project_failure(error), self.work))?;
        if !inserted.inserted { return Err(failed(Error::CycleOrAlias, self.work)); }
        Ok(())
    }

    fn completed(&mut self, delta: WorkCounters) -> Result<(), Failure> {
        let result = self.accounting.record_completed(delta);
        self.work = self.accounting.spent();
        result.map_err(|error| self.failure(error))
    }

    fn examine_value(&mut self, value: &F::Value) -> Result<u64, Failure> {
        let examined = WorkCounters { items_examined: 1, ..WorkCounters::default() };
        self.accounting.admit(examined).map_err(|error| self.failure(error))?;
        let bytes = F::value_owned_bytes(value);
        self.completed(examined)?;
        Ok(bytes)
    }

    fn leaf_bounds(&mut self, values: &[F::Value], pending: &Pending<F::Key>) -> Result<bool, Failure> {
        let examined = WorkCounters { items_examined: if values.is_empty() { 0 } else { 2 }, ..WorkCounters::default() };
        self.accounting.admit(examined).map_err(|error| self.failure(error))?;
        let within = leaf_within::<F>(values, pending.lower.as_ref(), pending.upper.as_ref());
        self.completed(examined)?;
        Ok(within)
    }

    fn child_bounds(&mut self, children: &[Child<F::Key>], pending: &Pending<F::Key>) -> Result<bool, Failure> {
        let examined = WorkCounters { items_examined: if children.is_empty() { 0 } else { 2 }, ..WorkCounters::default() };
        self.accounting.admit(examined).map_err(|error| self.failure(error))?;
        let within = children_within(children, pending.lower.as_ref(), pending.upper.as_ref());
        self.completed(examined)?;
        Ok(within)
    }

    fn value_start(&mut self, values: &[F::Value]) -> Result<usize, Failure> {
        let Some(cursor) = self.after else { return Ok(0); };
        let inclusive = self.inclusive;
        let result = partition_point_with_accounting(values, &mut self.accounting, |value| {
            let key = F::key(value);
            key < cursor || (!inclusive && key == cursor)
        });
        self.work = self.accounting.spent();
        result.map_err(|error| self.failure(error))
    }

    fn child_start(&mut self, children: &[Child<F::Key>]) -> Result<usize, Failure> {
        let Some(cursor) = self.after else { return Ok(0); };
        let result = partition_point_with_accounting(children, &mut self.accounting, |child| child.first <= *cursor);
        self.work = self.accounting.spent();
        result.map(|index| index.saturating_sub(1)).map_err(|error| self.failure(error))
    }

    fn accept(
        &mut self, decoded: OwnedPage<F>, mut allocation: KernelOwnedAllocation,
        mut pending: PendingState<F::Key>,
    ) -> Result<(), Failure> {
        #[cfg(not(target_arch = "wasm32"))]
        let native_custody = decoded.native_custody;
        let logical_bytes = decoded.logical_bytes;
        let page = decoded.page;
        let result = match page {
            persistent_io::PageLease::Owned(Page::Leaf(values)) => {
                self.accept_leaf_owned(values, logical_bytes, &mut allocation, &pending.pending)
            }
            persistent_io::PageLease::Owned(Page::Internal(children)) => {
                self.accept_internal_owned(children, logical_bytes, &mut allocation, &mut pending)
            }
            persistent_io::PageLease::Shared { page, .. } => {
                // Keep the decoded cache's original guard through the entire callback.
                let result = match page.as_ref() {
                    Page::Leaf(values) => self.accept_shared_leaf(values, &pending.pending),
                    Page::Internal(children) => self.accept_shared_internal_state(children, &mut pending),
                };
                drop(page);
                result
            }
        };
        #[cfg(not(target_arch = "wasm32"))]
        drop(native_custody);
        drop(allocation);
        pending.retire(&mut self.accounting, F::key_owned_bytes).map_err(|error| self.failure(error))?;
        result?;
        self.work = self.accounting.spent();
        Ok(())
    }

    fn accept_shared_leaf(&mut self, values: &[F::Value], pending: &Pending<F::Key>) -> Result<(), Failure> {
        if !self.leaf_bounds(values, pending)? {
            return Err(failed(Error::ChildBoundsMismatch, self.accounting.spent()));
        }
        let start = self.value_start(values)?;
        for value in values.iter().skip(start).take(self.target.saturating_sub(self.values.len())) {
            self.examine_value(value)?;
            let prefix = begin_custody_leaf(&mut self.accounting, self.data_custody).map_err(|error| self.failure(error))?;
            let result = persistent_io::clone_value_with_accounting::<F>(
                value, self.limits.maximum_name_bytes, &mut self.accounting,
            );
            let completed = finish_custody_leaf(&mut self.accounting, self.data_custody, prefix);
            let (cloned, allocation) = result.map_err(|error| self.failure(error))?;
            let nested = F::value_owned_bytes(&cloned);
            let cloned = Owned::new(cloned, allocation, nested);
            if let Err(error) = completed {
                cloned.retire(&mut self.accounting).map_err(|error| self.failure(error))?;
                return Err(self.failure(error));
            }
            self.push_value(cloned)?;
        }
        Ok(())
    }

    fn accept_shared_internal_state(
        &mut self, children: &[Child<F::Key>], pending: &mut PendingState<F::Key>,
    ) -> Result<(), Failure> {
        if !self.child_bounds(children, pending)? {
            return Err(failed(Error::ChildBoundsMismatch, self.accounting.spent()));
        }
        self.validate_pending(pending)?;
        let start = self.child_start(children)?;
        let height = self.next_height(pending.height)?;
        let end = start.saturating_add(self.target.saturating_sub(self.values.len()).saturating_add(1)).min(children.len());
        if let Some(child) = children.get(end) { self.successor_page = Some(child.page); }
        let mut next_lower: Option<Owned<F::Key>> = None;
        let result = (|| {
            for (offset, child) in children.iter().enumerate().take(end).skip(start).rev() {
                let lower = self.clone_key(&child.first)?;
                let upper = match next_lower.take() {
                    Some(next) => Some(next),
                    None => match self.take_upper(pending) {
                        Ok(upper) => upper,
                        Err(failure) => {
                            lower.retire(&mut self.accounting).map_err(|error| self.failure(error))?;
                            return Err(failure);
                        }
                    },
                };
                let previous = if offset == start { Ok(None) } else { self.clone_key(&child.first).map(Some) };
                let previous = match previous {
                    Ok(previous) => previous,
                    Err(failure) => {
                        self.retire_bounds(Some(lower), upper)?;
                        return Err(failure);
                    }
                };
                if let Err(failure) = self.push_bounds(child.page, Some(lower), upper, height) {
                    self.retire_bounds(previous, None)?;
                    return Err(failure);
                }
                next_lower = previous;
            }
            Ok(())
        })();
        self.retire_bounds(next_lower, None)?;
        result
    }

    fn validate_pending(&self, pending: &Pending<F::Key>) -> Result<(), Failure> {
        let expected = pending.lower.as_ref().map_or(0, F::key_owned_bytes)
            .checked_add(pending.upper.as_ref().map_or(0, F::key_owned_bytes))
            .ok_or_else(|| failed(Error::TraversalState, self.accounting.spent()))?;
        if expected != pending.nested_bytes { return Err(failed(Error::TraversalState, self.accounting.spent())); }
        Ok(())
    }

    fn next_height(&self, height: u16) -> Result<u16, Failure> {
        height.checked_add(1).filter(|&next| next <= self.limits.maximum_page_height)
            .ok_or_else(|| failed(Error::HeightExceeded, self.accounting.spent()))
    }

    fn take_upper(&mut self, pending: &mut PendingState<F::Key>) -> Result<Option<Owned<F::Key>>, Failure> {
        let Some(value) = pending.pending.upper.take() else { return Ok(None); };
        let bytes = F::key_owned_bytes(&value);
        pending.pending.nested_bytes = pending.pending.nested_bytes.checked_sub(bytes)
            .ok_or_else(|| failed(Error::TraversalState, self.accounting.spent()))?;
        let allocation = pending.upper_allocation.take()
            .ok_or_else(|| failed(Error::TraversalState, self.accounting.spent()))?;
        Ok(Some(Owned::new(value, allocation, bytes)))
    }

    fn retire_bounds(&mut self, lower: Option<Owned<F::Key>>, upper: Option<Owned<F::Key>>) -> Result<(), Failure> {
        if let Some(lower) = lower { lower.retire(&mut self.accounting).map_err(|error| self.failure(error))?; }
        if let Some(upper) = upper { upper.retire(&mut self.accounting).map_err(|error| self.failure(error))?; }
        Ok(())
    }

    fn push_bounds(
        &mut self, page: ObjectId, lower: Option<Owned<F::Key>>, upper: Option<Owned<F::Key>>, height: u16,
    ) -> Result<(), Failure> {
        let nested_bytes = match lower.as_ref().map_or(0, |key| key.bytes)
            .checked_add(upper.as_ref().map_or(0, |key| key.bytes)) {
            Some(bytes) => bytes,
            None => {
                self.retire_bounds(lower, upper)?;
                return Err(failed(Error::AllocationFailed, self.accounting.spent()));
            }
        };
        let (lower, lower_allocation) = lower.map_or((None, None), |key| (Some(key.value), Some(key.allocation)));
        let (upper, upper_allocation) = upper.map_or((None, None), |key| (Some(key.value), Some(key.allocation)));
        self.push_pending(PendingState {
            pending: Pending { page, lower, upper, height, nested_bytes },
            lower_allocation, upper_allocation,
        })
    }

    fn accept_leaf_owned(
        &mut self, values: Vec<F::Value>, decoded_bytes: u64,
        allocation: &mut KernelOwnedAllocation, pending: &Pending<F::Key>,
    ) -> Result<(), Failure> {
        let mut retained_nested = 0_u64;
        let result = (|| {
            if !self.leaf_bounds(&values, pending)? {
                return Err(failed(Error::ChildBoundsMismatch, self.accounting.spent()));
            }
            let start = self.value_start(&values)?;
            // Iterator destruction drops every skipped/unselected item and the source buffer.
            for value in values.into_iter().skip(start) {
                if self.values.len() == self.target { break; }
                let nested = self.examine_value(&value)?;
                let tag = split_allocation(allocation, nested).map_err(|error| self.failure(error))?;
                retained_nested = retained_nested.checked_add(nested)
                    .ok_or_else(|| failed(Error::AllocationFailed, self.accounting.spent()))?;
                self.push_value(Owned::new(value, tag, nested))?;
            }
            Ok(())
        })();
        let released = decoded_bytes.checked_sub(retained_nested)
            .ok_or_else(|| failed(Error::TraversalState, self.accounting.spent()))?;
        if matches!(allocation, KernelOwnedAllocation::Standalone) {
            retire_allocation(KernelOwnedAllocation::Standalone, released, &mut self.accounting)
                .map_err(|error| self.failure(error))?;
        }
        result
    }

    fn accept_internal_owned(
        &mut self, children: Vec<Child<F::Key>>, decoded_bytes: u64,
        allocation: &mut KernelOwnedAllocation, pending: &mut PendingState<F::Key>,
    ) -> Result<(), Failure> {
        let mut moved_nested = 0_u64;
        let mut next_lower: Option<Owned<F::Key>> = None;
        let result = (|| {
            if !self.child_bounds(&children, pending)? {
                return Err(failed(Error::ChildBoundsMismatch, self.accounting.spent()));
            }
            self.validate_pending(pending)?;
            let start = self.child_start(&children)?;
            let height = self.next_height(pending.height)?;
            let end = start.saturating_add(self.target.saturating_sub(self.values.len()).saturating_add(1)).min(children.len());
            if let Some(child) = children.get(end) { self.successor_page = Some(child.page); }
            let mut children = children;
            children.truncate(end);
            let mut tail = children.drain(start..);
            while let Some(child) = tail.next_back() {
                let previous = if tail.len() == 0 { None } else { Some(self.clone_key(&child.first)?) };
                let bytes = F::key_owned_bytes(&child.first);
                let tag = match split_allocation(allocation, bytes) {
                    Ok(tag) => tag,
                    Err(error) => {
                        self.retire_bounds(previous, None)?;
                        return Err(self.failure(error));
                    }
                };
                moved_nested = moved_nested.checked_add(bytes)
                    .ok_or_else(|| failed(Error::AllocationFailed, self.accounting.spent()))?;
                let lower = Owned::new(child.first, tag, bytes);
                let upper = match next_lower.take() {
                    Some(next) => Some(next),
                    None => match self.take_upper(pending) {
                        Ok(upper) => upper,
                        Err(failure) => {
                            self.retire_bounds(Some(lower), previous)?;
                            return Err(failure);
                        }
                    },
                };
                if let Err(failure) = self.push_bounds(child.page, Some(lower), upper, height) {
                    self.retire_bounds(previous, None)?;
                    return Err(failure);
                }
                next_lower = previous;
            }
            drop(tail);
            drop(children);
            Ok(())
        })();
        self.retire_bounds(next_lower, None)?;
        let released = decoded_bytes.checked_sub(moved_nested)
            .ok_or_else(|| failed(Error::TraversalState, self.accounting.spent()))?;
        if matches!(allocation, KernelOwnedAllocation::Standalone) {
            retire_allocation(KernelOwnedAllocation::Standalone, released, &mut self.accounting)
                .map_err(|error| self.failure(error))?;
        }
        result
    }

    #[cfg(test)]
    fn accept_shared_internal(&mut self, children: &[Child<F::Key>], pending: &mut Pending<F::Key>) -> Result<(), Failure> {
        let mut state = PendingState::from(Pending {
            page: pending.page, lower: pending.lower.take(), upper: pending.upper.take(),
            height: pending.height, nested_bytes: pending.nested_bytes,
        });
        let result = self.accept_shared_internal_state(children, &mut state);
        *pending = state.pending;
        result
    }

    #[cfg(test)]
    fn accept_internal(&mut self, children: Vec<Child<F::Key>>, decoded_bytes: u64, pending: &mut Pending<F::Key>) -> Result<(), Failure> {
        // The direct machine test supplies deliberately corrupt accounting.
        self.validate_pending(pending)?;
        let mut state = PendingState::from(Pending {
            page: pending.page, lower: pending.lower.take(), upper: pending.upper.take(),
            height: pending.height, nested_bytes: pending.nested_bytes,
        });
        let result = self.accept_internal_owned(children, decoded_bytes, &mut KernelOwnedAllocation::Standalone, &mut state);
        *pending = state.pending;
        result
    }

    fn clone_key(&mut self, key: &F::Key) -> Result<Owned<F::Key>, Failure> {
        let prefix = begin_custody_leaf(&mut self.accounting, self.data_custody).map_err(|error| self.failure(error))?;
        let result = persistent_io::clone_key_with_accounting::<F>(key, self.limits.maximum_name_bytes, &mut self.accounting);
        let completed = finish_custody_leaf(&mut self.accounting, self.data_custody, prefix);
        self.work = self.accounting.spent();
        let (value, allocation) = result.map_err(|error| self.failure(error))?;
        let bytes = F::key_owned_bytes(&value);
        let owned = Owned::new(value, allocation, bytes);
        if let Err(error) = completed {
            owned.retire(&mut self.accounting).map_err(|error| self.failure(error))?;
            return Err(self.failure(error));
        }
        Ok(owned)
    }

    fn push_value(&mut self, owned: Owned<F::Value>) -> Result<(), Failure> {
        let prefix = match begin_custody_leaf(&mut self.accounting, self.data_custody) {
            Ok(prefix) => prefix,
            Err(error) => {
                owned.retire(&mut self.accounting).map_err(|error| self.failure(error))?;
                return Err(self.failure(error));
            }
        };
        let copied = WorkCounters { bytes_copied: usize_to_u64(size_of::<F::Value>()), ..WorkCounters::default() };
        let admitted = self.accounting.admit(copied);
        let nested = match (admitted, self.values_nested_bytes.checked_add(owned.bytes)) {
            (Ok(()), Some(nested)) => nested,
            (admitted, _) => {
                let completed = finish_custody_leaf(&mut self.accounting, self.data_custody, prefix);
                owned.retire(&mut self.accounting).map_err(|error| self.failure(error))?;
                completed.map_err(|error| self.failure(error))?;
                admitted.map_err(|error| self.failure(error))?;
                return Err(failed(Error::AllocationFailed, self.accounting.spent()));
            }
        };
        let Owned { value, mut allocation, bytes } = owned;
        if let Err(error) = self.values_allocation.absorb(&mut allocation) {
            let completed = finish_custody_leaf(&mut self.accounting, self.data_custody, prefix);
            Owned::new(value, allocation, bytes).retire(&mut self.accounting).map_err(|error| self.failure(error))?;
            completed.map_err(|error| self.failure(error))?;
            return Err(self.failure(error));
        }
        self.values.push(value);
        self.values_nested_bytes = nested;
        let recorded = self.accounting.record_completed(copied);
        let completed = finish_custody_leaf(&mut self.accounting, self.data_custody, prefix);
        self.work = self.accounting.spent();
        recorded.and(completed).map_err(|error| self.failure(error))
    }

    fn retire_traversal(&mut self) -> Result<(), crate::FsError> {
        for pending in self.pending.value.drain(..) {
            pending.retire(&mut self.accounting, F::key_owned_bytes)?;
        }
        let empty_allocation = split_allocation(&mut self.pending.allocation, 0)?;
        let pending = std::mem::replace(&mut self.pending, Owned::new(Vec::new(), empty_allocation, 0));
        pending.retire(&mut self.accounting)?;
        if let Some(visited) = self.visited.take() {
            visited.retire(&mut self.accounting)?;
        }
        Ok(())
    }

    fn retire(mut self) -> Result<(), Error> {
        self.retire_traversal().map_err(project_failure)?;
        drop(self.values);
        retire_allocation(self.values_allocation,
            self.values_container_bytes.checked_add(self.values_nested_bytes).ok_or(Error::TraversalState)?,
            &mut self.accounting).map_err(project_failure)
    }

    fn finish_state(&mut self) -> Result<(bool, Option<ObjectReadRequest>), Failure> {
        let has_more = self.values.len() > self.maximum_values;
        let next_request = has_more.then(|| self.pending.value.last().map(|pending| pending.page).or(self.successor_page))
            .flatten().map(|object_id| ObjectReadRequest { object_id, maximum_bytes: self.limits.maximum_page_object_bytes() });
        if has_more {
            let removed = self.values.pop().ok_or_else(|| failed(Error::TraversalState, self.accounting.spent()))?;
            let bytes = F::value_owned_bytes(&removed);
            let tag = split_allocation(&mut self.values_allocation, bytes).map_err(|error| self.failure(error))?;
            drop(removed);
            retire_allocation(tag, bytes, &mut self.accounting).map_err(|error| self.failure(error))?;
            self.values_nested_bytes = self.values_nested_bytes.checked_sub(bytes)
                .ok_or_else(|| failed(Error::TraversalState, self.accounting.spent()))?;
        }
        self.completed(WorkCounters { items_returned: usize_to_u64(self.values.len()), ..WorkCounters::default() })?;
        self.retire_traversal().map_err(|error| self.failure(error))?;
        Ok((has_more, next_request))
    }

    fn finish(mut self) -> Result<(Receipt<F::Value>, KernelOwnedAllocation), Failure> {
        let (has_more, next_request) = match self.finish_state() {
            Ok(result) => result,
            Err(failure) => {
                self.retire().map_err(|error| failed(error, *failure.work))?;
                return Err(failure);
            }
        };
        let receipt = Receipt { values: self.values, has_more, next_request, work: self.accounting.spent() };
        Ok((receipt, self.values_allocation))
    }
}


pub(super) struct CustodyPrefix {
    before: WorkCounters,
    maximum_peak: u64,
    external: u64,
}

fn standalone_parts<'a>(
    accounting: &'a mut KernelAccounting<'_>,
) -> Option<(&'a mut AllocationLedger, &'a mut WorkCounters, &'a mut WorkBudget)> {
    match accounting {
        KernelAccounting::Standalone { allocations, work, budget, .. } => Some((allocations, work, budget)),
        KernelAccounting::BorrowedStandalone { allocations, work, budget, .. } => Some((allocations, work, budget)),
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { .. } => None,
    }
}

// The standalone proof projection retains its cache-residency adjustment.
// Original native leaves already charge their own ledger; they never replay work.
pub(super) fn begin_custody_leaf(
    accounting: &mut KernelAccounting<'_>, custody: Option<&super::ProofDataCustody>,
) -> Result<Option<CustodyPrefix>, crate::FsError> {
    let Some(owner) = custody else { return Ok(None); };
    let Some((_, work, budget)) = standalone_parts(accounting) else { return Ok(None); };
    let external = owner.operation_cache_bytes();
    let available = budget.peak_allocation_bytes.checked_sub(external).ok_or(WorkError::BudgetExceeded {
        counter: "peak_allocation_bytes", observed: external, maximum: budget.peak_allocation_bytes,
    })?;
    let prefix = CustodyPrefix { before: *work, maximum_peak: budget.peak_allocation_bytes, external };
    budget.peak_allocation_bytes = available;
    work.peak_allocation_bytes = 0;
    Ok(Some(prefix))
}

pub(super) fn finish_custody_leaf(
    accounting: &mut KernelAccounting<'_>, custody: Option<&super::ProofDataCustody>, prefix: Option<CustodyPrefix>,
) -> Result<(), crate::FsError> {
    let Some(prefix) = prefix else { return Ok(()); };
    let Some((allocations, work, budget)) = standalone_parts(accounting) else { unreachable!("standalone proof prefix"); };
    budget.peak_allocation_bytes = prefix.maximum_peak;
    let peak = work.peak_allocation_bytes.max(allocations.live_bytes());
    work.peak_allocation_bytes = prefix.before.peak_allocation_bytes.max(peak.checked_add(prefix.external).ok_or(WorkError::Overflow)?);
    let completed = WorkCounters {
        allocation_operations: work.allocation_operations.checked_sub(prefix.before.allocation_operations).ok_or(WorkError::Overflow)?,
        items_examined: work.items_examined.checked_sub(prefix.before.items_examined).ok_or(WorkError::Overflow)?,
        bytes_copied: work.bytes_copied.checked_sub(prefix.before.bytes_copied).ok_or(WorkError::Overflow)?,
        peak_allocation_bytes: peak,
        ..WorkCounters::default()
    };
    custody.ok_or(WorkError::Overflow)?.record_completed_leaf_work(completed)?;
    work.verify(*budget)?;
    Ok(())
}


fn query_error(error: Error) -> crate::FsError {
    match error {
        #[cfg(not(target_arch = "wasm32"))]
        Error::Storage(ObjectStoreError::Native(error)) => crate::FsError::NativeKernel(error),
        error => crate::FsError::Directory(super::list::map_error(error)),
    }
}

pub(super) fn project_failure(error: crate::FsError) -> Error {
    use super::list::DirectoryReadError as D;
    match error {
        crate::FsError::Directory(error) => match error {
            D::Cancelled => Error::Cancelled, D::ZeroLimit => Error::ZeroLimit,
            D::LimitOverflow => Error::LimitOverflow, D::WrongRootKind => Error::WrongRootKind,
            D::InvalidLimits => Error::InvalidLimits, D::HeightExceeded => Error::HeightExceeded,
            D::Cycle => Error::CycleOrAlias, D::ChildBoundsMismatch => Error::ChildBoundsMismatch,
            D::TraversalState => Error::TraversalState, D::AllocationFailed => Error::AllocationFailed,
            D::Storage(error) => Error::Storage(error), D::Decode(error) => Error::Decode(error),
            D::Work(error) => Error::Work(error),
        },
        crate::FsError::Cancelled(_) => Error::Cancelled,
        crate::FsError::Work(error) => Error::Work(error),
        crate::FsError::Decode(error) => Error::Decode(error),
        crate::FsError::Object(error) => Error::Storage(error),
        crate::FsError::PendingMutationAllocationFailed => Error::AllocationFailed,
        #[cfg(not(target_arch = "wasm32"))]
        crate::FsError::NativeKernel(error) => Error::Storage(ObjectStoreError::Native(error)),
        _ => unreachable!("pagination leaves return only typed directory, object, decode, cancellation and work errors"),
    }
}

#[cfg(test)]
fn upper_bound_values<F: Format>(values: &[F::Value], cursor: &F::Key) -> (usize, u64) {
    bound_values::<F>(values, cursor, false)
}

#[cfg(test)]
fn bound_values<F: Format>(values: &[F::Value], cursor: &F::Key, inclusive: bool) -> (usize, u64) {
    counted_partition_point(values, |value| {
        let key = F::key(value);
        key < cursor || (!inclusive && key == cursor)
    })
}

#[cfg(test)]
fn upper_bound_children<K: Ord>(children: &[Child<K>], cursor: &K) -> (usize, u64) {
    let (after, comparisons) = counted_partition_point(children, |child| child.first <= *cursor);
    (after.saturating_sub(1), comparisons)
}

#[cfg(test)]
fn charge_items(work: &mut WorkCounters, count: u64, budget: WorkBudget) -> Result<(), Failure> {
    work.charge_items(count, &budget)
        .map_err(|error| failed(error.into(), *work))
}

#[cfg(test)]
fn map_allocation(error: AllocationError) -> Error {
    match error {
        AllocationError::Work(error) => error.into(),
        _ => Error::AllocationFailed,
    }
}

#[cfg(test)]
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
