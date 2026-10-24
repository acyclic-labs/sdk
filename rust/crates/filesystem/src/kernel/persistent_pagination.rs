//! Shared bounded cursor pagination with field-owned original native custody.

use super::allocation::{
    AllocationLedger, KernelAccounting, KernelOwnedAllocation, VisitedObjectSet,
};
use super::list::{DirectoryReadError as Error, DirectoryReadFailure as Failure};
use super::persistent_btree::entries::Entries;
use super::persistent_btree::{Child, Format, children_within, leaf_within};
use super::persistent_io;
use super::persistent_point::{Owned, retire_allocation, split_allocation};
use super::{CanonicalDecodeError, DecodeLimits};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::foundation::usize_to_u64;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{ObjectId, ObjectReadRequest};
use std::mem::size_of;
use std::ops::Bound;

mod accept;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Receipt<V> {
    pub(crate) values: Vec<V>,
    pub(crate) has_more: bool,
    pub(crate) next_request: Option<ObjectReadRequest>,
    pub(crate) work: WorkCounters,
}

struct Pending<K> {
    page: ObjectId,
    lower: Option<Owned<K>>,
    upper: Option<Owned<K>>,
    height: u16,
}

impl<K> Pending<K> {
    fn retire(self, accounting: &mut KernelAccounting<'_>) -> Result<(), crate::FsError> {
        if let Some(lower) = self.lower {
            lower.retire(accounting)?;
        }
        if let Some(upper) = self.upper {
            upper.retire(accounting)?;
        }
        Ok(())
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "Store, bounds, accounting and cancellation are independent inputs to this operation."
)]
pub(crate) async fn paginate<S, F>(
    store: &S,
    query: (ObjectId, Bound<&F::Key>, u32),
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    data_custody: Option<&super::ProofDataCustody>,
) -> Result<Receipt<F::Value>, Failure>
where
    S: AsyncObjectStore,
    F: Format,
{
    let (root, bound, maximum_values) = query;
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    let result = paginate_with_accounting::<S, F>(
        store,
        (root, bound, maximum_values),
        limits,
        &mut accounting,
        cancellation,
        data_custody,
    )
    .await;
    result
        .map(|(receipt, _)| receipt)
        .map_err(|error| failed(project_failure(error), accounting.spent()))
}

#[allow(
    clippy::too_many_arguments,
    reason = "Store, bounds, accounting and cancellation are independent inputs to this operation."
)]
pub(crate) async fn paginate_with_accounting<S, F>(
    store: &S,
    query: (ObjectId, Bound<&F::Key>, u32),
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
    data_custody: Option<&super::ProofDataCustody>,
) -> Result<(Receipt<F::Value>, KernelOwnedAllocation), crate::FsError>
where
    S: AsyncObjectStore,
    F: Format,
{
    let (root, bound, maximum_values) = query;
    accounting.check()?;
    let mut machine = Machine::<F>::new_with_accounting(
        root,
        bound,
        maximum_values,
        limits,
        accounting,
        data_custody,
    )
    .map_err(|failure| query_error(failure.error))?;
    let result: Result<(), Failure> = async {
        while machine.values.len() < machine.target && !machine.pending.value.is_empty() {
            cancellation
                .check()
                .map_err(|error| machine.failure(error.into()))?;
            let mut pending = Some(machine.pop_pending()?);
            let page = pending
                .as_ref()
                .ok_or_else(|| failed(Error::TraversalState, machine.accounting.spent()))?
                .page;
            let step = async {
                machine.visit(page)?;
                let (decoded, allocation) = persistent_io::read_page_with_accounting::<S, F>(
                    store,
                    page,
                    limits,
                    machine.accounting,
                    cancellation,
                    data_custody,
                )
                .await
                .map_err(|error| machine.failure(error))?;
                let pending = pending
                    .take()
                    .ok_or_else(|| failed(Error::TraversalState, machine.accounting.spent()))?;
                machine.accept(decoded, allocation, pending)
            }
            .await;
            if let Some(pending) = pending {
                pending
                    .retire(machine.accounting)
                    .map_err(|error| machine.failure(error))?;
            }
            step?;
        }
        Ok(())
    }
    .await;
    match result {
        Ok(()) => machine
            .finish()
            .map_err(|failure| query_error(failure.error)),
        Err(failure) => {
            machine.retire().map_err(query_error)?;
            Err(query_error(failure.error))
        }
    }
}

struct Machine<'a, 'accounting, F: Format> {
    bound: Bound<&'a F::Key>,
    target: usize,
    maximum_traversal_pages: usize,
    limits: DecodeLimits,
    pending: Owned<Vec<Pending<F::Key>>>,
    visited: Option<Owned<VisitedObjectSet>>,
    values: Vec<F::Value>,
    values_allocation: KernelOwnedAllocation,
    values_container_bytes: u64,
    values_nested_bytes: u64,
    successor_page: Option<ObjectId>,
    data_custody: Option<&'a super::ProofDataCustody>,
    accounting: &'a mut KernelAccounting<'accounting>,
}

impl<'a, 'accounting, F: Format> Machine<'a, 'accounting, F> {
    #[allow(
        clippy::too_many_lines,
        reason = "The constructor admits and initializes its bounded seek state before exposing it."
    )]
    fn new_with_accounting(
        root: ObjectId,
        bound: Bound<&'a F::Key>,
        maximum_values: u32,
        limits: DecodeLimits,
        accounting: &'a mut KernelAccounting<'accounting>,
        data_custody: Option<&'a super::ProofDataCustody>,
    ) -> Result<Self, Failure> {
        let fail = |error| failed(error, accounting.spent());
        if root.kind != F::kind() {
            return Err(fail(Error::WrongRootKind));
        }
        if maximum_values == 0 {
            return Err(fail(Error::ZeroLimit));
        }
        if !limits.page_limits_valid(1) {
            return Err(fail(Error::InvalidLimits));
        }
        let cursor = match bound {
            Bound::Included(key) | Bound::Excluded(key) => Some(key),
            Bound::Unbounded => None,
        };
        if let Some(observed) = cursor
            .map(F::key_nested_bytes)
            .filter(|&bytes| bytes > u64::from(limits.maximum_name_bytes))
        {
            return Err(fail(Error::Decode(CanonicalDecodeError::FieldTooLarge {
                observed: u32::try_from(observed).unwrap_or(u32::MAX),
                maximum: limits.maximum_name_bytes,
            })));
        }
        let maximum_values =
            usize::try_from(maximum_values).map_err(|_| fail(Error::LimitOverflow))?;
        let target = maximum_values
            .checked_add(1)
            .ok_or_else(|| fail(Error::LimitOverflow))?;
        let configured_maximum = usize::try_from(limits.maximum_visited_pages)
            .map_err(|_| fail(Error::InvalidLimits))?;
        // One cursor-exhausted subtree plus each requested value/witness frontier.
        let maximum_traversal_pages = target
            .checked_add(1)
            .and_then(|value| value.checked_mul(usize::from(limits.maximum_page_height)))
            .ok_or_else(|| fail(Error::LimitOverflow))?
            .min(configured_maximum);
        accounting
            .admit(WorkCounters {
                items_returned: usize_to_u64(maximum_values),
                ..WorkCounters::default()
            })
            .map_err(|error| failed(project_failure(error), accounting.spent()))?;
        let prefix = begin_custody_leaf(accounting, data_custody)
            .map_err(|error| failed(project_failure(error), accounting.spent()))?;
        let values_result = Owned::<Vec<F::Value>>::allocate(target, accounting);
        let completed = finish_custody_leaf(accounting, data_custody, prefix);
        let values =
            values_result.map_err(|error| failed(project_failure(error), accounting.spent()))?;
        if let Err(error) = completed {
            values
                .retire(accounting)
                .map_err(|error| failed(project_failure(error), accounting.spent()))?;
            return Err(failed(project_failure(error), accounting.spent()));
        }
        let prefix = match begin_custody_leaf(accounting, data_custody) {
            Ok(prefix) => prefix,
            Err(error) => {
                values
                    .retire(accounting)
                    .map_err(|error| failed(project_failure(error), accounting.spent()))?;
                return Err(failed(project_failure(error), accounting.spent()));
            }
        };
        let visited_result =
            VisitedObjectSet::new_with_accounting(maximum_traversal_pages, accounting);
        let completed = finish_custody_leaf(accounting, data_custody, prefix);
        let visited = match visited_result {
            Ok(result) => result,
            Err(error) => {
                values
                    .retire(accounting)
                    .map_err(|error| failed(project_failure(error), accounting.spent()))?;
                return Err(failed(project_failure(error), accounting.spent()));
            }
        };
        if let Err(error) = completed {
            visited
                .retire(accounting)
                .map_err(|error| failed(project_failure(error), accounting.spent()))?;
            values
                .retire(accounting)
                .map_err(|error| failed(project_failure(error), accounting.spent()))?;
            return Err(failed(project_failure(error), accounting.spent()));
        }
        let pending_result = accounting
            .claim_owned(0, 0)
            .map(|allocation| (Vec::new(), allocation));
        let (pending, pending_allocation) = match pending_result {
            Ok(result) => result,
            Err(error) => {
                visited
                    .retire(accounting)
                    .map_err(|error| failed(project_failure(error), accounting.spent()))?;
                values
                    .retire(accounting)
                    .map_err(|error| failed(project_failure(error), accounting.spent()))?;
                return Err(failed(project_failure(error), accounting.spent()));
            }
        };
        let mut machine = Self {
            bound,
            target,
            maximum_traversal_pages,
            limits,
            pending: Owned::new(pending, pending_allocation, 0),
            visited: Some(visited),
            values: values.value,
            values_allocation: values.allocation,
            values_container_bytes: values.bytes,
            values_nested_bytes: 0,
            successor_page: None,
            data_custody,
            accounting,
        };
        if let Err(failure) = machine.push_pending(Pending {
            page: root,
            lower: None,
            upper: None,
            height: 1,
        }) {
            machine
                .retire()
                .map_err(|error| failed(error, *failure.work))?;
            return Err(failure);
        }
        Ok(machine)
    }

    fn failure(&self, error: crate::FsError) -> Failure {
        failed(project_failure(error), self.accounting.spent())
    }

    fn push_pending(&mut self, pending: Pending<F::Key>) -> Result<(), Failure> {
        let prefix = begin_custody_leaf(self.accounting, self.data_custody);
        let result = match prefix {
            Ok(prefix) => {
                let result = self
                    .pending
                    .ensure_for_push(self.maximum_traversal_pages, self.accounting);
                let completed = finish_custody_leaf(self.accounting, self.data_custody, prefix);
                result.and(completed)
            }
            Err(error) => Err(error),
        };
        if let Err(error) = result {
            pending
                .retire(self.accounting)
                .map_err(|error| self.failure(error))?;
            return Err(self.failure(error));
        }
        let copied = WorkCounters {
            bytes_copied: usize_to_u64(size_of::<Pending<F::Key>>()),
            ..WorkCounters::default()
        };
        let prefix = match begin_custody_leaf(self.accounting, self.data_custody) {
            Ok(prefix) => prefix,
            Err(error) => {
                pending
                    .retire(self.accounting)
                    .map_err(|error| self.failure(error))?;
                return Err(self.failure(error));
            }
        };
        if let Err(error) = self.accounting.admit(copied) {
            let completed = finish_custody_leaf(self.accounting, self.data_custody, prefix);
            pending
                .retire(self.accounting)
                .map_err(|error| self.failure(error))?;
            completed.map_err(|error| self.failure(error))?;
            return Err(self.failure(error));
        }
        self.pending.value.push(pending);
        let recorded = self.accounting.record_completed(copied);
        let completed = finish_custody_leaf(self.accounting, self.data_custody, prefix);
        recorded.and(completed).map_err(|error| self.failure(error))
    }

    fn pop_pending(&mut self) -> Result<Pending<F::Key>, Failure> {
        let pending = self
            .pending
            .value
            .pop()
            .ok_or_else(|| failed(Error::TraversalState, self.accounting.spent()))?;
        if pending.height > self.limits.maximum_page_height {
            pending
                .retire(self.accounting)
                .map_err(|error| self.failure(error))?;
            return Err(failed(Error::HeightExceeded, self.accounting.spent()));
        }
        Ok(pending)
    }

    fn visit(&mut self, page: ObjectId) -> Result<(), Failure> {
        let prefix = begin_custody_leaf(self.accounting, self.data_custody)
            .map_err(|error| self.failure(error))?;
        let visited = self
            .visited
            .as_mut()
            .ok_or_else(|| failed(Error::TraversalState, self.accounting.spent()))?;
        let result = VisitedObjectSet::insert_with_accounting(visited, page, self.accounting);
        let completed = finish_custody_leaf(self.accounting, self.data_custody, prefix);
        let inserted = result.map_err(|error| self.failure(error))?;
        completed.map_err(|error| self.failure(error))?;
        if !inserted.inserted {
            return Err(failed(Error::Cycle, self.accounting.spent()));
        }
        Ok(())
    }

    fn completed(&mut self, delta: WorkCounters) -> Result<(), Failure> {
        let result = self.accounting.record_completed(delta);
        result.map_err(|error| self.failure(error))
    }

    fn examine_value(&mut self, value: &F::Value) -> Result<u64, Failure> {
        let examined = WorkCounters {
            items_examined: 1,
            ..WorkCounters::default()
        };
        self.accounting
            .admit(examined)
            .map_err(|error| self.failure(error))?;
        let bytes = F::value_owned_bytes(value);
        self.completed(examined)?;
        Ok(bytes)
    }

    fn leaf_bounds<'b>(
        &mut self,
        values: impl Into<Entries<'b, F::Value>>,
        pending: &Pending<F::Key>,
    ) -> Result<bool, Failure> {
        let values = values.into();
        let examined = WorkCounters {
            items_examined: if values.is_empty() { 0 } else { 2 },
            ..WorkCounters::default()
        };
        self.accounting
            .admit(examined)
            .map_err(|error| self.failure(error))?;
        let within = leaf_within::<F>(
            values,
            pending.lower.as_ref().map(|key| &key.value),
            pending.upper.as_ref().map(|key| &key.value),
        );
        self.completed(examined)?;
        Ok(within)
    }

    fn child_bounds<'b>(
        &mut self,
        children: impl Into<Entries<'b, Child<F::Key>>>,
        pending: &Pending<F::Key>,
    ) -> Result<bool, Failure> {
        let children = children.into();
        let examined = WorkCounters {
            items_examined: if children.is_empty() { 0 } else { 2 },
            ..WorkCounters::default()
        };
        self.accounting
            .admit(examined)
            .map_err(|error| self.failure(error))?;
        let within = children_within(
            children,
            pending.lower.as_ref().map(|key| &key.value),
            pending.upper.as_ref().map(|key| &key.value),
        );
        self.completed(examined)?;
        Ok(within)
    }

    fn value_start<'b>(
        &mut self,
        values: impl Into<Entries<'b, F::Value>>,
    ) -> Result<usize, Failure> {
        let values = values.into();
        let bound = self.bound;
        if matches!(bound, Bound::Unbounded) {
            return Ok(0);
        }
        let result = values.partition(self.accounting, |value| {
            let key = F::key(value);
            match bound {
                Bound::Included(cursor) => key < cursor,
                Bound::Excluded(cursor) => key <= cursor,
                Bound::Unbounded => false,
            }
        });
        result.map_err(|error| self.failure(error))
    }

    fn child_start<'b>(
        &mut self,
        children: impl Into<Entries<'b, Child<F::Key>>>,
    ) -> Result<usize, Failure> {
        let children = children.into();
        let cursor = match self.bound {
            Bound::Included(cursor) | Bound::Excluded(cursor) => cursor,
            Bound::Unbounded => return Ok(0),
        };
        let result = children.partition(self.accounting, |child| child.first <= *cursor);
        result
            .map(|index| index.saturating_sub(1))
            .map_err(|error| self.failure(error))
    }

    fn clone_key(&mut self, key: &F::Key) -> Result<Owned<F::Key>, Failure> {
        let prefix = begin_custody_leaf(self.accounting, self.data_custody)
            .map_err(|error| self.failure(error))?;
        let result = persistent_io::clone_key::<F>(
            key,
            self.limits.maximum_name_bytes,
            self.accounting,
            None,
            0,
        );
        let completed = finish_custody_leaf(self.accounting, self.data_custody, prefix);
        let (value, allocation) = result.map_err(|error| self.failure(error))?;
        let bytes = F::key_owned_bytes(&value);
        let owned = Owned::new(value, allocation, bytes);
        if let Err(error) = completed {
            owned
                .retire(self.accounting)
                .map_err(|error| self.failure(error))?;
            return Err(self.failure(error));
        }
        Ok(owned)
    }

    fn push_value(&mut self, owned: Owned<F::Value>) -> Result<(), Failure> {
        let prefix = match begin_custody_leaf(self.accounting, self.data_custody) {
            Ok(prefix) => prefix,
            Err(error) => {
                owned
                    .retire(self.accounting)
                    .map_err(|error| self.failure(error))?;
                return Err(self.failure(error));
            }
        };
        let copied = WorkCounters {
            bytes_copied: usize_to_u64(size_of::<F::Value>()),
            ..WorkCounters::default()
        };
        let admitted = self.accounting.admit(copied);
        let nested = match (admitted, self.values_nested_bytes.checked_add(owned.bytes)) {
            (Ok(()), Some(nested)) => nested,
            (admitted, _) => {
                let completed = finish_custody_leaf(self.accounting, self.data_custody, prefix);
                owned
                    .retire(self.accounting)
                    .map_err(|error| self.failure(error))?;
                completed.map_err(|error| self.failure(error))?;
                admitted.map_err(|error| self.failure(error))?;
                return Err(failed(Error::AllocationFailed, self.accounting.spent()));
            }
        };
        let Owned {
            value,
            mut allocation,
            bytes,
        } = owned;
        if let Err(error) = self.values_allocation.absorb(&mut allocation) {
            let completed = finish_custody_leaf(self.accounting, self.data_custody, prefix);
            Owned::new(value, allocation, bytes)
                .retire(self.accounting)
                .map_err(|error| self.failure(error))?;
            completed.map_err(|error| self.failure(error))?;
            return Err(self.failure(error));
        }
        self.values.push(value);
        self.values_nested_bytes = nested;
        let recorded = self.accounting.record_completed(copied);
        let completed = finish_custody_leaf(self.accounting, self.data_custody, prefix);
        recorded.and(completed).map_err(|error| self.failure(error))
    }

    fn retire_traversal(&mut self) -> Result<(), crate::FsError> {
        for pending in self.pending.value.drain(..) {
            pending.retire(self.accounting)?;
        }
        let empty_allocation = split_allocation(&mut self.pending.allocation, 0)?;
        let pending = std::mem::replace(
            &mut self.pending,
            Owned::new(Vec::new(), empty_allocation, 0),
        );
        pending.retire(self.accounting)?;
        if let Some(visited) = self.visited.take() {
            visited.retire(self.accounting)?;
        }
        Ok(())
    }

    fn retire(mut self) -> Result<(), Error> {
        self.retire_traversal().map_err(project_failure)?;
        drop(self.values);
        retire_allocation(
            self.values_allocation,
            self.values_container_bytes
                .checked_add(self.values_nested_bytes)
                .ok_or(Error::TraversalState)?,
            self.accounting,
        )
        .map_err(project_failure)
    }

    fn finish_state(&mut self) -> Result<(bool, Option<ObjectReadRequest>), Failure> {
        let has_more = self.values.len() >= self.target;
        let next_request = has_more
            .then(|| {
                self.pending
                    .value
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
                .ok_or_else(|| failed(Error::TraversalState, self.accounting.spent()))?;
            let bytes = F::value_owned_bytes(&removed);
            let tag = split_allocation(&mut self.values_allocation, bytes)
                .map_err(|error| self.failure(error))?;
            drop(removed);
            retire_allocation(tag, bytes, self.accounting).map_err(|error| self.failure(error))?;
            self.values_nested_bytes = self
                .values_nested_bytes
                .checked_sub(bytes)
                .ok_or_else(|| failed(Error::TraversalState, self.accounting.spent()))?;
        }
        self.completed(WorkCounters {
            items_returned: usize_to_u64(self.values.len()),
            ..WorkCounters::default()
        })?;
        self.retire_traversal()
            .map_err(|error| self.failure(error))?;
        Ok((has_more, next_request))
    }

    fn finish(mut self) -> Result<(Receipt<F::Value>, KernelOwnedAllocation), Failure> {
        let (has_more, next_request) = match self.finish_state() {
            Ok(result) => result,
            Err(failure) => {
                self.retire()
                    .map_err(|error| failed(error, *failure.work))?;
                return Err(failure);
            }
        };
        let receipt = Receipt {
            values: self.values,
            has_more,
            next_request,
            work: self.accounting.spent(),
        };
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
) -> Option<(
    &'a mut AllocationLedger,
    &'a mut WorkCounters,
    &'a mut WorkBudget,
)> {
    match accounting {
        KernelAccounting::Standalone {
            allocations,
            work,
            budget,
            ..
        } => Some((allocations, work, budget)),
        KernelAccounting::BorrowedStandalone {
            allocations,
            work,
            budget,
            ..
        } => Some((allocations, work, budget)),
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { .. } => None,
    }
}

// The standalone proof projection retains its cache-residency adjustment.
// Original native leaves already charge their own ledger; they never replay work.
pub(super) fn begin_custody_leaf(
    accounting: &mut KernelAccounting<'_>,
    custody: Option<&super::ProofDataCustody>,
) -> Result<Option<CustodyPrefix>, crate::FsError> {
    let Some(owner) = custody else {
        return Ok(None);
    };
    let Some((_, work, budget)) = standalone_parts(accounting) else {
        return Ok(None);
    };
    let external = owner.operation_cache_bytes();
    let available =
        budget
            .peak_allocation_bytes
            .checked_sub(external)
            .ok_or(WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes",
                observed: external,
                maximum: budget.peak_allocation_bytes,
            })?;
    let prefix = CustodyPrefix {
        before: *work,
        maximum_peak: budget.peak_allocation_bytes,
        external,
    };
    budget.peak_allocation_bytes = available;
    work.peak_allocation_bytes = 0;
    Ok(Some(prefix))
}

pub(super) fn finish_custody_leaf(
    accounting: &mut KernelAccounting<'_>,
    custody: Option<&super::ProofDataCustody>,
    prefix: Option<CustodyPrefix>,
) -> Result<(), crate::FsError> {
    let Some(prefix) = prefix else {
        return Ok(());
    };
    let Some((allocations, work, budget)) = standalone_parts(accounting) else {
        unreachable!("standalone proof prefix");
    };
    budget.peak_allocation_bytes = prefix.maximum_peak;
    let peak = work.peak_allocation_bytes.max(allocations.live_bytes());
    work.peak_allocation_bytes = prefix.before.peak_allocation_bytes.max(
        peak.checked_add(prefix.external)
            .ok_or(WorkError::Overflow)?,
    );
    let completed = WorkCounters {
        allocation_operations: work
            .allocation_operations
            .checked_sub(prefix.before.allocation_operations)
            .ok_or(WorkError::Overflow)?,
        items_examined: work
            .items_examined
            .checked_sub(prefix.before.items_examined)
            .ok_or(WorkError::Overflow)?,
        bytes_copied: work
            .bytes_copied
            .checked_sub(prefix.before.bytes_copied)
            .ok_or(WorkError::Overflow)?,
        peak_allocation_bytes: peak,
        ..WorkCounters::default()
    };
    custody
        .ok_or(WorkError::Overflow)?
        .record_completed_leaf_work(completed)?;
    work.verify(*budget)?;
    Ok(())
}

fn query_error(error: Error) -> crate::FsError {
    match error {
        #[cfg(not(target_arch = "wasm32"))]
        Error::Storage(crate::storage::ObjectStoreError::Native(error)) => {
            crate::FsError::NativeKernel(error)
        }
        error => crate::FsError::Directory(error),
    }
}

pub(super) fn project_failure(error: crate::FsError) -> Error {
    match error {
        crate::FsError::Directory(error) => error,
        crate::FsError::Cancelled(_) => Error::Cancelled,
        crate::FsError::Work(error) => Error::Work(error),
        crate::FsError::Decode(error) => Error::Decode(error),
        crate::FsError::Object(error) => Error::Storage(error),
        crate::FsError::PendingMutationAllocationFailed => Error::AllocationFailed,
        #[cfg(not(target_arch = "wasm32"))]
        crate::FsError::NativeKernel(error) => {
            Error::Storage(crate::storage::ObjectStoreError::Native(error))
        }
        _ => unreachable!(
            "pagination leaves return only typed directory, object, decode, cancellation and work errors"
        ),
    }
}

fn failed(error: Error, work: WorkCounters) -> Failure {
    OperationFailure::new(error, work)
}
