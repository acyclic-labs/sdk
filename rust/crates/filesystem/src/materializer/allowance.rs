use super::*;
use crate::sync::{Mutex, MutexGuard};
use crate::{DemandError, WorkBudget, WorkCounters, WorkError};
use std::sync::{Arc, Weak};

#[cfg(all(test, not(acyclic_fs_loom)))]
std::thread_local! {
    static RESOURCE_LOCKS: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

/// Counts this thread's resource-allowance mutex acquisitions in `operation`.
#[cfg(all(test, not(acyclic_fs_loom)))]
pub(crate) fn count_resource_locks<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    let previous = RESOURCE_LOCKS.with(|count| count.replace(Some(0)));
    let result = operation();
    let count = RESOURCE_LOCKS.with(|count| count.replace(previous));
    (result, count.unwrap_or(0))
}

#[cfg(all(test, acyclic_fs_loom))]
loom::thread_local! {
    static OMIT_CHILD_DEBIT: std::cell::Cell<bool> = std::cell::Cell::new(false);
}

/// Negative control: completed work skips the scope it was recorded on.
#[cfg(all(test, acyclic_fs_loom))]
pub(crate) fn omit_child_debit(omit: bool) {
    OMIT_CHILD_DEBIT.with(|flag| flag.set(omit));
}

#[derive(Debug)]
pub(crate) struct NativeResourceAllowance {
    pub(super) budget: WorkBudget,
    // After the original leaf; nested allowances lock ancestors first. Release
    // every allowance guard before acquiring an original leaf.
    pub(super) state: Mutex<NativeResourceState>,
    pub(super) self_ref: Weak<Self>,
    pub(super) parent: Option<Arc<Self>>,
    pub(super) metadata: NativeAllocation,
}
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct NativeResourceState {
    work: WorkCounters,
    pending: WorkCounters,
    allocations: crate::kernel::allocation::AllocationLedger,
    poisoned: bool,
}
impl NativeResourceState {
    fn check(&self) -> Result<(), DemandError> {
        if self.poisoned {
            Err(DemandError::OriginalWorkPoisoned)
        } else {
            Ok(())
        }
    }
    fn verify(&self, budget: WorkBudget) -> Result<(), DemandError> {
        self.check()?;
        Ok(self.work.verify(self.pending.remaining(budget)?)?)
    }
    fn allocation(&self, mut delta: WorkCounters, bytes: u64) -> Result<WorkCounters, DemandError> {
        delta.peak_allocation_bytes = self
            .allocations
            .live_bytes()
            .checked_add(bytes)
            .ok_or(WorkError::Overflow)?;
        Ok(delta)
    }
    fn completed(&mut self, delta: WorkCounters, bytes: u64) -> Result<(), DemandError> {
        let delta = self.allocation(delta, bytes)?;
        self.work.try_add_assign(&delta)?;
        self.allocations
            .record_owned_bytes(bytes, &mut self.work)
            .map_err(|error| error.into_work_or(DemandError::OriginalWorkPoisoned))
    }
}
#[derive(Clone, Copy)]
pub(crate) enum NativeResourceCredit {
    Items,
    Copy,
    ReadOperations,
    WriteOperations,
    DurabilityOperations,
    ReadBytes,
    #[cfg(any(test, feature = "distributed"))]
    AuthorityReadBytes,
    WriteBytes,
    AllocationOperations,
}
impl NativeResourceCredit {
    pub(crate) fn counter(self, work: &mut WorkCounters) -> &mut u64 {
        match self {
            Self::Items => &mut work.items_examined,
            Self::Copy => &mut work.bytes_copied,
            Self::ReadOperations => &mut work.backend_read_operations,
            Self::WriteOperations => &mut work.backend_write_operations,
            Self::DurabilityOperations => &mut work.durability_operations,
            Self::ReadBytes => &mut work.source_bytes_read,
            #[cfg(any(test, feature = "distributed"))]
            Self::AuthorityReadBytes => &mut work.authority_bytes_read,
            Self::WriteBytes => &mut work.output_bytes,
            Self::AllocationOperations => &mut work.allocation_operations,
        }
    }
    pub(crate) fn delta(self, value: u64) -> WorkCounters {
        let mut work = WorkCounters::UNCHARGED;
        *self.counter(&mut work) = value;
        work
    }
}
impl NativeResourceAllowance {
    pub(crate) fn admitted_item_limit(&self) -> u64 {
        self.parent
            .as_ref()
            .map_or(self.budget.items_examined, |parent| {
                self.budget.items_examined.min(parent.admitted_item_limit())
            })
    }
    fn lock(&self) -> MutexGuard<'_, NativeResourceState> {
        #[cfg(all(test, not(acyclic_fs_loom)))]
        RESOURCE_LOCKS.with(|count| count.set(count.get().map(|count| count + 1)));
        self.state.lock().unwrap_or_else(|error| {
            let mut state = error.into_inner();
            state.poisoned = true;
            state
        })
    }
    pub(super) fn contains_scope(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
            || self
                .parent
                .as_ref()
                .is_some_and(|parent| parent.contains_scope(other))
    }
    fn check(
        &self,
        state: &NativeResourceState,
        delta: WorkCounters,
        bytes: u64,
    ) -> Result<(), DemandError> {
        #[cfg(all(test, not(acyclic_fs_loom)))]
        if super::views::tests::skip_ancestor_check(self) {
            return Ok(());
        }
        state.check()?;
        let bytes = bytes
            .checked_add(state.allocations.reserved_bytes())
            .ok_or(WorkError::Overflow)?;
        Ok(state.work.admit(
            &state.allocation(delta, bytes)?,
            &state.pending.remaining(self.budget)?,
        )?)
    }
    // Keep ancestor admissions locked until the original operation commits.
    fn transaction(
        &self,
        excluded: Option<&Self>,
        change: &dyn Fn(&Self, &mut NativeResourceState) -> Result<(), DemandError>,
        operation: &mut dyn FnMut() -> Result<(), DemandError>,
    ) -> Result<(), DemandError> {
        if excluded.is_some_and(|scope| scope.contains_scope(self)) {
            return operation();
        }
        let mut apply = || {
            let mut state = self.lock();
            let mut next = *state;
            change(self, &mut next)?;
            operation()?;
            *state = next;
            Ok(())
        };
        match &self.parent {
            Some(parent) => parent.transaction(excluded, change, &mut apply),
            None => apply(),
        }
    }
    // Completed work and retirement must reach every scope even if a parent fails.
    fn visit(
        &self,
        excluded: Option<&Self>,
        operation: &dyn Fn(
            &Self,
            &mut NativeResourceState,
        ) -> Result<Result<(), DemandError>, DemandError>,
    ) -> Result<(), DemandError> {
        if excluded.is_some_and(|scope| scope.contains_scope(self)) {
            return Ok(());
        }
        let parent = self
            .parent
            .as_ref()
            .map_or(Ok(()), |parent| parent.visit(excluded, operation));
        let local = operation(self, &mut self.lock())?;
        parent.and(local)
    }
    pub(crate) fn admit(&self, delta: WorkCounters, bytes: u64) -> Result<(), DemandError> {
        if let Some(parent) = &self.parent {
            parent.admit(delta, bytes)?;
        }
        self.check(&self.lock(), delta, bytes)
    }
    pub(crate) fn admission(
        &self,
        delta: WorkCounters,
        bytes: u64,
        operation: &mut dyn FnMut() -> Result<(), DemandError>,
    ) -> Result<(), DemandError> {
        self.transaction(
            None,
            &|scope, state| {
                scope.check(state, delta, bytes)?;
                #[cfg(all(test, acyclic_fs_loom))]
                if delta.bytes_copied != 0
                    && std::ptr::eq(scope, self)
                    && OMIT_CHILD_DEBIT.with(std::cell::Cell::get)
                {
                    return Ok(());
                }
                state.completed(delta, bytes)
            },
            operation,
        )
    }
    pub(crate) fn reserve(
        &self,
        maximum: WorkCounters,
        bytes: u64,
        excluded: Option<&Self>,
        operation: &mut dyn FnMut() -> Result<(), DemandError>,
    ) -> Result<(), DemandError> {
        self.transaction(
            excluded,
            &|scope, state| {
                state.check()?;
                if maximum.allocation_operations != 0 {
                    state
                        .allocations
                        .reserve_allocation(
                            bytes,
                            &state.work,
                            state.pending.remaining(scope.budget)?,
                        )
                        .map_err(|error| error.into_work_or(DemandError::OriginalWorkPoisoned))?;
                } else {
                    state
                        .work
                        .admit(&maximum, &state.pending.remaining(scope.budget)?)?;
                }
                state.pending = state.pending.checked_add(maximum)?;
                Ok(())
            },
            operation,
        )
    }
    pub(super) fn start_reserved_work(
        &self,
        actual: WorkCounters,
        operation: &mut dyn FnMut() -> Result<(), DemandError>,
    ) -> Result<(), DemandError> {
        self.transaction(
            None,
            &|scope, state| {
                state.check()?;
                state.pending = actual.remaining(state.pending)?;
                state.work = state.work.checked_add(actual)?;
                state.verify(scope.budget)
            },
            operation,
        )
    }
    pub(crate) fn settle_allocation(
        &self,
        bytes: u64,
        record: bool,
        operation: &mut dyn FnMut() -> Result<(), DemandError>,
    ) -> Result<(), DemandError> {
        self.transaction(
            None,
            &|_, state| {
                if record {
                    state.check()?;
                }
                let delta = NativeResourceCredit::AllocationOperations.delta(1);
                state.pending = delta.remaining(state.pending)?;
                if record {
                    state
                        .allocations
                        .record_reserved_allocation(bytes, &mut state.work)
                        .map_err(|error| error.into_work_or(DemandError::OriginalWorkPoisoned))?;
                } else {
                    state
                        .allocations
                        .cancel_reserved_allocation(bytes)
                        .map_err(|error| error.into_work_or(DemandError::OriginalWorkPoisoned))?;
                }
                Ok(())
            },
            operation,
        )
    }
    pub(crate) fn completed(&self, delta: WorkCounters, bytes: u64) -> Result<(), DemandError> {
        self.visit(None, &|scope, state| {
            #[cfg(all(test, acyclic_fs_loom))]
            if std::ptr::eq(scope, self) && OMIT_CHILD_DEBIT.with(std::cell::Cell::get) {
                return Ok(Ok(()));
            }
            state.completed(delta, bytes)?;
            Ok(state.verify(scope.budget))
        })
    }
    #[cfg(test)]
    pub(crate) fn local_chain(&self) -> Vec<(WorkCounters, WorkCounters)> {
        let state = *self.lock();
        let mut chain = vec![(state.work, state.pending)];
        if let Some(parent) = &self.parent {
            chain.extend(parent.local_chain());
        }
        chain
    }
    pub(crate) fn retire(&self, bytes: u64) -> Result<(), DemandError> {
        self.visit(None, &|_, state| {
            state.allocations.release(bytes).map_err(|_| {
                state.poisoned = true;
                DemandError::OriginalWorkPoisoned
            })?;
            Ok(Ok(()))
        })
    }
    pub(crate) fn remaining(&self) -> Result<WorkBudget, DemandError> {
        let state = self.lock();
        state.check()?;
        let mut remaining = state
            .work
            .remaining(state.pending.remaining(self.budget)?)?;
        remaining.peak_allocation_bytes = remaining
            .peak_allocation_bytes
            .checked_sub(state.allocations.live_bytes())
            .and_then(|bytes| bytes.checked_sub(state.allocations.reserved_bytes()))
            .ok_or(WorkError::Overflow)?;
        drop(state);
        if let Some(parent) = &self.parent {
            remaining = remaining.intersect_budget(parent.remaining()?);
        }
        Ok(remaining)
    }
    pub(crate) fn refund(
        &self,
        unused: WorkCounters,
        excluded: Option<&Self>,
    ) -> Result<(), DemandError> {
        self.visit(excluded, &|_, state| {
            state.pending = unused.remaining(state.pending).inspect_err(|_| {
                state.poisoned = true;
            })?;
            Ok(Ok(()))
        })
    }
    pub(crate) fn complete(
        &self,
        maximum: WorkCounters,
        actual: WorkCounters,
        excluded: Option<&Self>,
    ) -> Result<(), DemandError> {
        self.visit(excluded, &|scope, state| {
            let pending = maximum.remaining(state.pending);
            state.work.try_add_assign(&actual)?;
            state.pending = pending.inspect_err(|_| {
                state.poisoned = true;
            })?;
            if actual.intersect_budget(maximum) != actual {
                return Err(DemandError::InvalidRequest);
            }
            Ok(state.verify(scope.budget))
        })
    }
    pub(super) fn with_original_leaf<T>(
        &self,
        operation: impl FnOnce(&mut crate::demand::DemandWork) -> T,
    ) -> T {
        self.metadata.work().state(|state| {
            state.scope(
                |work| &mut work.native_resource,
                Some(self.self_ref.clone()),
                operation,
            )
        })
    }
}
