use super::*;
use crate::{DemandError, WorkBudget, WorkCounters, WorkError};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

#[derive(Debug)]
pub(crate) struct NativeResourceAllowance {
    pub(super) budget: WorkBudget,
    pub(super) state: Mutex<NativeResourceState>,
    pub(super) self_ref: Weak<Self>,
    pub(super) parent: Option<Arc<Self>>,
    pub(super) _root_only_metadata_guard: NativeAllocation,
}
#[derive(Debug)]
pub(super) struct NativeResourceState {
    pub(super) work: WorkCounters,
    pub(super) pending: WorkCounters,
    pub(super) live: u64,
    pub(super) reserved_allocation_bytes: u64,
    pub(super) poisoned: bool,
}
#[derive(Clone, Copy)]
pub(crate) enum NativeResourceCredit {
    Items,
    ReturnedItems,
    Copy,
    ReadOperations,
    WriteOperations,
    DurabilityOperations,
    ReadBytes,
    WriteBytes,
    AllocationOperations,
}
impl NativeResourceCredit {
    pub(crate) fn counter(self, work: &mut WorkCounters) -> &mut u64 {
        match self {
            Self::Items => &mut work.items_examined,
            Self::ReturnedItems => &mut work.items_returned,
            Self::Copy => &mut work.bytes_copied,
            Self::ReadOperations => &mut work.backend_read_operations,
            Self::WriteOperations => &mut work.backend_write_operations,
            Self::DurabilityOperations => &mut work.durability_operations,
            Self::ReadBytes => &mut work.source_bytes_read,
            Self::WriteBytes => &mut work.output_bytes,
            Self::AllocationOperations => &mut work.allocation_operations,
        }
    }
    pub(crate) fn delta(self, amount: u64) -> WorkCounters {
        let mut delta = WorkCounters::UNCHARGED;
        *self.counter(&mut delta) = amount;
        delta
    }
}
impl NativeResourceAllowance {
    fn lock(&self) -> Result<MutexGuard<'_, NativeResourceState>, DemandError> {
        let state = self
            .state
            .lock()
            .map_err(|_| DemandError::OriginalWorkPoisoned)?;
        if state.poisoned {
            return Err(DemandError::OriginalWorkPoisoned);
        }
        Ok(state)
    }
    fn retirement_state(&self) -> MutexGuard<'_, NativeResourceState> {
        self.state.lock().unwrap_or_else(|error| {
            let mut state = error.into_inner();
            state.poisoned = true;
            state
        })
    }
    fn local_delta(
        state: &NativeResourceState,
        mut delta: WorkCounters,
        bytes: u64,
    ) -> Result<WorkCounters, DemandError> {
        delta.peak_allocation_bytes = state.live.checked_add(bytes).ok_or(WorkError::Overflow)?;
        Ok(delta)
    }
    fn admission_delta(
        state: &NativeResourceState,
        delta: WorkCounters,
        bytes: u64,
    ) -> Result<WorkCounters, DemandError> {
        Self::local_delta(
            state,
            delta,
            bytes
                .checked_add(state.reserved_allocation_bytes)
                .ok_or(WorkError::Overflow)?,
        )
    }
    pub(super) fn contains_scope(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
            || self
                .parent
                .as_ref()
                .is_some_and(|parent| parent.contains_scope(other))
    }
    pub(crate) fn admit(&self, delta: WorkCounters, bytes: u64) -> Result<(), DemandError> {
        if let Some(parent) = &self.parent {
            parent.admit(delta, bytes)?;
        }
        let state = self.lock()?;
        state.work.admit(
            &Self::admission_delta(&state, delta, bytes)?,
            &state.pending.remaining(self.budget)?,
        )?;
        Ok(())
    }
    pub(crate) fn admission<T>(
        &self,
        delta: WorkCounters,
        bytes: u64,
        operation: &mut dyn FnMut() -> Result<T, DemandError>,
    ) -> Result<T, DemandError> {
        let mut admitted = || {
            let mut state = self.lock()?;
            let local = Self::local_delta(&state, delta, bytes)?;
            state.work.admit(
                &Self::admission_delta(&state, delta, bytes)?,
                &state.pending.remaining(self.budget)?,
            )?;
            let value = operation()?;
            state.work.try_add_assign(&local)?;
            state.live += bytes;
            Ok(value)
        };
        match &self.parent {
            Some(parent) => parent.admission(delta, bytes, &mut admitted),
            None => admitted(),
        }
    }
    pub(crate) fn reserve<T>(
        &self,
        maximum: WorkCounters,
        bytes: u64,
        excluded: Option<&Self>,
        operation: &mut dyn FnMut() -> Result<T, DemandError>,
    ) -> Result<T, DemandError> {
        if excluded.is_some_and(|excluded| excluded.contains_scope(self)) {
            return operation();
        }
        let mut reserved = || {
            let mut state = self.lock()?;
            let delta = if maximum.allocation_operations == 0 {
                maximum
            } else {
                Self::admission_delta(&state, maximum, bytes)?
            };
            state
                .work
                .admit(&delta, &state.pending.remaining(self.budget)?)?;
            let pending = state.pending.checked_add(maximum)?;
            let allocation = state
                .reserved_allocation_bytes
                .checked_add(bytes)
                .ok_or(WorkError::Overflow)?;
            let value = operation()?;
            state.pending = pending;
            state.reserved_allocation_bytes = allocation;
            Ok(value)
        };
        match &self.parent {
            Some(parent) => parent.reserve(maximum, bytes, excluded, &mut reserved),
            None => reserved(),
        }
    }
    pub(crate) fn completed(&self, delta: WorkCounters, bytes: u64) -> Result<(), DemandError> {
        let parent = self
            .parent
            .as_ref()
            .map_or(Ok(()), |parent| parent.completed(delta, bytes));
        let mut state = self.retirement_state();
        let local = Self::local_delta(&state, delta, bytes)?;
        state.work.try_add_assign(&local)?;
        state.live += bytes;
        let checked = if state.poisoned {
            Err(DemandError::OriginalWorkPoisoned)
        } else {
            state
                .work
                .verify(state.pending.remaining(self.budget)?)
                .map_err(Into::into)
        };
        parent.and(checked)
    }
    pub(crate) fn retire(&self, bytes: u64) -> Result<(), DemandError> {
        let parent = self
            .parent
            .as_ref()
            .map_or(Ok(()), |parent| parent.retire(bytes));
        let mut state = self.retirement_state();
        match state.live.checked_sub(bytes) {
            Some(live) => state.live = live,
            None => {
                state.poisoned = true;
                return Err(DemandError::OriginalWorkPoisoned);
            }
        }
        parent
    }
    pub(crate) fn remaining(&self) -> Result<WorkBudget, DemandError> {
        let state = self.lock()?;
        let mut remaining = state
            .work
            .remaining(state.pending.remaining(self.budget)?)?;
        remaining.peak_allocation_bytes = remaining
            .peak_allocation_bytes
            .checked_sub(state.live)
            .and_then(|bytes| bytes.checked_sub(state.reserved_allocation_bytes))
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
        if excluded.is_some_and(|excluded| excluded.contains_scope(self)) {
            return Ok(());
        }
        let parent = self
            .parent
            .as_ref()
            .map_or(Ok(()), |parent| parent.refund(unused, excluded));
        let mut state = self.retirement_state();
        match unused.remaining(state.pending) {
            Ok(pending) => state.pending = pending,
            Err(error) => {
                state.poisoned = true;
                return Err(error.into());
            }
        }
        parent
    }
    pub(crate) fn complete(
        &self,
        maximum: WorkCounters,
        actual: WorkCounters,
        excluded: Option<&Self>,
    ) -> Result<(), DemandError> {
        if excluded.is_some_and(|excluded| excluded.contains_scope(self)) {
            return Ok(());
        }
        let parent = self
            .parent
            .as_ref()
            .map_or(Ok(()), |parent| parent.complete(maximum, actual, excluded));
        let mut state = self.retirement_state();
        let pending = maximum.remaining(state.pending);
        state.work.try_add_assign(&actual)?;
        match pending {
            Ok(pending) => state.pending = pending,
            Err(error) => {
                state.poisoned = true;
                return Err(error.into());
            }
        }
        if actual.intersect_budget(maximum) != actual {
            return Err(DemandError::InvalidRequest);
        }
        let checked = if state.poisoned {
            Err(DemandError::OriginalWorkPoisoned)
        } else {
            state
                .work
                .verify(state.pending.remaining(self.budget)?)
                .map_err(Into::into)
        };
        parent.and(checked)
    }
    pub(super) fn start_reserved_work<T>(
        &self,
        actual: WorkCounters,
        operation: &mut dyn FnMut() -> Result<T, DemandError>,
    ) -> Result<T, DemandError> {
        let mut started = || {
            let mut state = self.lock()?;
            let pending = actual.remaining(state.pending)?;
            let work = state.work.checked_add(actual)?;
            work.verify(pending.remaining(self.budget)?)?;
            let value = operation()?;
            state.pending = pending;
            state.work = work;
            Ok(value)
        };
        match &self.parent {
            Some(parent) => parent.start_reserved_work(actual, &mut started),
            None => started(),
        }
    }
    pub(crate) fn settle_allocation<T>(
        &self,
        bytes: u64,
        record: bool,
        operation: &mut dyn FnMut() -> Result<T, DemandError>,
    ) -> Result<T, DemandError> {
        let mut settled = || {
            let mut state = self.retirement_state();
            if record && state.poisoned {
                return Err(DemandError::OriginalWorkPoisoned);
            }
            let delta = NativeResourceCredit::AllocationOperations.delta(1);
            let pending = delta.remaining(state.pending)?;
            let reserved = state
                .reserved_allocation_bytes
                .checked_sub(bytes)
                .ok_or(WorkError::Overflow)?;
            let (live, work) = if record {
                (
                    state.live.checked_add(bytes).ok_or(WorkError::Overflow)?,
                    state
                        .work
                        .checked_add(Self::local_delta(&state, delta, bytes)?)?,
                )
            } else {
                (state.live, state.work)
            };
            let value = operation()?;
            state.pending = pending;
            state.reserved_allocation_bytes = reserved;
            state.live = live;
            state.work = work;
            Ok(value)
        };
        match &self.parent {
            Some(parent) => parent.settle_allocation(bytes, record, &mut settled),
            None => settled(),
        }
    }
    pub(super) fn with_original_leaf<T>(
        &self,
        operation: impl FnOnce(&mut crate::demand::DemandWork) -> T,
    ) -> T {
        self._root_only_metadata_guard
            .work()
            .state(|demand| demand.with_resource_scope(Some(self.self_ref.clone()), operation))
    }
}
impl_original_native_read_meter!(NativeResourceAllowance);
