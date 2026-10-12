use super::*;
use crate::kernel::allocation::AllocationLedger;
use crate::materializer::NativeResourceAllowance;
use std::sync::{Arc, Weak};

/// Accounting for one bounded source operation.
pub struct DemandWork {
    pub(crate) work: WorkCounters,
    pub(crate) budget: WorkBudget,
    initial_budget: WorkBudget,
    pub(crate) original_work: Option<WorkCounters>,
    pub(super) allocations: AllocationLedger,
    pub(super) cancellation: CancellationToken,
    pub(crate) native_resource: Option<Weak<NativeResourceAllowance>>,
    pub(crate) native_invocation: Option<Weak<crate::materializer::NativeInvocationView>>,
    pub(crate) capture_custody: Option<Weak<CaptureTransientCustody>>,
    pub(super) roots: Admissions,
}
impl DemandWork {
    /// Starts one bounded operation.
    pub fn new(budget: WorkBudget, cancellation: &CancellationToken) -> Self {
        Self {
            work: WorkCounters::UNCHARGED,
            budget,
            initial_budget: budget,
            original_work: None,
            allocations: AllocationLedger::default(),
            cancellation: cancellation.clone(),
            native_resource: None,
            native_invocation: None,
            capture_custody: None,
            roots: Admissions::default(),
        }
    }
    pub(crate) fn scope<T, R>(
        &mut self,
        field: fn(&mut Self) -> &mut T,
        value: T,
        operation: impl FnOnce(&mut Self) -> R,
    ) -> R {
        let saved = std::mem::replace(field(self), value);
        let answer = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| operation(self)));
        *field(self) = saved;
        answer.unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    }
    pub(crate) fn original_budget(&self) -> WorkBudget {
        self.initial_budget
    }
    pub(crate) fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }
    pub(crate) fn capture_custody(&self) -> Option<Arc<CaptureTransientCustody>> {
        self.capture_custody.as_ref().and_then(Weak::upgrade)
    }
    pub(crate) fn check(&self) -> Result<(), DemandError> {
        if self.roots.poisoned {
            return Err(DemandError::OriginalWorkPoisoned);
        }
        self.cancellation
            .check()
            .map_err(|_| DemandError::Cancelled)?;
        if let Some(view) = self.native_invocation.as_ref().and_then(Weak::upgrade) {
            view.check()?;
        }
        if self
            .capture_custody
            .as_ref()
            .is_some_and(|owner| owner.strong_count() == 0)
        {
            return Err(DemandError::NativeWorkUnavailable);
        }
        Ok(())
    }
    pub(super) fn resource(&self) -> Option<Arc<NativeResourceAllowance>> {
        self.native_resource.as_ref().and_then(Weak::upgrade)
    }
    fn admission(
        &mut self,
        delta: WorkCounters,
        bytes: u64,
        mut operation: impl FnMut(&mut Self) -> Result<(), DemandError>,
    ) -> Result<(), DemandError> {
        match self.resource() {
            Some(resource) => resource.admission(delta, bytes, &mut || operation(self)),
            None => operation(self),
        }
    }
    pub(crate) fn take_for_native_queue(&mut self) -> Self {
        let shell = Self::new(WorkBudget::default(), &self.cancellation);
        std::mem::replace(self, shell)
    }
    pub(crate) fn allocation_parts(
        &mut self,
    ) -> (
        &mut AllocationLedger,
        &mut WorkCounters,
        WorkBudget,
        &CancellationToken,
    ) {
        (
            &mut self.allocations,
            &mut self.work,
            self.budget,
            &self.cancellation,
        )
    }
    pub(crate) fn live_allocation_bytes(&self) -> u64 {
        self.allocations
            .live_bytes()
            .checked_sub(self.roots.freed_bytes())
            .and_then(|live| live.checked_add(self.roots.unrecorded_bytes()))
            .expect("balanced retained bytes")
    }
    pub(crate) fn receipt(&mut self) -> WorkCounters {
        self.reconcile_native_controls()
            .expect("balanced native retirement");
        self.work
    }
    pub(crate) fn admit(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.reconcile_cancellation_registry()?;
        self.check()?;
        if let Some(resource) = self.resource() {
            resource.admit(delta, 0)?;
        }
        Ok(self.work.admit(&delta, &self.budget)?)
    }
    pub(crate) fn admit_retained(&mut self, bytes: u64) -> Result<(), DemandError> {
        self.reconcile_cancellation_registry()?;
        self.check()?;
        if let Some(resource) = self.resource() {
            resource.admit(WorkCounters::UNCHARGED, bytes)?;
        }
        let peak = self
            .allocations
            .live_bytes()
            .checked_add(bytes)
            .and_then(|bytes| bytes.checked_add(self.allocations.reserved_bytes()))
            .ok_or(crate::WorkError::Overflow)?;
        Ok(self.work.admit(
            &WorkCounters {
                peak_allocation_bytes: peak,
                ..WorkCounters::UNCHARGED
            },
            &self.budget,
        )?)
    }
    pub(crate) fn charge(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.reconcile_cancellation_registry()?;
        self.check()?;
        self.admission(delta, 0, |state| {
            Ok(state.work.charge(&delta, &state.budget)?)
        })
    }
    pub(crate) fn record(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        let resource = self
            .resource()
            .map_or(Ok(()), |resource| resource.completed(delta, 0));
        let reconciled = self
            .reconcile_cancellation_registry()
            .map_err(DemandError::from);
        let recorded = self
            .work
            .try_add_assign(&delta)
            .map_err(DemandError::from)
            .and_then(|()| self.work.verify(self.budget).map_err(DemandError::from));
        reconciled.and(recorded).and(resource)
    }
    pub(crate) fn spent(&mut self, mut delta: WorkCounters) -> Result<(), DemandError> {
        let resource = self
            .resource()
            .map_or(Ok(()), |resource| resource.completed(delta, 0));
        let reconciled = self
            .reconcile_cancellation_registry()
            .map_err(DemandError::from);
        delta.peak_allocation_bytes = delta
            .peak_allocation_bytes
            .checked_add(self.allocations.live_bytes())
            .ok_or(crate::WorkError::Overflow)?;
        self.work.try_add_assign(&delta)?;
        reconciled.and(resource)
    }
    pub(crate) fn claim_allocation(
        &mut self,
        bytes: u64,
        operations: u64,
        delta: WorkCounters,
    ) -> Result<(), DemandError> {
        self.reconcile_cancellation_registry()?;
        self.check()?;
        self.admission(
            delta.checked_add(WorkCounters {
                allocation_operations: operations,
                ..WorkCounters::UNCHARGED
            })?,
            bytes,
            |state| {
                let mut work = state.work;
                let mut ledger = state.allocations;
                work.charge(&delta, &state.budget)?;
                ledger
                    .claim_bytes(bytes, operations, &mut work, state.budget)
                    .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
                state.work = work;
                state.allocations = ledger;
                if let Some(capture) = state.capture_custody() {
                    capture.claim(bytes);
                }
                Ok(())
            },
        )
    }
    pub(crate) fn retain_native_owned(
        &mut self,
        bytes: u64,
        capture: &Option<Arc<CaptureTransientCustody>>,
    ) -> Result<(), DemandError> {
        if self.roots.poisoned {
            return Err(DemandError::OriginalWorkPoisoned);
        }
        self.admission(WorkCounters::UNCHARGED, bytes, |state| {
            state
                .allocations
                .claim_bytes(bytes, 0, &mut state.work, state.budget)
                .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
            if let Some(capture) = capture {
                capture.claim(bytes);
            }
            Ok(())
        })
    }
    pub(crate) fn record_native_owned_completed(
        &mut self,
        bytes: u64,
        capture: &Option<Arc<CaptureTransientCustody>>,
    ) -> (u64, Result<(), DemandError>) {
        if let Err(error) = self.allocations.record_owned_bytes(bytes, &mut self.work) {
            return (0, Err(error.into_work_or(DemandError::InvalidRequest)));
        }
        if let Some(capture) = capture {
            capture.claim(bytes);
        }
        let resource = self.resource().map_or(Ok(()), |resource| {
            resource.completed(WorkCounters::UNCHARGED, bytes)
        });
        (
            bytes,
            self.check()
                .and_then(|()| self.work.verify(self.budget).map_err(DemandError::from))
                .and(resource),
        )
    }
    pub(crate) fn release(&mut self, bytes: u64) -> Result<(), DemandError> {
        let released = self
            .allocations
            .release(bytes)
            .map_err(|error| error.into_work_or(DemandError::InvalidRequest));
        if let Some(capture) = self.capture_custody() {
            capture.release(bytes);
        }
        let resource = self
            .resource()
            .map_or(Ok(()), |resource| resource.retire(bytes));
        if let Err(error) = released.and(resource) {
            self.record_native_io_failure(error);
            self.poison_native_original_work();
            return Err(DemandError::OriginalWorkPoisoned);
        }
        Ok(())
    }
    pub(crate) fn reconcile_native_allocation(
        &mut self,
        allocation: &mut u64,
        actual: u64,
    ) -> Result<(), DemandError> {
        if actual < *allocation {
            self.release(*allocation - actual)?;
            *allocation = actual;
        }
        let (retained, result) =
            self.record_native_owned_completed(actual - *allocation, &self.capture_custody());
        *allocation += retained;
        result
    }
    pub(crate) fn remaining(&mut self) -> Result<WorkBudget, DemandError> {
        self.reconcile_native_controls()?;
        let mut remaining = self.work.remaining(self.budget)?;
        remaining.peak_allocation_bytes = remaining
            .peak_allocation_bytes
            .checked_sub(self.live_allocation_bytes())
            .and_then(|bytes| bytes.checked_sub(self.allocations.reserved_bytes()))
            .ok_or(crate::WorkError::Overflow)?;
        Ok(self.resource().map_or(Ok(remaining), |resource| {
            resource
                .remaining()
                .map(|local| remaining.intersect_budget(local))
        })?)
    }
    pub(crate) fn merge(&mut self, delta: WorkCounters, live: u64) -> Result<(), DemandError> {
        self.work = self.work.with_backend(delta, live)?;
        self.reconcile_cancellation_registry()?;
        Ok(self.work.verify(self.budget)?)
    }
    pub(crate) fn record_native_io_failure(&mut self, error: DemandError) -> std::io::Error {
        match self.roots.io_failure.get_or_insert(error) {
            DemandError::Allocation(_) => std::io::ErrorKind::OutOfMemory.into(),
            _ => std::io::ErrorKind::Other.into(),
        }
    }
    pub(crate) fn take_native_io_failure(&mut self) -> Option<DemandError> {
        self.roots.io_failure.take()
    }
    pub(crate) fn poison_native_original_work(&mut self) {
        self.roots.poisoned = true;
        self.record_native_io_failure(DemandError::OriginalWorkPoisoned);
    }
}
impl Drop for DemandWork {
    fn drop(&mut self) {
        self.reconcile_native_controls()
            .expect("balanced final native retirement");
    }
}
