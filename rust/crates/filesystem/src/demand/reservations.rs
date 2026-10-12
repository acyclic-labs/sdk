use super::*;
use crate::materializer::{NativeResourceAllowance, NativeResourceCredit};
use acyclic_native_runtime::NativeOperationKind;

impl DemandWork {
    pub(crate) fn reserve_native_work(&mut self, maximum: WorkCounters) -> Result<(), DemandError> {
        if maximum.peak_allocation_bytes != 0 || maximum.allocation_operations != 0 {
            return Err(DemandError::InvalidRequest);
        }
        if let Some(resource) = self.resource() {
            return resource.reserve(maximum, 0, None, &mut || {
                self.scope(
                    |work| &mut work.native_resource,
                    None,
                    |work| work.reserve_native_work(maximum),
                )
            });
        }
        self.admit(maximum)?;
        let pending = self.roots.pending.checked_add(maximum)?;
        let budget = maximum.remaining(self.budget)?;
        self.roots.pending = pending;
        self.budget = budget;
        Ok(())
    }
    pub(crate) fn refund_native_work(&mut self, unused: WorkCounters) -> Result<(), DemandError> {
        let pending = unused.remaining(self.roots.pending)?;
        let budget = self.budget.checked_add(unused)?;
        self.roots.pending = pending;
        self.budget = budget;
        self.resource()
            .map_or(Ok(()), |resource| resource.refund_work(unused))
    }
    pub(crate) fn record_native_work(&mut self, actual: WorkCounters) -> Result<(), DemandError> {
        let refund = self.refund_native_work(actual);
        let record = self.record(actual);
        refund.and(record).and(if self.roots.poisoned {
            Err(DemandError::OriginalWorkPoisoned)
        } else {
            Ok(())
        })
    }
    pub(crate) fn start_native_reserved_work(
        &mut self,
        actual: WorkCounters,
    ) -> Result<(), DemandError> {
        if self.roots.poisoned {
            return Err(DemandError::OriginalWorkPoisoned);
        }
        let pending = actual.remaining(self.roots.pending)?;
        let budget = self.budget.checked_add(actual)?;
        let work = self.work.checked_add(actual)?;
        work.verify(budget)?;
        self.roots.pending = pending;
        self.budget = budget;
        self.work = work;
        Ok(())
    }
    pub(crate) fn reserve(
        &mut self,
        kind: NativeResourceCredit,
        count: u64,
    ) -> Result<(), DemandError> {
        self.reserve_native_work(kind.delta(count))
    }
    pub(crate) fn refund(
        &mut self,
        kind: NativeResourceCredit,
        count: u64,
    ) -> Result<(), DemandError> {
        self.refund_native_work(kind.delta(count))
    }
    pub(crate) fn complete(
        &mut self,
        kind: NativeResourceCredit,
        reserved: u64,
        actual: u64,
    ) -> Result<(), DemandError> {
        let refund = self.refund(kind, reserved);
        let record = self.record(kind.delta(actual));
        if actual > reserved {
            return Err(DemandError::InvalidRequest);
        }
        refund.and(record)
    }
    pub(crate) fn operation_credit(kind: NativeOperationKind) -> NativeResourceCredit {
        match kind {
            NativeOperationKind::Read => NativeResourceCredit::ReadOperations,
            NativeOperationKind::Write => NativeResourceCredit::WriteOperations,
            NativeOperationKind::Durability => NativeResourceCredit::DurabilityOperations,
        }
    }
    pub(crate) fn byte_credit(
        kind: NativeOperationKind,
    ) -> Result<NativeResourceCredit, std::io::Error> {
        match kind {
            NativeOperationKind::Read => Ok(NativeResourceCredit::ReadBytes),
            NativeOperationKind::Write => Ok(NativeResourceCredit::WriteBytes),
            NativeOperationKind::Durability => Err(std::io::ErrorKind::Unsupported.into()),
        }
    }
    pub(crate) fn reserve_native_allocation(&mut self, bytes: u64) -> Result<(), DemandError> {
        if let Some(resource) = self.resource() {
            return resource.reserve(
                NativeResourceCredit::AllocationOperations.delta(1),
                bytes,
                None,
                &mut || {
                    self.scope(
                        |work| &mut work.native_resource,
                        None,
                        |work| work.reserve_native_allocation(bytes),
                    )
                },
            );
        }
        self.reconcile_cancellation_registry()?;
        self.check()?;
        let pending = self
            .roots
            .pending
            .allocation_operations
            .checked_add(1)
            .ok_or(crate::WorkError::Overflow)?;
        self.allocations
            .reserve_allocation(bytes, &self.work, self.budget)
            .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
        self.roots.pending.allocation_operations = pending;
        self.budget.allocation_operations -= 1;
        Ok(())
    }
    pub(crate) fn cancel_reserved_native_allocation(
        &mut self,
        bytes: u64,
    ) -> Result<(), DemandError> {
        if let Some(resource) = self.resource() {
            return resource.settle_allocation(bytes, false, &mut || {
                self.scope(
                    |work| &mut work.native_resource,
                    None,
                    |work| work.cancel_reserved_native_allocation(bytes),
                )
            });
        }
        let pending = self
            .roots
            .pending
            .allocation_operations
            .checked_sub(1)
            .ok_or(crate::WorkError::Overflow)?;
        let budget = self
            .budget
            .allocation_operations
            .checked_add(1)
            .ok_or(crate::WorkError::Overflow)?;
        self.allocations
            .cancel_reserved_allocation(bytes)
            .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
        self.roots.pending.allocation_operations = pending;
        self.budget.allocation_operations = budget;
        Ok(())
    }
    pub(crate) fn record_reserved_native_allocation(
        &mut self,
        bytes: u64,
    ) -> Result<(), DemandError> {
        if let Some(resource) = self.resource() {
            return resource.settle_allocation(bytes, true, &mut || {
                self.scope(
                    |work| &mut work.native_resource,
                    None,
                    |work| work.record_reserved_native_allocation(bytes),
                )
            });
        }
        if self.roots.poisoned {
            return Err(DemandError::OriginalWorkPoisoned);
        }
        let pending = self
            .roots
            .pending
            .allocation_operations
            .checked_sub(1)
            .ok_or(crate::WorkError::Overflow)?;
        let budget = self
            .budget
            .allocation_operations
            .checked_add(1)
            .ok_or(crate::WorkError::Overflow)?;
        self.allocations
            .record_reserved_allocation(bytes, &mut self.work)
            .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
        self.roots.pending.allocation_operations = pending;
        self.budget.allocation_operations = budget;
        if let Some(capture) = self.capture_custody() {
            capture.claim(bytes);
        }
        Ok(())
    }
}
