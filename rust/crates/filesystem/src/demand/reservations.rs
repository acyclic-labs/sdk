use super::*;
use crate::materializer::NativeResourceCredit;
use acyclic_native_runtime::NativeOperationKind;

impl DemandWork {
    pub(crate) fn reserve(&mut self, maximum: WorkCounters) -> Result<(), DemandError> {
        if maximum.peak_allocation_bytes != 0 || maximum.allocation_operations != 0 {
            return Err(DemandError::InvalidRequest);
        }
        self.reconcile_cancellation_registry()?;
        self.check()?;
        let resource = self.resource();
        let mut reserve = || {
            self.work.admit(&maximum, &self.budget)?;
            let pending = self.roots.pending.checked_add(maximum)?;
            let budget = maximum.remaining(self.budget)?;
            self.roots.pending = pending;
            self.budget = budget;
            Ok(())
        };
        match resource {
            Some(resource) => resource.reserve(maximum, 0, None, &mut reserve),
            None => reserve(),
        }
    }
    pub(crate) fn refund(&mut self, unused: WorkCounters) -> Result<(), DemandError> {
        let pending = unused.remaining(self.roots.pending)?;
        let budget = self.budget.checked_add(unused)?;
        self.roots.pending = pending;
        self.budget = budget;
        self.resource()
            .map_or(Ok(()), |resource| resource.refund(unused, None))
    }
    pub(crate) fn complete(
        &mut self,
        reserved: WorkCounters,
        actual: WorkCounters,
    ) -> Result<(), DemandError> {
        let refunded = self.refund(reserved);
        let recorded = self.record(actual);
        refunded
            .and(recorded)
            .and(
                actual
                    .remaining(reserved)
                    .map(|_| ())
                    .map_err(DemandError::from),
            )
            .and(if self.roots.poisoned {
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
        self.reconcile_cancellation_registry()?;
        self.check()?;
        let resource = self.resource();
        let mut reserve = || {
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
        };
        match resource {
            Some(resource) => resource.reserve(
                NativeResourceCredit::AllocationOperations.delta(1),
                bytes,
                None,
                &mut reserve,
            ),
            None => reserve(),
        }
    }
    pub(crate) fn settle_native_allocation(
        &mut self,
        bytes: u64,
        allocated: bool,
    ) -> Result<(), DemandError> {
        if allocated && self.roots.poisoned {
            return Err(DemandError::OriginalWorkPoisoned);
        }
        let resource = self.resource();
        let mut settle = || {
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
            if allocated {
                self.allocations
                    .record_reserved_allocation(bytes, &mut self.work)
                    .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
                if let Some(capture) = self.capture_custody() {
                    capture.claim(bytes);
                }
            } else {
                self.allocations
                    .cancel_reserved_allocation(bytes)
                    .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
            }
            self.roots.pending.allocation_operations = pending;
            self.budget.allocation_operations = budget;
            Ok(())
        };
        match resource {
            Some(resource) => resource.settle_allocation(bytes, allocated, &mut settle),
            None => settle(),
        }
    }
}
