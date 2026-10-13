use super::*;

/// Only the backing allocation this invocation actually created. A generation
/// identifies replacements without allocator pointer-reuse ambiguity.
#[derive(Default)]
pub(crate) struct CancellationWaitAdmission {
    pub(super) registry: Option<(u64, u64)>,
}

impl CancellationWaitAdmission {
    pub(crate) fn freed_registry_bytes(&self, token: &CancellationToken) -> u64 {
        self.registry
            .filter(|(generation, _)| {
                *generation != token.state.registry_generation.load(Ordering::Acquire)
            })
            .map_or(0, |(_, bytes)| bytes)
    }

    pub(crate) fn reconcile_registry(
        &mut self,
        token: &CancellationToken,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
    ) -> Result<(), crate::WorkError> {
        let bytes = self.freed_registry_bytes(token);
        if bytes != 0 {
            allocations
                .release(bytes)
                .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
            self.registry = None;
        }
        Ok(())
    }
}

pub(super) struct WaitMeter<'a> {
    pub(super) allocations: &'a mut crate::kernel::allocation::AllocationLedger,
    pub(super) work: &'a mut crate::WorkCounters,
    pub(super) budget: crate::WorkBudget,
    pub(super) admission: &'a mut CancellationWaitAdmission,
    pub(super) temporary_registry_bytes: u64,
    pub(super) pending_registry_bytes: u64,
    pub(super) cleanup_reserved: bool,
}

impl WaitMeter<'_> {
    pub(super) fn allowance(&self) -> Result<crate::WorkBudget, CancellationWaitError> {
        let mut budget = self.budget;
        if self.cleanup_reserved {
            budget.items_examined = budget
                .items_examined
                .checked_sub(1)
                .ok_or(crate::WorkError::Overflow)?;
        }
        Ok(budget)
    }

    pub(super) fn charge(
        &mut self,
        delta: crate::WorkCounters,
    ) -> Result<(), CancellationWaitError> {
        let budget = self.allowance()?;
        self.work.charge(&delta, &budget)?;
        Ok(())
    }

    pub(super) fn claim(
        &mut self,
        bytes: u64,
        operations: u64,
    ) -> Result<(), CancellationWaitError> {
        let budget = self.allowance()?;
        self.allocations
            .claim_bytes(bytes, operations, self.work, budget)
            .map_err(|error| {
                CancellationWaitError::Work(error.into_work_or(crate::WorkError::Overflow))
            })
    }

    pub(super) fn release(&mut self, bytes: u64) {
        self.allocations
            .release(bytes)
            .expect("cancellation allocation ownership invariant");
    }

    pub(super) fn observe_registry(&mut self, generation: u64) {
        if let Some((owned_generation, bytes)) = self.admission.registry {
            if owned_generation != generation {
                self.release(bytes);
                self.admission.registry = None;
            }
        }
    }
}
