//! Native adapter over the original allocation-ledger handoff.
use super::*;

/// The accepted native producer keeps the original ledger until settlement.
/// Cancellation abandons reply delivery; it cannot reactivate the caller's
/// standalone branch while native work can still charge it.
pub(crate) struct NativeAccounting {
    pub(crate) work: crate::NativeMaterializationWork,
    baseline: Option<WorkCounters>,
}

impl NativeAccounting {
    pub(crate) fn take(accounting: &mut KernelAccounting<'_>) -> Result<Self, crate::FsError> {
        accounting.check()?;
        if let Some(work) = accounting.native_work() {
            return Ok(Self {
                work: work.clone(),
                baseline: None,
            });
        }
        let backing = RetainedAllocation::claim(
            crate::NativeMaterializationWork::accounting_state_bytes(),
            1,
            accounting,
        )?;
        let RetainedAllocation::Standalone { _lease: backing } = backing else {
            return Err(crate::FsError::AccountingHandoffActive);
        };
        let (allocations, counters, budget, stop) = accounting
            .standalone_parts()
            .ok_or(crate::FsError::AccountingHandoffActive)?;
        let mut state = crate::demand::DemandWork::new(budget, stop);
        let (owned, owned_counters, _, _) = state.allocation_parts();
        *owned = allocations.handoff(counters, budget)?;
        *owned_counters = *counters;
        let baseline = *counters;
        let meter = crate::demand::NativeDemandReadMeter::with_backing(state, backing);
        Ok(Self {
            work: crate::NativeMaterializationWork::from_meter(Arc::new(meter)),
            baseline: Some(baseline),
        })
    }

    /// Only the producer's terminal result permits restoring admission.
    pub(crate) fn restore(
        self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        let Some(baseline) = self.baseline else {
            if !accounting
                .native_work()
                .is_some_and(|work| work.shares_ledger(&self.work))
            {
                return Err(crate::FsError::AccountingHandoffActive);
            }
            return Ok(());
        };
        self.work.state(|state| {
            let (allocations, counters, budget, _) = accounting
                .standalone_parts()
                .ok_or(crate::FsError::AccountingHandoffActive)?;
            if budget != state.budget || *counters != baseline {
                return Err(crate::FsError::AccountingHandoffActive);
            }
            state.receipt();
            let (owned, owned_counters, _, _) = state.allocation_parts();
            let original = owned.handoff(owned_counters, budget)?;
            allocations.resume(original)?;
            *counters = state.work;
            counters.verify(budget)?;
            Ok(())
        })
    }
}
