//! Scoped authority calls move the original ordinary frame and restore on drop.
use super::*;
use crate::kernel::allocation::{AllocationLedger, KernelAccounting};

pub(crate) struct AuthorityFrame<'a> {
    value: AuthorityWork<'a>,
    restore: Option<(&'a mut AllocationLedger, &'a mut WorkCounters)>,
}

impl<'a> AuthorityFrame<'a> {
    pub(crate) fn borrow(accounting: &'a mut KernelAccounting<'_>) -> Result<Self, FsError> {
        accounting.check()?;
        let cancellation = accounting.cancellation();
        match accounting {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work, .. } => Ok(Self {
                value: AuthorityWork::native(work, cancellation),
                restore: None,
            }),
            KernelAccounting::Standalone {
                allocations,
                work,
                budget,
                ..
            } => Self::standalone(allocations, work, *budget, cancellation),
            KernelAccounting::BorrowedStandalone {
                allocations,
                work,
                budget,
                ..
            } => Self::standalone(allocations, work, *budget, cancellation),
        }
    }

    fn standalone(
        allocations: &'a mut AllocationLedger,
        work: &'a mut WorkCounters,
        budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> Result<Self, FsError> {
        allocations.ensure_active()?;
        Ok(Self {
            value: AuthorityWork {
                allocations: std::mem::take(allocations),
                work: std::mem::take(work),
                budget,
                cancellation,
                read_observation: None,
                #[cfg(not(target_arch = "wasm32"))]
                original: None,
                #[cfg(all(not(target_arch = "wasm32"), any(test, feature = "distributed")))]
                request_allocation: None,
            },
            restore: Some((allocations, work)),
        })
    }
}

impl<'a> std::ops::Deref for AuthorityFrame<'a> {
    type Target = AuthorityWork<'a>;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl std::ops::DerefMut for AuthorityFrame<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

#[cfg(test)]
mod tests;

impl Drop for AuthorityFrame<'_> {
    fn drop(&mut self) {
        if let Some((allocations, work)) = self.restore.take() {
            *allocations = std::mem::take(&mut self.value.allocations);
            *work = self.value.work;
        }
    }
}
