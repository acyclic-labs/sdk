//! Scoped authority calls move the original ordinary frame and restore on drop.
use super::*;
use crate::kernel::allocation::{AllocationLedger, KernelAccounting};

pub(crate) struct AuthorityFrame<'a> {
    value: AuthorityWork<'a>,
    restore: Option<(&'a mut AllocationLedger, &'a mut WorkCounters)>,
    restore_construction: &'a mut crate::collection::ConstructionRoots,
}

impl<'a> AuthorityFrame<'a> {
    pub(crate) fn borrow(accounting: &'a mut KernelAccounting<'_>) -> Result<Self, FsError> {
        accounting.check()?;
        let cancellation = accounting.cancellation();
        match accounting {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal {
                work, construction, ..
            } => Ok(Self {
                value: AuthorityWork {
                    construction: std::mem::take(construction.roots()),
                    ..AuthorityWork::native(work, cancellation)
                },
                restore: None,
                restore_construction: construction.roots(),
            }),
            KernelAccounting::Standalone {
                allocations,
                work,
                budget,
                construction,
                ..
            } => Ok(Self::standalone(
                allocations,
                work,
                *budget,
                cancellation,
                construction.roots(),
            )),
            KernelAccounting::BorrowedStandalone {
                allocations,
                work,
                budget,
                construction,
                ..
            } => Ok(Self::standalone(
                allocations,
                work,
                *budget,
                cancellation,
                construction.roots(),
            )),
        }
    }

    fn standalone(
        allocations: &'a mut AllocationLedger,
        work: &'a mut WorkCounters,
        budget: WorkBudget,
        cancellation: &'a CancellationToken,
        construction: &'a mut crate::collection::ConstructionRoots,
    ) -> Self {
        Self {
            value: AuthorityWork {
                construction: std::mem::take(construction),
                allocations: std::mem::take(allocations),
                scratch_floor: 0,
                read_observation: None,
                work: std::mem::take(work),
                budget,
                cancellation,
                #[cfg(not(target_arch = "wasm32"))]
                original: None,
                #[cfg(not(target_arch = "wasm32"))]
                request_allocation: None,
            },
            restore: Some((allocations, work)),
            restore_construction: construction,
        }
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
        *self.restore_construction = std::mem::take(&mut self.value.construction);
        if let Some((allocations, work)) = self.restore.take() {
            *allocations = std::mem::take(&mut self.value.allocations);
            *work = self.value.work;
        }
    }
}
