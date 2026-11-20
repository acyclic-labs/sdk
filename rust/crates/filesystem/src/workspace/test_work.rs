//! A test fixture that retains part of the caller's original work budget.

use super::{CancellationToken, FsError, KernelAccounting, KernelOwnedAllocation, WorkBudget};
use crate::WorkCounters;
use crate::facade::FsWork;
use crate::kernel::allocation::retained::Retained;
use std::sync::Mutex;

pub(crate) struct Prefunded {
    pub(crate) budget: WorkBudget,
    pub(crate) held: Mutex<Option<(Retained<Vec<u8>>, u64)>>,
}

impl FsWork for &Prefunded {
    type Output<T, E> = (Result<T, E>, WorkCounters, u64, u64);
    type OwnedOutput<T, E> = Self::Output<(T, KernelOwnedAllocation), E>;

    fn finish_owned<T, E: From<FsError>>(
        &self,
        result: Result<(T, KernelOwnedAllocation), E>,
        accounting: &KernelAccounting<'_>,
    ) -> Self::OwnedOutput<T, E> {
        self.finish(result, accounting)
    }

    fn accounting<'a>(&'a self, cancellation: &'a CancellationToken) -> KernelAccounting<'a> {
        let mut accounting = KernelAccounting::standalone(self.budget, cancellation);
        let buffer = crate::kernel::Owned::<Vec<u8>>::allocate(4096, &mut accounting).unwrap();
        let held = Retained::from_owned(buffer, &mut accounting).unwrap();
        accounting.charge_items(1).unwrap();
        let baseline = accounting.standalone_parts().unwrap().0.live_bytes();
        *self.held.lock().unwrap() = Some((held, baseline));
        accounting
    }

    fn finish_with<T, E>(
        &self,
        result: Result<T, E>,
        accounting: &KernelAccounting<'_>,
        _observation_error: impl FnOnce(FsError) -> E,
    ) -> Self::Output<T, E> {
        let KernelAccounting::Standalone { allocations, .. } = accounting else {
            unreachable!("ordinary fixture")
        };
        let baseline = self.held.lock().unwrap().as_ref().unwrap().1;
        (
            result,
            accounting.spent().unwrap(),
            allocations.live_bytes(),
            baseline,
        )
    }
}
