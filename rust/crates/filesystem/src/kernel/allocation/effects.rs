use crate::{CancellationToken, FsError, NativeMaterializationWork, WorkCounters};

#[cfg(all(test, acyclic_fs_loom))]
loom::thread_local! {
    pub(crate) static OMIT_RESERVATION: std::cell::Cell<bool> = std::cell::Cell::new(false);
    pub(crate) static OMIT_REFUND: std::cell::Cell<bool> = std::cell::Cell::new(false);
}

#[cfg(all(test, acyclic_fs_loom))]
pub(crate) fn broken_reservation() -> bool {
    OMIT_RESERVATION.with(std::cell::Cell::get)
}

// Owns withheld credit on its exact original ledger. No leaf/resource lock is
// retained between calls: state() acquires the existing leaf -> ancestor order.
// Completion and Drop enter that same checked order, including during unwind.
#[must_use]
pub(crate) struct EffectCredit<'a> {
    work: &'a NativeMaterializationWork,
    cancellation: &'a CancellationToken,
    pending: WorkCounters,
}

impl<'a> EffectCredit<'a> {
    fn new(work: &'a NativeMaterializationWork, cancellation: &'a CancellationToken) -> Self {
        Self {
            work,
            cancellation,
            pending: WorkCounters::UNCHARGED,
        }
    }

    pub(crate) fn for_accounting(accounting: &super::KernelAccounting<'a>) -> Option<Self> {
        match accounting {
            super::KernelAccounting::NativeOriginal { work, cancellation } => {
                Some(Self::new(work, cancellation))
            }
            _ => None,
        }
    }

    pub(crate) fn reserve(&mut self, delta: WorkCounters, reuse: bool) -> Result<(), FsError> {
        reserve(
            self.work,
            self.cancellation,
            &mut self.pending,
            delta,
            reuse,
        )
    }

    pub(crate) fn complete(&mut self, actual: WorkCounters) -> Result<(), FsError> {
        complete(self.work, self.cancellation, &mut self.pending, actual)
    }
}

impl Drop for EffectCredit<'_> {
    fn drop(&mut self) {
        refund(self.work, &mut self.pending);
    }
}

pub(crate) fn perform<T>(
    work: &NativeMaterializationWork,
    cancellation: &CancellationToken,
    delta: WorkCounters,
    effect: impl FnOnce() -> T,
) -> Result<T, FsError> {
    let mut reserved = EffectCredit::new(work, cancellation);
    reserved.reserve(delta, false)?;
    // The effect runs outside the original leaf and ancestor resource locks.
    // A panic returns unused credit; completion stays charged if cancelled.
    let output = effect();
    reserved.complete(delta)?;
    Ok(output)
}

fn reserve(
    work: &NativeMaterializationWork,
    cancellation: &CancellationToken,
    pending: &mut WorkCounters,
    delta: WorkCounters,
    reuse: bool,
) -> Result<(), FsError> {
    work.state(|state| {
        // Existing ledger failures precede caller cancellation; cancellation
        // precedes proposed work. The leaf remains held through reservation.
        let native = crate::materializer::native_materialization_work_error;
        state.admit(WorkCounters::UNCHARGED).map_err(native)?;
        cancellation.check()?;
        let delta = state.item_charge(delta);
        let delta = if reuse {
            delta
                .intersect_budget(*pending)
                .remaining(delta)
                .map_err(crate::NativeTreeMaterializationError::from)?
        } else {
            delta
        };
        if delta == WorkCounters::UNCHARGED {
            return Ok(());
        }
        let next = pending
            .checked_add(delta)
            .map_err(crate::NativeTreeMaterializationError::from)?;
        #[cfg(all(test, acyclic_fs_loom))]
        let broken = broken_reservation();
        #[cfg(not(all(test, acyclic_fs_loom)))]
        let broken = false;
        let admitted = if broken {
            state.admit(delta)
        } else {
            state.reserve_reconciled(delta)
        };
        admitted.map_err(native)?;
        *pending = next;
        Ok(())
    })
}

fn complete(
    work: &NativeMaterializationWork,
    cancellation: &CancellationToken,
    pending: &mut WorkCounters,
    actual: WorkCounters,
) -> Result<(), FsError> {
    work.state(|state| {
        let actual = state.item_charge(actual);
        let credited = actual.intersect_budget(*pending);
        *pending = credited
            .remaining(*pending)
            .map_err(crate::NativeTreeMaterializationError::from)?;
        let native = crate::materializer::native_materialization_work_error;
        state.complete(credited, actual).map_err(native)?;
        state.admit(WorkCounters::UNCHARGED).map_err(native)?;
        cancellation.check().map_err(Into::into)
    })
}

fn refund(work: &NativeMaterializationWork, pending: &mut WorkCounters) {
    if *pending == WorkCounters::UNCHARGED {
        return;
    }
    #[cfg(all(test, acyclic_fs_loom))]
    if OMIT_REFUND.with(std::cell::Cell::get) {
        return;
    }
    let unused = std::mem::replace(pending, WorkCounters::UNCHARGED);
    // Release the leaf before any allocation owner can re-enter it on drop.
    work.state(|state| {
        if state.refund(unused).is_err() {
            state.poison_native_original_work();
        }
    });
}

#[cfg(all(test, not(acyclic_fs_loom)))]
mod tests;
