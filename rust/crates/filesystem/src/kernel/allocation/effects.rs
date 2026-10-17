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

pub(crate) fn perform<T>(
    work: &NativeMaterializationWork,
    cancellation: &CancellationToken,
    delta: WorkCounters,
    effect: impl FnOnce() -> T,
) -> Result<T, FsError> {
    struct Reserved<'a>(&'a NativeMaterializationWork, WorkCounters);
    impl Drop for Reserved<'_> {
        fn drop(&mut self) {
            refund(self.0, &mut self.1);
        }
    }
    let mut reserved = Reserved(work, WorkCounters::UNCHARGED);
    reserve(work, cancellation, &mut reserved.1, delta, false)?;
    // The effect runs outside the original leaf and ancestor resource locks.
    // A panic returns unused credit; completion stays charged if cancelled.
    let output = effect();
    complete(work, cancellation, &mut reserved.1, delta)?;
    Ok(output)
}

pub(crate) fn reserve(
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

pub(crate) fn complete(
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

pub(crate) fn refund(work: &NativeMaterializationWork, pending: &mut WorkCounters) {
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
