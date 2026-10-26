use super::*;

pub use crate::native_mount::shared::metering::NativeMountWorkScope;

impl crate::native_mount::shared::metering::MeteredView for Runtime {
    fn activate(&self, work: &crate::NativeMaterializationWork) -> Result<u64, NativeMountError> {
        lock_recover(&self.active_work).enter(work)
    }

    fn deactivate(&self, work: &crate::NativeMaterializationWork, generation: u64) {
        lock_recover(&self.active_work).leave(work, generation);
    }
}

impl Runtime {
    /// The meter a callback charges, the view's own when no invocation is
    /// active; `authored` checks the callback as a write.
    pub(super) fn native_work(
        &self,
        authored: bool,
    ) -> Result<crate::NativeMaterializationWork, NativeMountError> {
        let active = lock_recover(&self.active_work).upgrade();
        let original = lock_recover(&self.original_work).clone();
        let fallback = lock_recover(&self.work).clone();
        crate::native_mount::shared::metering::select(
            self.source.as_ref(),
            active,
            original.as_ref(),
            self.callback_guard.as_ref(),
            Some(fallback),
            authored,
        )?
        .ok_or(NativeMountError::ControlPending)
    }
}

pub(super) fn reserve<T>(
    values: &mut Vec<T>,
    additional: usize,
    allocation: &mut crate::NativeAllocation,
    work: &crate::NativeMaterializationWork,
) -> Result<(), MountSourceError> {
    let required = values
        .len()
        .checked_add(additional)
        .ok_or(crate::WorkError::Overflow)?;
    if required <= values.capacity() {
        return Ok(());
    }
    let expected = required
        .checked_mul(size_of::<T>())
        .ok_or(crate::WorkError::Overflow)? as u64;
    let current = (values.capacity() * size_of::<T>()) as u64;
    let copied = crate::WorkCounters {
        bytes_copied: (values.len() * size_of::<T>()) as u64,
        ..crate::WorkCounters::UNCHARGED
    };
    work.admit(copied)?;
    allocation.grow(expected - current)?;
    values
        .try_reserve_exact(additional)
        .map_err(MountSourceError::Allocation)?;
    allocation.reconcile_capacity((values.capacity() * size_of::<T>()) as u64)?;
    work.record_admitted(copied)?;
    Ok(())
}
