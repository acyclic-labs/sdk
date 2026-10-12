use super::*;

pub(in crate::native_mount) struct ProjectionWorkScope {
    runtime: Arc<Runtime>,
    work: crate::NativeMaterializationWork,
    generation: u64,
}

impl ProjectionWorkScope {
    pub(super) fn enter(
        runtime: Arc<Runtime>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        work.admit(crate::WorkCounters::UNCHARGED)?;
        let generation = lock_recover(&runtime.active_work).enter(work)?;
        Ok(Self {
            runtime,
            work: work.clone(),
            generation,
        })
    }

    pub(in crate::native_mount) fn try_clone_for_io(&self) -> Result<Self, NativeMountError> {
        Self::enter(Arc::clone(&self.runtime), &self.work)
    }
}

impl Drop for ProjectionWorkScope {
    fn drop(&mut self) {
        lock_recover(&self.runtime.active_work).leave(&self.work, self.generation);
    }
}

pub(super) fn same_work(
    left: &crate::NativeMaterializationWork,
    right: &crate::NativeMaterializationWork,
) -> bool {
    let mut activation = super::super::NativeMountMeterActivation::default();
    activation.enter(left).is_ok() && activation.enter(right).is_ok()
}

impl Runtime {
    pub(super) fn native_work(
        &self,
        authored: bool,
    ) -> Result<crate::NativeMaterializationWork, NativeMountError> {
        let original = self.source.native_original_work()?;
        match (&*lock_recover(&self.original_work), &original) {
            (Some(installed), Some(current)) if same_work(installed, current) => {}
            (None, None) => {}
            _ => return Err(NativeMountError::Lifecycle("source invocation changed")),
        }
        let guard = self.source.native_callback_guard();
        match (&self.callback_guard, &guard) {
            (Some(installed), Some(current)) if Arc::ptr_eq(installed, current) => {}
            (None, None) => {}
            _ => return Err(NativeMountError::Lifecycle("source authorization changed")),
        }
        let work = lock_recover(&self.active_work)
            .upgrade()
            .unwrap_or_else(|| lock_recover(&self.work).clone());
        if original
            .as_ref()
            .is_some_and(|original| !same_work(original, &work))
        {
            return Err(NativeMountError::Lifecycle(
                "callback invocation differs from its source",
            ));
        }
        work.admit(crate::WorkCounters::UNCHARGED)?;
        if let Some(guard) = guard {
            let callbacks = super::super::adapter::CallbackRuntime::existing()?;
            if authored {
                callbacks.verify_native_callback_write_guard(guard.as_ref(), &work)?;
            } else {
                callbacks.verify_native_callback_guard(guard.as_ref(), &work)?;
            }
        }
        Ok(work)
    }
}

pub(in crate::native_mount) fn detach_work_error(
    error: impl Into<NativeMountError>,
) -> NativeMountError {
    error.into()
}

pub(super) fn native_call<T>(
    work: &crate::NativeMaterializationWork,
    write: bool,
    operation: impl FnOnce() -> std::io::Result<T>,
) -> Result<T, NativeMountError> {
    let delta = if write {
        crate::WorkCounters {
            backend_write_operations: 1,
            ..crate::WorkCounters::UNCHARGED
        }
    } else {
        crate::WorkCounters {
            backend_read_operations: 1,
            ..crate::WorkCounters::UNCHARGED
        }
    };
    work.admit(delta)?;
    work.record_admitted(delta)?;
    operation().map_err(Into::into)
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
