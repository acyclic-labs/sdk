use super::*;

pub(in crate::native_mount) struct DarwinStartupMeterScope {
    pub(super) context: Arc<DarwinMountContext>,
    meter: crate::NativeMaterializationWork,
    generation: u64,
}

impl DarwinStartupMeterScope {
    pub(super) fn new(
        context: Arc<DarwinMountContext>,
        meter: &crate::NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        let generation = context
            .startup_meter
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .enter(meter)?;
        Ok(Self {
            context,
            meter: meter.clone(),
            generation,
        })
    }

    pub(in crate::native_mount) fn try_clone_for_io(&self) -> Result<Self, NativeMountError> {
        Self::new(Arc::clone(&self.context), &self.meter)
    }
}

impl Drop for DarwinStartupMeterScope {
    fn drop(&mut self) {
        self.context
            .startup_meter
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .leave(&self.meter, self.generation);
    }
}

fn same_view(
    left: &crate::NativeMaterializationWork,
    right: &crate::NativeMaterializationWork,
) -> bool {
    left.shares_allocation_context(right)
        && left.capture_origin().map(Weak::as_ptr) == right.capture_origin().map(Weak::as_ptr)
        && left.invocation_view_state().map(Arc::as_ptr)
            == right.invocation_view_state().map(Arc::as_ptr)
        && left.source_execution_view().map(Arc::as_ptr)
            == right.source_execution_view().map(Arc::as_ptr)
}

pub(super) fn original_work(
    source: &dyn MountFilesystem,
    candidate: Option<&crate::NativeMaterializationWork>,
) -> Result<Option<crate::NativeMaterializationWork>, NativeMountError> {
    let original = source.native_original_work()?;
    if let Some(original) = &original {
        if candidate.is_some_and(|candidate| !same_view(original, candidate)) {
            return Err(NativeMountError::ControlPending);
        }
        original.admit(crate::WorkCounters::UNCHARGED)?;
    }
    Ok(original)
}

pub(super) fn verify_guard(
    guard: Option<&Arc<dyn crate::native_mount::NativeMountCallbackGuard>>,
    work: Option<&crate::NativeMaterializationWork>,
) -> Result<(), NativeMountError> {
    if let Some(guard) = guard {
        crate::native_mount::adapter::CallbackRuntime::existing()?.verify_native_callback_guard(
            guard.as_ref(),
            work.ok_or(NativeMountError::ControlPending)?,
        )?;
    }
    Ok(())
}

impl DarwinMountContext {
    pub(super) fn callback_meter(
        &self,
    ) -> Result<Option<crate::NativeMaterializationWork>, NativeMountError> {
        let active = self
            .startup_meter
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .upgrade();
        let selected = active.or_else(|| self.source_original_work.clone());
        let current = original_work(self.source.as_ref(), selected.as_ref())?;
        match (&self.source_original_work, &current) {
            (Some(installed), Some(current)) if same_view(installed, current) => {}
            (None, None) => {}
            _ => return Err(NativeMountError::ControlPending),
        }
        if let Some(work) = &selected {
            work.admit(crate::WorkCounters::UNCHARGED)?;
        }
        let guard = self.source.native_callback_guard();
        match (&self.callback_guard, &guard) {
            (Some(installed), Some(current)) if Arc::ptr_eq(installed, current) => {
                verify_guard(Some(installed), selected.as_ref())?
            }
            (None, None) => {}
            _ => return Err(NativeMountError::ControlPending),
        }
        Ok(selected)
    }
}
