use super::*;

pub use crate::native_mount::shared::metering::NativeMountWorkScope;

impl crate::native_mount::shared::metering::MeteredView for DarwinMountContext {
    fn activate(&self, work: &Work) -> Result<u64, NativeMountError> {
        self.startup_meter
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .enter(work)
    }

    fn deactivate(&self, work: &Work, generation: u64) {
        self.startup_meter
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .leave(work, generation);
    }
}

impl DarwinMountContext {
    pub(super) fn callback_meter(&self) -> Result<Option<Work>, NativeMountError> {
        let active = self
            .startup_meter
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .upgrade();
        crate::native_mount::shared::metering::select(
            self.source.as_ref(),
            active,
            self.source_original_work.as_ref(),
            self.callback_guard.as_ref(),
            None,
            false,
        )
    }
}
