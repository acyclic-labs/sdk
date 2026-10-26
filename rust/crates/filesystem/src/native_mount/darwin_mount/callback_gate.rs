use super::*;
use crate::native_mount::shared::callback_gate::{CallbackGate as SharedGate, CallbackPass};

/// The NFS server's in-flight callbacks, counted by the shared gate. A
/// drain that has to wait is charged one read against the barrier's work.
pub(super) struct CallbackGate(SharedGate);

impl CallbackGate {
    pub(super) const fn new() -> Self {
        Self(SharedGate::new())
    }

    pub(in crate::native_mount) fn enter(&self) -> Option<CallbackPass<'_>> {
        self.0.enter()
    }

    pub(super) fn drain(&self, work: &Work) -> Result<(), NativeMountError> {
        work.admit(WorkCounters::UNCHARGED)?;
        self.0
            .drain(|| work.charge(READ_WORK).map_err(NativeMountError::from))
    }
}
