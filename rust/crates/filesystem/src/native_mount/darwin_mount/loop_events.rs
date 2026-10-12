use super::*;

/// What a mount loop has reported: that the kernel accepted its mount, and that the loop has returned.
#[derive(Default)]
pub(super) struct LoopEvents {
    pub(super) state: Mutex<LoopState>,
    pub(super) changed: Condvar,
    pub(super) _allocation: Option<crate::NativeAllocation>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct LoopState {
    pub(super) mounted: bool,
    pub(super) provider_exited: bool,
}

impl LoopEvents {
    pub(in crate::native_mount) fn report(&self, update: impl FnOnce(&mut LoopState)) {
        update(&mut self.state.lock().unwrap_or_else(PoisonError::into_inner));
        self.changed.notify_all();
    }

    pub(in crate::native_mount) fn current(&self) -> LoopState {
        *self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Waits up to `timeout` for a state `done` accepts, and returns the state reported by then.
    pub(in crate::native_mount) fn wait_until(
        &self,
        timeout: Duration,
        done: impl Fn(LoopState) -> bool,
    ) -> LoopState {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let (state, _) = self
            .changed
            .wait_timeout_while(state, timeout, |state| !done(*state))
            .unwrap_or_else(PoisonError::into_inner);
        *state
    }
}

/// Called by the bridge once the kernel has accepted the mount.
pub(super) unsafe extern "C" fn report_mounted(events: *mut c_void) {
    // SAFETY: the loop owns these events throughout the native callback.
    let events = unsafe { &*events.cast_const().cast::<LoopEvents>() };
    events.report(|state| state.mounted = true);
}
