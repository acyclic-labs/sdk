use super::*;

/// What a mount loop has reported: that the kernel accepted its mount, and
/// that the loop has returned. Mount and teardown block on these reports
/// rather than polling for their effects, so each wakes the moment the loop
/// reports; a poll's sleep is stretched far beyond its interval when the
/// system coalesces a background service's timers.
#[derive(Default)]
pub(super) struct LoopEvents {
    pub(super) state: Mutex<LoopState>,
    pub(super) changed: Condvar,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct LoopState {
    pub(super) mounted: bool,
    pub(super) exited: bool,
}

impl LoopEvents {
    pub(in crate::native_mount) fn report(&self, update: impl FnOnce(&mut LoopState)) {
        update(&mut self.state.lock().unwrap_or_else(PoisonError::into_inner));
        self.changed.notify_all();
    }

    pub(in crate::native_mount) fn current(&self) -> LoopState {
        *self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Waits up to `timeout` for a state `done` accepts, and returns the
    /// state reported by then.
    pub(in crate::native_mount) fn wait_until(&self, timeout: Duration, done: impl Fn(LoopState) -> bool) -> LoopState {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let (state, _) = self
            .changed
            .wait_timeout_while(state, timeout, |state| !done(*state))
            .unwrap_or_else(PoisonError::into_inner);
        *state
    }
}

/// Held by the mount loop thread; reports its return when dropped, so a
/// waiter also wakes if the thread unwinds.
pub(super) struct ExitReport(Arc<LoopEvents>);

impl Drop for ExitReport {
    fn drop(&mut self) {
        self.0.report(|state| state.exited = true);
    }
}

/// Called by the bridge once the kernel has accepted the mount.
pub(super) unsafe extern "C" fn report_mounted(events: *mut c_void) {
    // SAFETY: the loop thread's `ExitReport` keeps these events alive for
    // the whole native loop, which is the only caller.
    let events = unsafe { &*events.cast_const().cast::<LoopEvents>() };
    events.report(|state| state.mounted = true);
}

