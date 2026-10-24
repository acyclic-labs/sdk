use super::*;

pub(in crate::native_mount) fn with_native_callback_carrier<T>(
    runtime: &CallbackRuntime,
    carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    run: impl FnOnce(
        &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
        crate::native_mount::adapter::PreparedCallbackWait,
    ) -> Result<T, MountSourceError>,
) -> Result<T, MountSourceError> {
    let work = carrier
        .original_native_work()
        .ok_or(MountSourceError::OriginalSnapshotUnavailable)?;
    let prepared = runtime.prepare_native_wait(&work)?;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(carrier, prepared)));
    let reconciled = carrier
        .with_work(|work| work.record(crate::WorkCounters::default()))
        .map_err(MountSourceError::from);
    match result {
        Err(panic) => std::panic::resume_unwind(panic),
        Ok(Err(error)) => Err(error),
        Ok(Ok(value)) => reconciled.map(|()| value),
    }
}

struct CaptureObserverDispatch {
    gate: usize,
    prior: *const CaptureObserverDispatch,
}

std::thread_local! {
    static CAPTURE_OBSERVER_DISPATCH: std::cell::Cell<*const CaptureObserverDispatch> =
        const { std::cell::Cell::new(std::ptr::null()) };
}

pub(in crate::native_mount) fn with_capture_observer_dispatch<T>(
    gate: usize,
    dispatch: impl FnOnce() -> T,
) -> T {
    CAPTURE_OBSERVER_DISPATCH.with(|current| {
        struct Restore<'a>(
            &'a std::cell::Cell<*const CaptureObserverDispatch>,
            *const CaptureObserverDispatch,
        );
        impl Drop for Restore<'_> {
            fn drop(&mut self) {
                self.0.set(self.1);
            }
        }
        let frame = CaptureObserverDispatch {
            gate,
            prior: current.get(),
        };
        let _restore = Restore(current, frame.prior);
        current.set(std::ptr::from_ref(&frame));
        dispatch()
    })
}

#[allow(
    unsafe_code,
    reason = "thread-local links borrow synchronous dispatch frames until guard restoration"
)]
pub(in crate::native_mount) fn is_current_capture_observer_dispatch(gate: usize) -> bool {
    CAPTURE_OBSERVER_DISPATCH.with(|current| {
        let mut at = current.get();
        while !at.is_null() {
            // SAFETY: each frame is on this thread's live synchronous stack;
            // its unwind guard restores the prior link before that frame dies.
            let frame = unsafe { &*at };
            if frame.gate == gate {
                return true;
            }
            at = frame.prior;
        }
        false
    })
}
