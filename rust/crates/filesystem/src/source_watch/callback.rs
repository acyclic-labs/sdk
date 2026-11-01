use super::*;

// Callback custody is thread-local control only, not source authority. Frames
// live on the invoking stack, including nested callbacks of different watches.
struct CallbackFrame {
    delivery: *const Delivery,
    previous: *const CallbackFrame,
}

std::thread_local! {
    static ACTIVE_CALLBACK: std::cell::Cell<*const CallbackFrame> =
        const { std::cell::Cell::new(std::ptr::null()) };
}

impl Drop for CallbackFrame {
    fn drop(&mut self) {
        ACTIVE_CALLBACK.with(|active| active.set(self.previous));
    }
}

impl crate::native_host::DemandWorkContext for CallbackWork {
    fn check_work(&self) -> Result<(), DemandError> {
        match self {
            Self::Ordinary(work) => work.check(),
            Self::Original(work) => NativeHostWork::check(work),
        }
    }
    fn with_demand_work<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T {
        match self {
            Self::Ordinary(work) => operation(work),
            Self::Original(work) => work.with_original_leaf(operation),
        }
    }
}

impl SourceDemandReadWork for CallbackWork {
    #[cfg(windows)]
    fn with_native_read_meter<T>(
        &mut self,
        operation: impl FnOnce(&dyn acyclic_native_runtime::NativeReadMeter) -> T,
    ) -> T {
        match self {
            Self::Ordinary(work) => work.with_native_read_meter(operation),
            Self::Original(work) => work.with_native_read_meter(operation),
        }
    }
    fn with_ordinary_work<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> Option<T> {
        match self {
            Self::Ordinary(work) => Some(operation(work)),
            Self::Original(_) => None,
        }
    }
    fn native_source_work(&self) -> Option<crate::NativeMaterializationWork> {
        match self {
            Self::Ordinary(_) => None,
            Self::Original(work) => work.native_source_work(),
        }
    }
    fn is_native_original(&self) -> bool {
        matches!(self, Self::Original(_))
    }
}

impl Delivery {
    pub(super) fn callback<T>(&self, invoke: impl FnOnce() -> T) -> T {
        let frame = CallbackFrame {
            delivery: self,
            previous: ACTIVE_CALLBACK.with(std::cell::Cell::get),
        };
        ACTIVE_CALLBACK.with(|active| active.set(&frame));
        invoke()
    }

    #[allow(unsafe_code, reason = "reads live thread-local callback frames")]
    pub(super) fn ensure_stop_available(&self) -> Result<(), crate::demand::DemandError> {
        ACTIVE_CALLBACK.with(|active| {
            let mut frame = active.get();
            while !frame.is_null() {
                // SAFETY: this thread's chain points only to live invoking
                // stack frames; unwinding restores the previous frame first.
                let current = unsafe { &*frame };
                if std::ptr::eq(current.delivery, self) {
                    return Err(crate::demand::DemandError::WorkerUnavailable);
                }
                frame = current.previous;
            }
            Ok(())
        })
    }
}
