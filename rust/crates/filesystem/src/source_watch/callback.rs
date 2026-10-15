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

impl NativeHostWork for CallbackWork {
    type Error = DemandError;
    type Allocation = u64;
    type CleanupWrite = u64;
    fn io_error_kind(error: &DemandError) -> Option<std::io::ErrorKind> {
        <DemandWork as NativeHostWork>::io_error_kind(error)
    }
    fn allocation_failed(error: std::collections::TryReserveError) -> DemandError {
        DemandError::Allocation(error)
    }
    fn invalid_request() -> DemandError {
        DemandError::InvalidRequest
    }
    fn check(&self) -> Result<(), DemandError> {
        match self {
            Self::Ordinary(work) => work.check(),
            Self::Original(work) => NativeHostWork::check(work),
        }
    }
    fn admit(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.with_original_leaf(|work| work.admit(delta))
    }
    fn record(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.with_original_leaf(|work| work.record(delta))
    }
    fn started_io(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.with_original_leaf(|work| work.charge(delta))
    }
    fn claim(&mut self, bytes: u64) -> Result<u64, DemandError> {
        self.with_original_leaf(|work| NativeHostWork::claim(work, bytes))
    }
    #[cfg(windows)]
    fn reserve_opaque_decode(&mut self, maximum: u64) -> Result<u64, DemandError> {
        self.with_original_leaf(|work| work.reserve_opaque_decode(maximum))
    }
    fn reconcile_allocation(
        &mut self,
        allocation: &mut u64,
        actual: u64,
    ) -> Result<(), DemandError> {
        self.with_original_leaf(|work| work.reconcile_native_allocation(allocation, actual))
    }
    fn release(&mut self, bytes: u64) -> Result<(), DemandError> {
        self.with_original_leaf(|work| work.release(bytes))
    }
    #[cfg(any(windows, target_os = "linux", target_os = "macos", test))]
    fn reserve_cleanup_write(&mut self) -> Result<u64, DemandError> {
        self.with_original_leaf(NativeHostWork::reserve_cleanup_write)
    }
    #[cfg(any(target_os = "linux", all(test, unix)))]
    fn cancel_cleanup_write(&mut self, reservation: u64) {
        self.with_original_leaf(|work| work.cancel_cleanup_write(reservation));
    }
    #[cfg(any(windows, target_os = "linux", target_os = "macos", test))]
    fn attempt_cleanup_write<T>(
        &mut self,
        reservation: u64,
        operation: impl FnOnce() -> T,
    ) -> (T, Result<(), DemandError>) {
        let recorded =
            self.with_original_leaf(|work| work.attempt_cleanup_write(reservation, || ()).1);
        (operation(), recorded)
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
    fn with_original_leaf<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T {
        match self {
            Self::Ordinary(work) => operation(work),
            Self::Original(work) => work.with_original_leaf(operation),
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
