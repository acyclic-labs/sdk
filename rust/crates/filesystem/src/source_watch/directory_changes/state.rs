use super::*;

pub(super) struct Shared {
    pub(super) directory: WatchHandle,
    pub(super) completed: WatchHandle,
    pub(super) delivery: Arc<Delivery>,
    pub(super) extended: AtomicBool,
    pub(super) complete: AtomicBool,
    pub(super) recursive: bool,
    pub(super) reads: Mutex<Reads>,
    pub(super) buffer_owned: AtomicBool,
    pub(super) reading_changed: Condvar,
    pub(super) stopping: AtomicBool,
    pub(super) stop_completed: AtomicBool,
    pub(super) ownership: crate::demand::SourcePayloadOwnership,
    pub(super) native_cleanup: Mutex<[Option<crate::materializer::NativeWorkReservation>; 2]>,
    pub(super) thread: Mutex<Option<OwnedHandle>>,
    pub(super) native_retiring: AtomicBool,
    pub(super) native_entered_retirement: AtomicBool,
    pub(super) native_attempt_finished: AtomicBool,
    pub(super) native_drop_requested: AtomicBool,
    pub(super) native_failure: Mutex<Option<DemandError>>,
    #[cfg(test)]
    pub(super) native_wait_cancellation: Mutex<Option<crate::CancellationToken>>,
}

/// Main's pending read and its immutable native backing storage.
pub(super) struct Reads {
    pub(super) overlapped: Box<OVERLAPPED>,
    pub(super) buffer: Option<Box<std::mem::MaybeUninit<[u64; BUFFER_WORDS]>>>,
    pub(super) pending: bool,
    pub(super) filled: usize,
    pub(super) cursor: usize,
    pub(super) loss: bool,
    pub(super) blocked: bool,
}
// SAFETY: the read lock protects the buffer; kernel writes end before its retirement.
unsafe impl Send for Reads {}

pub(super) struct WatchHandle { raw: AtomicPtr<std::ffi::c_void>, ordinary: bool }
impl WatchHandle {
    pub(super) fn empty(ordinary: bool) -> Self { Self { raw: AtomicPtr::new(std::ptr::null_mut()), ordinary } }
    pub(super) fn install(&self, handle: OwnedHandle) {
        assert!(!self.is_open(), "one handle installation");
        self.raw.store(handle.into_raw_handle(), Ordering::Release);
    }
    pub(super) fn is_open(&self) -> bool { !self.raw.load(Ordering::Acquire).is_null() }
    pub(super) fn close(&self) -> windows::core::Result<()> {
        let handle = self.raw.load(Ordering::Acquire);
        if !handle.is_null() {
            // SAFETY: close is serialized after the read and callback are quiescent.
            unsafe { CloseHandle(HANDLE(handle)) }?;
            self.raw.store(std::ptr::null_mut(), Ordering::Release);
        }
        Ok(())
    }
}
impl std::os::windows::io::AsRawHandle for WatchHandle {
    fn as_raw_handle(&self) -> std::os::windows::io::RawHandle { self.raw.load(Ordering::Acquire) }
}
impl Drop for WatchHandle {
    fn drop(&mut self) {
        assert!(self.ordinary || self.raw.get_mut().is_null(), "native handle requires its retirement owner");
        let _ = self.close();
    }
}
