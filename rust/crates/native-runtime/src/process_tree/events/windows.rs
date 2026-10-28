//! Windows readiness: one I/O completion port. Pipe readiness is an
//! overlapped zero-byte read, stdin progress is an overlapped write, and a
//! process exit is a registered thread-pool wait that posts to the port.

use super::Source;
use std::cell::UnsafeCell;
use std::ffi::c_void;
use std::io;
use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use windows_sys::Win32::Foundation::{
    ERROR_IO_PENDING, HANDLE, INVALID_HANDLE_VALUE, NTSTATUS, RtlNtStatusToDosError, WAIT_TIMEOUT,
};
use windows_sys::Win32::Storage::FileSystem::{
    ReadFile, SetFileCompletionNotificationModes, WriteFile,
};
use windows_sys::Win32::System::IO::{
    CreateIoCompletionPort, GetOverlappedResult, GetQueuedCompletionStatusEx, OVERLAPPED,
    OVERLAPPED_ENTRY, PostQueuedCompletionStatus,
};
use windows_sys::Win32::System::Threading::{
    INFINITE, RegisterWaitForSingleObject, UnregisterWaitEx, WT_EXECUTEINWAITTHREAD,
    WT_EXECUTEONLYONCE,
};

/// `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS | FILE_SKIP_SET_EVENT_ON_HANDLE`.
const SKIP_MODES: u8 = 1 | 2;
const WAKE_KEY: usize = 0;
const EXIT_KEY: usize = 1;
const PIPE_KEY: usize = 2;
/// The largest stdin chunk one overlapped write carries.
const WRITE_CHUNK: usize = 64 * 1024;

/// The port the registered exit callbacks post to.
static PORT: AtomicPtr<c_void> = AtomicPtr::new(std::ptr::null_mut());

pub(super) struct Poller {
    port: OwnedHandle,
}

/// A registered process wait, unregistered when its wait is dropped.
pub(super) struct Registration(Option<HANDLE>);

// SAFETY: a registered wait handle has no thread affinity, and only the one
// owning Registration unregisters it.
unsafe impl Send for Registration {}
// SAFETY: shared access never touches the handle; only Drop does.
unsafe impl Sync for Registration {}

impl Drop for Registration {
    fn drop(&mut self) {
        if let Some(wait) = self.0.take() {
            // SAFETY: this owned registration is released once. A null
            // completion event returns at once; a callback still running
            // posts only a token, which the hub ignores once disarmed.
            unsafe { UnregisterWaitEx(wait, std::ptr::null_mut()) };
        }
    }
}

unsafe extern "system" fn exited(context: *mut c_void, _timed_out: bool) {
    // SAFETY: the port outlives the process, and the context is only the
    // token value; nothing dereferences it.
    unsafe {
        PostQueuedCompletionStatus(PORT.load(Ordering::Acquire), 0, EXIT_KEY, context.cast());
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    ZeroRead,
    Write,
}

/// One pipe's single outstanding overlapped operation.
///
/// While an operation is pending the kernel owns `overlapped` and `buffer`,
/// and the port owns one strong reference, which the hub releases when the
/// completion arrives, even after the pipe was closed and the operation
/// cancelled.
#[repr(C)]
pub(in crate::process_tree) struct PipeOperation {
    overlapped: UnsafeCell<OVERLAPPED>,
    handle: HANDLE,
    kind: Kind,
    pending: AtomicBool,
    token: AtomicU64,
    buffer: UnsafeCell<Vec<u8>>,
    result: Mutex<Option<io::Result<usize>>>,
    write_reservation: Mutex<Option<crate::NativeByteReservation<Arc<dyn crate::NativeReadMeter>>>>,
}

// SAFETY: the kernel and this process share `overlapped` and `buffer` only
// while `pending` is set; no Rust code touches them then, and issuing a new
// operation requires the owning pipe's exclusive writer.
unsafe impl Send for PipeOperation {}
// SAFETY: as above; every other field is atomic or locked.
unsafe impl Sync for PipeOperation {}

fn port() -> io::Result<HANDLE> {
    super::hub().map(|hub| hub.poller.port.as_raw_handle())
}

impl PipeOperation {
    fn associate(handle: HANDLE, kind: Kind) -> io::Result<Arc<Self>> {
        // SAFETY: the caller's live pipe handle joins the hub's owned port.
        if unsafe { CreateIoCompletionPort(handle, port()?, PIPE_KEY, 0) }.is_null() {
            return Err(io::Error::last_os_error());
        }
        // A synchronous success completes inline and posts nothing, and the
        // pipe handle itself is never waited on.
        // SAFETY: the same live pipe handle.
        if unsafe { SetFileCompletionNotificationModes(handle, SKIP_MODES) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Arc::new(Self {
            // SAFETY: an all-zero OVERLAPPED is its documented initial value.
            overlapped: UnsafeCell::new(unsafe { std::mem::zeroed() }),
            handle,
            kind,
            pending: AtomicBool::new(false),
            token: AtomicU64::new(0),
            buffer: UnsafeCell::new(Vec::new()),
            result: Mutex::new(None),
            write_reservation: Mutex::new(None),
        }))
    }

    /// Readiness for reading the pipe behind `handle`.
    pub(in crate::process_tree) fn reader(
        handle: &impl std::os::windows::io::AsRawHandle,
    ) -> io::Result<Arc<Self>> {
        Self::associate(handle.as_raw_handle(), Kind::ZeroRead)
    }

    /// Overlapped writes to the pipe behind `handle`.
    pub(in crate::process_tree) fn writer(
        handle: &impl std::os::windows::io::AsRawHandle,
    ) -> io::Result<Arc<Self>> {
        Self::associate(handle.as_raw_handle(), Kind::Write)
    }

    /// Starts one overlapped transfer: `Ok(Some(n))` when it completed at
    /// once, `Ok(None)` while it is pending.
    fn start(
        self: &Arc<Self>,
        transfer: impl FnOnce(*mut OVERLAPPED) -> i32,
    ) -> io::Result<Option<usize>> {
        // SAFETY: no operation is pending, so this process owns the
        // OVERLAPPED; zero it before handing it to the kernel.
        unsafe { *self.overlapped.get() = std::mem::zeroed() };
        self.pending.store(true, Ordering::SeqCst);
        let owned = Arc::into_raw(Arc::clone(self));
        if transfer(self.overlapped.get()) != 0 {
            self.pending.store(false, Ordering::SeqCst);
            // SAFETY: a synchronous success posts no completion, so the
            // reference handed to the port comes back here.
            drop(unsafe { Arc::from_raw(owned) });
            let mut transferred = 0;
            // SAFETY: the completed OVERLAPPED of this live pipe.
            if unsafe {
                GetOverlappedResult(self.handle, self.overlapped.get(), &raw mut transferred, 0)
            } == 0
            {
                return Err(io::Error::last_os_error());
            }
            return Ok(Some(transferred as usize));
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(ERROR_IO_PENDING.cast_signed()) {
            return Ok(None);
        }
        self.pending.store(false, Ordering::SeqCst);
        // SAFETY: an immediate failure posts no completion either.
        drop(unsafe { Arc::from_raw(owned) });
        Err(error)
    }

    /// Writes at most one chunk of `bytes`: `Ok(Some(n))` when the pipe took
    /// `n` bytes at once, `Ok(None)` while the write is pending. A pending
    /// write's count arrives through [`Self::take_written`].
    pub(in crate::process_tree) fn write(
        self: &Arc<Self>,
        bytes: &[u8],
        reservation: Option<crate::NativeByteReservation<Arc<dyn crate::NativeReadMeter>>>,
    ) -> io::Result<Option<usize>> {
        if self.pending.load(Ordering::SeqCst) {
            return Ok(None);
        }
        let length = Self::write_length(bytes.len());
        // SAFETY: no operation is pending, so this process owns the buffer.
        let buffer = unsafe { &mut *self.buffer.get() };
        buffer.clear();
        buffer.extend_from_slice(bytes.get(..length).unwrap_or(bytes));
        let pointer = buffer.as_ptr();
        let length = u32::try_from(buffer.len()).map_err(io::Error::other)?;
        *self
            .write_reservation
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = reservation;
        let result = self.start(|overlapped| {
            // SAFETY: the buffer and OVERLAPPED stay owned by this operation,
            // whose reference the port holds until the write completes.
            unsafe {
                WriteFile(
                    self.handle,
                    pointer,
                    length,
                    std::ptr::null_mut(),
                    overlapped,
                )
            }
        });
        match result {
            Ok(None) => Ok(None),
            Ok(Some(actual)) => self.finish_write(actual, Ok(actual)).map(Some),
            Err(error) => self.finish_write(0, Err(error)).map(Some),
        }
    }

    pub(in crate::process_tree) fn write_length(length: usize) -> usize {
        length.min(WRITE_CHUNK)
    }

    fn finish_write(&self, actual: usize, result: io::Result<usize>) -> io::Result<usize> {
        // Release the operation lock before visiting the original meter.
        let reservation = self
            .write_reservation
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(mut reservation) = reservation {
            reservation.record(actual)?;
        }
        result
    }

    /// Whether a write is still in flight.
    pub(in crate::process_tree) fn pending(&self) -> bool {
        self.pending.load(Ordering::SeqCst)
    }

    /// The result of a write that completed after it was started.
    pub(in crate::process_tree) fn take_written(&self) -> Option<io::Result<usize>> {
        if self.pending() {
            return None;
        }
        self.result
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    fn arm(self: &Arc<Self>, token: u64) -> io::Result<Option<Registration>> {
        self.token.store(token, Ordering::SeqCst);
        if self.pending() {
            return Ok(Some(Registration(None)));
        }
        if self.kind == Kind::Write {
            // Nothing in flight: the writer has a result or room to write.
            return Ok(None);
        }
        // SAFETY: no operation is pending, so this process owns the buffer.
        let empty = unsafe { (*self.buffer.get()).as_mut_ptr() };
        match self.start(|overlapped| {
            // SAFETY: a zero-byte read never touches its buffer, which this
            // operation owns until the read completes; so does the OVERLAPPED.
            unsafe { ReadFile(self.handle, empty, 0, std::ptr::null_mut(), overlapped) }
        }) {
            Ok(None) => Ok(Some(Registration(None))),
            // Data, end of file or a broken pipe: the reader observes it now.
            Ok(Some(_)) | Err(_) => Ok(None),
        }
    }

    fn complete(&self, entry: &OVERLAPPED_ENTRY) -> u64 {
        if self.kind == Kind::Write {
            let status: NTSTATUS = u32::try_from(entry.Internal & 0xFFFF_FFFF)
                .unwrap_or(u32::MAX)
                .cast_signed();
            let result = if status >= 0 {
                Ok(entry.dwNumberOfBytesTransferred as usize)
            } else {
                // SAFETY: a completed NTSTATUS is a valid input to this conversion.
                let error = unsafe { RtlNtStatusToDosError(status) };
                Err(io::Error::from_raw_os_error(error.cast_signed()))
            };
            // The completion count remains work even when the kernel reports
            // cancellation/error, or nobody polls the writer again.
            let result = self.finish_write(entry.dwNumberOfBytesTransferred as usize, result);
            *self.result.lock().unwrap_or_else(PoisonError::into_inner) = Some(result);
        }
        self.pending.store(false, Ordering::SeqCst);
        self.token.load(Ordering::SeqCst)
    }
}

impl Poller {
    pub(super) fn new() -> io::Result<Self> {
        // SAFETY: a new port with no file and the default concurrency.
        let port =
            unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 1) };
        if port.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a successful call returns one owned port handle.
        let port = unsafe { OwnedHandle::from_raw_handle(port) };
        PORT.store(port.as_raw_handle(), Ordering::Release);
        Ok(Self { port })
    }

    pub(super) fn register(
        &self,
        source: Source<'_>,
        token: u64,
    ) -> io::Result<Option<Registration>> {
        match source {
            Source::Exit(object) => {
                let mut wait: HANDLE = std::ptr::null_mut();
                // SAFETY: the borrowed object is live for this call and the
                // registration duplicates what it needs. The context is the
                // token value, which nothing dereferences.
                if unsafe {
                    RegisterWaitForSingleObject(
                        &raw mut wait,
                        object.as_raw_handle(),
                        Some(exited),
                        std::ptr::without_provenance(usize::try_from(token).unwrap_or(usize::MAX)),
                        INFINITE,
                        WT_EXECUTEONLYONCE | WT_EXECUTEINWAITTHREAD,
                    )
                } == 0
                {
                    return Err(io::Error::last_os_error());
                }
                Ok(Some(Registration(Some(wait))))
            }
            Source::Pipe(operation) => operation.arm(token),
        }
    }

    pub(super) fn notify(&self) -> io::Result<()> {
        // SAFETY: the owned port accepts a packet with no OVERLAPPED.
        if unsafe {
            PostQueuedCompletionStatus(self.port.as_raw_handle(), 0, WAKE_KEY, std::ptr::null())
        } == 0
        {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub(super) fn wait(&self, timeout: Option<Duration>, ready: &mut Vec<u64>) -> io::Result<()> {
        let milliseconds = timeout.map_or(INFINITE, |timeout| {
            u32::try_from(timeout.as_nanos().div_ceil(1_000_000))
                .unwrap_or(INFINITE - 1)
                .min(INFINITE - 1)
        });
        // SAFETY: plain fields; all-zero is a valid output value.
        let mut entries: [OVERLAPPED_ENTRY; 256] = unsafe { std::mem::zeroed() };
        let mut removed = 0;
        // SAFETY: the owned port fills at most the initialized array.
        if unsafe {
            GetQueuedCompletionStatusEx(
                self.port.as_raw_handle(),
                entries.as_mut_ptr(),
                u32::try_from(entries.len()).unwrap_or(u32::MAX),
                &raw mut removed,
                milliseconds,
                0,
            )
        } == 0
        {
            let error = io::Error::last_os_error();
            return if error.raw_os_error() == Some(WAIT_TIMEOUT.cast_signed()) {
                Ok(())
            } else {
                Err(error)
            };
        }
        for entry in entries.iter().take(removed as usize) {
            match entry.lpCompletionKey {
                EXIT_KEY => ready.push(entry.lpOverlapped.addr() as u64),
                PIPE_KEY => {
                    // SAFETY: every pipe packet carries the OVERLAPPED of a
                    // PipeOperation, the first field of that repr(C) struct,
                    // whose strong reference was handed to the port.
                    let operation =
                        unsafe { Arc::from_raw(entry.lpOverlapped.cast::<PipeOperation>()) };
                    ready.push(operation.complete(entry));
                }
                _ => {}
            }
        }
        Ok(())
    }
}
