//! Windows: `ReadDirectoryChangesExW` over the whole source root.
//!
//! The file system reports each change to the directory's notification
//! list before the operation that made it completes: into the pending read
//! if there is one, which completes it, and into the handle's own buffer
//! otherwise, which the next read returns at once. So a fence cancels the
//! pending read, takes whatever it had completed with, and reads again
//! until a read stays pending: every change that completed before the fence
//! began has then been delivered. Only the reader thread issues reads, so
//! their completions always run on a thread that outlives them.
//!
//! The watch holds the root open without sharing its deletion, so while it
//! lives the root can be neither renamed nor removed, and the directory the
//! source holds open stays the one its path names (as does every ancestor,
//! which no open directory beneath lets move). Each report names the entry
//! and, where the file system can say (NTFS can, `ReFS` cannot), its file
//! identity, so a change through one hard link reaches every name of the
//! file; elsewhere the node is identified as on Linux and macOS.
//!
//! NTFS reports a directory's own write time, which creating or removing an
//! entry in it changes, only once the directory is next read. The entry's
//! own report came first and already covered the directory's listing and
//! attributes, so the late one only repeats it.
//!
//! Only some file systems report every change or say that they lost some
//! (see [`crate::native_host::reports_every_change`]); `ReFS` dropped a
//! file's removal without a trace about once in six runs of a create, write,
//! remove sequence. On the others a fence also reports that changes may be
//! undelivered ([`HostChange::Unconfirmed`]): every remembered fact is
//! verified against the source at its next use.
//!
//! The native reader retains the full original resource view through actual
//! cancellation and read completion, then retires its directory, completion
//! event and buffer before publishing terminal state. A live controller still
//! waits for and closes its thread handle. Final controller drop in delivery
//! relinquishes only that handle; the existing reader owns physical cleanup.

#![allow(
    unsafe_code,
    reason = "watches through the Win32 ReadDirectoryChangesW API"
)]

use super::{Delivery, HostChangeWide};
#[cfg(test)]
use super::HostChange;
#[cfg(test)]
use std::ffi::OsString;
use std::io;
use std::mem::size_of;
#[cfg(test)]
use std::os::windows::ffi::OsStringExt as _;
use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, IntoRawHandle as _, OwnedHandle};
#[cfg(test)]
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use crate::demand::{DemandError, DemandWork, SourceDemandReadWork};
use crate::performance::{WorkBudget, WorkCounters};
use windows::Win32::Foundation::{
    ERROR_INVALID_FUNCTION, ERROR_IO_INCOMPLETE, ERROR_NOT_FOUND, ERROR_NOTIFY_ENUM_DIR, ERROR_OPERATION_ABORTED,
    CloseHandle, HANDLE, WAIT_OBJECT_0,
};
use windows::Win32::Storage::FileSystem::{
    FILE_LIST_DIRECTORY, FILE_NOTIFY_CHANGE_ATTRIBUTES,
    FILE_NOTIFY_CHANGE_CREATION, FILE_NOTIFY_CHANGE_DIR_NAME, FILE_NOTIFY_CHANGE_FILE_NAME,
    FILE_NOTIFY_CHANGE_LAST_WRITE, FILE_NOTIFY_CHANGE_SECURITY, FILE_NOTIFY_CHANGE_SIZE,
    FILE_NOTIFY_EXTENDED_INFORMATION, FILE_NOTIFY_INFORMATION, FILE_SHARE_READ, FILE_SHARE_WRITE,
    ReadDirectoryChangesExW, ReadDirectoryNotifyExtendedInformation,
    ReadDirectoryNotifyInformation,
};
#[cfg(test)]
use windows::Win32::Storage::FileSystem::FILE_ACTION_MODIFIED;
use windows::Win32::System::IO::{CancelIoEx, GetOverlappedResult, OVERLAPPED};
use windows::Win32::System::Threading::{
    CreateEventW, CreateThread, INFINITE, SetEvent, WaitForSingleObject,
};

/// Room for many reports per read; a local volume admits more than 64 KiB.
const BUFFER_WORDS: usize = 64 * 1024 / size_of::<u64>();
/// How long a fence waits for the reader before it assumes everything
/// changed instead.
const FENCE_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) struct PlatformWatch {
    shared: Arc<Shared>,
    thread: Mutex<Option<OwnedHandle>>,
}

struct Shared {
    directory: WatchHandle,
    /// Signaled when the pending read completes.
    completed: WatchHandle,
    /// Whether reads report each entry's file identity too, which the file
    /// system decides once.
    extended: AtomicBool,
    /// Whether the file system reports every change or its loss (NTFS).
    complete: bool,
    recursive: bool,
    reads: Mutex<Reads>,
    buffer_owned: AtomicBool,
    reading_changed: Condvar,
    /// Fences the reader thread has completed.
    fenced: Mutex<u64>,
    fence_delivered: Condvar,
    stopping: AtomicBool,
    stop_completed: AtomicBool,
    controller_alive: AtomicBool,
    ownership: crate::demand::SourcePayloadOwnership,
    cleanup: Mutex<(u64, u64)>,
    original_cleanup: bool,
    delivery: Arc<Delivery>,
}

/// The one read in flight. Its buffer and `OVERLAPPED` stay in place until
/// the read completes.
struct Reads {
    overlapped: Box<OVERLAPPED>,
    buffer: Option<Box<std::mem::MaybeUninit<[u64; BUFFER_WORDS]>>>,
    pending: bool,
    filled: usize,
    cursor: usize,
    loss: bool,
    blocked: bool,
    /// Fences requested so far.
    requested: u64,
}

/// Exactly-once native custody, independently retireable after quiescence.
struct WatchHandle(AtomicPtr<std::ffi::c_void>);

impl WatchHandle {
    fn new(handle: OwnedHandle) -> Self { Self(AtomicPtr::new(handle.into_raw_handle())) }
    fn is_open(&self) -> bool { !self.0.load(Ordering::Acquire).is_null() }
    fn close_with_record<W: SourceDemandReadWork>(
        &self, work: &mut W, record: impl FnOnce(&mut W) -> Result<(), DemandError>,
    ) -> Result<(), DemandError> {
        let handle = self.0.load(Ordering::Acquire);
        if handle.is_null() { return Ok(()); }
        // SAFETY: close is serialized after the native reader quiesces.
        let result = unsafe { CloseHandle(HANDLE(handle)) }
            .map_err(io::Error::from).map_err(DemandError::from);
        if result.is_ok() {
            self.0.store(std::ptr::null_mut(), Ordering::Release);
        }
        result.and(record(work))
    }
}

impl std::os::windows::io::AsRawHandle for WatchHandle {
    fn as_raw_handle(&self) -> std::os::windows::io::RawHandle { self.0.load(Ordering::Acquire) }
}

impl Drop for WatchHandle {
    fn drop(&mut self) {
        let handle = *self.0.get_mut();
        if !handle.is_null() {
            // SAFETY: ordinary fallback for a still-owned native handle.
            let _ = unsafe { CloseHandle(HANDLE(handle)) };
        }
    }
}

fn reserve_cleanup<W: SourceDemandReadWork>(work: &mut W, reads: u64, writes: u64) -> Result<(), DemandError> {
    work.with_original_leaf(|work| super::reserve_cleanup_credits(work, WorkCounters {
        backend_read_operations: reads, backend_write_operations: writes, ..WorkCounters::UNCHARGED
    }))
}

fn close_owned_measured<W: SourceDemandReadWork>(
    handle: &mut Option<OwnedHandle>, shared: &Shared, work: &mut W,
) -> Result<(), DemandError> {
    let Some(owned) = handle.as_ref() else { return Ok(()); };
    // SAFETY: ownership remains retryable if native close fails.
    let result = unsafe { CloseHandle(HANDLE(owned.as_raw_handle())) }
        .map_err(io::Error::from).map_err(DemandError::from);
    if result.is_ok() {
        let _ = handle.take().expect("closed native handle").into_raw_handle();
    }
    result.and(shared.record_cleanup(work, 0, 1))
}

// SAFETY: the read's buffer and `OVERLAPPED` are touched only under the
// `reads` lock, and the kernel writes them only while a read is pending.
unsafe impl Send for Reads {}

impl PlatformWatch {
    pub(super) fn start(
        directory: &crate::native_host::HostRoot, delivery: Arc<Delivery>,
    ) -> io::Result<Self> {
        let cancellation = crate::cancellation::CancellationToken::new();
        Self::start_measured(directory, delivery,
            &mut DemandWork::new(WorkBudget::UNBOUNDED, &cancellation)).map_err(ordinary_error)
    }

    pub(super) fn start_measured(
        directory: &crate::native_host::HostRoot, delivery: Arc<Delivery>, work: &mut DemandWork,
    ) -> Result<Self, DemandError> {
        Self::start_with_work(directory, delivery, work)
    }

    pub(super) fn start_with_work<W: SourceDemandReadWork>(
        directory: &crate::native_host::HostRoot, delivery: Arc<Delivery>, work: &mut W,
    ) -> Result<Self, DemandError> {
        Self::start_handle_with_work(directory.directory_handle(), delivery, true, None, true, work)
    }

    pub(super) fn start_directory_measured(
        directory: &cap_std::fs::Dir, delivery: Arc<Delivery>, complete: bool, work: &mut DemandWork,
    ) -> Result<Self, DemandError> {
        Self::start_directory_with_work(directory, delivery, complete, work)
    }

    pub(super) fn start_directory_with_work<W: SourceDemandReadWork>(
        directory: &cap_std::fs::Dir, delivery: Arc<Delivery>, complete: bool, work: &mut W,
    ) -> Result<Self, DemandError> {
        use std::os::windows::io::AsHandle as _;
        Self::start_handle_with_work(directory.as_handle(), delivery, false, Some(complete), false, work)
    }

    pub(super) fn start_held_root_measured(
        directory: &crate::native_host::HostRoot, delivery: Arc<Delivery>,
        complete: bool, recursive: bool, work: &mut DemandWork,
    ) -> Result<Self, DemandError> {
        Self::start_held_root_with_work(directory, delivery, complete, recursive, work)
    }

    pub(super) fn start_held_root_with_work<W: SourceDemandReadWork>(
        directory: &crate::native_host::HostRoot, delivery: Arc<Delivery>,
        complete: bool, recursive: bool, work: &mut W,
    ) -> Result<Self, DemandError> {
        Self::start_handle_with_work(directory.directory_handle(), delivery, false, Some(complete), recursive, work)
    }

    fn start_handle_with_work<W: SourceDemandReadWork>(
        root: std::os::windows::io::BorrowedHandle<'_>, delivery: Arc<Delivery>,
        pin_name: bool, complete: Option<bool>, recursive: bool, work: &mut W,
    ) -> Result<Self, DemandError> {
        work.preflight(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
        let carrier = delivery.prepare_startup_failure(work)?;
        let original_budget = work.with_original_leaf(|work| work.budget);
        let native_original = work.is_native_original();
        let mut shared = None;
        let mut owned_bytes = 0_u64;
        let mut cleanup_reserved = false;
        let ownership = crate::demand::SourcePayloadOwnership::new(work)?;
        let result = (|| {
            // One prepaid suffix covers every possible native startup prefix
            // and remains resident for the original watch's eventual shutdown.
            reserve_cleanup(work, 2, 5)?;
            cleanup_reserved = true;
            let overlapped_bytes = size_of::<OVERLAPPED>() as u64;
            work.allocate(overlapped_bytes)?;
            owned_bytes += overlapped_bytes;
            let overlapped = Box::new(OVERLAPPED::default());
            let buffer_bytes = size_of::<[u64; BUFFER_WORDS]>() as u64;
            work.allocate(buffer_bytes)?;
            owned_bytes += buffer_bytes;
            let buffer = Box::<[u64; BUFFER_WORDS]>::new_uninit();
            let shared_bytes = super::arc_control_bytes::<Shared>();
            work.allocate(shared_bytes)?;
            owned_bytes += shared_bytes;
            ownership.retain(owned_bytes, work)?;
            shared = Some(Arc::new(Shared {
                directory: WatchHandle(AtomicPtr::new(std::ptr::null_mut())),
                completed: WatchHandle(AtomicPtr::new(std::ptr::null_mut())), delivery,
                extended: AtomicBool::new(true), complete: complete.unwrap_or(false), recursive,
                reads: Mutex::new(Reads {
                    overlapped, buffer: Some(buffer), pending: false, requested: 0,
                    filled: 0, cursor: 0, loss: false, blocked: false,
                }),
                buffer_owned: AtomicBool::new(true),
                reading_changed: Condvar::new(), fenced: Mutex::new(0),
                fence_delivered: Condvar::new(), stopping: AtomicBool::new(false),
                stop_completed: AtomicBool::new(false),
                controller_alive: AtomicBool::new(true),
                cleanup: Mutex::new(if native_original { (2, 5) } else { (0, 0) }),
                original_cleanup: native_original, ownership,
            }));
            let state = Arc::get_mut(shared.as_mut().expect("prepared read state"))
                .expect("startup has sole custody before callback exposure");
            state.directory = WatchHandle::new(open_for_changes_handle(root, pin_name, work)?);
            if complete.is_none() {
                state.complete = crate::native_host::reports_every_change_with_work(&state.directory, work)?;
            }
            work.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::default() })?;
            // SAFETY: an unnamed manual-reset event, exclusively owned here.
            let event = unsafe { CreateEventW(None, true, false, None) }.map_err(io::Error::from)?;
            state.completed = WatchHandle::new(unsafe { OwnedHandle::from_raw_handle(event.0) });
            let reader = shared.as_ref().expect("constructed read state");
            let mut reads = reader.lock_reads();
            if let Err(error) = reader.issue_read(&mut reads, Some(&mut *work)) {
                if !matches!(&error, DemandError::Io(error)
                    if error.raw_os_error() == Some(ERROR_INVALID_FUNCTION.to_hresult().0))
                { return Err(error); }
                reader.extended.store(false, Ordering::Release);
                reader.issue_read(&mut reads, Some(&mut *work))?;
            }
            drop(reads);
            work.preflight(WorkCounters { backend_read_operations: 1, backend_write_operations: 1, ..WorkCounters::default() })?;
            work.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::default() })?;
            let parameter = Arc::into_raw(Arc::clone(reader));
            // SAFETY: successful native creation transfers exactly this Arc.
            let thread = match unsafe {
                CreateThread(None, 0, Some(reader_thread), Some(parameter.cast()), Default::default(), None)
            } {
                Ok(thread) => unsafe { OwnedHandle::from_raw_handle(thread.0) },
                Err(error) => {
                    // SAFETY: no native callback was created.
                    drop(unsafe { Arc::from_raw(parameter) });
                    return Err(io::Error::from(error).into());
                }
            };
            Ok(Self {
                shared: shared.take().expect("constructed read state"),
                thread: Mutex::new(Some(thread)),
            })
        })();
        // Ordinary invocation custody is transferred as before; native custody
        // keeps the original shutdown credits withheld until actual attempts.
        if !native_original {
            work.with_original_leaf(|work| work.budget = original_budget);
        } else if shared.is_none() && result.is_err() && cleanup_reserved {
            work.with_original_leaf(|work| super::cancel_cleanup_credits(work, WorkCounters {
                backend_read_operations: 2, backend_write_operations: 5, ..WorkCounters::UNCHARGED
            }));
        }
        let result = match result {
            Ok(watch) => Ok(watch),
            Err(error) => {
                if let Some(shared) = shared {
                    let failed = Self { shared, thread: Mutex::new(None) };
                    let cleanup = failed.stop_engine(work, true);
                    if !failed.is_stopped() {
                        work.release(owned_bytes)?;
                        if let Some(carrier) = carrier {
                            return Err(carrier.retain_failed_platform(error, failed));
                        }
                        return Err(cleanup.err().unwrap_or(error));
                    }
                    cleanup.and(Err(error))
                } else {
                    Err(error)
                }
            }
        };
        work.release(owned_bytes)?;
        result
    }

    pub(super) fn owned_bytes(&self) -> u64 {
        self.logical_owned_bytes()
    }

    pub(super) fn stop_measured(&self, work: &mut DemandWork) -> Result<(), DemandError> {
        self.stop_with_work(work)
    }

    pub(super) fn stop_with_work<W: SourceDemandReadWork>(
        &self, work: &mut W,
    ) -> Result<(), DemandError> {
        self.stop_engine(work, false)
    }

    fn stop_engine<W: SourceDemandReadWork>(
        &self, work: &mut W, retiring: bool,
    ) -> Result<(), DemandError> {
        if !retiring { work.check()?; }
        self.shared.delivery.ensure_stop_available()?;
        let mut thread = self.thread.lock().unwrap_or_else(PoisonError::into_inner);
        let mut reads = self.shared.lock_reads();
        let running = thread.is_some();
        let pending = reads.pending;
        let required_reads = u64::from(pending) + u64::from(running);
        let required_writes = u64::from(pending) + u64::from(running) * 2
            + u64::from(self.shared.directory.is_open()) + u64::from(self.shared.completed.is_open());
        if self.shared.birth_work(work) {
            let mut cleanup = self.shared.cleanup.lock().unwrap_or_else(PoisonError::into_inner);
            let reads = required_reads.saturating_sub(cleanup.0);
            let writes = required_writes.saturating_sub(cleanup.1);
            if !retiring && (reads != 0 || writes != 0) {
                reserve_cleanup(work, reads, writes)?;
                cleanup.0 += reads;
                cleanup.1 += writes;
            }
        } else if !retiring {
            work.preflight(WorkCounters {
                backend_write_operations: required_writes, backend_read_operations: required_reads,
                ..WorkCounters::default()
            })?;
        }
        // Explicit stop admits the complete suffix. Final retirement cannot
        // decline physical cleanup; both paths record every actual operation
        // and continue through late accounting failures. Native completion
        // and thread join assume eventual OS liveness, not a hard time bound.
        let mut result = Ok(());
        self.shared.stopping.store(true, Ordering::Release);
        self.shared.delivery.exact.store(false, Ordering::Release);
        if pending {
            // SAFETY: cancellation/completion retain the original buffers.
            if let Err(error) = unsafe {
                CancelIoEx(HANDLE(self.shared.directory.as_raw_handle()), Some(&raw const *reads.overlapped))
            } {
                if error.code() != ERROR_NOT_FOUND.to_hresult() {
                    result = Err(DemandError::from(io::Error::from(error)));
                }
            }
            result = result.and(self.shared.record_cleanup(work, 0, 1));
            let mut transferred = 0;
            // SAFETY: the original OVERLAPPED and buffer remain live through completion.
            let completed = unsafe { GetOverlappedResult(HANDLE(self.shared.directory.as_raw_handle()),
                &raw const *reads.overlapped, &mut transferred, true) };
            reads.pending = completed.as_ref().err()
                .is_some_and(|error| error.code() == ERROR_IO_INCOMPLETE.to_hresult());
            if let Err(error) = completed {
                if error.code() != ERROR_OPERATION_ABORTED.to_hresult() {
                    result = result.and(Err(DemandError::from(io::Error::from(error))));
                }
            }
            result = result.and(self.shared.record_cleanup(work, 1, 0));
        }
        if running && self.shared.completed.is_open() {
            // SAFETY: the existing completion event wakes the original reader.
            let signalled = unsafe { SetEvent(HANDLE(self.shared.completed.as_raw_handle())) }
                .map_err(io::Error::from).map_err(DemandError::from);
            result = result.and(signalled);
            result = result.and(self.shared.record_cleanup(work, 0, 1));
        }
        self.shared.reading_changed.notify_all();
        drop(reads);
        if let Some(handle) = thread.as_ref() {
            // SAFETY: the thread handle stays live through actual quiescence.
            let joined = unsafe { WaitForSingleObject(HANDLE(handle.as_raw_handle()), INFINITE) };
            let join_error = (joined != WAIT_OBJECT_0)
                .then(|| DemandError::from(io::Error::last_os_error()));
            result = result.and(self.shared.record_cleanup(work, 1, 0));
            if let Some(error) = join_error {
                // Without proven quiescence, every original handle and buffer
                // remains owned for retry; no native-visible storage is freed.
                return result.and(Err(error));
            }
            result = result.and(close_owned_measured(&mut thread, &self.shared, work));
        }
        let mut reads = self.shared.lock_reads();
        if reads.pending {
            return result.and(Err(io::Error::from(io::ErrorKind::WouldBlock).into()));
        }
        result = result.and(self.shared.directory.close_with_record(work, |work| self.shared.record_cleanup(work, 0, 1)));
        result = result.and(self.shared.completed.close_with_record(work, |work| self.shared.record_cleanup(work, 0, 1)));
        if let Some(buffer) = reads.buffer.take() {
            drop(buffer);
            result = result.and(self.shared.ownership.retire(size_of::<[u64; BUFFER_WORDS]>() as u64));
        }
        self.shared.buffer_owned.store(false, Ordering::Release);
        reads.filled = 0;
        reads.cursor = 0;
        self.shared.cancel_cleanup(work);
        if thread.is_none() && !self.shared.directory.is_open() && !self.shared.completed.is_open() {
            self.shared.stop_completed.store(true, Ordering::Release);
        }
        result
    }

    pub(super) fn is_stopped(&self) -> bool {
        self.shared.stop_completed.load(Ordering::Acquire)
    }

    pub(super) fn logical_owned_bytes(&self) -> u64 {
        super::arc_control_bytes::<Shared>() + size_of::<OVERLAPPED>() as u64
            + if self.shared.buffer_owned.load(Ordering::Acquire) { size_of::<[u64; BUFFER_WORDS]>() as u64 } else { 0 }
    }

    pub(super) fn fence(&self) -> io::Result<()> {
        self.fence_for(FENCE_TIMEOUT)
    }

    pub(super) fn fence_for(&self, timeout: Duration) -> io::Result<()> {
        let mut work = self.shared.delivery.callback_work();
        self.fence_with_work(timeout, &mut work).map(|_| ()).map_err(ordinary_error)
    }

    pub(super) fn fence_measured(
        &self, timeout: Duration, work: &mut DemandWork,
    ) -> Result<bool, DemandError> {
        self.fence_with_work(timeout, work)
    }

    pub(super) fn fence_with_work<W: SourceDemandReadWork>(
        &self, timeout: Duration, work: &mut W,
    ) -> Result<bool, DemandError> {
        work.check()?;
        if self.shared.stopping.load(Ordering::Acquire) {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe).into());
        }
        self.shared.delivery.take_failure(work)?;
        if timeout.is_zero() {
            self.shared.delivery.deliver_wide_with_work(&[HostChangeWide::Everything], work)?;
            return Ok(false);
        }
        let started = std::time::Instant::now();
        let mut reads = self.shared.lock_reads();
        reads.requested = reads.requested.checked_add(1).ok_or(crate::WorkError::Overflow)?;
        let fence = reads.requested;
        let original_budget = work.with_original_leaf(|work| work.budget);
        let result = (|| {
            if reads.pending {
                work.preflight(WorkCounters {
                    backend_write_operations: 1, backend_read_operations: 1,
                    source_bytes_read: size_of::<[u64; BUFFER_WORDS]>() as u64,
                    ..WorkCounters::default()
                })?;
                work.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::default() })?;
                // SAFETY: completes only the original queue's pending read.
                let _ = unsafe { CancelIoEx(HANDLE(self.shared.directory.as_raw_handle()),
                    Some(&raw const *reads.overlapped)) };
                self.shared.finish_read_measured(&mut reads, true, true, work)?;
            }
            self.shared.drain_measured(&mut reads, Some((started, timeout)), work)
        })();
        work.with_original_leaf(|work| work.budget = original_budget);
        reads.blocked = !matches!(result, Ok(true));
        if !reads.blocked {
            let mut fenced = self.shared.fenced.lock().unwrap_or_else(PoisonError::into_inner);
            *fenced = (*fenced).max(fence);
            self.shared.fence_delivered.notify_all();
            self.shared.reading_changed.notify_all();
        }
        drop(reads);
        if matches!(result, Ok(false)) {
            self.shared.delivery.deliver_wide_with_work(&[HostChangeWide::Everything], work)?;
        } else if matches!(result, Ok(true)) && !self.shared.complete {
            self.shared.delivery.deliver_wide_with_work(&[HostChangeWide::Unconfirmed], work)?;
            return Ok(false);
        }
        result
    }

    pub(super) fn delivery(&self) -> &Delivery {
        &self.shared.delivery
    }

    /// Holds delivery back while the returned guard lives.
    #[cfg(test)]
    pub(super) fn paused(&self) -> impl Sized + '_ {
        self.shared.lock_reads()
    }
}

impl Drop for PlatformWatch {
    fn drop(&mut self) {
        if self.is_stopped() && self.shared.delivery.original.is_none() { return; }
        if self.shared.delivery.original.is_some() && self.shared.delivery.ensure_stop_available().is_err() {
            self.shared.controller_alive.store(false, Ordering::Release);
            self.shared.stopping.store(true, Ordering::Release);
            self.shared.reading_changed.notify_all();
            let mut work = self.shared.delivery.callback_work();
            let thread = self.thread.get_mut().unwrap_or_else(PoisonError::into_inner);
            // Closing this controller's thread handle is bookkeeping, not a
            // claim of quiescence. The running reader retains actual Shared.
            if let Err(error) = close_owned_measured(thread, &self.shared, &mut work) {
                self.shared.delivery.failed_with_work(error, &mut work);
            }
            return;
        }
        let mut work = self.shared.delivery.callback_work();
        if let Err(error) = self.stop_engine(&mut work, true) {
            self.shared.delivery.failed_with_work(error, &mut work);
        }
        if self.shared.delivery.original.is_some() {
            let mut cleanup = self.shared.cleanup.lock().unwrap_or_else(PoisonError::into_inner);
            self.shared.controller_alive.store(false, Ordering::Release);
            if self.shared.stop_completed.load(Ordering::Acquire) {
                let (reads, writes) = std::mem::take(&mut *cleanup);
                work.with_original_leaf(|work| super::cancel_cleanup_credits(work, WorkCounters {
                    backend_read_operations: reads, backend_write_operations: writes,
                    ..WorkCounters::UNCHARGED
                }));
            }
        }
    }
}

unsafe extern "system" fn reader_thread(parameter: *mut std::ffi::c_void) -> u32 {
    // SAFETY: successful CreateThread transfers one Arc::into_raw reference.
    let reader = unsafe { Arc::<Shared>::from_raw(parameter.cast()) };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| reader.run()));
    let status = if result.is_err() {
        reader.stopping.store(true, Ordering::Release);
        {
            let mut reads = reader.lock_reads();
            if reads.pending {
                // SAFETY: cancellation and completion happen while both kernel
                // buffers remain alive; no decoding or sink callback is needed.
                unsafe {
                    let _ = CancelIoEx(HANDLE(reader.directory.as_raw_handle()),
                        Some(&raw const *reads.overlapped));
                    let mut transferred = 0;
                    let _ = GetOverlappedResult(HANDLE(reader.directory.as_raw_handle()),
                        &raw const *reads.overlapped, &mut transferred, true);
                }
                reads.pending = false;
                let mut work = reader.delivery.callback_work();
                let _ = reader.record_cleanup(&mut work, 1, 1);
            }
        }
        // A faulty sink cannot unwind through the native callback either.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| reader.delivery.abandon()));
        let mut fenced = reader.fenced.lock().unwrap_or_else(PoisonError::into_inner);
        *fenced = u64::MAX;
        reader.fence_delivered.notify_all();
        1
    } else {
        0
    };
    if reader.delivery.original.is_some() { reader.finish_native_reader(); }
    status
}

impl Shared {
    fn finish_native_reader(&self) {
        self.stopping.store(true, Ordering::Release);
        let mut work = self.delivery.callback_work();
        let mut reads = self.lock_reads();
        let mut result = Ok(());
        if reads.pending {
            // SAFETY: the worker retains its actual kernel-visible buffers
            // through cancellation and synchronous completion.
            let cancelled = unsafe { CancelIoEx(HANDLE(self.directory.as_raw_handle()),
                Some(&raw const *reads.overlapped)) };
            if let Err(error) = cancelled {
                if error.code() != ERROR_NOT_FOUND.to_hresult() {
                    result = Err(DemandError::from(io::Error::from(error)));
                }
            }
            result = result.and(self.record_cleanup(&mut work, 0, 1));
            let mut transferred = 0;
            let completed = unsafe { GetOverlappedResult(HANDLE(self.directory.as_raw_handle()),
                &raw const *reads.overlapped, &mut transferred, true) };
            reads.pending = completed.as_ref().err()
                .is_some_and(|error| error.code() == ERROR_IO_INCOMPLETE.to_hresult());
            if let Err(error) = completed {
                if error.code() != ERROR_OPERATION_ABORTED.to_hresult() {
                    result = result.and(Err(DemandError::from(io::Error::from(error))));
                }
            }
            result = result.and(self.record_cleanup(&mut work, 1, 0));
        }
        if reads.pending {
            self.delivery.failed_with_work(io::Error::from(io::ErrorKind::WouldBlock).into(), &mut work);
            return;
        }
        result = result.and(self.directory.close_with_record(&mut work, |work| self.record_cleanup(work, 0, 1)));
        result = result.and(self.completed.close_with_record(&mut work, |work| self.record_cleanup(work, 0, 1)));
        if let Some(buffer) = reads.buffer.take() {
            drop(buffer);
            result = result.and(self.ownership.retire(size_of::<[u64; BUFFER_WORDS]>() as u64));
        }
        self.buffer_owned.store(false, Ordering::Release);
        reads.filled = 0;
        reads.cursor = 0;
        if let Err(error) = result { self.delivery.failed_with_work(error, &mut work); }
        self.delivery.exact.store(false, Ordering::Release);
        let mut cleanup = self.cleanup.lock().unwrap_or_else(PoisonError::into_inner);
        let alive = u64::from(self.controller_alive.load(Ordering::Acquire));
        let reads = cleanup.0.min(alive);
        let writes = cleanup.1.min(alive * 2);
        work.with_original_leaf(|work| super::cancel_cleanup_credits(work, WorkCounters {
            backend_read_operations: cleanup.0 - reads, backend_write_operations: cleanup.1 - writes,
            ..WorkCounters::UNCHARGED
        }));
        *cleanup = (reads, writes);
        if !self.directory.is_open() && !self.completed.is_open() {
            self.stop_completed.store(true, Ordering::Release);
        }
        self.reading_changed.notify_all();
        self.fence_delivered.notify_all();
    }

    fn birth_work<W: SourceDemandReadWork>(&self, work: &W) -> bool {
        self.original_cleanup && self.delivery.birth_matches(work)
    }

    fn record_cleanup<W: SourceDemandReadWork>(&self, work: &mut W, reads: u64, writes: u64) -> Result<(), DemandError> {
        let delta = WorkCounters { backend_read_operations: reads, backend_write_operations: writes, ..WorkCounters::default() };
        if !self.birth_work(work) { return work.record(delta); }
        let mut cleanup = self.cleanup.lock().unwrap_or_else(PoisonError::into_inner);
        if cleanup.0 < reads || cleanup.1 < writes { return work.record(delta); }
        cleanup.0 -= reads;
        cleanup.1 -= writes;
        work.with_original_leaf(|work| super::record_cleanup_credits(work, delta))
    }

    fn cancel_cleanup<W: SourceDemandReadWork>(&self, _work: &mut W) {
        let mut cleanup = self.cleanup.lock().unwrap_or_else(PoisonError::into_inner);
        let (reads, writes) = std::mem::take(&mut *cleanup);
        if let Some(birth) = &self.delivery.original {
            crate::demand::NativeSourceWork::from_original_work(birth).with_original_leaf(|work| {
                super::cancel_cleanup_credits(work, WorkCounters {
                    backend_read_operations: reads, backend_write_operations: writes,
                    ..WorkCounters::UNCHARGED
                });
            });
        }
    }

    fn lock_reads(&self) -> MutexGuard<'_, Reads> {
        self.reads.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn run(&self) {
        loop {
            if self.stopping.load(Ordering::Acquire) { return; }
            // The original service owns this idle native wait; callback work
            // begins when the queue is ready, not once per idle timeout.
            unsafe { WaitForSingleObject(HANDLE(self.completed.as_raw_handle()), INFINITE) };
            let mut reads = self.lock_reads();
            while reads.blocked && !self.stopping.load(Ordering::Acquire) {
                reads = self.reading_changed.wait(reads).unwrap_or_else(PoisonError::into_inner);
            }
            if self.stopping.load(Ordering::Acquire) {
                if self.delivery.original.is_none() && reads.pending {
                    // SAFETY: ordinary lifetime cleanup also completes its
                    // original buffers before their owner can release them.
                    unsafe {
                        let _ = CancelIoEx(HANDLE(self.directory.as_raw_handle()),
                            Some(&raw const *reads.overlapped));
                        let mut transferred = 0;
                        let _ = GetOverlappedResult(HANDLE(self.directory.as_raw_handle()),
                            &raw const *reads.overlapped, &mut transferred, true);
                    }
                    reads.pending = false;
                }
                return;
            }
            let mut work = self.delivery.callback_work();
            match self.drain_measured(&mut reads, None, &mut work) {
                Ok(_) => {}
                Err(error) => {
                    reads.blocked = true;
                    self.delivery.failed_with_work(error, &mut work);
                }
            }
        }
    }


    fn issue_read<W: SourceDemandReadWork>(&self, reads: &mut Reads, work: Option<&mut W>) -> Result<(), DemandError> {
        let Reads { overlapped, buffer, .. } = reads;
        let buffer = buffer.as_mut().ok_or_else(|| io::Error::from(io::ErrorKind::BrokenPipe))?;
        **overlapped = OVERLAPPED {
            hEvent: HANDLE(self.completed.as_raw_handle()),
            ..OVERLAPPED::default()
        };
        if let Some(work) = work {
            work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
        }
        // SAFETY: the buffer and OVERLAPPED remain live until completion.
        unsafe {
            ReadDirectoryChangesExW(
                HANDLE(self.directory.as_raw_handle()),
                buffer.as_mut_ptr().cast(),
                size_of::<[u64; BUFFER_WORDS]>() as u32,
                self.recursive,
                FILE_NOTIFY_CHANGE_FILE_NAME | FILE_NOTIFY_CHANGE_DIR_NAME
                    | FILE_NOTIFY_CHANGE_ATTRIBUTES | FILE_NOTIFY_CHANGE_SIZE
                    | FILE_NOTIFY_CHANGE_LAST_WRITE | FILE_NOTIFY_CHANGE_CREATION
                    | FILE_NOTIFY_CHANGE_SECURITY,
                None,
                Some(&raw mut **overlapped),
                None,
                if self.extended.load(Ordering::Acquire) {
                    ReadDirectoryNotifyExtendedInformation
                } else {
                    ReadDirectoryNotifyInformation
                },
            )
        }.map_err(io::Error::from)?;
        reads.pending = true;
        Ok(())
    }

    /// Completes the same OVERLAPPED and preserves its original page cursor.
    fn finish_read_measured<W: SourceDemandReadWork>(
        &self, reads: &mut Reads, wait: bool, mandatory: bool, work: &mut W,
    ) -> Result<bool, DemandError> {
        if !reads.pending { return Ok(true); }
        if !mandatory {
            work.preflight(WorkCounters {
                source_bytes_read: size_of::<[u64; BUFFER_WORDS]>() as u64,
                ..WorkCounters::default()
            })?;
        }
        let delta = WorkCounters { backend_read_operations: 1, ..WorkCounters::default() };
        let recorded = if mandatory { Some(work.record(delta)) } else { work.charge(delta)?; None };
        let mut transferred = 0;
        // SAFETY: the original owned buffers stay pinned through completion.
        let result = unsafe { GetOverlappedResult(HANDLE(self.directory.as_raw_handle()),
            &raw const *reads.overlapped, &mut transferred, wait) };
        match result {
            Err(error) if error.code() == ERROR_IO_INCOMPLETE.to_hresult() => return recorded.unwrap_or(Ok(())).map(|_| false),
            Err(error) if error.code() == ERROR_OPERATION_ABORTED.to_hresult() => {
                reads.pending = false;
                return recorded.unwrap_or(Ok(())).map(|_| true);
            }
            Err(error) if error.code() == ERROR_NOTIFY_ENUM_DIR.to_hresult() => {
                reads.loss = true;
            }
            Err(error) => {
                reads.pending = false;
                return Err(io::Error::from(error).into());
            }
            Ok(()) => {
                reads.filled = (transferred as usize).min(size_of::<[u64; BUFFER_WORDS]>());
                reads.cursor = 0;
                reads.loss |= transferred == 0 || transferred as usize > size_of::<[u64; BUFFER_WORDS]>();
                reads.pending = false;
                work.record(WorkCounters { source_bytes_read: reads.filled as u64, ..WorkCounters::default() })?;
            }
        }
        reads.pending = false;
        if let Some(recorded) = recorded { recorded?; }
        Ok(true)
    }

    fn drain_measured<W: SourceDemandReadWork>(
        &self, reads: &mut Reads, deadline: Option<(std::time::Instant, Duration)>, work: &mut W,
    ) -> Result<bool, DemandError> {
        loop {
            work.check()?;
            if deadline.is_some_and(|(started, timeout)| started.elapsed() >= timeout) { return Ok(false); }
            if reads.loss {
                self.delivery.deliver_wide_with_work(&[HostChangeWide::Everything], work)?;
                reads.loss = false;
            }
            if !self.deliver_page_measured(reads, deadline, work)? { return Ok(false); }
            if reads.pending {
                if !self.finish_read_measured(reads, false, false, work)? {
                    return Ok(self.delivery.is_exact());
                }
                continue;
            }
            // Admit mandatory native cancellation/completion before issuing
            // a new read. Unused cleanup allowance is never reported as work.
            let budget = work.with_original_leaf(|work| work.budget);
            reserve_cleanup(work, 1, 1)?;
            let issued = (|| {
                work.preflight(WorkCounters {
                    source_bytes_read: size_of::<[u64; BUFFER_WORDS]>() as u64,
                    ..WorkCounters::default()
                })?;
                self.issue_read(reads, Some(&mut *work))?;
                self.finish_read_measured(reads, false, false, work)
            })();
            work.with_original_leaf(|work| work.budget = budget);
            match issued {
                Ok(false) => return Ok(self.delivery.is_exact()),
                Ok(true) => {}
                Err(error) => {
                    // Keep the kernel's original page and pending request in
                    // Shared custody. The reader is fail-closed by its caller;
                    // a later admitted fence or physical stop resumes it.
                    // No borrowed work facade is stored with the operation.
                    return Err(error);
                }
            }
        }
    }

    /// Uses the existing flexible fields, borrowing aligned UTF-16 directly
    /// from the original native u64 page instead of constructing an OsString.
    fn deliver_page_measured<W: SourceDemandReadWork>(
        &self, reads: &mut Reads, deadline: Option<(std::time::Instant, Duration)>, work: &mut W,
    ) -> Result<bool, DemandError> {
        let extended = self.extended.load(Ordering::Acquire);
        let (name_offset, length_offset) = if extended {
            (std::mem::offset_of!(FILE_NOTIFY_EXTENDED_INFORMATION, FileName),
                std::mem::offset_of!(FILE_NOTIFY_EXTENDED_INFORMATION, FileNameLength))
        } else {
            (std::mem::offset_of!(FILE_NOTIFY_INFORMATION, FileName),
                std::mem::offset_of!(FILE_NOTIFY_INFORMATION, FileNameLength))
        };
        while reads.cursor < reads.filled {
            if deadline.is_some_and(|(started, timeout)| started.elapsed() >= timeout) { return Ok(false); }
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
            // SAFETY: only the completed page is exposed, under the read lock.
            let buffer = reads.buffer.as_ref().ok_or_else(|| io::Error::from(io::ErrorKind::BrokenPipe))?;
            let bytes = unsafe { std::slice::from_raw_parts(buffer.as_ptr().cast::<u8>(), reads.filled) };
            let record = &bytes[reads.cursor..];
            let field = |at: usize| record.get(at..at + 4)
                .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok()).map(u32::from_le_bytes);
            let parsed = (|| {
                let next = field(0)? as usize;
                let action = field(4)?;
                let length = field(length_offset)? as usize;
                let name = record.get(name_offset..name_offset.checked_add(length)?)?;
                if length % 2 != 0 || name.as_ptr().align_offset(std::mem::align_of::<u16>()) != 0 {
                    return None;
                }
                let file = if extended {
                    let at = std::mem::offset_of!(FILE_NOTIFY_EXTENDED_INFORMATION, FileId);
                    Some(u64::from_le_bytes(record.get(at..at + 8)?.try_into().ok()?))
                } else { None };
                if next != 0 && (next < name_offset + length || next > record.len()) { return None; }
                // SAFETY: native page/name offsets are aligned for UTF-16, and
                // the checked byte span contains exactly length / 2 units.
                let path = unsafe { std::slice::from_raw_parts(name.as_ptr().cast::<u16>(), length / 2) };
                Some((next, action, path, file))
            })();
            let Some((next, action, path, file)) = parsed else {
                self.delivery.deliver_wide_with_work(&[HostChangeWide::Everything], work)?;
                reads.cursor = reads.filled;
                return Ok(true);
            };
            let changes = [HostChangeWide::Path { action, path }, HostChangeWide::File(file.unwrap_or(0))];
            self.delivery.deliver_wide_with_work(&changes[..if file.is_some() { 2 } else { 1 }], work)?;
            reads.cursor = if next == 0 { reads.filled } else { reads.cursor + next };
        }
        Ok(true)
    }
}

/// Opens the held root itself again (an empty name relative to it), for
/// asynchronous change reads, pinning its name while it is open.
fn open_for_changes_handle<W: SourceDemandReadWork>(
    root: std::os::windows::io::BorrowedHandle<'_>,
    pin_name: bool,
    work: &mut W,
) -> Result<OwnedHandle, DemandError> {
    use windows::Wdk::Foundation::OBJECT_ATTRIBUTES;
    use windows::Wdk::Storage::FileSystem::{
        FILE_DIRECTORY_FILE, FILE_OPEN, FILE_OPEN_FOR_BACKUP_INTENT, NtCreateFile,
    };
    use windows::Win32::Foundation::{RtlNtStatusToDosError, UNICODE_STRING};
    use windows::Win32::Storage::FileSystem::FILE_FLAGS_AND_ATTRIBUTES;
    use windows::Win32::System::IO::IO_STATUS_BLOCK;

    let name = UNICODE_STRING::default();
    let attributes = OBJECT_ATTRIBUTES {
        Length: u32::try_from(size_of::<OBJECT_ATTRIBUTES>()).unwrap_or(u32::MAX),
        RootDirectory: HANDLE(root.as_raw_handle()),
        ObjectName: &raw const name,
        ..OBJECT_ATTRIBUTES::default()
    };
    let mut handle = HANDLE::default();
    let mut status_block = IO_STATUS_BLOCK::default();
    work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
    // SAFETY: every pointer names a live value for this synchronous call;
    // without a synchronous-I/O option the handle is asynchronous.
    let status = unsafe {
        NtCreateFile(
            &raw mut handle,
            FILE_LIST_DIRECTORY,
            &raw const attributes,
            &raw mut status_block,
            None,
            FILE_FLAGS_AND_ATTRIBUTES(0),
            FILE_SHARE_READ | FILE_SHARE_WRITE
                | if pin_name { Default::default() } else { windows::Win32::Storage::FileSystem::FILE_SHARE_DELETE },
            FILE_OPEN,
            FILE_DIRECTORY_FILE | FILE_OPEN_FOR_BACKUP_INTENT,
            None,
            0,
        )
    };
    if status.is_err() {
        // SAFETY: a pure status-code translation.
        let code = unsafe { RtlNtStatusToDosError(status) };
        return Err(io::Error::from_raw_os_error(
            i32::try_from(code).unwrap_or(i32::MAX),
        ).into());
    }
    // SAFETY: a handle the call just returned is ours to own.
    Ok(unsafe { OwnedHandle::from_raw_handle(handle.0) })
}

fn ordinary_error(error: DemandError) -> io::Error {
    match error {
        DemandError::Io(error) => error,
        error => io::Error::other(error),
    }
}

#[cfg(test)]
/// Turns one read's reports into changes: each names the entry, relative to
/// the root, and, in `extended` reports, the file.
fn translate(mut bytes: &[u8], extended: bool, changes: &mut Vec<HostChange>) {
    let (name_offset, length_offset) = if extended {
        (
            std::mem::offset_of!(FILE_NOTIFY_EXTENDED_INFORMATION, FileName),
            std::mem::offset_of!(FILE_NOTIFY_EXTENDED_INFORMATION, FileNameLength),
        )
    } else {
        (
            std::mem::offset_of!(FILE_NOTIFY_INFORMATION, FileName),
            std::mem::offset_of!(FILE_NOTIFY_INFORMATION, FileNameLength),
        )
    };
    let file = |record: &[u8]| -> Option<u64> {
        let offset = std::mem::offset_of!(FILE_NOTIFY_EXTENDED_INFORMATION, FileId);
        record
            .get(offset..offset + 8)
            .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
            .map(u64::from_le_bytes)
    };
    loop {
        let (Some(next), Some(length)) = (
            bytes
                .get(..4)
                .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok()),
            bytes
                .get(length_offset..)
                .and_then(|bytes| bytes.get(..4))
                .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok()),
        ) else {
            changes.push(HostChange::Everything);
            return;
        };
        let length = usize::try_from(u32::from_le_bytes(length)).unwrap_or(usize::MAX);
        let Some(name) = bytes.get(name_offset..name_offset.saturating_add(length)) else {
            changes.push(HostChange::Everything);
            return;
        };
        let Some(units) = crate::kernel::types::utf16le_units(name) else {
            changes.push(HostChange::Everything);
            return;
        };
        let units = units.collect::<Vec<_>>();
        let path = PathBuf::from(OsString::from_wide(&units));
        let action = bytes
            .get(4..8)
            .and_then(|action| <[u8; 4]>::try_from(action).ok())
            .map_or(0, u32::from_le_bytes);
        changes.push(if action == FILE_ACTION_MODIFIED.0 {
            HostChange::Altered(path)
        } else {
            HostChange::Rebound(path)
        });
        if extended {
            let Some(file) = file(bytes) else {
                changes.push(HostChange::Everything);
                return;
            };
            changes.push(HostChange::File(file));
        }
        let next = usize::try_from(u32::from_le_bytes(next)).unwrap_or(0);
        if next == 0 {
            return;
        }
        let Some(rest) = bytes.get(next..) else {
            changes.push(HostChange::Everything);
            return;
        };
        bytes = rest;
    }
}

#[cfg(test)]
mod tests {
    use super::{Delivery, DemandError, DemandWork, HostChange, HostChangeWide, OVERLAPPED, Shared, translate};
    use std::mem::size_of;
    use std::path::PathBuf;

    /// One report as the file system lays it out: the fixed fields, then the
    /// name, padded to the next record.
    fn record(extended: bool, action: u32, file: u64, name: &str, last: bool) -> Vec<u8> {
        let name = name
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        let mut bytes = vec![0_u8; if extended { 84 } else { 12 }];
        bytes[4..8].copy_from_slice(&action.to_le_bytes());
        let length = u32::try_from(name.len()).unwrap_or(u32::MAX).to_le_bytes();
        if extended {
            bytes[64..72].copy_from_slice(&file.to_le_bytes());
            bytes[80..84].copy_from_slice(&length);
        } else {
            bytes[8..12].copy_from_slice(&length);
        }
        bytes.extend_from_slice(&name);
        bytes.resize(bytes.len().next_multiple_of(8), 0);
        if !last {
            let next = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
            bytes[..4].copy_from_slice(&next.to_le_bytes());
        }
        bytes
    }

    /// Both report layouts name each entry; only the extended one names its
    /// file, and only a modification alters an entry in place.
    #[test]
    fn reports_translate_in_either_layout() {
        for extended in [true, false] {
            let mut bytes = record(extended, 1, 7, r"d\created", false);
            bytes.extend(record(extended, 3, 9, "written", true));
            let mut changes = Vec::new();
            translate(&bytes, extended, &mut changes);
            let mut expected = vec![HostChange::Rebound(PathBuf::from("d").join("created"))];
            if extended {
                expected.push(HostChange::File(7));
            }
            expected.push(HostChange::Altered(PathBuf::from("written")));
            if extended {
                expected.push(HostChange::File(9));
            }
            assert_eq!(changes, expected, "extended: {extended}");
        }
    }

    #[test]
    fn flexible_utf16_notification_names_are_not_scalar_decoded() {
        use std::os::windows::ffi::OsStrExt as _;
        for extended in [false, true] {
            let mut bytes = vec![0_u8; if extended { 84 } else { 12 }];
            bytes[4..8].copy_from_slice(&3_u32.to_le_bytes());
            let offset = if extended { 80 } else { 8 };
            bytes[offset..offset + 4].copy_from_slice(&2_u32.to_le_bytes());
            bytes.extend_from_slice(&0xD800_u16.to_le_bytes());
            let mut changes = Vec::new();
            translate(&bytes, extended, &mut changes);
            let Some(HostChange::Altered(path)) = changes.first() else {
                panic!("the ordinary driver must preserve the opaque name");
            };
            assert_eq!(path.as_os_str().encode_wide().collect::<Vec<_>>(), vec![0xD800]);
        }
    }

    #[test]
    fn measured_nonrecursive_startup_admits_real_state_and_native_requests()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::source_watch::{HostChangeSink, NativeSourceWatch};
        use crate::demand::DemandWork;
        use crate::{CancellationToken, WorkBudget};
        use std::sync::Arc;
        struct Ignored;
        impl HostChangeSink for Ignored {
            fn logical_owned_bytes(&self) -> u64 { 0 }
            fn host_changed(&self, _: &[HostChange], work: &mut DemandWork) -> Result<(), DemandError> { work.check() }
            fn host_changed_wide(&self, _: &[HostChangeWide<'_>], work: &mut DemandWork) -> Result<(), DemandError> { work.check() }
        }
        let root = tempfile::tempdir()?;
        let held = cap_std::fs::Dir::open_ambient_dir(root.path(), cap_std::ambient_authority())?;
        let mut work = DemandWork::new(WorkBudget {
            backend_read_operations: 8,
            backend_write_operations: 8,
            allocation_operations: 4,
            peak_allocation_bytes: 256 * 1024,
            bytes_copied: 1024,
            ..WorkBudget::default()
        }, &CancellationToken::new());
        let watch = NativeSourceWatch::start_directory_measured(&held, Arc::new(Ignored), true, &mut work)?;
        assert!(!watch.platform.shared.recursive);
        assert_eq!(work.work.allocation_operations, 4);
        assert_eq!(work.work.peak_allocation_bytes, watch.owned_bytes());
        assert!(work.work.backend_read_operations >= 2);
        assert_eq!(work.work.backend_write_operations, 2);
        assert_eq!(work.work.bytes_copied, 0, "native startup has no owned thread-name or closure buffer");
        let before = work.work.backend_write_operations;
        let before_reads = work.work.backend_read_operations;
        watch.stop_measured(&mut work)?;
        assert_eq!(work.work.backend_read_operations, before_reads + 2);
        assert_eq!(work.work.backend_write_operations, before + 5);
        assert!(!watch.is_exact());
        assert_eq!(watch.logical_owned_bytes(), super::super::arc_control_bytes::<Shared>()
            + size_of::<OVERLAPPED>() as u64 + super::super::arc_control_bytes::<Delivery>());
        let stopped = work.work;
        watch.stop_measured(&mut work)?;
        assert_eq!(work.work, stopped, "retired handles cannot perform native cleanup twice");
        drop(watch);
        assert_eq!(work.work, stopped);
        Ok(())
    }

    #[test]
    fn short_startup_budget_denies_the_resident_cleanup_suffix_before_native_io()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::source_watch::{HostChangeSink, NativeSourceWatch};
        use crate::demand::{DemandError, DemandWork};
        use crate::{CancellationToken, WorkBudget};
        use std::sync::Arc;
        struct Ignored;
        impl HostChangeSink for Ignored {
            fn logical_owned_bytes(&self) -> u64 { 0 }
            fn host_changed(&self, _: &[HostChange], work: &mut DemandWork) -> Result<(), DemandError> { work.check() }
            fn host_changed_wide(&self, _: &[HostChangeWide<'_>], work: &mut DemandWork) -> Result<(), DemandError> { work.check() }
        }
        let root = tempfile::tempdir()?;
        let held = cap_std::fs::Dir::open_ambient_dir(root.path(), cap_std::ambient_authority())?;
        let mut work = DemandWork::new(WorkBudget {
            backend_read_operations: 1,
            ..WorkBudget::UNBOUNDED
        }, &CancellationToken::new());
        let failure = NativeSourceWatch::start_directory_measured(&held, Arc::new(Ignored), true, &mut work)
            .err().ok_or("startup unexpectedly exceeded its finite native allowance")?;
        assert!(matches!(failure, DemandError::Work(_)));
        assert_eq!(work.work.backend_read_operations, 0, "the complete native cleanup suffix precedes the first handle request");
        assert_eq!(work.work.backend_write_operations, 0, "denied shutdown custody creates no native event or directory handle");
        assert_eq!(work.work.allocation_operations, 1, "only the common delivery control exists before suffix admission");
        Ok(())
    }

    #[test]
    fn stop_denial_and_cancellation_retain_the_same_native_owner()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::source_watch::{HostChangeSink, NativeSourceWatch};
        use crate::{CancellationToken, WorkBudget, WorkCounters};
        use std::os::windows::io::AsRawHandle as _;
        use std::sync::Arc;
        struct Quiet;
        impl HostChangeSink for Quiet {
            fn logical_owned_bytes(&self) -> u64 { 0 }
            fn host_changed(&self, _: &[HostChange], work: &mut DemandWork) -> Result<(), DemandError> { work.check() }
            fn host_changed_wide(&self, _: &[HostChangeWide<'_>], work: &mut DemandWork) -> Result<(), DemandError> { work.check() }
        }
        let root = tempfile::tempdir()?;
        let held = cap_std::fs::Dir::open_ambient_dir(root.path(), cap_std::ambient_authority())?;
        let mut startup = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
        let watch = NativeSourceWatch::start_directory_measured(&held, Arc::new(Quiet), true, &mut startup)?;
        let directory = watch.platform.shared.directory.as_raw_handle();
        let completed = watch.platform.shared.completed.as_raw_handle();
        let owned = watch.logical_owned_bytes();
        assert!(!watch.is_stopped());
        let reads = watch.platform.shared.lock_reads();
        let observed = std::thread::scope(|scope| {
            let (sender, receiver) = std::sync::mpsc::channel();
            let watch = &watch;
            scope.spawn(move || { let _ = sender.send(watch.logical_owned_bytes()); });
            let observed = receiver.recv_timeout(std::time::Duration::from_secs(1));
            drop(reads);
            observed
        })?;
        assert_eq!(observed, owned, "readonly payload custody cannot wait on a callback-owned read lock");
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancelled);
        assert!(matches!(watch.stop_measured(&mut work), Err(DemandError::Cancelled)));
        assert_eq!(work.work, WorkCounters::default());
        assert!(!watch.is_stopped());
        let mut denied = DemandWork::new(WorkBudget {
            backend_read_operations: 1, backend_write_operations: 4, ..WorkBudget::UNBOUNDED
        }, &CancellationToken::new());
        assert!(matches!(watch.stop_measured(&mut denied), Err(DemandError::Work(_))));
        assert_eq!(denied.work, WorkCounters::default());
        assert!(!watch.is_stopped());
        assert!(watch.is_exact());
        assert_eq!(watch.platform.shared.directory.as_raw_handle(), directory);
        assert_eq!(watch.platform.shared.completed.as_raw_handle(), completed);
        assert_eq!(watch.logical_owned_bytes(), owned);
        let mut admitted = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
        watch.stop_measured(&mut admitted)?;
        assert_eq!(admitted.work.backend_read_operations, 2);
        assert_eq!(admitted.work.backend_write_operations, 5);
        assert_eq!(admitted.work.allocation_operations, 0);
        assert_eq!(watch.logical_owned_bytes(), owned - size_of::<[u64; super::BUFFER_WORDS]>() as u64);
        assert!(!watch.platform.shared.directory.is_open());
        assert!(!watch.platform.shared.completed.is_open());
        assert!(watch.platform.thread.lock().unwrap_or_else(std::sync::PoisonError::into_inner).is_none());
        assert!(!watch.is_exact());
        assert!(watch.is_stopped());
        Ok(())
    }

    #[cfg(feature = "native-mount")]
    #[test]
    fn shared_native_stop_keeps_the_original_grant_and_completes_a_late_fault()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::{NativeDemandReadMeter, NativeSourceWork, SourceDemandReadWork};
        use crate::native_host::NativeHostWork;
        use crate::{CancellationToken, WorkBudget, WorkCounters};
        use std::sync::{Arc, Mutex};

        struct Quiet;
        impl super::super::HostChangeSink for Quiet {
            fn logical_owned_bytes(&self) -> u64 { 0 }
            fn host_changed(&self, _: &[HostChange], work: &mut DemandWork)
                -> Result<(), DemandError> { work.check() }
            fn host_changed_wide(&self, _: &[HostChangeWide<'_>], work: &mut DemandWork)
                -> Result<(), DemandError> { work.check() }
        }
        struct LateFault<'a> {
            inner: NativeSourceWork<'a>,
            cancellation: &'a CancellationToken,
            error: bool,
            triggered: bool,
        }
        impl NativeHostWork for LateFault<'_> {
            type Error = DemandError;
            type Allocation = u64;
            type CleanupWrite = u64;
            fn io_error_kind(error: &DemandError) -> Option<std::io::ErrorKind> {
                <DemandWork as NativeHostWork>::io_error_kind(error)
            }
            fn allocation_failed(error: std::collections::TryReserveError) -> DemandError { DemandError::Allocation(error) }
            fn check(&self) -> Result<(), DemandError> { self.inner.check() }
            fn admit(&mut self, delta: WorkCounters) -> Result<(), DemandError> { self.inner.admit(delta) }
            fn started_io(&mut self, delta: WorkCounters) -> Result<(), DemandError> { self.inner.started_io(delta) }
            fn record(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
                let result = self.inner.record(delta);
                if !self.triggered && delta.backend_write_operations != 0 {
                    self.triggered = true;
                    self.cancellation.cancel();
                    if self.error {
                        return result.and(Err(std::io::Error::other("late stop recording fault").into()));
                    }
                }
                result
            }
            fn claim(&mut self, bytes: u64) -> Result<u64, DemandError> { self.inner.claim(bytes) }
            fn release(&mut self, bytes: u64) -> Result<(), DemandError> { self.inner.release(bytes) }
            fn reserve_opaque_decode(&mut self, maximum: u64) -> Result<u64, DemandError> { self.inner.reserve_opaque_decode(maximum) }
            fn reconcile_allocation(&mut self, allocation: &mut u64, actual: u64) -> Result<(), DemandError> { self.inner.reconcile_allocation(allocation, actual) }
            fn reserve_cleanup_write(&mut self) -> Result<u64, DemandError> { self.inner.reserve_cleanup_write() }
            fn cancel_cleanup_write(&mut self, reservation: u64) { self.inner.cancel_cleanup_write(reservation); }
            fn attempt_cleanup_write<T>(&mut self, reservation: u64, operation: impl FnOnce() -> T)
                -> (T, Result<(), DemandError>) {
                self.inner.attempt_cleanup_write(reservation, operation)
            }
        }
        impl SourceDemandReadWork for LateFault<'_> {
            fn with_native_read_meter<T>(
                &mut self, operation: impl FnOnce(&dyn acyclic_native_runtime::NativeReadMeter) -> T,
            ) -> T { self.inner.with_native_read_meter(operation) }
            fn with_original_leaf<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T {
                self.inner.with_original_leaf(operation)
            }
            fn is_native_original(&self) -> bool { true }
        }

        for late_error in [false, true] {
            let root = tempfile::tempdir()?;
            let held = cap_std::fs::Dir::open_ambient_dir(root.path(), cap_std::ambient_authority())?;
            let mut startup = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
            let watch = super::PlatformWatch::start_directory_measured(
                &held, Arc::new(Delivery::new_measured(Arc::new(Quiet), &startup)), true, &mut startup,
            )?;
            let owned = watch.logical_owned_bytes();
            let cancellation = CancellationToken::new();
            let budget = WorkBudget {
                backend_read_operations: 2, backend_write_operations: 5,
                allocation_operations: 1, peak_allocation_bytes: 8,
                ..WorkBudget::default()
            };
            let mut original = DemandWork::new(budget, &cancellation);
            original.allocate(8)?;
            let retained = Box::new([0_u8; 8]);
            let before = original.work;
            let original = Arc::new(NativeDemandReadMeter(Mutex::new(original)));
            original.with_original_leaf(|work| work.budget.backend_write_operations = 4);
            assert!(matches!(watch.stop_with_work(&mut NativeSourceWork::new(&original)), Err(DemandError::Work(_))));
            assert_eq!(original.with_original_leaf(|work| work.work), before);
            assert!(!watch.is_stopped());
            assert_eq!(watch.logical_owned_bytes(), owned);
            original.with_original_leaf(|work| work.budget = budget);
            let cancelled = CancellationToken::new();
            cancelled.cancel();
            let cancelled_original = Arc::new(NativeDemandReadMeter(Mutex::new(DemandWork::new(budget, &cancelled))));
            assert!(matches!(watch.stop_with_work(&mut NativeSourceWork::new(&cancelled_original)), Err(DemandError::Cancelled)));
            assert_eq!(cancelled_original.with_original_leaf(|work| work.work), WorkCounters::default());
            assert!(!watch.is_stopped());

            let mut work = LateFault {
                inner: NativeSourceWork::new(&original), cancellation: &cancellation,
                error: late_error, triggered: false,
            };
            let result = watch.stop_with_work(&mut work);
            assert!(work.triggered);
            if late_error {
                assert!(matches!(result, Err(DemandError::Io(error)) if error.to_string() == "late stop recording fault"));
            } else {
                result?;
            }
            assert!(watch.is_stopped());
            assert!(!watch.shared.directory.is_open());
            assert!(!watch.shared.completed.is_open());
            assert!(watch.thread.lock().map_err(|_| "poisoned thread custody")?.is_none());
            assert_eq!(watch.logical_owned_bytes(), owned - size_of::<[u64; super::BUFFER_WORDS]>() as u64);
            let spent = original.with_original_leaf(|work| {
                assert_eq!(work.work.backend_read_operations, 2);
                assert_eq!(work.work.backend_write_operations, 5);
                assert_eq!(work.work.allocation_operations, before.allocation_operations);
                assert_eq!(work.work.peak_allocation_bytes, before.peak_allocation_bytes);
                assert_eq!(work.live_allocation_bytes(), retained.len() as u64);
                assert_eq!(work.budget, budget);
                work.work
            });
            drop(watch);
            assert_eq!(original.with_original_leaf(|work| work.work), spent);
            drop(retained);
            original.with_original_leaf(|work| work.release(8))?;
        }
        Ok(())
    }

    #[test]
    fn startup_denies_handle_cleanup_before_opening_the_directory()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::source_watch::{HostChangeSink, NativeSourceWatch};
        use crate::{CancellationToken, WorkBudget};
        use std::sync::Arc;
        struct Quiet;
        impl HostChangeSink for Quiet {
            fn logical_owned_bytes(&self) -> u64 { 0 }
            fn host_changed(&self, _: &[HostChange], work: &mut DemandWork) -> Result<(), DemandError> { work.check() }
            fn host_changed_wide(&self, _: &[HostChangeWide<'_>], work: &mut DemandWork) -> Result<(), DemandError> { work.check() }
        }
        let root = tempfile::tempdir()?;
        let held = cap_std::fs::Dir::open_ambient_dir(root.path(), cap_std::ambient_authority())?;
        let mut work = DemandWork::new(WorkBudget {
            backend_write_operations: 0, ..WorkBudget::UNBOUNDED
        }, &CancellationToken::new());
        assert!(matches!(NativeSourceWatch::start_directory_measured(
            &held, Arc::new(Quiet), true, &mut work,
        ), Err(DemandError::Work(_))));
        assert_eq!(work.work.backend_read_operations, 0);
        assert_eq!(work.work.backend_write_operations, 0);
        assert_eq!(work.work.allocation_operations, 1, "only the admitted Delivery frame exists");
        Ok(())
    }
    #[test]
    fn actual_callback_stop_keeps_the_owner_retryable_without_work()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::source_watch::{HostChangeSink, NativeSourceWatch};
        use crate::{CancellationToken, WorkBudget, WorkCounters};
        use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
        struct SelfStop {
            watch: OnceLock<Weak<NativeSourceWatch>>,
            result: Mutex<Option<(bool, WorkCounters)>>,
            ready: Condvar,
        }
        impl SelfStop {
            fn attempt(&self, work: &mut DemandWork) -> Result<(), DemandError> {
                if self.result.lock().unwrap_or_else(std::sync::PoisonError::into_inner).is_some() {
                    return Ok(());
                }
                let Some(watch) = self.watch.get().and_then(Weak::upgrade) else { return Ok(()); };
                let mut stop = DemandWork::new(work.budget, work.cancellation());
                let unavailable = matches!(watch.stop_measured(&mut stop), Err(DemandError::WorkerUnavailable));
                *self.result.lock().unwrap_or_else(std::sync::PoisonError::into_inner) =
                    Some((unavailable, stop.work));
                self.ready.notify_all();
                Ok(())
            }
        }
        impl HostChangeSink for SelfStop {
            fn logical_owned_bytes(&self) -> u64 { 0 }
            fn host_changed(&self, _: &[HostChange], work: &mut DemandWork) -> Result<(), DemandError> {
                self.attempt(work)
            }
            fn host_changed_wide(&self, _: &[HostChangeWide<'_>], work: &mut DemandWork)
            -> Result<(), DemandError> { self.attempt(work) }
        }
        let root = tempfile::tempdir()?;
        let held = cap_std::fs::Dir::open_ambient_dir(root.path(), cap_std::ambient_authority())?;
        let sink = Arc::new(SelfStop {
            watch: OnceLock::new(), result: Mutex::new(None), ready: Condvar::new(),
        });
        let cancellation = CancellationToken::new();
        let mut startup = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let watch = Arc::new(NativeSourceWatch::start_directory_measured(
            &held, sink.clone(), true, &mut startup,
        )?);
        assert!(sink.watch.set(Arc::downgrade(&watch)).is_ok());
        std::fs::write(root.path().join("changed"), b"callback")?;
        let result = sink.result.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let (result, _) = sink.ready.wait_timeout_while(
            result, std::time::Duration::from_secs(5), |result| result.is_none(),
        ).unwrap_or_else(std::sync::PoisonError::into_inner);
        let observed = *result;
        drop(result);
        assert_eq!(observed, Some((true, WorkCounters::default())));
        assert!(!watch.is_stopped());
        assert!(watch.is_exact());
        let mut stop = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        watch.stop_measured(&mut stop)?;
        assert!(watch.is_stopped());
        let mut repeated = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        watch.stop_measured(&mut repeated)?;
        assert_eq!(repeated.work, WorkCounters::default());
        Ok(())
    }
}
