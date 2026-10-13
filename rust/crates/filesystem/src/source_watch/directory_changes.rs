//! Native source change notifications.

#![allow(
    unsafe_code,
    reason = "watches through the Win32 ReadDirectoryChangesW API"
)]

use super::{Delivery, HostChangeWide};
#[cfg(test)]
use super::HostChange;
use std::io;
use std::mem::size_of;
use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, IntoRawHandle as _, OwnedHandle};
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use crate::demand::{DemandError, DemandWork, SourceDemandReadWork};
use crate::performance::WorkCounters;
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
use windows::Win32::System::IO::{CancelIoEx, GetOverlappedResult, OVERLAPPED};
use windows::Win32::System::Threading::{
    CreateEventW, CreateThread, INFINITE, SetEvent, WaitForSingleObject,
};

/// Room for many reports per read; a local volume admits more than 64 KiB.
const BUFFER_WORDS: usize = 64 * 1024 / size_of::<u64>();
/// How long a fence waits for the reader before it assumes everything changed instead.
const FENCE_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) struct NativeSourceWatch {
    shared: Arc<Shared>,
    thread: Mutex<Option<OwnedHandle>>,
}

struct Shared {
    directory: WatchHandle,
    /// Signaled when the pending read completes.
    completed: WatchHandle,
    /// Whether reads report each entry's file identity too, which the file system decides once.
    extended: AtomicBool,
    /// Whether the file system reports every change or its loss (NTFS).
    complete: AtomicBool,
    recursive: bool,
    reads: Mutex<Reads>,
    buffer_owned: AtomicBool,
    reading_changed: Condvar,
    /// Fences the reader thread has completed.
    fenced: Mutex<u64>,
    fence_delivered: Condvar,
    stopping: AtomicBool,
    stop_completed: AtomicBool,
    ownership: crate::demand::SourcePayloadOwnership,
    native_cleanup: Mutex<[Option<crate::materializer::NativeWorkReservation>; 2]>,
    native_thread: Mutex<Option<OwnedHandle>>,
    native_ready: AtomicBool,
    native_retiring: AtomicBool,
    native_entered_retirement: AtomicBool,
    native_attempt_finished: AtomicBool,
    native_drop_requested: AtomicBool,
    native_failure: Mutex<Option<DemandError>>,
    #[cfg(test)]
    native_wait_cancellation: Mutex<Option<crate::CancellationToken>>,
    delivery: Arc<Delivery>,
}

/// The one read in flight.
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
struct WatchHandle {
    raw: AtomicPtr<std::ffi::c_void>,
    ordinary: bool,
}

impl WatchHandle {
    fn empty(ordinary: bool) -> Self {
        Self { raw: AtomicPtr::new(std::ptr::null_mut()), ordinary }
    }
    fn install(&self, handle: OwnedHandle) {
        assert!(!self.is_open(), "one original handle is installed once");
        self.raw.store(handle.into_raw_handle(), Ordering::Release);
    }
    fn is_open(&self) -> bool { !self.raw.load(Ordering::Acquire).is_null() }
    fn close(&self) -> windows::core::Result<()> {
        let handle = self.raw.load(Ordering::Acquire);
        if !handle.is_null() {
            // SAFETY: close is serialized after the native reader quiesces.
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
        let handle = *self.raw.get_mut();
        assert!(self.ordinary || handle.is_null(), "native handle outlived its physical retirement owner");
        let _ = self.close();
    }
}

fn reserve_cleanup<W: SourceDemandReadWork>(work: &mut W, reads: u64, writes: u64) -> Result<(), DemandError> {
    work.with_original_leaf(|work| super::reserve_cleanup_credits(work, WorkCounters {
        backend_read_operations: reads, backend_write_operations: writes, ..WorkCounters::UNCHARGED
    }))
}

fn close_owned<W: SourceDemandReadWork>(
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

fn create_reader<W: SourceDemandReadWork>(reader: &Arc<Shared>, work: &mut W) -> Result<OwnedHandle, DemandError> {
    work.admit(WorkCounters { backend_read_operations: 1, backend_write_operations: 1, ..WorkCounters::default() })?;
    work.started_io(WorkCounters { backend_write_operations: 1, ..WorkCounters::default() })?;
    let parameter = Arc::into_raw(Arc::clone(reader));
    // SAFETY: success transfers this real Shared to the existing reader.
    match unsafe { CreateThread(None, 0, Some(reader_thread), Some(parameter.cast()), Default::default(), None) } {
        Ok(thread) => Ok(unsafe { OwnedHandle::from_raw_handle(thread.0) }),
        Err(error) => {
            // SAFETY: failed native creation transferred no callback owner.
            drop(unsafe { Arc::from_raw(parameter) });
            Err(io::Error::from(error).into())
        }
    }
}

// SAFETY: the read's buffer and `OVERLAPPED` are touched only under the
// `reads` lock, and the kernel writes them only while a read is pending.
unsafe impl Send for Reads {}

impl NativeSourceWatch {
    pub(crate) fn start_driver<W: SourceDemandReadWork>(
        directory: &crate::native_host::HostRoot, delivery: Arc<Delivery>, work: &mut W,
    ) -> Result<Self, DemandError> {
        Self::start_handle_with_work(directory.directory_handle(), delivery, true, None, true, work)
    }

    pub(crate) fn start_directory_driver<W: SourceDemandReadWork>(
        directory: &cap_std::fs::Dir, delivery: Arc<Delivery>, complete: bool, work: &mut W,
    ) -> Result<Self, DemandError> {
        use std::os::windows::io::AsHandle as _;
        Self::start_handle_with_work(directory.as_handle(), delivery, false, Some(complete), false, work)
    }

    pub(crate) fn start_held_root_with_work<W: SourceDemandReadWork>(
        directory: &crate::native_host::HostRoot, delivery: Arc<Delivery>,
        complete: bool, recursive: bool, work: &mut W,
    ) -> Result<Self, DemandError> {
        Self::start_handle_with_work(directory.directory_handle(), delivery, false, Some(complete), recursive, work)
    }

    fn start_handle_with_work<W: SourceDemandReadWork>(
        root: std::os::windows::io::BorrowedHandle<'_>, delivery: Arc<Delivery>,
        pin_name: bool, complete: Option<bool>, recursive: bool, work: &mut W,
    ) -> Result<Self, DemandError> {
        work.admit(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
        let carrier = delivery.prepare_startup_failure(work)?;
        let original_budget = work.with_original_leaf(|work| work.budget);
        let native_original = work.is_native_original();
        let mut shared = None;
        let mut owned_bytes = 0_u64;
        let ownership = crate::demand::SourcePayloadOwnership::new(work)?;
        let result = (|| {
            let mut active = super::prepare_native_cleanup_credit(work, WorkCounters {
                backend_read_operations: 3, backend_write_operations: 10,
                ..WorkCounters::UNCHARGED
            })?;
            let terminal = active.as_mut().map(|credit| credit.partition_work(WorkCounters {
                backend_read_operations: 1, backend_write_operations: 5,
                ..WorkCounters::UNCHARGED
            })).transpose().map_err(crate::native_name::original_name_error)?;
            if !native_original { reserve_cleanup(work, 2, 5)?; }
            let overlapped_bytes = size_of::<OVERLAPPED>() as u64;
            work.claim_allocation(overlapped_bytes, u64::from((overlapped_bytes) != 0), crate::WorkCounters::UNCHARGED)?;
            owned_bytes += overlapped_bytes;
            let overlapped = Box::new(OVERLAPPED::default());
            let buffer_bytes = size_of::<[u64; BUFFER_WORDS]>() as u64;
            work.claim_allocation(buffer_bytes, u64::from((buffer_bytes) != 0), crate::WorkCounters::UNCHARGED)?;
            owned_bytes += buffer_bytes;
            let buffer = Box::<[u64; BUFFER_WORDS]>::new_uninit();
            let shared_bytes = super::arc_control_bytes::<Shared>();
            work.claim_allocation(shared_bytes, u64::from((shared_bytes) != 0), crate::WorkCounters::UNCHARGED)?;
            owned_bytes += shared_bytes;
            ownership.retain(owned_bytes, work)?;
            shared = Some(Arc::new(Shared {
                directory: WatchHandle::empty(!native_original),
                completed: WatchHandle::empty(!native_original), delivery,
                extended: AtomicBool::new(true), complete: AtomicBool::new(complete.unwrap_or(false)), recursive,
                reads: Mutex::new(Reads {
                    overlapped, buffer: Some(buffer), pending: false, requested: 0,
                    filled: 0, cursor: 0, loss: false, blocked: false,
                }),
                buffer_owned: AtomicBool::new(true),
                reading_changed: Condvar::new(), fenced: Mutex::new(0),
                fence_delivered: Condvar::new(), stopping: AtomicBool::new(false),
                stop_completed: AtomicBool::new(false),
                native_cleanup: Mutex::new([active, terminal]),
                native_thread: Mutex::new(None), native_ready: AtomicBool::new(false),
                native_retiring: AtomicBool::new(false), native_entered_retirement: AtomicBool::new(false),
                native_attempt_finished: AtomicBool::new(false),
                native_drop_requested: AtomicBool::new(false), native_failure: Mutex::new(None),
                #[cfg(test)]
                native_wait_cancellation: Mutex::new(None),
                ownership,
            }));
            let state = shared.as_ref().expect("prepared read state");
            if native_original {
                let thread = create_reader(state, work)?;
                *state.native_thread.lock().unwrap_or_else(PoisonError::into_inner) = Some(thread);
            }
            state.directory.install(open_for_changes_handle(root, pin_name, work)?);
            if complete.is_none() {
                state.complete.store(crate::native_host::reports_every_change(&state.directory, work)?, Ordering::Release);
            }
            work.started_io(WorkCounters { backend_write_operations: 1, ..WorkCounters::default() })?;
            // SAFETY: an unnamed manual-reset event, exclusively owned here.
            let event = unsafe { CreateEventW(None, true, false, None) }.map_err(io::Error::from)?;
            state.completed.install(unsafe { OwnedHandle::from_raw_handle(event.0) });
            let reader = shared.as_ref().expect("constructed read state");
            let mut reads = reader.lock_reads();
            if let Err(error) = reader.issue_read(&mut reads, work) {
                if !matches!(&error, DemandError::Io(error)
                    if error.raw_os_error() == Some(ERROR_INVALID_FUNCTION.to_hresult().0))
                { return Err(error); }
                reader.extended.store(false, Ordering::Release);
                reader.issue_read(&mut reads, work)?;
            }
            drop(reads);
            let thread = if native_original { None } else { Some(create_reader(reader, work)?) };
            reader.native_ready.store(true, Ordering::Release);
            reader.reading_changed.notify_all();
            Ok(Self {
                shared: shared.take().expect("constructed read state"),
                thread: Mutex::new(thread),
            })
        })();
        if !native_original {
            work.with_original_leaf(|work| work.budget = original_budget);
        }
        let result = match result {
            Ok(watch) => Ok(watch),
            Err(error) => {
                if let Some(shared) = shared {
                    let failed = Self { shared, thread: Mutex::new(None) };
                    let cleanup = failed.stop_engine(work, true, None);
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

    pub(crate) fn stop_with_work<W: SourceDemandReadWork>(
        &self, work: &mut W,
    ) -> Result<(), DemandError> {
        self.stop_engine(work, false, None)
    }

    /// Completes an already-owned native cleanup suffix without introducing a caller token, root, resource allowance or namespace authority.
    pub(crate) fn stop_accepted_cleanup(&self) -> Result<(), DemandError> {
        let birth = self.shared.delivery.original.as_ref().ok_or(DemandError::NativeWorkUnavailable)?;
        let mut work = crate::NativeMaterializationWork::clone(birth);
        self.stop_engine(&mut work, true, None)
    }

    /// Retires only this watch's original birth-paid native suffix.
    pub(crate) fn stop_with_cleanup_wait(
        &self, wait: &crate::CancellationToken,
    ) -> Result<(), DemandError> {
        wait.check().map_err(|_| DemandError::Cancelled)?;
        let birth = self.shared.delivery.original.as_ref()
            .ok_or(DemandError::NativeWorkUnavailable)?;
        let mut work = crate::NativeMaterializationWork::clone(birth);
        self.stop_engine(&mut work, true, Some(wait))
    }

    fn stop_engine<W: SourceDemandReadWork>(
        &self, work: &mut W, retiring: bool, wait: Option<&crate::CancellationToken>,
    ) -> Result<(), DemandError> {
        if !retiring { work.check()?; }
        self.shared.delivery.ensure_stop_available()?;
        if self.shared.delivery.original.is_some() { return self.stop_native_engine(work, wait); }
        let mut thread = self.thread.lock().unwrap_or_else(PoisonError::into_inner);
        let mut reads = self.shared.lock_reads();
        let running = thread.is_some();
        let pending = reads.pending;
        let required_reads = u64::from(pending) + u64::from(running);
        let required_writes = u64::from(pending) + u64::from(running) * 2
            + u64::from(self.shared.directory.is_open()) + u64::from(self.shared.completed.is_open());
        if !retiring {
            work.admit(WorkCounters {
                backend_write_operations: required_writes, backend_read_operations: required_reads,
                ..WorkCounters::default()
            })?;
        }
        let mut result = Ok(());
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
            reads.pending = !matches!(&completed, Ok(()))
                && !completed.as_ref().err().is_some_and(|error|
                    error.code() == ERROR_OPERATION_ABORTED.to_hresult()
                        || error.code() == ERROR_NOTIFY_ENUM_DIR.to_hresult());
            if let Err(error) = completed {
                if error.code() != ERROR_OPERATION_ABORTED.to_hresult() {
                    result = result.and(Err(DemandError::from(io::Error::from(error))));
                }
            }
            result = result.and(self.shared.record_cleanup(work, 1, 0));
            if reads.pending {
                return result;
            }
        }
        self.shared.stopping.store(true, Ordering::Release);
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
                return result.and(Err(error));
            }
            result = result.and(close_owned(&mut thread, &self.shared, work));
        }
        let mut reads = self.shared.lock_reads();
        if reads.pending {
            return result.and(Err(io::Error::from(io::ErrorKind::WouldBlock).into()));
        }
        result = result.and(self.shared.close_watch(&self.shared.directory, work));
        result = result.and(self.shared.close_watch(&self.shared.completed, work));
        if let Some(buffer) = reads.buffer.take() {
            drop(buffer);
            result = result.and(self.shared.ownership.retire(size_of::<[u64; BUFFER_WORDS]>() as u64));
        }
        self.shared.buffer_owned.store(false, Ordering::Release);
        reads.filled = 0;
        reads.cursor = 0;
        if thread.is_none() && !self.shared.directory.is_open() && !self.shared.completed.is_open() {
            self.shared.stop_completed.store(true, Ordering::Release);
        }
        result
    }

    /// Native completion means retirement of this managed driver's accepted IO, callbacks and handles, not a claim that the OS thread has exited.
    fn stop_native_engine<W: SourceDemandReadWork>(
        &self, work: &mut W, wait: Option<&crate::CancellationToken>,
    ) -> Result<(), DemandError> {
        if let Some(wait) = wait { wait.check().map_err(|_| DemandError::Cancelled)?; }
        if !self.shared.delivery.birth_matches(work) { return Err(DemandError::NativeWorkUnavailable); }
        if self.is_stopped() { return Ok(()); }
        let mut reads = if wait.is_some() {
            match self.shared.reads.try_lock() {
                Ok(reads) => reads,
                Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => return Err(DemandError::WorkerUnavailable),
            }
        } else { self.shared.lock_reads() };
        let thread = if wait.is_some() {
            match self.shared.native_thread.try_lock() {
                Ok(thread) => thread,
                Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => return Err(DemandError::WorkerUnavailable),
            }
        } else {
            self.shared.native_thread.lock().unwrap_or_else(PoisonError::into_inner)
        };
        if let Some(wait) = wait { wait.check().map_err(|_| DemandError::Cancelled)?; }
        if self.shared.native_retiring.load(Ordering::Acquire) {
            return Err(if self.shared.native_attempt_finished.load(Ordering::Acquire) {
                DemandError::NativeWorkUnavailable
            } else { DemandError::WorkerUnavailable });
        }
        let running = thread.is_some();
        let wake = running && self.shared.native_ready.load(Ordering::Acquire)
            && self.shared.completed.is_open();
        let required_reads = u64::from(reads.pending);
        let required_writes = u64::from(reads.pending) + u64::from(wake) + u64::from(running)
            + u64::from(self.shared.directory.is_open()) + u64::from(self.shared.completed.is_open());
        {
            let cleanup = self.shared.native_cleanup.lock().unwrap_or_else(PoisonError::into_inner);
            let active = cleanup[0].as_ref().ok_or(DemandError::NativeWorkUnavailable)?.remaining_work();
            if active.backend_read_operations < required_reads || active.backend_write_operations < required_writes {
                return Err(DemandError::NativeWorkUnavailable);
            }
        }
        self.shared.native_retiring.store(true, Ordering::Release);
        self.shared.stopping.store(true, Ordering::Release);
        self.shared.delivery.exact.store(false, Ordering::Release);
        drop(thread);
        if wake {
            self.shared.native_start(false, 0, 1)?;
            // SAFETY: admission still holds this original native state lock;
            // the worker cannot release the completion event during its wake.
            let signalled = unsafe { SetEvent(HANDLE(self.shared.completed.as_raw_handle())) };
            #[cfg(test)]
            if let Some(wait) = self.shared.native_wait_cancellation.lock()
                .unwrap_or_else(PoisonError::into_inner).take()
            { wait.cancel(); }
            if let Err(error) = signalled {
                self.shared.note_native_failure(self.shared.native_os_error(error));
                drop(reads);
                self.shared.reading_changed.notify_all();
                let failure = self.shared.native_failure.lock().unwrap_or_else(PoisonError::into_inner).take()
                    .unwrap_or(DemandError::NativeWorkUnavailable);
                return Err(work.with_original_leaf(DemandWork::take_native_io_failure).unwrap_or(failure));
            }
        }
        self.shared.reading_changed.notify_all();
        if !running {
            drop(reads);
            self.shared.finish_native_reader();
            reads = self.shared.lock_reads();
        }
        while !self.shared.native_attempt_finished.load(Ordering::Acquire) {
            reads = self.shared.reading_changed.wait(reads).unwrap_or_else(PoisonError::into_inner);
        }
        let failure = self.shared.native_failure.lock().unwrap_or_else(PoisonError::into_inner).take();
        let failure = work.with_original_leaf(DemandWork::take_native_io_failure).or(failure);
        if !self.is_stopped() {
            return Err(failure.unwrap_or(DemandError::NativeWorkUnavailable));
        }
        drop(reads);
        let result = failure.map_or(Ok(()), Err);
        match wait {
            Some(wait) => result.and(wait.check().map_err(|_| DemandError::Cancelled)),
            None => result,
        }
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.shared.stop_completed.load(Ordering::Acquire)
    }

    pub(crate) fn platform_owned_bytes(&self) -> u64 {
        super::arc_control_bytes::<Shared>() + size_of::<OVERLAPPED>() as u64
            + if self.shared.buffer_owned.load(Ordering::Acquire) { size_of::<[u64; BUFFER_WORDS]>() as u64 } else { 0 }
    }

    pub(crate) fn fence(&self) -> io::Result<()> {
        self.fence_for(FENCE_TIMEOUT)
    }

    pub(crate) fn fence_for(&self, timeout: Duration) -> io::Result<()> {
        let mut work = self.shared.delivery.callback_work();
        self.fence_with_work(timeout, &mut work).map(|_| ()).map_err(ordinary_error)
    }

    pub(crate) fn fence_with_work<W: SourceDemandReadWork>(
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
                work.admit(WorkCounters {
                    backend_write_operations: 1, backend_read_operations: 1,
                    source_bytes_read: size_of::<[u64; BUFFER_WORDS]>() as u64,
                    ..WorkCounters::default()
                })?;
                work.started_io(WorkCounters { backend_write_operations: 1, ..WorkCounters::default() })?;
                // SAFETY: completes only the original queue's pending read.
                let _ = unsafe { CancelIoEx(HANDLE(self.shared.directory.as_raw_handle()),
                    Some(&raw const *reads.overlapped)) };
                self.shared.finish_read(&mut reads, true, work)?;
            }
            self.shared.drain(&mut reads, Some((started, timeout)), work)
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
        } else if matches!(result, Ok(true)) && !self.shared.complete.load(Ordering::Acquire) {
            self.shared.delivery.deliver_wide_with_work(&[HostChangeWide::Unconfirmed], work)?;
            return Ok(false);
        }
        result
    }

    pub(crate) fn delivery(&self) -> &Delivery {
        &self.shared.delivery
    }

    /// Holds delivery back while the returned guard lives.
    #[cfg(test)]
    pub(crate) fn paused(&self) -> impl Sized + '_ {
        self.shared.lock_reads()
    }
}

impl Drop for NativeSourceWatch {
    fn drop(&mut self) {
        if self.is_stopped() { return; }
        if self.shared.delivery.original.is_some() {
            self.shared.native_drop_requested.store(true, Ordering::Release);
            let terminal_wake = self.shared.native_retiring.swap(true, Ordering::AcqRel);
            self.shared.stopping.store(true, Ordering::Release);
            let in_callback = self.shared.delivery.ensure_stop_available().is_err();
            let reads = if in_callback { None } else { Some(self.shared.lock_reads()) };
            if !self.shared.native_entered_retirement.load(Ordering::Acquire)
                && self.shared.native_ready.load(Ordering::Acquire)
                && self.shared.completed.is_open()
            {
                if let Err(error) = self.shared.native_start(terminal_wake, 0, 1) {
                    self.shared.note_native_failure(error);
                } else if let Err(error) = unsafe { SetEvent(HANDLE(self.shared.completed.as_raw_handle())) } {
                    self.shared.note_native_failure(self.shared.native_os_error(error));
                }
            }
            drop(reads);
            self.shared.reading_changed.notify_all();
            return;
        }
        let mut work = self.shared.delivery.callback_work();
        if let Err(error) = self.stop_engine(&mut work, true, None) {
            self.shared.delivery.failed_with_work(error, &mut work);
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
            if reads.pending && reader.delivery.original.is_none() {
                // SAFETY: the original storage remains live until a real
                // completed or aborted query proves kernel quiescence.
                let completed = unsafe {
                    let _ = CancelIoEx(HANDLE(reader.directory.as_raw_handle()),
                        Some(&raw const *reads.overlapped));
                    let mut transferred = 0;
                    GetOverlappedResult(HANDLE(reader.directory.as_raw_handle()),
                        &raw const *reads.overlapped, &mut transferred, true)
                };
                reads.pending = !matches!(&completed, Ok(()))
                    && !completed.as_ref().err().is_some_and(|error|
                        error.code() == ERROR_OPERATION_ABORTED.to_hresult()
                            || error.code() == ERROR_NOTIFY_ENUM_DIR.to_hresult());
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
        self.native_entered_retirement.store(true, Ordering::Release);
        self.stopping.store(true, Ordering::Release);
        self.delivery.exact.store(false, Ordering::Release);
        let mut terminal = false;
        loop {
            let mut reads = self.lock_reads();
            let mut result = Ok(());
            if reads.pending {
                if let Err(error) = self.native_start(terminal, 0, 1) {
                    result = Err(error);
                } else {
                    // SAFETY: the same worker and original storage retain
                    // custody through cancellation and real completion.
                    if let Err(error) = unsafe { CancelIoEx(HANDLE(self.directory.as_raw_handle()),
                        Some(&raw const *reads.overlapped)) }
                        && error.code() != ERROR_NOT_FOUND.to_hresult()
                    {
                        result = Err(self.native_os_error(error));
                    }
                    if let Err(error) = self.native_start(terminal, 1, 0) {
                        result = result.and(Err(error));
                    } else {
                        let mut transferred = 0;
                        let completed = unsafe { GetOverlappedResult(HANDLE(self.directory.as_raw_handle()),
                            &raw const *reads.overlapped, &mut transferred, true) };
                        reads.pending = !matches!(&completed, Ok(()))
                            && !completed.as_ref().err().is_some_and(|error|
                                error.code() == ERROR_OPERATION_ABORTED.to_hresult()
                                    || error.code() == ERROR_NOTIFY_ENUM_DIR.to_hresult());
                        if let Err(error) = completed
                            && error.code() != ERROR_OPERATION_ABORTED.to_hresult()
                        {
                            result = result.and(Err(self.native_os_error(error)));
                        }
                    }
                }
            }
            if !reads.pending {
                result = result.and(self.close_native_watch(&self.directory, terminal));
                result = result.and(self.close_native_watch(&self.completed, terminal));
                if !self.directory.is_open() && !self.completed.is_open() {
                    if let Some(buffer) = reads.buffer.take() {
                        drop(buffer);
                        result = result.and(self.ownership.retire(size_of::<[u64; BUFFER_WORDS]>() as u64));
                    }
                    self.buffer_owned.store(false, Ordering::Release);
                    reads.filled = 0;
                    reads.cursor = 0;
                    result = result.and(self.close_native_thread(terminal));
                }
            }
            if let Err(error) = result { self.note_native_failure(error); }
            self.native_attempt_finished.store(true, Ordering::Release);
            self.reading_changed.notify_all();
            self.fence_delivered.notify_all();
            if !reads.pending && reads.buffer.is_none() && !self.directory.is_open() && !self.completed.is_open() {
                if self.native_thread.lock().unwrap_or_else(PoisonError::into_inner).is_none() {
                    let cleanup = {
                        let mut cleanup = self.native_cleanup.lock().unwrap_or_else(PoisonError::into_inner);
                        std::mem::replace(&mut *cleanup, [None, None])
                    };
                    drop(cleanup);
                    self.stop_completed.store(true, Ordering::Release);
                    self.reading_changed.notify_all();
                    return;
                }
            }
            while terminal || !self.native_drop_requested.load(Ordering::Acquire) {
                reads = self.reading_changed.wait(reads).unwrap_or_else(PoisonError::into_inner);
            }
            terminal = true;
        }
    }

    fn native_start(&self, terminal: bool, reads: u64, writes: u64) -> Result<(), DemandError> {
        let actual = WorkCounters {
            backend_read_operations: reads, backend_write_operations: writes, ..WorkCounters::UNCHARGED
        };
        let mut cleanup = self.native_cleanup.lock().unwrap_or_else(PoisonError::into_inner);
        let credit = cleanup[usize::from(terminal)].as_mut().ok_or(DemandError::NativeWorkUnavailable)?;
        let remaining = credit.remaining_work();
        if remaining.backend_read_operations < reads || remaining.backend_write_operations < writes {
            return Err(DemandError::NativeWorkUnavailable);
        }
        credit.started_drop_io(actual);
        Ok(())
    }

    fn native_os_error(&self, error: windows::core::Error) -> DemandError {
        let error = io::Error::from(error);
        let original = error.raw_os_error().map(io::Error::from_raw_os_error)
            .unwrap_or_else(|| io::Error::from(error.kind()));
        if let Some(birth) = &self.delivery.original {
            crate::NativeMaterializationWork::clone(birth).with_original_leaf(|work| {
                work.record_native_io_failure(DemandError::Io(original));
                work.poison_native_original_work();
            });
        }
        DemandError::Io(error)
    }

    fn close_watch<W: SourceDemandReadWork>(&self, handle: &WatchHandle, work: &mut W) -> Result<(), DemandError> {
        if !handle.is_open() { return Ok(()); }
        let closed = handle.close().map_err(io::Error::from).map_err(DemandError::from);
        closed.and(self.record_cleanup(work, 0, 1))
    }

    fn close_native_watch(&self, handle: &WatchHandle, terminal: bool) -> Result<(), DemandError> {
        if !handle.is_open() { return Ok(()); }
        self.native_start(terminal, 0, 1)?;
        handle.close().map_err(|error| self.native_os_error(error))
    }

    fn close_native_thread(&self, terminal: bool) -> Result<(), DemandError> {
        let mut thread = self.native_thread.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(handle) = thread.as_ref() else { return Ok(()); };
        self.native_start(terminal, 0, 1)?;
        // SAFETY: closing this original opaque handle is bookkeeping, not a
        // claim of a completed join. Failure keeps exactly this owned handle.
        unsafe { CloseHandle(HANDLE(handle.as_raw_handle())) }.map_err(|error| self.native_os_error(error))?;
        let _ = thread.take().expect("closed original native thread handle").into_raw_handle();
        Ok(())
    }

    fn note_native_failure(&self, error: DemandError) {
        if let Some(birth) = &self.delivery.original {
            crate::NativeMaterializationWork::clone(birth).with_original_leaf(|work| {
                if matches!(&error, DemandError::NativeWorkUnavailable) {
                    work.record_native_io_failure(DemandError::NativeWorkUnavailable);
                }
                work.poison_native_original_work();
            });
        }
        let mut failure = self.native_failure.lock().unwrap_or_else(PoisonError::into_inner);
        if failure.is_none() { *failure = Some(error); }
    }

    fn record_cleanup<W: SourceDemandReadWork>(&self, work: &mut W, reads: u64, writes: u64) -> Result<(), DemandError> {
        assert!(self.delivery.original.is_none(), "native disposal uses its birth-paid START token");
        work.record(WorkCounters {
            backend_read_operations: reads, backend_write_operations: writes, ..WorkCounters::default()
        })
    }

    fn lock_reads(&self) -> MutexGuard<'_, Reads> {
        self.reads.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn run(&self) {
        if self.delivery.original.is_some() {
            let mut reads = self.lock_reads();
            while !self.native_ready.load(Ordering::Acquire) && !self.stopping.load(Ordering::Acquire) {
                reads = self.reading_changed.wait(reads).unwrap_or_else(PoisonError::into_inner);
            }
        }
        loop {
            if self.stopping.load(Ordering::Acquire) { return; }
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
            match self.drain(&mut reads, None, &mut work) {
                Ok(_) => {}
                Err(error) => {
                    reads.blocked = true;
                    self.delivery.failed_with_work(error, &mut work);
                }
            }
        }
    }

    fn issue_read<W: SourceDemandReadWork>(&self, reads: &mut Reads, work: &mut W) -> Result<(), DemandError> {
        let Reads { overlapped, buffer, .. } = reads;
        let buffer = buffer.as_mut().ok_or_else(|| io::Error::from(io::ErrorKind::BrokenPipe))?;
        **overlapped = OVERLAPPED {
            hEvent: HANDLE(self.completed.as_raw_handle()),
            ..OVERLAPPED::default()
        };
        work.started_io(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
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
    fn finish_read<W: SourceDemandReadWork>(
        &self, reads: &mut Reads, wait: bool, work: &mut W,
    ) -> Result<bool, DemandError> {
        if !reads.pending { return Ok(true); }
        if !wait {
            work.admit(WorkCounters {
                source_bytes_read: size_of::<[u64; BUFFER_WORDS]>() as u64,
                ..WorkCounters::default()
            })?;
        }
        let delta = WorkCounters { backend_read_operations: 1, ..WorkCounters::default() };
        let recorded = if wait { work.record(delta) } else { work.started_io(delta)?; Ok(()) };
        let mut transferred = 0;
        // SAFETY: the original owned buffers stay pinned through completion.
        let result = unsafe { GetOverlappedResult(HANDLE(self.directory.as_raw_handle()),
            &raw const *reads.overlapped, &mut transferred, wait) };
        match result {
            Err(error) if error.code() == ERROR_IO_INCOMPLETE.to_hresult() => return recorded.map(|_| false),
            Err(error) if error.code() == ERROR_OPERATION_ABORTED.to_hresult() => {
                reads.pending = false;
                return recorded.map(|_| true);
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
        recorded?;
        Ok(true)
    }

    fn drain<W: SourceDemandReadWork>(
        &self, reads: &mut Reads, deadline: Option<(std::time::Instant, Duration)>, work: &mut W,
    ) -> Result<bool, DemandError> {
        loop {
            if self.stopping.load(Ordering::Acquire) { return Ok(false); }
            work.check()?;
            if deadline.is_some_and(|(started, timeout)| started.elapsed() >= timeout) { return Ok(false); }
            if reads.loss {
                self.delivery.deliver_wide_with_work(&[HostChangeWide::Everything], work)?;
                reads.loss = false;
            }
            if !self.deliver_page(reads, deadline, work)? { return Ok(false); }
            if self.stopping.load(Ordering::Acquire) { return Ok(false); }
            if reads.pending {
                if !self.finish_read(reads, false, work)? {
                    return Ok(self.delivery.is_exact());
                }
                continue;
            }
            let budget = work.with_original_leaf(|work| work.budget);
            if self.delivery.original.is_none() { reserve_cleanup(work, 1, 1)?; }
            let issued = (|| {
                work.admit(WorkCounters {
                    source_bytes_read: size_of::<[u64; BUFFER_WORDS]>() as u64,
                    ..WorkCounters::default()
                })?;
                self.issue_read(reads, work)?;
                self.finish_read(reads, false, work)
            })();
            if self.delivery.original.is_none() { work.with_original_leaf(|work| work.budget = budget); }
            match issued {
                Ok(false) => return Ok(self.delivery.is_exact()),
                Ok(true) => {}
                Err(error) => {
                    return Err(error);
                }
            }
        }
    }

    /// Uses the existing flexible fields, borrowing aligned UTF-16 directly from the original native u64 page instead of constructing an OsString.
    fn deliver_page<W: SourceDemandReadWork>(
        &self, reads: &mut Reads, deadline: Option<(std::time::Instant, Duration)>, work: &mut W,
    ) -> Result<bool, DemandError> {
        let extended = self.extended.load(Ordering::Acquire);
        while reads.cursor < reads.filled {
            if self.stopping.load(Ordering::Acquire) { return Ok(false); }
            if deadline.is_some_and(|(started, timeout)| started.elapsed() >= timeout) { return Ok(false); }
            work.started_io(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
            // SAFETY: only the completed page is exposed, under the read lock.
            let buffer = reads.buffer.as_ref().ok_or_else(|| io::Error::from(io::ErrorKind::BrokenPipe))?;
            let bytes = unsafe { std::slice::from_raw_parts(buffer.as_ptr().cast::<u8>(), reads.filled) };
            let record = &bytes[reads.cursor..];
            let parsed = notification(record, extended);
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

/// Opens the held root itself again (an empty name relative to it), for asynchronous change reads, pinning its name while it is open.
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
    work.started_io(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
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

fn notification(record: &[u8], extended: bool) -> Option<(usize, u32, &[u16], Option<u64>)> {
    let (name_offset, length_offset) = if extended {
        (std::mem::offset_of!(FILE_NOTIFY_EXTENDED_INFORMATION, FileName),
        std::mem::offset_of!(FILE_NOTIFY_EXTENDED_INFORMATION, FileNameLength))
    } else {
        (std::mem::offset_of!(FILE_NOTIFY_INFORMATION, FileName),
        std::mem::offset_of!(FILE_NOTIFY_INFORMATION, FileNameLength))
    };
    let field = |at: usize| record.get(at..at + 4)
        .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok()).map(u32::from_le_bytes);
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
    // SAFETY: the checked aligned span contains exactly length / 2 units.
    let path = unsafe { std::slice::from_raw_parts(name.as_ptr().cast::<u16>(), length / 2) };
    Some((next, action, path, file))
}

#[cfg(test)]
mod tests {
    use super::{Delivery, DemandError, DemandWork, HostChange, HostChangeWide, OVERLAPPED, Shared, notification};
    use std::mem::size_of;

    struct Quiet;
    impl super::super::HostChangeSink for Quiet {
        fn logical_owned_bytes(&self) -> u64 { 0 }
        fn host_changed(&self, _: &[HostChange], work: &mut DemandWork) -> Result<(), DemandError> { work.check() }
        fn host_changed_wide(&self, _: &[HostChangeWide<'_>], work: &mut DemandWork) -> Result<(), DemandError> { work.check() }
    }

    /// One report as the file system lays it out: the fixed fields, then the name, padded to the next record.
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

    #[test]
    fn reports_borrow_native_names_and_identity_in_either_layout() {
        for extended in [true, false] {
            let mut bytes = record(extended, 1, 7, r"d\created", false);
            bytes.extend(record(extended, 3, 9, "written", true));
            let (next, action, name, file) = notification(&bytes, extended).expect("created report");
            assert_eq!(action, 1);
            assert_eq!(name, r"d\created".encode_utf16().collect::<Vec<_>>());
            assert_eq!(file, extended.then_some(7));
            let (next, action, name, file) = notification(&bytes[next..], extended).expect("modified report");
            assert_eq!(next, 0);
            assert_eq!(action, 3);
            assert_eq!(name, "written".encode_utf16().collect::<Vec<_>>());
            assert_eq!(file, extended.then_some(9));
        }
    }

    #[test]
    fn flexible_utf16_notification_names_are_not_scalar_decoded() {
        for extended in [false, true] {
            let mut bytes = vec![0_u8; if extended { 84 } else { 12 }];
            bytes[4..8].copy_from_slice(&3_u32.to_le_bytes());
            let offset = if extended { 80 } else { 8 };
            bytes[offset..offset + 4].copy_from_slice(&2_u32.to_le_bytes());
            bytes.extend_from_slice(&0xD800_u16.to_le_bytes());
            let (_, action, name, _) = notification(&bytes, extended).expect("opaque report");
            assert_eq!(action, 3);
            assert_eq!(name, &[0xD800]);
        }
    }

    #[test]
    fn short_startup_budget_denies_the_resident_cleanup_suffix_before_native_io()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::source_watch::NativeSourceWatch;
        use crate::demand::{DemandError, DemandWork};
        use crate::{CancellationToken, WorkBudget};
        use std::sync::Arc;
        let root = tempfile::tempdir()?;
        let held = cap_std::fs::Dir::open_ambient_dir(root.path(), cap_std::ambient_authority())?;
        let mut work = DemandWork::new(WorkBudget {
            backend_read_operations: 1,
            ..WorkBudget::UNBOUNDED
        }, &CancellationToken::new());
        let failure = NativeSourceWatch::start_directory_with_work(&held, Arc::new(Quiet), true, &mut work)
            .err().ok_or("startup unexpectedly exceeded its finite native allowance")?;
        assert!(matches!(failure, DemandError::Work(_)));
        assert_eq!(work.work.backend_read_operations, 0, "the complete native cleanup suffix precedes the first handle request");
        assert_eq!(work.work.backend_write_operations, 0, "denied shutdown custody creates no native event or directory handle");
        Ok(())
    }

    #[test]
    fn stop_denial_and_cancellation_retain_the_same_native_owner()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::source_watch::NativeSourceWatch;
        use crate::{CancellationToken, WorkBudget, WorkCounters};
        use std::os::windows::io::AsRawHandle as _;
        use std::sync::Arc;
        let root = tempfile::tempdir()?;
        let held = cap_std::fs::Dir::open_ambient_dir(root.path(), cap_std::ambient_authority())?;
        let mut startup = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
        let watch = NativeSourceWatch::start_directory_with_work(&held, Arc::new(Quiet), true, &mut startup)?;
        assert!(!watch.shared.recursive);
        let directory = watch.shared.directory.as_raw_handle();
        let completed = watch.shared.completed.as_raw_handle();
        let owned = watch.platform_owned_bytes();
        assert!(!watch.is_stopped());
        assert!(matches!(
            watch.stop_with_cleanup_wait(&CancellationToken::new()),
            Err(DemandError::NativeWorkUnavailable),
        ));
        assert!(matches!(watch.stop_accepted_cleanup(), Err(DemandError::NativeWorkUnavailable)));
        assert_eq!(watch.platform_owned_bytes(), owned);
        let reads = watch.shared.lock_reads();
        let observed = std::thread::scope(|scope| {
            let (sender, receiver) = std::sync::mpsc::channel();
            let watch = &watch;
            scope.spawn(move || { let _ = sender.send(watch.platform_owned_bytes()); });
            let observed = receiver.recv_timeout(std::time::Duration::from_secs(1));
            drop(reads);
            observed
        })?;
        assert_eq!(observed, owned, "readonly payload custody cannot wait on a callback-owned read lock");
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancelled);
        assert!(matches!(watch.stop_with_work(&mut work), Err(DemandError::Cancelled)));
        assert_eq!(work.work, WorkCounters::default());
        assert!(!watch.is_stopped());
        let mut denied = DemandWork::new(WorkBudget {
            backend_read_operations: 1, backend_write_operations: 4, ..WorkBudget::UNBOUNDED
        }, &CancellationToken::new());
        assert!(matches!(watch.stop_with_work(&mut denied), Err(DemandError::Work(_))));
        assert_eq!(denied.work, WorkCounters::default());
        assert!(!watch.is_stopped());
        assert!(watch.is_exact());
        assert_eq!(watch.shared.directory.as_raw_handle(), directory);
        assert_eq!(watch.shared.completed.as_raw_handle(), completed);
        assert_eq!(watch.platform_owned_bytes(), owned);
        let mut admitted = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
        watch.stop_with_work(&mut admitted)?;
        assert_eq!(admitted.work.allocation_operations, 0);
        assert_eq!(watch.platform_owned_bytes(), owned - size_of::<[u64; super::BUFFER_WORDS]>() as u64);
        assert!(!watch.shared.directory.is_open());
        assert!(!watch.shared.completed.is_open());
        assert!(watch.thread.lock().unwrap_or_else(std::sync::PoisonError::into_inner).is_none());
        assert!(!watch.is_exact());
        assert!(watch.is_stopped());
        Ok(())
    }

    #[cfg(feature = "native-mount")]
    #[test]
    fn cleanup_wait_preserves_cancelled_birth_and_finishes_late_cancellation()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::{NativeDemandReadMeter,  SourceDemandReadWork};
        use crate::native_host::NativeHostWork;
        use crate::{CancellationToken, WorkBudget, WorkCounters};
        use std::sync::{Arc, Mutex};

        for late_wait in [false, true] {
            let root = tempfile::tempdir()?;
            let held = cap_std::fs::Dir::open_ambient_dir(root.path(), cap_std::ambient_authority())?;
            let root_cancellation = CancellationToken::new();
            let budget = WorkBudget {
                backend_read_operations: 64, backend_write_operations: 64,
                items_examined: 4096, allocation_operations: 128,
                peak_allocation_bytes: 4 * 1024 * 1024, bytes_copied: 4 * 1024 * 1024,
                ..WorkBudget::default()
            };
            let birth = Arc::new(NativeDemandReadMeter(Mutex::new(DemandWork::new(budget, &root_cancellation))));
            let mut startup = crate::NativeMaterializationWork::from_meter((&birth).clone());
            let delivery = Arc::new(Delivery::new_with_work(Arc::new(Quiet), &mut startup)?);
            let watch = super::NativeSourceWatch::start_directory_driver(
                &held, delivery, true, &mut startup,
            )?;
            let owned = watch.platform_owned_bytes();
            let before = birth.with_original_leaf(|work| work.work);
            let held_budget = birth.with_original_leaf(|work| work.budget);
            let wait = CancellationToken::new();
            let declined = CancellationToken::new();
            declined.cancel();
            assert!(matches!(watch.stop_with_cleanup_wait(&declined), Err(DemandError::Cancelled)));
            assert!(!watch.is_stopped());
            assert!(watch.shared.directory.is_open());
            assert!(watch.shared.completed.is_open());
            assert_eq!(watch.platform_owned_bytes(), owned);
            assert_eq!(birth.with_original_leaf(|work| (work.work, work.budget)), (before, held_budget));
            let paused = watch.paused();
            assert!(matches!(watch.stop_with_cleanup_wait(&wait), Err(DemandError::WorkerUnavailable)));
            drop(paused);
            assert_eq!(birth.with_original_leaf(|work| work.work), before);
            root_cancellation.cancel();
            assert!(matches!(watch.stop_with_work(&mut crate::NativeMaterializationWork::from_meter((&birth).clone())), Err(DemandError::Cancelled)));
            if late_wait {
                *watch.shared.native_wait_cancellation.lock().map_err(|_| "poisoned wait control")? =
                    Some(wait.clone());
                assert!(matches!(watch.stop_with_cleanup_wait(&wait), Err(DemandError::Cancelled)));
                assert!(wait.is_cancelled(), "the independent token cancels after actual SetEvent");
            } else {
                watch.stop_with_cleanup_wait(&wait)?;
            }
            assert!(watch.is_stopped());
            assert!(!watch.shared.directory.is_open());
            assert!(!watch.shared.completed.is_open());
            assert!(watch.shared.native_thread.lock().map_err(|_| "poisoned native thread custody")?.is_none());
            let stopped = birth.with_original_leaf(|work| {
                assert_eq!(work.work.allocation_operations, before.allocation_operations);
                assert_eq!(work.work.peak_allocation_bytes, before.peak_allocation_bytes);
                assert_eq!(work.budget, budget);
                assert!(work.cancellation().is_cancelled());
                assert!(matches!(work.check(), Err(DemandError::Cancelled)));
                work.work
            });
            watch.stop_with_cleanup_wait(&CancellationToken::new())?;
            watch.stop_accepted_cleanup()?;
            drop(watch);
            assert_eq!(birth.with_original_leaf(|work| work.work), stopped);
        }
        Ok(())
    }

    #[cfg(feature = "native-mount")]
    struct NativeQuiet;

    #[cfg(feature = "native-mount")]
    impl super::super::HostChangeSink for NativeQuiet {
        fn logical_owned_bytes(&self) -> u64 { 0 }
        fn host_changed(&self, _: &[HostChange], work: &mut DemandWork) -> Result<(), DemandError> {
            work.check()
        }
        fn host_changed_wide(&self, _: &[HostChangeWide<'_>], work: &mut DemandWork) -> Result<(), DemandError> { work.check() }
        fn host_changed_wide_with_native_work(
            &self, _: &[HostChangeWide<'_>], work: &mut crate::NativeMaterializationWork,
        ) -> Result<(), DemandError> { crate::native_host::NativeHostWork::check(work) }
    }

    #[cfg(feature = "native-mount")]
    fn native_control_budget() -> crate::WorkBudget {
        crate::WorkBudget {
            backend_read_operations: 64, backend_write_operations: 64,
            items_examined: 4096, allocation_operations: 128,
            peak_allocation_bytes: 4 * 1024 * 1024, bytes_copied: 4 * 1024 * 1024,
            source_bytes_read: 4 * 1024 * 1024, ..crate::WorkBudget::default()
        }
    }

    #[cfg(feature = "native-mount")]
    #[test]
    fn native_birth_denies_the_known_final_drop_plan_before_the_first_native_owner()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::{NativeDemandReadMeter};
        use std::sync::{Arc, Mutex};
        let root = tempfile::tempdir()?;
        let held = cap_std::fs::Dir::open_ambient_dir(root.path(), cap_std::ambient_authority())?;
        let mut budget = native_control_budget();
        budget.backend_write_operations = 9;
        let birth = Arc::new(NativeDemandReadMeter(Mutex::new(DemandWork::new(
            budget, &crate::CancellationToken::new(),
        ))));
        let mut work = crate::NativeMaterializationWork::from_meter((&birth).clone());
        let delivery = Arc::new(Delivery::new_with_work(Arc::new(NativeQuiet), &mut work)?);
        let delivery_lifetime = Arc::downgrade(&delivery);
        assert!(matches!(super::NativeSourceWatch::start_directory_driver(
            &held, delivery, true, &mut work,
        ), Err(DemandError::Work(_))));
        assert!(delivery_lifetime.upgrade().is_none(), "no native worker/handle was born");
        birth.with_original_leaf(|work| {
            assert_eq!(work.work.backend_read_operations, 0);
            assert_eq!(work.work.backend_write_operations, 0);
            assert_eq!(work.budget, budget);
        });
        Ok(())
    }

    #[cfg(feature = "native-mount")]
    #[test]
    fn accepted_cleanup_and_poisoned_last_drop_use_the_same_born_worker()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::{NativeDemandReadMeter};
        use std::sync::{Arc, Mutex};
        for poisoned_drop in [false, true] {
            let root = tempfile::tempdir()?;
            let held = cap_std::fs::Dir::open_ambient_dir(root.path(), cap_std::ambient_authority())?;
            let cancellation = crate::CancellationToken::new();
            let budget = native_control_budget();
            let birth = Arc::new(NativeDemandReadMeter(Mutex::new(DemandWork::new(budget, &cancellation))));
            let mut work = crate::NativeMaterializationWork::from_meter((&birth).clone());
            let delivery = Arc::new(Delivery::new_with_work(Arc::new(NativeQuiet), &mut work)?);
            let watch = super::NativeSourceWatch::start_directory_driver(&held, delivery, true, &mut work)?;
            let completion = Arc::clone(&watch.shared);
            let reads = completion.lock_reads();
            let before = birth.with_original_leaf(|work| work.work);
            let live = birth.with_original_leaf(|work| work.live_allocation_bytes());
            cancellation.cancel();
            if poisoned_drop { birth.with_original_leaf(DemandWork::poison_native_original_work); }
            drop(reads);
            if !poisoned_drop {
                watch.stop_accepted_cleanup()?;
                assert!(watch.is_stopped());
            }
            drop(watch);
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while !completion.stop_completed.load(super::Ordering::Acquire)
                && std::time::Instant::now() < deadline
            { std::thread::sleep(std::time::Duration::from_millis(1)); }
            assert!(completion.stop_completed.load(super::Ordering::Acquire));
            assert!(!completion.directory.is_open());
            assert!(!completion.completed.is_open());
            assert!(completion.native_thread.lock().map_err(|_| "poisoned native thread custody")?.is_none());
            assert!(!completion.buffer_owned.load(super::Ordering::Acquire));
            birth.with_original_leaf(|work| {
                assert_eq!(work.work.allocation_operations, before.allocation_operations);
                assert_eq!(work.work.peak_allocation_bytes, before.peak_allocation_bytes);
                assert_eq!(work.budget, budget);
                assert!(work.live_allocation_bytes() > 0, "Shared/control custody is not zeroed at the retirement flag");
                assert!(work.live_allocation_bytes() < live, "only the physically dropped buffer retires early");
                if poisoned_drop {
                    assert!(matches!(work.take_native_io_failure(), Some(DemandError::OriginalWorkPoisoned)));
                }
            });
            drop(completion);
        }
        Ok(())
    }

    #[cfg(feature = "native-mount")]
    #[test]
    fn actual_close_failure_parks_the_same_owner_and_preserves_the_original_cause()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::{NativeDemandReadMeter};
        use std::sync::{Arc, Mutex};
        use std::os::windows::io::AsRawHandle as _;
        use windows::Win32::Foundation::{
            HANDLE, HANDLE_FLAGS, HANDLE_FLAG_PROTECT_FROM_CLOSE, STILL_ACTIVE, SetHandleInformation,
        };
        use windows::Win32::System::Threading::GetExitCodeThread;
        struct CloseProtection(Arc<Shared>);
        impl Drop for CloseProtection {
            fn drop(&mut self) {
                let _reads = self.0.lock_reads();
                unsafe { SetHandleInformation(HANDLE(self.0.directory.as_raw_handle()),
                    HANDLE_FLAG_PROTECT_FROM_CLOSE.0, HANDLE_FLAGS(0)) }
                    .expect("clear actual owned-directory fixture protection");
            }
        }
        let root = tempfile::tempdir()?;
        let held = cap_std::fs::Dir::open_ambient_dir(root.path(), cap_std::ambient_authority())?;
        let cancellation = crate::CancellationToken::new();
        let budget = native_control_budget();
        let birth = Arc::new(NativeDemandReadMeter(Mutex::new(DemandWork::new(budget, &cancellation))));
        let mut work = crate::NativeMaterializationWork::from_meter((&birth).clone());
        let mut control = super::super::prepare_native_cleanup_credit(&mut work, crate::WorkCounters {
            backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED
        })?.ok_or("missing original test-control credit")?;
        let delivery = Arc::new(Delivery::new_with_work(Arc::new(NativeQuiet), &mut work)?);
        let watch = super::NativeSourceWatch::start_directory_driver(&held, delivery, true, &mut work)?;
        let completion = Arc::clone(&watch.shared);
        let owner = Arc::downgrade(&watch.shared);
        let reads = completion.lock_reads();
        let directory = completion.directory.as_raw_handle();
        let cursor = reads.cursor;
        unsafe { SetHandleInformation(HANDLE(directory), HANDLE_FLAG_PROTECT_FROM_CLOSE.0,
            HANDLE_FLAG_PROTECT_FROM_CLOSE) }.map_err(std::io::Error::from)?;
        let protection = CloseProtection(Arc::clone(&completion));
        let before = birth.with_original_leaf(|work| work.work);
        cancellation.cancel();
        drop(reads);
        let failure = watch.stop_with_cleanup_wait(&crate::CancellationToken::new())
            .err().ok_or("a protected original directory cannot become a completed stop")?;
        assert!(matches!(failure, DemandError::Io(error) if error.raw_os_error().is_some()),
            "the original SDK close error is returned, not a synthetic refusal");
        assert!(!watch.is_stopped());
        assert!(owner.upgrade().is_some());
        assert!(completion.directory.is_open());
        assert_eq!(completion.directory.as_raw_handle(), directory, "failed close preserves actual identity");
        assert!(completion.buffer_owned.load(super::Ordering::Acquire));
        assert_eq!(completion.lock_reads().cursor, cursor);
        assert!(matches!(watch.stop_with_cleanup_wait(&crate::CancellationToken::new()),
            Err(DemandError::NativeWorkUnavailable)));
        {
            let thread = completion.native_thread.lock().map_err(|_| "poisoned retained native thread")?;
            let thread = thread.as_ref().ok_or("the original quarantine worker must remain owned")?;
            control.started_drop_io(crate::WorkCounters {
                backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED
            });
            let mut exit = 0;
            unsafe { GetExitCodeThread(HANDLE(thread.as_raw_handle()), &mut exit) }.map_err(std::io::Error::from)?;
            assert_eq!(exit, STILL_ACTIVE.0 as u32, "the original worker genuinely parks with its same Shared/root/G");
        }
        birth.with_original_leaf(|work| {
            assert!(matches!(work.check(), Err(DemandError::OriginalWorkPoisoned)));
            assert_eq!(work.work.allocation_operations, before.allocation_operations);
            assert_eq!(work.work.peak_allocation_bytes, before.peak_allocation_bytes);
        });
        drop(protection);
        drop(watch);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !completion.stop_completed.load(super::Ordering::Acquire)
            && std::time::Instant::now() < deadline
        { std::thread::sleep(std::time::Duration::from_millis(1)); }
        assert!(completion.stop_completed.load(super::Ordering::Acquire));
        assert!(!completion.directory.is_open());
        assert!(!completion.completed.is_open());
        assert!(!completion.buffer_owned.load(super::Ordering::Acquire));
        assert!(completion.native_thread.lock().map_err(|_| "poisoned retired native thread")?.is_none());
        birth.with_original_leaf(|work| {
            assert_eq!(work.budget, budget);
            assert!(work.live_allocation_bytes() > 0, "actual Shared/G data still has its funded owner after the flag");
        });
        drop(completion);
        while owner.upgrade().is_some() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(owner.upgrade().is_none(), "the real owner disappears only after its physical data drop");
        Ok(())
    }

    #[test]
    fn startup_denies_handle_cleanup_before_opening_the_directory()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::source_watch::NativeSourceWatch;
        use crate::{CancellationToken, WorkBudget};
        use std::sync::Arc;
        let root = tempfile::tempdir()?;
        let held = cap_std::fs::Dir::open_ambient_dir(root.path(), cap_std::ambient_authority())?;
        let mut work = DemandWork::new(WorkBudget {
            backend_write_operations: 0, ..WorkBudget::UNBOUNDED
        }, &CancellationToken::new());
        assert!(matches!(NativeSourceWatch::start_directory_with_work(
            &held, Arc::new(Quiet), true, &mut work,
        ), Err(DemandError::Work(_))));
        assert_eq!(work.work.backend_read_operations, 0);
        assert_eq!(work.work.backend_write_operations, 0);
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
                let unavailable = matches!(watch.stop_with_work(&mut stop), Err(DemandError::WorkerUnavailable));
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
        let watch = Arc::new(NativeSourceWatch::start_directory_with_work(
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
        watch.stop_with_work(&mut stop)?;
        assert!(watch.is_stopped());
        let mut repeated = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        watch.stop_with_work(&mut repeated)?;
        assert_eq!(repeated.work, WorkCounters::default());
        Ok(())
    }
}
