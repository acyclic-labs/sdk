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

#![allow(
    unsafe_code,
    reason = "watches through the Win32 ReadDirectoryChangesW API"
)]

use super::{Delivery, HostChange};
use std::ffi::OsString;
use std::io;
use std::os::windows::ffi::OsStringExt as _;
use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use crate::demand::{DemandError, DemandWork};
use crate::performance::{WorkBudget, WorkCounters};
use windows::Win32::Foundation::{
    ERROR_INVALID_FUNCTION, ERROR_IO_INCOMPLETE, ERROR_NOTIFY_ENUM_DIR, ERROR_OPERATION_ABORTED,
    HANDLE, WAIT_OBJECT_0,
};
use windows::Win32::Storage::FileSystem::{
    FILE_ACTION_MODIFIED, FILE_LIST_DIRECTORY, FILE_NOTIFY_CHANGE_ATTRIBUTES,
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
/// How long a fence waits for the reader before it assumes everything
/// changed instead.
const FENCE_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) struct PlatformWatch {
    shared: Arc<Shared>,
    thread: Option<OwnedHandle>,
    owned_bytes: u64,
}

struct Shared {
    directory: OwnedHandle,
    /// Signaled when the pending read completes.
    completed: OwnedHandle,
    delivery: Arc<Delivery>,
    /// Whether reads report each entry's file identity too, which the file
    /// system decides once.
    extended: AtomicBool,
    /// Whether the file system reports every change or its loss (NTFS).
    complete: bool,
    recursive: bool,
    reads: Mutex<Reads>,
    /// Fences the reader thread has completed.
    fenced: Mutex<u64>,
    fence_delivered: Condvar,
    stopping: AtomicBool,
}

/// The one read in flight. Its buffer and `OVERLAPPED` stay in place until
/// the read completes.
struct Reads {
    overlapped: Box<OVERLAPPED>,
    buffer: Box<[u64; BUFFER_WORDS]>,
    pending: bool,
    /// Fences requested so far.
    requested: u64,
}

// SAFETY: the read's buffer and `OVERLAPPED` are touched only under the
// `reads` lock, and the kernel writes them only while a read is pending.
unsafe impl Send for Reads {}

impl PlatformWatch {
    pub(super) fn start(
        directory: &crate::native_host::HostRoot,
        delivery: Arc<Delivery>,
    ) -> io::Result<Self> {
        let cancellation = crate::cancellation::CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        Self::start_measured(directory, delivery, &mut work).map_err(ordinary_error)
    }

    pub(super) fn start_measured(
        directory: &crate::native_host::HostRoot,
        delivery: Arc<Delivery>,
        work: &mut DemandWork,
    ) -> Result<Self, DemandError> {
        let directory = open_for_changes_handle(directory.directory_handle(), true, work)?;
        let complete = crate::native_host::reports_every_change_measured(&directory, work)?;
        Self::start_opened_measured(directory, delivery, complete, true, work)
    }

    /// The same driver, observing one held child directory without pinning
    /// its name. The parent's held-root identity fence remains authoritative.
    pub(super) fn start_directory_measured(
        directory: &cap_std::fs::Dir,
        delivery: Arc<Delivery>,
        complete: bool,
        work: &mut DemandWork,
    ) -> Result<Self, DemandError> {
        use std::os::windows::io::AsHandle as _;
        let directory = open_for_changes_handle(directory.as_handle(), false, work)?;
        Self::start_opened_measured(directory, delivery, complete, false, work)
    }

    fn start_opened_measured(
        directory: OwnedHandle,
        delivery: Arc<Delivery>,
        complete: bool,
        recursive: bool,
        work: &mut DemandWork,
    ) -> Result<Self, DemandError> {
        let mut owned_bytes = 0_u64;
        let result = (|| {
            // Reserve the bounded fallback and mandatory failure cleanup
            // before allocating startup state or issuing any pending read.
            // Unused allowance is never added to the spent-work receipt.
            work.check()?;
            work.work.admit(&WorkCounters {
                backend_read_operations: 3,
                backend_write_operations: 3,
                ..WorkCounters::default()
            }, &work.budget)?;
            work.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::default() })?;
            // SAFETY: an unnamed manual-reset event.
            let completed = unsafe { CreateEventW(None, true, false, None) }
                .map_err(io::Error::from)?;
            // SAFETY: the successfully created handle is exclusively ours.
            let completed = unsafe { OwnedHandle::from_raw_handle(completed.0) };
            let overlapped_bytes = size_of::<OVERLAPPED>() as u64;
            work.allocate(overlapped_bytes)?;
            let overlapped = Box::new(OVERLAPPED::default());
            owned_bytes += overlapped_bytes;
            let buffer_bytes = size_of::<[u64; BUFFER_WORDS]>() as u64;
            work.allocate(buffer_bytes)?;
            let mut buffer = Box::<[u64; BUFFER_WORDS]>::new_uninit();
            // SAFETY: zero is valid for every u64, and initializes the whole
            // admitted buffer in place, without a 64-KiB stack copy.
            let buffer = unsafe {
                buffer.as_mut_ptr().write_bytes(0, 1);
                buffer.assume_init()
            };
            owned_bytes += buffer_bytes;
            let shared_bytes = size_of::<Shared>() as u64;
            work.allocate(shared_bytes)?;
            let shared = Arc::new(Shared {
                directory,
                completed,
                delivery,
                extended: AtomicBool::new(true),
                complete,
                recursive,
                reads: Mutex::new(Reads { overlapped, buffer, pending: false, requested: 0 }),
                fenced: Mutex::new(0),
                fence_delivered: Condvar::new(),
                stopping: AtomicBool::new(false),
            });
            owned_bytes += shared_bytes;
            let started = (|| {
                let mut reads = shared.lock_reads();
                if let Err(error) = shared.issue_read(&mut reads, Some(&mut *work)) {
                    if !matches!(&error, DemandError::Io(error)
                        if error.raw_os_error() == Some(ERROR_INVALID_FUNCTION.to_hresult().0))
                    {
                        return Err(error);
                    }
                    shared.extended.store(false, Ordering::Release);
                    shared.issue_read(&mut reads, Some(&mut *work))?;
                }
                drop(reads);
                work.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::default() })?;
                let reader = Arc::into_raw(Arc::clone(&shared));
                // SAFETY: the callback consumes exactly this strong reference
                // after successful creation. Failure returns ownership here.
                let thread = unsafe {
                    CreateThread(None, 0, Some(reader_thread), Some(reader.cast()),
                        Default::default(), None)
                };
                match thread {
                    Ok(thread) => {
                        // SAFETY: this successfully created thread handle is ours.
                        Ok(unsafe { OwnedHandle::from_raw_handle(thread.0) })
                    }
                    Err(error) => {
                        // SAFETY: no callback was created to consume the reference.
                        drop(unsafe { Arc::from_raw(reader) });
                        Err(io::Error::from(error).into())
                    }
                }
            })();
            match started {
                Ok(thread) => Ok(Self { shared, thread: Some(thread), owned_bytes }),
                Err(error) => {
                    shared.cancel_startup_measured(work)?;
                    Err(error)
                }
            }
        })();
        if result.is_err() {
            work.release(owned_bytes)?;
        }
        result
    }

    pub(super) fn owned_bytes(&self) -> u64 {
        self.owned_bytes
    }

    pub(super) fn stop_measured(&mut self, work: &mut DemandWork) -> Result<(), DemandError> {
        work.check()?;
        if self.thread.is_none() {
            return Ok(());
        }
        let reads = self.shared.lock_reads();
        work.work.admit(&WorkCounters {
            backend_write_operations: 1 + u64::from(reads.pending),
            backend_read_operations: 1,
            ..WorkCounters::default()
        }, &work.budget)?;
        self.shared.stopping.store(true, Ordering::Release);
        if reads.pending {
            work.record(WorkCounters { backend_write_operations: 1, ..WorkCounters::default() })?;
            // SAFETY: only this live directory's own pending read is cancelled.
            let _ = unsafe { CancelIoEx(HANDLE(self.shared.directory.as_raw_handle()), Some(&raw const *reads.overlapped)) };
        }
        work.record(WorkCounters { backend_write_operations: 1, ..WorkCounters::default() })?;
        // SAFETY: wakes the existing reader to finish and release its buffer.
        let _ = unsafe { SetEvent(HANDLE(self.shared.completed.as_raw_handle())) };
        drop(reads);
        if let Some(thread) = self.thread.as_ref() {
            work.record(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
            // SAFETY: the owned thread handle remains live through the wait.
            if unsafe { WaitForSingleObject(HANDLE(thread.as_raw_handle()), INFINITE) } != WAIT_OBJECT_0 {
                return Err(io::Error::last_os_error().into());
            }
            drop(self.thread.take());
        }
        Ok(())
    }

    pub(super) fn fence(&self) -> io::Result<()> {
        self.fence_for(FENCE_TIMEOUT)
    }

    pub(super) fn fence_for(&self, timeout: Duration) -> io::Result<()> {
        let shared = &self.shared;
        let fence = {
            let mut reads = shared.lock_reads();
            reads.requested += 1;
            if reads.pending {
                // Completes the read, which wakes the reader to drain.
                // SAFETY: cancels only this handle's read in flight.
                let _ = unsafe {
                    CancelIoEx(
                        HANDLE(shared.directory.as_raw_handle()),
                        Some(&raw const *reads.overlapped),
                    )
                };
            } else {
                // SAFETY: signals our own event so the reader looks again.
                let _ = unsafe { SetEvent(HANDLE(shared.completed.as_raw_handle())) };
            }
            reads.requested
        };
        let fenced = shared.fenced.lock().unwrap_or_else(PoisonError::into_inner);
        let (_fenced, timeout) = shared
            .fence_delivered
            .wait_timeout_while(fenced, timeout, |delivered| *delivered < fence)
            .unwrap_or_else(PoisonError::into_inner);
        if timeout.timed_out() {
            // Unproven delivery: assume everything changed instead.
            shared.delivery.deliver(&[HostChange::Everything]);
        } else if !shared.complete {
            shared.delivery.deliver(&[HostChange::Unconfirmed]);
        }
        Ok(())
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
        if self.thread.is_none() {
            return;
        }
        self.shared.stopping.store(true, Ordering::Release);
        {
            let reads = self.shared.lock_reads();
            // SAFETY: cancels our own read and wakes the reader.
            unsafe {
                if reads.pending {
                    let _ = CancelIoEx(
                        HANDLE(self.shared.directory.as_raw_handle()),
                        Some(&raw const *reads.overlapped),
                    );
                }
                let _ = SetEvent(HANDLE(self.shared.completed.as_raw_handle()));
            }
        }
        if let Some(thread) = self.thread.take() {
            // SAFETY: the owned thread handle is live until its reader exits.
            unsafe { WaitForSingleObject(HANDLE(thread.as_raw_handle()), INFINITE) };
        }
    }
}

unsafe extern "system" fn reader_thread(parameter: *mut std::ffi::c_void) -> u32 {
    // SAFETY: successful CreateThread transfers one Arc::into_raw reference.
    let reader = unsafe { Arc::<Shared>::from_raw(parameter.cast()) };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| reader.run()));
    if result.is_err() {
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
    }
}

impl Shared {
    fn lock_reads(&self) -> MutexGuard<'_, Reads> {
        self.reads.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn run(&self) {
        loop {
            // SAFETY: waits on our own live event.
            unsafe { WaitForSingleObject(HANDLE(self.completed.as_raw_handle()), INFINITE) };
            let mut reads = self.lock_reads();
            let fence = reads.requested;
            let mut changes = Vec::new();
            let drained = self
                .finish_read(&mut reads, true, &mut changes)
                .and_then(|_| {
                    if self.stopping.load(Ordering::Acquire) {
                        Ok(())
                    } else {
                        self.read_until_pending(&mut reads, &mut changes)
                    }
                });
            self.delivery.deliver(&changes);
            if drained.is_err() {
                self.delivery.abandon();
            }
            let stop = drained.is_err() || self.stopping.load(Ordering::Acquire);
            if stop {
                // A read still pending must finish before its buffer goes. It
                // may have been issued after the dropping thread cancelled
                // the one it saw, so it is cancelled here, where the lock
                // that issued it is held.
                if reads.pending {
                    // SAFETY: cancels only this handle's read in flight.
                    let _ = unsafe {
                        CancelIoEx(
                            HANDLE(self.directory.as_raw_handle()),
                            Some(&raw const *reads.overlapped),
                        )
                    };
                }
                let _ = self.finish_read(&mut reads, true, &mut Vec::new());
            }
            drop(reads);
            let mut fenced = self.fenced.lock().unwrap_or_else(PoisonError::into_inner);
            *fenced = (*fenced).max(fence);
            self.fence_delivered.notify_all();
            drop(fenced);
            if stop {
                return;
            }
            if !self.delivery.is_exact() {
                // Nothing more is read; later fences return at once.
                let mut fenced = self.fenced.lock().unwrap_or_else(PoisonError::into_inner);
                *fenced = u64::MAX;
                self.fence_delivered.notify_all();
                return;
            }
        }
    }

    /// Issues reads until one stays pending, collecting every read that
    /// completed at once.
    fn read_until_pending(
        &self,
        reads: &mut Reads,
        changes: &mut Vec<HostChange>,
    ) -> io::Result<()> {
        loop {
            self.issue_read(reads, None).map_err(ordinary_error)?;
            if !self.finish_read(reads, false, changes)? {
                return Ok(());
            }
        }
    }

    fn issue_read(&self, reads: &mut Reads, work: Option<&mut DemandWork>) -> Result<(), DemandError> {
        let Reads { overlapped, buffer, .. } = reads;
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

    fn cancel_startup_measured(&self, work: &mut DemandWork) -> Result<(), DemandError> {
        let mut reads = self.lock_reads();
        if !reads.pending {
            return Ok(());
        }
        work.record(WorkCounters { backend_write_operations: 1, ..WorkCounters::default() })?;
        // SAFETY: cancel only the pending read whose buffer is still held.
        let _ = unsafe { CancelIoEx(HANDLE(self.directory.as_raw_handle()), Some(&raw const *reads.overlapped)) };
        let mut transferred = 0_u32;
        work.record(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
        // SAFETY: wait for completion before freeing kernel-owned storage.
        // Startup failure does not decode or allocate discarded notifications.
        let _ = unsafe { GetOverlappedResult(HANDLE(self.directory.as_raw_handle()),
            &raw const *reads.overlapped, &raw mut transferred, true) };
        reads.pending = false;
        Ok(())
    }

    /// Takes the pending read's result into `changes`; false while it is
    /// still pending (only when not waiting).
    fn finish_read(
        &self,
        reads: &mut Reads,
        wait: bool,
        changes: &mut Vec<HostChange>,
    ) -> io::Result<bool> {
        if !reads.pending {
            return Ok(true);
        }
        let mut transferred = 0_u32;
        // SAFETY: the read's own `OVERLAPPED`, which is live.
        let result = unsafe {
            GetOverlappedResult(
                HANDLE(self.directory.as_raw_handle()),
                &raw const *reads.overlapped,
                &raw mut transferred,
                wait,
            )
        };
        match result {
            Err(error) if error.code() == ERROR_IO_INCOMPLETE.to_hresult() => return Ok(false),
            Ok(()) if transferred == 0 => changes.push(HostChange::Everything),
            Ok(()) => {
                let length = usize::try_from(transferred).unwrap_or(usize::MAX);
                // SAFETY: the completed read wrote `transferred` bytes into
                // the buffer, which is at least that long.
                let bytes = unsafe {
                    std::slice::from_raw_parts(
                        reads.buffer.as_ptr().cast::<u8>(),
                        length.min(size_of::<[u64; BUFFER_WORDS]>()),
                    )
                };
                translate(bytes, self.extended.load(Ordering::Acquire), changes);
            }
            Err(error) if error.code() == ERROR_OPERATION_ABORTED.to_hresult() => {}
            // The handle's own buffer overflowed: reports were lost.
            Err(error) if error.code() == ERROR_NOTIFY_ENUM_DIR.to_hresult() => {
                changes.push(HostChange::Everything);
            }
            Err(error) => {
                reads.pending = false;
                return Err(io::Error::from(error));
            }
        }
        reads.pending = false;
        Ok(true)
    }
}

/// Opens the held root itself again (an empty name relative to it), for
/// asynchronous change reads, pinning its name while it is open.
fn open_for_changes_handle(
    root: std::os::windows::io::BorrowedHandle<'_>,
    pin_name: bool,
    work: &mut DemandWork,
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
    use super::{HostChange, translate};
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
            fn host_changed(&self, _: &[HostChange]) {}
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
        let mut watch = NativeSourceWatch::start_directory_measured(&held, Arc::new(Ignored), true, &mut work)?;
        assert!(!watch.platform.shared.recursive);
        assert_eq!(work.work.allocation_operations, 4);
        assert_eq!(work.work.peak_allocation_bytes, watch.owned_bytes());
        assert!(work.work.backend_read_operations >= 2);
        assert_eq!(work.work.backend_write_operations, 2);
        assert_eq!(work.work.bytes_copied, 0, "native startup has no owned thread-name or closure buffer");
        let before = work.work.backend_write_operations;
        let before_reads = work.work.backend_read_operations;
        watch.stop_measured(&mut work)?;
        assert_eq!(work.work.backend_read_operations, before_reads + 1);
        assert!(work.work.backend_write_operations > before);
        assert!(work.work.backend_write_operations <= before + 2);
        Ok(())
    }

    #[test]
    fn short_startup_budget_stops_after_the_admitted_handle_request()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::source_watch::{HostChangeSink, NativeSourceWatch};
        use crate::demand::{DemandError, DemandWork};
        use crate::{CancellationToken, WorkBudget};
        use std::sync::Arc;
        struct Ignored;
        impl HostChangeSink for Ignored {
            fn host_changed(&self, _: &[HostChange]) {}
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
        assert_eq!(work.work.backend_read_operations, 1);
        assert_eq!(work.work.backend_write_operations, 0);
        assert_eq!(work.work.allocation_operations, 1);
        Ok(())
    }
}
