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

#![allow(unsafe_code)]

use super::{Delivery, HostChange};
use std::ffi::OsString;
use std::io;
use std::os::windows::ffi::OsStringExt as _;
use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;
use windows::Win32::Foundation::{
    ERROR_INVALID_FUNCTION, ERROR_IO_INCOMPLETE, ERROR_NOTIFY_ENUM_DIR, ERROR_OPERATION_ABORTED,
    HANDLE,
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
use windows::Win32::System::Threading::{CreateEventW, INFINITE, SetEvent, WaitForSingleObject};

/// Room for many reports per read; a local volume admits more than 64 KiB.
const BUFFER_WORDS: usize = 64 * 1024 / size_of::<u64>();
/// How long a fence waits for the reader before it assumes everything
/// changed instead.
const FENCE_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) struct PlatformWatch {
    shared: Arc<Shared>,
    thread: Option<JoinHandle<()>>,
}

struct Shared {
    directory: OwnedHandle,
    /// Signaled when the pending read completes.
    completed: OwnedHandle,
    delivery: Arc<Delivery>,
    /// Whether reads report each entry's file identity too, which the file
    /// system decides once.
    extended: AtomicBool,
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
        let directory = open_for_changes(directory)?;
        // SAFETY: an unnamed manual-reset event.
        let completed =
            unsafe { CreateEventW(None, true, false, None) }.map_err(io::Error::from)?;
        // SAFETY: as above.
        let completed = unsafe { OwnedHandle::from_raw_handle(completed.0) };
        let shared = Arc::new(Shared {
            directory,
            completed,
            delivery,
            extended: AtomicBool::new(true),
            reads: Mutex::new(Reads {
                overlapped: Box::default(),
                buffer: Box::new([0; BUFFER_WORDS]),
                pending: false,
                requested: 0,
            }),
            fenced: Mutex::new(0),
            fence_delivered: Condvar::new(),
            stopping: AtomicBool::new(false),
        });
        {
            let mut reads = shared.lock_reads();
            let mut changes = Vec::new();
            // The first read has nothing to complete: a failure here means
            // the root cannot be watched at all, unless only file
            // identities are what the file system cannot report.
            if let Err(error) = shared.read_until_pending(&mut reads, &mut changes) {
                if error.raw_os_error() != Some(ERROR_INVALID_FUNCTION.to_hresult().0) {
                    return Err(error);
                }
                shared.extended.store(false, Ordering::Release);
                shared.read_until_pending(&mut reads, &mut changes)?;
            }
        }
        let reader = Arc::clone(&shared);
        let thread = std::thread::Builder::new()
            .name("acyclic-source-watch".to_owned())
            .spawn(move || reader.run())?;
        Ok(Self {
            shared,
            thread: Some(thread),
        })
    }

    pub(super) fn fence(&self) -> io::Result<()> {
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
            .wait_timeout_while(fenced, FENCE_TIMEOUT, |delivered| *delivered < fence)
            .unwrap_or_else(PoisonError::into_inner);
        if timeout.timed_out() {
            // Unproven delivery: assume everything changed instead.
            shared.delivery.deliver(&[HostChange::Everything]);
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
            let _ = thread.join();
        }
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
                // A read still pending must finish before its buffer goes.
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
            let Reads {
                overlapped, buffer, ..
            } = reads;
            **overlapped = OVERLAPPED {
                hEvent: HANDLE(self.completed.as_raw_handle()),
                ..OVERLAPPED::default()
            };
            // SAFETY: the buffer and `OVERLAPPED` stay in place, under the
            // lock, until the read completes (see `finish_read`).
            unsafe {
                ReadDirectoryChangesExW(
                    HANDLE(self.directory.as_raw_handle()),
                    buffer.as_mut_ptr().cast(),
                    u32::try_from(size_of::<[u64; BUFFER_WORDS]>()).unwrap_or(u32::MAX),
                    true,
                    FILE_NOTIFY_CHANGE_FILE_NAME
                        | FILE_NOTIFY_CHANGE_DIR_NAME
                        | FILE_NOTIFY_CHANGE_ATTRIBUTES
                        | FILE_NOTIFY_CHANGE_SIZE
                        | FILE_NOTIFY_CHANGE_LAST_WRITE
                        | FILE_NOTIFY_CHANGE_CREATION
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
            }
            .map_err(io::Error::from)?;
            reads.pending = true;
            if !self.finish_read(reads, false, changes)? {
                return Ok(());
            }
        }
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
fn open_for_changes(root: &crate::native_host::HostRoot) -> io::Result<OwnedHandle> {
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
        RootDirectory: HANDLE(root.directory_handle().as_raw_handle()),
        ObjectName: &raw const name,
        ..OBJECT_ATTRIBUTES::default()
    };
    let mut handle = HANDLE::default();
    let mut status_block = IO_STATUS_BLOCK::default();
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
            FILE_SHARE_READ | FILE_SHARE_WRITE,
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
        ));
    }
    // SAFETY: a handle the call just returned is ours to own.
    Ok(unsafe { OwnedHandle::from_raw_handle(handle.0) })
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
        let units = name
            .chunks_exact(2)
            .map(|pair| <[u8; 2]>::try_from(pair).map_or(0, u16::from_le_bytes))
            .collect::<Vec<_>>();
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
}
