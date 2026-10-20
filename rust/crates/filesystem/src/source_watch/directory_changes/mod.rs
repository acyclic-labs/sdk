//! Bounded Windows directory notifications.
#![allow(unsafe_code, reason = "owns Win32 overlapped directory reads")]

#[cfg(test)]
use super::HostChange;
use super::{Delivery, HostChangeWide};
use crate::demand::{DemandError, DemandWork, SourceDemandReadWork, SourcePayloadOwnership};
use crate::performance::WorkCounters;
use std::io;
use std::mem::size_of;
use std::os::windows::io::{AsRawHandle, FromRawHandle, IntoRawHandle, OwnedHandle};
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};
use windows::Win32::Foundation::{
    CloseHandle, ERROR_INVALID_FUNCTION, ERROR_IO_INCOMPLETE, ERROR_NOT_FOUND,
    ERROR_NOTIFY_ENUM_DIR, ERROR_OPERATION_ABORTED, HANDLE, WAIT_OBJECT_0,
};
use windows::Win32::Storage::FileSystem::{
    FILE_LIST_DIRECTORY, FILE_NOTIFY_CHANGE_ATTRIBUTES, FILE_NOTIFY_CHANGE_CREATION,
    FILE_NOTIFY_CHANGE_DIR_NAME, FILE_NOTIFY_CHANGE_FILE_NAME, FILE_NOTIFY_CHANGE_LAST_WRITE,
    FILE_NOTIFY_CHANGE_SECURITY, FILE_NOTIFY_CHANGE_SIZE, FILE_NOTIFY_EXTENDED_INFORMATION,
    FILE_NOTIFY_INFORMATION, FILE_SHARE_READ, FILE_SHARE_WRITE, ReadDirectoryChangesExW,
    ReadDirectoryNotifyExtendedInformation, ReadDirectoryNotifyInformation,
};
use windows::Win32::System::IO::{CancelIoEx, GetOverlappedResult, OVERLAPPED};
use windows::Win32::System::Threading::{
    CreateEventW, CreateThread, INFINITE, SetEvent, WaitForSingleObject,
};

mod reader;
mod shutdown;
mod startup;
#[cfg(test)]
use reader::notification;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_rename;

const BUFFER_WORDS: usize = 64 * 1024 / size_of::<u64>();
const BUFFER_BYTES: u64 = size_of::<[u64; BUFFER_WORDS]>() as u64;
const READ: WorkCounters = io_work(1, 0);
const WRITE: WorkCounters = io_work(0, 1);
const ITEM: WorkCounters = WorkCounters {
    items_examined: 1,
    ..WorkCounters::UNCHARGED
};
const SOURCE: WorkCounters = WorkCounters {
    source_bytes_read: BUFFER_BYTES,
    ..WorkCounters::UNCHARGED
};
const fn io_work(reads: u64, writes: u64) -> WorkCounters {
    WorkCounters {
        backend_read_operations: reads,
        backend_write_operations: writes,
        ..WorkCounters::UNCHARGED
    }
}

pub(crate) struct NativeSourceWatch {
    shared: Arc<Shared>,
}
struct Shared {
    directory: AtomicPtr<std::ffi::c_void>,
    completed: AtomicPtr<std::ffi::c_void>,
    thread: Mutex<Option<OwnedHandle>>,
    extended: AtomicBool,
    complete: AtomicBool,
    recursive: bool,
    reads: Mutex<Reads>,
    buffer_owned: AtomicBool,
    reading_changed: Condvar,
    stopping: AtomicBool,
    stop_completed: AtomicBool,
    ownership: SourcePayloadOwnership,
    cleanup: Mutex<[Option<crate::materializer::NativeWorkReservation>; 2]>,
    dropped: AtomicBool,
    #[cfg(test)]
    native_wait_cancellation: Mutex<Option<crate::CancellationToken>>,
    delivery: Arc<Delivery>,
}
struct Reads {
    overlapped: Box<OVERLAPPED>,
    buffer: Option<Box<std::mem::MaybeUninit<[u64; BUFFER_WORDS]>>>,
    pending: bool,
    filled: usize,
    cursor: usize,
    loss: bool,
    blocked: bool,
}
// SAFETY: reads serialize access; pending kernel writes keep their storage pinned.
unsafe impl Send for Reads {}

fn is_open(handle: &AtomicPtr<std::ffi::c_void>) -> bool {
    !handle.load(Ordering::Acquire).is_null()
}
fn close(handle: &AtomicPtr<std::ffi::c_void>) -> Result<(), DemandError> {
    // SAFETY: serialized after pending reads and callbacks have quiesced.
    unsafe { CloseHandle(HANDLE(handle.load(Ordering::Acquire))) }.map_err(io::Error::from)?;
    handle.store(std::ptr::null_mut(), Ordering::Release);
    Ok(())
}
impl Drop for Shared {
    fn drop(&mut self) {
        for handle in [&self.directory, &self.completed] {
            assert!(
                self.delivery.original.is_none() || !is_open(handle),
                "native owner must retire before release"
            );
            if is_open(handle) {
                let _ = close(handle);
            }
        }
    }
}
impl Shared {
    fn lock_reads(&self) -> MutexGuard<'_, Reads> {
        self.reads.lock().unwrap_or_else(PoisonError::into_inner)
    }
    fn note_failure(&self, error: DemandError) {
        if let Some(birth) = &self.delivery.original {
            birth.with_original_leaf(|work| {
                work.record_native_io_failure(error);
                work.poison_native_original_work();
            });
        }
    }
}
impl NativeSourceWatch {
    pub(super) fn read_authority(&self) -> Arc<Delivery> {
        Arc::clone(&self.shared.delivery)
    }
    pub(crate) fn delivery(&self) -> &Delivery {
        &self.shared.delivery
    }
    pub(crate) fn is_stopped(&self) -> bool {
        self.shared.stop_completed.load(Ordering::Acquire)
    }
    pub(crate) fn platform_owned_bytes(&self) -> u64 {
        super::arc_control_bytes::<Shared>()
            + size_of::<OVERLAPPED>() as u64
            + if self.shared.buffer_owned.load(Ordering::Acquire) {
                BUFFER_BYTES
            } else {
                0
            }
    }
    #[cfg(test)]
    pub(crate) fn paused(&self) -> impl Sized + '_ {
        self.shared.lock_reads()
    }
}
