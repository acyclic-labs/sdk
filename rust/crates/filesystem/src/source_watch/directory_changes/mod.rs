//! Bounded Windows directory notifications.
//!
//! Every watch's overlapped reads complete on one process-wide completion
//! port, which one thread serves (`hub`). A watch holds only its directory
//! handle and its read slot: an `OVERLAPPED` and a buffer that starts at
//! 4 KiB and doubles, up to 64 KiB, each time the kernel drops changes.
#![allow(unsafe_code, reason = "owns Win32 overlapped directory reads")]

#[cfg(test)]
use super::HostChange;
use super::{Delivery, HostChangeWide};
use crate::demand::{DemandError, DemandWork, SourceDemandReadWork, SourcePayloadOwnership};
use crate::performance::WorkCounters;
use std::io;
use std::mem::size_of;
use std::os::windows::io::{AsRawHandle, FromRawHandle, IntoRawHandle, OwnedHandle};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};
use windows::Win32::Foundation::{
    CloseHandle, ERROR_INVALID_FUNCTION, ERROR_IO_INCOMPLETE, ERROR_NOT_FOUND,
    ERROR_NOTIFY_ENUM_DIR, ERROR_OPERATION_ABORTED, HANDLE,
};
use windows::Win32::Storage::FileSystem::{
    FILE_LIST_DIRECTORY, FILE_NOTIFY_CHANGE_ATTRIBUTES, FILE_NOTIFY_CHANGE_CREATION,
    FILE_NOTIFY_CHANGE_DIR_NAME, FILE_NOTIFY_CHANGE_FILE_NAME, FILE_NOTIFY_CHANGE_LAST_WRITE,
    FILE_NOTIFY_CHANGE_SECURITY, FILE_NOTIFY_CHANGE_SIZE, FILE_NOTIFY_EXTENDED_INFORMATION,
    FILE_NOTIFY_INFORMATION, FILE_SHARE_READ, FILE_SHARE_WRITE, ReadDirectoryChangesExW,
    ReadDirectoryNotifyExtendedInformation, ReadDirectoryNotifyInformation,
};
use windows::Win32::System::IO::{CancelIoEx, GetOverlappedResult, OVERLAPPED};

mod hub;
mod reader;
mod shutdown;
mod startup;
#[cfg(test)]
use reader::notification;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_burst;
#[cfg(test)]
mod tests_rename;

/// A new watch's read buffer, and so the kernel's change buffer for its
/// directory handle, which the first read sizes for the handle's lifetime.
const INITIAL_BUFFER_BYTES: u64 = 4 * 1024;
/// The most a watch's buffer grows to after the kernel drops its changes.
const MAXIMUM_BUFFER_BYTES: u64 = 64 * 1024;
const READ: WorkCounters = io_work(1, 0);
const WRITE: WorkCounters = io_work(0, 1);
const ITEM: WorkCounters = WorkCounters {
    items_examined: 1,
    ..WorkCounters::UNCHARGED
};
const fn source(bytes: u64) -> WorkCounters {
    WorkCounters {
        source_bytes_read: bytes,
        ..WorkCounters::UNCHARGED
    }
}
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
    extended: AtomicBool,
    complete: AtomicBool,
    recursive: bool,
    /// Whether the directory handle denies deletion of its name.
    pin_name: bool,
    /// The most the read buffer grows to.
    maximum_buffer_bytes: u64,
    reads: Mutex<Reads>,
    /// The bytes of the read buffer this watch owns; zero once retired.
    buffer_bytes: AtomicU64,
    /// Signalled each time the hub collects a packet for this watch, which
    /// completes the read it names: a read's status and bytes land when the
    /// port's thread removes its packet, not when the kernel finishes it.
    arrived: Condvar,
    arrivals: Mutex<()>,
    /// The hub found the reads held; their holder wakes it on release.
    missed: AtomicBool,
    stopping: AtomicBool,
    stop_completed: AtomicBool,
    /// A native retirement failed on its terminal credit; nothing retries it.
    parked: AtomicBool,
    ownership: SourcePayloadOwnership,
    cleanup: Mutex<[Option<crate::materializer::NativeWorkReservation>; 2]>,
    dropped: AtomicBool,
    #[cfg(test)]
    native_wait_cancellation: Mutex<Option<crate::CancellationToken>>,
    delivery: Arc<Delivery>,
}
struct Reads {
    overlapped: Box<OVERLAPPED>,
    buffer: Option<Box<[std::mem::MaybeUninit<u64>]>>,
    pending: bool,
    filled: usize,
    cursor: usize,
    loss: bool,
    blocked: bool,
    /// The pending read was cancelled; its completion is still to come.
    cancelling: bool,
}
// SAFETY: reads serialize access; pending kernel writes keep their storage pinned.
unsafe impl Send for Reads {}
/// `STATUS_PENDING`: an `OVERLAPPED`'s status until its read completes.
const STATUS_PENDING: usize = 0x103;
impl Reads {
    /// Whether the pending read's completion has been collected.
    fn collected(&self) -> bool {
        // SAFETY: the port's thread writes the status while collecting the
        // completion; the box stays pinned while a read is pending.
        unsafe { std::ptr::read_volatile(&raw const self.overlapped.Internal) != STATUS_PENDING }
    }
    /// The read buffer's bytes; zero once retired.
    fn capacity(&self) -> u64 {
        self.buffer
            .as_deref()
            .map_or(0, |buffer| size_of_val(buffer) as u64)
    }
}

fn is_open(handle: &AtomicPtr<std::ffi::c_void>) -> bool {
    !handle.load(Ordering::Acquire).is_null()
}
fn close(handle: &AtomicPtr<std::ffi::c_void>) -> Result<(), DemandError> {
    // SAFETY: serialized after pending reads and callbacks have quiesced.
    unsafe { CloseHandle(HANDLE(handle.load(Ordering::Acquire))) }.map_err(io::Error::from)?;
    handle.store(std::ptr::null_mut(), Ordering::Release);
    Ok(())
}
fn buffer(bytes: u64) -> Box<[std::mem::MaybeUninit<u64>]> {
    Box::new_uninit_slice(usize::try_from(bytes).unwrap_or(0) / size_of::<u64>())
}
impl Drop for Shared {
    fn drop(&mut self) {
        assert!(
            self.delivery.original.is_none() || !is_open(&self.directory),
            "native owner must retire before release"
        );
        if is_open(&self.directory) {
            let _ = close(&self.directory);
        }
    }
}
/// The reads, held. Releasing them re-wakes the hub if it found them held.
struct ReadsGuard<'a> {
    shared: &'a Shared,
    reads: Option<MutexGuard<'a, Reads>>,
}
impl std::ops::Deref for ReadsGuard<'_> {
    type Target = Reads;
    fn deref(&self) -> &Reads {
        self.reads
            .as_deref()
            .unwrap_or_else(|| unreachable!("held until drop"))
    }
}
impl std::ops::DerefMut for ReadsGuard<'_> {
    fn deref_mut(&mut self) -> &mut Reads {
        self.reads
            .as_deref_mut()
            .unwrap_or_else(|| unreachable!("held until drop"))
    }
}
impl Drop for ReadsGuard<'_> {
    fn drop(&mut self) {
        drop(self.reads.take());
        // After the release: a hub that found the reads held set this first.
        if self.shared.missed.swap(false, Ordering::AcqRel) && hub::wake(self.shared).is_err() {
            self.shared.delivery.abandon();
        }
    }
}
impl Shared {
    fn lock_reads(&self) -> ReadsGuard<'_> {
        ReadsGuard {
            shared: self,
            reads: Some(self.reads.lock().unwrap_or_else(PoisonError::into_inner)),
        }
    }
    /// The reads, unless another thread holds them.
    fn try_lock_reads(&self) -> Option<ReadsGuard<'_>> {
        let reads = match self.reads.try_lock() {
            Ok(reads) => reads,
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return None,
        };
        Some(ReadsGuard {
            shared: self,
            reads: Some(reads),
        })
    }
    /// The hub collected a packet for this watch.
    fn collected(&self) {
        drop(self.arrivals.lock().unwrap_or_else(PoisonError::into_inner));
        self.arrived.notify_all();
    }
    /// Waits until the hub has collected the pending read's completion.
    /// The hub itself cannot wait for its own collection: it answers false,
    /// and the read's packet resumes whatever waited.
    fn await_completion(&self, reads: &Reads) -> bool {
        if reads.collected() {
            return true;
        }
        if hub::serving() {
            return false;
        }
        let mut arrivals = self.arrivals.lock().unwrap_or_else(PoisonError::into_inner);
        while !reads.collected() {
            arrivals = self
                .arrived
                .wait(arrivals)
                .unwrap_or_else(PoisonError::into_inner);
        }
        true
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
            + self.shared.buffer_bytes.load(Ordering::Acquire)
    }
    #[cfg(test)]
    pub(crate) fn paused(&self) -> impl Sized + '_ {
        self.shared.lock_reads()
    }
}
