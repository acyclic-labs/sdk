//! Linux source notifications with bounded admission and cleanup.
//!
//! Every watch shares one process-wide inotify instance through [`hub`]; a
//! watch owns its registrations, its inbox of routed records and its held root.
#![allow(unsafe_code, reason = "watches through the Linux inotify API")]
use super::{
    Delivery, HostChange, arc_control_bytes, cancel_cleanup_credits, record_cleanup_credits,
    reserve_cleanup_credits,
};
use crate::demand::{DemandError, DemandWork, SourceDemandReadWork, SourcePayloadOwnership};
use crate::native_host::NativeHostWork;
use crate::performance::{WorkBudget, WorkCounters, WorkError};
use std::ffi::{CString, OsStr};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};

mod cleanup;
mod events;
mod hub;
mod lifecycle;
mod registration;
use cleanup::{clear_records, close, close_component, stop_custody};
use registration::{owned, proc_fd_path, watch_ancestors};

const READ: WorkCounters = WorkCounters {
    backend_read_operations: 1,
    ..WorkCounters::UNCHARGED
};
const WRITE: WorkCounters = WorkCounters {
    backend_write_operations: 1,
    ..WorkCounters::UNCHARGED
};
const ITEM: WorkCounters = WorkCounters {
    items_examined: 1,
    ..WorkCounters::UNCHARGED
};
pub(super) fn copied(bytes: u64) -> WorkCounters {
    WorkCounters {
        bytes_copied: bytes,
        ..WorkCounters::UNCHARGED
    }
}
const WATCHED: u32 = libc::IN_CREATE
    | libc::IN_DELETE
    | libc::IN_MODIFY
    | libc::IN_CLOSE_WRITE
    | libc::IN_ATTRIB
    | libc::IN_MOVED_FROM
    | libc::IN_MOVED_TO
    | libc::IN_DELETE_SELF
    | libc::IN_MOVE_SELF
    | libc::IN_ONLYDIR
    | libc::IN_EXCL_UNLINK;
const REBINDING: u32 = libc::IN_CREATE | libc::IN_DELETE | libc::IN_MOVED_FROM | libc::IN_MOVED_TO;
const ANCESTRAL: u32 = REBINDING | libc::IN_DELETE_SELF | libc::IN_MOVE_SELF | libc::IN_ONLYDIR;
const READ_BUFFER_BYTES: usize = 64 * 1024;
const EVENT_HEADER_BYTES: usize = 16;

pub(crate) struct NativeSourceWatch {
    shared: Arc<Shared>,
    /// Background delivery through the hub's service pool, until retired.
    worker: Mutex<Option<Worker>>,
}

/// Custody of background delivery for one watch.
#[derive(Clone, Copy, Debug)]
struct Worker;

struct Shared {
    /// The hub's descriptor while this watch holds hub registrations, else -1.
    /// The hub owns the descriptor; a watch withdraws its registrations
    /// instead of closing it.
    inotify: AtomicI32,
    root: AtomicI32,
    /// This watch's identity in the hub's routing tables.
    subscriber: u64,
    this: std::sync::Weak<Shared>,
    inbox: Mutex<hub::Inbox>,
    /// Background delivery ended: an error, a panic, or stop. Held while
    /// retirement runs so every retirer returns only once it completed.
    retired: Mutex<bool>,
    /// Set once retirement began: a service pass then returns at once
    /// instead of waiting on `retired` behind a retirer.
    retiring: AtomicBool,
    ancestors: Mutex<Vec<(i32, std::ffi::OsString)>>,
    retained_variable_bytes: AtomicU64,
    reading: Reading,
    reading_changed: Condvar,
    watches: Mutex<Vec<(PathBuf, i32)>>,
    stopping: AtomicBool,
    completed: AtomicBool,
    controller_alive: AtomicBool,
    #[cfg(test)]
    polling: AtomicBool,
    cleanup: Mutex<StartupCleanup>,
    ownership: SourcePayloadOwnership,
    delivery: Arc<Delivery>,
}

#[derive(Default)]
struct StartupCleanup {
    reads: u64,
    writes: u64,
    records: u64,
}
#[derive(Default)]
struct RawReads {
    buffer: Vec<u8>,
    filled: usize,
    cursor: usize,
    alias_cursor: usize,
    blocked: bool,
    cleanup: Vec<PathBuf>,
}

/// The read cursor, locked through [`Shared::reading`].
///
/// Every watch in the process is delivered by the hub's small service pool,
/// so a service pass never waits for this lock: a fence, a stop or a paused
/// test holding it would otherwise park pool threads and stall delivery to
/// every other watch for as long as it held on. A pass that finds the lock
/// held records that it is owed instead, and the holder reschedules the watch
/// when its guard drops.
#[derive(Default)]
struct Reading {
    raw: Mutex<RawReads>,
    owed: AtomicBool,
}

/// Exclusive use of a watch's read cursor; dropping it serves any pass that
/// found the cursor held.
struct ReadingGuard<'a> {
    // Fields drop in order: the cursor unlocks before `_served` runs.
    raw: std::sync::MutexGuard<'a, RawReads>,
    _served: OwedService<'a>,
}

/// Reschedules a watch whose service pass found its cursor held.
struct OwedService<'a>(&'a Shared);

impl Drop for OwedService<'_> {
    fn drop(&mut self) {
        // After the unlock: a pass that failed to lock had set `owed` before
        // its last attempt, which this holder's unlock had not yet released.
        if self.0.reading.owed.swap(false, Ordering::SeqCst) {
            if self.0.stopping.load(Ordering::Acquire) {
                // The owed pass may retire the watch with no record waiting.
                self.0.force_service();
            } else {
                self.0.reschedule();
            }
        }
    }
}

impl std::ops::Deref for ReadingGuard<'_> {
    type Target = RawReads;
    fn deref(&self) -> &RawReads {
        &self.raw
    }
}

impl std::ops::DerefMut for ReadingGuard<'_> {
    fn deref_mut(&mut self) -> &mut RawReads {
        &mut self.raw
    }
}

impl Shared {
    /// Locks the read cursor, waiting for its holder.
    fn reading(&self) -> ReadingGuard<'_> {
        ReadingGuard {
            raw: self.reading.raw.lock().unwrap_or_else(PoisonError::into_inner),
            _served: OwedService(self),
        }
    }

    /// Locks the read cursor if it is free.
    fn try_reading(&self) -> Option<ReadingGuard<'_>> {
        let raw = match self.reading.raw.try_lock() {
            Ok(raw) => raw,
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return None,
        };
        Some(ReadingGuard {
            raw,
            _served: OwedService(self),
        })
    }

    /// The read cursor for a service pass, without waiting: when it is held,
    /// the holder's guard reschedules this watch on release instead.
    fn service_reading(&self) -> Option<ReadingGuard<'_>> {
        if let Some(reading) = self.try_reading() {
            return Some(reading);
        }
        self.reading.owed.store(true, Ordering::SeqCst);
        // The holder may have released before `owed` was set; retry once so
        // that a release between the two attempts is never missed.
        let reading = self.try_reading()?;
        self.reading.owed.store(false, Ordering::SeqCst);
        Some(reading)
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod admission_tests;
