//! Linux source notifications with bounded admission and cleanup.
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
mod lifecycle;
mod registration;
use cleanup::{clear_records, close, close_component, stop_custody};
use lifecycle::run_reader;
use registration::{
    contains_path_with_work, owned, prepare_insert_with_work, proc_fd_path, watch_ancestors,
};

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
    thread: Mutex<Option<libc::pthread_t>>,
}

struct Shared {
    inotify: AtomicI32,
    wake: AtomicI32,
    root: AtomicI32,
    ancestors: Mutex<Vec<(i32, std::ffi::OsString)>>,
    retained_variable_bytes: AtomicU64,
    reading: Mutex<RawReads>,
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

#[cfg(test)]
#[path = "tests.rs"]
mod admission_tests;
