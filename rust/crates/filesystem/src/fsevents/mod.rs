//! macOS `FSEvents`, loaded on first use.
//!
//! Linking `CoreServices` loads it and `CoreFoundation` into every process at
//! start, before `main`: about 1.1 ms of each start of an executable that most
//! of the time watches nothing, such as a hook. [`EventStream`] resolves the
//! few functions that it needs when the first one starts. It is the one
//! stream every watcher in the crate runs on: [`FsEventsWatcher`] translates
//! its events exactly as the `notify` `FSEvents` backend does, so it stands in
//! for that backend behind the same [`Watcher`] interface, and a native
//! source's watch reads them itself. Every stream is a registration on one
//! process-wide hub of a few shared native streams, since a process can run
//! only about 512 of them.

#![allow(
    unsafe_code,
    reason = "FSEvents is a C API resolved at run time; each call documents its contract"
)]

use crate::demand::{DemandWork, SourceDemandReadWork};
use crate::native_host::NativeHostWork as _;
use crate::{DemandError, WorkBudget, WorkCounters};
use notify::event::{
    CreateKind, DataChange, Flag, MetadataKind, ModifyKind, RemoveKind, RenameMode,
};
use notify::{Config, Error, Event, EventHandler, EventKind, RecursiveMode, Watcher, WatcherKind};
use std::collections::HashMap;
use std::ffi::{CStr, OsStr, c_char, c_void};
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

type CfTypeRef = *const c_void;
type FsEventStreamRef = *mut c_void;
type DispatchQueue = *mut c_void;

type FsEventStreamCallback = extern "C" fn(
    stream: FsEventStreamRef,
    info: *mut c_void,
    event_count: usize,
    event_paths: *mut c_void,
    event_flags: *const u32,
    event_ids: *const u64,
);

#[repr(C)]
struct FsEventStreamContext {
    version: isize,
    info: *mut c_void,
    retain: Option<extern "C" fn(*const c_void) -> *const c_void>,
    release: Option<extern "C" fn(*const c_void)>,
    copy_description: Option<extern "C" fn(*const c_void) -> CfTypeRef>,
}

unsafe extern "C" {
    // libdispatch is part of libSystem, which every process links.
    fn dispatch_queue_create(label: *const c_char, attributes: *const c_void) -> DispatchQueue;
    fn dispatch_async_f(
        queue: DispatchQueue,
        context: *mut c_void,
        work: extern "C" fn(*mut c_void),
    );
    fn dispatch_sync_f(
        queue: DispatchQueue,
        context: *mut c_void,
        work: extern "C" fn(*mut c_void),
    );
    #[cfg(test)]
    fn dispatch_release(object: DispatchQueue);
    fn dispatch_suspend(object: DispatchQueue);
    fn dispatch_resume(object: DispatchQueue);
}

const CF_STRING_ENCODING_UTF8: u32 = 0x0800_0100;
const EVENT_ID_SINCE_NOW: u64 = u64::MAX;
const CREATE_NO_DEFER: u32 = 0x02;
const CREATE_FILE_EVENTS: u32 = 0x10;

pub(crate) const MUST_SCAN_SUBDIRS: u32 = 0x0000_0001;
pub(crate) const USER_DROPPED: u32 = 0x0000_0002;
pub(crate) const KERNEL_DROPPED: u32 = 0x0000_0004;
const HISTORY_DONE: u32 = 0x0000_0010;
const ROOT_CHANGED: u32 = 0x0000_0020;
pub(crate) const MOUNT: u32 = 0x0000_0040;
pub(crate) const UNMOUNT: u32 = 0x0000_0080;
pub(crate) const ITEM_CREATED: u32 = 0x0000_0100;
pub(crate) const ITEM_REMOVED: u32 = 0x0000_0200;
const INODE_META_MOD: u32 = 0x0000_0400;
pub(crate) const ITEM_RENAMED: u32 = 0x0000_0800;
const ITEM_MODIFIED: u32 = 0x0000_1000;
const FINDER_INFO_MOD: u32 = 0x0000_2000;
const ITEM_CHANGE_OWNER: u32 = 0x0000_4000;
const ITEM_XATTR_MOD: u32 = 0x0000_8000;
const IS_FILE: u32 = 0x0001_0000;
const IS_DIR: u32 = 0x0002_0000;
const IS_SYMLINK: u32 = 0x0004_0000;
const OWN_EVENT: u32 = 0x0008_0000;
const IS_HARDLINK: u32 = 0x0010_0000;
const ITEM_CLONED: u32 = 0x0040_0000;

type StringCreateWithBytes =
    unsafe extern "C" fn(CfTypeRef, *const u8, isize, u32, u8) -> CfTypeRef;
type ArrayCreate =
    unsafe extern "C" fn(CfTypeRef, *const CfTypeRef, isize, *const c_void) -> CfTypeRef;
type Release = unsafe extern "C" fn(CfTypeRef);
type StreamCreate = unsafe extern "C" fn(
    CfTypeRef,
    FsEventStreamCallback,
    *const FsEventStreamContext,
    CfTypeRef,
    u64,
    f64,
    u32,
) -> FsEventStreamRef;
type StreamSetDispatchQueue = unsafe extern "C" fn(FsEventStreamRef, DispatchQueue);
type StreamStart = unsafe extern "C" fn(FsEventStreamRef) -> u8;
type StreamAction = unsafe extern "C" fn(FsEventStreamRef);
type CurrentEventId = unsafe extern "C" fn() -> u64;

fn native_error(message: &'static str) -> DemandError {
    DemandError::Io(std::io::Error::other(message))
}

fn notify_error(error: DemandError) -> Error {
    Error::from(crate::demand::native_demand_read_error(error))
}

/// Keep mandatory failure cleanup admissible without charging unused calls.
fn native_write<W: SourceDemandReadWork>(work: &mut W, cleanup: u64) -> Result<(), DemandError> {
    work.check()?;
    work.admit(WorkCounters {
        backend_write_operations: cleanup + 1,
        ..WorkCounters::default()
    })?;
    work.started_io(WorkCounters {
        backend_write_operations: 1,
        ..WorkCounters::default()
    })
}

fn native_allocate<W: SourceDemandReadWork>(work: &mut W, cleanup: u64) -> Result<(), DemandError> {
    work.check()?;
    work.admit(WorkCounters {
        backend_write_operations: cleanup + 1,
        allocation_operations: 1,
        ..WorkCounters::default()
    })?;
    work.started_io(WorkCounters {
        backend_write_operations: 1,
        allocation_operations: 1,
        ..WorkCounters::default()
    })
}

mod api;
mod callback;
mod hub;
mod notify_adapter;
mod stream;
use api::Api;
pub(crate) use callback::{BorrowedEventHandlerFn, EventBatch};
use callback::{OnQueue, drain, on_queue, terminal_suffix};
use hub::{Hub, Registration};
pub use notify_adapter::FsEventsWatcher;
pub(crate) use stream::{EventStream, StreamTerminal};
#[cfg(test)]
mod tests;
