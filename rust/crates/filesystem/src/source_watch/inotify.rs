//! Linux: one inotify instance watching exactly the directories the source
//! reads, each before it is read.
//!
//! inotify has no recursive watch, so the source admits every directory on
//! the way to what it reads ([`PlatformWatch::admit`]). A fact about a path
//! is read after its parent (and every ancestor) is watched, so every later
//! change to the entries it depends on is reported: the kernel queues an
//! event before the system call that caused it returns.
//!
//! Watch descriptors name inodes, not paths, so this keeps the paths it
//! admitted each directory under. A directory moved, removed, or replaced
//! beneath a watched parent is reported by that parent, which invalidates
//! everything beneath the old name, and its paths are forgotten, so the
//! next read beneath the new name admits it again. A read that raced such a
//! change sampled its stamp before the change was recorded, so its fact is
//! invalidated too. A directory reached through a symbolic link is one inode
//! under two paths; each event on it is reported under both.
//!
//! The source fails closed once its root's path stops naming the held root,
//! so the watch also watches every ancestor of the root for the one name
//! on the way to it: once that name is removed, replaced, or moved, the
//! watch becomes inexact.
//!
//! Writes through a shared memory mapping raise no event of their own; the
//! writer's close of a descriptor opened for writing does, so they are
//! reported then, as NFS reports them (close-to-open).
//!
//! The native reader retains the complete original resource view and retires
//! its descriptors and buffered payload on every terminal exit, including an
//! unwound callback. A live controller joins it; when the final controller is
//! dropped inside delivery, the same reader finishes cleanup and only then
//! detaches its pthread bookkeeping. Detachment is not terminal evidence.

#![allow(unsafe_code, reason = "watches through the Linux inotify API")]

use super::{arc_control_bytes, Delivery, HostChange};
use std::ffi::{CString, OsStr};
use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _, IntoRawFd as _, OwnedFd};
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use crate::demand::{DemandError, DemandWork, SourceDemandReadWork, SourcePayloadOwnership};
use crate::native_host::NativeHostWork;
use crate::performance::{WorkBudget, WorkCounters, WorkError};

/// Every change to a directory's entries or to the directory itself.
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
/// Events that change which entry a name binds.
const REBINDING: u32 = libc::IN_CREATE | libc::IN_DELETE | libc::IN_MOVED_FROM | libc::IN_MOVED_TO;
/// Every change to the names an ancestor of the root holds, or to itself.
const ANCESTRAL: u32 = REBINDING | libc::IN_DELETE_SELF | libc::IN_MOVE_SELF | libc::IN_ONLYDIR;
/// Room for many events per read; the kernel never splits one.
const READ_BUFFER_BYTES: usize = 64 * 1024;
/// `struct inotify_event` before its name.
const EVENT_HEADER_BYTES: usize = 16;

pub(super) struct PlatformWatch {
    shared: Arc<Shared>,
    thread: Mutex<Option<libc::pthread_t>>,
}

struct Shared {
    inotify: HeldFd,
    /// Wakes the reader thread to stop.
    wake: HeldFd,
    /// The source root, which every watched path is relative to.
    root: HeldFd,
    /// Each ancestor of the root, by the name it holds on the way to it.
    ancestors: Mutex<Vec<(i32, std::ffi::OsString)>>,
    /// Actual variable payload, published at the existing admitted mutations.
    retained_variable_bytes: AtomicU64,
    /// Held while reading the queue and delivering what was read, so a
    /// fence returns only after everything queued before it is delivered.
    reading: Mutex<RawReads>,
    reading_changed: Condvar,
    watches: Mutex<Watches>,
    stopping: AtomicBool,
    /// Published only after actual native quiescence and full physical cleanup.
    completed: AtomicBool,
    controller_alive: AtomicBool,
    cleanup: Mutex<StartupCleanup>,
    ownership: SourcePayloadOwnership,
    delivery: Arc<Delivery>,
}

/// The reader's terminal cleanup or the startup owner closes these descriptors.
/// There is deliberately no native-I/O Drop.
struct HeldFd(AtomicI32);

impl HeldFd {
    fn take(&self) -> i32 {
        self.0.swap(-1, Ordering::AcqRel)
    }
}

impl std::os::fd::AsRawFd for HeldFd {
    fn as_raw_fd(&self) -> i32 {
        self.0.load(Ordering::Acquire)
    }
}

/// Withholds mandatory failure cleanup from the original invocation's
/// allowance before the corresponding native owner or retained record exists.
#[derive(Default)]
struct StartupCleanup {
    reads: u64,
    writes: u64,
    records: u64,
}

impl StartupCleanup {
    fn reserve<W: SourceDemandReadWork>(work: &mut W) -> Result<Self, DemandError> {
        let mut cleanup = Self::default();
        cleanup.extend(WorkCounters {
            backend_write_operations: 3, ..WorkCounters::UNCHARGED
        }, work)?;
        Ok(cleanup)
    }

    fn extend<W: SourceDemandReadWork>(&mut self, delta: WorkCounters, work: &mut W) -> Result<(), DemandError> {
        work.check()?;
        let reads = self.reads.checked_add(delta.backend_read_operations).ok_or(WorkError::Overflow)?;
        let writes = self.writes.checked_add(delta.backend_write_operations).ok_or(WorkError::Overflow)?;
        let records = self.records.checked_add(delta.items_examined).ok_or(WorkError::Overflow)?;
        work.with_original_leaf(|work| super::reserve_cleanup_credits(work, delta))?;
        self.reads = reads;
        self.writes = writes;
        self.records = records;
        Ok(())
    }

    fn reserve_record<W: SourceDemandReadWork>(&mut self, work: &mut W) -> Result<(), DemandError> {
        self.extend(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }, work)
    }

    fn reserve_worker<W: SourceDemandReadWork>(&mut self, work: &mut W) -> Result<(), DemandError> {
        let attempts = if work.is_native_original() { 2 } else { 1 };
        self.extend(WorkCounters {
            backend_read_operations: attempts, backend_write_operations: attempts,
            ..WorkCounters::UNCHARGED
        }, work)
    }

    fn record<W: SourceDemandReadWork>(&mut self, delta: WorkCounters, work: &mut W) -> Result<(), DemandError> {
        let reads = self.reads.checked_sub(delta.backend_read_operations).ok_or(DemandError::NativeWorkUnavailable)?;
        let writes = self.writes.checked_sub(delta.backend_write_operations).ok_or(DemandError::NativeWorkUnavailable)?;
        let records = self.records.checked_sub(delta.items_examined).ok_or(DemandError::NativeWorkUnavailable)?;
        self.reads = reads;
        self.writes = writes;
        self.records = records;
        work.with_original_leaf(|work| super::record_cleanup_credits(work, delta))
    }

    fn consume_record<W: SourceDemandReadWork>(&mut self, work: &mut W) -> Result<(), DemandError> {
        self.record(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }, work)
    }

    fn cancel<W: SourceDemandReadWork>(&mut self, work: &mut W) {
        work.with_original_leaf(|work| super::cancel_cleanup_credits(work, WorkCounters {
            backend_read_operations: self.reads, backend_write_operations: self.writes,
            items_examined: self.records, ..WorkCounters::UNCHARGED
        }));
        *self = Self::default();
    }

    fn covers(&self, delta: WorkCounters) -> bool {
        self.reads >= delta.backend_read_operations
            && self.writes >= delta.backend_write_operations
            && self.records >= delta.items_examined
    }

    fn extend_for<W: SourceDemandReadWork>(&mut self, delta: WorkCounters, work: &mut W) -> Result<(), DemandError> {
        let missing = WorkCounters {
            backend_read_operations: delta.backend_read_operations.saturating_sub(self.reads),
            backend_write_operations: delta.backend_write_operations.saturating_sub(self.writes),
            items_examined: delta.items_examined.saturating_sub(self.records),
            ..WorkCounters::UNCHARGED
        };
        if missing != WorkCounters::UNCHARGED { self.extend(missing, work)?; }
        Ok(())
    }
}

/// Linux close consumes the descriptor even on EINTR; never retry a reused fd.
fn close_measured<W: SourceDemandReadWork>(
    descriptor: i32, work: &mut W, record: impl FnOnce(&mut W) -> Result<(), DemandError>,
) -> Result<(), DemandError> {
    // SAFETY: exclusive custody was removed from its owner after quiescence.
    let status = unsafe { libc::close(descriptor) };
    let error = (status < 0).then(last_error);
    record(work)?;
    match error {
        Some(error) => Err(error.into()),
        None => Ok(()),
    }
}

fn close_component<W: SourceDemandReadWork>(
    (directory, reservation): (OwnedFd, u64), work: &mut W,
) -> Result<(), DemandError> {
    let descriptor = directory.into_raw_fd();
    let (error, recorded) = work.attempt_cleanup_write(reservation, || {
        // SAFETY: the consumed temporary directory owns this descriptor.
        (unsafe { libc::close(descriptor) } < 0).then(last_error)
    });
    recorded?;
    match error { Some(error) => Err(error.into()), None => Ok(()) }
}

fn clear_records<T, W: SourceDemandReadWork>(
    records: &mut Vec<T>,
    work: &mut W,
    retained: Option<&AtomicU64>,
    payload_bytes: fn(&T) -> u64,
    mut record_drop: impl FnMut(&mut W) -> Result<(), DemandError>,
) -> Result<(), DemandError> {
    let mut result = Ok(());
    while let Some(record) = records.pop() {
        let bytes = payload_bytes(&record);
        drop(record);
        if let Some(retained) = retained {
            retained.fetch_sub(bytes, Ordering::AcqRel);
        }
        let recorded = record_drop(work);
        result = result.and(recorded);
    }
    let backing_bytes = records.capacity() as u64 * std::mem::size_of::<T>() as u64;
    *records = Vec::new();
    if let Some(retained) = retained {
        retained.fetch_sub(backing_bytes, Ordering::AcqRel);
    }
    result
}

#[derive(Default)]
struct RawReads {
    buffer: Vec<u8>,
    filled: usize,
    cursor: usize,
    alias_cursor: usize,
    blocked: bool,
    cleanup: Vec<PathBuf>,
    cleanup_path_bytes: u64,
}

/// One deterministic owned buffer of path/descriptor records. Demand admission
/// can bound every comparison and every buffer growth without an opaque map
/// allocator or a second alias index.
#[derive(Default)]
struct Watches {
    entries: Vec<(PathBuf, i32)>,
    path_bytes: u64,
}

impl Watches {
    #[cfg(test)]
    fn contains_path(&self, path: &Path) -> bool {
        self.entries.iter().any(|(candidate, _)| candidate == path)
    }

    fn contains_path_with_work<W: SourceDemandReadWork>(
        &self, path: &Path, work: &mut W,
    ) -> Result<bool, DemandError> {
        for (candidate, _) in &self.entries {
            work.with_original_leaf(|work| work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() }))?;
            if candidate == path {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Reserves the real backing buffer and creates the one retained path
    /// before registration. Once the kernel succeeds, insertion is infallible.
    fn prepare_insert_with_work<W: SourceDemandReadWork>(
        &mut self,
        path: &Path,
        work: &mut W,
        retained: &AtomicU64,
        ownership: &SourcePayloadOwnership,
    ) -> Result<PathBuf, DemandError> {
        if self.entries.len() == self.entries.capacity() {
            let count = self.entries.len().checked_add(1).ok_or(WorkError::Overflow)?;
            let bytes = (count as u64)
                .checked_mul(std::mem::size_of::<(PathBuf, i32)>() as u64)
                .ok_or(WorkError::Overflow)?;
            let old_bytes = (self.entries.capacity() as u64)
                .checked_mul(std::mem::size_of::<(PathBuf, i32)>() as u64)
                .ok_or(WorkError::Overflow)?;
            let copied = WorkCounters { bytes_copied: old_bytes, ..WorkCounters::default() };
            work.allocate_for(bytes, copied)?;
            let mut grown = Vec::with_capacity(count);
            grown.append(&mut self.entries);
            self.entries = grown;
            let grown_bytes = self.entries.capacity() as u64 * std::mem::size_of::<(PathBuf, i32)>() as u64;
            retained.fetch_add(grown_bytes - old_bytes, Ordering::AcqRel);
            ownership.retain(bytes, work)?;
            ownership.retire(old_bytes)?;
            if !ownership.has_native_custody() { work.release(bytes)?; }
        }
        let bytes = path.as_os_str().len() as u64;
        let copied = WorkCounters { bytes_copied: bytes, ..WorkCounters::default() };
        work.allocate_for(bytes, copied)?;
        Ok(path.to_path_buf())
    }

}

fn last_error() -> io::Error {
    io::Error::last_os_error()
}

/// Renders the held descriptor without an owned formatting buffer.
fn proc_fd_path(descriptor: i32) -> ([u8; 24], usize) {
    let mut path = [0_u8; 24];
    let prefix = b"/proc/self/fd/";
    path[..prefix.len()].copy_from_slice(prefix);
    let mut digits = [0_u8; 10];
    let mut start = digits.len();
    let mut value = descriptor as u32;
    loop {
        start -= 1;
        digits[start] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    let length = prefix.len() + digits.len() - start;
    path[prefix.len()..length].copy_from_slice(&digits[start..]);
    (path, length)
}

/// Watches every ancestor of the held root for the name on the way to it.
/// All temporary payloads remain alive until their claims have been released,
/// including when cancellation interrupts a partially registered ancestry.
fn watch_ancestors<W: SourceDemandReadWork>(
    inotify: &OwnedFd,
    root: &OwnedFd,
    ancestors: &mut Vec<(i32, std::ffi::OsString)>,
    retained: &mut u64,
    cleanup: &mut StartupCleanup,
    work: &mut W,
) -> Result<(), DemandError> {
    use std::os::unix::ffi::OsStringExt as _;
    let (proc_path, _) = proc_fd_path(root.as_raw_fd());
    let mut path = Vec::new();
    let mut c_path = Vec::new();
    let mut pending_name = std::ffi::OsString::new();
    let mut temporary = 0_u64;
    let result = (|| {
        let mut capacity = 256_usize;
        loop {
            let bytes = u64::try_from(capacity).map_err(|_| WorkError::Overflow)?;
            let next_temporary = temporary.checked_add(bytes).ok_or(WorkError::Overflow)?;
            work.allocate(bytes)?;
            temporary = next_temporary;
            let grown = vec![0_u8; capacity];
            let old_bytes = path.capacity() as u64;
            drop(std::mem::replace(&mut path, grown));
            work.release(old_bytes)?;
            temporary -= old_bytes;
            let read = WorkCounters { source_bytes_read: bytes, ..WorkCounters::default() };
            work.preflight(read)?;
            work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
            // SAFETY: the stack C path and the complete writable buffer live
            // through readlink; a full buffer is retried, never truncated.
            let length = unsafe {
                libc::readlink(proc_path.as_ptr().cast(), path.as_mut_ptr().cast(), path.len())
            };
            if length < 0 {
                return Err(last_error().into());
            }
            work.record(WorkCounters { source_bytes_read: length as u64, ..WorkCounters::default() })?;
            if (length as usize) < capacity {
                path.truncate(length as usize);
                break;
            }
            capacity = capacity.checked_mul(2).ok_or(WorkError::Overflow)?;
        }
        let root_path = Path::new(OsStr::from_bytes(&path));
        let mut child = root_path;
        loop {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
            let (Some(parent), Some(name)) = (child.parent(), child.file_name()) else {
                break;
            };
            cleanup.reserve_record(work)?;
            let name_bytes = name.len() as u64;
            let copy = WorkCounters { bytes_copied: name_bytes, ..WorkCounters::default() };
            work.preflight(copy)?;
            let next_temporary = temporary.checked_add(name_bytes).ok_or(WorkError::Overflow)?;
            work.allocate(name_bytes)?;
            temporary = next_temporary;
            work.record(copy)?;
            pending_name = std::ffi::OsString::from_vec(name.as_bytes().to_vec());

            // Reserve the actual retained-record backing before registration.
            if ancestors.len() == ancestors.capacity() {
                let count = ancestors.len().checked_add(1).ok_or(WorkError::Overflow)?;
                let bytes = (count as u64)
                    .checked_mul(std::mem::size_of::<(i32, std::ffi::OsString)>() as u64)
                    .ok_or(WorkError::Overflow)?;
                let old_bytes = (ancestors.capacity() as u64)
                    .checked_mul(std::mem::size_of::<(i32, std::ffi::OsString)>() as u64)
                    .ok_or(WorkError::Overflow)?;
                let copied = WorkCounters { bytes_copied: old_bytes, ..WorkCounters::default() };
                work.preflight(copied)?;
                let next_retained = retained.checked_add(bytes).ok_or(WorkError::Overflow)?;
                work.allocate(bytes)?;
                *retained = next_retained;
                work.record(copied)?;
                let mut grown = Vec::with_capacity(count);
                grown.append(ancestors);
                *ancestors = grown;
                work.release(old_bytes)?;
                *retained -= old_bytes;
            }
            let parent_bytes = parent.as_os_str().len();
            let length = parent_bytes.checked_add(1).ok_or(WorkError::Overflow)?;
            let bytes = length as u64;
            let copy = WorkCounters { bytes_copied: parent_bytes as u64, ..WorkCounters::default() };
            work.preflight(copy)?;
            let next_temporary = temporary.checked_add(bytes).ok_or(WorkError::Overflow)?;
            work.allocate(bytes)?;
            temporary = next_temporary;
            work.record(copy)?;
            c_path = Vec::with_capacity(length);
            c_path.extend_from_slice(parent.as_os_str().as_bytes());
            c_path.push(0);
            work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
            // SAFETY: the complete NUL-terminated path lives through the call.
            let descriptor = unsafe {
                libc::inotify_add_watch(inotify.as_raw_fd(), c_path.as_ptr().cast(), ANCESTRAL)
            };
            if descriptor < 0 {
                return Err(last_error().into());
            }
            c_path = Vec::new();
            work.release(bytes)?;
            temporary -= bytes;
            let mut prior = None;
            for (index, (registered, _)) in ancestors.iter().enumerate() {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                if *registered == descriptor {
                    prior = Some(index);
                    break;
                }
            }
            if let Some(index) = prior {
                let old_bytes = ancestors[index].1.capacity() as u64;
                cleanup.consume_record(work)?;
                ancestors[index].1 = std::mem::take(&mut pending_name);
                work.release(old_bytes)?;
                *retained -= old_bytes;
            } else {
                ancestors.push((descriptor, std::mem::take(&mut pending_name)));
            }
            temporary -= name_bytes;
            *retained = retained.checked_add(name_bytes).ok_or(WorkError::Overflow)?;
            child = parent;
        }
        Ok(())
    })();
    drop(path);
    drop(c_path);
    drop(pending_name);
    work.release(temporary)?;
    result
}

/// The pthread owns exactly one raw Arc reference, not an opaque Rust thread
/// closure/name allocation. Panics never unwind across the native ABI.
extern "C" fn run_reader(payload: *mut libc::c_void) -> *mut libc::c_void {
    // SAFETY: startup transfers one Arc reference to this entrypoint.
    let reader = unsafe { Arc::from_raw(payload.cast::<Shared>()) };
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| reader.run())).is_err() {
        reader.delivery.abandon();
    }
    if reader.delivery.original.is_some() {
        reader.finish_native_reader();
    }
    std::ptr::null_mut()
}

fn owned(descriptor: i32) -> Result<OwnedFd, DemandError> {
    if descriptor < 0 {
        return Err(last_error().into());
    }
    // SAFETY: a non-negative descriptor a system call just returned is ours.
    Ok(unsafe { OwnedFd::from_raw_fd(descriptor) })
}

impl PlatformWatch {
    pub(super) fn start(
        directory: &crate::native_host::HostRoot,
        delivery: Arc<Delivery>,
    ) -> io::Result<Self> {
        let cancellation = crate::cancellation::CancellationToken::new();
        Self::start_measured(
            directory, delivery, &mut DemandWork::new(WorkBudget::UNBOUNDED, &cancellation),
        ).map_err(|error| match error {
            DemandError::Io(error) => error,
            error => io::Error::other(error),
        })
    }

    pub(super) fn start_measured(
        directory: &crate::native_host::HostRoot,
        delivery: Arc<Delivery>,
        work: &mut DemandWork,
    ) -> Result<Self, DemandError> {
        Self::start_with_work(directory, delivery, work)
    }

    pub(super) fn start_with_work<W: SourceDemandReadWork>(
        directory: &crate::native_host::HostRoot, delivery: Arc<Delivery>, work: &mut W,
    ) -> Result<Self, DemandError> {
        let mut cleanup = StartupCleanup::reserve(work)?;
        let prepared = (|| {
            work.preflight(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })?;
            let failure = delivery.prepare_startup_failure(work)?;
            let ownership = SourcePayloadOwnership::new(work)?;
            Ok::<_, DemandError>((failure, ownership))
        })();
        let (startup_failure, ownership) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => { cleanup.cancel(work); return Err(error); }
        };
        let mut inotify = None;
        let mut wake = None;
        let mut root = None;
        let mut ancestors = Vec::new();
        let mut buffer = Vec::new();
        let mut watches = Watches::default();
        let mut shared = None;
        let mut retained = 0_u64;
        let mut transferred = false;
        let control_bytes = arc_control_bytes::<Shared>();
        let mut control_transferred = false;
        let result = (|| {
            work.preflight(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })?;
            work.allocate(control_bytes)?;
            retained = control_bytes;
            shared = Some(Arc::new(Shared {
                inotify: HeldFd(AtomicI32::new(-1)),
                wake: HeldFd(AtomicI32::new(-1)),
                root: HeldFd(AtomicI32::new(-1)),
                ancestors: Mutex::new(Vec::new()),
                retained_variable_bytes: AtomicU64::new(0),
                reading: Mutex::new(RawReads::default()),
                reading_changed: Condvar::new(),
                watches: Mutex::new(Watches::default()),
                stopping: AtomicBool::new(false),
                completed: AtomicBool::new(false),
                controller_alive: AtomicBool::new(true),
                cleanup: Mutex::new(StartupCleanup::default()),
                ownership,
                delivery,
            }));
            let initialized = shared.as_ref().expect("shared initialized");
            initialized.ownership.retain(control_bytes, work)?;
            control_transferred = true;
            work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
            // SAFETY: plain descriptor creation with valid flags.
            inotify = Some(owned(unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) })?);
            work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
            // SAFETY: as above.
            wake = Some(owned(unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) })?);
            work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
            // SAFETY: duplicating a live descriptor the root holds.
            root = Some(owned(unsafe { libc::fcntl(directory.directory_fd(), libc::F_DUPFD_CLOEXEC, 0) })?);
            let held_inotify = inotify.as_ref().expect("inotify initialized");
            let held_root = root.as_ref().expect("root initialized");
            watch_ancestors(held_inotify, held_root, &mut ancestors, &mut retained, &mut cleanup, work)?;
            let bytes = READ_BUFFER_BYTES as u64;
            let next_retained = retained.checked_add(bytes).ok_or(WorkError::Overflow)?;
            work.allocate(bytes)?;
            retained = next_retained;
            buffer = vec![0; READ_BUFFER_BYTES];
            cleanup.reserve_record(work)?;
            let bytes = std::mem::size_of::<(PathBuf, i32)>() as u64;
            let next_retained = retained.checked_add(bytes).ok_or(WorkError::Overflow)?;
            work.allocate(bytes)?;
            retained = next_retained;
            watches.entries = Vec::with_capacity(1);
            let (root_path, _) = proc_fd_path(held_root.as_raw_fd());
            work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
            // SAFETY: this stack C path names the held root and lives through
            // registration, exactly as for every later admitted directory.
            let descriptor = unsafe {
                libc::inotify_add_watch(held_inotify.as_raw_fd(), root_path.as_ptr().cast(), WATCHED)
            };
            if descriptor < 0 {
                return Err(last_error().into());
            }
            watches.entries.push((PathBuf::new(), descriptor));
            let initialized = shared.as_ref().expect("shared initialized");
            initialized.inotify.0.store(
                inotify.take().expect("inotify initialized").into_raw_fd(), Ordering::Release,
            );
            initialized.wake.0.store(
                wake.take().expect("wake initialized").into_raw_fd(), Ordering::Release,
            );
            initialized.root.0.store(
                root.take().expect("root initialized").into_raw_fd(), Ordering::Release,
            );
            *initialized.ancestors.lock().unwrap_or_else(PoisonError::into_inner) = std::mem::take(&mut ancestors);
            initialized.reading.lock().unwrap_or_else(PoisonError::into_inner).buffer = std::mem::take(&mut buffer);
            *initialized.watches.lock().unwrap_or_else(PoisonError::into_inner) = std::mem::take(&mut watches);
            initialized.retained_variable_bytes.store(retained - control_bytes, Ordering::Release);
            initialized.ownership.retain(retained - control_bytes, work)?;
            transferred = true;
            *initialized.cleanup.lock().unwrap_or_else(PoisonError::into_inner) = std::mem::take(&mut cleanup);
            initialized.cleanup.lock().unwrap_or_else(PoisonError::into_inner).reserve_worker(work)?;
            // No callback may mutate the startup cleanup inventory before
            // publication or before a cancelled startup has been stopped.
            let initialized = shared.as_ref().expect("shared initialized");
            let _startup_reading = initialized.reading.lock().unwrap_or_else(PoisonError::into_inner);
            // Native thread machinery is SDK-private (zero canonical payload),
            // but its actual allocation and native call are still admitted.
            work.charge(WorkCounters {
                backend_read_operations: 1, allocation_operations: 1,
                ..WorkCounters::default()
            })?;
            let reader = Arc::into_raw(Arc::clone(shared.as_ref().expect("shared initialized")));
            let mut thread = std::mem::MaybeUninit::<libc::pthread_t>::uninit();
            // SAFETY: pthread_create writes the thread ID on success, and the
            // raw Arc payload is consumed either by run_reader or by failure.
            let status = unsafe {
                libc::pthread_create(thread.as_mut_ptr(), std::ptr::null(), run_reader, reader.cast_mut().cast())
            };
            if status != 0 {
                // SAFETY: no thread was created, so startup retains this Arc.
                drop(unsafe { Arc::from_raw(reader) });
                return Err(io::Error::from_raw_os_error(status).into());
            }
            // SAFETY: successful pthread_create initialized the ID.
            let cancelled = work.check().err();
            if cancelled.is_some() {
                initialized.stopping.store(true, Ordering::Release);
            }
            Ok((unsafe { thread.assume_init() }, cancelled))
        })();
        match result {
            Ok((thread, cancelled)) => {
                let watch = Self {
                    shared: shared.expect("successful startup initialized shared"),
                    thread: Mutex::new(Some(thread)),
                };
                if let Some(error) = cancelled {
                    // The native startup reserved every operation and record
                    // below before creation. Cancellation cannot deny cleanup.
                    let stopped = watch.stop_admitted(
                        &mut watch.thread.lock().unwrap_or_else(PoisonError::into_inner), work,
                    );
                    if let Err(stop_error) = stopped {
                        if !watch.is_stopped() {
                            if let Some(failure) = &startup_failure {
                                return Err(failure.retain_failed_platform(stop_error, watch));
                            }
                            // Only the explicit ordinary service can retry its
                            // failed cleanup with the legacy Drop allowance.
                            work.release(retained)?;
                            return Err(stop_error);
                        }
                        if !work.is_native_original() { work.release(retained)?; }
                        return Err(stop_error);
                    }
                    if !work.is_native_original() { work.release(retained)?; }
                    return Err(error);
                }
                // The owner carries the actual buffers; this invocation no
                // longer has custody of their original allocation claims.
                if !work.is_native_original() {
                    watch.shared.cleanup.lock().unwrap_or_else(PoisonError::into_inner).cancel(work);
                }
                if !work.is_native_original() { work.release(retained)?; }
                Ok(watch)
            }
            Err(error) => {
                let mut cleaned = Ok(());
                if let Some(shared) = shared {
                    cleaned = cleaned.and(shared.clear_measured(work));
                    shared.cancel_cleanup(work);
                }
                cleaned = cleaned.and(clear_records(&mut ancestors, work, None, |_| 0, |work| cleanup.consume_record(work)));
                cleaned = cleaned.and(clear_records(&mut watches.entries, work, None, |_| 0, |work| cleanup.consume_record(work)));
                drop(std::mem::take(&mut buffer));
                for descriptor in [&mut inotify, &mut wake, &mut root] {
                    if let Some(descriptor) = descriptor.take() {
                        cleaned = cleaned.and(close_measured(descriptor.into_raw_fd(), work, |work| cleanup.record(
                            WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED }, work,
                        )));
                    }
                }
                cleanup.cancel(work);
                let released_bytes = if work.is_native_original() && control_transferred {
                    if transferred { 0 } else { retained - control_bytes }
                } else { retained };
                cleaned = cleaned.and(work.release(released_bytes));
                cleaned?;
                Err(error)
            }
        }
    }

    pub(super) fn admit(&self, directory: &Path) -> bool {
        let cancellation = crate::cancellation::CancellationToken::new();
        self.admit_measured(directory, &mut DemandWork::new(WorkBudget::UNBOUNDED, &cancellation))
            .unwrap_or(false)
    }

    pub(super) fn admit_measured(
        &self,
        directory: &Path,
        work: &mut DemandWork,
    ) -> Result<bool, DemandError> {
        self.admit_with_work(directory, work)
    }

    pub(super) fn admit_with_work<W: SourceDemandReadWork>(
        &self, directory: &Path, work: &mut W,
    ) -> Result<bool, DemandError> {
        self.shared.admit_with_work(directory, work)
    }

    pub(super) fn unadmit_measured(
        &self, directory: &Path, work: &mut DemandWork,
    ) -> Result<(), DemandError> {
        self.unadmit_with_work(directory, work)
    }

    pub(super) fn unadmit_with_work<W: SourceDemandReadWork>(
        &self, directory: &Path, work: &mut W,
    ) -> Result<(), DemandError> {
        self.shared.unadmit_with_work(directory, work)
    }

    pub(super) fn fence(&self) -> io::Result<()> {
        self.shared.read_and_deliver()
    }

    pub(super) fn fence_events_measured(
        &self, timeout: std::time::Duration, work: &mut DemandWork,
    ) -> Result<bool, DemandError> {
        self.fence_events_with_work(timeout, work)
    }

    pub(super) fn fence_events_with_work<W: SourceDemandReadWork>(
        &self, timeout: std::time::Duration, work: &mut W,
    ) -> Result<bool, DemandError> {
        self.shared.read_events_and_deliver_with_work(timeout, work)
    }

    pub(super) fn delivery(&self) -> &Delivery {
        &self.shared.delivery
    }

    /// Actual SDK payload only. The readonly snapshot never acquires a lock
    /// held by native callbacks or waits for an in-flight delivery.
    pub(super) fn logical_owned_bytes(&self) -> u64 {
        arc_control_bytes::<Shared>()
            + self.shared.retained_variable_bytes.load(Ordering::Acquire)
    }

    pub(super) fn is_stopped(&self) -> bool {
        self.shared.completed.load(Ordering::Acquire)
    }

    pub(super) fn stop_measured(&self, work: &mut DemandWork) -> Result<(), DemandError> {
        self.stop_with_work(work)
    }

    /// Admits the complete irreversible suffix while every mutable record
    /// owner is quiescent. Denial leaves the worker and all descriptors intact.
    pub(super) fn stop_with_work<W: SourceDemandReadWork>(
        &self, work: &mut W,
    ) -> Result<(), DemandError> {
        self.shared.delivery.ensure_stop_available()?;
        work.check()?;
        let mut thread = self.thread.lock().unwrap_or_else(PoisonError::into_inner);
        if self.is_stopped() {
            return self.join_completed(&mut thread, work);
        }
        let reading = self.shared.reading.lock().unwrap_or_else(PoisonError::into_inner);
        let watches = self.shared.lock_watches();
        let ancestors = self.shared.ancestors.lock().unwrap_or_else(PoisonError::into_inner);
        let records = reading.cleanup.len().checked_add(watches.entries.len())
            .and_then(|count| count.checked_add(ancestors.len())).ok_or(WorkError::Overflow)?;
        let worker = u64::from(thread.is_some());
        let descriptors = [&self.shared.inotify, &self.shared.wake, &self.shared.root]
            .into_iter().filter(|descriptor| descriptor.as_raw_fd() >= 0).count() as u64;
        let suffix = WorkCounters {
            backend_read_operations: worker, backend_write_operations: descriptors + worker,
            items_examined: u64::try_from(records).map_err(|_| WorkError::Overflow)?,
            ..WorkCounters::UNCHARGED
        };
        if self.shared.birth_matches(work) {
            self.shared.cleanup.lock().unwrap_or_else(PoisonError::into_inner).extend_for(suffix, work)?;
        } else {
            work.preflight(suffix)?;
        }
        work.check()?;
        self.shared.stopping.store(true, Ordering::Release);
        self.shared.reading_changed.notify_all();
        // The worker and other admitted operations recheck stopping under
        // their existing locks. Do not hold those locks across pthread_join.
        drop(ancestors);
        drop(watches);
        drop(reading);
        self.stop_admitted(&mut thread, work)
    }

    fn stop_from_birth_prepaid(&self) -> Result<(), DemandError> {
        self.shared.delivery.ensure_stop_available()?;
        if self.is_stopped() {
            let mut thread = self.thread.lock().unwrap_or_else(PoisonError::into_inner);
            let original = self.shared.delivery.original.as_ref().ok_or(DemandError::NativeWorkUnavailable)?;
            return self.join_completed(&mut thread, &mut crate::demand::NativeSourceWork::from_original_work(original));
        }
        let original = self.shared.delivery.original.as_ref().ok_or(DemandError::NativeWorkUnavailable)?;
        let mut work = crate::demand::NativeSourceWork::from_original_work(original);
        let mut thread = self.thread.lock().unwrap_or_else(PoisonError::into_inner);
        let reading = self.shared.reading.lock().unwrap_or_else(PoisonError::into_inner);
        let watches = self.shared.lock_watches();
        let ancestors = self.shared.ancestors.lock().unwrap_or_else(PoisonError::into_inner);
        self.shared.stopping.store(true, Ordering::Release);
        self.shared.reading_changed.notify_all();
        drop(ancestors);
        drop(watches);
        drop(reading);
        self.stop_admitted(&mut thread, &mut work)
    }

    /// Executes the already-admitted mandatory suffix against the same work
    /// carrier, without consulting cancellation after native stop has begun.
    fn stop_admitted<W: SourceDemandReadWork>(
        &self, thread: &mut Option<libc::pthread_t>, work: &mut W,
    ) -> Result<(), DemandError> {
        self.shared.stopping.store(true, Ordering::Release);
        self.shared.reading_changed.notify_all();
        let mut result = Ok(());
        if let Some(worker) = *thread {
            let reading = self.shared.reading.lock().unwrap_or_else(PoisonError::into_inner);
            let descriptor = self.shared.wake.as_raw_fd();
            let wake_error = if descriptor >= 0 {
                let one = 1_u64.to_ne_bytes();
                // SAFETY: the existing read custody lock excludes terminal
                // descriptor retirement throughout this actual wake attempt.
                let wake = unsafe { libc::write(descriptor, one.as_ptr().cast(), one.len()) };
                let error = (wake < 0).then(last_error);
                result = self.shared.record_cleanup(WorkCounters {
                    backend_write_operations: 1, ..WorkCounters::default()
                }, work);
                error
            } else { None };
            drop(reading);
            if let Some(error) = wake_error {
                // Preserve join/descriptor custody for a subsequent stop attempt.
                return result.and(Err(error.into()));
            }
            // SAFETY: this is the one joinable worker returned by pthread_create.
            let joined = unsafe { libc::pthread_join(worker, std::ptr::null_mut()) };
            result = result.and(self.shared.record_cleanup(WorkCounters {
                backend_read_operations: 1, ..WorkCounters::default()
            }, work));
            if joined != 0 {
                return result.and(Err(io::Error::from_raw_os_error(joined).into()));
            }
            *thread = None;
        }
        // A native worker retires its physical state before its entrypoint
        // returns. Startup without a worker and ordinary owners retire here.
        if self.shared.delivery.original.is_none() || !self.shared.completed.load(Ordering::Acquire) {
            result = result.and(self.shared.clear_measured(work));
            self.shared.delivery.exact.store(false, Ordering::Release);
            self.shared.completed.store(true, Ordering::Release);
        }
        self.shared.cancel_cleanup(work);
        result
    }

    fn join_completed<W: SourceDemandReadWork>(
        &self, thread: &mut Option<libc::pthread_t>, work: &mut W,
    ) -> Result<(), DemandError> {
        let mut result = Ok(());
        if let Some(worker) = *thread {
            // SAFETY: terminal publication follows physical cleanup, but this
            // controller still owns the one joinable pthread returned at birth.
            let joined = unsafe { libc::pthread_join(worker, std::ptr::null_mut()) };
            result = self.shared.record_cleanup(WorkCounters {
                backend_read_operations: 1, ..WorkCounters::UNCHARGED
            }, work);
            if joined != 0 {
                return result.and(Err(io::Error::from_raw_os_error(joined).into()));
            }
            *thread = None;
        }
        self.shared.cancel_cleanup(work);
        result
    }

    /// Holds delivery back while the returned guard lives.
    #[cfg(test)]
    pub(super) fn paused(&self) -> impl Sized + '_ {
        self.shared
            .reading
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for PlatformWatch {
    fn drop(&mut self) {
        if self.shared.delivery.original.is_some() {
            if self.shared.delivery.ensure_stop_available().is_err() {
                self.shared.controller_alive.store(false, Ordering::Release);
                // The invoking reader retains Shared through callback return.
                // It performs terminal cleanup itself rather than self-joining.
                self.shared.stopping.store(true, Ordering::Release);
                self.shared.reading_changed.notify_all();
                return;
            }
            let _ = self.stop_from_birth_prepaid();
            let completed = {
                let _cleanup = self.shared.cleanup.lock().unwrap_or_else(PoisonError::into_inner);
                self.shared.controller_alive.store(false, Ordering::Release);
                self.is_stopped()
            };
            if completed {
                let mut work = self.shared.delivery.callback_work();
                let thread = self.thread.get_mut().unwrap_or_else(PoisonError::into_inner);
                if let Some(worker) = *thread {
                    if let Err(error) = self.shared.detach_terminal_thread(worker, &mut work) {
                        self.shared.delivery.failed_with_work(error, &mut work);
                    }
                    *thread = None;
                }
                self.shared.cancel_cleanup(&mut work);
            }
            return;
        }
        if self.is_stopped() { return; }
        let cancellation = crate::cancellation::CancellationToken::new();
        // Ordinary convenience cleanup uses exactly the measured engine.
        let _ = self.stop_measured(&mut DemandWork::new(WorkBudget::UNBOUNDED, &cancellation));
    }
}

impl Shared {
    fn finish_native_reader(&self) {
        self.stopping.store(true, Ordering::Release);
        let mut work = self.delivery.callback_work();
        let result = self.clear_measured(&mut work);
        if let Err(error) = result { self.delivery.failed_with_work(error, &mut work); }
        self.delivery.exact.store(false, Ordering::Release);
        let detach = {
            let mut cleanup = self.cleanup.lock().unwrap_or_else(PoisonError::into_inner);
            let keep = u64::from(self.controller_alive.load(Ordering::Acquire));
            let reads = cleanup.reads.min(keep);
            let writes = cleanup.writes.min(1);
            let unused = WorkCounters {
                backend_read_operations: cleanup.reads - reads,
                backend_write_operations: cleanup.writes - writes,
                items_examined: cleanup.records, ..WorkCounters::UNCHARGED
            };
            work.with_original_leaf(|work| super::cancel_cleanup_credits(work, unused));
            cleanup.reads = reads;
            cleanup.writes = writes;
            cleanup.records = 0;
            self.completed.store(true, Ordering::Release);
            keep == 0
        };
        if detach {
            // SAFETY: the actual reader has retired its native payload. With
            // no controller left, detachment releases only pthread bookkeeping.
            let worker = unsafe { libc::pthread_self() };
            if let Err(error) = self.detach_terminal_thread(worker, &mut work) {
                self.delivery.failed_with_work(error, &mut work);
            }
            self.cancel_cleanup(&mut work);
        }
        self.reading_changed.notify_all();
    }

    fn detach_terminal_thread<W: SourceDemandReadWork>(
        &self, worker: libc::pthread_t, work: &mut W,
    ) -> Result<(), DemandError> {
        // SAFETY: cleanup's controller-alive decision selected this sole
        // terminal bookkeeping owner; no live controller can join this thread.
        let detached = unsafe { libc::pthread_detach(worker) };
        let recorded = self.record_cleanup(WorkCounters {
            backend_write_operations: 1, ..WorkCounters::UNCHARGED
        }, work);
        if detached != 0 {
            return Err(io::Error::from_raw_os_error(detached).into());
        }
        recorded
    }

    fn lock_watches(&self) -> std::sync::MutexGuard<'_, Watches> {
        self.watches.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // Credit provenance is custody accounting, not cleanup permission.
    fn birth_matches<W: SourceDemandReadWork>(&self, work: &W) -> bool {
        self.delivery.birth_matches(work)
    }

    fn record_cleanup<W: SourceDemandReadWork>(&self, delta: WorkCounters, work: &mut W) -> Result<(), DemandError> {
        let mut cleanup = self.cleanup.lock().unwrap_or_else(PoisonError::into_inner);
        if (self.birth_matches(work) || self.delivery.original.is_none()) && cleanup.covers(delta) {
            cleanup.record(delta, work)
        } else {
            work.record(delta)
        }
    }

    fn cancel_cleanup<W: SourceDemandReadWork>(&self, work: &mut W) {
        let mut cleanup = self.cleanup.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(birth) = &self.delivery.original {
            cleanup.cancel(&mut crate::demand::NativeSourceWork::from_original_work(birth));
        } else {
            cleanup.cancel(work);
        }
    }

    fn reserve_record_cleanup<W: SourceDemandReadWork>(&self, work: &mut W) -> Result<(), DemandError> {
        if self.birth_matches(work) {
            self.cleanup.lock().unwrap_or_else(PoisonError::into_inner).reserve_record(work)?;
        }
        Ok(())
    }

    fn cancel_record_cleanup<W: SourceDemandReadWork>(&self, work: &mut W) {
        if !self.birth_matches(work) { return; }
        let mut cleanup = self.cleanup.lock().unwrap_or_else(PoisonError::into_inner);
        cleanup.records -= 1;
        work.with_original_leaf(|work| work.budget.items_examined += 1);
    }

    /// Called only without a worker or after its successful join. No owned
    /// native descriptor or variable-length record remains for a later Drop.
    fn clear_measured<W: SourceDemandReadWork>(&self, work: &mut W) -> Result<(), DemandError> {
        let mut reading = self.reading.lock().unwrap_or_else(PoisonError::into_inner);
        let mut watches = self.lock_watches();
        let mut ancestors = self.ancestors.lock().unwrap_or_else(PoisonError::into_inner);
        let retired_bytes = self.retained_variable_bytes.load(Ordering::Acquire);
        let mut result = Ok(());
        for descriptor in [&self.inotify, &self.wake, &self.root] {
            let descriptor = descriptor.take();
            if descriptor >= 0 {
                result = result.and(close_measured(descriptor, work, |work| self.record_cleanup(
                    WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED }, work,
                )));
            }
        }
        result = result.and(clear_records(
            &mut ancestors, work, Some(&self.retained_variable_bytes),
            |(_, name)| name.capacity() as u64,
            |work| self.record_cleanup(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }, work),
        ));
        result = result.and(clear_records(
            &mut watches.entries, work, Some(&self.retained_variable_bytes),
            |(path, _)| path.capacity() as u64,
            |work| self.record_cleanup(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }, work),
        ));
        result = result.and(clear_records(
            &mut reading.cleanup, work, Some(&self.retained_variable_bytes),
            |path| path.capacity() as u64,
            |work| self.record_cleanup(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }, work),
        ));
        watches.path_bytes = 0;
        reading.cleanup_path_bytes = 0;
        let buffer_bytes = reading.buffer.capacity() as u64;
        reading.buffer = Vec::new();
        self.retained_variable_bytes.fetch_sub(buffer_bytes, Ordering::AcqRel);
        reading.filled = 0;
        reading.cursor = 0;
        reading.alias_cursor = 0;
        reading.blocked = true;
        result = result.and(self.ownership.retire(retired_bytes));
        self.retained_variable_bytes.store(0, Ordering::Release);
        result
    }


    #[cfg(test)]
    fn add_watch_measured(&self, relative: &Path, work: &mut DemandWork) -> Result<i32, DemandError> {
        let _watches = self.lock_watches();
        if self.stopping.load(Ordering::Acquire) { return Err(DemandError::Cancelled); }
        self.add_watch_at_with_work(self.root.as_raw_fd(), relative, work)
    }


    fn add_watch_at_with_work<W: SourceDemandReadWork>(
        &self, directory: i32, relative: &Path, work: &mut W,
    ) -> Result<i32, DemandError> {
        if self.stopping.load(Ordering::Acquire) { return Err(DemandError::Cancelled); }
        // Render the held descriptor on the stack: only the final C path is
        // owned, and it is admitted before any bytes are copied.
        let mut digits = [0_u8; 10];
        let mut start = digits.len();
        let mut descriptor = directory as u32;
        loop {
            start -= 1;
            digits[start] = b'0' + (descriptor % 10) as u8;
            descriptor /= 10;
            if descriptor == 0 {
                break;
            }
        }
        let relative = relative.as_os_str().as_bytes();
        let prefix = b"/proc/self/fd/";
        let length = prefix.len()
            .checked_add(digits.len() - start)
            .and_then(|length| length.checked_add(usize::from(!relative.is_empty())))
            .and_then(|length| length.checked_add(relative.len()))
            .and_then(|length| length.checked_add(1))
            .ok_or(WorkError::Overflow)?;
        let bytes = u64::try_from(length).map_err(|_| WorkError::Overflow)?;
        let path = work.with_original_leaf(|work| {
            work.work.admit(&WorkCounters { bytes_copied: bytes - 1, ..WorkCounters::default() }, &work.budget)?;
            work.allocate(bytes)?;
            work.record(WorkCounters { bytes_copied: bytes - 1, ..WorkCounters::default() })?;
            let mut path = Vec::with_capacity(length);
            path.extend_from_slice(prefix);
            path.extend_from_slice(&digits[start..]);
            if !relative.is_empty() {
                path.push(b'/');
                path.extend_from_slice(relative);
            }
            Ok::<_, DemandError>(path)
        })?;
        let result = (|| {
            let path = CString::new(path)
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
            work.with_original_leaf(|work| work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() }))?;
            // SAFETY: a NUL-terminated path that outlives the call, through the
            // held root rather than a newly resolved root name.
            let descriptor = unsafe {
                libc::inotify_add_watch(self.inotify.as_raw_fd(), path.as_ptr(), WATCHED)
            };
            if descriptor < 0 {
                Err(last_error().into())
            } else {
                Ok(descriptor)
            }
        })();
        work.release(bytes)?;
        result
    }

    fn admit_with_work<W: SourceDemandReadWork>(
        &self, directory: &Path, work: &mut W,
    ) -> Result<bool, DemandError> {
        work.check()?;
        if !self.delivery.is_exact() || directory.as_os_str().is_empty() {
            return Ok(false);
        }
        let mut watches = self.lock_watches();
        if self.stopping.load(Ordering::Acquire) { return Ok(false); }
        let bytes = directory.as_os_str().len() as u64;
        let mut abandoned = false;
        let result = (|| {
            let mut prefix = work.with_original_leaf(|work| {
                work.allocate(bytes)?;
                Ok::<_, DemandError>(PathBuf::with_capacity(directory.as_os_str().len()))
            })?;
            let mut held: Option<(OwnedFd, u64)> = None;
            let result = (|| {
                let mut added = false;
                for component in directory.components() {
                    let std::path::Component::Normal(name) = component else {
                        return Err(DemandError::InvalidRequest);
                    };
                    work.with_original_leaf(|work| {
                        work.charge(WorkCounters {
                            items_examined: 1,
                            bytes_copied: (component.as_os_str().len() as u64)
                                .checked_add(u64::from(!prefix.as_os_str().is_empty()))
                                .ok_or(WorkError::Overflow)?,
                            ..WorkCounters::default()
                        })?;
                        prefix.push(component);
                        Ok::<_, DemandError>(())
                    })?;
                    let parent = held.as_ref().map_or_else(|| self.root.as_raw_fd(), |(directory, _)| directory.as_raw_fd());
                    let opened = match Self::open_component_with_work(parent, name, work) {
                        Ok(directory) => directory,
                        Err(DemandError::Io(error))
                            if matches!(error.raw_os_error(), Some(libc::ENOENT | libc::ENOTDIR)) => return Ok(false),
                        Err(error) => return Err(error),
                    };
                    if let Some(previous) = held.replace(opened) { close_component(previous, work)?; }
                    added = false;
                    if watches.contains_path_with_work(&prefix, work)? {
                        continue;
                    }
                    self.reserve_record_cleanup(work)?;
                    let owned = match watches.prepare_insert_with_work(
                        &prefix, work, &self.retained_variable_bytes, &self.ownership,
                    ) {
                        Ok(owned) => owned,
                        Err(error) => { self.cancel_record_cleanup(work); return Err(error); }
                    };
                    let path_bytes = owned.as_os_str().len() as u64;
                    match self.add_watch_at_with_work(
                        held.as_ref().ok_or(DemandError::InvalidRequest)?.0.as_raw_fd(), Path::new("."), work,
                    ) {
                        Ok(descriptor) => {
                            let capacity = owned.capacity() as u64;
                            watches.entries.push((owned, descriptor));
                            watches.path_bytes += capacity;
                            self.retained_variable_bytes.fetch_add(capacity, Ordering::AcqRel);
                            self.ownership.retain(path_bytes, work)?;
                            if !self.ownership.has_native_custody() { work.release(path_bytes)?; }
                            added = true;
                        }
                        Err(error) => {
                            drop(owned);
                            self.cancel_record_cleanup(work);
                            work.release(path_bytes)?;
                            match error {
                                DemandError::Io(error)
                                    if matches!(error.raw_os_error(), Some(libc::ENOENT | libc::ENOTDIR)) =>
                                {
                                    return Ok(false);
                                }
                                DemandError::Io(_) => {
                                    abandoned = true;
                                    return Ok(false);
                                }
                                error => return Err(error),
                            }
                        }
                    }
                }
                Ok(added)
            })();
            let closed = match held.take() {
                Some(directory) => close_component(directory, work),
                None => Ok(()),
            };
            drop(prefix);
            work.release(bytes)?;
            result.and_then(|value| closed.map(|()| value))
        })();
        drop(watches);
        if abandoned {
            self.delivery.abandon();
        }
        result
    }

    fn unadmit_measured(&self, directory: &Path, work: &mut DemandWork) -> Result<(), DemandError> {
        self.unadmit_with_work(directory, work)
    }

    fn unadmit_with_work<W: SourceDemandReadWork>(
        &self, directory: &Path, work: &mut W,
    ) -> Result<(), DemandError> {
        work.check()?;
        let mut watches = self.lock_watches();
        if self.stopping.load(Ordering::Acquire) { return Ok(()); }
        let mut at = 0;
        while at < watches.entries.len() {
            work.with_original_leaf(|work| work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() }))?;
            let (path, descriptor) = &watches.entries[at];
            if path.as_os_str().is_empty() || !path.starts_with(directory) {
                at += 1;
                continue;
            }
            let descriptor = *descriptor;
            let mut shared = false;
            for (index, (_, other)) in watches.entries.iter().enumerate() {
                work.with_original_leaf(|work| work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() }))?;
                if index != at && *other == descriptor {
                    shared = true;
                    break;
                }
            }
            let copied = WorkCounters {
                bytes_copied: ((watches.entries.len() - at - 1) as u64)
                    .checked_mul(size_of::<(PathBuf, i32)>() as u64).ok_or(WorkError::Overflow)?,
                ..WorkCounters::default()
            };
            work.with_original_leaf(|work| work.work.admit(&copied, &work.budget))?;
            if !shared {
                work.with_original_leaf(|work| work.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::default() }))?;
                // SAFETY: removes only this instance's admitted descriptor.
                if unsafe { libc::inotify_rm_watch(self.inotify.as_raw_fd(), descriptor) } < 0 {
                    let error = last_error();
                    if error.raw_os_error() != Some(libc::EINVAL) { return Err(error.into()); }
                }
            }
            work.record(copied)?;
            let (removed, _) = watches.entries.remove(at);
            let bytes = removed.capacity() as u64;
            watches.path_bytes -= bytes;
            drop(removed);
            self.cancel_record_cleanup(work);
            self.ownership.retire(bytes)?;
            self.retained_variable_bytes.fetch_sub(bytes, Ordering::AcqRel);
        }
        Ok(())
    }

    fn open_component_with_work<W: SourceDemandReadWork>(
        parent: i32, name: &OsStr, work: &mut W,
    ) -> Result<(OwnedFd, u64), DemandError> {
        let name = name.as_bytes();
        let bytes = name.len().checked_add(1).ok_or(WorkError::Overflow)? as u64;
        let copied = WorkCounters { bytes_copied: name.len() as u64, ..WorkCounters::default() };
        let reservation = work.reserve_cleanup_write()?;
        let buffer = match work.with_original_leaf(|work| {
            work.work.admit(&copied, &work.budget)?;
            work.allocate(bytes)?;
            work.record(copied)?;
            let mut buffer = Vec::with_capacity(bytes as usize);
            buffer.extend_from_slice(name);
            buffer.push(0);
            Ok::<_, DemandError>(buffer)
        }) {
            Ok(buffer) => buffer,
            Err(error) => { work.cancel_cleanup_write(reservation); return Err(error); }
        };
        let result = (|| {
            let name = CString::from_vec_with_nul(buffer)
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
            work.with_original_leaf(|work| work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() }))?;
            // SAFETY: one normal component beneath a held directory, never
            // following a symlink or re-resolving a previously opened prefix.
            let directory = unsafe { libc::openat(parent, name.as_ptr(),
                libc::O_PATH | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC) };
            if directory < 0 {
                Err(last_error().into())
            } else {
                // SAFETY: the successful open returned one exclusively owned fd.
                Ok(unsafe { OwnedFd::from_raw_fd(directory) })
            }
        })();
        let released = work.release(bytes);
        match result {
            Ok(directory) => match released {
                Ok(()) => Ok((directory, reservation)),
                Err(error) => {
                    // Even a late accounting failure must not defer this
                    // admitted temporary descriptor close to OwnedFd::drop.
                    close_component((directory, reservation), work)?;
                    Err(error)
                }
            },
            Err(error) => {
                work.cancel_cleanup_write(reservation);
                released?;
                Err(error)
            }
        }
    }

    fn run(&self) {
        let mut descriptors = [
            libc::pollfd {
                fd: self.inotify.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: self.wake.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        while !self.stopping.load(Ordering::Acquire) {
            let mut work = self.delivery.callback_work();
            if let Err(error) = work.charge(WorkCounters {
                backend_read_operations: 1, ..WorkCounters::UNCHARGED
            }) {
                self.delivery.failed_with_work(error, &mut work);
                return;
            }
            // SAFETY: two initialized pollfd records in a live array.
            let ready = unsafe { libc::poll(descriptors.as_mut_ptr(), 2, -1) };
            if ready < 0 {
                let error = last_error();
                if error.kind() == io::ErrorKind::Interrupted { continue; }
                self.delivery.failed_with_work(error.into(), &mut work);
                return;
            }
            {
                let mut reading = self.reading.lock().unwrap_or_else(PoisonError::into_inner);
                while reading.blocked && !self.stopping.load(Ordering::Acquire) {
                    reading = self.reading_changed.wait(reading).unwrap_or_else(PoisonError::into_inner);
                }
            }
            if self.stopping.load(Ordering::Acquire) {
                return;
            }
            if let Err(error) = self.read_events_and_deliver_with_work(std::time::Duration::MAX, &mut work) {
                self.delivery.failed_with_work(error, &mut work);
                return;
            }
        }
    }

    /// Reads everything queued and delivers it, holding the read lock
    /// throughout.
    fn read_and_deliver(&self) -> io::Result<()> {
        let mut work = self.delivery.callback_work();
        match self.read_events_and_deliver_with_work(std::time::Duration::MAX, &mut work) {
            Ok(_) => Ok(()),
            Err(error) => {
                self.delivery.exact.store(false, Ordering::Release);
                match error { DemandError::Io(error) => Err(error), error => Err(io::Error::other(error)) }
            }
        }
    }

    /// Drains the same native page through the known SDK event sink. A denied
    /// record leaves both its bytes and alias position pending; the background
    /// reader cannot consume that denied suffix before a subsequent fence.
    fn read_events_and_deliver_with_work<W: SourceDemandReadWork>(
        &self, timeout: std::time::Duration, work: &mut W,
    ) -> Result<bool, DemandError> {
        work.check()?;
        if self.delivery.original.is_some() && !self.delivery.is_exact() { return Ok(false); }
        let started = std::time::Instant::now();
        let mut reading = self.reading.lock().unwrap_or_else(PoisonError::into_inner);
        if self.stopping.load(Ordering::Acquire) { return Ok(false); }
        let reading = &mut *reading;
        let result = (|| {
            loop {
                work.check()?;
                if started.elapsed() >= timeout {
                    self.delivery.invalidate_with_work(work)?;
                    return Ok(false);
                }
                while reading.alias_cursor == 0 && !reading.cleanup.is_empty() {
                    if started.elapsed() >= timeout {
                        self.delivery.invalidate_with_work(work)?;
                        return Ok(false);
                    }
                    let path = reading.cleanup.last().expect("nonempty pending cleanup");
                    self.unadmit_with_work(path, work)?;
                    let path = reading.cleanup.pop().expect("pending cleanup remains held");
                    let bytes = path.capacity() as u64;
                    reading.cleanup_path_bytes -= bytes;
                    drop(path);
                    self.cancel_record_cleanup(work);
                    self.ownership.retire(bytes)?;
                    self.retained_variable_bytes.fetch_sub(bytes, Ordering::AcqRel);
                }
                if reading.cursor == reading.filled {
                    let maximum = (work.remaining()?.source_bytes_read.min(reading.buffer.len() as u64)) as usize;
                    if maximum < EVENT_HEADER_BYTES {
                        work.preflight(WorkCounters {
                            source_bytes_read: EVENT_HEADER_BYTES as u64, ..WorkCounters::default()
                        })?;
                    }
                    work.preflight(WorkCounters {
                        source_bytes_read: maximum as u64, ..WorkCounters::default()
                    })?;
                    work.charge(WorkCounters {
                        backend_read_operations: 1, ..WorkCounters::default()
                    })?;
                    // SAFETY: no competing reader can use this owned page.
                    let read = unsafe {
                        libc::read(self.inotify.as_raw_fd(), reading.buffer.as_mut_ptr().cast(), maximum)
                    };
                    if read < 0 {
                        let error = last_error();
                        match error.kind() {
                            io::ErrorKind::WouldBlock => return Ok(self.delivery.is_exact()),
                            io::ErrorKind::Interrupted => continue,
                            _ => return Err(error.into()),
                        }
                    }
                    reading.filled = usize::try_from(read).map_err(|_| DemandError::InvalidRequest)?;
                    reading.cursor = 0;
                    reading.alias_cursor = 0;
                    work.record(WorkCounters {
                        source_bytes_read: reading.filled as u64, ..WorkCounters::default()
                    })?;
                    if reading.filled == 0 { return Ok(self.delivery.is_exact()); }
                }
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                let raw = &reading.buffer[reading.cursor..reading.filled];
                let Some(header) = raw.get(..EVENT_HEADER_BYTES) else {
                    self.delivery.invalidate_with_work(work)?;
                    reading.cursor = reading.filled;
                    return Ok(false);
                };
                let field = |offset: usize| u32::from_ne_bytes(
                    header[offset..offset + 4].try_into().expect("fixed inotify header"),
                );
                let descriptor = i32::from_ne_bytes(field(0).to_ne_bytes());
                let mask = field(4);
                let cookie = field(8) as usize;
                let end = EVENT_HEADER_BYTES.checked_add(field(12) as usize)
                    .ok_or(WorkError::Overflow)?;
                let Some(bytes) = raw.get(EVENT_HEADER_BYTES..end) else {
                    self.delivery.invalidate_with_work(work)?;
                    reading.cursor = reading.filled;
                    return Ok(false);
                };
                let mut name_length = 0;
                for byte in bytes {
                    work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                    if *byte == 0 { break; }
                    name_length += 1;
                }
                let name = OsStr::from_bytes(&bytes[..name_length]);
                if mask & (libc::IN_Q_OVERFLOW | libc::IN_UNMOUNT) != 0 {
                    self.delivery.invalidate_with_work(work)?;
                    reading.cursor += end;
                    reading.alias_cursor = 0;
                    continue;
                }
                let mut ancestor = None;
                {
                    let ancestors = self.ancestors.lock().unwrap_or_else(PoisonError::into_inner);
                    for (watched, on_the_way) in &*ancestors {
                        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                        if *watched == descriptor {
                            ancestor = Some(
                                mask & (libc::IN_DELETE_SELF | libc::IN_MOVE_SELF | libc::IN_IGNORED) != 0
                                    || mask & REBINDING != 0 && name == on_the_way.as_os_str(),
                            );
                            break;
                        }
                    }
                }
                if let Some(invalidated) = ancestor {
                    if invalidated {
                        self.delivery.invalidate_with_work(work)?;
                        self.delivery.exact.store(false, Ordering::Release);
                        return Ok(false);
                    }
                    reading.cursor += end;
                    reading.alias_cursor = 0;
                    continue;
                }
                let cleanup_needed = mask & (REBINDING | libc::IN_DELETE_SELF | libc::IN_MOVE_SELF) != 0;
                let event_needed = mask & (REBINDING | libc::IN_MODIFY | libc::IN_ATTRIB
                    | if self.delivery.events.is_none() { libc::IN_CLOSE_WRITE } else { 0 }) != 0;
                loop {
                    if started.elapsed() >= timeout {
                        self.delivery.invalidate_with_work(work)?;
                        return Ok(false);
                    }
                    let watches = self.lock_watches();
                    if reading.alias_cursor >= watches.entries.len() { break; }
                    work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                    let (alias, watched) = &watches.entries[reading.alias_cursor];
                    if *watched != descriptor {
                        reading.alias_cursor += 1;
                        continue;
                    }
                    if !cleanup_needed && !event_needed {
                        reading.alias_cursor += 1;
                        continue;
                    }
                    if alias.as_os_str().is_empty() && mask & libc::IN_DELETE_SELF != 0 {
                        drop(watches);
                        self.delivery.invalidate_with_work(work)?;
                        self.delivery.exact.store(false, Ordering::Release);
                        return Ok(false);
                    }
                    let bytes = alias.as_os_str().as_bytes().len()
                        .checked_add(usize::from(!alias.as_os_str().is_empty() && !name.is_empty()))
                        .and_then(|length| length.checked_add(name.len())).ok_or(WorkError::Overflow)?;
                    let delta = WorkCounters { bytes_copied: bytes as u64, ..WorkCounters::default() };
                    if cleanup_needed { self.reserve_record_cleanup(work)?; }
                    if let Err(error) = work.allocate_for(bytes as u64, delta) {
                        if cleanup_needed { self.cancel_record_cleanup(work); }
                        return Err(error);
                    }
                    let mut path = PathBuf::with_capacity(bytes);
                    if !alias.as_os_str().is_empty() { path.push(alias); }
                    if !name.is_empty() { path.push(name); }
                    drop(watches);
                    if cleanup_needed && reading.cleanup.len() == reading.cleanup.capacity() {
                        let old_bytes = (reading.cleanup.capacity() as u64)
                            .checked_mul(std::mem::size_of::<PathBuf>() as u64).ok_or(WorkError::Overflow)?;
                        let next_capacity = reading.cleanup.len().checked_add(1).ok_or(WorkError::Overflow)?;
                        let next_bytes = (next_capacity as u64)
                            .checked_mul(std::mem::size_of::<PathBuf>() as u64).ok_or(WorkError::Overflow)?;
                        let delta = WorkCounters {
                            bytes_copied: (reading.cleanup.len() as u64) * std::mem::size_of::<PathBuf>() as u64,
                            ..WorkCounters::default()
                        };
                        let prepared = (|| {
                            work.allocate_for(next_bytes, delta)?;
                            let mut replacement = Vec::with_capacity(next_capacity);
                            replacement.append(&mut reading.cleanup);
                            reading.cleanup = replacement;
                            let retained_bytes = reading.cleanup.capacity() as u64
                                * std::mem::size_of::<PathBuf>() as u64;
                            self.retained_variable_bytes.fetch_add(retained_bytes - old_bytes, Ordering::AcqRel);
                            self.ownership.retain(next_bytes, work)?;
                            self.ownership.retire(old_bytes)?;
                            if !self.ownership.has_native_custody() { work.release(next_bytes)?; }
                            Ok::<(), DemandError>(())
                        })();
                        if let Err(error) = prepared {
                            drop(path);
                            self.cancel_record_cleanup(work);
                            work.release(bytes as u64)?;
                            return Err(error);
                        }
                    }
                    let delivered = if self.delivery.events.is_some() {
                        self.delivery.event_with_work(&path, mask, cookie, work)
                    } else {
                        let change = if mask & (REBINDING | libc::IN_DELETE_SELF | libc::IN_MOVE_SELF) != 0 {
                            HostChange::Rebound(path)
                        } else {
                            HostChange::Altered(path)
                        };
                        let answer = self.delivery.deliver_with_work(std::slice::from_ref(&change), work);
                        path = match change {
                            HostChange::Rebound(path) | HostChange::Altered(path) => path,
                            HostChange::Everything => unreachable!("constructed path change"),
                        };
                        answer
                    };
                    if delivered.is_err() {
                        drop(path);
                        if cleanup_needed { self.cancel_record_cleanup(work); }
                        work.release(bytes as u64)?;
                        return delivered.map(|()| false);
                    }
                    reading.alias_cursor += 1;
                    if cleanup_needed {
                        let retained_bytes = path.capacity() as u64;
                        reading.cleanup_path_bytes += retained_bytes;
                        reading.cleanup.push(path);
                        self.retained_variable_bytes.fetch_add(retained_bytes, Ordering::AcqRel);
                        self.ownership.retain(bytes as u64, work)?;
                        if !self.ownership.has_native_custody() { work.release(bytes as u64)?; }
                    } else {
                        drop(path);
                        work.release(bytes as u64)?;
                    }
                }
                if mask & libc::IN_IGNORED != 0 {
                    let mut watches = self.lock_watches();
                    let mut at = 0;
                    while at < watches.entries.len() {
                        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                        if watches.entries[at].1 == descriptor {
                            let shifted = (watches.entries.len() - at - 1)
                                .checked_mul(std::mem::size_of::<(PathBuf, i32)>()).ok_or(WorkError::Overflow)?;
                            work.charge(WorkCounters { bytes_copied: shifted as u64, ..WorkCounters::default() })?;
                            let (path, _) = watches.entries.remove(at);
                            let bytes = path.capacity() as u64;
                            watches.path_bytes -= bytes;
                            drop(path);
                            self.cancel_record_cleanup(work);
                            self.ownership.retire(bytes)?;
                            self.retained_variable_bytes.fetch_sub(bytes, Ordering::AcqRel);
                        } else { at += 1; }
                    }
                }
                reading.cursor += end;
                reading.alias_cursor = 0;
            }
        })();
        reading.blocked = !matches!(result, Ok(true));
        if result.is_err() { self.delivery.exact.store(false, Ordering::Release); }
        if !reading.blocked { self.reading_changed.notify_all(); }
        result
    }

}

#[cfg(test)]
mod admission_tests {
    use super::*;
    use crate::cancellation::CancellationToken;
    use crate::source_watch::HostChangeSink;

    struct Ignored;
    impl HostChangeSink for Ignored {
        fn logical_owned_bytes(&self) -> u64 { 0 }
        fn host_changed(&self, _: &[HostChange], _: &mut DemandWork) -> Result<(), DemandError> {
            Ok(())
        }
        fn host_changed_with_native_work(
            &self, _: &[HostChange], work: &mut crate::demand::NativeSourceWork<'_>,
        ) -> Result<(), DemandError> { work.check() }
    }

    fn watcher(root: &Path) -> io::Result<PlatformWatch> {
        PlatformWatch::start(
            &crate::native_host::HostRoot::open(root)?,
            Arc::new(Delivery::new(Arc::new(Ignored))),
        )
    }

    fn retained_records(watch: &PlatformWatch) -> u64 {
        let reading = watch.shared.reading.lock().unwrap_or_else(PoisonError::into_inner);
        let watches = watch.shared.lock_watches();
        let ancestors = watch.shared.ancestors.lock().unwrap_or_else(PoisonError::into_inner);
        (reading.cleanup.len() + watches.entries.len() + ancestors.len()) as u64
    }

    fn actual_payload_bytes(
        reading: &RawReads,
        watches: &Watches,
        ancestors: &[(i32, std::ffi::OsString)],
        ancestor_capacity: usize,
    ) -> u64 {
        arc_control_bytes::<Shared>() + reading.buffer.capacity() as u64
            + reading.cleanup.capacity() as u64 * std::mem::size_of::<PathBuf>() as u64
            + reading.cleanup.iter().map(|path| path.capacity() as u64).sum::<u64>()
            + watches.entries.capacity() as u64 * std::mem::size_of::<(PathBuf, i32)>() as u64
            + watches.entries.iter().map(|(path, _)| path.capacity() as u64).sum::<u64>()
            + ancestor_capacity as u64 * std::mem::size_of::<(i32, std::ffi::OsString)>() as u64
            + ancestors.iter().map(|(_, name)| name.capacity() as u64).sum::<u64>()
    }

    fn assert_actual_payload(watch: &PlatformWatch) {
        let reading = watch.shared.reading.lock().unwrap_or_else(PoisonError::into_inner);
        let watches = watch.shared.lock_watches();
        let ancestors = watch.shared.ancestors.lock().unwrap_or_else(PoisonError::into_inner);
        assert_eq!(watch.logical_owned_bytes(), actual_payload_bytes(
            &reading, &watches, &ancestors, ancestors.capacity(),
        ));
    }

    #[test]
    fn later_alias_attempts_borrow_original_payload_without_allocation_claims()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("a"))?;
        let watch = watcher(root.path())?;
        assert!(watch.admit(Path::new("a")));
        let retained = watch.logical_owned_bytes();
        let mut existing = DemandWork::new(WorkBudget {
            backend_read_operations: 1,
            allocation_operations: 2,
            peak_allocation_bytes: 3,
            items_examined: 3,
            ..WorkBudget::UNBOUNDED
        }, &CancellationToken::new());
        assert!(!watch.admit_measured(Path::new("a"), &mut existing)?);
        assert_eq!(existing.work.peak_allocation_bytes, 3);
        assert_eq!(existing.work.allocation_operations, 2);
        assert_eq!(existing.work.items_examined, 3);
        assert_eq!(existing.remaining()?.peak_allocation_bytes, 3);
        assert_eq!(watch.logical_owned_bytes(), retained);
        let mut removed = DemandWork::new(WorkBudget {
            peak_allocation_bytes: 0, ..WorkBudget::UNBOUNDED
        }, &CancellationToken::new());
        watch.unadmit_measured(Path::new("a"), &mut removed)?;
        assert_eq!(removed.work.peak_allocation_bytes, 0);
        assert_eq!(removed.work.allocation_operations, 0);
        assert_eq!(removed.work.backend_write_operations, 1);
        assert_actual_payload(&watch);
        Ok(())
    }

    #[test]
    fn readonly_payload_returns_while_all_callback_owned_mutexes_are_held()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir_all(root.path().join("a/b"))?;
        let watch = Arc::new(watcher(root.path())?);
        assert_actual_payload(&watch);
        assert!(watch.admit(Path::new("a/b")));
        assert_actual_payload(&watch);
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
        watch.unadmit_measured(Path::new("a/b"), &mut work)?;
        assert_actual_payload(&watch);
        let reading = watch.shared.reading.lock().unwrap_or_else(PoisonError::into_inner);
        let watches = watch.shared.lock_watches();
        let ancestors = watch.shared.ancestors.lock().unwrap_or_else(PoisonError::into_inner);
        let expected = actual_payload_bytes(&reading, &watches, &ancestors, ancestors.capacity());
        let querying = Arc::clone(&watch);
        let (sent, received) = std::sync::mpsc::channel();
        let getter = std::thread::spawn(move || {
            let _ = sent.send((querying.logical_owned_bytes(), querying.is_stopped()));
        });
        let answer = received.recv_timeout(std::time::Duration::from_secs(5));
        drop(ancestors);
        drop(watches);
        drop(reading);
        getter.join().expect("readonly getter does not panic");
        assert_eq!(answer?, (expected, false));
        Ok(())
    }

    #[test]
    fn payload_tracks_pending_cleanup_growth_transfer_pop_and_native_ignored()
    -> Result<(), Box<dyn std::error::Error>> {
        struct CancelOnce(Mutex<Option<CancellationToken>>);
        impl HostChangeSink for CancelOnce {
            fn logical_owned_bytes(&self) -> u64 { 0 }
            fn host_changed(&self, _: &[HostChange], _: &mut DemandWork) -> Result<(), DemandError> {
                if let Some(cancellation) = self.0.lock().unwrap_or_else(PoisonError::into_inner).take() {
                    cancellation.cancel();
                }
                Ok(())
            }
        }
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("a"))?;
        let cancellation = CancellationToken::new();
        let sink = Arc::new(CancelOnce(Mutex::new(Some(cancellation.clone()))));
        let watch = PlatformWatch::start(
            &crate::native_host::HostRoot::open(root.path())?,
            Arc::new(Delivery::new(sink)),
        )?;
        assert!(watch.admit(Path::new("a")));
        assert_actual_payload(&watch);
        {
            let mut reading = watch.shared.reading.lock().unwrap_or_else(PoisonError::into_inner);
            reading.blocked = true;
            std::fs::create_dir(root.path().join("event"))?;
        }
        let cleanup_peak = "event".len() as u64 + std::mem::size_of::<PathBuf>() as u64;
        let mut cancelled = DemandWork::new(WorkBudget {
            peak_allocation_bytes: cleanup_peak, ..WorkBudget::UNBOUNDED
        }, &cancellation);
        assert!(matches!(
            watch.fence_events_measured(std::time::Duration::MAX, &mut cancelled),
            Err(DemandError::Cancelled),
        ));
        assert_eq!(cancelled.work.peak_allocation_bytes, cleanup_peak);
        assert_actual_payload(&watch);
        {
            let reading = watch.shared.reading.lock().unwrap_or_else(PoisonError::into_inner);
            assert_eq!(reading.cleanup, [PathBuf::from("event")]);
            assert_eq!(reading.cleanup_path_bytes, reading.cleanup[0].capacity() as u64);
        }
        let mut resumed = DemandWork::new(WorkBudget {
            peak_allocation_bytes: 0, ..WorkBudget::UNBOUNDED
        }, &CancellationToken::new());
        assert!(watch.fence_events_measured(std::time::Duration::MAX, &mut resumed)?);
        assert_actual_payload(&watch);
        {
            let reading = watch.shared.reading.lock().unwrap_or_else(PoisonError::into_inner);
            assert!(reading.cleanup.is_empty());
            assert_eq!(reading.cleanup_path_bytes, 0);
            assert!(reading.cleanup.capacity() > 0);
        }
        {
            let mut reading = watch.shared.reading.lock().unwrap_or_else(PoisonError::into_inner);
            reading.blocked = true;
            let watches = watch.shared.lock_watches();
            let descriptor = watches.entries.iter()
                .find(|(path, _)| path == Path::new("a")).expect("admitted directory").1;
            // SAFETY: this fixture removes its own live registration while
            // the native reader is paused, then drains the real IN_IGNORED.
            assert_eq!(unsafe { libc::inotify_rm_watch(watch.shared.inotify.as_raw_fd(), descriptor) }, 0);
        }
        assert!(watch.fence_events_measured(std::time::Duration::MAX, &mut resumed)?);
        assert!(!watch.shared.lock_watches().contains_path(Path::new("a")));
        assert_actual_payload(&watch);
        watch.stop_measured(&mut resumed)?;
        assert_eq!(watch.shared.retained_variable_bytes.load(Ordering::Acquire), 0);
        assert_eq!(watch.logical_owned_bytes(), arc_control_bytes::<Shared>());
        Ok(())
    }

    fn stop_budget(records: u64) -> WorkBudget {
        WorkBudget {
            backend_read_operations: 1,
            backend_write_operations: 4,
            items_examined: records,
            ..WorkBudget::default()
        }
    }

    #[test]
    fn stop_denial_and_precancellation_keep_the_actual_worker_and_descriptors()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("a"))?;
        let watch = watcher(root.path())?;
        assert!(!watch.is_stopped());
        assert!(watch.admit(Path::new("a")));
        let descriptors = [
            watch.shared.inotify.as_raw_fd(), watch.shared.wake.as_raw_fd(),
            watch.shared.root.as_raw_fd(),
        ];
        let records = retained_records(&watch);
        let bytes = watch.logical_owned_bytes();
        for budget in [
            WorkBudget { backend_write_operations: 3, ..stop_budget(records) },
            WorkBudget { backend_read_operations: 0, ..stop_budget(records) },
            WorkBudget { items_examined: records - 1, ..stop_budget(records) },
        ] {
            let mut work = DemandWork::new(budget, &CancellationToken::new());
            assert!(matches!(watch.stop_measured(&mut work), Err(DemandError::Work(_))));
            assert_eq!(work.work, WorkCounters::default());
            assert!(!watch.shared.stopping.load(Ordering::Acquire));
            assert!(!watch.is_stopped());
            assert!(watch.thread.lock().unwrap_or_else(PoisonError::into_inner).is_some());
            assert!(watch.shared.lock_watches().contains_path(Path::new("a")));
            assert!(watch.delivery().is_exact());
            assert_eq!(watch.logical_owned_bytes(), bytes);
            for descriptor in descriptors {
                // SAFETY: only queries the existing native owner.
                assert!(unsafe { libc::fcntl(descriptor, libc::F_GETFD) } >= 0);
            }
        }
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let mut cancelled = DemandWork::new(stop_budget(records), &cancellation);
        assert!(matches!(watch.stop_measured(&mut cancelled), Err(DemandError::Cancelled)));
        assert_eq!(cancelled.work, WorkCounters::default());
        assert!(!watch.shared.stopping.load(Ordering::Acquire));
        assert!(!watch.is_stopped());
        assert_eq!(watch.logical_owned_bytes(), bytes);
        let mut allowed = DemandWork::new(stop_budget(records), &CancellationToken::new());
        watch.stop_measured(&mut allowed)?;
        assert!(watch.is_stopped());
        assert_eq!(allowed.work.backend_read_operations, 1);
        assert_eq!(allowed.work.backend_write_operations, 4);
        assert_eq!(allowed.work.items_examined, records);
        Ok(())
    }

    #[test]
    fn finite_stop_closes_the_same_fds_and_destroys_every_retained_record_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir_all(root.path().join("a/b"))?;
        let host = crate::native_host::HostRoot::open(root.path())?;
        let watch = watcher(root.path())?;
        assert!(watch.admit(Path::new("a/b")));
        {
            let mut reading = watch.shared.reading.lock().unwrap_or_else(PoisonError::into_inner);
            let mut path = PathBuf::with_capacity(37);
            path.push("a/pending");
            reading.cleanup = Vec::with_capacity(2);
            reading.cleanup_path_bytes = path.capacity() as u64;
            reading.cleanup.push(path);
            watch.shared.retained_variable_bytes.fetch_add(
                reading.cleanup.capacity() as u64 * std::mem::size_of::<PathBuf>() as u64
                    + reading.cleanup_path_bytes,
                Ordering::AcqRel,
            );
            reading.blocked = true;
        }
        let bytes = {
            let reading = watch.shared.reading.lock().unwrap_or_else(PoisonError::into_inner);
            let watches = watch.shared.lock_watches();
            let ancestors = watch.shared.ancestors.lock().unwrap_or_else(PoisonError::into_inner);
            arc_control_bytes::<Shared>() + reading.buffer.capacity() as u64
                + reading.cleanup.capacity() as u64 * std::mem::size_of::<PathBuf>() as u64
                + reading.cleanup.iter().map(|path| path.capacity() as u64).sum::<u64>()
                + watches.entries.capacity() as u64 * std::mem::size_of::<(PathBuf, i32)>() as u64
                + watches.entries.iter().map(|(path, _)| path.capacity() as u64).sum::<u64>()
                + ancestors.capacity() as u64 * std::mem::size_of::<(i32, std::ffi::OsString)>() as u64
                + ancestors.iter().map(|(_, name)| name.capacity() as u64).sum::<u64>()
        };
        assert_eq!(watch.logical_owned_bytes(), bytes);
        let descriptors = [
            watch.shared.inotify.as_raw_fd(), watch.shared.wake.as_raw_fd(),
            watch.shared.root.as_raw_fd(),
        ];
        let records = retained_records(&watch);
        let mut work = DemandWork::new(stop_budget(records), &CancellationToken::new());
        watch.stop_measured(&mut work)?;
        assert!(watch.is_stopped());
        assert_eq!(work.work, WorkCounters {
            backend_read_operations: 1, backend_write_operations: 4,
            items_examined: records, ..WorkCounters::default()
        });
        assert_eq!(watch.logical_owned_bytes(), arc_control_bytes::<Shared>());
        assert!(watch.thread.lock().unwrap_or_else(PoisonError::into_inner).is_none());
        assert!(!watch.delivery().is_exact());
        for descriptor in descriptors {
            // SAFETY: a closed integer descriptor can be queried without reuse.
            assert_eq!(unsafe { libc::fcntl(descriptor, libc::F_GETFD) }, -1);
            assert_eq!(last_error().raw_os_error(), Some(libc::EBADF));
        }
        let mut stopped = DemandWork::new(WorkBudget::default(), &CancellationToken::new());
        watch.stop_measured(&mut stopped)?;
        assert!(watch.is_stopped());
        assert!(!watch.admit_measured(Path::new("a"), &mut stopped)?);
        watch.unadmit_measured(Path::new("a"), &mut stopped)?;
        assert!(!watch.fence_events_measured(std::time::Duration::MAX, &mut stopped)?);
        assert_eq!(stopped.work, WorkCounters::default());
        // Allocate new owners that may reuse the just-released native numbers,
        // without replacing any descriptor another parallel fixture acquired.
        let mut replacements = Vec::with_capacity(3);
        for _ in descriptors {
            // SAFETY: duplicates the live root into a new exclusively-owned fd.
            let duplicate = unsafe {
                libc::fcntl(host.directory_fd(), libc::F_DUPFD_CLOEXEC, 0)
            };
            replacements.push(owned(duplicate)?);
        }
        drop(watch);
        for replacement in &replacements {
            // SAFETY: these duplicates now belong to this fixture alone.
            assert!(unsafe { libc::fcntl(replacement.as_raw_fd(), libc::F_GETFD) } >= 0);
        }
        Ok(())
    }

    #[test]
    fn admitted_native_cleanup_keeps_the_original_meter_after_cancellation()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let watch = watcher(root.path())?;
        let records = retained_records(&watch);
        let cancellation = CancellationToken::new();
        let mut work = DemandWork::new(stop_budget(records), &cancellation);
        work.work.admit(&WorkCounters {
            backend_read_operations: 1, backend_write_operations: 4,
            items_examined: records, ..WorkCounters::default()
        }, &work.budget)?;
        cancellation.cancel();
        watch.stop_admitted(
            &mut watch.thread.lock().unwrap_or_else(PoisonError::into_inner), &mut work,
        )?;
        assert_eq!(work.work, WorkCounters {
            backend_read_operations: 1, backend_write_operations: 4,
            items_examined: records, ..WorkCounters::default()
        });
        assert_eq!(watch.logical_owned_bytes(), arc_control_bytes::<Shared>());
        assert!(watch.is_stopped());
        Ok(())
    }

    #[cfg(feature = "native-mount")]
    #[test]
    fn shared_stop_borrows_retained_payload_and_preserves_original_allocation_ledger()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("a"))?;
        let watch = watcher(root.path())?;
        assert!(watch.admit(Path::new("a")));
        let records = retained_records(&watch);
        let bytes = watch.logical_owned_bytes();
        let mut original = DemandWork::new(WorkBudget {
            backend_read_operations: 2, backend_write_operations: 5,
            items_examined: records + 2, allocation_operations: 1,
            peak_allocation_bytes: 7, ..WorkBudget::default()
        }, &CancellationToken::new());
        original.allocate(7)?;
        original.charge(WorkCounters {
            backend_read_operations: 1, backend_write_operations: 1,
            items_examined: 2, ..WorkCounters::default()
        })?;
        let prefix = original.work;
        let shared = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(original)));
        let mut work = crate::demand::NativeSourceWork::new(&shared);
        // The source-aware callback guard must run before even the original
        // leaf lock, not merely before locking the native worker owner.
        shared.with_original_leaf(|_| watch.delivery().callback(|| {
            assert!(matches!(watch.stop_with_work(&mut work), Err(DemandError::WorkerUnavailable)));
        }));
        shared.with_original_leaf(|original| original.budget.backend_write_operations = 4);
        assert!(matches!(watch.stop_with_work(&mut work), Err(DemandError::Work(_))));
        assert_eq!(shared.lock().work, prefix);
        assert_eq!(watch.logical_owned_bytes(), bytes);
        assert!(!watch.shared.stopping.load(Ordering::Acquire));
        assert!(!watch.is_stopped());
        shared.with_original_leaf(|original| original.budget.backend_write_operations = 5);
        watch.stop_with_work(&mut work)?;
        let spent = {
            let original = shared.lock();
            assert_eq!(original.work, WorkCounters {
                backend_read_operations: 2, backend_write_operations: 5,
                items_examined: records + 2, allocation_operations: 1,
                peak_allocation_bytes: 7, ..WorkCounters::default()
            });
            assert_eq!(original.live_allocation_bytes(), 7);
            original.work
        };
        assert!(watch.is_stopped());
        assert!(watch.thread.lock().unwrap_or_else(PoisonError::into_inner).is_none());
        assert_eq!(watch.logical_owned_bytes(), arc_control_bytes::<Shared>());
        for descriptor in [&watch.shared.inotify, &watch.shared.wake, &watch.shared.root] {
            assert_eq!(descriptor.as_raw_fd(), -1);
        }
        watch.stop_with_work(&mut work)?;
        assert_eq!(shared.lock().work, spent);
        assert_eq!(shared.lock().live_allocation_bytes(), 7);
        Ok(())
    }

    #[cfg(feature = "native-mount")]
    #[test]
    fn admitted_shared_stop_preserves_actual_join_close_prefix_on_late_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("a"))?;
        let watch = watcher(root.path())?;
        assert!(watch.admit(Path::new("a")));
        let descriptors = [
            watch.shared.inotify.as_raw_fd(), watch.shared.wake.as_raw_fd(),
            watch.shared.root.as_raw_fd(),
        ];
        let records = retained_records(&watch);
        let cancellation = CancellationToken::new();
        let mut original = DemandWork::new(WorkBudget {
            allocation_operations: 1, peak_allocation_bytes: 7,
            items_examined: records + 2, ..stop_budget(records)
        }, &cancellation);
        original.allocate(7)?;
        original.charge(WorkCounters { items_examined: 2, ..WorkCounters::default() })?;
        let shared = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(original)));
        let mut work = crate::demand::NativeSourceWork::new(&shared);
        work.preflight(WorkCounters {
            backend_read_operations: 1, backend_write_operations: 4,
            items_examined: records, ..WorkCounters::default()
        })?;
        // Simulate a late accounting denial after full suffix admission.
        // Cancellation must not prevent the already-admitted native cleanup.
        shared.with_original_leaf(|original| original.budget.backend_write_operations = 3);
        cancellation.cancel();
        assert!(matches!(watch.stop_admitted(
            &mut watch.thread.lock().unwrap_or_else(PoisonError::into_inner), &mut work,
        ), Err(DemandError::Work(WorkError::BudgetExceeded {
            counter: "backend_write_operations", ..
        }))));
        let original = shared.lock();
        assert_eq!(original.work, WorkCounters {
            backend_read_operations: 1, backend_write_operations: 4,
            items_examined: records + 2, allocation_operations: 1,
            peak_allocation_bytes: 7, ..WorkCounters::default()
        });
        assert_eq!(original.live_allocation_bytes(), 7);
        drop(original);
        assert!(watch.is_stopped());
        assert!(watch.thread.lock().unwrap_or_else(PoisonError::into_inner).is_none());
        assert!(!watch.delivery().is_exact());
        assert_eq!(watch.shared.retained_variable_bytes.load(Ordering::Acquire), 0);
        assert_eq!(watch.logical_owned_bytes(), arc_control_bytes::<Shared>());
        for descriptor in descriptors {
            // SAFETY: queries the original descriptor after its actual close.
            assert_eq!(unsafe { libc::fcntl(descriptor, libc::F_GETFD) }, -1);
            assert_eq!(last_error().raw_os_error(), Some(libc::EBADF));
        }
        Ok(())
    }

    #[test]
    fn stop_waits_for_the_actual_inflight_callback_to_quiesce()
    -> Result<(), Box<dyn std::error::Error>> {
        struct BlockingSink {
            entered: std::sync::mpsc::Sender<u64>,
            watch: Mutex<std::sync::Weak<PlatformWatch>>,
            released: Mutex<bool>,
            changed: Condvar,
        }
        impl HostChangeSink for BlockingSink {
            fn logical_owned_bytes(&self) -> u64 { 0 }
            fn host_changed(&self, _: &[HostChange], _: &mut DemandWork) -> Result<(), DemandError> {
                let watch = self.watch.lock().unwrap_or_else(PoisonError::into_inner)
                    .upgrade().expect("callback fixture owns the watch");
                let _ = self.entered.send(watch.logical_owned_bytes());
                let mut released = self.released.lock().unwrap_or_else(PoisonError::into_inner);
                while !*released {
                    released = self.changed.wait(released).unwrap_or_else(PoisonError::into_inner);
                }
                Ok(())
            }
        }
        let root = tempfile::tempdir()?;
        let (entered, entering) = std::sync::mpsc::channel();
        let sink = Arc::new(BlockingSink {
            entered, watch: Mutex::new(std::sync::Weak::new()),
            released: Mutex::new(false), changed: Condvar::new(),
        });
        let watch = Arc::new(PlatformWatch::start(
            &crate::native_host::HostRoot::open(root.path())?,
            Arc::new(Delivery::new(sink.clone())),
        )?);
        *sink.watch.lock().unwrap_or_else(PoisonError::into_inner) = Arc::downgrade(&watch);
        let expected_callback_bytes = watch.logical_owned_bytes() + std::mem::size_of::<PathBuf>() as u64;
        std::fs::write(root.path().join("event"), b"change")?;
        let entered = entering.recv_timeout(std::time::Duration::from_secs(5));
        let callback_bytes = match entered {
            Ok(bytes) => bytes,
            Err(error) => {
                *sink.released.lock().unwrap_or_else(PoisonError::into_inner) = true;
                sink.changed.notify_all();
                return Err(error.into());
            }
        };
        let querying = Arc::clone(&watch);
        let (sent, received) = std::sync::mpsc::channel();
        let getter = std::thread::spawn(move || {
            let _ = sent.send(querying.logical_owned_bytes());
        });
        let queried = received.recv_timeout(std::time::Duration::from_secs(5));
        let (invoked, invoking) = std::sync::mpsc::channel();
        let (finished, finishing) = std::sync::mpsc::channel();
        let stopping = Arc::clone(&watch);
        let stopper = std::thread::spawn(move || {
            let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
            let _ = invoked.send(());
            let result = stopping.stop_measured(&mut work);
            let _ = finished.send((result, work.work));
        });
        let invoked = invoking.recv_timeout(std::time::Duration::from_secs(5));
        let early = finishing.recv_timeout(std::time::Duration::from_millis(50));
        *sink.released.lock().unwrap_or_else(PoisonError::into_inner) = true;
        sink.changed.notify_all();
        stopper.join().expect("stopper does not panic");
        getter.join().expect("readonly getter does not panic");
        invoked?;
        assert_eq!(callback_bytes, expected_callback_bytes);
        assert_eq!(queried?, callback_bytes);
        assert!(matches!(early, Err(std::sync::mpsc::RecvTimeoutError::Timeout)));
        let (result, work) = finishing.recv_timeout(std::time::Duration::from_secs(5))?;
        result?;
        assert_eq!(work.backend_read_operations, 1);
        assert_eq!(work.backend_write_operations, 4);
        assert_eq!(watch.logical_owned_bytes(), arc_control_bytes::<Shared>());
        assert!(watch.is_stopped());
        Ok(())
    }

    #[test]
    fn actual_callback_stop_is_denied_before_work_and_independent_retry_succeeds()
    -> Result<(), Box<dyn std::error::Error>> {
        struct StoppingSink {
            watch: Mutex<std::sync::Weak<PlatformWatch>>,
            reported: AtomicBool,
            result: std::sync::mpsc::Sender<(Result<(), DemandError>, WorkCounters, bool, bool)>,
        }
        impl HostChangeSink for StoppingSink {
            fn logical_owned_bytes(&self) -> u64 { 0 }
            fn host_changed(&self, _: &[HostChange], _: &mut DemandWork) -> Result<(), DemandError> {
                if self.reported.swap(true, Ordering::AcqRel) {
                    return Ok(());
                }
                let watch = self.watch.lock().unwrap_or_else(PoisonError::into_inner)
                    .upgrade().expect("callback fixture owns the original watch");
                let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
                let result = watch.stop_measured(&mut work);
                let _ = self.result.send((
                    result, work.work, watch.is_stopped(),
                    watch.shared.stopping.load(Ordering::Acquire),
                ));
                Ok(())
            }
        }
        let root = tempfile::tempdir()?;
        let (sent, received) = std::sync::mpsc::channel();
        let sink = Arc::new(StoppingSink {
            watch: Mutex::new(std::sync::Weak::new()),
            reported: AtomicBool::new(false), result: sent,
        });
        let watch = Arc::new(PlatformWatch::start(
            &crate::native_host::HostRoot::open(root.path())?,
            Arc::new(Delivery::new(sink.clone())),
        )?);
        *sink.watch.lock().unwrap_or_else(PoisonError::into_inner) = Arc::downgrade(&watch);
        assert!(!watch.is_stopped());
        let descriptors = [
            watch.shared.inotify.as_raw_fd(), watch.shared.wake.as_raw_fd(),
            watch.shared.root.as_raw_fd(),
        ];
        std::fs::write(root.path().join("event"), b"change")?;
        let (result, work, completed, stopping) =
            received.recv_timeout(std::time::Duration::from_secs(5))?;
        assert!(matches!(result, Err(DemandError::WorkerUnavailable)));
        assert_eq!(work, WorkCounters::default());
        assert!(!completed);
        assert!(!stopping);
        assert!(watch.thread.lock().unwrap_or_else(PoisonError::into_inner).is_some());
        assert!(watch.shared.lock_watches().contains_path(Path::new("")));
        assert!(watch.delivery().is_exact());
        for descriptor in descriptors {
            // SAFETY: queries the same still-owned native descriptor.
            assert!(unsafe { libc::fcntl(descriptor, libc::F_GETFD) } >= 0);
        }
        let retrying = Arc::clone(&watch);
        let retry = std::thread::spawn(move || {
            let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
            let result = retrying.stop_measured(&mut work);
            (result, work.work)
        });
        let (result, work) = retry.join().expect("independent stop does not panic");
        result?;
        assert_eq!(work.backend_read_operations, 1);
        assert_eq!(work.backend_write_operations, 4);
        assert!(watch.is_stopped());
        assert_eq!(watch.logical_owned_bytes(), arc_control_bytes::<Shared>());
        assert!(watch.thread.lock().unwrap_or_else(PoisonError::into_inner).is_none());
        let mut repeated = DemandWork::new(WorkBudget::default(), &CancellationToken::new());
        watch.stop_measured(&mut repeated)?;
        assert!(watch.is_stopped());
        assert_eq!(repeated.work, WorkCounters::default());
        Ok(())
    }

    #[test]
    fn failed_native_wake_is_not_completed_and_preserves_retry_custody()
    -> Result<(), Box<dyn std::error::Error>> {
        struct SignallingSink(std::sync::mpsc::Sender<()>);
        impl HostChangeSink for SignallingSink {
            fn logical_owned_bytes(&self) -> u64 { 0 }
            fn host_changed(&self, _: &[HostChange], _: &mut DemandWork) -> Result<(), DemandError> {
                let _ = self.0.send(());
                Ok(())
            }
        }
        let root = tempfile::tempdir()?;
        let (sent, received) = std::sync::mpsc::channel();
        let watch = PlatformWatch::start(
            &crate::native_host::HostRoot::open(root.path())?,
            Arc::new(Delivery::new(Arc::new(SignallingSink(sent)))),
        )?;
        std::fs::write(root.path().join("event"), b"change")?;
        // An actual worker callback proves poll already cached the original fd.
        received.recv_timeout(std::time::Duration::from_secs(5))?;
        let records = retained_records(&watch);
        let bytes = watch.logical_owned_bytes();
        let descriptor = watch.shared.wake.take();
        let mut denied = DemandWork::new(stop_budget(records), &CancellationToken::new());
        let result = watch.stop_measured(&mut denied);
        // Restore the same still-live fd before any assertion or fallible exit.
        watch.shared.wake.0.store(descriptor, Ordering::Release);
        assert!(matches!(result, Err(DemandError::Io(_))));
        assert_eq!(denied.work, WorkCounters {
            backend_write_operations: 1, ..WorkCounters::default()
        });
        assert!(watch.shared.stopping.load(Ordering::Acquire));
        assert!(!watch.is_stopped());
        assert!(watch.thread.lock().unwrap_or_else(PoisonError::into_inner).is_some());
        assert_eq!(watch.logical_owned_bytes(), bytes);
        let mut retry = DemandWork::new(stop_budget(records), &CancellationToken::new());
        watch.stop_measured(&mut retry)?;
        assert!(watch.is_stopped());
        assert_eq!(retry.work.backend_read_operations, 1);
        assert_eq!(retry.work.backend_write_operations, 4);
        assert_eq!(retry.work.items_examined, records);
        assert_eq!(watch.logical_owned_bytes(), arc_control_bytes::<Shared>());
        Ok(())
    }

    #[test]
    fn startup_reserves_native_cleanup_before_creation_and_records_only_spent_closes()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let host = crate::native_host::HostRoot::open(root.path())?;
        let mut denied = DemandWork::new(WorkBudget {
            backend_write_operations: 2, ..WorkBudget::UNBOUNDED
        }, &CancellationToken::new());
        assert!(matches!(PlatformWatch::start_measured(
            &host, Arc::new(Delivery::new(Arc::new(Ignored))), &mut denied,
        ), Err(DemandError::Work(_))));
        assert_eq!(denied.work, WorkCounters::default());
        let budget = WorkBudget {
            backend_read_operations: 1, backend_write_operations: 3,
            ..WorkBudget::UNBOUNDED
        };
        let mut partial = DemandWork::new(budget, &CancellationToken::new());
        assert!(matches!(PlatformWatch::start_measured(
            &host, Arc::new(Delivery::new(Arc::new(Ignored))), &mut partial,
        ), Err(DemandError::Work(_))));
        assert_eq!(partial.work.backend_read_operations, 1);
        assert_eq!(partial.work.backend_write_operations, 1);
        assert_eq!(partial.work.allocation_operations, 1);
        assert_eq!(partial.work.peak_allocation_bytes, arc_control_bytes::<Shared>());
        assert_eq!(partial.live_allocation_bytes(), 0);
        assert_eq!(partial.budget, budget);
        Ok(())
    }

    #[test]
    fn startup_denies_descriptor_creation_before_any_native_work()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let host = crate::native_host::HostRoot::open(root.path())?;
        let cancellation = CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::default(), &cancellation);
        assert!(matches!(
            PlatformWatch::start_measured(&host,
                Arc::new(Delivery::new(Arc::new(Ignored))), &mut work),
            Err(DemandError::Work(_))
        ));
        assert_eq!(work.work, WorkCounters::default());
        cancellation.cancel();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        assert!(matches!(
            PlatformWatch::start_measured(&host,
                Arc::new(Delivery::new(Arc::new(Ignored))), &mut work),
            Err(DemandError::Cancelled)
        ));
        assert_eq!(work.work, WorkCounters::default());
        Ok(())
    }

    #[test]
    fn startup_releases_the_proc_readlink_buffer_when_the_native_read_is_denied()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let host = crate::native_host::HostRoot::open(root.path())?;
        let cancellation = CancellationToken::new();
        let control_bytes = arc_control_bytes::<Shared>();
        let simultaneous_bytes = control_bytes + 256;
        let mut work = DemandWork::new(WorkBudget {
            backend_read_operations: 3,
            peak_allocation_bytes: simultaneous_bytes,
            ..WorkBudget::UNBOUNDED
        }, &cancellation);
        assert!(matches!(
            PlatformWatch::start_measured(&host,
                Arc::new(Delivery::new(Arc::new(Ignored))), &mut work),
            Err(DemandError::Work(WorkError::BudgetExceeded {
                counter: "backend_read_operations", ..
            }))
        ));
        assert_eq!(work.work.backend_read_operations, 3);
        assert_eq!(work.work.backend_write_operations, 3);
        assert_eq!(work.work.allocation_operations, 2);
        assert_eq!(work.work.peak_allocation_bytes, simultaneous_bytes);
        assert_eq!(work.work.source_bytes_read, 0);
        assert_eq!(work.live_allocation_bytes(), 0);
        assert_eq!(work.remaining()?.peak_allocation_bytes, simultaneous_bytes);
        Ok(())
    }

    #[test]
    fn finite_startup_uses_the_same_root_and_ancestor_registrations()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let host = crate::native_host::HostRoot::open(root.path())?;
        let ordinary = watcher(root.path())?;
        let cancellation = CancellationToken::new();
        let budget = WorkBudget {
            backend_read_operations: 128,
            backend_write_operations: 4,
            source_bytes_read: 64 * 1024,
            bytes_copied: 1024 * 1024,
            items_examined: 4096,
            allocation_operations: 256,
            peak_allocation_bytes: 1024 * 1024,
            ..WorkBudget::default()
        };
        let mut work = DemandWork::new(budget, &cancellation);
        let measured = PlatformWatch::start_measured(&host,
            Arc::new(Delivery::new(Arc::new(Ignored))), &mut work)?;
        assert_eq!(
            *measured.shared.ancestors.lock().unwrap_or_else(PoisonError::into_inner),
            *ordinary.shared.ancestors.lock().unwrap_or_else(PoisonError::into_inner),
        );
        assert!(measured.shared.lock_watches().contains_path(Path::new("")));
        assert!(measured.delivery().is_exact());
        assert!(work.work.backend_read_operations >= 6);
        assert!(work.work.source_bytes_read > 0);
        assert!(work.work.allocation_operations >= 5);
        assert!(work.work.peak_allocation_bytes >= READ_BUFFER_BYTES as u64);
        assert!(measured.thread.lock().unwrap_or_else(PoisonError::into_inner).is_some());
        assert_eq!(work.remaining()?.peak_allocation_bytes, budget.peak_allocation_bytes);
        Ok(())
    }

    #[test]
    fn denied_worker_start_releases_all_retained_startup_payloads()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let host = crate::native_host::HostRoot::open(root.path())?;
        let cancellation = CancellationToken::new();
        let mut complete = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let watch = PlatformWatch::start_measured(&host,
            Arc::new(Delivery::new(Arc::new(Ignored))), &mut complete)?;
        let before_worker = complete.work.backend_read_operations - 1;
        let records = retained_records(&watch);
        drop(watch);
        let mut denied = DemandWork::new(WorkBudget {
            backend_read_operations: before_worker,
            peak_allocation_bytes: 1024 * 1024,
            ..WorkBudget::UNBOUNDED
        }, &cancellation);
        assert!(matches!(
            PlatformWatch::start_measured(&host,
                Arc::new(Delivery::new(Arc::new(Ignored))), &mut denied),
            Err(DemandError::Work(WorkError::BudgetExceeded {
                counter: "backend_read_operations", ..
            }))
        ));
        assert_eq!(denied.work.backend_read_operations, before_worker);
        assert_eq!(denied.work.backend_write_operations, 3);
        assert_eq!(denied.work.items_examined, complete.work.items_examined + records);
        assert!(denied.work.peak_allocation_bytes >= READ_BUFFER_BYTES as u64);
        assert_eq!(denied.remaining()?.peak_allocation_bytes, 1024 * 1024);
        Ok(())
    }

    #[test]
    fn startup_grows_the_proc_readlink_buffer_without_truncating_ancestry()
    -> Result<(), Box<dyn std::error::Error>> {
        let base = tempfile::tempdir()?;
        let mut root = base.path().to_path_buf();
        for index in 0..8 {
            root.push(format!("ancestor-{index}-{}", "x".repeat(40)));
            std::fs::create_dir(&root)?;
        }
        let host = crate::native_host::HostRoot::open(&root)?;
        let cancellation = CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget {
            backend_read_operations: 128,
            backend_write_operations: 4,
            source_bytes_read: 4096,
            bytes_copied: 64 * 1024,
            items_examined: 1024,
            allocation_operations: 128,
            peak_allocation_bytes: 128 * 1024,
            ..WorkBudget::default()
        }, &cancellation);
        let watch = PlatformWatch::start_measured(&host,
            Arc::new(Delivery::new(Arc::new(Ignored))), &mut work)?;
        let actual_path = std::fs::read_link(format!("/proc/self/fd/{}", watch.shared.root.as_raw_fd()))?;
        assert!(actual_path.as_os_str().len() > 256);
        let mut expected_read = actual_path.as_os_str().len() as u64;
        let mut capacity = 256_usize;
        while capacity <= actual_path.as_os_str().len() {
            expected_read += capacity as u64;
            capacity *= 2;
        }
        assert_eq!(work.work.source_bytes_read, expected_read);
        assert_eq!(watch.shared.ancestors.lock().unwrap_or_else(PoisonError::into_inner).len(),
            actual_path.ancestors().filter(|path| path.file_name().is_some()).count());
        assert!(watch.shared.lock_watches().contains_path(Path::new("")));
        Ok(())
    }

    #[test]
    fn finite_admission_preserves_the_first_watch_and_denies_the_later_native_call()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir_all(root.path().join("a/b"))?;
        let watch = watcher(root.path())?;
        let cancellation = CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget {
            backend_read_operations: 3,
            ..WorkBudget::UNBOUNDED
        }, &cancellation);
        assert!(matches!(watch.admit_measured(Path::new("a/b"), &mut work),
            Err(DemandError::Work(WorkError::BudgetExceeded { .. }))));
        assert_eq!(work.work.backend_read_operations, 3);
        assert!(watch.shared.lock_watches().contains_path(Path::new("a")));
        assert!(!watch.shared.lock_watches().contains_path(Path::new("a/b")));
        assert!(watch.delivery().is_exact());
        assert_actual_payload(&watch);
        let mut work = DemandWork::new(WorkBudget {
            backend_read_operations: 3,
            ..WorkBudget::UNBOUNDED
        }, &cancellation);
        assert!(watch.admit_measured(Path::new("a/b"), &mut work)?);
        assert_eq!(work.work.backend_read_operations, 3);
        assert_actual_payload(&watch);
        Ok(())
    }

    #[cfg(feature = "native-mount")]
    #[test]
    fn shared_admission_and_removal_keep_original_counters_and_allocation_ledger()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir_all(root.path().join("a/b"))?;
        let watch = watcher(root.path())?;
        let cancellation = CancellationToken::new();
        let mut original = DemandWork::new(WorkBudget {
            backend_read_operations: 3,
            backend_write_operations: 2,
            ..WorkBudget::UNBOUNDED
        }, &cancellation);
        original.allocate(7)?;
        original.charge(WorkCounters {
            backend_read_operations: 1, ..WorkCounters::default()
        })?;
        let shared = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(original)));
        let mut work = crate::demand::NativeSourceWork::new(&shared);
        assert!(matches!(watch.admit_with_work(Path::new("a/b"), &mut work),
            Err(DemandError::Work(WorkError::BudgetExceeded { .. }))));
        {
            let original = shared.lock();
            assert_eq!(original.work.backend_read_operations, 3);
            assert_eq!(original.live_allocation_bytes(), 7);
            assert!(original.work.peak_allocation_bytes > 7);
        }
        assert!(watch.shared.lock_watches().contains_path(Path::new("a")));
        assert!(!watch.shared.lock_watches().contains_path(Path::new("a/b")));
        assert!(watch.delivery().is_exact());
        assert_actual_payload(&watch);
        watch.unadmit_with_work(Path::new("a"), &mut work)?;
        assert!(!watch.shared.lock_watches().contains_path(Path::new("a")));
        assert_actual_payload(&watch);
        let spent = {
            let original = shared.lock();
            assert_eq!(original.work.backend_write_operations, 2);
            assert_eq!(original.live_allocation_bytes(), 7);
            original.work
        };
        cancellation.cancel();
        assert!(matches!(watch.admit_with_work(Path::new("a/b"), &mut work),
            Err(DemandError::Cancelled)));
        assert!(matches!(watch.unadmit_with_work(Path::new("a"), &mut work),
            Err(DemandError::Cancelled)));
        assert_eq!(shared.lock().work, spent);
        shared.lock().release(7)?;
        Ok(())
    }

    #[test]
    fn denied_native_call_releases_its_admitted_c_path_buffer()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let watch = watcher(root.path())?;
        let mut work = DemandWork::new(WorkBudget {
            backend_read_operations: 0,
            peak_allocation_bytes: 1024,
            ..WorkBudget::UNBOUNDED
        }, &CancellationToken::new());
        assert!(matches!(watch.shared.add_watch_measured(Path::new("a"), &mut work),
            Err(DemandError::Work(WorkError::BudgetExceeded {
                counter: "backend_read_operations", ..
            }))));
        assert_eq!(work.work.backend_read_operations, 0);
        assert_eq!(work.work.allocation_operations, 1);
        assert!(work.work.peak_allocation_bytes > 0);
        assert_eq!(work.remaining()?.peak_allocation_bytes, 1024);
        Ok(())
    }

    #[test]
    fn denied_or_cancelled_admission_never_starts_a_native_call()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("a"))?;
        let watch = watcher(root.path())?;
        let cancellation = CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::default(), &cancellation);
        assert!(matches!(watch.admit_measured(Path::new("a"), &mut work),
            Err(DemandError::Work(_))));
        assert_eq!(work.work, WorkCounters::default());
        cancellation.cancel();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        assert!(matches!(watch.admit_measured(Path::new("a"), &mut work),
            Err(DemandError::Cancelled)));
        assert_eq!(work.work, WorkCounters::default());
        assert!(!watch.shared.lock_watches().contains_path(Path::new("a")));
        Ok(())
    }

    #[test]
    fn absent_prefix_counts_its_failed_native_request_and_keeps_the_ancestor_watch()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("a"))?;
        let watch = watcher(root.path())?;
        let mut work = DemandWork::new(WorkBudget {
            backend_read_operations: 3,
            items_examined: 32,
            ..WorkBudget::UNBOUNDED
        }, &CancellationToken::new());
        assert!(!watch.admit_measured(Path::new("a/missing/deeper"), &mut work)?);
        assert_eq!(work.work.backend_read_operations, 3);
        assert_eq!(work.work.source_path_components, 0);
        assert!(work.work.items_examined >= 2);
        assert!(watch.shared.lock_watches().contains_path(Path::new("a")));
        assert!(watch.delivery().is_exact());
        Ok(())
    }

    #[test]
    fn retained_buffer_growth_is_admitted_before_registration()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("a"))?;
        let watch = watcher(root.path())?;
        let mut work = DemandWork::new(WorkBudget {
            allocation_operations: 2,
            ..WorkBudget::UNBOUNDED
        }, &CancellationToken::new());
        assert!(matches!(watch.admit_measured(Path::new("a"), &mut work),
            Err(DemandError::Work(_))));
        assert_eq!(work.work.allocation_operations, 2);
        assert_eq!(work.work.source_path_components, 0);
        assert!(work.work.items_examined >= 2);
        assert_eq!(work.work.bytes_copied, 2);
        assert_eq!(work.work.backend_read_operations, 1);
        assert!(!watch.shared.lock_watches().contains_path(Path::new("a")));
        assert!(watch.delivery().is_exact());
        assert_actual_payload(&watch);
        Ok(())
    }

    #[test]
    fn cancellation_preserves_work_already_spent_by_the_shared_attempt()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir_all(root.path().join("a/b"))?;
        let watch = watcher(root.path())?;
        let cancellation = CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        assert!(watch.admit_measured(Path::new("a"), &mut work)?);
        let spent = work.work;
        assert_eq!(spent.backend_read_operations, 2);
        cancellation.cancel();
        assert!(matches!(watch.admit_measured(Path::new("a/b"), &mut work),
            Err(DemandError::Cancelled)));
        assert_eq!(work.work, spent);
        assert!(!watch.shared.lock_watches().contains_path(Path::new("a/b")));
        Ok(())
    }

    #[test]
    fn component_admission_does_not_follow_a_directory_symlink_outside_the_held_root()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let outside = tempfile::tempdir()?;
        std::fs::create_dir(outside.path().join("child"))?;
        std::os::unix::fs::symlink(outside.path(), root.path().join("a"))?;
        let watch = watcher(root.path())?;
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
        assert!(!watch.admit_measured(Path::new("a/child"), &mut work)?);
        assert_eq!(work.work.backend_read_operations, 1);
        assert!(!watch.shared.lock_watches().contains_path(Path::new("a")));
        assert!(!watch.shared.lock_watches().contains_path(Path::new("a/child")));
        Ok(())
    }

    #[test]
    fn removal_admits_the_native_write_before_forgetting_the_live_registration()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("a"))?;
        let watch = watcher(root.path())?;
        watch.admit_measured(Path::new("a"),
            &mut DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()))?;
        let mut denied = DemandWork::new(WorkBudget {
            backend_write_operations: 0, peak_allocation_bytes: 0, ..WorkBudget::UNBOUNDED
        }, &CancellationToken::new());
        assert!(matches!(watch.unadmit_measured(Path::new("a"), &mut denied),
            Err(DemandError::Work(_))));
        assert_eq!(denied.work.backend_write_operations, 0);
        assert!(watch.shared.lock_watches().contains_path(Path::new("a")));
        let mut allowed = DemandWork::new(WorkBudget {
            backend_write_operations: 1, peak_allocation_bytes: 0, ..WorkBudget::UNBOUNDED
        }, &CancellationToken::new());
        watch.unadmit_measured(Path::new("a"), &mut allowed)?;
        assert_eq!(allowed.work.backend_write_operations, 1);
        assert_eq!(allowed.work.allocation_operations, 0);
        assert_eq!(allowed.work.peak_allocation_bytes, 0);
        assert!(!watch.shared.lock_watches().contains_path(Path::new("a")));
        Ok(())
    }

    #[test]
    fn retained_growth_receipt_borrows_existing_buffers_and_counts_new_claims()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("a"))?;
        let watch = watcher(root.path())?;
        let record_bytes = std::mem::size_of::<(PathBuf, i32)>() as u64;
        let fd_digits = std::os::fd::AsRawFd::as_raw_fd(&watch.shared.root).to_string().len() as u64;
        let c_path_bytes = b"/proc/self/fd/".len() as u64 + fd_digits + 3;
        let expected_peak = (2 * record_bytes + 1).max(2 + c_path_bytes);
        let mut work = DemandWork::new(WorkBudget {
            backend_read_operations: 2,
            allocation_operations: 5,
            peak_allocation_bytes: expected_peak,
            items_examined: 2,
            ..WorkBudget::UNBOUNDED
        }, &CancellationToken::new());
        assert!(watch.admit_measured(Path::new("a"), &mut work)?);
        assert_eq!(work.work.allocation_operations, 5);
        assert_eq!(work.work.peak_allocation_bytes, expected_peak);
        assert_eq!(work.work.items_examined, 2);
        assert_eq!(work.work.backend_read_operations, 2);
        Ok(())
    }

    #[cfg(feature = "native-mount")]
    #[test]
    fn original_startup_failure_keeps_the_actual_root_prefix_and_retires_its_controls()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let host = crate::native_host::HostRoot::open(root.path())?;
        let cancellation = CancellationToken::new();
        let mut original = DemandWork::new(WorkBudget {
            backend_read_operations: 3, backend_write_operations: 3,
            source_bytes_read: 1024 * 1024, bytes_copied: 1024 * 1024,
            items_examined: 4096, allocation_operations: 128,
            peak_allocation_bytes: 1024 * 1024, ..WorkBudget::default()
        }, &cancellation);
        original.allocate(7)?;
        let original = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(original)));
        let mut work = crate::demand::NativeSourceWork::new(&original);
        let delivery = Arc::new(Delivery::new_with_work(Arc::new(Ignored), &mut work)?);
        assert!(Arc::ptr_eq(delivery.original.as_ref().expect("original delivery"), &original));
        let retired = Arc::downgrade(&delivery);
        assert!(matches!(
            PlatformWatch::start_with_work(&host, delivery, &mut work),
            Err(DemandError::Work(WorkError::BudgetExceeded {
                counter: "backend_read_operations", ..
            }))
        ));
        assert!(retired.upgrade().is_none());
        assert_eq!(Arc::strong_count(&original), 1);
        let state = original.lock();
        assert_eq!(state.work.backend_read_operations, 3);
        assert_eq!(state.work.backend_write_operations, 3);
        assert_eq!(state.work.source_bytes_read, 0);
        assert!(state.work.peak_allocation_bytes >= 7 + arc_control_bytes::<Shared>() + 256);
        drop(state);
        original.with_original_leaf(|state| state.release(7))?;
        Ok(())
    }

    #[cfg(feature = "native-mount")]
    #[test]
    fn original_callback_quota_keeps_its_raw_cursor_and_does_not_restart_a_child_allowance()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let host = crate::native_host::HostRoot::open(root.path())?;
        let cancellation = CancellationToken::new();
        let original = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(DemandWork::new(
            WorkBudget {
                backend_read_operations: 128, backend_write_operations: 128,
                source_bytes_read: 1024 * 1024, bytes_copied: 1024 * 1024,
                items_examined: 4096, items_returned: 4096,
                allocation_operations: 128, peak_allocation_bytes: 1024 * 1024,
                ..WorkBudget::default()
            }, &cancellation,
        ))));
        let mut work = crate::demand::NativeSourceWork::new(&original);
        let delivery = Arc::new(Delivery::new_with_work(Arc::new(Ignored), &mut work)?);
        let watch = PlatformWatch::start_with_work(&host, delivery, &mut work)?;
        let paused = watch.paused();
        original.with_original_leaf(|state| state.budget.items_examined = state.work.items_examined);
        std::fs::write(root.path().join("reported"), b"changed")?;
        drop(paused);
        let _ = watch.fence_events_with_work(std::time::Duration::MAX, &mut work);
        assert!(!watch.delivery().is_exact());
        let cursor = {
            let reading = watch.shared.reading.lock().unwrap_or_else(PoisonError::into_inner);
            assert!(reading.filled > 0);
            assert_eq!(reading.cursor, 0);
            assert_eq!(reading.alias_cursor, 0);
            (reading.filled, reading.cursor, reading.alias_cursor)
        };
        original.with_original_leaf(|state| state.budget.items_examined += 4096);
        let prefix = original.lock().receipt();
        assert!(!watch.fence_events_with_work(std::time::Duration::MAX, &mut work)?);
        assert_eq!(original.lock().receipt(), prefix);
        {
            let reading = watch.shared.reading.lock().unwrap_or_else(PoisonError::into_inner);
            assert_eq!((reading.filled, reading.cursor, reading.alias_cursor), cursor);
        }
        watch.stop_with_work(&mut work)?;
        assert!(watch.is_stopped());
        assert_eq!(watch.logical_owned_bytes(), arc_control_bytes::<Shared>());
        drop(watch);
        assert_eq!(Arc::strong_count(&original), 1);
        Ok(())
    }

    #[cfg(feature = "native-mount")]
    #[test]
    fn original_late_callback_cancellation_retains_the_driver_until_admitted_quiescence()
    -> Result<(), Box<dyn std::error::Error>> {
        struct Cancels {
            original: std::sync::Weak<crate::demand::NativeDemandReadMeter>,
            cancellation: CancellationToken,
            invoked: AtomicBool,
        }
        impl HostChangeSink for Cancels {
            fn logical_owned_bytes(&self) -> u64 { 0 }
            fn host_changed(&self, _: &[HostChange], _: &mut DemandWork) -> Result<(), DemandError> {
                Err(DemandError::NativeWorkUnavailable)
            }
            fn host_changed_with_native_work(
                &self, _: &[HostChange], work: &mut crate::demand::NativeSourceWork<'_>,
            ) -> Result<(), DemandError> {
                let original = self.original.upgrade().expect("live original callback");
                assert!(Arc::ptr_eq(&original, work.original_arc()));
                assert!(original.0.try_lock().is_ok());
                work.charge(WorkCounters { items_returned: 1, ..WorkCounters::UNCHARGED })?;
                self.invoked.store(true, Ordering::Release);
                self.cancellation.cancel();
                work.check()
            }
        }
        let root = tempfile::tempdir()?;
        let host = crate::native_host::HostRoot::open(root.path())?;
        let cancellation = CancellationToken::new();
        let original = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(DemandWork::new(
            WorkBudget {
                backend_read_operations: 128, backend_write_operations: 128,
                source_bytes_read: 1024 * 1024, bytes_copied: 1024 * 1024,
                items_examined: 4096, items_returned: 4096,
                allocation_operations: 128, peak_allocation_bytes: 1024 * 1024,
                ..WorkBudget::default()
            }, &cancellation,
        ))));
        let sink = Arc::new(Cancels {
            original: Arc::downgrade(&original), cancellation: cancellation.clone(),
            invoked: AtomicBool::new(false),
        });
        let mut work = crate::demand::NativeSourceWork::new(&original);
        let delivery = Arc::new(Delivery::new_with_work(sink.clone(), &mut work)?);
        let watch = PlatformWatch::start_with_work(&host, delivery, &mut work)?;
        let records = retained_records(&watch);
        let paused = watch.paused();
        assert!(watch.shared.cleanup.lock().unwrap_or_else(PoisonError::into_inner).covers(WorkCounters {
            backend_read_operations: 1, backend_write_operations: 4,
            items_examined: records, ..WorkCounters::UNCHARGED
        }));
        std::fs::write(root.path().join("reported"), b"changed")?;
        drop(paused);
        let _ = watch.fence_events_with_work(std::time::Duration::MAX, &mut work);
        {
            let reading = watch.shared.reading.lock().unwrap_or_else(PoisonError::into_inner);
            assert!(reading.filled > reading.cursor);
            assert_eq!(reading.alias_cursor, 0);
        }
        assert!(sink.invoked.load(Ordering::Acquire));
        assert!(!watch.delivery().is_exact());
        assert!(!watch.is_stopped());
        assert!(matches!(watch.stop_with_work(&mut work), Err(DemandError::Cancelled)));
        for descriptor in [&watch.shared.inotify, &watch.shared.wake, &watch.shared.root] {
            assert!(descriptor.as_raw_fd() >= 0);
        }
        let prefix = original.lock().receipt();
        assert!(matches!(watch.delivery().take_failure(&mut work), Ok(()) | Err(DemandError::Cancelled)));
        assert_eq!(original.lock().receipt(), prefix);
        watch.stop_from_birth_prepaid()?;
        assert!(watch.is_stopped());
        assert_eq!(watch.logical_owned_bytes(), arc_control_bytes::<Shared>());
        assert_eq!(original.lock().receipt().items_returned, 1);
        drop(watch);
        assert_eq!(Arc::strong_count(&original), 1);
        Ok(())
    }

    #[cfg(feature = "native-mount")]
    #[test]
    fn fresh_cleanup_charges_current_io_and_refunds_only_unused_birth_credits()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let host = crate::native_host::HostRoot::open(root.path())?;
        let cancellation = CancellationToken::new();
        let birth = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(DemandWork::new(
            WorkBudget {
                backend_read_operations: 128, backend_write_operations: 128,
                source_bytes_read: 1024 * 1024, bytes_copied: 1024 * 1024,
                items_examined: 4096, allocation_operations: 128,
                peak_allocation_bytes: 1024 * 1024, ..WorkBudget::default()
            }, &cancellation,
        ))));
        let mut startup = crate::demand::NativeSourceWork::new(&birth);
        let delivery = Arc::new(Delivery::new_with_work(Arc::new(Ignored), &mut startup)?);
        let watch = PlatformWatch::start_with_work(&host, delivery, &mut startup)?;
        let records = retained_records(&watch);
        cancellation.cancel();
        let (prefix, held_budget) = birth.with_original_leaf(|work| (work.receipt(), work.budget));
        let current = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(DemandWork::new(
            stop_budget(records), &CancellationToken::new(),
        ))));
        let mut cleanup = crate::demand::NativeSourceWork::new(&current);
        watch.stop_with_work(&mut cleanup)?;
        assert!(watch.is_stopped());
        assert_eq!(current.lock().receipt(), WorkCounters {
            backend_read_operations: 1, backend_write_operations: 4,
            items_examined: records, ..WorkCounters::UNCHARGED
        });
        assert_eq!(current.lock().live_allocation_bytes(), 0);
        birth.with_original_leaf(|work| {
            assert_eq!(work.receipt(), prefix);
            assert_eq!(work.budget.backend_read_operations, held_budget.backend_read_operations + 2);
            assert_eq!(work.budget.backend_write_operations, held_budget.backend_write_operations + 5);
            assert_eq!(work.budget.items_examined, held_budget.items_examined + records);
        });
        assert_eq!(watch.logical_owned_bytes(), arc_control_bytes::<Shared>());
        Ok(())
    }

    #[cfg(feature = "native-mount")]
    #[test]
    fn original_post_pthread_cancellation_spends_only_its_prepaid_physical_cleanup()
    -> Result<(), Box<dyn std::error::Error>> {
        struct CancelsAfterThread<'a> {
            inner: crate::demand::NativeSourceWork<'a>,
            cancellation: CancellationToken,
            thread_read: u64,
        }
        impl NativeHostWork for CancelsAfterThread<'_> {
            type Error = DemandError;
            type Allocation = u64;
            type CleanupWrite = u64;
            fn io_error_kind(error: &DemandError) -> Option<io::ErrorKind> {
                <DemandWork as NativeHostWork>::io_error_kind(error)
            }
            fn allocation_failed(error: std::collections::TryReserveError) -> DemandError { DemandError::Allocation(error) }
            fn check(&self) -> Result<(), DemandError> {
                if self.inner.original_arc().with_original_leaf(|work| {
                    work.work.backend_read_operations >= self.thread_read
                }) { self.cancellation.cancel(); }
                self.inner.check()
            }
            fn admit(&mut self, delta: WorkCounters) -> Result<(), DemandError> { self.inner.admit(delta) }
            fn started_io(&mut self, delta: WorkCounters) -> Result<(), DemandError> { self.inner.started_io(delta) }
            fn record(&mut self, delta: WorkCounters) -> Result<(), DemandError> { self.inner.record(delta) }
            fn claim(&mut self, bytes: u64) -> Result<u64, DemandError> { self.inner.claim(bytes) }
            fn reserve_opaque_decode(&mut self, maximum: u64) -> Result<u64, DemandError> { self.inner.reserve_opaque_decode(maximum) }
            fn reconcile_allocation(&mut self, allocation: &mut u64, actual: u64) -> Result<(), DemandError> { self.inner.reconcile_allocation(allocation, actual) }
            fn release(&mut self, bytes: u64) -> Result<(), DemandError> { self.inner.release(bytes) }
            fn reserve_cleanup_write(&mut self) -> Result<u64, DemandError> { self.inner.reserve_cleanup_write() }
            fn cancel_cleanup_write(&mut self, reserved: u64) { self.inner.cancel_cleanup_write(reserved); }
            fn attempt_cleanup_write<T>(
                &mut self, reserved: u64, operation: impl FnOnce() -> T,
            ) -> (T, Result<(), DemandError>) { self.inner.attempt_cleanup_write(reserved, operation) }
        }
        impl SourceDemandReadWork for CancelsAfterThread<'_> {
            fn with_native_read_meter<T>(
                &mut self, operation: impl FnOnce(&dyn acyclic_native_runtime::NativeReadMeter) -> T,
            ) -> T { self.inner.with_native_read_meter(operation) }
            fn with_original_leaf<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T {
                self.inner.with_original_leaf(operation)
            }
            fn native_source_work(&self) -> Option<crate::demand::NativeSourceWork<'_>> {
                self.inner.native_source_work()
            }
            fn is_native_original(&self) -> bool { true }
            fn original_meter(&self) -> Option<&Arc<crate::demand::NativeDemandReadMeter>> {
                self.inner.original_meter()
            }
        }
        let root = tempfile::tempdir()?;
        let host = crate::native_host::HostRoot::open(root.path())?;
        let mut probe = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
        let ordinary = PlatformWatch::start_measured(
            &host, Arc::new(Delivery::new(Arc::new(Ignored))), &mut probe,
        )?;
        let thread_read = probe.work.backend_read_operations;
        ordinary.stop_measured(&mut DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()))?;
        let cancellation = CancellationToken::new();
        let original = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(DemandWork::new(
            WorkBudget {
                backend_read_operations: 128, backend_write_operations: 128,
                source_bytes_read: 1024 * 1024, bytes_copied: 1024 * 1024,
                items_examined: 4096, allocation_operations: 128,
                peak_allocation_bytes: 1024 * 1024, ..WorkBudget::default()
            }, &cancellation,
        ))));
        let mut work = CancelsAfterThread {
            inner: crate::demand::NativeSourceWork::new(&original),
            cancellation, thread_read,
        };
        let delivery = Arc::new(Delivery::new_with_work(Arc::new(Ignored), &mut work)?);
        let dropped = Arc::downgrade(&delivery);
        assert!(matches!(
            PlatformWatch::start_with_work(&host, delivery, &mut work),
            Err(DemandError::Cancelled)
        ));
        assert!(dropped.upgrade().is_none());
        assert_eq!(Arc::strong_count(&original), 1);
        original.with_original_leaf(|state| {
            assert!(state.cancellation().is_cancelled());
            assert!(state.work.backend_read_operations >= thread_read + 1);
            assert_eq!(state.work.backend_write_operations, 4);
            assert_eq!(state.budget.backend_read_operations, 128);
            assert_eq!(state.budget.backend_write_operations, 128);
            assert_eq!(state.budget.items_examined, 4096);
        });
        Ok(())
    }

    #[cfg(feature = "native-mount")]
    #[test]
    fn original_last_controller_drop_in_callback_retires_the_actual_reader()
    -> Result<(), Box<dyn std::error::Error>> {
        struct Retires {
            watch: Mutex<Option<PlatformWatch>>,
            original: std::sync::Weak<crate::demand::NativeDemandReadMeter>,
            invoked: AtomicBool,
        }
        impl HostChangeSink for Retires {
            fn logical_owned_bytes(&self) -> u64 { 0 }
            fn host_changed(&self, _: &[HostChange], _: &mut DemandWork) -> Result<(), DemandError> {
                Err(DemandError::NativeWorkUnavailable)
            }
            fn host_changed_with_native_work(
                &self, _: &[HostChange], work: &mut crate::demand::NativeSourceWork<'_>,
            ) -> Result<(), DemandError> {
                let original = self.original.upgrade().expect("the actual reader retains its original");
                assert!(Arc::ptr_eq(&original, work.original_arc()));
                assert!(original.0.try_lock().is_ok());
                let watch = self.watch.lock().unwrap_or_else(PoisonError::into_inner).take();
                drop(watch);
                self.invoked.store(true, Ordering::Release);
                Ok(())
            }
        }
        let root = tempfile::tempdir()?;
        let host = crate::native_host::HostRoot::open(root.path())?;
        let original = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(DemandWork::new(
            WorkBudget {
                backend_read_operations: 128, backend_write_operations: 128,
                source_bytes_read: 1024 * 1024, bytes_copied: 1024 * 1024,
                items_examined: 4096, items_returned: 4096,
                allocation_operations: 128, peak_allocation_bytes: 1024 * 1024,
                ..WorkBudget::default()
            }, &CancellationToken::new(),
        ))));
        let lifetime = Arc::downgrade(&original);
        let sink = Arc::new(Retires {
            watch: Mutex::new(None), original: lifetime.clone(), invoked: AtomicBool::new(false),
        });
        let mut work = crate::demand::NativeSourceWork::new(&original);
        let delivery = Arc::new(Delivery::new_with_work(sink.clone(), &mut work)?);
        let watch = PlatformWatch::start_with_work(&host, delivery, &mut work)?;
        let reader = Arc::downgrade(&watch.shared);
        *sink.watch.lock().unwrap_or_else(PoisonError::into_inner) = Some(watch);
        drop(original);
        assert!(lifetime.upgrade().is_some());
        std::fs::write(root.path().join("retire"), b"event")?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while reader.upgrade().is_some() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(sink.invoked.load(Ordering::Acquire));
        assert!(reader.upgrade().is_none(), "physical worker custody reaches its actual terminal");
        assert!(lifetime.upgrade().is_none(), "the original keeper outlives physical payload, not vice versa");
        Ok(())
    }
}
