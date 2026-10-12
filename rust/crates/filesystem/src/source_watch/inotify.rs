//! Linux: one inotify instance watching exactly the directories the source

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
    inotify: AtomicI32,
    /// Wakes the reader thread to stop.
    wake: AtomicI32,
    /// The source root, which every watched path is relative to.
    root: AtomicI32,
    /// Each ancestor of the root, by the name it holds on the way to it.
    ancestors: Mutex<Vec<(i32, std::ffi::OsString)>>,
    /// Actual variable payload, published at the existing admitted mutations.
    retained_variable_bytes: AtomicU64,
    /// Held while reading the queue and delivering what was read, so a fence returns only after everything queued before it is delivered.
    reading: Mutex<RawReads>,
    reading_changed: Condvar,
    watches: Mutex<Vec<(PathBuf, i32)>>,
    stopping: AtomicBool,
    /// Published only after actual native quiescence and full physical cleanup.
    completed: AtomicBool,
    controller_alive: AtomicBool,
    #[cfg(test)]
    polling: AtomicBool,
    cleanup: Mutex<StartupCleanup>,
    ownership: SourcePayloadOwnership,
    delivery: Arc<Delivery>,
}

/// Withholds mandatory failure cleanup from the original invocation's allowance before the corresponding native owner or retained record exists.
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

/// Explicit cleanup callers do not block before acquiring stop custody.
fn stop_custody<'a, T>(
    mutex: &'a Mutex<T>, wait: Option<&crate::CancellationToken>,
) -> Result<std::sync::MutexGuard<'a, T>, DemandError> {
    if let Some(wait) = wait {
        if wait.is_cancelled() { return Err(DemandError::Cancelled); }
        match mutex.try_lock() {
            Ok(guard) => Ok(guard),
            Err(std::sync::TryLockError::Poisoned(error)) => Ok(error.into_inner()),
            Err(std::sync::TryLockError::WouldBlock) => Err(DemandError::WorkerUnavailable),
        }
    } else {
        Ok(mutex.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

/// Linux close consumes the descriptor even on EINTR; never retry a reused fd.
fn close_measured<W: SourceDemandReadWork>(
    descriptor: i32, work: &mut W, record: impl FnOnce(&mut W) -> Result<(), DemandError>,
) -> Result<(), DemandError> {
    // SAFETY: exclusive custody was removed from its owner after quiescence.
    let status = unsafe { libc::close(descriptor) };
    let error = (status < 0).then(io::Error::last_os_error);
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
        (unsafe { libc::close(descriptor) } < 0).then(io::Error::last_os_error)
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
}

fn contains_path_with_work<W: SourceDemandReadWork>(
    watches: &[(PathBuf, i32)], path: &Path, work: &mut W,
) -> Result<bool, DemandError> {
    for (candidate, _) in watches {
        work.with_original_leaf(|work| work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() }))?;
        if candidate == path {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Reserves the real backing buffer and creates the one retained path before registration.
fn prepare_insert_with_work<W: SourceDemandReadWork>(
    watches: &mut Vec<(PathBuf, i32)>,
    path: &Path,
    work: &mut W,
    retained: &AtomicU64,
    ownership: &SourcePayloadOwnership,
) -> Result<PathBuf, DemandError> {
    if watches.len() == watches.capacity() {
        let count = watches.len().checked_add(1).ok_or(WorkError::Overflow)?;
        let bytes = (count as u64)
            .checked_mul(std::mem::size_of::<(PathBuf, i32)>() as u64)
            .ok_or(WorkError::Overflow)?;
        let old_bytes = (watches.capacity() as u64)
            .checked_mul(std::mem::size_of::<(PathBuf, i32)>() as u64)
            .ok_or(WorkError::Overflow)?;
        let copied = WorkCounters { bytes_copied: old_bytes, ..WorkCounters::default() };
        work.allocate_for(bytes, copied)?;
        let mut grown = Vec::with_capacity(count);
        grown.append(watches);
        *watches = grown;
        let grown_bytes = watches.capacity() as u64 * std::mem::size_of::<(PathBuf, i32)>() as u64;
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
            work.admit(read)?;
            work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
            // SAFETY: the stack C path and the complete writable buffer live through readlink; a full buffer is retried, never truncated.
            let length = unsafe {
                libc::readlink(proc_path.as_ptr().cast(), path.as_mut_ptr().cast(), path.len())
            };
            if length < 0 {
                return Err(io::Error::last_os_error().into());
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
            work.admit(copy)?;
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
                work.admit(copied)?;
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
            work.admit(copy)?;
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
                return Err(io::Error::last_os_error().into());
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

/// The pthread owns exactly one raw Arc reference, not an opaque Rust thread closure/name allocation.
extern "C" fn run_reader(payload: *mut libc::c_void) -> *mut libc::c_void {
    // SAFETY: startup transfers one Arc reference to this entrypoint.
    let reader = unsafe { Arc::from_raw(payload.cast::<Shared>()) };
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| reader.run())).is_err() {
        // A panicked sink must not be invoked again outside this ABI unwind boundary. Fail closed, then let the same reader retire its payload.
        reader.delivery.exact.store(false, Ordering::Release);
    }
    if reader.delivery.original.is_some() {
        reader.finish_native_reader();
    }
    std::ptr::null_mut()
}

fn owned(descriptor: i32) -> Result<OwnedFd, DemandError> {
    if descriptor < 0 {
        return Err(io::Error::last_os_error().into());
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
        Self::start_with_work(
            directory, delivery, &mut DemandWork::new(WorkBudget::UNBOUNDED, &cancellation),
        ).map_err(|error| match error {
            DemandError::Io(error) => error,
            error => io::Error::other(error),
        })
    }

    pub(super) fn start_with_work<W: SourceDemandReadWork>(
        directory: &crate::native_host::HostRoot, delivery: Arc<Delivery>, work: &mut W,
    ) -> Result<Self, DemandError> {
        let mut cleanup = StartupCleanup::reserve(work)?;
        let prepared = (|| {
            work.admit(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })?;
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
        let mut watches = Vec::new();
        let mut shared = None;
        let mut retained = 0_u64;
        let mut transferred = false;
        let control_bytes = arc_control_bytes::<Shared>();
        let mut control_transferred = false;
        let result = (|| {
            work.admit(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })?;
            work.allocate(control_bytes)?;
            retained = control_bytes;
            shared = Some(Arc::new(Shared {
                inotify: AtomicI32::new(-1),
                wake: AtomicI32::new(-1),
                root: AtomicI32::new(-1),
                ancestors: Mutex::new(Vec::new()),
                retained_variable_bytes: AtomicU64::new(0),
                reading: Mutex::new(RawReads::default()),
                reading_changed: Condvar::new(),
                watches: Mutex::new(Vec::new()),
                stopping: AtomicBool::new(false),
                completed: AtomicBool::new(false),
                controller_alive: AtomicBool::new(true),
                #[cfg(test)]
                polling: AtomicBool::new(false),
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
            watches = Vec::with_capacity(1);
            let (root_path, _) = proc_fd_path(held_root.as_raw_fd());
            work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
            // SAFETY: this stack C path names the held root and lives through registration, exactly as for every later admitted directory.
            let descriptor = unsafe {
                libc::inotify_add_watch(held_inotify.as_raw_fd(), root_path.as_ptr().cast(), WATCHED)
            };
            if descriptor < 0 {
                return Err(io::Error::last_os_error().into());
            }
            watches.push((PathBuf::new(), descriptor));
            let initialized = shared.as_ref().expect("shared initialized");
            initialized.inotify.store(
                inotify.take().expect("inotify initialized").into_raw_fd(), Ordering::Release,
            );
            initialized.wake.store(
                wake.take().expect("wake initialized").into_raw_fd(), Ordering::Release,
            );
            initialized.root.store(
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
            // No callback may mutate the startup cleanup inventory before publication or before a cancelled startup has been stopped.
            let initialized = shared.as_ref().expect("shared initialized");
            let _startup_reading = initialized.reading.lock().unwrap_or_else(PoisonError::into_inner);
            // Native thread machinery is SDK-private (zero canonical payload), but its actual allocation and native call are still admitted.
            work.charge(WorkCounters {
                backend_read_operations: 1, allocation_operations: 1,
                ..WorkCounters::default()
            })?;
            let reader = Arc::into_raw(Arc::clone(shared.as_ref().expect("shared initialized")));
            let mut thread = std::mem::MaybeUninit::<libc::pthread_t>::uninit();
            // SAFETY: pthread_create writes the thread ID on success, and the raw Arc payload is consumed either by run_reader or by failure.
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
                    // The native startup reserved every operation and record below before creation. Cancellation cannot deny cleanup.
                    let stopped = watch.stop_admitted(
                        &mut watch.thread.lock().unwrap_or_else(PoisonError::into_inner), work,
                    );
                    if let Err(stop_error) = stopped {
                        if !watch.is_stopped() {
                            if let Some(failure) = &startup_failure {
                                return Err(failure.retain_failed_platform(stop_error, watch));
                            }
                            // Only the explicit ordinary service can retry its failed cleanup with the legacy Drop allowance.
                            work.release(retained)?;
                            return Err(stop_error);
                        }
                        if !work.is_native_original() { work.release(retained)?; }
                        return Err(stop_error);
                    }
                    if !work.is_native_original() { work.release(retained)?; }
                    return Err(error);
                }
                // The owner carries the actual buffers; this invocation no longer has custody of their original allocation claims.
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
                cleaned = cleaned.and(clear_records(&mut watches, work, None, |_| 0, |work| cleanup.consume_record(work)));
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
        self.admit_with_work(directory, &mut DemandWork::new(WorkBudget::UNBOUNDED, &cancellation))
            .unwrap_or(false)
    }

    pub(super) fn admit_with_work<W: SourceDemandReadWork>(
        &self, directory: &Path, work: &mut W,
    ) -> Result<bool, DemandError> {
        self.shared.admit_with_work(directory, work)
    }

    pub(super) fn unadmit_with_work<W: SourceDemandReadWork>(
        &self, directory: &Path, work: &mut W,
    ) -> Result<(), DemandError> {
        self.shared.unadmit_with_work(directory, work)
    }

    pub(super) fn fence(&self) -> io::Result<()> {
        self.shared.read_and_deliver()
    }

    pub(super) fn fence_events_with_work<W: SourceDemandReadWork>(
        &self, timeout: std::time::Duration, work: &mut W,
    ) -> Result<bool, DemandError> {
        self.shared.read_events_and_deliver_with_work(timeout, work)
    }

    pub(super) fn delivery(&self) -> &Delivery {
        &self.shared.delivery
    }

    /// Actual SDK payload only.
    pub(super) fn logical_owned_bytes(&self) -> u64 {
        arc_control_bytes::<Shared>()
            + self.shared.retained_variable_bytes.load(Ordering::Acquire)
    }

    pub(super) fn is_stopped(&self) -> bool {
        self.shared.completed.load(Ordering::Acquire)
    }

    pub(super) fn stop_with_work<W: SourceDemandReadWork>(
        &self, work: &mut W,
    ) -> Result<(), DemandError> {
        self.stop(work, None, false)
    }

    pub(super) fn stop_with_cleanup_wait(
        &self, wait: &crate::CancellationToken,
    ) -> Result<(), DemandError> {
        // Only the original owner carries credits accepted before native birth. The caller's token controls waiting, never that owner's authority.
        let original = self.shared.delivery.original.as_ref().ok_or(DemandError::NativeWorkUnavailable)?;
        if wait.is_cancelled() { return Err(DemandError::Cancelled); }
        self.stop(&mut crate::demand::NativeSourceWork::from_original_work(original), Some(wait), true)?;
        if wait.is_cancelled() { return Err(DemandError::Cancelled); }
        Ok(())
    }

    pub(super) fn stop_accepted_cleanup(&self) -> Result<(), DemandError> {
        self.shared.delivery.ensure_stop_available()?;
        let original = self.shared.delivery.original.as_ref().ok_or(DemandError::NativeWorkUnavailable)?;
        self.stop(&mut crate::demand::NativeSourceWork::from_original_work(original), None, true)
    }

    fn stop<W: SourceDemandReadWork>(
        &self, work: &mut W, wait: Option<&crate::CancellationToken>, prepaid: bool,
    ) -> Result<(), DemandError> {
        self.shared.delivery.ensure_stop_available()?;
        if !prepaid { work.check()?; }
        let mut thread = stop_custody(&self.thread, wait)?;
        if wait.is_some_and(crate::CancellationToken::is_cancelled) {
            return Err(DemandError::Cancelled);
        }
        if self.is_stopped() {
            if prepaid {
                let cleanup = stop_custody(&self.shared.cleanup, wait)?;
                if cleanup.reads < u64::from(thread.is_some()) {
                    return Err(DemandError::NativeWorkUnavailable);
                }
                drop(cleanup);
                if wait.is_some_and(crate::CancellationToken::is_cancelled) {
                    return Err(DemandError::Cancelled);
                }
            }
            let result = self.join_thread(&mut thread, work);
            if thread.is_none() { self.shared.cancel_cleanup(work); }
            return result;
        }
        let reading = stop_custody(&self.shared.reading, wait)?;
        let watches = stop_custody(&self.shared.watches, wait)?;
        let ancestors = stop_custody(&self.shared.ancestors, wait)?;
        let records = reading.cleanup.len().checked_add(watches.len())
            .and_then(|count| count.checked_add(ancestors.len())).ok_or(WorkError::Overflow)?;
        let worker = u64::from(thread.is_some());
        let descriptors = [&self.shared.inotify, &self.shared.wake, &self.shared.root]
            .into_iter().filter(|descriptor| descriptor.load(Ordering::Acquire) >= 0).count() as u64;
        let suffix = WorkCounters {
            backend_read_operations: worker,
            backend_write_operations: descriptors
                + if prepaid { u64::from(thread.is_some() && self.shared.wake.load(Ordering::Acquire) >= 0) } else { worker },
            items_examined: u64::try_from(records).map_err(|_| WorkError::Overflow)?,
            ..WorkCounters::UNCHARGED
        };
        if prepaid {
            // Accepted cleanup spends only birth credits, even after root cancellation.
            if !stop_custody(&self.shared.cleanup, wait)?.covers(suffix) {
                return Err(DemandError::NativeWorkUnavailable);
            }
        } else if self.shared.delivery.birth_matches(work) {
            self.shared.cleanup.lock().unwrap_or_else(PoisonError::into_inner).extend_for(suffix, work)?;
        } else {
            work.preflight(suffix)?;
        }
        if !prepaid { work.check()?; }
        if wait.is_some_and(crate::CancellationToken::is_cancelled) {
            return Err(DemandError::Cancelled);
        }
        self.shared.stopping.store(true, Ordering::Release);
        self.shared.reading_changed.notify_all();
        drop(ancestors);
        drop(watches);
        drop(reading);
        // Native stop must quiesce even if the caller cancels after admission.
        self.stop_admitted(&mut thread, work)
    }

    /// Executes the already-admitted mandatory suffix against the same work carrier, without consulting cancellation after native stop has begun.
    fn stop_admitted<W: SourceDemandReadWork>(
        &self, thread: &mut Option<libc::pthread_t>, work: &mut W,
    ) -> Result<(), DemandError> {
        self.shared.stopping.store(true, Ordering::Release);
        self.shared.reading_changed.notify_all();
        let mut result = Ok(());
        if thread.is_some() {
            let reading = self.shared.reading.lock().unwrap_or_else(PoisonError::into_inner);
            let descriptor = self.shared.wake.load(Ordering::Acquire);
            let wake_error = if descriptor >= 0 {
                self.shared.ensure_cleanup_covered(WorkCounters {
                    backend_write_operations: 1, ..WorkCounters::UNCHARGED
                }, work)?;
                let one = 1_u64.to_ne_bytes();
                // SAFETY: the existing read custody lock excludes terminal descriptor retirement throughout this actual wake attempt.
                let wake = unsafe { libc::write(descriptor, one.as_ptr().cast(), one.len()) };
                let error = (wake < 0).then(io::Error::last_os_error);
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
            let joined = self.join_thread(thread, work);
            result = result.and(joined);
            if thread.is_some() { return result; }
        }
        // A native worker retires its physical state before its entrypoint returns. Startup without a worker and ordinary owners retire here.
        if self.shared.delivery.original.is_none() || !self.shared.completed.load(Ordering::Acquire) {
            result = result.and(self.shared.clear_measured(work));
            self.shared.delivery.exact.store(false, Ordering::Release);
            self.shared.completed.store(self.shared.physically_cleared(), Ordering::Release);
        }
        if self.is_stopped() { self.shared.cancel_cleanup(work); }
        result
    }

    fn join_thread<W: SourceDemandReadWork>(
        &self, thread: &mut Option<libc::pthread_t>, work: &mut W,
    ) -> Result<(), DemandError> {
        let mut result = Ok(());
        if let Some(worker) = *thread {
            self.shared.ensure_cleanup_covered(WorkCounters {
                backend_read_operations: 1, ..WorkCounters::UNCHARGED
            }, work)?;
            // SAFETY: this controller owns the one joinable worker returned by pthread_create.
            let joined = unsafe { libc::pthread_join(worker, std::ptr::null_mut()) };
            result = self.shared.record_cleanup(WorkCounters {
                backend_read_operations: 1, ..WorkCounters::UNCHARGED
            }, work);
            if joined != 0 {
                return result.and(Err(io::Error::from_raw_os_error(joined).into()));
            }
            *thread = None;
        }
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
                // The invoking reader retains Shared through callback return. It performs terminal cleanup itself rather than self-joining.
                self.shared.stopping.store(true, Ordering::Release);
                self.shared.reading_changed.notify_all();
                return;
            }
            let _ = self.stop_accepted_cleanup();
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
        let _ = self.stop_with_work(&mut DemandWork::new(WorkBudget::UNBOUNDED, &cancellation));
    }
}

impl Shared {
    fn finish_native_reader(&self) {
        self.stopping.store(true, Ordering::Release);
        let mut work = self.delivery.callback_work();
        let result = self.clear_measured(&mut work);
        if let Err(error) = result { self.delivery.failed_with_work(error, &mut work); }
        self.delivery.exact.store(false, Ordering::Release);
        if !self.physically_cleared() {
            // The exited worker is joinable, but failed admission retained its actual descriptor/record owners for a later controller attempt.
            self.reading_changed.notify_all();
            return;
        }
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
            // SAFETY: the actual reader has retired its native payload. With no controller left, detachment releases only pthread bookkeeping.
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
        self.ensure_cleanup_covered(WorkCounters {
            backend_write_operations: 1, ..WorkCounters::UNCHARGED
        }, work)?;
        // SAFETY: cleanup's controller-alive decision selected this sole terminal bookkeeping owner; no live controller can join this thread.
        let detached = unsafe { libc::pthread_detach(worker) };
        let recorded = self.record_cleanup(WorkCounters {
            backend_write_operations: 1, ..WorkCounters::UNCHARGED
        }, work);
        if detached != 0 {
            return Err(io::Error::from_raw_os_error(detached).into());
        }
        recorded
    }

    fn record_cleanup<W: SourceDemandReadWork>(&self, delta: WorkCounters, work: &mut W) -> Result<(), DemandError> {
        let mut cleanup = self.cleanup.lock().unwrap_or_else(PoisonError::into_inner);
        if self.delivery.birth_matches(work) || (self.delivery.original.is_none() && cleanup.covers(delta)) {
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

    fn ensure_cleanup_covered<W: SourceDemandReadWork>(
        &self, delta: WorkCounters, work: &W,
    ) -> Result<(), DemandError> {
        if self.delivery.birth_matches(work)
            && !self.cleanup.lock().unwrap_or_else(PoisonError::into_inner).covers(delta)
        {
            return Err(DemandError::NativeWorkUnavailable);
        }
        Ok(())
    }

    fn physically_cleared(&self) -> bool {
        self.retained_variable_bytes.load(Ordering::Acquire) == 0
            && [&self.inotify, &self.wake, &self.root]
                .into_iter().all(|descriptor| descriptor.load(Ordering::Acquire) < 0)
    }

    fn reserve_record_cleanup<W: SourceDemandReadWork>(&self, work: &mut W) -> Result<(), DemandError> {
        if let Some(original) = &self.delivery.original {
            // Every retained record belongs to this native owner, even if a different caller requested admission. Fund its mandatory drop from that owner's immutable original before record birth.
            self.cleanup.lock().unwrap_or_else(PoisonError::into_inner)
                .reserve_record(&mut crate::demand::NativeSourceWork::from_original_work(original))?;
        } else if self.delivery.birth_matches(work) {
            self.cleanup.lock().unwrap_or_else(PoisonError::into_inner).reserve_record(work)?;
        }
        Ok(())
    }

    fn cancel_record_cleanup<W: SourceDemandReadWork>(&self, work: &mut W) {
        let mut cleanup = self.cleanup.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(original) = &self.delivery.original {
            cleanup.records -= 1;
            original.meter().with_original_leaf(|work| work.refund(crate::materializer::NativeResourceCredit::Items, 1).expect("original reservation is balanced"));
        } else if self.delivery.birth_matches(work) {
            cleanup.records -= 1;
            work.with_original_leaf(|work| work.refund(crate::materializer::NativeResourceCredit::Items, 1).expect("original reservation is balanced"));
        }
    }

    /// Called only without a worker or after its successful join.
    fn clear_measured<W: SourceDemandReadWork>(&self, work: &mut W) -> Result<(), DemandError> {
        let mut reading = self.reading.lock().unwrap_or_else(PoisonError::into_inner);
        let mut watches = self.watches.lock().unwrap_or_else(PoisonError::into_inner);
        let mut ancestors = self.ancestors.lock().unwrap_or_else(PoisonError::into_inner);
        let records = reading.cleanup.len().checked_add(watches.len())
            .and_then(|count| count.checked_add(ancestors.len())).ok_or(WorkError::Overflow)?;
        self.ensure_cleanup_covered(WorkCounters {
            backend_write_operations: [&self.inotify, &self.wake, &self.root]
                .into_iter().filter(|descriptor| descriptor.load(Ordering::Acquire) >= 0).count() as u64,
            items_examined: u64::try_from(records).map_err(|_| WorkError::Overflow)?,
            ..WorkCounters::UNCHARGED
        }, work)?;
        let retired_bytes = self.retained_variable_bytes.load(Ordering::Acquire);
        let mut result = Ok(());
        for descriptor in [&self.inotify, &self.wake, &self.root] {
            let descriptor = descriptor.swap(-1, Ordering::AcqRel);
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
            &mut watches, work, Some(&self.retained_variable_bytes),
            |(path, _)| path.capacity() as u64,
            |work| self.record_cleanup(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }, work),
        ));
        result = result.and(clear_records(
            &mut reading.cleanup, work, Some(&self.retained_variable_bytes),
            |path| path.capacity() as u64,
            |work| self.record_cleanup(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }, work),
        ));


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

    fn add_watch_at_with_work<W: SourceDemandReadWork>(
        &self, directory: i32, relative: &Path, work: &mut W,
    ) -> Result<i32, DemandError> {
        if self.stopping.load(Ordering::Acquire) { return Err(DemandError::Cancelled); }
        // Render the held descriptor on the stack: only the final C path is owned, and it is admitted before any bytes are copied.
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
            // SAFETY: a NUL-terminated path that outlives the call, through the held root rather than a newly resolved root name.
            let descriptor = unsafe {
                libc::inotify_add_watch(self.inotify.load(Ordering::Acquire), path.as_ptr(), WATCHED)
            };
            if descriptor < 0 {
                Err(io::Error::last_os_error().into())
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
        let mut watches = self.watches.lock().unwrap_or_else(PoisonError::into_inner);
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
                    let parent = held.as_ref().map_or_else(|| self.root.load(Ordering::Acquire), |(directory, _)| directory.as_raw_fd());
                    let opened = match Self::open_component_with_work(parent, name, work) {
                        Ok(directory) => directory,
                        Err(DemandError::Io(error))
                            if matches!(error.raw_os_error(), Some(libc::ENOENT | libc::ENOTDIR)) => return Ok(false),
                        Err(error) => return Err(error),
                    };
                    if let Some(previous) = held.replace(opened) { close_component(previous, work)?; }
                    added = false;
                    if contains_path_with_work(&watches, &prefix, work)? {
                        continue;
                    }
                    self.reserve_record_cleanup(work)?;
                    let owned = match prepare_insert_with_work(
                        &mut watches, &prefix, work, &self.retained_variable_bytes, &self.ownership,
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
                            watches.push((owned, descriptor));

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

    fn unadmit_with_work<W: SourceDemandReadWork>(
        &self, directory: &Path, work: &mut W,
    ) -> Result<(), DemandError> {
        work.check()?;
        let mut watches = self.watches.lock().unwrap_or_else(PoisonError::into_inner);
        if self.stopping.load(Ordering::Acquire) { return Ok(()); }
        let mut at = 0;
        while at < watches.len() {
            work.with_original_leaf(|work| work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() }))?;
            let (path, descriptor) = &watches[at];
            if path.as_os_str().is_empty() || !path.starts_with(directory) {
                at += 1;
                continue;
            }
            let descriptor = *descriptor;
            let mut shared = false;
            for (index, (_, other)) in watches.iter().enumerate() {
                work.with_original_leaf(|work| work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() }))?;
                if index != at && *other == descriptor {
                    shared = true;
                    break;
                }
            }
            let copied = WorkCounters {
                bytes_copied: ((watches.len() - at - 1) as u64)
                    .checked_mul(size_of::<(PathBuf, i32)>() as u64).ok_or(WorkError::Overflow)?,
                ..WorkCounters::default()
            };
            work.with_original_leaf(|work| work.work.admit(&copied, &work.budget))?;
            if !shared {
                work.with_original_leaf(|work| work.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::default() }))?;
                // SAFETY: removes only this instance's admitted descriptor.
                if unsafe { libc::inotify_rm_watch(self.inotify.load(Ordering::Acquire), descriptor) } < 0 {
                    let error = io::Error::last_os_error();
                    if error.raw_os_error() != Some(libc::EINVAL) { return Err(error.into()); }
                }
            }
            work.record(copied)?;
            let (removed, _) = watches.remove(at);
            let bytes = removed.capacity() as u64;

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
            // SAFETY: one normal component beneath a held directory, never following a symlink or re-resolving a previously opened prefix.
            let directory = unsafe { libc::openat(parent, name.as_ptr(),
                libc::O_PATH | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC) };
            if directory < 0 {
                Err(io::Error::last_os_error().into())
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
                    // Even a late accounting failure must not defer this admitted temporary descriptor close to OwnedFd::drop.
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
                fd: self.inotify.load(Ordering::Acquire),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: self.wake.load(Ordering::Acquire),
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
            #[cfg(test)]
            self.polling.store(true, Ordering::Release);
            // SAFETY: two initialized pollfd records in a live array.
            let ready = unsafe { libc::poll(descriptors.as_mut_ptr(), 2, -1) };
            #[cfg(test)]
            self.polling.store(false, Ordering::Release);
            if ready < 0 {
                let error = io::Error::last_os_error();
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

    /// Reads everything queued and delivers it, holding the read lock throughout.
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

    /// Drains the same native page through the known SDK event sink.
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

                    drop(path);
                    self.cancel_record_cleanup(work);
                    self.ownership.retire(bytes)?;
                    self.retained_variable_bytes.fetch_sub(bytes, Ordering::AcqRel);
                }
                if reading.cursor == reading.filled {
                    let maximum = (work.remaining()?.source_bytes_read.min(reading.buffer.len() as u64)) as usize;
                    if maximum < EVENT_HEADER_BYTES {
                        work.admit(WorkCounters {
                            source_bytes_read: EVENT_HEADER_BYTES as u64, ..WorkCounters::default()
                        })?;
                    }
                    work.admit(WorkCounters {
                        source_bytes_read: maximum as u64, ..WorkCounters::default()
                    })?;
                    work.charge(WorkCounters {
                        backend_read_operations: 1, ..WorkCounters::default()
                    })?;
                    // SAFETY: no competing reader can use this owned page.
                    let read = unsafe {
                        libc::read(self.inotify.load(Ordering::Acquire), reading.buffer.as_mut_ptr().cast(), maximum)
                    };
                    if read < 0 {
                        let error = io::Error::last_os_error();
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
                    let watches = self.watches.lock().unwrap_or_else(PoisonError::into_inner);
                    if reading.alias_cursor >= watches.len() { break; }
                    work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                    let (alias, watched) = &watches[reading.alias_cursor];
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
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            self.delivery.event_with_work(&path, mask, cookie, work)
                        }))
                    } else {
                        let change = if mask & (REBINDING | libc::IN_DELETE_SELF | libc::IN_MOVE_SELF) != 0 {
                            HostChange::Rebound(path)
                        } else {
                            HostChange::Altered(path)
                        };
                        let answer = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            self.delivery.deliver_with_work(std::slice::from_ref(&change), work)
                        }));
                        path = match change {
                            HostChange::Rebound(path) | HostChange::Altered(path) => path,
                            HostChange::Everything => unreachable!("constructed path change"),
                        };
                        answer
                    };
                    let delivered = match delivered {
                        Ok(result) => result,
                        Err(panic) => {
                            drop(path);
                            if cleanup_needed { self.cancel_record_cleanup(work); }
                            if let Err(error) = work.release(bytes as u64) {
                                self.delivery.failed_with_work(error, work);
                            }
                            std::panic::resume_unwind(panic);
                        }
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
                    let mut watches = self.watches.lock().unwrap_or_else(PoisonError::into_inner);
                    let mut at = 0;
                    while at < watches.len() {
                        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                        if watches[at].1 == descriptor {
                            let shifted = (watches.len() - at - 1)
                                .checked_mul(std::mem::size_of::<(PathBuf, i32)>()).ok_or(WorkError::Overflow)?;
                            work.charge(WorkCounters { bytes_copied: shifted as u64, ..WorkCounters::default() })?;
                            let (path, _) = watches.remove(at);
                            let bytes = path.capacity() as u64;

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

    #[cfg(feature = "native-mount")]
    const ORIGINAL_BUDGET: WorkBudget = WorkBudget {
        backend_read_operations: 128, backend_write_operations: 128,
        source_bytes_read: 1024 * 1024, bytes_copied: 1024 * 1024,
        items_examined: 4096, items_returned: 4096,
        allocation_operations: 128, peak_allocation_bytes: 1024 * 1024,
        ..WorkBudget::UNCHARGED
    };

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
            &crate::native_host::HostRoot::open(root, &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?,
            Arc::new(Delivery::new(Arc::new(Ignored))),
        )
    }

    #[test]
    fn stop_closes_descriptors_without_closing_reused_numbers()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir_all(root.path().join("a/b"))?;
        let host = crate::native_host::HostRoot::open(root.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let watch = watcher(root.path())?;
        assert!(watch.admit(Path::new("a/b")));
        let descriptors = [watch.shared.inotify.load(Ordering::Acquire), watch.shared.wake.load(Ordering::Acquire),
            watch.shared.root.load(Ordering::Acquire)];
        let mut work = DemandWork::new(WorkBudget {
            backend_read_operations: 128, backend_write_operations: 128,
            items_examined: 4096, ..WorkBudget::default()
        }, &crate::CancellationToken::new());
        watch.stop_with_work(&mut work)?;
        assert!(watch.is_stopped());
        assert!(!watch.delivery().is_exact());
        for descriptor in descriptors {
            // SAFETY: querying a closed descriptor does not reuse it.
            assert_eq!(unsafe { libc::fcntl(descriptor, libc::F_GETFD) }, -1);
            assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::EBADF));
        }
        watch.stop_with_work(&mut work)?;
        let mut replacements = Vec::new();
        for _ in descriptors {
            // SAFETY: the root is held and each duplicate gets one owner.
            replacements.push(owned(unsafe {
                libc::fcntl(host.directory_fd(), libc::F_DUPFD_CLOEXEC, 0)
            })?);
        }
        drop(watch);
        for replacement in replacements {
            // SAFETY: this fixture owns the duplicate.
            assert!(unsafe { libc::fcntl(replacement.as_raw_fd(), libc::F_GETFD) } >= 0);
        }
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
        assert!(matches!(watch.admit_with_work(Path::new("a/b"), &mut work),
            Err(DemandError::Work(WorkError::BudgetExceeded { .. }))));

        assert!(watch.shared.watches.lock().unwrap_or_else(PoisonError::into_inner).iter().any(|(path, _)| path == Path::new("a")));
        assert!(!watch.shared.watches.lock().unwrap_or_else(PoisonError::into_inner).iter().any(|(path, _)| path == Path::new("a/b")));
        assert!(watch.delivery().is_exact());

        let mut work = DemandWork::new(WorkBudget {
            backend_read_operations: 3,
            ..WorkBudget::UNBOUNDED
        }, &cancellation);
        assert!(watch.admit_with_work(Path::new("a/b"), &mut work)?);

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
        assert!(!watch.admit_with_work(Path::new("a/child"), &mut work)?);

        assert!(!watch.shared.watches.lock().unwrap_or_else(PoisonError::into_inner).iter().any(|(path, _)| path == Path::new("a")));
        assert!(!watch.shared.watches.lock().unwrap_or_else(PoisonError::into_inner).iter().any(|(path, _)| path == Path::new("a/child")));
        Ok(())
    }

    #[test]
    fn removal_admits_the_native_write_before_forgetting_the_live_registration()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("a"))?;
        let watch = watcher(root.path())?;
        watch.admit_with_work(Path::new("a"),
            &mut DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()))?;
        let mut denied = DemandWork::new(WorkBudget {
            backend_write_operations: 0, peak_allocation_bytes: 0, ..WorkBudget::UNBOUNDED
        }, &CancellationToken::new());
        assert!(matches!(watch.unadmit_with_work(Path::new("a"), &mut denied),
            Err(DemandError::Work(_))));

        assert!(watch.shared.watches.lock().unwrap_or_else(PoisonError::into_inner).iter().any(|(path, _)| path == Path::new("a")));
        let mut allowed = DemandWork::new(WorkBudget {
            backend_write_operations: 1, peak_allocation_bytes: 0, ..WorkBudget::UNBOUNDED
        }, &CancellationToken::new());
        watch.unadmit_with_work(Path::new("a"), &mut allowed)?;

        assert!(!watch.shared.watches.lock().unwrap_or_else(PoisonError::into_inner).iter().any(|(path, _)| path == Path::new("a")));
        Ok(())
    }

    #[cfg(feature = "native-mount")]
    #[test]
    fn original_cleanup_wait_precancel_and_contention_preserve_custody_for_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let host = crate::native_host::HostRoot::open(root.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let cancellation = CancellationToken::new();
        let original = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(DemandWork::new(
            WorkBudget { items_returned: 0, ..ORIGINAL_BUDGET }, &cancellation,
        ))));
        let mut work = crate::demand::NativeSourceWork::new(&original);
        let delivery = Arc::new(Delivery::new_with_work(Arc::new(Ignored), &mut work)?);
        let watch = PlatformWatch::start_with_work(&host, delivery, &mut work)?;

        let paused = watch.paused();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !watch.shared.polling.load(Ordering::Acquire)
            && std::time::Instant::now() < deadline
        {
            std::thread::yield_now();
        }
        assert!(watch.shared.polling.load(Ordering::Acquire));
        cancellation.cancel();
        let prefix = original.lock().receipt();

        let wait = CancellationToken::new();
        wait.cancel();
        assert!(matches!(watch.stop_with_cleanup_wait(&wait), Err(DemandError::Cancelled)));
        assert!(matches!(watch.stop_with_cleanup_wait(&CancellationToken::new()),
            Err(DemandError::WorkerUnavailable)));
        assert_eq!(original.lock().receipt(), prefix);
        assert!(!watch.shared.stopping.load(Ordering::Acquire));
        assert!(!watch.is_stopped());
        assert!(watch.thread.lock().unwrap_or_else(PoisonError::into_inner).is_some());

        for descriptor in [&watch.shared.inotify, &watch.shared.wake, &watch.shared.root] {
            assert!(descriptor.load(Ordering::Acquire) >= 0);
        }
        drop(paused);
        // Withhold the already-owned join cursor, then restore that exact cursor. Neither denial nor retry creates additional native credit.
        let reads = {
            let mut cleanup = watch.shared.cleanup.lock().unwrap_or_else(PoisonError::into_inner);
            std::mem::take(&mut cleanup.reads)
        };
        let denied = watch.stop_with_cleanup_wait(&CancellationToken::new());
        watch.shared.cleanup.lock().unwrap_or_else(PoisonError::into_inner).reads = reads;
        assert!(matches!(denied, Err(DemandError::NativeWorkUnavailable)));
        assert_eq!(original.lock().receipt(), prefix);
        assert!(!watch.shared.stopping.load(Ordering::Acquire));
        assert!(!watch.is_stopped());

        let wait = CancellationToken::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match watch.stop_with_cleanup_wait(&wait) {
                Err(DemandError::WorkerUnavailable) if std::time::Instant::now() < deadline => {
                    std::thread::yield_now();
                }
                result => { result?; break; }
            }
        }
        assert!(watch.is_stopped());
        assert!(watch.shared.physically_cleared());
        assert!(watch.thread.lock().unwrap_or_else(PoisonError::into_inner).is_none());
        let terminal = original.lock().receipt();

        watch.stop_with_cleanup_wait(&wait)?;
        assert_eq!(original.lock().receipt(), terminal);
        drop(watch);

        Ok(())
    }

    #[cfg(feature = "native-mount")]
    #[test]
    fn original_cleanup_wait_late_cancel_waits_for_the_actual_native_terminal()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let host = crate::native_host::HostRoot::open(root.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let original = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(DemandWork::new(
            WorkBudget { items_returned: 0, ..ORIGINAL_BUDGET }, &CancellationToken::new(),
        ))));
        let mut work = crate::demand::NativeSourceWork::new(&original);
        let delivery = Arc::new(Delivery::new_with_work(Arc::new(Ignored), &mut work)?);
        let watch = Arc::new(PlatformWatch::start_with_work(&host, delivery, &mut work)?);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !watch.shared.polling.load(Ordering::Acquire)
            && std::time::Instant::now() < deadline
        {
            std::thread::yield_now();
        }
        assert!(watch.shared.polling.load(Ordering::Acquire));
        let wait = CancellationToken::new();
        // Holding only the test's accounting leaf delays recording a genuine wake/retirement, without inventing a mock native completion.
        let mut state = original.lock();

        let stopping = Arc::clone(&watch);
        let caller_wait = wait.clone();
        let (finished, received) = std::sync::mpsc::channel();
        let stopper = std::thread::spawn(move || {
            let result = stopping.stop_with_cleanup_wait(&caller_wait);
            finished.send(result).expect("live test controller");
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !watch.shared.stopping.load(Ordering::Acquire)
            && std::time::Instant::now() < deadline
        {
            std::thread::yield_now();
        }
        let started = watch.shared.stopping.load(Ordering::Acquire);
        wait.cancel();
        let premature = received.try_recv();
        let terminal_before_release = watch.is_stopped();
        drop(state);
        let result = match premature {
            Ok(result) => result,
            Err(std::sync::mpsc::TryRecvError::Empty) => received.recv()?,
            Err(error) => return Err(error.into()),
        };
        stopper.join().expect("actual native stop controller");
        assert!(started);
        assert!(!terminal_before_release);
        assert!(matches!(result, Err(DemandError::Cancelled)));
        assert!(watch.is_stopped());
        assert!(watch.shared.physically_cleared());
        assert!(watch.thread.lock().unwrap_or_else(PoisonError::into_inner).is_none());

        Ok(())
    }

    #[cfg(feature = "native-mount")]
    #[test]
    fn original_cleanup_wait_retains_inflight_callback_and_retires_after_its_panic()
    -> Result<(), Box<dyn std::error::Error>> {
        struct PanicsAfterRelease {
            entered: std::sync::mpsc::Sender<()>,
            release: Mutex<bool>,
            changed: Condvar,
        }
        impl HostChangeSink for PanicsAfterRelease {
            fn logical_owned_bytes(&self) -> u64 { 0 }
            fn host_changed(&self, _: &[HostChange], _: &mut DemandWork) -> Result<(), DemandError> {
                Err(DemandError::NativeWorkUnavailable)
            }
            fn host_changed_with_native_work(
                &self, _: &[HostChange], work: &mut crate::demand::NativeSourceWork<'_>,
            ) -> Result<(), DemandError> {
                work.charge(WorkCounters { items_returned: 1, ..WorkCounters::UNCHARGED })?;
                self.entered.send(()).expect("live callback controller");
                let mut release = self.release.lock().unwrap_or_else(PoisonError::into_inner);
                while !*release {
                    release = self.changed.wait(release).unwrap_or_else(PoisonError::into_inner);
                }
                panic!("controlled native callback unwind");
            }
        }
        let root = tempfile::tempdir()?;
        let host = crate::native_host::HostRoot::open(root.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let cancellation = CancellationToken::new();
        let original = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(DemandWork::new(
            ORIGINAL_BUDGET, &cancellation,
        ))));
        let (entered, receive) = std::sync::mpsc::channel();
        let sink = Arc::new(PanicsAfterRelease {
            entered, release: Mutex::new(false), changed: Condvar::new(),
        });
        let mut work = crate::demand::NativeSourceWork::new(&original);
        let delivery = Arc::new(Delivery::new_with_work(sink.clone(), &mut work)?);
        let watch = PlatformWatch::start_with_work(&host, delivery, &mut work)?;
        let retained_control = original.lock().live_allocation_bytes()
            - watch.shared.retained_variable_bytes.load(Ordering::Acquire);
        std::fs::write(root.path().join("reported"), b"changed")?;
        receive.recv_timeout(std::time::Duration::from_secs(5))?;
        cancellation.cancel();

        let denied = watch.stop_with_cleanup_wait(&CancellationToken::new());
        let pending_terminal = watch.is_stopped();
        *sink.release.lock().unwrap_or_else(PoisonError::into_inner) = true;
        sink.changed.notify_all();
        assert!(matches!(denied, Err(DemandError::WorkerUnavailable)));
        assert!(!pending_terminal);

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !watch.is_stopped() && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(watch.is_stopped(), "callback unwind is not itself terminal evidence");
        watch.stop_with_cleanup_wait(&CancellationToken::new())?;
        assert!(watch.shared.physically_cleared());
        assert!(watch.thread.lock().unwrap_or_else(PoisonError::into_inner).is_none());
        assert!(!watch.delivery().is_exact());
        let mut state = original.lock();
        assert_eq!(state.receipt().items_returned, 1, "the panicked sink is never re-entered");
        assert_eq!(state.live_allocation_bytes(), retained_control);

        drop(state);
        drop(watch);

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
        let host = crate::native_host::HostRoot::open(root.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let cancellation = CancellationToken::new();
        let original = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(DemandWork::new(
            ORIGINAL_BUDGET, &cancellation,
        ))));
        let sink = Arc::new(Cancels {
            original: Arc::downgrade(&original), cancellation: cancellation.clone(),
            invoked: AtomicBool::new(false),
        });
        let mut work = crate::demand::NativeSourceWork::new(&original);
        let delivery = Arc::new(Delivery::new_with_work(sink.clone(), &mut work)?);
        let watch = PlatformWatch::start_with_work(&host, delivery, &mut work)?;

        let paused = watch.paused();
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
            assert!(descriptor.load(Ordering::Acquire) >= 0);
        }
        let prefix = original.lock().receipt();
        assert!(matches!(watch.delivery().take_failure(&mut work), Ok(()) | Err(DemandError::Cancelled)));
        assert_eq!(original.lock().receipt(), prefix);
        let wait = CancellationToken::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match watch.stop_with_cleanup_wait(&wait) {
                Err(DemandError::WorkerUnavailable) if std::time::Instant::now() < deadline => {
                    std::thread::yield_now();
                }
                result => { result?; break; }
            }
        }
        assert!(watch.is_stopped());

        assert_eq!(original.lock().receipt().items_returned, 1);
        drop(watch);

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
        let host = crate::native_host::HostRoot::open(root.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let original = Arc::new(crate::demand::NativeDemandReadMeter(Mutex::new(DemandWork::new(
            ORIGINAL_BUDGET, &CancellationToken::new(),
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
