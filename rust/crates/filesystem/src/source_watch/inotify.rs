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

#![allow(unsafe_code, reason = "watches through the Linux inotify API")]

use super::{Delivery, HostChange};
use std::ffi::{CString, OsStr};
use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use crate::demand::{DemandError, DemandWork};
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
    thread: Option<libc::pthread_t>,
}

struct Shared {
    inotify: OwnedFd,
    /// Wakes the reader thread to stop.
    wake: OwnedFd,
    /// The source root, which every watched path is relative to.
    root: OwnedFd,
    delivery: Arc<Delivery>,
    /// Each ancestor of the root, by the name it holds on the way to it.
    ancestors: Vec<(i32, std::ffi::OsString)>,
    /// Held while reading the queue and delivering what was read, so a
    /// fence returns only after everything queued before it is delivered.
    reading: Mutex<RawReads>,
    reading_changed: Condvar,
    watches: Mutex<Watches>,
    stopping: AtomicBool,
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

/// One deterministic owned buffer of path/descriptor records. Demand admission
/// can bound every comparison and every buffer growth without an opaque map
/// allocator or a second alias index.
#[derive(Default)]
struct Watches {
    entries: Vec<(PathBuf, i32)>,
}

impl Watches {
    #[cfg(test)]
    fn contains_path(&self, path: &Path) -> bool {
        self.entries.iter().any(|(candidate, _)| candidate == path)
    }

    fn contains_path_measured(&self, path: &Path, work: &mut DemandWork) -> Result<bool, DemandError> {
        for (candidate, _) in &self.entries {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
            if candidate == path {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn insert(&mut self, descriptor: i32, path: PathBuf) {
        if let Some((_, prior)) = self.entries.iter_mut().find(|(candidate, _)| *candidate == path) {
            *prior = descriptor;
        } else {
            if self.entries.len() == self.entries.capacity() {
                self.entries.reserve_exact(1);
            }
            self.entries.push((path, descriptor));
        }
    }

    fn owned_bytes_measured(&self, work: &mut DemandWork) -> Result<u64, DemandError> {
        let mut bytes = (self.entries.capacity() as u64)
            .checked_mul(std::mem::size_of::<(PathBuf, i32)>() as u64)
            .ok_or(WorkError::Overflow)?;
        for (path, _) in &self.entries {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
            bytes = bytes.checked_add(path.capacity() as u64).ok_or(WorkError::Overflow)?;
        }
        Ok(bytes)
    }

    /// Reserves the real backing buffer and creates the one retained path
    /// before registration. Once the kernel succeeds, insertion is infallible.
    fn prepare_insert_measured(
        &mut self,
        path: &Path,
        work: &mut DemandWork,
        owned_bytes: &mut u64,
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
            work.work.admit(&copied, &work.budget)?;
            work.allocate(bytes)?;
            work.record(copied)?;
            let mut grown = Vec::with_capacity(count);
            grown.append(&mut self.entries);
            self.entries = grown;
            work.release(old_bytes)?;
            *owned_bytes = owned_bytes.checked_sub(old_bytes)
                .and_then(|prior| prior.checked_add(bytes)).ok_or(WorkError::Overflow)?;
        }
        let bytes = path.as_os_str().len() as u64;
        let copied = WorkCounters { bytes_copied: bytes, ..WorkCounters::default() };
        work.work.admit(&copied, &work.budget)?;
        work.allocate(bytes)?;
        work.record(copied)?;
        Ok(path.to_path_buf())
    }

    /// Forgets every matching path, removing a kernel descriptor only once
    /// its last alias has disappeared.
    fn forget_beneath(&mut self, path: &Path) -> Vec<i32> {
        let mut forgotten = Vec::new();
        self.entries.retain(|(candidate, descriptor)| {
            if candidate.starts_with(path) {
                if !forgotten.contains(descriptor) {
                    forgotten.push(*descriptor);
                }
                false
            } else {
                true
            }
        });
        forgotten.retain(|descriptor| !self.entries.iter().any(|(_, kept)| kept == descriptor));
        forgotten
    }

    fn drop_descriptor(&mut self, descriptor: i32) {
        self.entries.retain(|(_, kept)| *kept != descriptor);
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
fn watch_ancestors(
    inotify: &OwnedFd,
    root: &OwnedFd,
    ancestors: &mut Vec<(i32, std::ffi::OsString)>,
    retained: &mut u64,
    work: &mut DemandWork,
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
            work.release(old_bytes)?;
            temporary -= old_bytes;
            path = grown;
            let read = WorkCounters { source_bytes_read: bytes, ..WorkCounters::default() };
            work.work.admit(&read, &work.budget)?;
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
            let name_bytes = name.len() as u64;
            let copy = WorkCounters { bytes_copied: name_bytes, ..WorkCounters::default() };
            work.work.admit(&copy, &work.budget)?;
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
                work.work.admit(&copied, &work.budget)?;
                let next_retained = retained.checked_add(bytes).ok_or(WorkError::Overflow)?;
                work.allocate(bytes)?;
                *retained = next_retained;
                work.record(copied)?;
                let mut grown = Vec::with_capacity(count);
                grown.append(ancestors);
                work.release(old_bytes)?;
                *retained -= old_bytes;
                *ancestors = grown;
            }
            let parent_bytes = parent.as_os_str().len();
            let length = parent_bytes.checked_add(1).ok_or(WorkError::Overflow)?;
            let bytes = length as u64;
            let copy = WorkCounters { bytes_copied: parent_bytes as u64, ..WorkCounters::default() };
            work.work.admit(&copy, &work.budget)?;
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
            work.release(bytes)?;
            temporary -= bytes;
            c_path = Vec::new();
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
                work.release(old_bytes)?;
                *retained -= old_bytes;
                ancestors[index].1 = std::mem::take(&mut pending_name);
            } else {
                ancestors.push((descriptor, std::mem::take(&mut pending_name)));
            }
            temporary -= name_bytes;
            *retained = retained.checked_add(name_bytes).ok_or(WorkError::Overflow)?;
            child = parent;
        }
        Ok(())
    })();
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
        work.check()?;
        work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
        // SAFETY: plain descriptor creation with valid flags.
        let inotify = owned(unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) })?;
        work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
        // SAFETY: as above.
        let wake = owned(unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) })?;
        work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
        // SAFETY: duplicating a live descriptor the root holds.
        let root = owned(unsafe { libc::fcntl(directory.directory_fd(), libc::F_DUPFD_CLOEXEC, 0) })?;
        let mut ancestors = Vec::new();
        let mut buffer = Vec::new();
        let mut watches = Watches::default();
        let mut shared = None;
        let mut retained = 0_u64;
        let result = (|| {
            watch_ancestors(&inotify, &root, &mut ancestors, &mut retained, work)?;
            let bytes = READ_BUFFER_BYTES as u64;
            let next_retained = retained.checked_add(bytes).ok_or(WorkError::Overflow)?;
            work.allocate(bytes)?;
            retained = next_retained;
            buffer = vec![0; READ_BUFFER_BYTES];
            let bytes = std::mem::size_of::<(PathBuf, i32)>() as u64;
            let next_retained = retained.checked_add(bytes).ok_or(WorkError::Overflow)?;
            work.allocate(bytes)?;
            retained = next_retained;
            watches.entries = Vec::with_capacity(1);
            let (root_path, _) = proc_fd_path(root.as_raw_fd());
            work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
            // SAFETY: this stack C path names the held root and lives through
            // registration, exactly as for every later admitted directory.
            let descriptor = unsafe {
                libc::inotify_add_watch(inotify.as_raw_fd(), root_path.as_ptr().cast(), WATCHED)
            };
            if descriptor < 0 {
                return Err(last_error().into());
            }
            watches.entries.push((PathBuf::new(), descriptor));
            let bytes = std::mem::size_of::<Shared>() as u64;
            let next_retained = retained.checked_add(bytes).ok_or(WorkError::Overflow)?;
            work.allocate(bytes)?;
            retained = next_retained;
            shared = Some(Arc::new(Shared {
                inotify,
                wake,
                root,
                delivery,
                ancestors: std::mem::take(&mut ancestors),
                reading: Mutex::new(RawReads {
                    buffer: std::mem::take(&mut buffer), ..RawReads::default()
                }),
                reading_changed: Condvar::new(),
                watches: Mutex::new(std::mem::take(&mut watches)),
                stopping: AtomicBool::new(false),
            }));
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
            Ok(unsafe { thread.assume_init() })
        })();
        match result {
            Ok(thread) => {
                let watch = Self {
                    shared: shared.expect("successful startup initialized shared"),
                    thread: Some(thread),
                };
                if let Err(error) = work.check() {
                    // A cancellation concurrent with pthread_create must not
                    // leave a live subscription or its admitted buffers.
                    work.release(retained)?;
                    drop(watch);
                    return Err(error);
                }
                Ok(watch)
            }
            Err(error) => {
                // Release claims before owned temporary/shared buffers drop.
                work.release(retained)?;
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
        self.shared.admit_measured(directory, work)
    }

    pub(super) fn unadmit_measured(
        &self, directory: &Path, work: &mut DemandWork,
    ) -> Result<(), DemandError> {
        self.shared.unadmit_measured(directory, work)
    }

    pub(super) fn fence(&self) -> io::Result<()> {
        self.shared.read_and_deliver()
    }

    pub(super) fn fence_events_measured(
        &self, timeout: std::time::Duration, work: &mut DemandWork,
    ) -> Result<bool, DemandError> {
        self.shared.read_events_and_deliver_measured(timeout, work)
    }

    pub(super) fn delivery(&self) -> &Delivery {
        &self.shared.delivery
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
        self.shared.stopping.store(true, Ordering::Release);
        self.shared.reading_changed.notify_all();
        let one = 1_u64.to_ne_bytes();
        // SAFETY: writes eight bytes from a live buffer to our eventfd.
        let _ =
            unsafe { libc::write(self.shared.wake.as_raw_fd(), one.as_ptr().cast(), one.len()) };
        if let Some(thread) = self.thread.take() {
            // SAFETY: this is the joinable worker ID returned by startup; it
            // is joined exactly once after waking the worker.
            let _ = unsafe { libc::pthread_join(thread, std::ptr::null_mut()) };
        }
    }
}

impl Shared {
    fn lock_watches(&self) -> std::sync::MutexGuard<'_, Watches> {
        self.watches.lock().unwrap_or_else(PoisonError::into_inner)
    }


    #[cfg(test)]
    fn add_watch_measured(&self, relative: &Path, work: &mut DemandWork) -> Result<i32, DemandError> {
        self.add_watch_at_measured(self.root.as_raw_fd(), relative, work)
    }

    fn add_watch_at_measured(&self, directory: i32, relative: &Path, work: &mut DemandWork) -> Result<i32, DemandError> {
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
        let result = (|| {
            let path = CString::new(path)
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
            work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
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

    fn admit_measured(&self, directory: &Path, work: &mut DemandWork) -> Result<bool, DemandError> {
        work.check()?;
        if !self.delivery.is_exact() || directory.as_os_str().is_empty() {
            return Ok(false);
        }
        let mut watches = self.lock_watches();
        let retained = watches.owned_bytes_measured(work)?;
        work.adopt(retained)?;
        let mut retained_bytes = retained;
        let bytes = directory.as_os_str().len() as u64;
        let mut abandoned = false;
        let result = (|| {
            work.allocate(bytes)?;
            let mut prefix = PathBuf::with_capacity(directory.as_os_str().len());
            let mut held: Option<OwnedFd> = None;
            let result = (|| {
                let mut added = false;
                for component in directory.components() {
                    let std::path::Component::Normal(name) = component else {
                        return Err(DemandError::InvalidRequest);
                    };
                    work.charge(WorkCounters {
                        items_examined: 1,
                        bytes_copied: (component.as_os_str().len() as u64)
                            .checked_add(u64::from(!prefix.as_os_str().is_empty()))
                            .ok_or(WorkError::Overflow)?,
                        ..WorkCounters::default()
                    })?;
                    prefix.push(component);
                    let parent = held.as_ref().map_or_else(|| self.root.as_raw_fd(), |directory| directory.as_raw_fd());
                    held = match Self::open_component_measured(parent, name, work) {
                        Ok(directory) => Some(directory),
                        Err(DemandError::Io(error))
                            if matches!(error.raw_os_error(), Some(libc::ENOENT | libc::ENOTDIR)) => return Ok(false),
                        Err(error) => return Err(error),
                    };
                    added = false;
                    if watches.contains_path_measured(&prefix, work)? {
                        continue;
                    }
                    let owned = watches.prepare_insert_measured(&prefix, work, &mut retained_bytes)?;
                    let path_bytes = owned.as_os_str().len() as u64;
                    match self.add_watch_at_measured(
                        held.as_ref().ok_or(DemandError::InvalidRequest)?.as_raw_fd(), Path::new("."), work,
                    ) {
                        Ok(descriptor) => {
                            watches.entries.push((owned, descriptor));
                            retained_bytes = retained_bytes.checked_add(path_bytes)
                                .ok_or(WorkError::Overflow)?;
                            added = true;
                        }
                        Err(error) => {
                            drop(owned);
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
            drop(prefix);
            work.release(bytes)?;
            result
        })();
        drop(watches);
        work.release(retained_bytes)?;
        if abandoned {
            self.delivery.abandon();
        }
        result
    }

    fn unadmit_measured(&self, directory: &Path, work: &mut DemandWork) -> Result<(), DemandError> {
        work.check()?;
        let mut watches = self.lock_watches();
        let mut retained = watches.owned_bytes_measured(work)?;
        work.adopt(retained)?;
        let result = (|| {
            let mut at = 0;
            while at < watches.entries.len() {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                let (path, descriptor) = &watches.entries[at];
                if path.as_os_str().is_empty() || !path.starts_with(directory) {
                    at += 1;
                    continue;
                }
                let descriptor = *descriptor;
                let mut shared = false;
                for (index, (_, other)) in watches.entries.iter().enumerate() {
                    work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
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
                work.work.admit(&copied, &work.budget)?;
                if !shared {
                    work.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::default() })?;
                    // SAFETY: removes only this instance's admitted descriptor.
                    if unsafe { libc::inotify_rm_watch(self.inotify.as_raw_fd(), descriptor) } < 0 {
                        let error = last_error();
                        if error.raw_os_error() != Some(libc::EINVAL) { return Err(error.into()); }
                    }
                }
                work.record(copied)?;
                let (removed, _) = watches.entries.remove(at);
                let bytes = removed.capacity() as u64;
                drop(removed);
                work.release(bytes)?;
                retained = retained.checked_sub(bytes).ok_or(WorkError::Overflow)?;
            }
            Ok(())
        })();
        drop(watches);
        work.release(retained)?;
        result
    }

    fn open_component_measured(
        parent: i32, name: &OsStr, work: &mut DemandWork,
    ) -> Result<OwnedFd, DemandError> {
        let name = name.as_bytes();
        let bytes = name.len().checked_add(1).ok_or(WorkError::Overflow)? as u64;
        let copied = WorkCounters { bytes_copied: name.len() as u64, ..WorkCounters::default() };
        work.work.admit(&copied, &work.budget)?;
        work.allocate(bytes)?;
        work.record(copied)?;
        let mut buffer = Vec::with_capacity(bytes as usize);
        buffer.extend_from_slice(name);
        buffer.push(0);
        let result = (|| {
            let name = CString::from_vec_with_nul(buffer)
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
            work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
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
        work.release(bytes)?;
        result
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
            // SAFETY: two initialized pollfd records in a live array.
            let ready = unsafe { libc::poll(descriptors.as_mut_ptr(), 2, -1) };
            if ready < 0 {
                if last_error().kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                self.delivery.abandon();
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
            if self.read_and_deliver().is_err() {
                self.delivery.abandon();
                return;
            }
        }
    }

    /// Reads everything queued and delivers it, holding the read lock
    /// throughout.
    fn read_and_deliver(&self) -> io::Result<()> {
        if self.delivery.events.is_some() {
            let cancellation = crate::cancellation::CancellationToken::new();
            return self.read_events_and_deliver_measured(
                std::time::Duration::MAX,
                &mut DemandWork::new(WorkBudget::UNBOUNDED, &cancellation),
            ).map(|_| ()).map_err(|error| match error {
                DemandError::Io(error) => error,
                error => io::Error::other(error),
            });
        }
        let mut reading = self.reading.lock().unwrap_or_else(PoisonError::into_inner);
        let mut changes = Vec::new();
        let mut native_events = Vec::new();
        loop {
            if reading.cursor < reading.filled {
                self.translate(&reading.buffer[reading.cursor..reading.filled], &mut changes, &mut native_events);
                reading.cursor = reading.filled;
            }
            // SAFETY: reads only into the original owned native page.
            let read = unsafe {
                libc::read(self.inotify.as_raw_fd(), reading.buffer.as_mut_ptr().cast(), reading.buffer.len())
            };
            if read < 0 {
                let error = last_error();
                match error.kind() {
                    io::ErrorKind::WouldBlock => break,
                    io::ErrorKind::Interrupted => continue,
                    _ => return Err(error),
                }
            }
            reading.filled = usize::try_from(read).unwrap_or(0);
            reading.cursor = 0;
            if reading.filled == 0 { break; }
        }
        self.delivery.deliver(&changes);
        Ok(())
    }

    /// Drains the same native page through the known SDK event sink. A denied
    /// record leaves both its bytes and alias position pending; the background
    /// reader cannot consume that denied suffix before a subsequent fence.
    fn read_events_and_deliver_measured(
        &self, timeout: std::time::Duration, work: &mut DemandWork,
    ) -> Result<bool, DemandError> {
        work.check()?;
        let sink = self.delivery.events.as_ref().ok_or(
            DemandError::Observation(crate::watch::NativeWatchError::BoundedObservationUnavailable),
        )?;
        let started = std::time::Instant::now();
        let mut reading = self.reading.lock().unwrap_or_else(PoisonError::into_inner);
        let reading = &mut *reading;
        let mut cleanup_backing_claim = 0_u64;
        let mut cleanup_paths_claim = 0_u64;
        let mut new_cleanup_paths = 0_usize;
        let result = (|| {
            loop {
                work.check()?;
                if started.elapsed() >= timeout {
                    return Ok(false);
                }
                while reading.alias_cursor == 0 && !reading.cleanup.is_empty() {
                    let path = reading.cleanup.last().expect("nonempty pending cleanup");
                    self.unadmit_measured(path, work)?;
                    let path = reading.cleanup.pop().expect("pending cleanup remains held");
                    if new_cleanup_paths != 0 {
                        let bytes = path.capacity() as u64;
                        work.release(bytes)?;
                        cleanup_paths_claim -= bytes;
                        new_cleanup_paths -= 1;
                    }
                    drop(path);
                }
                if reading.cursor == reading.filled {
                    work.work.admit(&WorkCounters {
                        source_bytes_read: reading.buffer.len() as u64,
                        ..WorkCounters::default()
                    }, &work.budget)?;
                    work.charge(WorkCounters {
                        backend_read_operations: 1, ..WorkCounters::default()
                    })?;
                    // SAFETY: no competing reader can use this owned page.
                    let read = unsafe {
                        libc::read(self.inotify.as_raw_fd(), reading.buffer.as_mut_ptr().cast(), reading.buffer.len())
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
                    sink.invalidate_measured(work)?;
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
                    sink.invalidate_measured(work)?;
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
                    sink.invalidate_measured(work)?;
                    reading.cursor += end;
                    reading.alias_cursor = 0;
                    continue;
                }
                let mut ancestor = None;
                for (watched, on_the_way) in &self.ancestors {
                    work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                    if *watched == descriptor {
                        ancestor = Some(on_the_way);
                        break;
                    }
                }
                if let Some(on_the_way) = ancestor {
                    if mask & (libc::IN_DELETE_SELF | libc::IN_MOVE_SELF | libc::IN_IGNORED) != 0
                        || mask & REBINDING != 0 && name == on_the_way.as_os_str()
                    {
                        sink.invalidate_measured(work)?;
                        self.delivery.exact.store(false, Ordering::Release);
                        return Ok(false);
                    }
                    reading.cursor += end;
                    reading.alias_cursor = 0;
                    continue;
                }
                let cleanup_needed = mask & (REBINDING | libc::IN_DELETE_SELF | libc::IN_MOVE_SELF) != 0;
                let event_needed = mask & (REBINDING | libc::IN_MODIFY | libc::IN_ATTRIB) != 0;
                loop {
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
                        sink.invalidate_measured(work)?;
                        self.delivery.exact.store(false, Ordering::Release);
                        return Ok(false);
                    }
                    let bytes = alias.as_os_str().as_bytes().len()
                        .checked_add(usize::from(!alias.as_os_str().is_empty() && !name.is_empty()))
                        .and_then(|length| length.checked_add(name.len())).ok_or(WorkError::Overflow)?;
                    let delta = WorkCounters { bytes_copied: bytes as u64, ..WorkCounters::default() };
                    work.work.admit(&delta, &work.budget)?;
                    work.allocate(bytes as u64)?;
                    work.record(delta)?;
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
                            work.work.admit(&delta, &work.budget)?;
                            if cleanup_backing_claim == 0 {
                                work.adopt(old_bytes)?;
                                cleanup_backing_claim = old_bytes;
                            }
                            work.allocate(next_bytes)?;
                            cleanup_backing_claim += next_bytes;
                            work.record(delta)?;
                            let mut replacement = Vec::with_capacity(next_capacity);
                            replacement.append(&mut reading.cleanup);
                            reading.cleanup = replacement;
                            work.release(old_bytes)?;
                            cleanup_backing_claim -= old_bytes;
                            Ok::<(), DemandError>(())
                        })();
                        if let Err(error) = prepared {
                            drop(path);
                            work.release(bytes as u64)?;
                            return Err(error);
                        }
                    }
                    let delivered = sink.event_measured(&path, mask, cookie, work);
                    if delivered.is_err() {
                        drop(path);
                        work.release(bytes as u64)?;
                        return delivered.map(|()| false);
                    }
                    reading.alias_cursor += 1;
                    if cleanup_needed {
                        reading.cleanup.push(path);
                        cleanup_paths_claim += bytes as u64;
                        new_cleanup_paths += 1;
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
                            watches.entries.remove(at);
                        } else { at += 1; }
                    }
                }
                reading.cursor += end;
                reading.alias_cursor = 0;
            }
        })();
        work.release(cleanup_paths_claim)?;
        work.release(cleanup_backing_claim)?;
        reading.blocked = !matches!(result, Ok(true));
        if !reading.blocked { self.reading_changed.notify_all(); }
        result
    }

    /// Turns raw events into changes, keeping the admitted paths current.
    fn translate(&self, mut events: &[u8], changes: &mut Vec<HostChange>, native_events: &mut Vec<notify::Event>) {
        let mut watches = self.lock_watches();
        let mut orphaned = Vec::new();
        while let Some(header) = events.get(..EVENT_HEADER_BYTES) {
            let field = |offset: usize| {
                header
                    .get(offset..offset + 4)
                    .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
                    .map_or(0, u32::from_ne_bytes)
            };
            let descriptor = i32::from_ne_bytes(field(0).to_ne_bytes());
            let mask = field(4);
            let cookie = field(8) as usize;
            let length = usize::try_from(field(12)).unwrap_or(usize::MAX);
            let Some(name) =
                events.get(EVENT_HEADER_BYTES..EVENT_HEADER_BYTES.saturating_add(length))
            else {
                changes.push(HostChange::Everything);
                return;
            };
            events = events
                .get(EVENT_HEADER_BYTES + length..)
                .unwrap_or_default();
            let name = OsStr::from_bytes(name.split(|byte| *byte == 0).next().unwrap_or_default());
            if mask & (libc::IN_Q_OVERFLOW | libc::IN_UNMOUNT) != 0 {
                changes.push(HostChange::Everything);
                continue;
            }
            if let Some((_, on_the_way)) = self.ancestors.iter().find(|(watched, _)| *watched == descriptor) {
                // The root's path no longer names the held root, or can no
                // longer be watched.
                if mask & (libc::IN_DELETE_SELF | libc::IN_MOVE_SELF | libc::IN_IGNORED) != 0
                    || mask & REBINDING != 0 && name == on_the_way.as_os_str()
                {
                    drop(watches);
                    self.delivery.abandon();
                    return;
                }
                continue;
            }
            if mask & libc::IN_IGNORED != 0 {
                watches.drop_descriptor(descriptor);
                continue;
            }
            let aliases = watches.entries.iter()
                .filter(|(_, watched)| *watched == descriptor)
                .map(|(path, _)| path.clone())
                .collect::<Vec<_>>();
            for alias in aliases {
                if self.delivery.events.is_some() {
                    use notify::event::{CreateKind, DataChange, MetadataKind, ModifyKind, RemoveKind, RenameMode};
                    use notify::{Event, EventKind};
                    let directory = mask & libc::IN_ISDIR != 0;
                    let kind = if mask & libc::IN_MOVED_FROM != 0 {
                        Some(EventKind::Modify(ModifyKind::Name(RenameMode::From)))
                    } else if mask & libc::IN_MOVED_TO != 0 {
                        Some(EventKind::Modify(ModifyKind::Name(RenameMode::To)))
                    } else if mask & libc::IN_CREATE != 0 {
                        Some(EventKind::Create(if directory { CreateKind::Folder } else { CreateKind::File }))
                    } else if mask & libc::IN_DELETE != 0 {
                        Some(EventKind::Remove(if directory { RemoveKind::Folder } else { RemoveKind::File }))
                    } else if mask & libc::IN_MODIFY != 0 {
                        Some(EventKind::Modify(ModifyKind::Data(DataChange::Any)))
                    } else if mask & libc::IN_ATTRIB != 0 {
                        Some(EventKind::Modify(ModifyKind::Metadata(MetadataKind::Any)))
                    } else {
                        None
                    };
                    if let Some(kind) = kind {
                        let mut event = Event::new(kind).add_path(alias.join(name));
                        if cookie != 0 { event.attrs.set_tracker(cookie); }
                        native_events.push(event);
                    }
                }
                if name.is_empty() {
                    // The directory itself: its attributes, or its own move
                    // or removal, which its parent reports too. The root is
                    // held open, so it answers wherever it moves, but once
                    // removed nothing reports changes to it.
                    if alias.as_os_str().is_empty() && mask & libc::IN_DELETE_SELF != 0 {
                        drop(watches);
                        self.delivery.abandon();
                        return;
                    }
                    if mask & (libc::IN_DELETE_SELF | libc::IN_MOVE_SELF) == 0 {
                        changes.push(HostChange::Altered(alias));
                    } else {
                        if !alias.as_os_str().is_empty() {
                            orphaned.extend(watches.forget_beneath(&alias));
                        }
                        changes.push(HostChange::Rebound(alias));
                    }
                } else {
                    let path = alias.join(name);
                    if mask & REBINDING == 0 {
                        changes.push(HostChange::Altered(path));
                    } else {
                        orphaned.extend(watches.forget_beneath(&path));
                        changes.push(HostChange::Rebound(path));
                    }
                }
            }
        }
        for descriptor in orphaned {
            // SAFETY: removing a watch this instance added; failure means the
            // kernel already dropped it.
            unsafe { libc::inotify_rm_watch(self.inotify.as_raw_fd(), descriptor) };
        }
    }
}

#[cfg(test)]
mod admission_tests {
    use super::*;
    use crate::cancellation::CancellationToken;
    use crate::source_watch::HostChangeSink;

    struct Ignored;
    impl HostChangeSink for Ignored {
        fn host_changed(&self, _: &[HostChange]) {}
    }

    fn watcher(root: &Path) -> io::Result<PlatformWatch> {
        PlatformWatch::start(
            &crate::native_host::HostRoot::open(root)?,
            Arc::new(Delivery::new(Arc::new(Ignored))),
        )
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
        let mut work = DemandWork::new(WorkBudget {
            backend_read_operations: 3,
            peak_allocation_bytes: 256,
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
        assert_eq!(work.work.allocation_operations, 1);
        assert_eq!(work.work.peak_allocation_bytes, 256);
        assert_eq!(work.work.source_bytes_read, 0);
        assert_eq!(work.remaining()?.peak_allocation_bytes, 256);
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
            backend_write_operations: 0,
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
        assert_eq!(measured.shared.ancestors, ordinary.shared.ancestors);
        assert!(measured.shared.lock_watches().contains_path(Path::new("")));
        assert!(measured.delivery().is_exact());
        assert!(work.work.backend_read_operations >= 6);
        assert!(work.work.source_bytes_read > 0);
        assert!(work.work.allocation_operations >= 5);
        assert!(work.work.peak_allocation_bytes >= READ_BUFFER_BYTES as u64);
        assert!(measured.thread.is_some());
        let retained = measured.shared.ancestors.capacity() as u64
            * std::mem::size_of::<(i32, std::ffi::OsString)>() as u64
            + measured.shared.ancestors.iter().map(|(_, name)| name.capacity() as u64).sum::<u64>()
            + READ_BUFFER_BYTES as u64
            + std::mem::size_of::<(PathBuf, i32)>() as u64
            + std::mem::size_of::<Shared>() as u64;
        assert_eq!(work.remaining()?.peak_allocation_bytes,
            budget.peak_allocation_bytes - retained);
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
        assert_eq!(watch.shared.ancestors.len(),
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
        let mut work = DemandWork::new(WorkBudget {
            backend_read_operations: 3,
            ..WorkBudget::UNBOUNDED
        }, &cancellation);
        assert!(watch.admit_measured(Path::new("a/b"), &mut work)?);
        assert_eq!(work.work.backend_read_operations, 3);
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
        assert!(work.work.items_examined >= 3);
        assert_eq!(work.work.bytes_copied, 2);
        assert_eq!(work.work.backend_read_operations, 1);
        assert!(!watch.shared.lock_watches().contains_path(Path::new("a")));
        assert!(watch.delivery().is_exact());
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
            backend_write_operations: 0, ..WorkBudget::UNBOUNDED
        }, &CancellationToken::new());
        assert!(matches!(watch.unadmit_measured(Path::new("a"), &mut denied),
            Err(DemandError::Work(_))));
        assert_eq!(denied.work.backend_write_operations, 0);
        assert!(watch.shared.lock_watches().contains_path(Path::new("a")));
        let mut allowed = DemandWork::new(WorkBudget {
            backend_write_operations: 1, ..WorkBudget::UNBOUNDED
        }, &CancellationToken::new());
        watch.unadmit_measured(Path::new("a"), &mut allowed)?;
        assert_eq!(allowed.work.backend_write_operations, 1);
        assert!(!watch.shared.lock_watches().contains_path(Path::new("a")));
        Ok(())
    }

    #[test]
    fn retained_growth_receipt_counts_real_buffers_and_their_coexisting_peak()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("a"))?;
        let watch = watcher(root.path())?;
        let record_bytes = std::mem::size_of::<(PathBuf, i32)>() as u64;
        let baseline = watch.shared.lock_watches().entries.capacity() as u64 * record_bytes;
        let fd_digits = std::os::fd::AsRawFd::as_raw_fd(&watch.shared.root).to_string().len() as u64;
        let c_path_bytes = b"/proc/self/fd/".len() as u64 + fd_digits + 3;
        let expected_peak = (baseline + 2 * record_bytes + 1)
            .max(2 * record_bytes + 2 + c_path_bytes);
        let mut work = DemandWork::new(WorkBudget {
            backend_read_operations: 2,
            allocation_operations: 5,
            peak_allocation_bytes: expected_peak,
            items_examined: 3,
            ..WorkBudget::UNBOUNDED
        }, &CancellationToken::new());
        assert!(watch.admit_measured(Path::new("a"), &mut work)?);
        assert_eq!(work.work.allocation_operations, 5);
        assert_eq!(work.work.peak_allocation_bytes, expected_peak);
        assert_eq!(work.work.items_examined, 3);
        assert_eq!(work.work.backend_read_operations, 2);
        Ok(())
    }
}
