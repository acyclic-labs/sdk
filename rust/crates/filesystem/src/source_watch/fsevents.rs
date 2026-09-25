//! macOS: one `FSEvents` stream over the source root and a private fence
//! directory, and one kqueue over the root's path.
//!
//! The kernel appends one event to a single ordered queue as each operation
//! completes, and a stream delivers the events for its paths in that order,
//! on one serial queue. A fence writes a cookie into the private directory
//! and waits for its event: everything that completed before the cookie was
//! written has been delivered by then. (`FSEventStreamFlushSync` delivers
//! only what the event daemon already holds, not what the kernel has yet
//! to hand it.)
//!
//! The stream follows the root by path, so a root that moves, or whose
//! ancestor moves, leaves it reporting a path the held root no longer has.
//! The kqueue watches the root and every ancestor for being renamed,
//! removed, or revoked; once the root's path no longer names the held root,
//! the watch is inexact, as the source then fails closed.
//!
//! `FSEvents` coalesces what happened to a path: flags accumulate, so an
//! item written in place soon after it was created is reported as created,
//! which the watch treats as the rebinding it may be.

#![allow(
    unsafe_code,
    reason = "kqueue is a C API; each call documents its contract"
)]

use super::{Delivery, HostChange};
use crate::fsevents::{
    EventStream, ITEM_CREATED, ITEM_REMOVED, ITEM_RENAMED, KERNEL_DROPPED, MOUNT,
    MUST_SCAN_SUBDIRS, UNMOUNT, USER_DROPPED,
};
use std::ffi::{CStr, CString, c_void};
use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;

/// Every flag that says what changed about an item; an event with none of
/// them says only that something at its path did.
const ITEM_FLAGS: u32 = 0x00ff_ff00;
/// How long a fence waits for its cookie before it assumes everything
/// changed instead.
const FENCE_TIMEOUT: Duration = Duration::from_secs(10);
/// Identifies the kqueue event that stops its thread.
const STOP: usize = usize::MAX;

pub(super) struct PlatformWatch {
    shared: Arc<Shared>,
    stream: Option<EventStream>,
    path: RootPath,
}

struct Shared {
    /// The held root's real path, which the stream reports paths under.
    root: PathBuf,
    /// A private directory whose cookies fence delivery.
    fences: PathBuf,
    delivery: Arc<Delivery>,
    next_fence: AtomicU64,
    /// The newest cookie delivered.
    fenced: Mutex<u64>,
    fence_delivered: Condvar,
}

static FENCE_DIRECTORIES: AtomicU64 = AtomicU64::new(0);

impl PlatformWatch {
    pub(super) fn start(
        directory: &crate::native_host::HostRoot,
        delivery: Arc<Delivery>,
    ) -> io::Result<Self> {
        let root = held_path(directory.directory_fd())?;
        let fences = std::env::temp_dir().join(format!(
            "acyclic-source-fence-{}-{}",
            std::process::id(),
            FENCE_DIRECTORIES.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&fences)?;
        let started = (|| {
            let fences = fences.canonicalize()?;
            let path = RootPath::watch(&root, Arc::clone(&delivery))?;
            let shared = Arc::new(Shared {
                root,
                fences,
                delivery,
                next_fence: AtomicU64::new(1),
                fenced: Mutex::new(0),
                fence_delivered: Condvar::new(),
            });
            let reader = Arc::clone(&shared);
            let stream = EventStream::start(
                &[shared.root.clone(), shared.fences.clone()],
                Arc::new(move |events: &[(PathBuf, u32)]| reader.events(events)),
            )
            .map_err(|error| io::Error::other(error.to_string()))?;
            Ok(Self {
                shared,
                stream: Some(stream),
                path,
            })
        })();
        if started.is_err() {
            let _ = std::fs::remove_dir(&fences);
        }
        started
    }

    #[allow(clippy::unnecessary_wraps)]
    pub(super) fn fence(&self) -> io::Result<()> {
        let shared = &self.shared;
        let fence = shared.next_fence.fetch_add(1, Ordering::Relaxed);
        let cookie = shared.fences.join(fence.to_string());
        let written =
            std::fs::File::create_new(&cookie).and_then(|_| std::fs::remove_file(&cookie));
        let fenced = shared.fenced.lock().unwrap_or_else(PoisonError::into_inner);
        let delivered = written.is_ok()
            && !shared
                .fence_delivered
                .wait_timeout_while(fenced, FENCE_TIMEOUT, |delivered| *delivered < fence)
                .unwrap_or_else(PoisonError::into_inner)
                .1
                .timed_out();
        if !delivered {
            // Unproven delivery: assume everything changed instead.
            shared.delivery.deliver(&[HostChange::Everything]);
        }
        Ok(())
    }

    pub(super) fn delivery(&self) -> &Delivery {
        &self.shared.delivery
    }

    /// Delivers one event as the stream would.
    #[cfg(test)]
    pub(super) fn inject(&self, path: &Path, flags: u32) {
        self.shared.events(&[(path.to_path_buf(), flags)]);
    }
}

impl Drop for PlatformWatch {
    fn drop(&mut self) {
        // Stopped first, after any batch in delivery.
        self.stream = None;
        self.path.stop();
        let _ = std::fs::remove_dir_all(&self.shared.fences);
    }
}

impl Shared {
    /// Delivers one batch, then releases the fences it reached: the
    /// stream's order puts every earlier change in this batch or before it.
    fn events(&self, events: &[(PathBuf, u32)]) {
        let mut changes = Vec::with_capacity(events.len());
        let mut fenced = None;
        for (path, flags) in events {
            if flags & (USER_DROPPED | KERNEL_DROPPED | MOUNT | UNMOUNT) != 0 {
                changes.push(HostChange::Everything);
                continue;
            }
            if let Ok(cookie) = path.strip_prefix(&self.fences) {
                if let Some(fence) = cookie.to_str().and_then(|name| name.parse::<u64>().ok()) {
                    fenced = fenced.max(Some(fence));
                }
                continue;
            }
            if let Ok(relative) = path.strip_prefix(&self.root) {
                changes.push(change(relative.to_path_buf(), *flags));
            }
        }
        self.delivery.deliver(&changes);
        if let Some(fence) = fenced {
            let mut delivered = self.fenced.lock().unwrap_or_else(PoisonError::into_inner);
            *delivered = (*delivered).max(fence);
            self.fence_delivered.notify_all();
        }
    }
}

/// What one event says of the path it names.
fn change(relative: PathBuf, flags: u32) -> HostChange {
    // The root itself stays the held root, which the kqueue watches: an
    // event naming it can only have altered it in place. Otherwise only an
    // event that never created, removed, or renamed its item, and says what
    // it did, altered the item in place.
    if relative.as_os_str().is_empty() {
        return HostChange::Altered(relative);
    }
    if flags & ITEM_FLAGS == 0
        || flags & (MUST_SCAN_SUBDIRS | ITEM_CREATED | ITEM_REMOVED | ITEM_RENAMED) != 0
    {
        HostChange::Rebound(relative)
    } else {
        HostChange::Altered(relative)
    }
}

/// A kqueue over the root and each ancestor, whose thread makes the watch
/// inexact once the root's path stops naming the held root.
struct RootPath {
    kqueue: Arc<OwnedFd>,
    thread: Option<JoinHandle<()>>,
}

impl RootPath {
    fn watch(root: &Path, delivery: Arc<Delivery>) -> io::Result<Self> {
        // SAFETY: creates a new kqueue descriptor, which we own.
        let kqueue = unsafe { libc::kqueue() };
        if kqueue < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a descriptor the call just returned.
        let kqueue = Arc::new(unsafe { OwnedFd::from_raw_fd(kqueue) });
        let held = identity(&std::fs::symlink_metadata(root)?);
        let mut directories = Vec::new();
        let mut changes = vec![event(STOP, libc::EVFILT_USER, 0)];
        for directory in root.ancestors() {
            let path = CString::new(directory.as_os_str().as_bytes())
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
            // SAFETY: a NUL-terminated path; the descriptor only reports.
            let descriptor = unsafe {
                libc::open(
                    path.as_ptr(),
                    libc::O_EVTONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
                )
            };
            if descriptor < 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: a descriptor the call just returned.
            directories.push(unsafe { OwnedFd::from_raw_fd(descriptor) });
            changes.push(event(
                usize::try_from(descriptor).unwrap_or(usize::MAX),
                libc::EVFILT_VNODE,
                libc::NOTE_DELETE | libc::NOTE_RENAME | libc::NOTE_REVOKE,
            ));
        }
        // SAFETY: registers live descriptors from a live change list.
        let registered = unsafe {
            libc::kevent(
                kqueue.as_raw_fd(),
                changes.as_ptr(),
                i32::try_from(changes.len()).unwrap_or(i32::MAX),
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        if registered < 0 {
            return Err(io::Error::last_os_error());
        }
        let (queue, root) = (Arc::clone(&kqueue), root.to_path_buf());
        let thread = std::thread::Builder::new()
            .name("acyclic-source-root".to_owned())
            .spawn(move || {
                // The descriptors stay open, and registered, while it runs.
                let _directories = directories;
                let mut happened = event(0, 0, 0);
                loop {
                    // SAFETY: waits for one event into a live record.
                    let count = unsafe {
                        libc::kevent(
                            queue.as_raw_fd(),
                            std::ptr::null(),
                            0,
                            &raw mut happened,
                            1,
                            std::ptr::null(),
                        )
                    };
                    if count < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted
                    {
                        continue;
                    }
                    if count <= 0 || happened.ident == STOP {
                        return;
                    }
                    // A directory on the way moved or went: the path may
                    // have been restored since, naming the root still.
                    let named = std::fs::symlink_metadata(&root)
                        .is_ok_and(|metadata| identity(&metadata) == held);
                    if !named {
                        delivery.abandon();
                        return;
                    }
                }
            })?;
        Ok(Self {
            kqueue,
            thread: Some(thread),
        })
    }

    fn stop(&mut self) {
        let mut stop = event(STOP, libc::EVFILT_USER, 0);
        stop.flags = 0;
        stop.fflags = libc::NOTE_TRIGGER;
        // SAFETY: triggers the registered user event on a live kqueue.
        unsafe {
            libc::kevent(
                self.kqueue.as_raw_fd(),
                &raw const stop,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            );
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// One kqueue registration of `ident`.
fn event(ident: usize, filter: i16, fflags: u32) -> libc::kevent {
    libc::kevent {
        ident,
        filter,
        flags: libc::EV_ADD | libc::EV_CLEAR,
        fflags,
        data: 0,
        udata: std::ptr::null_mut::<c_void>(),
    }
}

fn identity(metadata: &std::fs::Metadata) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt as _;
    (metadata.dev(), metadata.ino())
}

/// The real path of a held directory, as `FSEvents` reports paths.
fn held_path(descriptor: i32) -> io::Result<PathBuf> {
    let mut buffer = vec![0_u8; usize::try_from(libc::PATH_MAX).unwrap_or(1024)];
    // SAFETY: `F_GETPATH` writes at most `PATH_MAX` bytes into the buffer.
    if unsafe { libc::fcntl(descriptor, libc::F_GETPATH, buffer.as_mut_ptr()) } < 0 {
        return Err(io::Error::last_os_error());
    }
    let path = CStr::from_bytes_until_nul(&buffer)
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
    Ok(PathBuf::from(std::ffi::OsStr::from_bytes(path.to_bytes())))
}
