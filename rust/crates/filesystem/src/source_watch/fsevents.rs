//! macOS: one `FSEvents` stream over the source root, and one kqueue over
//! the root's path.
//!
//! The stream reports each change shortly after it happens, but gives no
//! barrier: the event daemon holds an event for a path it expects to change
//! again (a file just written) and assigns its id only when it releases it,
//! so a later change to another path can be delivered first. A cookie file
//! was seen reported ahead of a file written before it, with a lower event
//! id, and `FSEventStreamFlushSync` does not release held events either. So
//! nothing proves that every change completed before a fence has been
//! delivered, and a fence reports instead that anything may have changed:
//! every remembered fact is read again, and the reports that arrive later
//! only invalidate what they name once more.
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
use std::sync::Arc;
use std::thread::JoinHandle;

/// Every flag that says what changed about an item; an event with none of
/// them says only that something at its path did.
const ITEM_FLAGS: u32 = 0x00ff_ff00;
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
    delivery: Arc<Delivery>,
}

impl PlatformWatch {
    pub(super) fn start(
        directory: &crate::native_host::HostRoot,
        delivery: Arc<Delivery>,
    ) -> io::Result<Self> {
        let root = held_path(directory.directory_fd())?;
        let path = RootPath::watch(&root, Arc::clone(&delivery))?;
        let shared = Arc::new(Shared { root, delivery });
        let reader = Arc::clone(&shared);
        let stream = EventStream::start(
            std::slice::from_ref(&shared.root),
            Arc::new(move |events: &[(PathBuf, u32)]| reader.events(events)),
        )
        .map_err(|error| io::Error::other(error.to_string()))?;
        Ok(Self {
            shared,
            stream: Some(stream),
            path,
        })
    }

    /// Reports that anything may have changed: see the module docs.
    #[allow(clippy::unnecessary_wraps)]
    pub(super) fn fence(&self) -> io::Result<()> {
        self.shared.delivery.deliver(&[HostChange::Everything]);
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
    }
}

impl Shared {
    fn events(&self, events: &[(PathBuf, u32)]) {
        let mut changes = Vec::with_capacity(events.len());
        for (path, flags) in events {
            if flags & (USER_DROPPED | KERNEL_DROPPED | MOUNT | UNMOUNT) != 0 {
                changes.push(HostChange::Everything);
            } else if let Ok(relative) = path.strip_prefix(&self.root) {
                changes.push(change(relative.to_path_buf(), *flags));
            }
        }
        self.delivery.deliver(&changes);
    }
}

/// What one event says of the path it names.
///
/// An event with no item flags, or one that says its directory must be
/// scanned, says only that something at or beneath its path changed; for
/// the root that is anything at all. Otherwise only an event that never
/// created, removed, or renamed its item altered the item in place. The
/// root itself stays the held root, which the kqueue watches, so an item
/// event naming it (a replayed creation included) altered it in place.
fn change(relative: PathBuf, flags: u32) -> HostChange {
    let unspecific = flags & ITEM_FLAGS == 0 || flags & MUST_SCAN_SUBDIRS != 0;
    if relative.as_os_str().is_empty() {
        return if unspecific {
            HostChange::Everything
        } else {
            HostChange::Altered(relative)
        };
    }
    if unspecific || flags & (ITEM_CREATED | ITEM_REMOVED | ITEM_RENAMED) != 0 {
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

#[cfg(test)]
mod tests {
    use super::{HostChange, ITEM_CREATED, ITEM_REMOVED, ITEM_RENAMED, MUST_SCAN_SUBDIRS, change};
    use std::path::PathBuf;

    const ITEM_MODIFIED: u32 = 0x1000;
    const ITEM_IS_FILE: u32 = 0x1_0000;
    const ITEM_IS_DIR: u32 = 0x2_0000;

    /// `FSEvents` proves no delivery order, so a fence reports everything.
    #[test]
    fn a_fence_reports_everything() -> Result<(), Box<dyn std::error::Error>> {
        struct Recorded(std::sync::Mutex<Vec<HostChange>>);
        impl super::super::HostChangeSink for Recorded {
            fn host_changed(&self, changes: &[HostChange]) {
                if let Ok(mut recorded) = self.0.lock() {
                    recorded.extend_from_slice(changes);
                }
            }
        }
        let root = tempfile::tempdir()?;
        let recorded = std::sync::Arc::new(Recorded(std::sync::Mutex::new(Vec::new())));
        let watch = super::PlatformWatch::start(
            &crate::native_host::HostRoot::open(root.path())?,
            std::sync::Arc::new(super::super::Delivery::new(recorded.clone())),
        )?;
        watch.fence()?;
        let recorded = recorded.0.lock().map_err(|_| "poisoned")?;
        assert!(recorded.contains(&HostChange::Everything));
        Ok(())
    }

    /// An event that does not say which item it changed, or says its
    /// directory must be scanned, covers everything beneath its path.
    #[test]
    fn unspecific_events_cover_everything_beneath_their_path() {
        let root = PathBuf::new;
        let name = || PathBuf::from("d");
        assert_eq!(change(root(), 0), HostChange::Everything);
        assert_eq!(
            change(root(), MUST_SCAN_SUBDIRS | ITEM_IS_DIR),
            HostChange::Everything
        );
        assert_eq!(
            change(root(), ITEM_CREATED | ITEM_IS_DIR),
            HostChange::Altered(root())
        );
        assert_eq!(change(name(), 0), HostChange::Rebound(name()));
        assert_eq!(
            change(name(), MUST_SCAN_SUBDIRS),
            HostChange::Rebound(name())
        );
        for flag in [ITEM_CREATED, ITEM_REMOVED, ITEM_RENAMED] {
            assert_eq!(
                change(name(), flag | ITEM_MODIFIED | ITEM_IS_FILE),
                HostChange::Rebound(name())
            );
        }
        assert_eq!(
            change(name(), ITEM_MODIFIED | ITEM_IS_FILE),
            HostChange::Altered(name())
        );
    }
}
