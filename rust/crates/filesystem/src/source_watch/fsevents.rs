//! macOS: one `FSEvents` stream over the source root.
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
//! A fence checks that the path still names the held root, and once it
//! does not, the watch is inexact, as the source then fails closed. Only a
//! fence checks: `FSEvents` watches a path's ancestors by holding a
//! descriptor on each (about ten a root), which ran a service with several
//! roots out of descriptors, and between fences a macOS view already
//! answers only as of the reports that have arrived.
//!
//! `FSEvents` coalesces what happened to a path: flags accumulate, so an
//! item written in place soon after it was created is reported as created,
//! which the watch treats as the rebinding it may be.

#![allow(
    unsafe_code,
    reason = "fcntl is a C API; each call documents its contract"
)]

use super::{Delivery, HostChange};
use crate::fsevents::{
    EventStream, ITEM_CREATED, ITEM_REMOVED, ITEM_RENAMED, KERNEL_DROPPED, MOUNT,
    MUST_SCAN_SUBDIRS, UNMOUNT, USER_DROPPED,
};
use std::ffi::CStr;
use std::io;
use std::os::unix::ffi::OsStrExt as _;
use std::path::PathBuf;
use std::sync::Arc;

/// Every flag that says what changed about an item; an event with none of
/// them says only that something at its path did.
const ITEM_FLAGS: u32 = 0x00ff_ff00;

pub(super) struct PlatformWatch {
    shared: Arc<Shared>,
    stream: Option<EventStream>,
}

struct Shared {
    /// The held root's real path, which the stream reports paths under.
    root: PathBuf,
    /// The held root's device and inode.
    held: (u64, u64),
    delivery: Arc<Delivery>,
}

impl PlatformWatch {
    pub(super) fn start(
        directory: &crate::native_host::HostRoot,
        delivery: Arc<Delivery>,
    ) -> io::Result<Self> {
        let root = held_path(directory.directory_fd())?;
        let held = identity(&std::fs::symlink_metadata(&root)?);
        let shared = Arc::new(Shared {
            root,
            held,
            delivery,
        });
        let reader = Arc::clone(&shared);
        let stream = EventStream::start(
            std::slice::from_ref(&shared.root),
            Arc::new(move |events: &[(PathBuf, u32)]| reader.events(events)),
        )
        .map_err(|error| io::Error::other(error.to_string()))?;
        Ok(Self {
            shared,
            stream: Some(stream),
        })
    }

    /// Reports that anything may have changed, or, once the root's path no
    /// longer names the held root, makes the watch inexact: see the module
    /// docs.
    #[allow(clippy::unnecessary_wraps)]
    pub(super) fn fence(&self) -> io::Result<()> {
        let shared = &self.shared;
        let named = std::fs::symlink_metadata(&shared.root)
            .is_ok_and(|metadata| identity(&metadata) == shared.held);
        if named {
            shared.delivery.deliver(&[HostChange::Everything]);
        } else {
            shared.delivery.abandon();
        }
        Ok(())
    }

    pub(super) fn delivery(&self) -> &Delivery {
        &self.shared.delivery
    }

    /// Delivers one event as the stream would.
    #[cfg(test)]
    pub(super) fn inject(&self, path: &std::path::Path, flags: u32) {
        self.shared.events(&[(path.to_path_buf(), flags)]);
    }
}

impl Drop for PlatformWatch {
    fn drop(&mut self) {
        // Stopped first, after any batch in delivery.
        self.stream = None;
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
/// root itself stays the held root, which the stream watches, so an item
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
