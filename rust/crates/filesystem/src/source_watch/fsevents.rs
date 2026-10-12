//! macOS: one `FSEvents` stream over the source root.

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
const ITEM_FLAGS: u32 = 0x00ff_ff00;

pub(crate) struct NativeSourceWatch {
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

impl NativeSourceWatch {
    pub(super) fn start_driver(
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
    #[allow(
        clippy::unnecessary_wraps,
        reason = "keeps the fallible fence signature the other watch sources share"
    )]
    pub(crate) fn fence(&self) -> io::Result<()> {
        let shared = &self.shared;
        let named = std::fs::symlink_metadata(&shared.root)
            .is_ok_and(|metadata| identity(&metadata) == shared.held);
        if named {
            shared.delivery.deliver(&[HostChange::Unconfirmed]);
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

impl Drop for NativeSourceWatch {
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
        let watch = super::NativeSourceWatch::start_driver(
            &crate::native_host::HostRoot::open(root.path())?,
            std::sync::Arc::new(super::super::Delivery::new(recorded.clone())),
        )?;
        watch.fence()?;
        let recorded = recorded.0.lock().map_err(|_| "poisoned")?;
        assert!(recorded.contains(&HostChange::Unconfirmed));
        assert!(!recorded.contains(&HostChange::Everything));
        Ok(())
    }

    /// An event that does not say which item it changed, or says its
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
