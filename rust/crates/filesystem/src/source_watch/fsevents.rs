//! macOS: one `FSEvents` stream over the source root.
//!
//! The stream reports each change shortly after it happens, but gives no
//! barrier: the event daemon holds an event for a path it expects to change
//! again (a file just written) and assigns its id only when it releases it,
//! so a later change to another path can be delivered first. A cookie file
//! was seen reported ahead of a file written before it, with a lower event
//! id, and `FSEventStreamFlushSync` does not release held events either. So
//! nothing proves that every change completed before a fence has been
//! delivered, and a fence reports instead that changes may be undelivered
//! ([`HostChange::Unconfirmed`]): every remembered fact is verified against
//! the source at its next use and kept where it still holds, and the
//! reports that arrive later only invalidate what they name once more.
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
use crate::demand::DemandWork;
use crate::{DemandError, WorkBudget, WorkCounters};
use std::ffi::CStr;
use std::io;
use std::os::unix::ffi::OsStrExt as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

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
        let cancellation = crate::cancellation::CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        Self::start_measured(directory, delivery, &mut work)
            .map_err(crate::demand::native_demand_read_error)
    }

    pub(super) fn start_measured(
        directory: &crate::native_host::HostRoot,
        delivery: Arc<Delivery>,
        work: &mut DemandWork,
    ) -> Result<Self, DemandError> {
        let before = work.live_allocation_bytes();
        let result = (|| {
            let root = held_path_measured(directory.directory_fd(), work)?;
            work.charge(WorkCounters {
                backend_read_operations: 1, source_entries_visited: 1,
                ..WorkCounters::default()
            })?;
            let held = identity(&std::fs::symlink_metadata(&root).map_err(DemandError::Io)?);
            work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
            let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
            // SAFETY: fstat initializes the complete stat structure on success.
            if unsafe { libc::fstat(directory.directory_fd(), metadata.as_mut_ptr()) } < 0 {
                return Err(DemandError::Io(io::Error::last_os_error()));
            }
            // SAFETY: the successful fstat initialized metadata.
            let metadata = unsafe { metadata.assume_init() };
            if held != (metadata.st_dev as u64, metadata.st_ino) {
                return Err(DemandError::SourceUnavailable);
            }
            work.allocate(size_of::<Shared>() as u64)?;
            let shared = Arc::new(Shared { root, held, delivery });
            let reader = Arc::clone(&shared);
            // The closure owns one Arc pointer; the refcount header is private.
            work.allocate(size_of::<Arc<Shared>>() as u64)?;
            let handler = Arc::new(move |events: &[(PathBuf, u32)]| reader.events(events));
            let stream = EventStream::start_measured(
                std::slice::from_ref(&shared.root), handler, work,
            )?;
            Ok(Self { shared, stream: Some(stream) })
        })();
        if result.is_err() {
            work.release(work.live_allocation_bytes() - before)?;
        }
        result
    }

    /// Checks the original held-root identity, without pretending that the
    /// event daemon's delayed delivery is an ordered native barrier.
    pub(super) fn fence_measured(
        &self, _timeout: Duration, work: &mut DemandWork,
    ) -> Result<bool, DemandError> {
        let shared = &self.shared;
        work.charge(WorkCounters {
            backend_read_operations: 1, source_entries_visited: 1,
            ..WorkCounters::default()
        })?;
        let named = std::fs::symlink_metadata(&shared.root)
            .is_ok_and(|metadata| identity(&metadata) == shared.held);
        // Finish the already-admitted root check even if cancellation arrived
        // during metadata lookup; later calls must see an abandoned root.
        if named {
            shared.delivery.deliver(&[HostChange::Unconfirmed]);
        } else {
            shared.delivery.abandon();
        }
        Ok(false)
    }

    pub(super) fn fence(&self) -> io::Result<()> {
        let cancellation = crate::cancellation::CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        self.fence_measured(Duration::ZERO, &mut work)
            .map(|_| ())
            .map_err(crate::demand::native_demand_read_error)
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
fn held_path_measured(descriptor: i32, work: &mut DemandWork) -> Result<PathBuf, DemandError> {
    let buffer_bytes = u64::try_from(libc::PATH_MAX).map_err(|_| DemandError::InvalidRequest)?;
    work.allocate(buffer_bytes)?;
    let mut buffer = vec![0_u8; libc::PATH_MAX as usize];
    let result = (|| {
        work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
        // SAFETY: F_GETPATH writes at most PATH_MAX bytes into this buffer.
        if unsafe { libc::fcntl(descriptor, libc::F_GETPATH, buffer.as_mut_ptr()) } < 0 {
            return Err(DemandError::Io(io::Error::last_os_error()));
        }
        let path = CStr::from_bytes_until_nul(&buffer)
            .map_err(|_| DemandError::Io(io::Error::from(io::ErrorKind::InvalidData)))?;
        let bytes = path.to_bytes();
        let owned = bytes.len() as u64;
        let copy = WorkCounters { bytes_copied: owned, ..WorkCounters::default() };
        work.work.admit(&copy, &work.budget)?;
        work.allocate(owned)?;
        let path = PathBuf::from(std::ffi::OsStr::from_bytes(bytes));
        work.record(copy)?;
        Ok(path)
    })();
    drop(buffer);
    work.release(buffer_bytes)?;
    result
}

#[cfg(test)]
mod tests {
    use super::{HostChange, ITEM_CREATED, ITEM_REMOVED, ITEM_RENAMED, MUST_SCAN_SUBDIRS, change};
    use std::path::PathBuf;

    const ITEM_MODIFIED: u32 = 0x1000;
    const ITEM_IS_FILE: u32 = 0x1_0000;
    const ITEM_IS_DIR: u32 = 0x2_0000;

    struct Ignored;
    impl super::super::HostChangeSink for Ignored {
        fn host_changed(&self, _: &[HostChange]) {}
    }

    fn finite_budget() -> crate::WorkBudget {
        crate::WorkBudget {
            backend_read_operations: 32,
            backend_write_operations: 32,
            source_entries_visited: 2,
            bytes_copied: 1024 * 1024,
            allocation_operations: 16,
            peak_allocation_bytes: 1024 * 1024,
            ..crate::WorkBudget::default()
        }
    }

    #[test]
    fn finite_start_runs_the_original_stream_and_never_claims_a_barrier()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        let directory = crate::native_host::HostRoot::open(root.path())?;
        let mut work = super::DemandWork::new(
            finite_budget(), &crate::cancellation::CancellationToken::new(),
        );
        let watch = super::PlatformWatch::start_measured(
            &directory, std::sync::Arc::new(super::super::Delivery::new(
                std::sync::Arc::new(Ignored),
            )), &mut work,
        )?;
        assert!(watch.stream.is_some());
        assert_eq!(work.work.source_entries_visited, 1);
        assert!(work.work.backend_read_operations >= 3);
        assert!(work.work.backend_write_operations >= 6);
        assert!(work.work.allocation_operations >= 7);
        assert!(!watch.fence_measured(std::time::Duration::ZERO, &mut work)?);
        assert_eq!(work.work.source_entries_visited, 2);
        assert!(watch.delivery().is_exact());
        Ok(())
    }

    #[test]
    fn denied_root_copy_never_reaches_metadata_or_the_framework()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        let directory = crate::native_host::HostRoot::open(root.path())?;
        let mut budget = finite_budget();
        budget.bytes_copied = 0;
        let mut work = super::DemandWork::new(
            budget, &crate::cancellation::CancellationToken::new(),
        );
        let error = super::PlatformWatch::start_measured(
            &directory, std::sync::Arc::new(super::super::Delivery::new(
                std::sync::Arc::new(Ignored),
            )), &mut work,
        ).err().ok_or("root conversion exceeded its admitted copy allowance")?;
        assert!(matches!(error, crate::DemandError::Work(_)));
        assert_eq!(work.work.backend_read_operations, 1);
        assert_eq!(work.work.backend_write_operations, 0);
        assert_eq!(work.work.source_entries_visited, 0);
        assert_eq!(work.work.bytes_copied, 0);
        assert_eq!(work.live_allocation_bytes(), 0);
        Ok(())
    }

    #[test]
    fn denied_framework_registration_releases_all_startup_payload()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        let directory = crate::native_host::HostRoot::open(root.path())?;
        let mut budget = finite_budget();
        budget.backend_write_operations = 1;
        let mut work = super::DemandWork::new(
            budget, &crate::cancellation::CancellationToken::new(),
        );
        let error = super::PlatformWatch::start_measured(
            &directory, std::sync::Arc::new(super::super::Delivery::new(
                std::sync::Arc::new(Ignored),
            )), &mut work,
        ).err().ok_or("stream registration exceeded its admitted allowance")?;
        assert!(matches!(error, crate::DemandError::Work(_)));
        assert_eq!(work.work.backend_write_operations, 0);
        assert_eq!(work.work.source_entries_visited, 1);
        assert_eq!(work.live_allocation_bytes(), 0);
        Ok(())
    }

    #[test]
    fn denied_stream_creation_releases_the_real_cf_path_array()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        let directory = crate::native_host::HostRoot::open(root.path())?;
        // Warm the process-lifetime function table to exercise stream creation.
        drop(super::PlatformWatch::start(
            &directory, std::sync::Arc::new(super::super::Delivery::new(
                std::sync::Arc::new(Ignored),
            )),
        )?);
        let mut budget = finite_budget();
        budget.backend_write_operations = 7;
        let mut work = super::DemandWork::new(
            budget, &crate::cancellation::CancellationToken::new(),
        );
        let error = super::PlatformWatch::start_measured(
            &directory, std::sync::Arc::new(super::super::Delivery::new(
                std::sync::Arc::new(Ignored),
            )), &mut work,
        ).err().ok_or("stream creation exceeded its cleanup-reserved allowance")?;
        assert!(matches!(error, crate::DemandError::Work(_)));
        // CFStringCreate, CFArrayCreate, and both CFRelease calls occurred;
        // FSEventStreamCreate and its allocation operation were not admitted.
        assert_eq!(work.work.backend_write_operations, 4);
        assert_eq!(work.work.allocation_operations, 8);
        assert_eq!(work.live_allocation_bytes(), 0);
        Ok(())
    }

    #[test]
    fn cancelled_start_admits_no_root_conversion()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        let directory = crate::native_host::HostRoot::open(root.path())?;
        let cancellation = crate::cancellation::CancellationToken::new();
        cancellation.cancel();
        let mut work = super::DemandWork::new(finite_budget(), &cancellation);
        let error = super::PlatformWatch::start_measured(
            &directory, std::sync::Arc::new(super::super::Delivery::new(
                std::sync::Arc::new(Ignored),
            )), &mut work,
        ).err().ok_or("cancelled startup unexpectedly ran")?;
        assert!(matches!(error, crate::DemandError::Cancelled));
        assert_eq!(work.work, crate::WorkCounters::default());
        Ok(())
    }

    #[test]
    fn measured_fence_rejects_denied_reads_and_abandons_a_replaced_root()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        let source = root.path().join("source");
        std::fs::create_dir(&source)?;
        let directory = crate::native_host::HostRoot::open(&source)?;
        let watch = super::PlatformWatch::start(
            &directory, std::sync::Arc::new(super::super::Delivery::new(
                std::sync::Arc::new(Ignored),
            )),
        )?;
        let cancellation = crate::cancellation::CancellationToken::new();
        let mut denied = super::DemandWork::new(crate::WorkBudget::default(), &cancellation);
        assert!(matches!(
            watch.fence_measured(std::time::Duration::ZERO, &mut denied),
            Err(crate::DemandError::Work(_))
        ));
        assert_eq!(denied.work, crate::WorkCounters::default());
        assert!(watch.delivery().is_exact());
        std::fs::rename(&source, root.path().join("moved"))?;
        std::fs::create_dir(&source)?;
        let mut work = super::DemandWork::new(crate::WorkBudget {
            backend_read_operations: 1, source_entries_visited: 1,
            ..crate::WorkBudget::default()
        }, &cancellation);
        assert!(!watch.fence_measured(std::time::Duration::ZERO, &mut work)?);
        assert!(!watch.delivery().is_exact());
        assert_eq!(work.work.backend_read_operations, 1);
        assert_eq!(work.work.source_entries_visited, 1);
        Ok(())
    }

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
        assert!(recorded.contains(&HostChange::Unconfirmed));
        assert!(!recorded.contains(&HostChange::Everything));
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
