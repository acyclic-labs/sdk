//! One FUSE session serving every view of a process.
//!
//! The session's root (inode 1) is a synthetic directory with one entry per
//! live view. A view's inodes are `serial * VIEW_INODE_SPAN + 1 ..` (see
//! [`ProjectionState::new`]), so every kernel request names its view in the
//! inode's high bits. Requests are routed through the shared core's
//! [`NamedViews`]: a retired or unknown serial answers `ESTALE` and never
//! reaches a core, and retirement waits for every admitted request. Requests that span two views
//! (`rename`, `link`, `copy_file_range`) answer `EXDEV`. Kernel cache
//! invalidation for every view runs on one thread.

use super::*;
use crate::native_mount::shared::view_table::ViewLease;
use crate::native_mount::shared::views::{NamedView, NamedViews};
use std::collections::VecDeque;
use std::sync::Condvar;
use std::sync::atomic::AtomicBool;

/// The largest serial whose inode span fits in a `u64` inode. Serial 0 is
/// the synthetic root's span.
const MAX_SERIAL: u64 = u64::MAX / VIEW_INODE_SPAN - 1;

/// Views listed per root `readdirplus` batch, so attributes are read without
/// holding the name lock.
pub(super) const ROOT_LISTING_BATCH: usize = 64;

pub(super) fn serial_of(inode: u64) -> u64 {
    inode / VIEW_INODE_SPAN
}

pub(super) fn view_root_inode(serial: u64) -> u64 {
    serial * VIEW_INODE_SPAN + ROOT_INODE
}

pub(super) struct SharedViews {
    pub(super) views: Arc<NamedViews<Vec<u8>, FuseProjection>>,
    pub(super) root_attr: FileAttr,
    notifier: OnceLock<CacheNotifier>,
    pending: Mutex<PendingInvalidations>,
    invalidation: Condvar,
}

#[derive(Default)]
struct PendingInvalidations {
    serials: VecDeque<u64>,
    stopping: bool,
}

/// Wakes the shared invalidator for one view; held by that view's core.
pub(super) struct SharedWake {
    views: std::sync::Weak<SharedViews>,
    serial: u64,
    queued: AtomicBool,
}

impl SharedWake {
    pub(super) fn wake(&self) {
        if self.queued.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(views) = self.views.upgrade() {
            let mut pending = views.pending.lock().unwrap_or_else(PoisonError::into_inner);
            if !pending.stopping {
                pending.serials.push_back(self.serial);
                drop(pending);
                views.invalidation.notify_one();
            }
        }
    }
}

/// A view's place in the shared session. Dropping it removes the view's
/// name, stops its core, waits for its in-flight requests and drops the
/// kernel's entry for the name.
pub(super) struct SharedViewEntry {
    views: Arc<SharedViews>,
    view: NamedView<Vec<u8>, FuseProjection>,
    core: Arc<ProjectionCore>,
}

impl SharedViewEntry {
    pub(super) fn core(&self) -> &Arc<ProjectionCore> {
        &self.core
    }

    /// Retires the view; returns whether this call retired it.
    pub(super) fn retire(&mut self) -> bool {
        // Close admission, then stop queued and blocked requests before
        // waiting for every request already admitted.
        let core = &self.core;
        let Some((name, _)) = self.view.retire_after(|| core.stop()) else {
            return false;
        };
        if let Some(notifier) = self.views.notifier.get() {
            // The kernel may still cache the name. A late lookup of it now
            // misses, and a late request on its inodes answers ESTALE.
            let _ = notifier.inval_entry(ROOT_INODE, &name);
        }
        true
    }
}

impl Drop for SharedViewEntry {
    fn drop(&mut self) {
        self.retire();
    }
}

impl SharedViews {
    pub(super) fn new(uid: u32, gid: u32) -> Arc<Self> {
        let now = SystemTime::now();
        Arc::new(Self {
            views: Arc::new(NamedViews::new(MAX_SERIAL)),
            root_attr: FileAttr {
                ino: INodeNo(ROOT_INODE),
                size: 0,
                blocks: 0,
                atime: now,
                mtime: now,
                ctime: now,
                crtime: now,
                kind: FileType::Directory,
                perm: 0o555,
                nlink: 2,
                uid,
                gid,
                rdev: 0,
                blksize: 4096,
                flags: 0,
            },
            notifier: OnceLock::new(),
            pending: Mutex::new(PendingInvalidations::default()),
            invalidation: Condvar::new(),
        })
    }

    pub(super) fn set_notifier(&self, notifier: CacheNotifier) {
        let _ = self.notifier.set(notifier);
    }

    /// Lists a view named `name` in the root. `build` receives the view's
    /// inode base and returns its core.
    pub(super) fn insert(
        self: &Arc<Self>,
        name: &[u8],
        build: impl FnOnce(u64) -> Result<Arc<ProjectionCore>, NativeMountError>,
    ) -> Result<SharedViewEntry, NativeMountError> {
        if name.is_empty() || name.contains(&b'/') || name == b"." || name == b".." {
            return Err(NativeMountError::InvalidDestination);
        }
        let mut built = None;
        let view = self.views.insert(name.to_vec(), |serial| {
            let core = build(serial * VIEW_INODE_SPAN)?;
            if let Some(notifier) = self.notifier.get() {
                let _ = core.notifier.set(notifier.clone());
            }
            let _ = core.shared_wake.set(Arc::new(SharedWake {
                views: Arc::downgrade(self),
                serial,
                queued: AtomicBool::new(false),
            }));
            built = Some(Arc::clone(&core));
            Ok(FuseProjection { core })
        })?;
        Ok(SharedViewEntry {
            views: Arc::clone(self),
            core: built.ok_or(NativeMountError::InvalidDestination)?,
            view,
        })
    }

    /// Refuses new views and stops every listed view's core.
    pub(super) fn close(&self) {
        self.views.close(|view| view.core.stop());
    }

    pub(super) fn stop_invalidator(&self) {
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .stopping = true;
        self.invalidation.notify_all();
    }

    pub(super) fn view(&self, inode: INodeNo) -> Result<ViewLease<FuseProjection>, i32> {
        match serial_of(inode.0) {
            0 => Err(libc::EACCES),
            serial => self.views.enter(serial).ok_or(libc::ESTALE),
        }
    }

    pub(super) fn same_view(&self, first: INodeNo, second: INodeNo) -> Result<(), i32> {
        match (serial_of(first.0), serial_of(second.0)) {
            (0, _) | (_, 0) => Err(libc::EACCES),
            (first, second) if first != second => Err(libc::EXDEV),
            _ => Ok(()),
        }
    }

    pub(super) fn listed_after(&self, offset: u64, limit: usize) -> Vec<(u64, Vec<u8>)> {
        // Entry offsets: "." is 1, ".." is 2, and the view with serial `s`
        // is `s + 2`, so a listing resumes after its last view.
        let first = offset.saturating_sub(2).saturating_add(1).max(1);
        self.views.listed_from(first, limit)
    }
}

/// Runs kernel cache invalidation for every view of one session, one view
/// pass at a time, until the session stops.
pub(super) fn run_shared_invalidator(views: &SharedViews, notifier: &CacheNotifier) {
    loop {
        let serial = {
            let mut pending = views.pending.lock().unwrap_or_else(PoisonError::into_inner);
            loop {
                if pending.stopping {
                    return;
                }
                if let Some(serial) = pending.serials.pop_front() {
                    break serial;
                }
                pending = views
                    .invalidation
                    .wait(pending)
                    .unwrap_or_else(PoisonError::into_inner);
            }
        };
        let Some(view) = views.views.enter(serial) else {
            continue;
        };
        if let Some(wake) = view.core.shared_wake.get() {
            // Cleared before the pass, so a change during it queues another.
            wake.queued.store(false, Ordering::Release);
        }
        shared_invalidation_pass(&view.core, notifier);
    }
}
