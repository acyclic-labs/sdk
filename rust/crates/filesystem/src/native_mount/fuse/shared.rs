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
const ROOT_LISTING_BATCH: usize = 64;

pub(super) fn serial_of(inode: u64) -> u64 {
    inode / VIEW_INODE_SPAN
}

pub(super) fn view_root_inode(serial: u64) -> u64 {
    serial * VIEW_INODE_SPAN + ROOT_INODE
}

pub(super) struct SharedViews {
    views: Arc<NamedViews<Vec<u8>, FuseProjection>>,
    root_attr: FileAttr,
    notifier: OnceLock<Notifier>,
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
    name: Vec<u8>,
}

impl SharedViewEntry {
    pub(super) fn core(&self) -> &Arc<ProjectionCore> {
        &self.core
    }

    /// Retires the view; returns whether this call retired it.
    pub(super) fn retire(&mut self) -> bool {
        // Stopping first fails the view's queued and blocked requests fast;
        // retirement then waits for every request already admitted.
        let core = &self.core;
        if self.view.retire_after(|| core.stop()).is_none() {
            return false;
        }
        if let Some(notifier) = self.views.notifier.get() {
            // The kernel may still cache the name. A late lookup of it now
            // misses, and a late request on its inodes answers ESTALE.
            let _ = notifier.inval_entry(INodeNo(ROOT_INODE), OsStr::from_bytes(&self.name));
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

    pub(super) fn set_notifier(&self, notifier: Notifier) {
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
            name: name.to_vec(),
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

    fn view(&self, inode: INodeNo) -> Result<ViewLease<FuseProjection>, i32> {
        match serial_of(inode.0) {
            0 => Err(libc::EACCES),
            serial => self.views.enter(serial).ok_or(libc::ESTALE),
        }
    }

    fn same_view(&self, first: INodeNo, second: INodeNo) -> Result<(), i32> {
        match (serial_of(first.0), serial_of(second.0)) {
            (0, _) | (_, 0) => Err(libc::EACCES),
            (first, second) if first != second => Err(libc::EXDEV),
            _ => Ok(()),
        }
    }

    fn listed_after(&self, offset: u64, limit: usize) -> Vec<(u64, Vec<u8>)> {
        // Entry offsets: "." is 1, ".." is 2, and the view with serial `s`
        // is `s + 2`, so a listing resumes after its last view.
        let first = offset.saturating_sub(2).saturating_add(1).max(1);
        self.views.listed_from(first, limit)
    }
}

/// The shared session's request router.
pub(super) struct SharedDispatch(pub(super) Arc<SharedViews>);

macro_rules! routed {
    ($self:ident, $inode:expr, $reply:ident, |$view:ident| $call:expr) => {
        match $self.0.view($inode) {
            Ok($view) => $call,
            Err(errno) => $reply.error(Errno::from_i32(errno)),
        }
    };
}

#[allow(clippy::too_many_arguments, reason = "fuser fixes callback signatures")]
impl Filesystem for SharedDispatch {
    fn init(&mut self, _: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        config
            .add_capabilities(REQUESTED_CAPABILITIES & config.capabilities())
            .map_err(|_| std::io::Error::from_raw_os_error(libc::EOPNOTSUPP))
    }

    fn destroy(&mut self) {
        self.0.close();
    }

    fn lookup(&self, request: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        if parent.0 != ROOT_INODE {
            return routed!(self, parent, reply, |view| Filesystem::lookup(
                &*view, request, parent, name, reply
            ));
        }
        let span = callback_span!(TRACE, lookup, ino = parent.0);
        let Some((serial, view)) = self.0.views.enter_name(name.as_bytes()) else {
            return reply.error(errno_on(&span, libc::ENOENT));
        };
        let root = view_root_inode(serial);
        respond!(
            view.core,
            reply,
            span,
            work,
            view.node_attributes(root, None, work),
            |(attr, ttl)| reply.entry(&ttl, &attr, Generation(0))
        );
    }

    fn getattr(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: Option<FuseFileHandle>,
        reply: ReplyAttr,
    ) {
        if inode.0 == ROOT_INODE {
            return reply.attr(&CACHE_TTL, &self.0.root_attr);
        }
        routed!(self, inode, reply, |view| Filesystem::getattr(
            &*view, request, inode, fh, reply
        ));
    }

    fn forget(&self, request: &Request, inode: INodeNo, nlookup: u64) {
        if let Ok(view) = self.0.view(inode) {
            Filesystem::forget(&*view, request, inode, nlookup);
        }
    }

    fn readlink(&self, request: &Request, inode: INodeNo, reply: ReplyData) {
        routed!(self, inode, reply, |view| Filesystem::readlink(
            &*view, request, inode, reply
        ));
    }

    fn setattr(
        &self,
        request: &Request,
        inode: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        fh: Option<FuseFileHandle>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        routed!(self, inode, reply, |view| Filesystem::setattr(
            &*view, request, inode, mode, uid, gid, size, atime, mtime, ctime, fh, crtime, chgtime,
            bkuptime, flags, reply
        ));
    }

    fn mkdir(
        &self,
        request: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        routed!(self, parent, reply, |view| Filesystem::mkdir(
            &*view, request, parent, name, mode, umask, reply
        ));
    }

    fn mknod(
        &self,
        request: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        routed!(self, parent, reply, |view| Filesystem::mknod(
            &*view, request, parent, name, mode, umask, rdev, reply
        ));
    }

    fn symlink(
        &self,
        request: &Request,
        parent: INodeNo,
        name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        routed!(self, parent, reply, |view| Filesystem::symlink(
            &*view, request, parent, name, target, reply
        ));
    }

    fn unlink(&self, request: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        routed!(self, parent, reply, |view| Filesystem::unlink(
            &*view, request, parent, name, reply
        ));
    }

    fn rmdir(&self, request: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        routed!(self, parent, reply, |view| Filesystem::rmdir(
            &*view, request, parent, name, reply
        ));
    }

    fn rename(
        &self,
        request: &Request,
        parent: INodeNo,
        name: &OsStr,
        new_parent: INodeNo,
        new_name: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        if let Err(errno) = self.0.same_view(parent, new_parent) {
            return reply.error(Errno::from_i32(errno));
        }
        routed!(self, parent, reply, |view| Filesystem::rename(
            &*view, request, parent, name, new_parent, new_name, flags, reply
        ));
    }

    fn link(
        &self,
        request: &Request,
        inode: INodeNo,
        parent: INodeNo,
        name: &OsStr,
        reply: ReplyEntry,
    ) {
        if let Err(errno) = self.0.same_view(inode, parent) {
            return reply.error(Errno::from_i32(errno));
        }
        routed!(self, parent, reply, |view| Filesystem::link(
            &*view, request, inode, parent, name, reply
        ));
    }

    fn open(&self, request: &Request, inode: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        routed!(self, inode, reply, |view| Filesystem::open(
            &*view, request, inode, flags, reply
        ));
    }

    fn read(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        offset: u64,
        size: u32,
        flags: OpenFlags,
        lock: Option<LockOwner>,
        reply: ReplyData,
    ) {
        routed!(self, inode, reply, |view| Filesystem::read(
            &*view, request, inode, fh, offset, size, flags, lock, reply
        ));
    }

    fn write(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        offset: u64,
        data: &[u8],
        write_flags: WriteFlags,
        flags: OpenFlags,
        lock: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        routed!(self, inode, reply, |view| Filesystem::write(
            &*view,
            request,
            inode,
            fh,
            offset,
            data,
            write_flags,
            flags,
            lock,
            reply
        ));
    }

    fn fsync(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        routed!(self, inode, reply, |view| Filesystem::fsync(
            &*view, request, inode, fh, datasync, reply
        ));
    }

    fn flush(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        lock: LockOwner,
        reply: ReplyEmpty,
    ) {
        routed!(self, inode, reply, |view| Filesystem::flush(
            &*view, request, inode, fh, lock, reply
        ));
    }

    fn release(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        flags: OpenFlags,
        lock: Option<LockOwner>,
        flush: bool,
        reply: ReplyEmpty,
    ) {
        routed!(self, inode, reply, |view| Filesystem::release(
            &*view, request, inode, fh, flags, lock, flush, reply
        ));
    }

    fn opendir(&self, request: &Request, inode: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        if inode.0 == ROOT_INODE {
            return reply.opened(FuseFileHandle(0), FopenFlags::empty());
        }
        routed!(self, inode, reply, |view| Filesystem::opendir(
            &*view, request, inode, flags, reply
        ));
    }

    fn readdir(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        if inode.0 != ROOT_INODE {
            return routed!(self, inode, reply, |view| Filesystem::readdir(
                &*view, request, inode, fh, offset, reply
            ));
        }
        for (entry_offset, name) in [(1, "."), (2, "..")] {
            if offset < entry_offset
                && reply.add(INodeNo(ROOT_INODE), entry_offset, FileType::Directory, name)
            {
                return reply.ok();
            }
        }
        let mut offset = offset;
        loop {
            let batch = self.0.listed_after(offset, ROOT_LISTING_BATCH);
            if batch.is_empty() {
                return reply.ok();
            }
            for (serial, name) in batch {
                offset = serial + 2;
                if reply.add(
                    INodeNo(view_root_inode(serial)),
                    offset,
                    FileType::Directory,
                    OsStr::from_bytes(&name),
                ) {
                    return reply.ok();
                }
            }
        }
    }

    fn readdirplus(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        offset: u64,
        mut reply: ReplyDirectoryPlus,
    ) {
        if inode.0 != ROOT_INODE {
            return routed!(self, inode, reply, |view| Filesystem::readdirplus(
                &*view, request, inode, fh, offset, reply
            ));
        }
        let root = &self.0.root_attr;
        for (entry_offset, name) in [(1, "."), (2, "..")] {
            if offset < entry_offset
                && reply.add(
                    root.ino,
                    entry_offset,
                    name,
                    &CACHE_TTL,
                    root,
                    Generation(0),
                )
            {
                return reply.ok();
            }
        }
        let mut offset = offset;
        loop {
            let batch = self.0.listed_after(offset, ROOT_LISTING_BATCH);
            if batch.is_empty() {
                return reply.ok();
            }
            for (serial, name) in batch {
                offset = serial + 2;
                // A view retired since it was listed is left out.
                let Some(view) = self.0.views.enter(serial) else {
                    continue;
                };
                let attributes = view
                    .core
                    .request(|work| view.node_attributes(view_root_inode(serial), None, work));
                let Ok((attr, ttl)) = view.core.finish(attributes) else {
                    continue;
                };
                if reply.add(
                    attr.ino,
                    offset,
                    OsStr::from_bytes(&name),
                    &ttl,
                    &attr,
                    Generation(0),
                ) {
                    return reply.ok();
                }
            }
        }
    }

    fn releasedir(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        if inode.0 == ROOT_INODE {
            return reply.ok();
        }
        routed!(self, inode, reply, |view| Filesystem::releasedir(
            &*view, request, inode, fh, flags, reply
        ));
    }

    fn fsyncdir(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        if inode.0 == ROOT_INODE {
            return reply.ok();
        }
        routed!(self, inode, reply, |view| Filesystem::fsyncdir(
            &*view, request, inode, fh, datasync, reply
        ));
    }

    fn setxattr(
        &self,
        request: &Request,
        inode: INodeNo,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        reply: ReplyEmpty,
    ) {
        routed!(self, inode, reply, |view| Filesystem::setxattr(
            &*view, request, inode, name, value, flags, position, reply
        ));
    }

    fn getxattr(
        &self,
        request: &Request,
        inode: INodeNo,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        if inode.0 == ROOT_INODE {
            return reply.error(Errno::from_i32(libc::ENODATA));
        }
        routed!(self, inode, reply, |view| Filesystem::getxattr(
            &*view, request, inode, name, size, reply
        ));
    }

    fn listxattr(&self, request: &Request, inode: INodeNo, size: u32, reply: ReplyXattr) {
        if inode.0 == ROOT_INODE {
            let span = callback_span!(TRACE, listxattr, ino = inode.0);
            return reply_xattr(reply, &[], size, &span);
        }
        routed!(self, inode, reply, |view| Filesystem::listxattr(
            &*view, request, inode, size, reply
        ));
    }

    fn removexattr(&self, request: &Request, inode: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        routed!(self, inode, reply, |view| Filesystem::removexattr(
            &*view, request, inode, name, reply
        ));
    }

    fn fallocate(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        offset: u64,
        length: u64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        routed!(self, inode, reply, |view| Filesystem::fallocate(
            &*view, request, inode, fh, offset, length, mode, reply
        ));
    }

    fn lseek(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        offset: i64,
        whence: i32,
        reply: ReplyLseek,
    ) {
        routed!(self, inode, reply, |view| Filesystem::lseek(
            &*view, request, inode, fh, offset, whence, reply
        ));
    }

    fn copy_file_range(
        &self,
        request: &Request,
        inode_in: INodeNo,
        fh_in: FuseFileHandle,
        offset_in: u64,
        inode_out: INodeNo,
        fh_out: FuseFileHandle,
        offset_out: u64,
        length: u64,
        flags: CopyFileRangeFlags,
        reply: ReplyWrite,
    ) {
        // The kernel falls back to reading and writing across views.
        if let Err(errno) = self.0.same_view(inode_in, inode_out) {
            return reply.error(Errno::from_i32(errno));
        }
        routed!(self, inode_in, reply, |view| Filesystem::copy_file_range(
            &*view, request, inode_in, fh_in, offset_in, inode_out, fh_out, offset_out, length,
            flags, reply
        ));
    }

    fn create(
        &self,
        request: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        routed!(self, parent, reply, |view| Filesystem::create(
            &*view, request, parent, name, mode, umask, flags, reply
        ));
    }
}

/// Runs kernel cache invalidation for every view of one session, one view
/// pass at a time, until the session stops.
pub(super) fn run_shared_invalidator(views: &SharedViews, notifier: &Notifier) {
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
