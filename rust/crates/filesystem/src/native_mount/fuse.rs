//! Linux FUSE projection over the common callback contract.

use super::{
    MountDirectoryEntry, MountFilesystem, MountLookup, MountNode, MountNodeKind, MountOpenFile,
    MountPath, MountSeekTarget, MountSourceError, NativeMountError, NativeMountRequest, ViewStamp,
    metadata_or, system_time_ns,
};
use crate::kernel::{FileMetadata, MetadataField};
use bytes::Bytes;
use fuser::{
    BackgroundSession, BsdFileFlags, Config, CopyFileRangeFlags, Errno, FileAttr,
    FileHandle as FuseFileHandle, FileType, Filesystem, FopenFlags, Generation, INodeNo, InitFlags,
    KernelConfig, LockOwner, MountOption, OpenFlags, RenameFlags, ReplyAttr, ReplyCreate,
    ReplyData, ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyLseek, ReplyOpen,
    ReplyWrite, ReplyXattr, Request, TimeOrNow, WriteFlags,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

#[cfg(test)]
struct TestWriteGate {
    admitted: std::sync::mpsc::SyncSender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
static TEST_WRITE_GATE: std::sync::OnceLock<std::sync::Mutex<Option<TestWriteGate>>> =
    std::sync::OnceLock::new();

/// One-shot control for a real FUSE write paused after handle admission and
/// dirty tracking but before the authenticated mutation begins.
#[cfg(test)]
pub(super) struct TestWriteControl {
    admitted: std::sync::mpsc::Receiver<()>,
    release: std::sync::mpsc::SyncSender<()>,
}

#[cfg(test)]
impl TestWriteControl {
    pub(super) fn wait_until_admitted(&self, timeout: Duration) -> bool {
        self.admitted.recv_timeout(timeout).is_ok()
    }

    pub(super) fn release(self) -> bool {
        self.release.send(()).is_ok()
    }
}

#[cfg(test)]
pub(super) fn pause_next_write_after_admission() -> TestWriteControl {
    let (admitted_send, admitted_receive) = std::sync::mpsc::sync_channel(0);
    let (release_send, release_receive) = std::sync::mpsc::sync_channel(0);
    let gate = TEST_WRITE_GATE.get_or_init(|| std::sync::Mutex::new(None));
    *gate
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(TestWriteGate {
        admitted: admitted_send,
        release: release_receive,
    });
    TestWriteControl {
        admitted: admitted_receive,
        release: release_send,
    }
}

#[cfg(test)]
fn pause_test_write() {
    let Some(gate) = TEST_WRITE_GATE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
    else {
        return;
    };
    let _ = gate.admitted.send(());
    let _ = gate.release.recv_timeout(Duration::from_secs(10));
}

const ROOT_INODE: u64 = 1;
/// Kernel lifetime of entries, negative entries, attributes, symlink targets,
/// and retained file and directory data. Nothing here expires by time: the
/// kernel applies mounted mutations itself, [`FuseSession::invalidate`]
/// publishes source changes made around the mount, and
/// [`FuseSession::revalidate`] drops everything a source rebind superseded.
const CACHE_TTL: Duration = Duration::from_secs(24 * 60 * 60);
/// Negative entries tracked per directory. Revalidation must reach every
/// negative entry the kernel may hold, so past this bound a directory answers
/// further absent names without a cache lifetime.
const MAXIMUM_NEGATIVE_ENTRIES_PER_DIRECTORY: usize = 1024;
/// Optional kernel capabilities; each has an exact kernel fallback.
const REQUESTED_CAPABILITIES: InitFlags = InitFlags::FUSE_DO_READDIRPLUS
    .union(InitFlags::FUSE_PARALLEL_DIROPS)
    .union(InitFlags::FUSE_AUTO_INVAL_DATA)
    .union(InitFlags::FUSE_CACHE_SYMLINKS);
/// FUSE RENAME2 wire flag; equals Linux renameat2's `RENAME_NOREPLACE`.
const RENAME_NOREPLACE: u32 = 1;
// FUSE hands modes as `u32` while Darwin's `mode_t` is `u16`; widen the file
// type masks once so match arms and metadata stay wire-width on every host.
#[allow(clippy::unnecessary_cast)]
mod mode {
    pub(super) const S_IFMT: u32 = libc::S_IFMT as u32;
    pub(super) const S_IFIFO: u32 = libc::S_IFIFO as u32;
    pub(super) const S_IFSOCK: u32 = libc::S_IFSOCK as u32;
    pub(super) const S_IFCHR: u32 = libc::S_IFCHR as u32;
    pub(super) const S_IFBLK: u32 = libc::S_IFBLK as u32;
    pub(super) const S_IFDIR: u32 = libc::S_IFDIR as u32;
    pub(super) const S_IFLNK: u32 = libc::S_IFLNK as u32;
    pub(super) const S_IFREG: u32 = libc::S_IFREG as u32;
}
use mode::{S_IFBLK, S_IFCHR, S_IFDIR, S_IFIFO, S_IFLNK, S_IFMT, S_IFREG, S_IFSOCK};
const DIRECTORY_PAGE_SIZE: u32 = 256;
const ATTRIBUTE_PAGE_SIZE: u32 = 256;
const MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES: usize = 1024 * 1024;
const DETACHED_COPY_CHUNK_BYTES: u32 = 1024 * 1024;

struct DirectoryHandle {
    path: MountPath,
    parent_inode: u64,
    binding_epoch: Option<u64>,
    cursor: Option<Vec<u8>>,
    entries: VecDeque<MountDirectoryEntry>,
    /// Source view the buffered entries were read after, if cacheable.
    entries_stamp: Option<ViewStamp>,
    exhausted: bool,
    emitted: u64,
}

struct InodeEntry {
    bindings: Vec<MountPath>,
    lookup: MountLookup,
    view_stamp: Option<ViewStamp>,
    lookup_references: u64,
    open_handles: u64,
    /// Version the kernel page cache was last admitted under.
    cached_content: Option<ContentVersion>,
    /// Absent child names the kernel may hold as negative entries.
    negative_children: HashSet<Vec<u8>>,
}

impl InodeEntry {
    fn new(
        binding: MountPath,
        lookup: MountLookup,
        view_stamp: Option<ViewStamp>,
        lookup_references: u64,
    ) -> Self {
        Self {
            bindings: vec![binding],
            lookup,
            view_stamp,
            lookup_references,
            open_handles: 0,
            cached_content: None,
            negative_children: HashSet::new(),
        }
    }

    /// Whether the cached record still describes the source through its
    /// first binding.
    fn is_current(&self, source: &dyn MountFilesystem) -> bool {
        self.view_stamp.is_some_and(|stamp| {
            self.bindings.first().is_some_and(|path| {
                source.unchanged_since(path, Some(self.lookup.node.file_id), stamp)
            })
        })
    }

    /// Records the version a new handle opens and reports whether pages the
    /// kernel retained from earlier handles were admitted under it.
    fn admit_cached_content(&mut self, lookup: &MountLookup) -> bool {
        let opened = ContentVersion::of(lookup);
        self.cached_content.replace(opened) == Some(opened)
    }

    /// Records that the kernel may hold `name` as a negative entry; false
    /// when this directory already tracks its maximum.
    fn remember_negative_child(&mut self, name: &[u8]) -> bool {
        self.negative_children.contains(name)
            || (self.negative_children.len() < MAXIMUM_NEGATIVE_ENTRIES_PER_DIRECTORY
                && self.negative_children.insert(name.to_vec()))
    }
}

/// Size and times a regular file was opened with. Within the source's cache
/// contract, bytes change only through this mount, which updates the kernel
/// page cache itself, or across a rebind, which drops it; a differing version
/// also drops pages left by content changed around that contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ContentVersion {
    logical_bytes: u64,
    modified_ns: MetadataField<i64>,
    changed_ns: MetadataField<i64>,
}

impl ContentVersion {
    fn of(lookup: &MountLookup) -> Self {
        Self {
            logical_bytes: lookup.node.logical_bytes,
            modified_ns: lookup.metadata.modified_ns,
            changed_ns: lookup.metadata.changed_ns,
        }
    }
}

/// One kernel cache item a session can drop.
#[derive(Debug, Eq, PartialEq)]
enum KernelCacheItem {
    /// Attributes, file data, directory listing, and symlink target.
    Inode(u64),
    /// One positive or negative name under a directory.
    Entry { parent: u64, name: Vec<u8> },
}

struct FileHandle {
    inode: u64,
    open_file: Arc<dyn MountOpenFile>,
    operation: Arc<std::sync::Mutex<()>>,
    dirty: bool,
}

#[derive(Clone, Copy)]
enum InitialHandleState {
    Clean,
    Dirty,
}

struct FuseProjectionState {
    source: Arc<dyn MountFilesystem>,
    stopping: Arc<AtomicBool>,
    writable: bool,
    mount_uid: u32,
    mount_gid: u32,
    next_inode: u64,
    next_handle: u64,
    by_inode: HashMap<u64, InodeEntry>,
    inode_by_path: HashMap<MountPath, u64>,
    inode_by_file: HashMap<crate::FileId, u64>,
    files: HashMap<u64, FileHandle>,
    directories: HashMap<u64, DirectoryHandle>,
    /// Source binding every item the kernel caches was derived from.
    kernel_binding_epoch: Option<u64>,
}

struct FuseProjection {
    state: Arc<std::sync::Mutex<FuseProjectionState>>,
}

/// One background libfuse session.
pub(super) struct FuseSession {
    session: Option<BackgroundSession>,
    state: Arc<std::sync::Mutex<FuseProjectionState>>,
    stopping: Arc<AtomicBool>,
    shutdown_failed: bool,
}

impl FuseSession {
    pub(super) fn invalidate(&self, path: &[u8]) -> Result<(), NativeMountError> {
        let path = path.strip_prefix(b"/").unwrap_or(path);
        let mut components = path.split(|byte| *byte == b'/').peekable();
        let mut parent = MountPath::root();
        let mut name = None;
        while let Some(component) = components.next() {
            if component.is_empty()
                || component.contains(&0)
                || component == b"."
                || component == b".."
            {
                return Err(NativeMountError::Driver(
                    "FUSE invalidation requires a canonical non-root path".to_owned(),
                ));
            }
            if components.peek().is_some() {
                parent = parent.child(component.to_vec());
            } else {
                name = Some(component);
            }
        }
        let Some(name) = name else {
            return Err(NativeMountError::Driver(
                "FUSE invalidation requires a canonical non-root path".to_owned(),
            ));
        };
        let items = {
            let state = self.lock_state()?;
            let parent_inode = state.inode_by_path.get(&parent).copied().ok_or_else(|| {
                NativeMountError::Driver("FUSE invalidation parent is not cached".to_owned())
            })?;
            let file_inode = state
                .inode_by_path
                .get(&parent.child(name.to_vec()))
                .copied();
            // The parent's listing changes with the name it contains.
            file_inode
                .map(KernelCacheItem::Inode)
                .into_iter()
                .chain([
                    KernelCacheItem::Entry {
                        parent: parent_inode,
                        name: name.to_vec(),
                    },
                    KernelCacheItem::Inode(parent_inode),
                ])
                .collect::<Vec<_>>()
        };
        self.drop_kernel_caches(items)
    }

    /// Drops every kernel cache item derived from a superseded source binding.
    ///
    /// Mount owners call this after rebinding the source, such as advancing
    /// to a new head, and before exposing the rebound view. A reply computed
    /// under the old binding cannot outlive this call: replies are validated
    /// against the binding while this state is locked, and in-flight ones are
    /// ordered before these notifications by the kernel's per-directory
    /// locking, page locks, and attribute versions.
    pub(super) fn revalidate(&self) -> Result<(), NativeMountError> {
        let items = {
            let mut state = self.lock_state()?;
            let binding_epoch = state.source.binding_epoch();
            if state.kernel_binding_epoch == binding_epoch {
                return Ok(());
            }
            state.kernel_binding_epoch = binding_epoch;
            let state = &mut *state;
            drain_kernel_cache_items(&mut state.by_inode, &state.inode_by_path)
        };
        self.drop_kernel_caches(items)
    }

    fn lock_state(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, FuseProjectionState>, NativeMountError> {
        self.state
            .lock()
            .map_err(|_| NativeMountError::Driver("FUSE projection state is poisoned".to_owned()))
    }

    /// Notifies the kernel item by item without holding projection state, so
    /// callbacks that hold the kernel locks a notification waits on finish.
    /// Every item is attempted; the first failure is reported.
    fn drop_kernel_caches(
        &self,
        items: impl IntoIterator<Item = KernelCacheItem>,
    ) -> Result<(), NativeMountError> {
        let session = self
            .session
            .as_ref()
            .ok_or_else(|| NativeMountError::Driver("session is stopped".to_owned()))?;
        let notifier = session.notifier();
        let mut failure = None;
        for item in items {
            let dropped = match &item {
                KernelCacheItem::Inode(inode) => notifier.inval_inode(INodeNo(*inode), 0, 0),
                KernelCacheItem::Entry { parent, name } => {
                    notifier.inval_entry(INodeNo(*parent), OsStr::from_bytes(name))
                }
            };
            // `ENOENT` means the kernel held nothing to invalidate.
            if let Err(error) = dropped
                && error.kind() != std::io::ErrorKind::NotFound
            {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), |error| {
            Err(NativeMountError::Driver(error.to_string()))
        })
    }

    pub(super) fn start(
        request: &NativeMountRequest,
        source: Arc<dyn MountFilesystem>,
    ) -> Result<Self, NativeMountError> {
        // Epochs are sampled before the root so that a concurrent change can
        // only make them older than the facts they label, never newer.
        let kernel_binding_epoch = source.binding_epoch();
        let view_stamp = source.view_stamp();
        let root = source
            .lookup(&MountPath::root())
            .map_err(source_error)?
            .ok_or_else(|| NativeMountError::Driver("volume root is absent".to_owned()))?;
        if root.node.kind != MountNodeKind::Directory {
            return Err(NativeMountError::Driver(
                "volume root is not a directory".to_owned(),
            ));
        }
        let mut by_inode = HashMap::new();
        by_inode.insert(
            ROOT_INODE,
            InodeEntry::new(MountPath::root(), root, view_stamp, 1),
        );
        let mut inode_by_file = HashMap::new();
        inode_by_file.insert(root.node.file_id, ROOT_INODE);
        let mut inode_by_path = HashMap::new();
        inode_by_path.insert(MountPath::root(), ROOT_INODE);
        let destination_metadata = request
            .destination
            .metadata()
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        let stopping = Arc::new(AtomicBool::new(false));
        let state = FuseProjectionState {
            source,
            stopping: Arc::clone(&stopping),
            writable: request.writable,
            mount_uid: destination_metadata.uid(),
            mount_gid: destination_metadata.gid(),
            next_inode: ROOT_INODE + 1,
            next_handle: 1,
            by_inode,
            inode_by_path,
            inode_by_file,
            files: HashMap::new(),
            directories: HashMap::new(),
            kernel_binding_epoch,
        };
        let state = Arc::new(std::sync::Mutex::new(state));
        let filesystem = FuseProjection {
            state: Arc::clone(&state),
        };
        let mut options = vec![
            MountOption::FSName("acyclic-fs".to_owned()),
            MountOption::DefaultPermissions,
            MountOption::Exec,
            MountOption::NoAtime,
        ];
        if !request.writable {
            options.push(MountOption::RO);
        }
        let mut config = Config::default();
        config.mount_options = options;
        config.n_threads = Some(8);
        config.clone_fd = false;
        let session = fuser::spawn_mount(filesystem, &request.destination, &config)
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        Ok(Self {
            session: Some(session),
            state,
            stopping,
            shutdown_failed: false,
        })
    }

    pub(super) fn stop(&mut self) -> Result<(), NativeMountError> {
        if self.shutdown_failed {
            return Err(NativeMountError::Driver(
                "FUSE background session previously failed during shutdown".to_owned(),
            ));
        }
        self.stopping.store(true, Ordering::Release);
        if let Some(session) = self.session.take() {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                session.umount_and_join()
            })) {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    self.shutdown_failed = true;
                    return Err(NativeMountError::Driver(error.to_string()));
                }
                Err(_) => {
                    self.shutdown_failed = true;
                    return Err(NativeMountError::Driver(
                        "FUSE background session failed during shutdown".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }
}

impl FuseProjectionState {
    fn reject_stopping(&self) -> bool {
        self.stopping.load(Ordering::Acquire)
    }

    fn allocate_file_handle(
        &mut self,
        inode: u64,
        initial_state: InitialHandleState,
    ) -> Result<u64, i32> {
        let path = self.path(inode)?.to_owned();
        let open_file = self.source.open_file(&path).map_err(errno)?;
        let handle = self.next_handle;
        self.next_handle = self.next_handle.saturating_add(1).max(1);
        if self.files.contains_key(&handle) {
            return Err(libc::EMFILE);
        }
        self.files.try_reserve(1).map_err(|_| libc::ENOMEM)?;
        let entry = self.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        self.files.insert(
            handle,
            FileHandle {
                inode,
                open_file,
                operation: Arc::new(std::sync::Mutex::new(())),
                dirty: matches!(initial_state, InitialHandleState::Dirty),
            },
        );
        entry.open_handles = entry.open_handles.saturating_add(1);
        Ok(handle)
    }

    fn discard_file_handle(&mut self, inode: u64, handle: u64) -> Result<(), i32> {
        let Some(file) = self.files.remove(&handle) else {
            return Err(libc::ESTALE);
        };
        if file.inode != inode {
            self.files.insert(handle, file);
            return Err(libc::ESTALE);
        }
        let Some(entry) = self.by_inode.get_mut(&inode) else {
            self.files.insert(handle, file);
            return Err(libc::ESTALE);
        };
        if entry.open_handles == 0 {
            self.files.insert(handle, file);
            return Err(libc::EIO);
        }
        entry.open_handles -= 1;
        if entry.bindings.is_empty()
            && entry.lookup_references == 0
            && entry.open_handles == 0
            && let Some(entry) = self.by_inode.remove(&inode)
        {
            self.inode_by_file.remove(&entry.lookup.node.file_id);
        }
        Ok(())
    }

    fn discard_created_handle(&mut self, inode: u64, handle: u64) -> Result<(), i32> {
        self.discard_file_handle(inode, handle)?;
        self.release_lookup_reference(inode, 1);
        Ok(())
    }

    fn open_handle(&self, inode: u64, handle: u64) -> Result<Arc<dyn MountOpenFile>, i32> {
        let file = self.files.get(&handle).ok_or(libc::ESTALE)?;
        if file.inode != inode {
            return Err(libc::ESTALE);
        }
        Ok(Arc::clone(&file.open_file))
    }

    fn open_handle_operation(
        &self,
        inode: u64,
        handle: u64,
    ) -> Result<Arc<std::sync::Mutex<()>>, i32> {
        let file = self.files.get(&handle).ok_or(libc::ESTALE)?;
        if file.inode != inode {
            return Err(libc::ESTALE);
        }
        Ok(Arc::clone(&file.operation))
    }

    fn mark_handle_dirty(&mut self, inode: u64, handle: u64) -> Result<(), i32> {
        let file = self.files.get_mut(&handle).ok_or(libc::ESTALE)?;
        if file.inode != inode {
            return Err(libc::ESTALE);
        }
        file.dirty = true;
        Ok(())
    }

    fn open_inode(&self, inode: u64) -> Option<Arc<dyn MountOpenFile>> {
        self.files
            .values()
            .find(|file| file.inode == inode)
            .map(|file| Arc::clone(&file.open_file))
    }

    fn refresh_handle(&mut self, inode: u64, handle: Option<u64>) -> Result<MountLookup, i32> {
        if let Some(handle) = handle {
            let lookup = self.open_handle(inode, handle)?.lookup().map_err(errno)?;
            let entry = self.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
            entry.lookup = lookup;
            // Handle identity can intentionally outlive its namespace binding.
            // Never authorize path-cache reuse from a handle-relative lookup.
            entry.view_stamp = None;
            return Ok(lookup);
        }
        if self
            .by_inode
            .get(&inode)
            .is_some_and(|entry| entry.is_current(self.source.as_ref()))
        {
            return self
                .by_inode
                .get(&inode)
                .map(|entry| entry.lookup)
                .ok_or(libc::ESTALE);
        }
        self.refresh(inode)
    }

    fn retain_detached_handles(&mut self, inode: u64, detached: &Arc<dyn MountOpenFile>) {
        let mut assigned = 0_u64;
        for file in self.files.values_mut().filter(|file| file.inode == inode) {
            file.open_file = Arc::clone(detached);
            assigned = assigned.saturating_add(1);
        }
        let expected = self
            .by_inode
            .get(&inode)
            .map_or(0, |entry| entry.open_handles);
        debug_assert_eq!(assigned, expected);
    }

    #[allow(clippy::too_many_arguments)]
    fn copy_open_range(
        &mut self,
        source_inode: u64,
        source_handle: u64,
        source_offset: u64,
        destination_inode: u64,
        destination_handle: u64,
        destination_offset: u64,
        length: u64,
    ) -> Result<u32, i32> {
        let source_open = self.open_handle(source_inode, source_handle)?;
        let destination_open = self.open_handle(destination_inode, destination_handle)?;
        let source_lookup = source_open.lookup().map_err(errno)?;
        let destination_lookup = destination_open.lookup().map_err(errno)?;
        let source_size = source_lookup.node.logical_bytes;
        let copy_length = length.min(source_size.saturating_sub(source_offset));
        if copy_length == 0 {
            return Ok(0);
        }
        if source_lookup.node.link_count != 0 && destination_lookup.node.link_count != 0 {
            match self.source.clone_range_by_id(
                source_lookup.node.file_id,
                source_offset,
                destination_lookup.node.file_id,
                destination_offset,
                copy_length,
            ) {
                Ok(()) => {
                    let _ = self.refresh(destination_inode);
                    return u32::try_from(copy_length).map_err(|_| libc::EOVERFLOW);
                }
                Err(MountSourceError::Unsupported(_)) => {}
                Err(error) => return Err(errno(error)),
            }
        }
        let same_open_file = Arc::ptr_eq(&source_open, &destination_open);
        let backwards = same_open_file
            && destination_offset > source_offset
            && destination_offset < source_offset.saturating_add(copy_length);
        let mut transferred = 0_u64;
        while transferred < copy_length {
            let remaining = copy_length - transferred;
            let chunk = remaining.min(u64::from(DETACHED_COPY_CHUNK_BYTES));
            let relative = if backwards {
                remaining - chunk
            } else {
                transferred
            };
            let chunk = u32::try_from(chunk).unwrap_or(DETACHED_COPY_CHUNK_BYTES);
            let sparse = match source_open.read_sparse_range(source_offset + relative, chunk) {
                Ok(sparse) => sparse,
                Err(error) if transferred == 0 => return Err(errno(error)),
                Err(_) => break,
            };
            let copied = sparse.logical_bytes;
            if let Err(error) = destination_open
                .write_sparse_range(destination_offset + relative, &sparse)
                .map_err(errno)
            {
                if transferred == 0 {
                    return Err(error);
                }
                break;
            }
            transferred = transferred.saturating_add(copied);
            if copied < u64::from(chunk) {
                break;
            }
        }
        let _ = self.refresh_handle(destination_inode, Some(destination_handle));
        u32::try_from(transferred).map_err(|_| libc::EOVERFLOW)
    }

    fn path(&self, inode: u64) -> Result<&MountPath, i32> {
        self.by_inode
            .get(&inode)
            .and_then(|entry| entry.bindings.first())
            .ok_or(libc::ESTALE)
    }

    fn node(&self, inode: u64) -> Result<MountNode, i32> {
        self.by_inode
            .get(&inode)
            .map(|entry| entry.lookup.node)
            .ok_or(libc::ESTALE)
    }

    fn child_path(&self, parent: u64, name: &OsStr) -> Result<MountPath, i32> {
        let parent = self.path(parent)?;
        let name = name.as_bytes();
        if name.is_empty() || name == b"." || name == b".." || name.contains(&b'/') {
            return Err(libc::EINVAL);
        }
        Ok(parent.child(name.to_vec()))
    }

    fn intern_with_reference(
        &mut self,
        path: MountPath,
        lookup: &MountLookup,
        view_stamp: Option<ViewStamp>,
        lookup_reference: bool,
    ) -> Result<u64, i32> {
        if let Some(previous) = self.inode_by_path.get(&path).copied()
            && self
                .by_inode
                .get(&previous)
                .is_some_and(|entry| entry.lookup.node.file_id != lookup.node.file_id)
        {
            self.remove_binding(previous, &path);
        }
        intern_projected(
            &mut self.next_inode,
            &mut self.by_inode,
            &mut self.inode_by_path,
            &mut self.inode_by_file,
            path,
            lookup,
            view_stamp,
            lookup_reference,
        )
    }

    fn intern(&mut self, path: MountPath, lookup: &MountLookup) -> Result<u64, i32> {
        let view_stamp = self.source.view_stamp();
        self.intern_with_reference(path, lookup, view_stamp, true)
    }

    fn release_lookup_reference(&mut self, inode: u64, references: u64) {
        if inode == ROOT_INODE {
            return;
        }
        let remove = if let Some(entry) = self.by_inode.get_mut(&inode) {
            entry.lookup_references = entry.lookup_references.saturating_sub(references);
            entry.lookup_references == 0 && entry.open_handles == 0
        } else {
            false
        };
        if remove && let Some(entry) = self.by_inode.remove(&inode) {
            for path in &entry.bindings {
                if self.inode_by_path.get(path) == Some(&inode) {
                    self.inode_by_path.remove(path);
                }
            }
            self.inode_by_file.remove(&entry.lookup.node.file_id);
        }
    }

    fn cached_lookup(&mut self, path: &MountPath) -> Option<(u64, MountLookup)> {
        let source = Arc::clone(&self.source);
        cached_projected_lookup(
            &mut self.by_inode,
            &self.inode_by_path,
            path,
            |file_id, stamp| source.unchanged_since(path, Some(file_id), stamp),
        )
    }

    fn coherent_lookup(&self, path: &MountPath) -> Result<CoherentSourceLookup, i32> {
        coherent_source_lookup(&self.source, path)
    }

    fn refresh(&mut self, inode: u64) -> Result<MountLookup, i32> {
        if inode == ROOT_INODE {
            let refreshed = self.coherent_lookup(&MountPath::root())?;
            let lookup = refreshed.lookup.ok_or(libc::ENOENT)?;
            if lookup.node.kind != MountNodeKind::Directory {
                return Err(libc::EIO);
            }
            replace_root_lookup(
                &mut self.by_inode,
                &mut self.inode_by_file,
                lookup,
                refreshed.cache_stamp,
            )?;
            return Ok(lookup);
        }
        let (file_id, bindings) = self
            .by_inode
            .get(&inode)
            .map(|entry| (entry.lookup.node.file_id, entry.bindings.clone()))
            .ok_or(libc::ESTALE)?;
        for path in bindings {
            let refreshed = self.coherent_lookup(&path)?;
            if let Some(lookup) = refreshed.lookup {
                if lookup.node.file_id != file_id {
                    continue;
                }
                let entry = self.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
                entry.lookup = lookup;
                entry.view_stamp = refreshed.cache_stamp;
                return Ok(lookup);
            }
            self.remove_binding(inode, &path);
        }
        Err(libc::ENOENT)
    }

    /// Attributes of a known inode from its most recent lookup.
    fn node_attr(&self, inode: u64) -> Result<FileAttr, i32> {
        let lookup = self.by_inode.get(&inode).ok_or(libc::ESTALE)?.lookup;
        self.attr(inode, &lookup)
    }

    fn attr(&self, inode: u64, lookup: &MountLookup) -> Result<FileAttr, i32> {
        let node = lookup.node;
        let kind = match node.kind {
            MountNodeKind::Regular => FileType::RegularFile,
            MountNodeKind::Directory => FileType::Directory,
            MountNodeKind::SymbolicLink => FileType::Symlink,
            MountNodeKind::Fifo => FileType::NamedPipe,
            MountNodeKind::Socket => FileType::Socket,
            MountNodeKind::CharacterDevice => FileType::CharDevice,
            MountNodeKind::BlockDevice => FileType::BlockDevice,
            MountNodeKind::Unsupported => return Err(libc::EOPNOTSUPP),
        };
        let default_mode = if self.writable { 0o755 } else { 0o555 };
        Ok(FileAttr {
            ino: INodeNo(inode),
            size: node.logical_bytes,
            blocks: node.logical_bytes.div_ceil(512),
            atime: metadata_time(lookup.metadata.accessed_ns),
            mtime: metadata_time(lookup.metadata.modified_ns),
            ctime: metadata_time(lookup.metadata.changed_ns),
            crtime: metadata_time(lookup.metadata.created_ns),
            kind,
            perm: u16::try_from(metadata_or(lookup.metadata.posix_mode, default_mode) & 0o7777)
                .unwrap_or(0o7777),
            nlink: u32::try_from(node.link_count).unwrap_or(u32::MAX),
            uid: metadata_or(lookup.metadata.posix_uid, self.mount_uid),
            gid: metadata_or(lookup.metadata.posix_gid, self.mount_gid),
            rdev: node
                .device
                .map(|(major, minor)| native_device_number(major, minor))
                .transpose()?
                .unwrap_or(0),
            blksize: 4096,
            flags: u32::try_from(metadata_or(lookup.metadata.posix_flags, 0)).unwrap_or(u32::MAX),
        })
    }

    fn admit_write(&self) -> Result<(), i32> {
        self.writable.then_some(()).ok_or(libc::EROFS)
    }

    /// Kernel flags for a new handle on `inode` that opened `lookup`.
    ///
    /// Pages retained from earlier handles are kept only while the file opens
    /// with the [`ContentVersion`] they were admitted under. Closing a handle
    /// is a publication boundary only when it may write and the source
    /// publishes on close; every other close needs no FLUSH round trip.
    fn file_open_flags(&mut self, inode: u64, lookup: &MountLookup, flags: i32) -> FopenFlags {
        let retains_content = self
            .by_inode
            .get_mut(&inode)
            .is_some_and(|entry| entry.admit_cached_content(lookup));
        let flushes_on_close =
            flags & libc::O_ACCMODE != libc::O_RDONLY && self.source.flush_on_handle_close();
        let mut open_flags = FopenFlags::empty();
        open_flags.set(FopenFlags::FOPEN_KEEP_CACHE, retains_content);
        open_flags.set(FopenFlags::FOPEN_NOFLUSH, !flushes_on_close);
        open_flags
    }

    fn remove_path_cache(&mut self, path: &MountPath) {
        let affected = self
            .by_inode
            .iter()
            .filter_map(|(inode, entry)| entry.bindings.contains(path).then_some(*inode))
            .collect::<Vec<_>>();
        for inode in affected {
            self.remove_binding(inode, path);
        }
    }

    fn remove_binding(&mut self, inode: u64, path: &MountPath) {
        if self.inode_by_path.get(path) == Some(&inode) {
            self.inode_by_path.remove(path);
        }
        let remove_inode = if let Some(entry) = self.by_inode.get_mut(&inode) {
            entry.bindings.retain(|candidate| candidate != path);
            entry.bindings.is_empty() && entry.lookup_references == 0 && entry.open_handles == 0
        } else {
            false
        };
        if remove_inode && let Some(entry) = self.by_inode.remove(&inode) {
            self.inode_by_file.remove(&entry.lookup.node.file_id);
        }
    }

    fn invalidate_prefix(&mut self, prefix: &MountPath) {
        let affected = self
            .by_inode
            .iter()
            .flat_map(|(inode, entry)| {
                entry
                    .bindings
                    .iter()
                    .filter(|path| has_prefix(path, prefix))
                    .cloned()
                    .map(|path| (*inode, path))
            })
            .collect::<Vec<_>>();
        for (inode, path) in affected {
            self.remove_binding(inode, &path);
        }
    }

    fn rename_prefix(&mut self, source: &MountPath, destination: &MountPath) {
        for entry in self.by_inode.values_mut() {
            for binding in &mut entry.bindings {
                if let Some(rebased) = replace_prefix(binding, source, destination) {
                    *binding = rebased;
                }
            }
        }
        for directory in self.directories.values_mut() {
            if let Some(rebased) = replace_prefix(&directory.path, source, destination) {
                directory.path = rebased;
            }
        }
        self.inode_by_path.clear();
        for (inode, entry) in &self.by_inode {
            for binding in &entry.bindings {
                self.inode_by_path.insert(binding.clone(), *inode);
            }
        }
    }
}

/// Every kernel cache item a projection may have handed out: each known
/// inode, each name bound under a known directory, and each tracked negative
/// name, which stops being tracked.
fn drain_kernel_cache_items(
    by_inode: &mut HashMap<u64, InodeEntry>,
    inode_by_path: &HashMap<MountPath, u64>,
) -> Vec<KernelCacheItem> {
    let mut items = Vec::new();
    for (inode, entry) in by_inode {
        items.push(KernelCacheItem::Inode(*inode));
        for binding in &entry.bindings {
            if let Some((parent, name)) = split_parent(binding)
                && let Some(parent) = inode_by_path.get(&parent)
            {
                items.push(KernelCacheItem::Entry {
                    parent: *parent,
                    name: name.to_vec(),
                });
            }
        }
        items.extend(
            entry
                .negative_children
                .drain()
                .map(|name| KernelCacheItem::Entry {
                    parent: *inode,
                    name,
                }),
        );
    }
    items
}

fn cached_projected_lookup(
    by_inode: &mut HashMap<u64, InodeEntry>,
    inode_by_path: &HashMap<MountPath, u64>,
    path: &MountPath,
    unchanged_since: impl FnOnce(crate::FileId, ViewStamp) -> bool,
) -> Option<(u64, MountLookup)> {
    let inode = inode_by_path.get(path).copied()?;
    let entry = by_inode.get_mut(&inode)?;
    let stamp = entry.view_stamp?;
    if !unchanged_since(entry.lookup.node.file_id, stamp) {
        return None;
    }
    entry.lookup_references = entry.lookup_references.saturating_add(1);
    Some((inode, entry.lookup))
}

fn replace_root_lookup(
    by_inode: &mut HashMap<u64, InodeEntry>,
    inode_by_file: &mut HashMap<crate::FileId, u64>,
    lookup: MountLookup,
    view_stamp: Option<ViewStamp>,
) -> Result<(), i32> {
    let previous = by_inode
        .get(&ROOT_INODE)
        .ok_or(libc::ESTALE)?
        .lookup
        .node
        .file_id;
    if previous != lookup.node.file_id {
        if inode_by_file
            .get(&lookup.node.file_id)
            .is_some_and(|inode| *inode != ROOT_INODE)
        {
            return Err(libc::EIO);
        }
        if inode_by_file.get(&previous) == Some(&ROOT_INODE) {
            inode_by_file.remove(&previous);
        }
        inode_by_file.insert(lookup.node.file_id, ROOT_INODE);
    }
    let root = by_inode.get_mut(&ROOT_INODE).ok_or(libc::ESTALE)?;
    root.lookup = lookup;
    root.view_stamp = view_stamp;
    Ok(())
}

#[allow(clippy::too_many_arguments)] // Projection indexes are updated together.
fn intern_projected(
    next_inode: &mut u64,
    by_inode: &mut HashMap<u64, InodeEntry>,
    inode_by_path: &mut HashMap<MountPath, u64>,
    inode_by_file: &mut HashMap<crate::FileId, u64>,
    path: MountPath,
    lookup: &MountLookup,
    view_stamp: Option<ViewStamp>,
    lookup_reference: bool,
) -> Result<u64, i32> {
    if let Some(inode) = inode_by_file.get(&lookup.node.file_id).copied() {
        let entry = by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        if !entry.bindings.contains(&path) {
            if u64::try_from(entry.bindings.len()).unwrap_or(u64::MAX) >= lookup.node.link_count {
                return Err(libc::EIO);
            }
            entry.bindings.try_reserve(1).map_err(|_| libc::ENOMEM)?;
            entry.bindings.push(path.clone());
        }
        entry.lookup = *lookup;
        entry.view_stamp = view_stamp;
        if lookup_reference {
            entry.lookup_references = entry.lookup_references.saturating_add(1);
        }
        inode_by_path.insert(path, inode);
        return Ok(inode);
    }
    let inode = *next_inode;
    if inode <= ROOT_INODE {
        return Err(libc::EOVERFLOW);
    }
    let following = inode.checked_add(1).ok_or(libc::EOVERFLOW)?;
    *next_inode = following;
    inode_by_file.insert(lookup.node.file_id, inode);
    inode_by_path.insert(path.clone(), inode);
    by_inode.insert(
        inode,
        InodeEntry::new(path, *lookup, view_stamp, u64::from(lookup_reference)),
    );
    Ok(inode)
}

macro_rules! reject_stopping {
    ($filesystem:expr, $reply:expr) => {
        if $filesystem.reject_stopping() {
            return $reply.error(Errno::from_i32(libc::ENODEV));
        }
    };
}

macro_rules! with_fuse_state {
    ($filesystem:expr, $reply:ident, $method:ident($($argument:expr),* $(,)?)) => {
        match $filesystem.state.lock() {
            Ok(mut state) => state.$method($($argument,)* $reply),
            Err(_) => $reply.error(Errno::from_i32(libc::EIO)),
        }
    };
}

/// One kernel directory listing reply. `READDIRPLUS` also instantiates every
/// listed child, which the kernel counts as one lookup of it.
trait DirectoryListing {
    const COUNTS_LOOKUPS: bool;

    /// Adds one entry; true when the reply is full and nothing was added.
    fn push(&mut self, offset: u64, name: &OsStr, attr: &FileAttr, ttl: &Duration) -> bool;
    fn ok(self);
    fn error(self, error: Errno);
}

impl DirectoryListing for ReplyDirectory {
    const COUNTS_LOOKUPS: bool = false;

    fn push(&mut self, offset: u64, name: &OsStr, attr: &FileAttr, _ttl: &Duration) -> bool {
        self.add(attr.ino, offset, attr.kind, name)
    }

    fn ok(self) {
        Self::ok(self);
    }

    fn error(self, error: Errno) {
        Self::error(self, error);
    }
}

impl DirectoryListing for ReplyDirectoryPlus {
    const COUNTS_LOOKUPS: bool = true;

    fn push(&mut self, offset: u64, name: &OsStr, attr: &FileAttr, ttl: &Duration) -> bool {
        self.add(attr.ino, offset, name, ttl, attr, Generation(0))
    }

    fn ok(self) {
        Self::ok(self);
    }

    fn error(self, error: Errno) {
        Self::error(self, error);
    }
}

struct CoherentSourceLookup {
    lookup: Option<MountLookup>,
    cache_stamp: Option<ViewStamp>,
    binding_epoch: Option<u64>,
}

fn coherent_source_lookup(
    source: &Arc<dyn MountFilesystem>,
    path: &MountPath,
) -> Result<CoherentSourceLookup, i32> {
    let binding_epoch = source.binding_epoch();
    let lease = source.acquire_binding_lease(binding_epoch).map_err(errno)?;
    // Sampled first: a concurrent native mutation of anything this lookup
    // read records a later change, so the result can never validate over it.
    let cache_stamp = source.view_stamp();
    let lookup = source.lookup(path).map_err(errno)?;
    if !source.view_is_stable() || source.binding_epoch() != binding_epoch {
        return Err(libc::ESTALE);
    }
    // Never carry a view lease into the projection-state lock: a writer may
    // hold that lock while waiting for the view gate. Revalidate below after
    // taking the state lock instead.
    drop(lease);
    Ok(CoherentSourceLookup {
        lookup,
        cache_stamp,
        binding_epoch,
    })
}

#[allow(clippy::manual_let_else)] // Reply errors stay explicit at callback boundaries.
impl FuseProjection {
    fn lookup_parallel(&self, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let (source, path) = {
            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(_) => return reply.error(Errno::EIO),
            };
            if state.reject_stopping() {
                return reply.error(Errno::ENODEV);
            }
            let path = match state.child_path(parent, name) {
                Ok(path) => path,
                Err(error) => return reply.error(Errno::from_i32(error)),
            };
            if let Some((inode, lookup)) = state.cached_lookup(&path) {
                return match state.attr(inode, &lookup) {
                    Ok(attr) => reply.entry(&CACHE_TTL, &attr, Generation(0)),
                    Err(error) => reply.error(Errno::from_i32(error)),
                };
            }
            (Arc::clone(&state.source), path)
        };

        let lookup = coherent_source_lookup(&source, &path);
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return reply.error(Errno::EIO),
        };
        let _binding_lease = match &lookup {
            Ok(refreshed) => match source.acquire_binding_lease(refreshed.binding_epoch) {
                Ok(lease) => Some(lease),
                Err(error) => return reply.error(Errno::from_i32(errno(error))),
            },
            Err(_) => None,
        };
        if state.reject_stopping() {
            return reply.error(Errno::ENODEV);
        }
        match lookup {
            Ok(refreshed) if refreshed.lookup.is_some() => {
                let lookup = refreshed.lookup.unwrap_or_else(|| unreachable!());
                let inode =
                    match state.intern_with_reference(path, &lookup, refreshed.cache_stamp, true) {
                        Ok(inode) => inode,
                        Err(error) => return reply.error(Errno::from_i32(error)),
                    };
                match state.attr(inode, &lookup) {
                    Ok(attr) => reply.entry(&CACHE_TTL, &attr, Generation(0)),
                    Err(error) => reply.error(Errno::from_i32(error)),
                }
            }
            Ok(refreshed)
                if refreshed.lookup.is_none()
                    && refreshed
                        .cache_stamp
                        .is_some_and(|stamp| source.unchanged_since(&path, None, stamp)) =>
            {
                state.remove_path_cache(&path);
                // A zero-inode LOOKUP response carries a negative-dentry TTL.
                // Plain ENOENT has no cache lifetime and makes compiler probes
                // traverse the same absent dependency paths thousands of times.
                // Only tracked negatives are cacheable: revalidation must
                // reach every one the kernel holds.
                let tracked = state
                    .by_inode
                    .get_mut(&parent)
                    .is_some_and(|entry| entry.remember_negative_child(name.as_bytes()));
                if !tracked {
                    return reply.error(Errno::ENOENT);
                }
                let Ok(mut attr) = state.node_attr(ROOT_INODE) else {
                    return reply.error(Errno::ESTALE);
                };
                attr.ino = INodeNo(0);
                reply.entry(&CACHE_TTL, &attr, Generation(0));
            }
            Ok(_) => reply.error(Errno::ENOENT),
            Err(error) => reply.error(Errno::from_i32(error)),
        }
    }

    #[allow(clippy::too_many_lines)] // Keep the handle/path refresh decision in one callback.
    fn getattr_parallel(&self, inode: u64, handle: Option<u64>, reply: ReplyAttr) {
        enum Refresh {
            Handle(Arc<dyn MountOpenFile>),
            Paths {
                source: Arc<dyn MountFilesystem>,
                file_id: crate::FileId,
                paths: Vec<MountPath>,
            },
        }
        let refresh = {
            let state = match self.state.lock() {
                Ok(state) => state,
                Err(_) => return reply.error(Errno::EIO),
            };
            if state.reject_stopping() {
                return reply.error(Errno::ENODEV);
            }
            if let Some(handle) = handle {
                match state.open_handle(inode, handle) {
                    Ok(open) => Refresh::Handle(open),
                    Err(error) => return reply.error(Errno::from_i32(error)),
                }
            } else if state
                .by_inode
                .get(&inode)
                .is_some_and(|entry| entry.bindings.is_empty() && entry.open_handles != 0)
            {
                match state.open_inode(inode) {
                    Some(open) => Refresh::Handle(open),
                    None => return reply.error(Errno::ESTALE),
                }
            } else {
                if state
                    .by_inode
                    .get(&inode)
                    .is_some_and(|entry| entry.is_current(state.source.as_ref()))
                {
                    return match state.node_attr(inode) {
                        Ok(attr) => reply.attr(&CACHE_TTL, &attr),
                        Err(_) => reply.error(Errno::ESTALE),
                    };
                }
                let Some(entry) = state.by_inode.get(&inode) else {
                    return reply.error(Errno::ESTALE);
                };
                Refresh::Paths {
                    source: Arc::clone(&state.source),
                    file_id: entry.lookup.node.file_id,
                    paths: entry.bindings.clone(),
                }
            }
        };

        let refreshed = match &refresh {
            Refresh::Handle(open) => open
                .lookup()
                .map(|lookup| (lookup, None, None))
                .map_err(errno),
            Refresh::Paths {
                source,
                file_id,
                paths,
            } => {
                let mut found = None;
                for path in paths {
                    match coherent_source_lookup(source, path) {
                        Ok(result)
                            if result
                                .lookup
                                .is_some_and(|lookup| lookup.node.file_id == *file_id) =>
                        {
                            found = result.lookup.map(|lookup| {
                                (lookup, result.cache_stamp, Some(result.binding_epoch))
                            });
                            break;
                        }
                        Ok(_) => {}
                        Err(error) => return reply.error(Errno::from_i32(error)),
                    }
                }
                found.ok_or(libc::ENOENT)
            }
        };
        let (lookup, view_stamp, binding_epoch) = match refreshed {
            Ok(value) => value,
            Err(error) => return reply.error(Errno::from_i32(error)),
        };
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return reply.error(Errno::EIO),
        };
        let _binding_lease =
            if let (Refresh::Paths { source, .. }, Some(epoch)) = (&refresh, binding_epoch) {
                match source.acquire_binding_lease(epoch) {
                    Ok(lease) => Some(lease),
                    Err(error) => return reply.error(Errno::from_i32(errno(error))),
                }
            } else {
                None
            };
        if state.reject_stopping() {
            return reply.error(Errno::ENODEV);
        }
        let Some(entry) = state.by_inode.get_mut(&inode) else {
            return reply.error(Errno::ESTALE);
        };
        if entry.lookup.node.file_id != lookup.node.file_id {
            return reply.error(Errno::ESTALE);
        }
        entry.lookup = lookup;
        entry.view_stamp = view_stamp;
        match state.attr(inode, &lookup) {
            Ok(attr) => reply.attr(&CACHE_TTL, &attr),
            Err(error) => reply.error(Errno::from_i32(error)),
        }
    }

    fn open_parallel(&self, inode: u64, flags: i32, reply: ReplyOpen) {
        let (source, path, file_id, stamp_before, binding_before) = {
            let state = match self.state.lock() {
                Ok(state) => state,
                Err(_) => return reply.error(Errno::EIO),
            };
            if state.reject_stopping() {
                return reply.error(Errno::ENODEV);
            }
            if let Err(error) = admit_open(state.writable, flags) {
                return reply.error(Errno::from_i32(error));
            }
            let file_id = match state.node(inode) {
                Ok(node) if node.kind == MountNodeKind::Regular => node.file_id,
                Ok(_) => return reply.error(Errno::EISDIR),
                Err(error) => return reply.error(Errno::from_i32(error)),
            };
            let path = match state.path(inode) {
                Ok(path) => path.to_owned(),
                Err(error) => return reply.error(Errno::from_i32(error)),
            };
            (
                Arc::clone(&state.source),
                path,
                file_id,
                state.source.view_stamp(),
                state.source.binding_epoch(),
            )
        };
        let open_file = match source.open_file(&path) {
            Ok(open_file) => open_file,
            Err(error) => return reply.error(Errno::from_i32(errno(error))),
        };
        // The inode may have outlived its path binding. Check the attached
        // file before O_TRUNC can mutate a replacement at that path.
        let opened_lookup = match open_file.lookup() {
            Ok(lookup) if lookup.node.file_id == file_id => lookup,
            _ => return reply.error(Errno::ESTALE),
        };
        let truncated = flags & libc::O_TRUNC != 0;
        if truncated && let Err(error) = open_file.resize(0) {
            return reply.error(Errno::from_i32(errno(error)));
        }
        // O_TRUNC (and a lazy-file promotion during open) legitimately
        // changes the node. Validate the attached handle instead of treating
        // that authored mutation as an external rebind.
        let unchanged = !truncated
            && stamp_before
                .is_some_and(|stamp| source.unchanged_since(&path, Some(file_id), stamp));
        let (refreshed, view_stamp) = if unchanged {
            (opened_lookup, stamp_before)
        } else {
            let stamp = source.view_stamp();
            match open_file.lookup() {
                Ok(lookup) if lookup.node.file_id == file_id => (lookup, stamp),
                _ => return reply.error(Errno::ESTALE),
            }
        };
        // Lazy promotion and O_TRUNC may write while opening, so pin the
        // external binding only after those operations have completed.
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return reply.error(Errno::EIO),
        };
        let _binding_lease = match source.acquire_binding_lease(binding_before) {
            Ok(lease) => lease,
            Err(error) => return reply.error(Errno::from_i32(errno(error))),
        };
        if state.reject_stopping() || state.path(inode).ok() != Some(&path) {
            return reply.error(Errno::ESTALE);
        }
        let handle = state.next_handle;
        if state.files.contains_key(&handle) {
            return reply.error(Errno::EMFILE);
        }
        if state.files.try_reserve(1).is_err() {
            return reply.error(Errno::ENOMEM);
        }
        let Some(entry) = state.by_inode.get_mut(&inode) else {
            return reply.error(Errno::ESTALE);
        };
        if entry.lookup.node.file_id != file_id {
            return reply.error(Errno::ESTALE);
        }
        entry.lookup = refreshed;
        entry.view_stamp = view_stamp;
        entry.open_handles = entry.open_handles.saturating_add(1);
        let open_flags = state.file_open_flags(inode, &refreshed, flags);
        state.next_handle = state.next_handle.saturating_add(1).max(1);
        state.files.insert(
            handle,
            FileHandle {
                inode,
                open_file,
                operation: Arc::new(std::sync::Mutex::new(())),
                dirty: truncated,
            },
        );
        reply.opened(FuseFileHandle(handle), open_flags);
    }

    fn read_parallel(&self, inode: u64, handle: u64, offset: u64, size: u32, reply: ReplyData) {
        let (open_file, stopping) = {
            let state = match self.state.lock() {
                Ok(state) => state,
                Err(_) => return reply.error(Errno::EIO),
            };
            if state.reject_stopping() {
                return reply.error(Errno::ENODEV);
            }
            let open_file = match state.open_handle(inode, handle) {
                Ok(open_file) => open_file,
                Err(error) => return reply.error(Errno::from_i32(error)),
            };
            (open_file, Arc::clone(&state.stopping))
        };
        let bytes = match open_file.read_up_to(offset, size) {
            Ok(bytes) => bytes,
            Err(error) => return reply.error(Errno::from_i32(errno(error))),
        };
        if stopping.load(Ordering::Acquire) {
            reply.error(Errno::ENODEV);
        } else {
            reply.data(&bytes);
        }
    }

    fn write_parallel(&self, inode: u64, handle: u64, offset: u64, data: &[u8], reply: ReplyWrite) {
        let operation = {
            let state = match self.state.lock() {
                Ok(state) => state,
                Err(_) => return reply.error(Errno::EIO),
            };
            if state.reject_stopping() {
                return reply.error(Errno::ENODEV);
            }
            if let Err(error) = state.admit_write() {
                return reply.error(Errno::from_i32(error));
            }
            match state.open_handle_operation(inode, handle) {
                Ok(operation) => operation,
                Err(error) => return reply.error(Errno::from_i32(error)),
            }
        };
        let _operation = match operation.lock() {
            Ok(operation) => operation,
            Err(_) => return reply.error(Errno::EIO),
        };
        let open_file = {
            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(_) => return reply.error(Errno::EIO),
            };
            if state.reject_stopping() {
                return reply.error(Errno::ENODEV);
            }
            let Some(file) = state
                .files
                .get_mut(&handle)
                .filter(|file| file.inode == inode && Arc::ptr_eq(&file.operation, &operation))
            else {
                return reply.error(Errno::ESTALE);
            };
            file.dirty = true;
            let open_file = Arc::clone(&file.open_file);
            if let Some(entry) = state.by_inode.get_mut(&inode) {
                // The write advances the source view. Until the handle-relative
                // refresh below completes, no namespace lookup may reuse this
                // cached record.
                entry.view_stamp = None;
            }
            open_file
        };

        #[cfg(test)]
        pause_test_write();

        let length = u32::try_from(data.len()).unwrap_or(u32::MAX);
        if let Err(error) = open_file.write_range(offset, Bytes::copy_from_slice(data)) {
            return reply.error(Errno::from_i32(errno(error)));
        }
        let refreshed = open_file.lookup().ok();
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return reply.error(Errno::EIO),
        };
        let current = state.files.get(&handle).is_some_and(|file| {
            file.inode == inode
                && Arc::ptr_eq(&file.operation, &operation)
                && Arc::ptr_eq(&file.open_file, &open_file)
        });
        if current
            && let Some(lookup) = refreshed
            && let Some(entry) = state.by_inode.get_mut(&inode)
            && entry.lookup.node.file_id == lookup.node.file_id
        {
            entry.lookup = lookup;
        }
        reply.written(length);
    }

    fn flush_parallel(
        &self,
        inode: u64,
        handle: u64,
        force: bool,
        release: bool,
        reply: ReplyEmpty,
    ) {
        let operation = {
            let state = match self.state.lock() {
                Ok(state) => state,
                Err(_) => return reply.error(Errno::EIO),
            };
            match state.open_handle_operation(inode, handle) {
                Ok(operation) => operation,
                Err(error) => return reply.error(Errno::from_i32(error)),
            }
        };
        let _operation = match operation.lock() {
            Ok(operation) => operation,
            Err(_) => return reply.error(Errno::EIO),
        };
        let (source, should_flush) = {
            let state = match self.state.lock() {
                Ok(state) => state,
                Err(_) => return reply.error(Errno::EIO),
            };
            if state.reject_stopping() {
                return reply.error(Errno::ENODEV);
            }
            let Some(file) = state.files.get(&handle) else {
                return reply.error(Errno::ESTALE);
            };
            if file.inode != inode || !Arc::ptr_eq(&file.operation, &operation) {
                return reply.error(Errno::ESTALE);
            }
            // fsync is an explicit durability request for the file, including
            // writes made through other descriptors. A close only flushes when
            // this handle was dirty and the source requires it.
            (
                Arc::clone(&state.source),
                force || (file.dirty && state.source.flush_on_handle_close()),
            )
        };
        // The per-handle operation gate remains held, but unrelated FUSE
        // callbacks must not wait on the global state mutex during durable IO.
        let flushed = if should_flush {
            source.flush().map_err(errno)
        } else {
            Ok(())
        };
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return reply.error(Errno::EIO),
        };
        let current = state
            .files
            .get(&handle)
            .is_some_and(|file| file.inode == inode && Arc::ptr_eq(&file.operation, &operation));
        if !current {
            return reply.error(Errno::ESTALE);
        }
        if flushed.is_ok()
            && let Some(file) = state.files.get_mut(&handle)
        {
            file.dirty = false;
        }
        let result = if release {
            let discarded = state.discard_file_handle(inode, handle);
            flushed.and(discarded)
        } else {
            flushed
        };
        match result {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(Errno::from_i32(error)),
        }
    }

    fn list_directory_parallel<L: DirectoryListing>(
        &self,
        inode: u64,
        handle: u64,
        offset: u64,
        listing: L,
    ) {
        if let Err(error) = self.buffer_directory_page(handle) {
            return listing.error(Errno::from_i32(error));
        }
        with_fuse_state!(self, listing, list_directory(inode, handle, offset));
    }

    /// Buffers source entries for an open directory handle until it holds
    /// some or its listing is exhausted. Source paging dominates listing
    /// cost, so it runs outside the projection lock; a lease pins the
    /// handle's binding instead, and the page is labelled with the view it
    /// was read in.
    fn buffer_directory_page(&self, handle: u64) -> Result<(), i32> {
        loop {
            let (source, path, cursor, binding_epoch) = {
                let state = self.state.lock().map_err(|_| libc::EIO)?;
                if state.reject_stopping() {
                    return Err(libc::ENODEV);
                }
                let directory = state.directories.get(&handle).ok_or(libc::ESTALE)?;
                if !directory.entries.is_empty() || directory.exhausted {
                    return Ok(());
                }
                (
                    Arc::clone(&state.source),
                    directory.path.clone(),
                    directory.cursor.clone(),
                    directory.binding_epoch,
                )
            };
            let (page, entries_stamp) = {
                let _lease = source
                    .acquire_binding_lease(binding_epoch)
                    .map_err(|_| libc::ESTALE)?;
                let stamp = source.view_stamp();
                let page = source
                    .read_directory(&path, cursor.as_deref(), DIRECTORY_PAGE_SIZE)
                    .map_err(errno)?;
                if !source.view_is_stable() || source.binding_epoch() != binding_epoch {
                    return Err(libc::ESTALE);
                }
                (page, stamp)
            };
            let mut state = self.state.lock().map_err(|_| libc::EIO)?;
            let directory = state.directories.get_mut(&handle).ok_or(libc::ESTALE)?;
            // Only the page after the handle's current position may land.
            if directory.cursor == cursor && directory.entries.is_empty() && !directory.exhausted {
                directory.exhausted = page.next_cursor.is_none();
                directory.cursor = page.next_cursor;
                directory.entries.extend(page.entries);
                directory.entries_stamp = entries_stamp;
            }
        }
    }

    fn fsyncdir_parallel(&self, handle: u64, reply: ReplyEmpty) {
        let source = {
            let state = match self.state.lock() {
                Ok(state) => state,
                Err(_) => return reply.error(Errno::EIO),
            };
            if state.reject_stopping() {
                return reply.error(Errno::ENODEV);
            }
            if !state.directories.contains_key(&handle) {
                return reply.error(Errno::ESTALE);
            }
            Arc::clone(&state.source)
        };
        // Directory fsync is an explicit durability boundary too; never hold
        // the projection's global state mutex across SDK publication.
        match source.flush() {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(Errno::from_i32(errno(error))),
        }
    }
}

impl Filesystem for FuseProjection {
    fn init(&mut self, request: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        let _ = request;
        let supported = REQUESTED_CAPABILITIES & config.capabilities();
        config
            .add_capabilities(supported)
            .map_err(|unsupported| std::io::Error::other(format!("{unsupported:?}")))
    }

    fn lookup(&self, request: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let _ = request;
        self.lookup_parallel(parent.0, name, reply);
    }

    fn getattr(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: Option<FuseFileHandle>,
        reply: ReplyAttr,
    ) {
        let _ = request;
        self.getattr_parallel(inode.0, fh.map(|handle| handle.0), reply);
    }

    fn forget(&self, request: &Request, inode: INodeNo, nlookup: u64) {
        if let Ok(mut state) = self.state.lock() {
            state.forget(request, inode.0, nlookup);
        }
    }

    fn readlink(&self, request: &Request, inode: INodeNo, reply: ReplyData) {
        with_fuse_state!(self, reply, readlink(request, inode.0));
    }

    #[allow(clippy::too_many_arguments)]
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
        with_fuse_state!(
            self,
            reply,
            setattr(
                request,
                inode.0,
                mode,
                uid,
                gid,
                size,
                atime,
                mtime,
                ctime,
                fh.map(|handle| handle.0),
                crtime,
                chgtime,
                bkuptime,
                flags.map(|flags| flags.bits())
            )
        );
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
        with_fuse_state!(self, reply, mkdir(request, parent.0, name, mode, umask));
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
        with_fuse_state!(
            self,
            reply,
            mknod(request, parent.0, name, mode, umask, rdev)
        );
    }

    fn symlink(
        &self,
        request: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        with_fuse_state!(self, reply, symlink(request, parent.0, link_name, target));
    }

    fn unlink(&self, request: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        with_fuse_state!(self, reply, unlink(request, parent.0, name));
    }

    fn rmdir(&self, request: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        with_fuse_state!(self, reply, rmdir(request, parent.0, name));
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
        with_fuse_state!(
            self,
            reply,
            rename(
                request,
                parent.0,
                name,
                new_parent.0,
                new_name,
                flags.bits()
            )
        );
    }

    fn link(
        &self,
        request: &Request,
        inode: INodeNo,
        new_parent: INodeNo,
        new_name: &OsStr,
        reply: ReplyEntry,
    ) {
        with_fuse_state!(self, reply, link(request, inode.0, new_parent.0, new_name));
    }

    fn open(&self, request: &Request, inode: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let _ = request;
        self.open_parallel(inode.0, flags.0, reply);
    }

    fn read(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        offset: u64,
        size: u32,
        flags: OpenFlags,
        lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let _ = (request, flags, lock_owner);
        self.read_parallel(inode.0, fh.0, offset, size, reply);
    }

    #[allow(clippy::too_many_arguments)]
    fn write(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        offset: u64,
        data: &[u8],
        write_flags: WriteFlags,
        flags: OpenFlags,
        lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let _ = (request, write_flags, flags, lock_owner);
        self.write_parallel(inode.0, fh.0, offset, data, reply);
    }

    fn fsync(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        let _ = (request, datasync);
        self.flush_parallel(inode.0, fh.0, true, false, reply);
    }

    fn flush(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        let _ = (request, lock_owner);
        self.flush_parallel(inode.0, fh.0, false, false, reply);
    }

    #[allow(clippy::too_many_arguments)]
    fn release(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        flags: OpenFlags,
        lock_owner: Option<LockOwner>,
        flush: bool,
        reply: ReplyEmpty,
    ) {
        let _ = (request, flags, lock_owner, flush);
        self.flush_parallel(inode.0, fh.0, false, true, reply);
    }

    fn opendir(&self, request: &Request, inode: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        with_fuse_state!(self, reply, opendir(request, inode.0, flags.0));
    }

    fn readdir(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        offset: u64,
        reply: ReplyDirectory,
    ) {
        let _ = request;
        self.list_directory_parallel(inode.0, fh.0, offset, reply);
    }

    fn readdirplus(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        offset: u64,
        reply: ReplyDirectoryPlus,
    ) {
        let _ = request;
        self.list_directory_parallel(inode.0, fh.0, offset, reply);
    }

    fn releasedir(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        with_fuse_state!(self, reply, releasedir(request, inode.0, fh.0, flags.0));
    }

    fn fsyncdir(
        &self,
        request: &Request,
        _inode: INodeNo,
        fh: FuseFileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        let _ = request;
        self.fsyncdir_parallel(fh.0, reply);
    }

    #[allow(clippy::too_many_arguments)]
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
        with_fuse_state!(
            self,
            reply,
            setxattr(request, inode.0, name, value, flags, position)
        );
    }

    fn getxattr(
        &self,
        request: &Request,
        inode: INodeNo,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        with_fuse_state!(self, reply, getxattr(request, inode.0, name, size));
    }

    fn listxattr(&self, request: &Request, inode: INodeNo, size: u32, reply: ReplyXattr) {
        with_fuse_state!(self, reply, listxattr(request, inode.0, size));
    }

    fn removexattr(&self, request: &Request, inode: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        with_fuse_state!(self, reply, removexattr(request, inode.0, name));
    }

    #[allow(clippy::too_many_arguments)]
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
        with_fuse_state!(
            self,
            reply,
            fallocate(request, inode.0, fh.0, offset, length, mode)
        );
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
        with_fuse_state!(self, reply, lseek(request, inode.0, fh.0, offset, whence));
    }

    #[allow(clippy::too_many_arguments)]
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
        with_fuse_state!(
            self,
            reply,
            copy_file_range(
                request,
                inode_in.0,
                fh_in.0,
                offset_in,
                inode_out.0,
                fh_out.0,
                offset_out,
                length,
                flags.bits()
            )
        );
    }

    #[allow(clippy::too_many_arguments)]
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
        with_fuse_state!(
            self,
            reply,
            create(request, parent.0, name, mode, umask, flags)
        );
    }
}

#[allow(clippy::too_many_arguments)] // Mirrors the fixed FUSE callback signatures.
#[allow(clippy::manual_let_else)] // Callback errors also remove stale directory handles.
#[allow(clippy::single_match_else)]
#[allow(clippy::too_many_lines)] // Directory pagination and validation are one callback.
impl FuseProjectionState {
    fn forget(&mut self, _request: &Request, inode: u64, nlookup: u64) {
        self.release_lookup_reference(inode, nlookup);
    }

    fn readlink(&mut self, _request: &Request, inode: u64, reply: ReplyData) {
        reject_stopping!(self, reply);
        let path = match self.path(inode) {
            Ok(path) => path,
            Err(error) => return reply.error(Errno::from_i32(error)),
        };
        match self.source.read_link(path) {
            Ok(target) => reply.data(&target),
            Err(error) => reply.error(Errno::from_i32(errno(error))),
        }
    }

    #[allow(clippy::similar_names, clippy::too_many_arguments)]
    fn setattr(
        &mut self,
        _request: &Request,
        inode: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        fh: Option<u64>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        reject_stopping!(self, reply);
        if self.admit_write().is_err() || bkuptime.is_some() {
            return reply.error(Errno::from_i32(libc::EOPNOTSUPP));
        }
        let open_file = match fh {
            Some(handle) => match self.open_handle(inode, handle) {
                Ok(open_file) => Some(open_file),
                Err(error) => return reply.error(Errno::from_i32(error)),
            },
            None => None,
        };
        let current = match open_file.as_ref() {
            Some(open_file) => open_file.lookup().map_err(errno),
            None => self.refresh(inode),
        };
        let current = match current {
            Ok(current) => current,
            Err(error) => return reply.error(Errno::from_i32(error)),
        };
        let mut metadata = current.metadata;
        if let Some(mode) = mode {
            metadata.posix_mode = MetadataField::Value(mode);
        }
        if let Some(uid) = uid {
            metadata.posix_uid = MetadataField::Value(uid);
        }
        if let Some(gid) = gid {
            metadata.posix_gid = MetadataField::Value(gid);
        }
        if let Some(atime) = atime {
            metadata.accessed_ns = match time_or_now_ns(atime) {
                Ok(value) => MetadataField::Value(value),
                Err(error) => return reply.error(Errno::from_i32(error)),
            };
        }
        if let Some(mtime) = mtime {
            metadata.modified_ns = match time_or_now_ns(mtime) {
                Ok(value) => MetadataField::Value(value),
                Err(error) => return reply.error(Errno::from_i32(error)),
            };
        }
        let changed = chgtime.or(ctime);
        if let Some(changed) = changed {
            metadata.changed_ns = match system_time_ns(changed) {
                Ok(value) => MetadataField::Value(value),
                Err(error) => return reply.error(Errno::from_i32(error)),
            };
        }
        if let Some(created) = crtime {
            metadata.created_ns = match system_time_ns(created) {
                Ok(value) => MetadataField::Value(value),
                Err(error) => return reply.error(Errno::from_i32(error)),
            };
        }
        if let Some(flags) = flags {
            metadata.posix_flags = MetadataField::Value(u64::from(flags));
        }
        if mode.is_none()
            && uid.is_none()
            && gid.is_none()
            && size.is_none()
            && atime.is_none()
            && mtime.is_none()
            && ctime.is_none()
            && crtime.is_none()
            && chgtime.is_none()
            && flags.is_none()
        {
            return match self.attr(inode, &current) {
                Ok(attr) => reply.attr(&CACHE_TTL, &attr),
                Err(error) => reply.error(Errno::from_i32(error)),
            };
        }
        let updated = if let Some(open_file) = open_file {
            open_file
                .set_attributes(metadata, size)
                .and_then(|()| open_file.lookup())
                .map_err(errno)
        } else {
            let path = match self.path(inode) {
                Ok(path) => path.to_owned(),
                Err(error) => return reply.error(Errno::from_i32(error)),
            };
            self.source
                .set_attributes(&path, metadata, size)
                .map_err(errno)
                .and_then(|()| self.refresh(inode))
        };
        if updated.is_ok()
            && let Some(handle) = fh
        {
            let _ = self.mark_handle_dirty(inode, handle);
        }
        match updated.and_then(|node| self.attr(inode, &node)) {
            Ok(attr) => reply.attr(&CACHE_TTL, &attr),
            Err(error) => reply.error(Errno::from_i32(error)),
        }
    }

    fn mkdir(
        &mut self,
        request: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        reject_stopping!(self, reply);
        if let Err(error) = self.admit_write() {
            return reply.error(Errno::from_i32(error));
        }
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(error) => return reply.error(Errno::from_i32(error)),
        };
        let metadata = create_metadata(request, mode & !umask, S_IFDIR);
        match self.source.create_directory(&path, metadata) {
            Ok(lookup) => {
                let inode = match self.intern(path, &lookup) {
                    Ok(inode) => inode,
                    Err(error) => return reply.error(Errno::from_i32(error)),
                };
                match self.attr(inode, &lookup) {
                    Ok(attr) => reply.entry(&CACHE_TTL, &attr, Generation(0)),
                    Err(error) => reply.error(Errno::from_i32(error)),
                }
            }
            Err(error) => reply.error(Errno::from_i32(errno(error))),
        }
    }

    fn mknod(
        &mut self,
        request: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        reject_stopping!(self, reply);
        if let Err(error) = self.admit_write() {
            return reply.error(Errno::from_i32(error));
        }
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(error) => return reply.error(Errno::from_i32(error)),
        };
        let (kind, device) = match mode & S_IFMT {
            S_IFIFO => (MountNodeKind::Fifo, None),
            S_IFSOCK => (MountNodeKind::Socket, None),
            S_IFCHR => (
                MountNodeKind::CharacterDevice,
                Some(native_device_parts(rdev)),
            ),
            S_IFBLK => (MountNodeKind::BlockDevice, Some(native_device_parts(rdev))),
            _ => return reply.error(Errno::from_i32(libc::EOPNOTSUPP)),
        };
        let metadata = create_metadata(request, mode & !umask, mode & S_IFMT);
        match self.source.create_special(&path, kind, device, metadata) {
            Ok(lookup) => {
                let inode = match self.intern(path, &lookup) {
                    Ok(inode) => inode,
                    Err(error) => return reply.error(Errno::from_i32(error)),
                };
                match self.attr(inode, &lookup) {
                    Ok(attr) => reply.entry(&CACHE_TTL, &attr, Generation(0)),
                    Err(error) => reply.error(Errno::from_i32(error)),
                }
            }
            Err(error) => reply.error(Errno::from_i32(errno(error))),
        }
    }

    fn symlink(
        &mut self,
        request: &Request,
        parent: u64,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        reject_stopping!(self, reply);
        if let Err(error) = self.admit_write() {
            return reply.error(Errno::from_i32(error));
        }
        let path = match self.child_path(parent, link_name) {
            Ok(path) => path,
            Err(error) => return reply.error(Errno::from_i32(error)),
        };
        let metadata = create_metadata(request, 0o777, S_IFLNK);
        match self.source.create_symbolic_link(
            &path,
            Bytes::copy_from_slice(target.as_os_str().as_bytes()),
            metadata,
        ) {
            Ok(lookup) => {
                let inode = match self.intern(path, &lookup) {
                    Ok(inode) => inode,
                    Err(error) => return reply.error(Errno::from_i32(error)),
                };
                match self.attr(inode, &lookup) {
                    Ok(attr) => reply.entry(&CACHE_TTL, &attr, Generation(0)),
                    Err(error) => reply.error(Errno::from_i32(error)),
                }
            }
            Err(error) => reply.error(Errno::from_i32(errno(error))),
        }
    }

    fn unlink(&mut self, request: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        self.remove_callback(request, parent, name, reply);
    }

    fn rmdir(&mut self, request: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        self.remove_callback(request, parent, name, reply);
    }

    fn rename(
        &mut self,
        _request: &Request,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        flags: u32,
        reply: ReplyEmpty,
    ) {
        reject_stopping!(self, reply);
        if let Err(error) = self.admit_write() {
            return reply.error(Errno::from_i32(error));
        }
        if flags & !RENAME_NOREPLACE != 0 {
            return reply.error(Errno::from_i32(libc::EOPNOTSUPP));
        }
        let source = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(error) => return reply.error(Errno::from_i32(error)),
        };
        let destination = match self.child_path(new_parent, new_name) {
            Ok(path) => path,
            Err(error) => return reply.error(Errno::from_i32(error)),
        };
        let replace = flags & RENAME_NOREPLACE == 0;
        let replaced = if replace {
            match self.source.lookup(&destination) {
                Ok(Some(lookup))
                    if lookup.node.kind == MountNodeKind::Regular
                        && lookup.node.link_count == 1 =>
                {
                    if let Some(inode) = self.inode_by_file.get(&lookup.node.file_id).copied()
                        && self
                            .by_inode
                            .get(&inode)
                            .is_some_and(|entry| entry.open_handles != 0)
                    {
                        match self.source.detach_file(&destination) {
                            Ok(detached) => Some((inode, detached)),
                            Err(error) => return reply.error(Errno::from_i32(errno(error))),
                        }
                    } else {
                        None
                    }
                }
                Ok(_) => None,
                Err(error) => return reply.error(Errno::from_i32(errno(error))),
            }
        } else {
            None
        };
        match self.source.rename(&source, &destination, replace) {
            Ok(()) => {
                if let Some((inode, detached)) = replaced {
                    self.retain_detached_handles(inode, &detached);
                }
                if replace {
                    self.invalidate_prefix(&destination);
                }
                self.rename_prefix(&source, &destination);
                reply.ok();
            }
            Err(error) => reply.error(Errno::from_i32(errno(error))),
        }
    }

    fn link(
        &mut self,
        _request: &Request,
        inode: u64,
        new_parent: u64,
        new_name: &OsStr,
        reply: ReplyEntry,
    ) {
        reject_stopping!(self, reply);
        if let Err(error) = self.admit_write() {
            return reply.error(Errno::from_i32(error));
        }
        let source = match self.path(inode) {
            Ok(path) => path.to_owned(),
            Err(error) => return reply.error(Errno::from_i32(error)),
        };
        let destination = match self.child_path(new_parent, new_name) {
            Ok(path) => path,
            Err(error) => return reply.error(Errno::from_i32(error)),
        };
        let mut projected = match self.refresh_handle(inode, None) {
            Ok(lookup) => lookup,
            Err(error) => return reply.error(Errno::from_i32(error)),
        };
        projected.node.link_count = match projected.node.link_count.checked_add(1) {
            Some(count) => count,
            None => return reply.error(Errno::from_i32(libc::EMLINK)),
        };
        let attr = match self.attr(inode, &projected) {
            Ok(attr) => attr,
            Err(error) => return reply.error(Errno::from_i32(error)),
        };
        let Some(entry) = self.by_inode.get_mut(&inode) else {
            return reply.error(Errno::from_i32(libc::ESTALE));
        };
        if entry.bindings.contains(&destination) {
            return reply.error(Errno::from_i32(libc::EEXIST));
        }
        if entry.bindings.try_reserve(1).is_err() {
            return reply.error(Errno::from_i32(libc::ENOMEM));
        }
        match self.source.hard_link(&source, &destination) {
            Ok(()) => {
                entry.bindings.push(destination.clone());
                entry.lookup = projected;
                entry.lookup_references = entry.lookup_references.saturating_add(1);
                self.inode_by_path.insert(destination, inode);
                reply.entry(&CACHE_TTL, &attr, Generation(0));
            }
            Err(error) => reply.error(Errno::from_i32(errno(error))),
        }
    }

    fn opendir(&mut self, _request: &Request, inode: u64, _flags: i32, reply: ReplyOpen) {
        reject_stopping!(self, reply);
        if !self.source.view_is_stable() {
            return reply.error(Errno::from_i32(libc::ESTALE));
        }
        let binding_epoch = self.source.binding_epoch();
        let path = match self.path(inode) {
            Ok(path) => path.to_owned(),
            Err(error) => return reply.error(Errno::from_i32(error)),
        };
        let handle = self.next_handle;
        self.next_handle = self.next_handle.saturating_add(1).max(1);
        let parent_inode = parent_inode(self, inode);
        self.directories.insert(
            handle,
            DirectoryHandle {
                path,
                parent_inode,
                binding_epoch,
                cursor: None,
                entries: VecDeque::new(),
                entries_stamp: None,
                exhausted: false,
                emitted: 0,
            },
        );
        // The kernel resets a cached listing whenever the directory changes
        // through the mount or its modification time changes, and drops it
        // on invalidation; otherwise it is the listing this handle would read.
        reply.opened(
            FuseFileHandle(handle),
            FopenFlags::FOPEN_CACHE_DIR | FopenFlags::FOPEN_KEEP_CACHE,
        );
    }

    /// Emits an open directory handle's buffered entries into one kernel
    /// listing reply; [`FuseProjection::buffer_directory_page`] reads the next
    /// source page beforehand, outside this state's lock.
    fn list_directory<L: DirectoryListing>(
        &mut self,
        inode: u64,
        handle: u64,
        offset: u64,
        mut listing: L,
    ) {
        reject_stopping!(self, listing);
        let expected_epoch = match self.directories.get(&handle) {
            Some(directory) => directory.binding_epoch,
            None => return listing.error(Errno::from_i32(libc::ESTALE)),
        };
        let _view_lease = match self.source.acquire_binding_lease(expected_epoch) {
            Ok(lease) => lease,
            Err(_) => {
                self.directories.remove(&handle);
                return listing.error(Errno::from_i32(libc::ESTALE));
            }
        };
        if !self.source.view_is_stable() || self.source.binding_epoch() != expected_epoch {
            self.directories.remove(&handle);
            return listing.error(Errno::from_i32(libc::ESTALE));
        }
        let Some(directory) = self.directories.get(&handle) else {
            return listing.error(Errno::from_i32(libc::ESTALE));
        };
        if offset != directory.emitted {
            return listing.error(Errno::from_i32(libc::EINVAL));
        }
        if directory.emitted == 0 {
            let parent_inode = directory.parent_inode;
            let attr = match self.node_attr(inode) {
                Ok(attr) => attr,
                Err(error) => return listing.error(Errno::from_i32(error)),
            };
            // The kernel never instantiates dot entries, so they carry the
            // directory's own attributes without a cache lifetime.
            for (dot_inode, name, next) in [(inode, ".", 1), (parent_inode, "..", 2)] {
                let dot = FileAttr {
                    ino: INodeNo(dot_inode),
                    ..attr
                };
                if listing.push(next, OsStr::new(name), &dot, &Duration::ZERO) {
                    return listing.ok();
                }
                if let Some(directory) = self.directories.get_mut(&handle) {
                    directory.emitted = next;
                }
            }
        }
        let mut listed = false;
        loop {
            let Some(directory) = self.directories.get_mut(&handle) else {
                return listing.error(Errno::from_i32(libc::ESTALE));
            };
            let next = directory.emitted.saturating_add(1);
            let Some(entry) = directory.entries.pop_front() else {
                return listing.ok();
            };
            let child_path = directory.path.child(entry.name.clone());
            // A buffered entry is current only while its directory's listing
            // and its own node are.
            let view_stamp = directory.entries_stamp.filter(|stamp| {
                self.source.unchanged_since(&directory.path, None, *stamp)
                    && self
                        .source
                        .unchanged_since(&child_path, Some(entry.node.file_id), *stamp)
            });
            let page_lookup = MountLookup {
                node: entry.node,
                metadata: entry.metadata,
            };
            // A page older than the current view may predate mounted writes
            // the kernel already applied; the projection's own record, which
            // every mounted write refreshes, must not be rolled back by it.
            let lookup = if view_stamp.is_some() {
                page_lookup
            } else {
                self.inode_by_file
                    .get(&page_lookup.node.file_id)
                    .and_then(|known| self.by_inode.get(known))
                    .map_or(page_lookup, |known| known.lookup)
            };
            let listed_attr = match self.intern_with_reference(
                child_path,
                &lookup,
                view_stamp,
                L::COUNTS_LOOKUPS,
            ) {
                Ok(child) => self
                    .attr(child, &lookup)
                    .inspect_err(|_| self.release_listed_reference::<L>(child)),
                Err(error) => Err(error),
            };
            let attr = match listed_attr {
                Ok(attr) => attr,
                // Entries already listed stand; the failure repeats on the
                // kernel's next request, which starts at this entry.
                Err(error) => {
                    self.restore_directory_entry(handle, entry);
                    if listed {
                        return listing.ok();
                    }
                    return listing.error(Errno::from_i32(error));
                }
            };
            let ttl = if view_stamp.is_some() {
                CACHE_TTL
            } else {
                Duration::ZERO
            };
            if listing.push(next, OsStr::from_bytes(&entry.name), &attr, &ttl) {
                self.release_listed_reference::<L>(attr.ino.0);
                self.restore_directory_entry(handle, entry);
                return listing.ok();
            }
            listed = true;
            if let Some(directory) = self.directories.get_mut(&handle) {
                directory.emitted = next;
            }
        }
    }

    fn releasedir(
        &mut self,
        _request: &Request,
        _inode: u64,
        handle: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        reject_stopping!(self, reply);
        self.directories.remove(&handle);
        reply.ok();
    }

    fn setxattr(
        &mut self,
        _request: &Request,
        inode: u64,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        reply: ReplyEmpty,
    ) {
        reject_stopping!(self, reply);
        if let Err(error) = self.admit_write() {
            return reply.error(Errno::from_i32(error));
        }
        if position != 0 || flags & !(libc::XATTR_CREATE | libc::XATTR_REPLACE) != 0 {
            return reply.error(Errno::from_i32(libc::EOPNOTSUPP));
        }
        if flags & libc::XATTR_CREATE != 0 && flags & libc::XATTR_REPLACE != 0 {
            return reply.error(Errno::from_i32(libc::EINVAL));
        }
        let mode = if flags & libc::XATTR_CREATE != 0 {
            super::MountAttributeWriteMode::Create
        } else if flags & libc::XATTR_REPLACE != 0 {
            super::MountAttributeWriteMode::Replace
        } else {
            super::MountAttributeWriteMode::Upsert
        };
        let written = match self.open_inode(inode) {
            Some(open_file) => {
                open_file.write_attribute(name.as_bytes(), Bytes::copy_from_slice(value), mode)
            }
            None => match self.path(inode) {
                Ok(path) => self.source.write_attribute(
                    path,
                    name.as_bytes(),
                    Bytes::copy_from_slice(value),
                    mode,
                ),
                Err(error) => return reply.error(Errno::from_i32(error)),
            },
        };
        match written {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(Errno::from_i32(errno(error))),
        }
    }

    fn getxattr(
        &mut self,
        _request: &Request,
        inode: u64,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        reject_stopping!(self, reply);
        let value = match self.open_inode(inode) {
            Some(open_file) => open_file.read_attribute(name.as_bytes()),
            None => match self.path(inode) {
                Ok(path) => self.source.read_attribute(path, name.as_bytes()),
                Err(error) => return reply.error(Errno::from_i32(error)),
            },
        };
        match value {
            Ok(Some(value)) if size == 0 => {
                reply.size(u32::try_from(value.len()).unwrap_or(u32::MAX));
            }
            Ok(Some(value)) if value.len() <= size as usize => reply.data(&value),
            Ok(Some(_)) => reply.error(Errno::from_i32(libc::ERANGE)),
            Ok(None) | Err(MountSourceError::NotFound) => {
                reply.error(Errno::from_i32(libc::ENODATA));
            }
            Err(error) => reply.error(Errno::from_i32(errno(error))),
        }
    }

    fn listxattr(&mut self, _request: &Request, inode: u64, size: u32, reply: ReplyXattr) {
        reject_stopping!(self, reply);
        let open_file = self.open_inode(inode);
        let path = if open_file.is_none() {
            match self.path(inode) {
                Ok(path) => Some(path.to_owned()),
                Err(error) => return reply.error(Errno::from_i32(error)),
            }
        } else {
            None
        };
        let mut cursor: Option<Vec<u8>> = None;
        let mut encoded = Vec::new();
        loop {
            let page = if let Some(open_file) = open_file.as_ref() {
                open_file.list_attributes(cursor.as_deref(), ATTRIBUTE_PAGE_SIZE)
            } else {
                let Some(path) = path.as_ref() else {
                    return reply.error(Errno::from_i32(libc::EIO));
                };
                self.source
                    .list_attributes(path, cursor.as_deref(), ATTRIBUTE_PAGE_SIZE)
            };
            let page = match page {
                Ok(page) => page,
                Err(error) => return reply.error(Errno::from_i32(errno(error))),
            };
            for name in page.names {
                let Some(required) = name.len().checked_add(1) else {
                    return reply.error(Errno::from_i32(libc::EOVERFLOW));
                };
                if encoded.len().saturating_add(required) > MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES {
                    return reply.error(Errno::from_i32(libc::E2BIG));
                }
                if encoded.try_reserve(required).is_err() {
                    return reply.error(Errno::from_i32(libc::ENOMEM));
                }
                encoded.extend_from_slice(&name);
                encoded.push(0);
            }
            let Some(next) = page.next_cursor else {
                break;
            };
            if cursor.as_ref() == Some(&next) {
                return reply.error(Errno::from_i32(libc::EIO));
            }
            cursor = Some(next);
        }
        if size == 0 {
            reply.size(u32::try_from(encoded.len()).unwrap_or(u32::MAX));
        } else if encoded.len() <= size as usize {
            reply.data(&encoded);
        } else {
            reply.error(Errno::from_i32(libc::ERANGE));
        }
    }

    fn removexattr(&mut self, _request: &Request, inode: u64, name: &OsStr, reply: ReplyEmpty) {
        reject_stopping!(self, reply);
        if let Err(error) = self.admit_write() {
            return reply.error(Errno::from_i32(error));
        }
        let removed = match self.open_inode(inode) {
            Some(open_file) => open_file.remove_attribute(name.as_bytes()),
            None => match self.path(inode) {
                Ok(path) => self.source.remove_attribute(path, name.as_bytes()),
                Err(error) => return reply.error(Errno::from_i32(error)),
            },
        };
        match removed {
            Ok(()) => reply.ok(),
            Err(MountSourceError::NotFound) => reply.error(Errno::from_i32(libc::ENODATA)),
            Err(error) => reply.error(Errno::from_i32(errno(error))),
        }
    }

    fn fallocate(
        &mut self,
        _request: &Request,
        inode: u64,
        handle: u64,
        offset: u64,
        length: u64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        reject_stopping!(self, reply);
        const KEEP_SIZE: i32 = 0x01;
        const PUNCH_HOLE: i32 = 0x02;
        const ZERO_RANGE: i32 = 0x10;
        if let Err(error) = self.admit_write() {
            return reply.error(Errno::from_i32(error));
        }
        if length == 0 {
            return reply.error(Errno::from_i32(libc::EINVAL));
        }
        let operation = if mode == (PUNCH_HOLE | KEEP_SIZE) {
            crate::MountRangeAllocation::PunchHole
        } else if mode == ZERO_RANGE || mode == (ZERO_RANGE | KEEP_SIZE) {
            crate::MountRangeAllocation::ZeroRange {
                extend: mode == ZERO_RANGE,
            }
        } else if mode == 0 || mode == KEEP_SIZE {
            crate::MountRangeAllocation::Preallocate {
                keep_size: mode == KEEP_SIZE,
            }
        } else {
            return reply.error(Errno::from_i32(libc::EOPNOTSUPP));
        };
        let open_file = match self.open_handle(inode, handle) {
            Ok(open_file) => open_file,
            Err(error) => return reply.error(Errno::from_i32(error)),
        };
        let allocated = open_file.allocate_range(offset, length, operation);
        match allocated {
            Ok(()) => {
                let _ = self.mark_handle_dirty(inode, handle);
                let _ = self.refresh_handle(inode, Some(handle));
                reply.ok();
            }
            Err(error) => reply.error(Errno::from_i32(errno(error))),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn lseek(
        &mut self,
        _request: &Request,
        inode: u64,
        file_handle: u64,
        offset: i64,
        whence: i32,
        reply: ReplyLseek,
    ) {
        reject_stopping!(self, reply);
        let Ok(offset) = u64::try_from(offset) else {
            return reply.error(Errno::from_i32(libc::EINVAL));
        };
        let target = match whence {
            libc::SEEK_DATA => MountSeekTarget::Data,
            libc::SEEK_HOLE => MountSeekTarget::Hole,
            _ => return reply.error(Errno::from_i32(libc::EINVAL)),
        };
        let open_file = match self.open_handle(inode, file_handle) {
            Ok(open_file) => open_file,
            Err(error) => return reply.error(Errno::from_i32(error)),
        };
        let found = open_file.seek(offset, target);
        match found {
            Ok(Some(found)) => match i64::try_from(found) {
                Ok(found) => reply.offset(found),
                Err(_) => reply.error(Errno::from_i32(libc::EOVERFLOW)),
            },
            Ok(None) => reply.error(Errno::from_i32(libc::ENXIO)),
            Err(error) => reply.error(Errno::from_i32(errno(error))),
        }
    }

    fn copy_file_range(
        &mut self,
        _request: &Request,
        source_inode: u64,
        source_handle: u64,
        source_offset: u64,
        destination_inode: u64,
        destination_handle: u64,
        destination_offset: u64,
        length: u64,
        flags: u64,
        reply: ReplyWrite,
    ) {
        reject_stopping!(self, reply);
        if let Err(error) = self.admit_write() {
            return reply.error(Errno::from_i32(error));
        }
        let Ok(_) = u32::try_from(length) else {
            return reply.error(Errno::from_i32(libc::EOVERFLOW));
        };
        if flags != 0 {
            return reply.error(Errno::from_i32(libc::EOPNOTSUPP));
        }
        if length == 0 {
            return reply.written(0);
        }
        match self.copy_open_range(
            source_inode,
            source_handle,
            source_offset,
            destination_inode,
            destination_handle,
            destination_offset,
            length,
        ) {
            Ok(transferred) => {
                let _ = self.mark_handle_dirty(destination_inode, destination_handle);
                reply.written(transferred);
            }
            Err(error) => reply.error(Errno::from_i32(error)),
        }
    }

    fn create(
        &mut self,
        request: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        reject_stopping!(self, reply);
        if let Err(error) = self.admit_write() {
            return reply.error(Errno::from_i32(error));
        }
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(error) => return reply.error(Errno::from_i32(error)),
        };
        match self.source.lookup(&path) {
            Ok(Some(lookup)) => {
                if flags & libc::O_EXCL != 0 {
                    return reply.error(Errno::EEXIST);
                }
                if lookup.node.kind != MountNodeKind::Regular {
                    return reply.error(Errno::EISDIR);
                }
                let inode = match self.intern(path, &lookup) {
                    Ok(inode) => inode,
                    Err(error) => return reply.error(Errno::from_i32(error)),
                };
                let handle = match self.allocate_file_handle(inode, InitialHandleState::Clean) {
                    Ok(handle) => handle,
                    Err(error) => {
                        self.release_lookup_reference(inode, 1);
                        return reply.error(Errno::from_i32(error));
                    }
                };
                let create_result = (|| -> Result<(FileAttr, FopenFlags), i32> {
                    let lookup = if flags & libc::O_TRUNC != 0 {
                        let open_file = self.open_handle(inode, handle)?;
                        open_file.resize(0).map_err(errno)?;
                        self.mark_handle_dirty(inode, handle)?;
                        open_file.lookup().map_err(errno)?
                    } else {
                        lookup
                    };
                    if let Some(entry) = self.by_inode.get_mut(&inode) {
                        entry.lookup = lookup;
                        entry.view_stamp = self.source.view_stamp();
                    }
                    Ok((
                        self.attr(inode, &lookup)?,
                        self.file_open_flags(inode, &lookup, flags),
                    ))
                })();
                return match create_result {
                    Ok((attr, open_flags)) => reply.created(
                        &CACHE_TTL,
                        &attr,
                        Generation(0),
                        FuseFileHandle(handle),
                        open_flags,
                    ),
                    Err(error) => {
                        let error = match self.discard_created_handle(inode, handle) {
                            Ok(()) => error,
                            Err(cleanup_error) => cleanup_error,
                        };
                        reply.error(Errno::from_i32(error));
                    }
                };
            }
            Ok(None) => {}
            Err(error) => return reply.error(Errno::from_i32(errno(error))),
        }
        let metadata = create_metadata(request, mode & !umask, S_IFREG);
        match self.source.create_file(&path, metadata) {
            Ok(lookup) => {
                let inode = match self.intern(path, &lookup) {
                    Ok(inode) => inode,
                    Err(error) => return reply.error(Errno::from_i32(error)),
                };
                let handle = match self.allocate_file_handle(inode, InitialHandleState::Dirty) {
                    Ok(handle) => handle,
                    Err(error) => {
                        self.release_lookup_reference(inode, 1);
                        return reply.error(Errno::from_i32(error));
                    }
                };
                match self.attr(inode, &lookup) {
                    Ok(attr) => reply.created(
                        &CACHE_TTL,
                        &attr,
                        Generation(0),
                        FuseFileHandle(handle),
                        self.file_open_flags(inode, &lookup, flags),
                    ),
                    Err(error) => {
                        let error = match self.discard_created_handle(inode, handle) {
                            Ok(()) => error,
                            Err(cleanup_error) => cleanup_error,
                        };
                        reply.error(Errno::from_i32(error));
                    }
                }
            }
            Err(error) => reply.error(Errno::from_i32(errno(error))),
        }
    }
}

impl FuseProjectionState {
    fn restore_directory_entry(&mut self, handle: u64, entry: MountDirectoryEntry) {
        if let Some(directory) = self.directories.get_mut(&handle) {
            directory.entries.push_front(entry);
        }
    }

    /// Returns the lookup a listing took for a child it did not list.
    fn release_listed_reference<L: DirectoryListing>(&mut self, inode: u64) {
        if L::COUNTS_LOOKUPS {
            self.release_lookup_reference(inode, 1);
        }
    }

    fn remove_callback(
        &mut self,
        _request: &Request,
        parent: u64,
        name: &OsStr,
        reply: ReplyEmpty,
    ) {
        reject_stopping!(self, reply);
        if let Err(error) = self.admit_write() {
            return reply.error(Errno::from_i32(error));
        }
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(error) => return reply.error(Errno::from_i32(error)),
        };
        let current = match self.source.lookup(&path) {
            Ok(current) => current,
            Err(error) => return reply.error(Errno::from_i32(errno(error))),
        };
        let expected = current.map(|lookup| lookup.node.file_id);
        let detached = if let Some(lookup) = current
            && lookup.node.kind == MountNodeKind::Regular
            && lookup.node.link_count == 1
            && let Some(inode) = self.inode_by_file.get(&lookup.node.file_id).copied()
            && self
                .by_inode
                .get(&inode)
                .is_some_and(|entry| entry.open_handles != 0)
        {
            match self.source.detach_file(&path) {
                Ok(detached) => Some((inode, detached)),
                Err(error) => return reply.error(Errno::from_i32(errno(error))),
            }
        } else {
            None
        };
        match self.source.remove(&path, expected) {
            Ok(()) => {
                if let Some((inode, detached)) = detached {
                    self.retain_detached_handles(inode, &detached);
                }
                self.remove_path_cache(&path);
                reply.ok();
            }
            Err(error) => reply.error(Errno::from_i32(errno(error))),
        }
    }
}

fn has_prefix(path: &MountPath, prefix: &MountPath) -> bool {
    path.components().starts_with(prefix.components())
}

fn replace_prefix(
    path: &MountPath,
    source: &MountPath,
    destination: &MountPath,
) -> Option<MountPath> {
    if !has_prefix(path, source) {
        return None;
    }
    let mut rebased = destination.clone();
    for component in path
        .components()
        .get(source.components().len()..)
        .unwrap_or(&[])
    {
        rebased = rebased.child(component.clone());
    }
    Some(rebased)
}

fn metadata_time(field: MetadataField<i64>) -> SystemTime {
    match field {
        MetadataField::Unavailable => SystemTime::UNIX_EPOCH,
        MetadataField::Value(value) if value >= 0 => {
            SystemTime::UNIX_EPOCH + Duration::from_nanos(value.unsigned_abs())
        }
        MetadataField::Value(value) => {
            SystemTime::UNIX_EPOCH - Duration::from_nanos(value.unsigned_abs())
        }
    }
}

fn time_or_now_ns(value: TimeOrNow) -> Result<i64, i32> {
    system_time_ns(match value {
        TimeOrNow::SpecificTime(value) => value,
        TimeOrNow::Now => SystemTime::now(),
    })
}

fn create_metadata(request: &Request, mode: u32, kind: u32) -> FileMetadata {
    let now = system_time_ns(SystemTime::now()).unwrap_or(i64::MAX);
    FileMetadata {
        posix_mode: MetadataField::Value((mode & 0o7777) | kind),
        posix_uid: MetadataField::Value(request.uid()),
        posix_gid: MetadataField::Value(request.gid()),
        posix_flags: MetadataField::Value(0),
        windows_attributes: MetadataField::Unavailable,
        created_ns: MetadataField::Value(now),
        modified_ns: MetadataField::Value(now),
        accessed_ns: MetadataField::Value(now),
        changed_ns: MetadataField::Value(now),
        named_attributes: MetadataField::Unavailable,
        acl: MetadataField::Unavailable,
        security_descriptor: MetadataField::Unavailable,
    }
}

fn native_device_parts(device: u32) -> (u32, u32) {
    super::device::split_device(u64::from(device))
}

#[cfg(target_os = "linux")]
fn native_device_number(major: u32, minor: u32) -> Result<u32, i32> {
    super::device::join_device(major, minor)
        .and_then(|device| u32::try_from(device).map_err(|_| libc::EOVERFLOW))
}

#[cfg(target_os = "macos")]
fn native_device_number(major: u32, minor: u32) -> Result<u32, i32> {
    super::device::join_device(major, minor).map(|device| u32::from_ne_bytes(device.to_ne_bytes()))
}

/// Splits a non-root path into its parent directory and final name.
fn split_parent(path: &MountPath) -> Option<(MountPath, &[u8])> {
    let (name, components) = path.components().split_last()?;
    let parent = components
        .iter()
        .fold(MountPath::root(), |parent, component| {
            parent.child(component.clone())
        });
    Some((parent, name))
}

fn parent_inode(filesystem: &FuseProjectionState, inode: u64) -> u64 {
    filesystem
        .path(inode)
        .ok()
        .and_then(split_parent)
        .and_then(|(parent, _name)| filesystem.inode_by_path.get(&parent).copied())
        .unwrap_or(ROOT_INODE)
}

#[allow(clippy::needless_pass_by_value)]
fn source_error(error: MountSourceError) -> NativeMountError {
    NativeMountError::Driver(error.to_string())
}

#[allow(clippy::needless_pass_by_value)]
fn errno(error: MountSourceError) -> i32 {
    match error {
        MountSourceError::NotFound => libc::ENOENT,
        MountSourceError::AlreadyExists => libc::EEXIST,
        MountSourceError::Invalid(_) => libc::EINVAL,
        MountSourceError::Unsupported(_) => libc::EOPNOTSUPP,
        MountSourceError::Engine(_) => libc::EIO,
        MountSourceError::Stale => libc::ESTALE,
    }
}

fn admit_open(writable: bool, flags: i32) -> Result<(), i32> {
    if flags & libc::O_TRUNC != 0 && !writable {
        Err(libc::EROFS)
    } else {
        Ok(())
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::{
        InodeEntry, KernelCacheItem, MAXIMUM_NEGATIVE_ENTRIES_PER_DIRECTORY, MountFilesystem,
        MountLookup, MountNode, MountNodeKind, MountPath, ROOT_INODE, ViewStamp, admit_open,
        cached_projected_lookup, drain_kernel_cache_items, intern_projected, replace_root_lookup,
    };
    use crate::kernel::{FileMetadata, MetadataField};
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::sync::Arc;

    type MemorySource = super::super::CheckoutMountSource<
        crate::facade::MemoryAuthorityBackend,
        crate::facade::MemoryObjectBackend,
    >;

    fn regular(file_id: crate::FileId, logical_bytes: u64) -> MountLookup {
        MountLookup {
            node: MountNode {
                file_id,
                kind: MountNodeKind::Regular,
                logical_bytes,
                link_count: 1,
                device: None,
            },
            metadata: FileMetadata::default(),
        }
    }

    #[test]
    fn retained_pages_require_the_version_they_were_admitted_under() {
        let file_id = crate::FileId::new();
        let path = MountPath::root().child(b"file".to_vec());
        let mut entry = InodeEntry::new(path, regular(file_id, 4), None, 1);
        let admitted = regular(file_id, 4);
        let resized = regular(file_id, 5);
        let mut touched = admitted;
        touched.metadata.modified_ns = MetadataField::Value(7);

        assert!(
            !entry.admit_cached_content(&admitted),
            "nothing is cached yet"
        );
        assert!(entry.admit_cached_content(&admitted));
        assert!(!entry.admit_cached_content(&resized));
        assert!(!entry.admit_cached_content(&touched));
        assert!(entry.admit_cached_content(&touched));
    }

    #[test]
    fn negative_entries_are_cacheable_only_while_tracked() {
        let mut directory = regular(crate::FileId::new(), 0);
        directory.node.kind = MountNodeKind::Directory;
        let mut entry = InodeEntry::new(MountPath::root(), directory, None, 1);
        for index in 0..MAXIMUM_NEGATIVE_ENTRIES_PER_DIRECTORY {
            assert!(entry.remember_negative_child(format!("absent-{index}").as_bytes()));
        }
        assert!(entry.remember_negative_child(b"absent-0"));
        assert!(!entry.remember_negative_child(b"untracked"));
        assert_eq!(
            entry.negative_children.len(),
            MAXIMUM_NEGATIVE_ENTRIES_PER_DIRECTORY
        );
    }

    #[test]
    fn revalidation_reaches_every_kernel_cache_item() -> Result<(), i32> {
        let directory = MountPath::root().child(b"directory".to_vec());
        let nested = directory.child(b"nested".to_vec());
        let alias = MountPath::root().child(b"alias".to_vec());
        let mut root = regular(crate::FileId::new(), 0);
        root.node.kind = MountNodeKind::Directory;
        let mut next_inode = ROOT_INODE + 1;
        let mut by_inode = HashMap::from([(
            ROOT_INODE,
            InodeEntry::new(MountPath::root(), root, None, 1),
        )]);
        let mut inode_by_path = HashMap::from([(MountPath::root(), ROOT_INODE)]);
        let mut inode_by_file = HashMap::new();
        let mut intern = |path: MountPath, lookup: &MountLookup| {
            intern_projected(
                &mut next_inode,
                &mut by_inode,
                &mut inode_by_path,
                &mut inode_by_file,
                path,
                lookup,
                None,
                true,
            )
        };
        let directory_inode = intern(directory, &root)?;
        let mut linked = regular(crate::FileId::new(), 3);
        linked.node.link_count = 2;
        let file_inode = intern(nested, &linked)?;
        assert_eq!(intern(alias, &linked)?, file_inode);
        for (inode, name) in [(ROOT_INODE, "absent"), (directory_inode, "missing")] {
            let entry = by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
            assert!(entry.remember_negative_child(name.as_bytes()));
        }

        let entry = |parent: u64, name: &str| KernelCacheItem::Entry {
            parent,
            name: name.as_bytes().to_vec(),
        };
        let mut expected = vec![
            KernelCacheItem::Inode(ROOT_INODE),
            KernelCacheItem::Inode(directory_inode),
            KernelCacheItem::Inode(file_inode),
            entry(ROOT_INODE, "directory"),
            entry(directory_inode, "nested"),
            entry(ROOT_INODE, "alias"),
            entry(ROOT_INODE, "absent"),
            entry(directory_inode, "missing"),
        ];
        let mut items = drain_kernel_cache_items(&mut by_inode, &inode_by_path);
        let key = |item: &KernelCacheItem| format!("{item:?}");
        items.sort_by_key(key);
        expected.sort_by_key(key);
        assert_eq!(items, expected);
        assert!(
            by_inode
                .values()
                .all(|entry| entry.negative_children.is_empty()),
            "drained negative entries are no longer tracked"
        );
        Ok(())
    }

    /// A tracking-safe reader and an independent writer of one volume.
    fn tracking_sources() -> Result<(MemorySource, MemorySource), Box<dyn std::error::Error>> {
        use crate::model::{
            AccessMode, CheckoutMode, ConsistencyMode, FilesystemProfile, GenerationSelector,
            Lifecycle, MutationMode, VolumeConfig,
        };
        let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
        config.profile = FilesystemProfile::Posix;
        let fs = crate::Fs::memory();
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let mode = CheckoutMode {
            access: AccessMode::ReadWrite,
            consistency: ConsistencyMode::TrackingSafe,
            mutations: MutationMode::PrivateOverlay,
        };
        let (reader, writer) = runtime.block_on(async {
            let cancellation = crate::CancellationToken::new();
            let volume = fs
                .create_volume(config, crate::WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value;
            let checkout = || {
                volume.checkout(
                    GenerationSelector::Head,
                    mode,
                    crate::WorkBudget::UNBOUNDED,
                    &cancellation,
                )
            };
            let reader = checkout().await?.value;
            let writer = checkout().await?.value;
            Ok::<_, crate::OperationFailure<crate::FsError>>((reader, writer))
        })?;
        let source = |checkout| {
            MemorySource::new(
                Arc::new(super::super::SharedCheckout::new(checkout)),
                config,
            )
        };
        Ok((source(reader)?, source(writer)?))
    }

    #[test]
    #[ignore = "requires a live Linux FUSE mount"]
    fn linux_rebind_revalidates_every_retained_kernel_cache()
    -> Result<(), Box<dyn std::error::Error>> {
        let (reader, writer) = tracking_sources()?;
        let reader = Arc::new(reader);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let file = MountPath::root().child(b"file".to_vec());
        writer.create_file(&file, FileMetadata::default())?;
        writer.write_range(&file, 0, Bytes::from_static(b"first"))?;
        writer.sync()?;
        runtime.block_on(reader.advance_to_head_async())?;
        let temporary = tempfile::tempdir()?;
        let mut mount = crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id: reader.volume_id()?,
                destination: temporary.path().to_path_buf(),
                writable: true,
            },
            Arc::clone(&reader) as Arc<dyn MountFilesystem>,
        )?;
        let listing = || -> std::io::Result<Vec<std::ffi::OsString>> {
            let mut names = std::fs::read_dir(temporary.path())?
                .map(|entry| entry.map(|entry| entry.file_name()))
                .collect::<Result<Vec<_>, _>>()?;
            names.sort();
            Ok(names)
        };
        let mounted = temporary.path().join("file");
        let added = temporary.path().join("added");
        assert_eq!(std::fs::read(&mounted)?, b"first");
        assert!(!added.exists());
        assert_eq!(listing()?, ["file"]);

        writer.write_range(&file, 0, Bytes::from_static(b"other"))?;
        writer.create_file(
            &MountPath::root().child(b"added".to_vec()),
            FileMetadata::default(),
        )?;
        writer.sync()?;
        runtime.block_on(reader.advance_to_head_async())?;
        // Data, negative entries, and listings carry no expiry, so the
        // superseded binding stays visible until the owner revalidates.
        assert_eq!(std::fs::read(&mounted)?, b"first");
        assert!(!added.exists());
        assert_eq!(listing()?, ["file"]);

        mount.revalidate()?;
        assert_eq!(std::fs::read(&mounted)?, b"other");
        assert!(added.is_file());
        assert_eq!(listing()?, ["added", "file"]);
        mount.revalidate()?;
        assert!(mount.stop()?);
        Ok(())
    }

    #[test]
    fn rename_noreplace_matches_linux_libc() {
        assert_eq!(super::RENAME_NOREPLACE, libc::RENAME_NOREPLACE);
    }

    #[test]
    fn read_only_mount_rejects_truncating_open_before_source_mutation() {
        assert_eq!(admit_open(false, libc::O_TRUNC), Err(libc::EROFS));
        assert_eq!(admit_open(false, libc::O_RDONLY), Ok(()));
        assert_eq!(admit_open(true, libc::O_TRUNC), Ok(()));
    }

    #[test]
    fn lookup_cache_requires_the_exact_source_epoch() {
        let path = MountPath::root().child(b"cached".to_vec());
        let lookup = MountLookup {
            node: MountNode {
                file_id: crate::FileId::from_bytes([3; 16]),
                kind: MountNodeKind::Regular,
                logical_bytes: 7,
                link_count: 1,
                device: None,
            },
            metadata: FileMetadata::default(),
        };
        let stamp = ViewStamp::current();
        let mut by_inode =
            HashMap::from([(2, InodeEntry::new(path.clone(), lookup, Some(stamp), 0))]);
        let by_path = HashMap::from([(path.clone(), 2)]);

        assert!(cached_projected_lookup(&mut by_inode, &by_path, &path, |_, _| false).is_none());
        assert_eq!(by_inode[&2].lookup_references, 0);
        assert_eq!(
            cached_projected_lookup(&mut by_inode, &by_path, &path, |file_id, sampled| {
                file_id == lookup.node.file_id && sampled == stamp
            }),
            Some((2, lookup))
        );
        assert_eq!(by_inode[&2].lookup_references, 1);
    }

    #[test]
    fn root_inode_survives_checkout_identity_changes() -> Result<(), i32> {
        let old_file = crate::FileId::from_bytes([1; 16]);
        let new_file = crate::FileId::from_bytes([2; 16]);
        let root = |file_id| MountLookup {
            node: MountNode {
                file_id,
                kind: MountNodeKind::Directory,
                logical_bytes: 0,
                link_count: 1,
                device: None,
            },
            metadata: FileMetadata::default(),
        };
        let mut by_inode = HashMap::from([(
            ROOT_INODE,
            InodeEntry::new(MountPath::root(), root(old_file), None, 1),
        )]);
        let mut inode_by_file = HashMap::from([(old_file, ROOT_INODE)]);
        let stamp = ViewStamp::current();

        replace_root_lookup(
            &mut by_inode,
            &mut inode_by_file,
            root(new_file),
            Some(stamp),
        )?;

        assert_eq!(by_inode[&ROOT_INODE].lookup.node.file_id, new_file);
        assert_eq!(by_inode[&ROOT_INODE].bindings, vec![MountPath::root()]);
        assert_eq!(inode_by_file.get(&new_file), Some(&ROOT_INODE));
        assert!(!inode_by_file.contains_key(&old_file));
        assert_eq!(by_inode[&ROOT_INODE].view_stamp, Some(stamp));
        Ok(())
    }

    #[test]
    fn enumerated_entries_receive_nonzero_inodes_without_fake_lookup_references() -> Result<(), i32>
    {
        let mut next_inode = ROOT_INODE + 1;
        let mut by_inode = HashMap::<u64, InodeEntry>::new();
        let mut inode_by_path = HashMap::new();
        let mut inode_by_file = HashMap::new();
        let lookup = MountLookup {
            node: MountNode {
                file_id: crate::FileId::new(),
                kind: MountNodeKind::Regular,
                logical_bytes: 4,
                link_count: 1,
                device: None,
            },
            metadata: FileMetadata::default(),
        };
        let path = MountPath::root().child(b"preexisting.bin".to_vec());

        let enumerated = intern_projected(
            &mut next_inode,
            &mut by_inode,
            &mut inode_by_path,
            &mut inode_by_file,
            path.clone(),
            &lookup,
            None,
            false,
        )?;
        assert_ne!(enumerated, 0);
        assert_eq!(by_inode[&enumerated].lookup_references, 0);

        let looked_up = intern_projected(
            &mut next_inode,
            &mut by_inode,
            &mut inode_by_path,
            &mut inode_by_file,
            path,
            &lookup,
            None,
            true,
        )?;
        assert_eq!(looked_up, enumerated);
        assert_eq!(by_inode[&enumerated].lookup_references, 1);

        let retained_by_inode_count = by_inode.len();
        let retained_inode_by_file_count = inode_by_file.len();
        next_inode = u64::MAX;
        let overflow = MountLookup {
            node: MountNode {
                file_id: crate::FileId::new(),
                ..lookup.node
            },
            ..lookup
        };
        assert_eq!(
            intern_projected(
                &mut next_inode,
                &mut by_inode,
                &mut inode_by_path,
                &mut inode_by_file,
                MountPath::root().child(b"overflow.bin".to_vec()),
                &overflow,
                None,
                false,
            ),
            Err(libc::EOVERFLOW)
        );
        assert_eq!(next_inode, u64::MAX);
        assert_eq!(by_inode.len(), retained_by_inode_count);
        assert_eq!(inode_by_file.len(), retained_inode_by_file_count);
        assert_eq!(by_inode[&enumerated].lookup_references, 1);
        assert_eq!(inode_by_file[&lookup.node.file_id], enumerated);
        Ok(())
    }
}
