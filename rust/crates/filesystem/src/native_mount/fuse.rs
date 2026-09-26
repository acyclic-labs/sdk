//! Linux FUSE projection over the common callback contract.
//!
//! Callbacks share one projection state that holds only in-memory records
//! and is never held across a source call: each callback reads what it needs
//! under the state lock, calls the source without it, and applies the result
//! under the lock again. A namespace lock orders callbacks that resolve names
//! against the removals and renames that change them, so no lookup observes
//! a name change in the source before the projection's records do.
//!
//! The kernel keeps entries, attributes, and data without expiry. Changes
//! made through this mount reach it as it makes them; an invalidator thread
//! drops exactly the kernel items that every other change to the source
//! superseded, as the source reports them. A small file's content reaches
//! the kernel with its first read-only open rather than page by page
//! ([`PAGE_STORE_LIMIT`]).
//!
//! # Inode identity
//!
//! An inode stands for one source node, the node its file identity named
//! when the inode was made, and records the names it was reached through.
//! One rule decides which inode a name gets:
//!
//! **A name joins the inode of its node's identity only while that inode
//! still stands for the same node. Otherwise that inode is retired and the
//! name gets a fresh one.**
//!
//! The inode still stands for the node while another of its names is still
//! bound to the identity: a host reuses an identity only once the node that
//! had it lost its last name, so one name still bound shows the node is the
//! one the inode was made for. Each recorded name is judged as of the moment
//! the joining name's facts were resolved, and is kept only on evidence that
//! it is still bound:
//!
//! - the view vouches that the name has not changed since it was last seen
//!   bound (a watched source reports every change to it); or
//! - a resolution of the name, made with the joining name's facts and outside
//!   the state's lock, found the same identity. One made at a position holds
//!   only while the view vouches that the name has not changed since, since a
//!   listed page's entries are used after the page was read. One from a
//!   source that cannot be watched has no position and is exactly as current
//!   as the facts it came with, which such a source reads afresh whenever
//!   they are used again.
//!
//! A node with one name has no other name to keep. A name without evidence
//! is forgotten, and an inode left without names is retired: its identity
//! names no inode, the next name bound to the identity gets a fresh one, and
//! a request through the retired inode that needs a name fails as stale. The
//! mount's own links and renames move names between inodes it already knows,
//! so they need no evidence. Whether a node's facts are current is a separate
//! question: a node with several names is read afresh on every use, but its
//! names stay bound under this rule.

use super::view_ledger::ViewOriginScope;
use super::{
    MountDirectoryEntry, MountFilesystem, MountLookup, MountNode, MountNodeKind, MountOpenFile,
    MountPath, MountSeekTarget, MountSourceError, NativeMountError, NativeMountRequest,
    ViewObserver, ViewOrigin, ViewStamp, metadata_or, system_time_ns,
};
use crate::kernel::{FileMetadata, MetadataField};
use bytes::Bytes;
use fuser::{
    BackgroundSession, BsdFileFlags, Config, CopyFileRangeFlags, Errno, FileAttr,
    FileHandle as FuseFileHandle, FileType, Filesystem, FopenFlags, Generation, INodeNo, InitFlags,
    KernelConfig, LockOwner, MountOption, Notifier, OpenFlags, RenameFlags, ReplyAttr, ReplyCreate,
    ReplyData, ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyLseek, ReplyOpen,
    ReplyWrite, ReplyXattr, Request, TimeOrNow, WriteFlags,
};
use std::collections::{HashMap, VecDeque};
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{
    Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError, RwLock, RwLockReadGuard,
    RwLockWriteGuard, Weak,
};
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
/// The path every detached inode is anchored at for view checks.
static ROOT_PATH: MountPath = MountPath::root();
/// Kernel lifetime of entries, negative entries, attributes, symlink targets,
/// and retained file and directory data. Nothing here expires by time: the
/// kernel applies mounted mutations itself, and the invalidator drops what
/// every other change to the source superseded.
const CACHE_TTL: Duration = Duration::from_secs(24 * 60 * 60);
/// Largest file a first read-only open hands the kernel whole, as the
/// default FUSE readahead window bounds a first read's fetch.
///
/// Every page the kernel reads from the daemon marks the inode's cached
/// access time stale unless the whole superblock is read-only
/// (`fuse_invalidate_atime`, even under `noatime`), and `stat` always asks
/// for it: a file read once would cost one `GETATTR` round trip at its next
/// `stat`. Pages stored with the open (`FUSE_NOTIFY_STORE`) are never read,
/// so they spare both that round trip and the `READ` itself. Larger files
/// are read on demand as before.
const PAGE_STORE_LIMIT: u64 = 128 * 1024;
/// Negative entries tracked per directory. Invalidation must reach every
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
/// Directory listings a projection keeps positioned for the kernel.
const MAXIMUM_DIRECTORY_STREAMS: usize = 256;

/// One position in a directory listing the kernel is reading.
///
/// Listings need no open handle: the kernel resumes one at the offset it
/// last received, so streams wait keyed by inode and offset. That lets the
/// kernel skip `OPENDIR` and `RELEASEDIR` entirely where it supports that.
/// Names the source was seen binding to the node each is paired with, after
/// the view could no longer vouch for them, each with the position it was
/// resolved after: the evidence the module's inode identity rule accepts
/// from a resolution.
///
/// A confirmation made at a position holds only while the view vouches that
/// its name has not changed since. One without a position, from a source
/// that cannot report its changes, is as current as the facts it was
/// resolved with, which such a source reads afresh whenever they are used
/// again.
#[derive(Default)]
struct ConfirmedNames {
    names: HashMap<(crate::FileId, MountPath), Option<ViewStamp>>,
}

impl ConfirmedNames {
    fn confirm(&mut self, file_id: crate::FileId, name: MountPath, stamp: Option<ViewStamp>) {
        self.names.insert((file_id, name), stamp);
    }

    /// Whether `name` is confirmed bound to `file_id` now.
    fn still_bound(
        &self,
        source: &dyn MountFilesystem,
        file_id: crate::FileId,
        name: &MountPath,
    ) -> bool {
        self.names
            .get(&(file_id, name.clone()))
            .is_some_and(|stamp| {
                stamp.is_none_or(|stamp| source.unchanged_since(name, None, stamp))
            })
    }
}

struct DirectoryStream {
    path: MountPath,
    parent_inode: u64,
    binding_epoch: Option<u64>,
    cursor: Option<Vec<u8>>,
    entries: VecDeque<MountDirectoryEntry>,
    /// Source view the buffered entries were read after, if cacheable.
    entries_stamp: Option<ViewStamp>,
    /// Names of the buffered entries' nodes the source still binds.
    confirmed: ConfirmedNames,
    exhausted: bool,
    /// Offset of the next entry: `.` and `..` are 1 and 2.
    emitted: u64,
    /// When this stream was last parked, for eviction.
    parked: u64,
}

/// Streams waiting for the kernel's next listing request, at most
/// [`MAXIMUM_DIRECTORY_STREAMS`]. A request whose stream was evicted
/// restarts the listing and skips to its offset.
#[derive(Default)]
struct DirectoryStreams {
    waiting: HashMap<(u64, u64), DirectoryStream>,
    parked: u64,
}

impl DirectoryStreams {
    fn take(&mut self, inode: u64, offset: u64) -> Option<DirectoryStream> {
        self.waiting.remove(&(inode, offset))
    }

    fn park(&mut self, inode: u64, mut stream: DirectoryStream) {
        if self.waiting.len() >= MAXIMUM_DIRECTORY_STREAMS
            && let Some(oldest) = self
                .waiting
                .iter()
                .min_by_key(|(_, stream)| stream.parked)
                .map(|(key, _)| *key)
        {
            self.waiting.remove(&oldest);
        }
        self.parked = self.parked.wrapping_add(1);
        stream.parked = self.parked;
        self.waiting.insert((inode, stream.emitted), stream);
    }

    fn rename_prefix(&mut self, source: &MountPath, destination: &MountPath) {
        for stream in self.waiting.values_mut() {
            if let Some(rebased) = replace_prefix(&stream.path, source, destination) {
                stream.path = rebased;
            }
        }
    }
}

/// One name of an inode.
struct Binding {
    path: MountPath,
    /// Position after which this name was last seen resolving to the node;
    /// the name's lookups are reusable while nothing changed since.
    verified: Option<ViewStamp>,
    /// Lower bound on the position the kernel's entry for this name was
    /// derived after; `None` while the kernel holds no such entry.
    kernel: Option<ViewStamp>,
}

impl Binding {
    fn new(path: MountPath, verified: Option<ViewStamp>) -> Self {
        Self {
            path,
            verified,
            kernel: None,
        }
    }
}

struct InodeEntry {
    bindings: Vec<Binding>,
    lookup: MountLookup,
    /// Position after which `lookup` was read, through a name or a handle;
    /// `None` when unknown.
    facts: Option<ViewStamp>,
    /// Lower bound on the position everything the kernel holds for this
    /// inode (attributes, pages, listing, link target) was derived after.
    kernel: Option<ViewStamp>,
    lookup_references: u64,
    open_handles: u64,
    /// Version the kernel page cache was last admitted under.
    cached_content: Option<ContentVersion>,
    /// Absent child names the kernel may hold as negative entries, each with
    /// a lower bound on the position its absence was derived after.
    negative_children: HashMap<Vec<u8>, ViewStamp>,
}

impl InodeEntry {
    fn new(
        binding: MountPath,
        lookup: MountLookup,
        verified: Option<ViewStamp>,
        lookup_references: u64,
    ) -> Self {
        Self {
            bindings: vec![Binding::new(binding, verified)],
            lookup,
            facts: verified,
            kernel: None,
            lookup_references,
            open_handles: 0,
            cached_content: None,
            negative_children: HashMap::new(),
        }
    }

    fn binding(&self, path: &MountPath) -> Option<&Binding> {
        self.bindings.iter().find(|binding| binding.path == *path)
    }

    fn binding_mut(&mut self, path: &MountPath) -> Option<&mut Binding> {
        self.bindings
            .iter_mut()
            .find(|binding| binding.path == *path)
    }

    /// The name view checks of the node itself go through.
    fn anchor(&self) -> &MountPath {
        self.bindings
            .first()
            .map_or(&ROOT_PATH, |binding| &binding.path)
    }

    /// The position the node facts are current after, if they still are.
    ///
    /// A directory's attributes follow its listing, which the source tracks
    /// by path; every other node's attributes change only with the node.
    fn current_facts(&self, source: &dyn MountFilesystem) -> Option<ViewStamp> {
        let stamp = self.facts?;
        let file_id = self.lookup.node.file_id;
        let current = if self.lookup.node.kind == MountNodeKind::Directory {
            self.bindings
                .first()
                .is_some_and(|anchor| source.unchanged_since(&anchor.path, Some(file_id), stamp))
        } else {
            source.node_unchanged_since(file_id, stamp)
        };
        current.then_some(stamp)
    }

    /// Whether the node certainly has no named attributes: its current
    /// metadata carries no attribute table, which every source reads first.
    fn lacks_named_attributes(&self, source: &dyn MountFilesystem) -> bool {
        self.lookup.metadata.named_attributes == MetadataField::Unavailable
            && self.current_facts(source).is_some()
    }

    /// Records the version a new handle opens and reports what the kernel
    /// may hold of the file's pages: none, when no handle opened it before,
    /// or pages earlier handles left under the same or another version.
    fn admit_cached_content(&mut self, lookup: &MountLookup) -> CachedContent {
        let opened = ContentVersion::of(lookup);
        match self.cached_content.replace(opened) {
            None => CachedContent::Absent,
            Some(cached) if cached == opened => CachedContent::Current,
            Some(_) => CachedContent::Superseded,
        }
    }

    /// Records that the kernel may hold `name` as a negative entry derived
    /// after `stamp`; false when this directory already tracks its maximum.
    fn remember_negative_child(&mut self, name: &[u8], stamp: ViewStamp) -> bool {
        if let Some(held) = self.negative_children.get_mut(name) {
            *held = (*held).min(stamp);
            return true;
        }
        self.negative_children.len() < MAXIMUM_NEGATIVE_ENTRIES_PER_DIRECTORY
            && self
                .negative_children
                .insert(name.to_vec(), stamp)
                .is_none()
    }
}

/// Lowers `held` to cover one more item derived after `stamp`.
fn hold(held: &mut Option<ViewStamp>, stamp: ViewStamp) {
    *held = Some(held.map_or(stamp, |held| held.min(stamp)));
}

/// Size and times a regular file was opened with. Within the source's cache
/// contract, bytes change only through this mount, which updates the kernel
/// page cache itself, or around it, which the invalidator drops; a differing
/// version also drops pages left by content changed around that contract.
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

/// What the kernel may hold of one file's pages as a new handle opens it.
///
/// Pages reach the kernel only through open handles, so a file no handle
/// opened before has none: the kernel inode is as new as the projection's.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CachedContent {
    Absent,
    /// Pages admitted under the version the handle opens.
    Current,
    /// Pages admitted under another version.
    Superseded,
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
    operation: Arc<Mutex<()>>,
    dirty: bool,
    /// The file whose content this handle claims, when it may change it.
    claim: Option<crate::FileId>,
}

/// A read-only handle to a file whose pages the kernel already holds at the
/// version this handle opens. Reads come from those pages, so the source file
/// opens only when a use reaches it, and serves that use only while it is
/// still the file and version the handle opened.
struct DeferredOpenFile {
    source: Arc<dyn MountFilesystem>,
    path: MountPath,
    opened: MountLookup,
    file: Mutex<Option<Arc<dyn MountOpenFile>>>,
}

impl DeferredOpenFile {
    fn file(&self) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let mut file = self.file.lock().map_err(|_| MountSourceError::Stale)?;
        if let Some(file) = file.as_ref() {
            return Ok(Arc::clone(file));
        }
        let opened = self.source.open_file(&self.path)?;
        let current = opened.lookup()?;
        if current.node.file_id != self.opened.node.file_id
            || ContentVersion::of(&current) != ContentVersion::of(&self.opened)
        {
            return Err(MountSourceError::Stale);
        }
        *file = Some(Arc::clone(&opened));
        Ok(opened)
    }
}

impl MountOpenFile for DeferredOpenFile {
    fn lookup(&self) -> Result<MountLookup, MountSourceError> {
        self.file()?.lookup()
    }

    fn read_range(&self, offset: u64, length: u32) -> Result<Bytes, MountSourceError> {
        self.file()?.read_range(offset, length)
    }

    fn read_up_to(&self, offset: u64, maximum_bytes: u32) -> Result<Bytes, MountSourceError> {
        self.file()?.read_up_to(offset, maximum_bytes)
    }

    fn seek(&self, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError> {
        self.file()?.seek(offset, target)
    }

    fn write_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
        self.file()?.write_range(offset, bytes)
    }

    fn resize(&self, logical_bytes: u64) -> Result<(), MountSourceError> {
        self.file()?.resize(logical_bytes)
    }

    fn allocate_range(
        &self,
        offset: u64,
        length: u64,
        operation: super::MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        self.file()?.allocate_range(offset, length, operation)
    }

    fn set_attributes(
        &self,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        self.file()?.set_attributes(metadata, logical_bytes)
    }

    fn read_attribute(&self, name: &[u8]) -> Result<Option<Bytes>, MountSourceError> {
        self.file()?.read_attribute(name)
    }

    fn list_attributes(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<super::MountAttributePage, MountSourceError> {
        self.file()?.list_attributes(cursor, maximum_entries)
    }

    fn write_attribute(
        &self,
        name: &[u8],
        value: Bytes,
        mode: super::MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        self.file()?.write_attribute(name, value, mode)
    }

    fn remove_attribute(&self, name: &[u8]) -> Result<(), MountSourceError> {
        self.file()?.remove_attribute(name)
    }
}

/// A handle this session opened for a file the kernel opened without
/// asking.
#[derive(Clone, Copy)]
struct Implicit {
    handle: u64,
    writes: bool,
}

/// What I/O the kernel sent without a handle needs of the inode's own.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Access {
    Read,
    Write,
}

/// Whether an open with `flags` may change the file's content.
fn changes_content(flags: i32) -> bool {
    flags & libc::O_ACCMODE != libc::O_RDONLY || flags & libc::O_TRUNC != 0
}

/// Page stores in flight, and the claims that exclude them, per file.
///
/// A store reads a file's content and hands it to the kernel page cache
/// outside every lock, so it must never land over content this mount
/// changed meanwhile: a store begins only while nothing claims the file's
/// content, and a claim is granted only once the file's store has landed.
/// Changes around the mount are checked as a store lands, and the
/// revalidation barrier waits for every store read before its target.
#[derive(Default)]
struct PageStores {
    /// Position each store's content was read after, by file.
    in_flight: HashMap<crate::FileId, ViewStamp>,
    /// Handles that may change each file's content, and callbacks changing
    /// it now.
    claims: HashMap<crate::FileId, u64>,
}

impl PageStores {
    fn may_begin(&self, file_id: crate::FileId) -> bool {
        !self.in_flight.contains_key(&file_id) && !self.claims.contains_key(&file_id)
    }

    fn begin(&mut self, file_id: crate::FileId, stamp: ViewStamp) {
        debug_assert!(self.may_begin(file_id));
        self.in_flight.insert(file_id, stamp);
    }

    fn land(&mut self, file_id: crate::FileId) {
        self.in_flight.remove(&file_id);
    }

    /// Adds a claim on `file_id`, whose store must already have landed.
    fn claim(&mut self, file_id: crate::FileId) {
        debug_assert!(!self.in_flight.contains_key(&file_id));
        *self.claims.entry(file_id).or_default() += 1;
    }

    fn release(&mut self, file_id: crate::FileId) {
        if let Some(claims) = self.claims.get_mut(&file_id) {
            *claims -= 1;
            if *claims == 0 {
                self.claims.remove(&file_id);
            }
        }
    }

    /// Whether a store in flight read its content before `target`.
    fn precede(&self, target: ViewStamp) -> bool {
        self.in_flight.values().any(|stamp| *stamp < target)
    }
}

/// A store of one file's whole content that an open began, which the open
/// completes before it replies.
struct PageStore {
    inode: u64,
    file_id: crate::FileId,
    path: MountPath,
    stamp: ViewStamp,
    length: u32,
    open_file: Arc<dyn MountOpenFile>,
}

/// One handle a callback opened, for the projection to admit.
struct OpenedFile<'a> {
    path: &'a MountPath,
    lookup: &'a MountLookup,
    /// Position `lookup` was read after.
    stamp: Option<ViewStamp>,
    flags: i32,
    open_file: Arc<dyn MountOpenFile>,
    dirty: bool,
    /// Whether the open may send the kernel small files' pages.
    stores_pages: bool,
}

/// Mount-wide facts every attribute reply is built from.
#[derive(Clone, Copy)]
struct AttributeDefaults {
    writable: bool,
    mount_uid: u32,
    mount_gid: u32,
}

/// Kernel invalidation owed to changes made around this mount.
#[derive(Default)]
struct InvalidationQueue {
    /// Latest change by another origin the source reported.
    notified: Option<ViewStamp>,
    /// Latest reported change every kernel item was checked against. A reply
    /// admitted later is checked against it instead.
    scanned: Option<ViewStamp>,
    /// Latest reported change whose invalidations reached the kernel.
    processed: Option<ViewStamp>,
    /// Items to drop regardless of any change.
    deferred: Vec<KernelCacheItem>,
    /// First invalidation the kernel rejected since the last barrier.
    failure: Option<String>,
}

impl InvalidationQueue {
    fn is_idle(&self) -> bool {
        self.processed >= self.notified && self.deferred.is_empty()
    }
}

/// Node facts one callback read from the source.
struct Resolved {
    lookup: MountLookup,
    /// Position the facts were read after.
    stamp: Option<ViewStamp>,
    /// The name they were resolved through; `None` for an open handle.
    through: Option<MountPath>,
}

/// How a callback reads current facts for one inode from the source.
enum NodeFacts {
    /// Read through an open handle.
    Handle(Arc<dyn MountOpenFile>),
    /// Resolve through the first name still bound to `file_id` (any node,
    /// for the root, whose identity follows the checkout).
    Names {
        file_id: Option<crate::FileId>,
        paths: Vec<MountPath>,
    },
}

/// What a named-attribute callback reads or changes.
enum AttributeTarget {
    Handle(Arc<dyn MountOpenFile>),
    Path(MountPath),
}

/// One name under a directory, as a caller spelled it and as the projection
/// records it.
///
/// A source that folds names resolves every spelling of a name to one entry,
/// while the kernel caches each spelling as its own entry. The projection
/// keys its records by the folded spelling so all spellings share them, and
/// the kernel never keeps a folded name's entry: a change through one
/// spelling could not reach the entries of the others.
struct ChildName {
    spelled: MountPath,
    folded: Option<MountPath>,
}

impl ChildName {
    fn new(source: &dyn MountFilesystem, spelled: MountPath) -> Self {
        let folded = source.folded_path(&spelled);
        Self { spelled, folded }
    }

    /// The path the projection's records use.
    fn key(&self) -> &MountPath {
        self.folded.as_ref().unwrap_or(&self.spelled)
    }

    /// Whether the kernel may cache this spelling's entry.
    fn is_exact(&self) -> bool {
        self.folded.is_none()
    }
}

/// One `LOOKUP`-style reply.
struct Entry {
    attr: FileAttr,
    ttl: Duration,
}

impl Entry {
    fn reply(self, reply: ReplyEntry) {
        reply.entry(&self.ttl, &self.attr, Generation(0));
    }
}

/// Every in-memory record of one projection. Nothing here calls the source
/// in a way that can wait, so the lock guarding it is only ever held briefly.
struct ProjectionState {
    defaults: AttributeDefaults,
    next_inode: u64,
    next_handle: u64,
    by_inode: HashMap<u64, InodeEntry>,
    inode_by_path: HashMap<MountPath, u64>,
    inode_by_file: HashMap<crate::FileId, u64>,
    files: HashMap<u64, FileHandle>,
    /// The handle this session opened for each inode whose I/O the kernel
    /// sends without one (see [`FuseProjection::skips_open`]), and whether
    /// it writes, until the kernel forgets the inode.
    implicit: HashMap<u64, Implicit>,
    streams: DirectoryStreams,
    invalidation: InvalidationQueue,
    page_stores: PageStores,
}

impl ProjectionState {
    fn path(&self, inode: u64) -> Result<&MountPath, i32> {
        self.by_inode
            .get(&inode)
            .and_then(|entry| entry.bindings.first())
            .map(|binding| &binding.path)
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

    fn admit_write(&self) -> Result<(), i32> {
        self.defaults.writable.then_some(()).ok_or(libc::EROFS)
    }

    /// Records facts about `path`, resolved after `stamp`, and returns the
    /// inode they belong to. `confirmed` holds names the source was seen
    /// binding to their nodes since the view last vouched for them (see
    /// [`FuseProjection::confirm_names`]).
    fn intern(
        &mut self,
        source: &dyn MountFilesystem,
        path: MountPath,
        lookup: &MountLookup,
        stamp: Option<ViewStamp>,
        lookup_reference: bool,
        confirmed: &ConfirmedNames,
    ) -> Result<u64, i32> {
        if let Some(previous) = self.inode_by_path.get(&path).copied()
            && self
                .by_inode
                .get(&previous)
                .is_some_and(|entry| entry.lookup.node.file_id != lookup.node.file_id)
        {
            // The kernel's entry for the name still names the old node.
            let held = self
                .by_inode
                .get(&previous)
                .and_then(|entry| entry.binding(&path))
                .is_some_and(|binding| binding.kernel.is_some());
            if held
                && let Some((parent, name)) = split_parent(&path)
                && let Some(parent) = self.inode_by_path.get(&parent).copied()
            {
                self.invalidation.deferred.push(KernelCacheItem::Entry {
                    parent,
                    name: name.to_vec(),
                });
            }
            self.remove_binding(previous, &path);
        }
        if let Some(inode) = self.inode_by_file.get(&lookup.node.file_id).copied() {
            let forgotten = forget_unbound_names(
                &mut self.by_inode,
                &mut self.inode_by_path,
                inode,
                &path,
                |name, verified| {
                    confirmed.still_bound(source, lookup.node.file_id, name)
                        || verified
                            .is_some_and(|verified| source.unchanged_since(name, None, verified))
                },
            );
            self.invalidation.deferred.extend(forgotten);
            retire_reused_identity(&self.by_inode, &mut self.inode_by_file, inode);
        }
        intern_projected(
            &mut self.next_inode,
            &mut self.by_inode,
            &mut self.inode_by_path,
            &mut self.inode_by_file,
            path,
            lookup,
            stamp,
            lookup_reference,
        )
    }

    /// The names other than `path` recorded for the node `file_id` names
    /// that the view cannot vouch are still bound to it.
    fn unvouched_names(
        &self,
        source: &dyn MountFilesystem,
        file_id: crate::FileId,
        path: &MountPath,
    ) -> Vec<MountPath> {
        self.inode_by_file
            .get(&file_id)
            .and_then(|inode| self.by_inode.get(inode))
            .map(|entry| {
                entry
                    .bindings
                    .iter()
                    .filter(|binding| {
                        binding.path != *path
                            && !binding.verified.is_some_and(|verified| {
                                source.unchanged_since(&binding.path, None, verified)
                            })
                    })
                    .map(|binding| binding.path.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn release_lookup_reference(&mut self, inode: u64, references: u64) {
        if inode == ROOT_INODE {
            return;
        }
        let forgotten = self.by_inode.get_mut(&inode).is_some_and(|entry| {
            entry.lookup_references = entry.lookup_references.saturating_sub(references);
            entry.lookup_references == 0
        });
        // The kernel forgets only an inode no open file holds, so the
        // handle its I/O arrived on is done.
        if forgotten && let Some(implicit) = self.implicit.remove(&inode) {
            let _ = self.discard_file_handle(inode, implicit.handle);
        }
        let remove = self
            .by_inode
            .get(&inode)
            .is_some_and(|entry| entry.lookup_references == 0 && entry.open_handles == 0);
        if remove && let Some(entry) = self.by_inode.remove(&inode) {
            for binding in &entry.bindings {
                if self.inode_by_path.get(&binding.path) == Some(&inode) {
                    self.inode_by_path.remove(&binding.path);
                }
            }
            forget_file(&mut self.inode_by_file, entry.lookup.node.file_id, inode);
        }
    }

    fn cached_lookup(
        &mut self,
        source: &dyn MountFilesystem,
        path: &MountPath,
    ) -> Option<(u64, MountLookup, ViewStamp)> {
        cached_projected_lookup(
            &mut self.by_inode,
            &self.inode_by_path,
            path,
            |file_id, stamp| source.unchanged_since(path, Some(file_id), stamp),
        )
    }

    /// Whether facts about `file_id` at `anchor`, derived after `stamp`, may
    /// enter the kernel: the source reports every change to the node, so the
    /// invalidator will drop them once one supersedes them, and every change
    /// the invalidator already checked the kernel against either precedes
    /// them or left them unchanged. Every kernel admission (entry and
    /// attribute lifetimes, negative entries, listings, kept pages, and page
    /// stores) is decided here.
    fn admissible(
        &self,
        source: &dyn MountFilesystem,
        anchor: &MountPath,
        file_id: Option<crate::FileId>,
        stamp: Option<ViewStamp>,
    ) -> bool {
        stamp.is_some_and(|stamp| {
            file_id.is_none_or(|file_id| source.reports_changes_to(file_id))
                && (self
                    .invalidation
                    .scanned
                    .is_none_or(|scanned| scanned <= stamp)
                    || source.unchanged_since(anchor, file_id, stamp))
        })
    }

    /// Records that a reply hands the kernel `inode`'s attributes, derived
    /// after `stamp`; returns their cache lifetime.
    fn admit_attributes(
        &mut self,
        source: &dyn MountFilesystem,
        inode: u64,
        stamp: Option<ViewStamp>,
    ) -> Duration {
        let Some(entry) = self.by_inode.get(&inode) else {
            return Duration::ZERO;
        };
        let admitted = self.admissible(
            source,
            entry.anchor(),
            Some(entry.lookup.node.file_id),
            stamp,
        );
        if let Some(entry) = self.by_inode.get_mut(&inode) {
            hold(&mut entry.kernel, stamp.unwrap_or(ViewStamp::ORIGIN));
        }
        if admitted { CACHE_TTL } else { Duration::ZERO }
    }

    /// Records that a reply hands the kernel `name`'s entry for `inode`,
    /// with its attributes, derived after `stamp`; returns the lifetime of
    /// both, which share one reply.
    fn admit_entry(
        &mut self,
        source: &dyn MountFilesystem,
        inode: u64,
        name: &ChildName,
        stamp: Option<ViewStamp>,
    ) -> Duration {
        let attributes = self.admit_attributes(source, inode, stamp);
        if !name.is_exact() {
            return Duration::ZERO;
        }
        let Some(entry) = self.by_inode.get(&inode) else {
            return Duration::ZERO;
        };
        let admitted = self.admissible(source, name.key(), Some(entry.lookup.node.file_id), stamp);
        if let Some(binding) = self
            .by_inode
            .get_mut(&inode)
            .and_then(|entry| entry.binding_mut(name.key()))
        {
            hold(&mut binding.kernel, stamp.unwrap_or(ViewStamp::ORIGIN));
        }
        if admitted { attributes } else { Duration::ZERO }
    }

    /// The position after which `name` under `parent` was last found absent,
    /// as the negative entry the kernel holds for it records.
    fn absent_since(&self, parent: u64, name: &ChildName) -> Option<ViewStamp> {
        let component = name.key().components().last()?;
        name.is_exact()
            .then(|| {
                self.by_inode
                    .get(&parent)?
                    .negative_children
                    .get(component)
                    .copied()
            })
            .flatten()
    }

    /// Records a negative entry for `name` under `parent`, derived after
    /// `stamp`, returning its cache lifetime when the kernel may keep it.
    fn admit_negative(
        &mut self,
        source: &dyn MountFilesystem,
        parent: u64,
        name: &ChildName,
        stamp: Option<ViewStamp>,
    ) -> Option<Duration> {
        let stamp = stamp
            .filter(|_| name.is_exact() && self.admissible(source, name.key(), None, stamp))?;
        let component = name.key().components().last()?;
        self.by_inode
            .get_mut(&parent)
            .is_some_and(|entry| entry.remember_negative_child(component, stamp))
            .then_some(CACHE_TTL)
    }

    /// Records a directory listing handed to the kernel, read after `stamp`;
    /// one the kernel may not keep is dropped by the invalidator.
    fn admit_listing(
        &mut self,
        source: &dyn MountFilesystem,
        inode: u64,
        stamp: Option<ViewStamp>,
    ) {
        let Some(entry) = self.by_inode.get(&inode) else {
            return;
        };
        let admitted = self.admissible(
            source,
            entry.anchor(),
            Some(entry.lookup.node.file_id),
            stamp,
        );
        if let Some(entry) = self.by_inode.get_mut(&inode) {
            hold(&mut entry.kernel, stamp.unwrap_or(ViewStamp::ORIGIN));
        }
        if !admitted {
            self.invalidation
                .deferred
                .push(KernelCacheItem::Inode(inode));
        }
    }

    /// Applies facts one callback resolved, returning the attribute reply.
    fn apply(
        &mut self,
        source: &dyn MountFilesystem,
        inode: u64,
        resolved: &Resolved,
    ) -> Result<(FileAttr, Duration), i32> {
        let lookup = resolved.lookup;
        if inode == ROOT_INODE {
            replace_root_lookup(&mut self.by_inode, &mut self.inode_by_file, lookup)?;
        }
        let entry = self.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        if entry.lookup.node.file_id != lookup.node.file_id {
            return Err(libc::ESTALE);
        }
        entry.lookup = lookup;
        entry.facts = resolved.stamp;
        // A handle can outlive its names, so only a name it was resolved
        // through is verified.
        if let Some(binding) = resolved
            .through
            .as_ref()
            .and_then(|path| entry.binding_mut(path))
        {
            binding.verified = resolved.stamp;
        }
        let ttl = self.admit_attributes(source, inode, resolved.stamp);
        Ok((self.attr(inode, &lookup)?, ttl))
    }

    /// Current facts for `inode`: the projection's own record while it is
    /// current and no handle asks for its own, or else how to read them.
    fn node_facts(
        &self,
        source: &dyn MountFilesystem,
        inode: u64,
        handle: Option<u64>,
    ) -> Result<Result<Resolved, NodeFacts>, i32> {
        let entry = self.by_inode.get(&inode).ok_or(libc::ESTALE)?;
        if let Some(handle) = handle {
            let open_file = self.open_handle(inode, handle)?;
            // The handle names the inode's file, whose changes are recorded
            // by identity: facts nothing changed since answer for it too.
            return Ok(match entry.current_facts(source) {
                Some(stamp) => Ok(Resolved {
                    lookup: entry.lookup,
                    stamp: Some(stamp),
                    through: None,
                }),
                None => Err(NodeFacts::Handle(open_file)),
            });
        }
        if let Some(stamp) = entry.current_facts(source) {
            return Ok(Ok(Resolved {
                lookup: entry.lookup,
                stamp: Some(stamp),
                through: entry.bindings.first().map(|binding| binding.path.clone()),
            }));
        }
        if entry.bindings.is_empty() {
            return self
                .open_inode(inode)
                .map(|open_file| Err(NodeFacts::Handle(open_file)))
                .ok_or(libc::ESTALE);
        }
        Ok(Err(NodeFacts::Names {
            file_id: (inode != ROOT_INODE).then_some(entry.lookup.node.file_id),
            paths: entry
                .bindings
                .iter()
                .map(|binding| binding.path.clone())
                .collect(),
        }))
    }

    fn node_attr(&self, inode: u64) -> Result<FileAttr, i32> {
        let lookup = self.by_inode.get(&inode).ok_or(libc::ESTALE)?.lookup;
        self.attr(inode, &lookup)
    }

    fn attr(&self, inode: u64, lookup: &MountLookup) -> Result<FileAttr, i32> {
        let node = lookup.node;
        let kind = file_type(node.kind)?;
        let defaults = self.defaults;
        let default_mode = if defaults.writable { 0o755 } else { 0o555 };
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
            uid: metadata_or(lookup.metadata.posix_uid, defaults.mount_uid),
            gid: metadata_or(lookup.metadata.posix_gid, defaults.mount_gid),
            rdev: node
                .device
                .map(|(major, minor)| native_device_number(major, minor))
                .transpose()?
                .unwrap_or(0),
            blksize: 4096,
            flags: u32::try_from(metadata_or(lookup.metadata.posix_flags, 0)).unwrap_or(u32::MAX),
        })
    }

    /// Admits a new handle on `inode`, returning it with its kernel flags
    /// and the page store its open completes, if any.
    ///
    /// Pages retained from earlier handles are kept only while the file opens
    /// with the [`ContentVersion`] they were admitted under and the kernel may
    /// keep what it opens. A read-only open of a small file the kernel holds
    /// no pages of hands it the whole content instead ([`PAGE_STORE_LIMIT`]).
    /// A handle that may change the content claims it ([`PageStores`]); the
    /// caller already holds a claim, so no store of the file is in flight.
    /// Closing a handle is a publication boundary only when it may write and
    /// the source publishes on close; every other close needs no FLUSH round
    /// trip.
    fn admit_file_handle(
        &mut self,
        source: &dyn MountFilesystem,
        inode: u64,
        opened: OpenedFile<'_>,
    ) -> Result<(u64, FopenFlags, Option<PageStore>), i32> {
        let OpenedFile {
            path,
            lookup,
            stamp,
            flags,
            open_file,
            dirty,
            stores_pages,
        } = opened;
        let file_id = lookup.node.file_id;
        let handle = self.next_handle;
        if self.files.contains_key(&handle) {
            return Err(libc::EMFILE);
        }
        self.files.try_reserve(1).map_err(|_| libc::ENOMEM)?;
        let admitted = self.admissible(source, path, Some(file_id), stamp);
        let entry = self.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        entry.open_handles = entry.open_handles.saturating_add(1);
        hold(&mut entry.kernel, stamp.unwrap_or(ViewStamp::ORIGIN));
        let cached = entry.admit_cached_content(lookup);
        let claim = changes_content(flags).then_some(file_id);
        let store = match (stamp, u32::try_from(lookup.node.logical_bytes)) {
            (Some(stamp), Ok(length))
                if stores_pages
                    && admitted
                    && cached == CachedContent::Absent
                    && claim.is_none()
                    && flags & libc::O_DIRECT == 0
                    && length != 0
                    && u64::from(length) <= PAGE_STORE_LIMIT
                    && self.page_stores.may_begin(file_id) =>
            {
                self.page_stores.begin(file_id, stamp);
                Some(PageStore {
                    inode,
                    file_id,
                    path: path.clone(),
                    stamp,
                    length,
                    open_file: Arc::clone(&open_file),
                })
            }
            _ => None,
        };
        if let Some(file_id) = claim {
            self.page_stores.claim(file_id);
        }
        self.next_handle = self.next_handle.saturating_add(1).max(1);
        self.files.insert(
            handle,
            FileHandle {
                inode,
                open_file,
                operation: Arc::new(Mutex::new(())),
                dirty,
                claim,
            },
        );
        let flushes_on_close =
            flags & libc::O_ACCMODE != libc::O_RDONLY && source.flush_on_handle_close();
        let mut open_flags = FopenFlags::empty();
        open_flags.set(
            FopenFlags::FOPEN_KEEP_CACHE,
            admitted && cached == CachedContent::Current,
        );
        open_flags.set(FopenFlags::FOPEN_NOFLUSH, !flushes_on_close);
        Ok((handle, open_flags, store))
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
        if let Some(file_id) = file.claim {
            self.page_stores.release(file_id);
        }
        if entry.bindings.is_empty()
            && entry.lookup_references == 0
            && entry.open_handles == 0
            && let Some(entry) = self.by_inode.remove(&inode)
        {
            forget_file(&mut self.inode_by_file, entry.lookup.node.file_id, inode);
        }
        Ok(())
    }

    fn open_handle(&self, inode: u64, handle: u64) -> Result<Arc<dyn MountOpenFile>, i32> {
        let file = self.files.get(&handle).ok_or(libc::ESTALE)?;
        if file.inode != inode {
            return Err(libc::ESTALE);
        }
        Ok(Arc::clone(&file.open_file))
    }

    fn open_handle_operation(&self, inode: u64, handle: u64) -> Result<Arc<Mutex<()>>, i32> {
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

    /// Where to read `inode`'s named attributes; `None` when its current
    /// facts prove it has none, as the kernel's capability check before
    /// every write asks, so no source call is needed.
    fn attributes_to_read(
        &self,
        source: &dyn MountFilesystem,
        inode: u64,
    ) -> Result<Option<AttributeTarget>, i32> {
        if self
            .by_inode
            .get(&inode)
            .is_some_and(|entry| entry.lacks_named_attributes(source))
        {
            return Ok(None);
        }
        self.attribute_target(inode).map(Some)
    }

    fn attribute_target(&self, inode: u64) -> Result<AttributeTarget, i32> {
        match self.open_inode(inode) {
            Some(open_file) => Ok(AttributeTarget::Handle(open_file)),
            None => self
                .path(inode)
                .map(|path| AttributeTarget::Path(path.clone())),
        }
    }

    /// Replaces the node facts of a handle's inode with facts read through
    /// that handle after `stamp`.
    fn apply_handle_facts(&mut self, inode: u64, lookup: MountLookup, stamp: Option<ViewStamp>) {
        if let Some(entry) = self.by_inode.get_mut(&inode)
            && entry.lookup.node.file_id == lookup.node.file_id
        {
            entry.lookup = lookup;
            entry.facts = stamp;
        }
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

    /// The inode the kernel may still use for I/O, without having opened it
    /// through this session, once `lookup`'s name is removed: a regular file
    /// with one name, which the kernel holds and has no handle for yet.
    fn unopened_referenced_inode(&self, lookup: Option<&MountLookup>) -> Option<u64> {
        let lookup = lookup?;
        if lookup.node.kind != MountNodeKind::Regular || lookup.node.link_count != 1 {
            return None;
        }
        let inode = self.inode_by_file.get(&lookup.node.file_id).copied()?;
        self.by_inode
            .get(&inode)
            .is_some_and(|entry| entry.lookup_references != 0 && entry.open_handles == 0)
            .then_some(inode)
    }

    /// The inode with open handles that loses its last name when `lookup`'s
    /// name is removed; its handles must move to a detached view first.
    fn detaching_inode(&self, lookup: Option<&MountLookup>) -> Option<u64> {
        let lookup = lookup?;
        if lookup.node.kind != MountNodeKind::Regular || lookup.node.link_count != 1 {
            return None;
        }
        let inode = self.inode_by_file.get(&lookup.node.file_id).copied()?;
        self.by_inode
            .get(&inode)
            .is_some_and(|entry| entry.open_handles != 0)
            .then_some(inode)
    }

    fn remove_path_cache(&mut self, path: &MountPath) {
        if let Some(inode) = self.inode_by_path.remove(path) {
            unbind(&mut self.by_inode, &mut self.inode_by_file, inode, path);
        }
    }

    fn remove_binding(&mut self, inode: u64, path: &MountPath) {
        if self.inode_by_path.get(path) == Some(&inode) {
            self.inode_by_path.remove(path);
        }
        unbind(&mut self.by_inode, &mut self.inode_by_file, inode, path);
    }

    fn invalidate_prefix(&mut self, prefix: &MountPath) {
        let affected = self
            .by_inode
            .iter()
            .flat_map(|(inode, entry)| {
                entry
                    .bindings
                    .iter()
                    .filter(|binding| has_prefix(&binding.path, prefix))
                    .map(|binding| (*inode, binding.path.clone()))
            })
            .collect::<Vec<_>>();
        for (inode, path) in affected {
            self.remove_binding(inode, &path);
        }
    }

    fn rename_prefix(&mut self, source: &MountPath, destination: &MountPath) {
        for entry in self.by_inode.values_mut() {
            for binding in &mut entry.bindings {
                if let Some(rebased) = replace_prefix(&binding.path, source, destination) {
                    binding.path = rebased;
                }
            }
        }
        self.streams.rename_prefix(source, destination);
        self.inode_by_path.clear();
        for (inode, entry) in &self.by_inode {
            for binding in &entry.bindings {
                let previous = self.inode_by_path.insert(binding.path.clone(), *inode);
                debug_assert!(previous.is_none(), "a path is bound to one inode");
            }
        }
    }

    /// Kernel caching of `inode`'s listing: the kernel resets a cached
    /// listing whenever the directory changes through the mount or its
    /// modification time changes, and the invalidator drops it when the
    /// source changes around the mount. A source without view stamps cannot
    /// report that, so its listings are never kept.
    fn directory_open_flags(source: &dyn MountFilesystem) -> FopenFlags {
        if source.view_stamp().is_some() {
            FopenFlags::FOPEN_CACHE_DIR | FopenFlags::FOPEN_KEEP_CACHE
        } else {
            FopenFlags::empty()
        }
    }

    /// The stream the kernel resumes `inode`'s listing at `offset` from: the
    /// one parked there, or a fresh one that must first skip to it.
    fn directory_stream(
        &mut self,
        source: &dyn MountFilesystem,
        inode: u64,
        offset: u64,
    ) -> Result<DirectoryStream, i32> {
        if let Some(stream) = self.streams.take(inode, offset) {
            return Ok(stream);
        }
        if !source.view_is_stable() {
            return Err(libc::ESTALE);
        }
        let path = self.path(inode)?.to_owned();
        let parent_inode = split_parent(&path)
            .and_then(|(parent, _name)| self.inode_by_path.get(&parent).copied())
            .unwrap_or(ROOT_INODE);
        Ok(DirectoryStream {
            path,
            parent_inode,
            binding_epoch: source.binding_epoch(),
            cursor: None,
            entries: VecDeque::new(),
            entries_stamp: None,
            confirmed: ConfirmedNames::default(),
            exhausted: false,
            emitted: 0,
            parked: 0,
        })
    }

    /// Emits a stream's buffered entries into one kernel listing reply;
    /// [`FuseProjection::position_directory_stream`] reads the next source
    /// page beforehand, outside this state's lock.
    fn list_directory<L: DirectoryListing>(
        &mut self,
        source: &dyn MountFilesystem,
        inode: u64,
        stream: &mut DirectoryStream,
        mut listing: L,
    ) {
        if stream.emitted < 2 {
            let parent_inode = stream.parent_inode;
            let attr = match self.node_attr(inode) {
                Ok(attr) => attr,
                Err(error) => return listing.error(Errno::from_i32(error)),
            };
            // The kernel never instantiates dot entries, so they carry the
            // directory's own attributes without a cache lifetime.
            let resumed = stream.emitted;
            let dots = [(inode, ".", 1), (parent_inode, "..", 2)];
            for (dot_inode, name, next) in dots.into_iter().filter(|dot| dot.2 > resumed) {
                let dot = FileAttr {
                    ino: INodeNo(dot_inode),
                    ..attr
                };
                if listing.push(next, OsStr::new(name), &dot, &Duration::ZERO) {
                    return listing.ok();
                }
                stream.emitted = next;
            }
        }
        self.admit_listing(source, inode, stream.entries_stamp);
        let mut listed = false;
        loop {
            let next = stream.emitted.saturating_add(1);
            let Some(entry) = stream.entries.pop_front() else {
                return listing.ok();
            };
            let child = ChildName::new(source, stream.path.child(entry.name.clone()));
            // A buffered entry is current only while its directory's listing
            // and its own node are.
            let current = stream.entries_stamp.filter(|stamp| {
                source.unchanged_since(&stream.path, None, *stamp)
                    && source.unchanged_since(child.key(), Some(entry.node.file_id), *stamp)
            });
            let page_lookup = MountLookup {
                node: entry.node,
                metadata: entry.metadata,
            };
            // A page older than the current view may predate mounted writes
            // the kernel already applied; the projection's own record, which
            // every mounted write refreshes, must not be rolled back by it.
            let lookup = if current.is_some() {
                page_lookup
            } else {
                self.inode_by_file
                    .get(&page_lookup.node.file_id)
                    .and_then(|known| self.by_inode.get(known))
                    .map_or(page_lookup, |known| known.lookup)
            };
            let listed_attr = match self.intern(
                source,
                child.key().clone(),
                &lookup,
                current,
                L::COUNTS_LOOKUPS,
                &stream.confirmed,
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
                    stream.entries.push_front(entry);
                    if listed {
                        return listing.ok();
                    }
                    return listing.error(Errno::from_i32(error));
                }
            };
            let ttl = if L::COUNTS_LOOKUPS {
                self.admit_entry(source, attr.ino.0, &child, current)
            } else {
                Duration::ZERO
            };
            if listing.push(next, OsStr::from_bytes(&entry.name), &attr, &ttl) {
                self.release_listed_reference::<L>(attr.ino.0);
                stream.entries.push_front(entry);
                return listing.ok();
            }
            listed = true;
            stream.emitted = next;
        }
    }

    /// Returns the lookup a listing took for a child it did not list.
    fn release_listed_reference<L: DirectoryListing>(&mut self, inode: u64) {
        if L::COUNTS_LOOKUPS {
            self.release_lookup_reference(inode, 1);
        }
    }
}

/// Every kernel cache item whose lower bound no longer proves it current,
/// each of which stops being held.
fn stale_kernel_items(
    by_inode: &mut HashMap<u64, InodeEntry>,
    inode_by_path: &HashMap<MountPath, u64>,
    unchanged_since: impl Fn(&MountPath, Option<crate::FileId>, ViewStamp) -> bool,
    through: ViewStamp,
) -> Vec<KernelCacheItem> {
    let mut items = Vec::new();
    for (inode, entry) in by_inode {
        let file_id = entry.lookup.node.file_id;
        let anchor = entry
            .bindings
            .first()
            .map_or(&ROOT_PATH, |binding| &binding.path);
        if let Some(held) = entry.kernel
            && !unchanged_since(anchor, Some(file_id), held)
        {
            items.push(KernelCacheItem::Inode(*inode));
            // Pages an open handle reads from now on postdate this check.
            entry.kernel = Some(through);
        }
        entry.negative_children.retain(|name, held| {
            let unchanged = unchanged_since(&anchor.child(name.clone()), None, *held);
            if !unchanged {
                items.push(KernelCacheItem::Entry {
                    parent: *inode,
                    name: name.clone(),
                });
            }
            unchanged
        });
        for binding in &mut entry.bindings {
            if let Some(held) = binding.kernel
                && !unchanged_since(&binding.path, Some(file_id), held)
            {
                binding.kernel = None;
                if let Some((parent, name)) = split_parent(&binding.path)
                    && let Some(parent) = inode_by_path.get(&parent)
                {
                    items.push(KernelCacheItem::Entry {
                        parent: *parent,
                        name: name.to_vec(),
                    });
                }
            }
        }
    }
    items
}

fn cached_projected_lookup(
    by_inode: &mut HashMap<u64, InodeEntry>,
    inode_by_path: &HashMap<MountPath, u64>,
    path: &MountPath,
    unchanged_since: impl FnOnce(crate::FileId, ViewStamp) -> bool,
) -> Option<(u64, MountLookup, ViewStamp)> {
    let inode = inode_by_path.get(path).copied()?;
    let entry = by_inode.get_mut(&inode)?;
    // Reuse needs both this name's binding and the node facts current.
    let stamp = entry.binding(path)?.verified?.min(entry.facts?);
    if !unchanged_since(entry.lookup.node.file_id, stamp) {
        return None;
    }
    entry.lookup_references = entry.lookup_references.saturating_add(1);
    Some((inode, entry.lookup, stamp))
}

/// Rebinds the root inode to the checkout's current root identity.
fn replace_root_lookup(
    by_inode: &mut HashMap<u64, InodeEntry>,
    inode_by_file: &mut HashMap<crate::FileId, u64>,
    lookup: MountLookup,
) -> Result<(), i32> {
    if lookup.node.kind != MountNodeKind::Directory {
        return Err(libc::EIO);
    }
    let root = by_inode.get_mut(&ROOT_INODE).ok_or(libc::ESTALE)?;
    let previous = root.lookup.node.file_id;
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
        root.lookup = lookup;
    }
    Ok(())
}

/// Forgets the names `inode` no longer has, other than `path`, returning
/// the kernel entries held by them to drop.
///
/// A source can remove a name and its host reuse the node's identity for a
/// file it creates under another name, so a name recorded before may be one
/// the view no longer binds: only names `still_bound` (given the position
/// they were last verified after) count against the node's links. The inode
/// itself stays, as the kernel may still hold it.
///
/// Whether a name is still bound is a question about the name, not about
/// the node's facts: a node with several names is read afresh on every
/// lookup, yet each of its names stays bound until the source reports it
/// changed or a lookup sees it naming another node.
fn forget_unbound_names(
    by_inode: &mut HashMap<u64, InodeEntry>,
    inode_by_path: &mut HashMap<MountPath, u64>,
    inode: u64,
    path: &MountPath,
    still_bound: impl Fn(&MountPath, Option<ViewStamp>) -> bool,
) -> Vec<KernelCacheItem> {
    let Some(entry) = by_inode.get_mut(&inode) else {
        return Vec::new();
    };
    let mut forgotten = Vec::new();
    entry.bindings.retain(|binding| {
        let bound = binding.path == *path || still_bound(&binding.path, binding.verified);
        if !bound {
            forgotten.push((binding.path.clone(), binding.kernel.is_some()));
        }
        bound
    });
    forgotten
        .into_iter()
        .filter_map(|(name, held)| {
            if inode_by_path.get(&name) == Some(&inode) {
                inode_by_path.remove(&name);
            }
            let (parent, component) = split_parent(&name)?;
            let parent = inode_by_path.get(&parent).copied()?;
            held.then(|| KernelCacheItem::Entry {
                parent,
                name: component.to_vec(),
            })
        })
        .collect()
}

/// Forgets that `file_id` is `inode`, unless it already names another.
/// Records `path` as bound to `inode` alone. A path names one file, so an
/// inode that held it before holds it no longer, and one left with no name,
/// reference, or handle is forgotten. `inode_by_path` thereby indexes every
/// binding, which lets a path's binding go without scanning every inode.
fn bind_path(
    by_inode: &mut HashMap<u64, InodeEntry>,
    inode_by_path: &mut HashMap<MountPath, u64>,
    inode_by_file: &mut HashMap<crate::FileId, u64>,
    path: &MountPath,
    inode: u64,
) {
    if let Some(previous) = inode_by_path.insert(path.clone(), inode)
        && previous != inode
    {
        unbind(by_inode, inode_by_file, previous, path);
    }
}

/// Removes `path` from `inode`'s bindings and forgets an inode left with no
/// name, lookup reference, or open handle. The caller keeps `inode_by_path`.
fn unbind(
    by_inode: &mut HashMap<u64, InodeEntry>,
    inode_by_file: &mut HashMap<crate::FileId, u64>,
    inode: u64,
    path: &MountPath,
) {
    let remove_inode = by_inode.get_mut(&inode).is_some_and(|entry| {
        entry.bindings.retain(|binding| binding.path != *path);
        entry.bindings.is_empty() && entry.lookup_references == 0 && entry.open_handles == 0
    });
    if remove_inode && let Some(entry) = by_inode.remove(&inode) {
        forget_file(inode_by_file, entry.lookup.node.file_id, inode);
    }
}

fn forget_file(
    inode_by_file: &mut HashMap<crate::FileId, u64>,
    file_id: crate::FileId,
    inode: u64,
) {
    if inode_by_file.get(&file_id) == Some(&inode) {
        inode_by_file.remove(&file_id);
    }
}

/// Retires `inode` from its identity once it has no name left, so the next
/// name bound to that identity gets a fresh inode.
///
/// A host that reuses a removed file's identity for a new file makes the
/// old inode, which the kernel may still hold, name a file that no longer
/// exists; attaching the new file to it would let a request against the
/// held inode, such as a `SETATTR` without a handle, change the new file.
/// A retired inode keeps its open handles and answers every request that
/// needs a name as stale until the kernel forgets it.
fn retire_reused_identity(
    by_inode: &HashMap<u64, InodeEntry>,
    inode_by_file: &mut HashMap<crate::FileId, u64>,
    inode: u64,
) {
    if inode != ROOT_INODE
        && let Some(entry) = by_inode.get(&inode)
        && entry.bindings.is_empty()
    {
        forget_file(inode_by_file, entry.lookup.node.file_id, inode);
    }
}

#[allow(clippy::too_many_arguments)] // Projection indexes are updated together.
fn intern_projected(
    next_inode: &mut u64,
    by_inode: &mut HashMap<u64, InodeEntry>,
    inode_by_path: &mut HashMap<MountPath, u64>,
    inode_by_file: &mut HashMap<crate::FileId, u64>,
    path: MountPath,
    lookup: &MountLookup,
    stamp: Option<ViewStamp>,
    lookup_reference: bool,
) -> Result<u64, i32> {
    if let Some(inode) = inode_by_file.get(&lookup.node.file_id).copied() {
        let entry = by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        if let Some(binding) = entry.binding_mut(&path) {
            binding.verified = stamp;
        } else {
            entry.bindings.try_reserve(1).map_err(|_| libc::ENOMEM)?;
            entry.bindings.push(Binding::new(path.clone(), stamp));
        }
        entry.lookup = *lookup;
        entry.facts = stamp;
        if lookup_reference {
            entry.lookup_references = entry.lookup_references.saturating_add(1);
        }
        bind_path(by_inode, inode_by_path, inode_by_file, &path, inode);
        return Ok(inode);
    }
    let inode = *next_inode;
    if inode <= ROOT_INODE {
        return Err(libc::EOVERFLOW);
    }
    let following = inode.checked_add(1).ok_or(libc::EOVERFLOW)?;
    *next_inode = following;
    inode_by_file.insert(lookup.node.file_id, inode);
    bind_path(by_inode, inode_by_path, inode_by_file, &path, inode);
    by_inode.insert(
        inode,
        InodeEntry::new(path, *lookup, stamp, u64::from(lookup_reference)),
    );
    Ok(inode)
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

/// Everything one mount session's callbacks and invalidator share.
///
/// Locks are only ever taken in this order: `names`, then a source view
/// lease, then `state`. The state lock is never held across a source call
/// that can wait, so no callback ever waits on another's source work to
/// reach the in-memory records.
struct ProjectionCore {
    source: Arc<dyn MountFilesystem>,
    state: Mutex<ProjectionState>,
    /// Shared by callbacks that resolve names or open them; exclusive while a
    /// removal or rename changes names in the source and in `state` together.
    names: RwLock<()>,
    /// Signals invalidation work and its completion.
    invalidation: Condvar,
    /// Signals that a page store landed.
    page_stored: Condvar,
    stopping: AtomicBool,
    /// Attributes this session's own changes, which its kernel applied.
    origin: ViewOrigin,
    /// Set once, before the session serves its first request.
    notifier: OnceLock<Notifier>,
    /// Every request a callback served, by operation.
    #[cfg(test)]
    requests: Mutex<Vec<&'static str>>,
}

/// A callback's hold on the projection state. Releasing it wakes the
/// invalidator for kernel items the callback deferred, so they drop without
/// waiting for the next change to the source (a source that cannot be
/// watched reports none).
struct StateGuard<'a> {
    state: Option<MutexGuard<'a, ProjectionState>>,
    /// The invalidator's condition.
    wake: &'a Condvar,
}

impl std::ops::Deref for StateGuard<'_> {
    type Target = ProjectionState;

    fn deref(&self) -> &ProjectionState {
        self.state
            .as_deref()
            .unwrap_or_else(|| unreachable!("held until dropped"))
    }
}

impl std::ops::DerefMut for StateGuard<'_> {
    fn deref_mut(&mut self) -> &mut ProjectionState {
        self.state
            .as_deref_mut()
            .unwrap_or_else(|| unreachable!("held until dropped"))
    }
}

impl StateGuard<'_> {
    /// Waits on `condition`, releasing the state meanwhile.
    fn wait(mut self, condition: &Condvar) -> Result<Self, i32> {
        let state = self
            .state
            .take()
            .unwrap_or_else(|| unreachable!("held until dropped"));
        self.state = Some(condition.wait(state).map_err(|_| libc::EIO)?);
        Ok(self)
    }
}

impl Drop for StateGuard<'_> {
    fn drop(&mut self) {
        let deferred = self
            .state
            .take()
            .is_some_and(|state| !state.invalidation.deferred.is_empty());
        if deferred {
            self.wake.notify_all();
        }
    }
}

impl ProjectionCore {
    fn new(source: Arc<dyn MountFilesystem>, state: ProjectionState) -> Self {
        Self {
            source,
            state: Mutex::new(state),
            names: RwLock::new(()),
            invalidation: Condvar::new(),
            page_stored: Condvar::new(),
            stopping: AtomicBool::new(false),
            origin: ViewOrigin::new(),
            notifier: OnceLock::new(),
            #[cfg(test)]
            requests: Mutex::new(Vec::new()),
        }
    }

    /// The state for a callback serving a request.
    fn state(&self) -> Result<StateGuard<'_>, i32> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(libc::ENODEV);
        }
        let state = self.state.lock().map_err(|_| libc::EIO)?;
        Ok(self.guard(state))
    }

    /// The state for a callback that must finish what it started, such as
    /// landing a page store or releasing a claim, even while the session
    /// stops or after another callback panicked.
    fn state_to_finish(&self) -> StateGuard<'_> {
        self.guard(self.state.lock().unwrap_or_else(PoisonError::into_inner))
    }

    fn guard<'a>(&'a self, state: MutexGuard<'a, ProjectionState>) -> StateGuard<'a> {
        StateGuard {
            state: Some(state),
            wake: &self.invalidation,
        }
    }

    fn names(&self) -> Result<RwLockReadGuard<'_, ()>, i32> {
        self.names.read().map_err(|_| libc::EIO)
    }

    fn names_exclusive(&self) -> Result<RwLockWriteGuard<'_, ()>, i32> {
        self.names.write().map_err(|_| libc::EIO)
    }

    /// Begins serving one kernel request: every view change it causes is
    /// this session's own until the returned scope drops.
    fn request(&self, operation: &'static str) -> ViewOriginScope {
        #[cfg(test)]
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(operation);
        #[cfg(not(test))]
        let _ = operation;
        self.origin.enter()
    }

    /// Claims `file_id`'s content for a change through this mount, once its
    /// page store in flight, if any, has landed.
    fn claim_content(&self, file_id: crate::FileId) -> Result<ContentClaim<'_>, i32> {
        let mut state = self.state()?;
        while state.page_stores.in_flight.contains_key(&file_id) {
            state = state.wait(&self.page_stored)?;
        }
        state.page_stores.claim(file_id);
        Ok(ContentClaim {
            core: self,
            file_id,
        })
    }
}

/// A claim on one file's content held while a callback changes it or
/// admits a handle that may; see [`PageStores`].
struct ContentClaim<'a> {
    core: &'a ProjectionCore,
    file_id: crate::FileId,
}

impl Drop for ContentClaim<'_> {
    fn drop(&mut self) {
        self.core
            .state_to_finish()
            .page_stores
            .release(self.file_id);
    }
}

impl ViewObserver for ProjectionCore {
    fn view_changed(&self, position: ViewStamp, origin: ViewOrigin) {
        // The kernel applied this session's own changes as it made them: a
        // change reaches every entry it keeps for the changed name, since it
        // keeps no entry for a spelling of a folded name (`ChildName`), and
        // attributes, pages, and listings belong to inodes, not spellings.
        if origin == self.origin {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.invalidation.notified = state.invalidation.notified.max(Some(position));
        self.invalidation.notify_all();
    }
}

/// Drops every kernel item that changes made around the mount superseded,
/// until the session stops.
fn run_invalidator(core: &ProjectionCore, notifier: &Notifier) {
    let mut state = core.state.lock().unwrap_or_else(PoisonError::into_inner);
    loop {
        if core.stopping.load(Ordering::Acquire) {
            return;
        }
        if state.invalidation.is_idle() {
            state = core
                .invalidation
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
            continue;
        }
        let through = state.invalidation.notified;
        let mut items = std::mem::take(&mut state.invalidation.deferred);
        if let Some(through) = through
            && state.invalidation.scanned < Some(through)
        {
            state.invalidation.scanned = Some(through);
            let state = &mut *state;
            items.extend(stale_kernel_items(
                &mut state.by_inode,
                &state.inode_by_path,
                |path, file_id, stamp| core.source.unchanged_since(path, file_id, stamp),
                through,
            ));
        }
        drop(state);
        let dropped = drop_kernel_caches(notifier, items);
        state = core.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.invalidation.processed = state.invalidation.processed.max(through);
        if let Err(error) = dropped {
            state.invalidation.failure.get_or_insert(error);
        }
        core.invalidation.notify_all();
    }
}

/// Notifies the kernel item by item. Every item is attempted; the first
/// failure is reported.
fn drop_kernel_caches(
    notifier: &Notifier,
    items: impl IntoIterator<Item = KernelCacheItem>,
) -> Result<(), String> {
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
    failure.map_or(Ok(()), |error| Err(error.to_string()))
}

/// Reads `path` from the source under a lease on the current binding.
fn resolve_path(
    source: &dyn MountFilesystem,
    path: &MountPath,
) -> Result<(Option<MountLookup>, Option<ViewStamp>), i32> {
    let binding_epoch = source.binding_epoch();
    let _lease = source.acquire_binding_lease(binding_epoch).map_err(errno)?;
    // Sampled first: a later change to anything this lookup read records a
    // later position, so the result can never validate over it.
    let stamp = source.view_stamp();
    let lookup = source.lookup(path).map_err(errno)?;
    if !source.view_is_stable() || source.binding_epoch() != binding_epoch {
        return Err(libc::ESTALE);
    }
    Ok((lookup, stamp))
}

/// Current facts for one node: `facts` when they already are, or else read
/// from the source as they direct.
fn resolve_node(
    source: &dyn MountFilesystem,
    facts: Result<Resolved, NodeFacts>,
) -> Result<Resolved, i32> {
    match facts {
        Ok(current) => Ok(current),
        Err(NodeFacts::Handle(open_file)) => {
            let stamp = source.view_stamp();
            Ok(Resolved {
                lookup: open_file.lookup().map_err(errno)?,
                stamp,
                through: None,
            })
        }
        Err(NodeFacts::Names { file_id, paths }) => {
            for path in paths {
                let (lookup, stamp) = resolve_path(source, &path)?;
                if let Some(lookup) = lookup
                    && file_id.is_none_or(|file_id| lookup.node.file_id == file_id)
                {
                    return Ok(Resolved {
                        lookup,
                        stamp,
                        through: Some(path),
                    });
                }
            }
            Err(libc::ENOENT)
        }
    }
}

struct FuseProjection {
    core: Arc<ProjectionCore>,
    /// Whether the kernel lists directories without opening them, which it
    /// then does with the listing cached as [`ProjectionState::directory_open_flags`]
    /// would allow for a source with view stamps.
    skips_opendir: bool,
    /// Whether the kernel opens files without asking. It then keeps every
    /// file's pages, which a source that reports its changes keeps coherent
    /// (and, should its reports stop, attributes that expire at once let
    /// the kernel drop on the next read), and sends each file's I/O on its
    /// inode, which [`Self::handle`] gives one handle of its own. A source
    /// that flushes when a handle closes needs every close, so it is asked.
    skips_open: bool,
}

/// One background libfuse session.
pub(super) struct FuseSession {
    session: Option<BackgroundSession>,
    core: Arc<ProjectionCore>,
    invalidator: Option<std::thread::JoinHandle<()>>,
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
        let source = self.core.source.as_ref();
        let child = ChildName::new(source, parent.child(name.to_vec()));
        let parent = source.folded_path(&parent).unwrap_or(parent);
        let items = {
            let state = self.lock_state()?;
            let file_inode = state.inode_by_path.get(child.key()).copied();
            // The kernel holds entries only beneath inodes it has not
            // forgotten, so a parent without one caches nothing under this
            // name; the node itself may still be reachable through another
            // name. The parent's listing changes with the name it contains.
            let parent_inode = state.inode_by_path.get(&parent).copied();
            file_inode
                .map(KernelCacheItem::Inode)
                .into_iter()
                .chain(parent_inode.into_iter().flat_map(|parent_inode| {
                    [
                        KernelCacheItem::Entry {
                            parent: parent_inode,
                            name: name.to_vec(),
                        },
                        KernelCacheItem::Inode(parent_inode),
                    ]
                }))
                .collect::<Vec<_>>()
        };
        drop_kernel_caches(self.notifier()?, items).map_err(NativeMountError::Driver)
    }

    /// Waits until the kernel no longer holds anything superseded by a change
    /// to the source recorded before this call.
    ///
    /// Changes made around the mount reach the kernel as soon as the
    /// invalidator processes them; this is the barrier for callers that must
    /// observe one through the mount immediately afterwards.
    pub(super) fn revalidate(&self) -> Result<(), NativeMountError> {
        let stopped = || NativeMountError::Driver("session is stopped".to_owned());
        // Every change made to the source before this call is recorded, and
        // so notified, before the wait begins.
        self.core.source.fence_changes().map_err(source_error)?;
        let poisoned = |_| NativeMountError::Driver("FUSE projection state is poisoned".to_owned());
        let mut state = self.lock_state()?;
        let target = state.invalidation.notified;
        // A page store read before the target may land after the invalidator
        // dropped what it superseded; it defers that drop again as it lands.
        loop {
            if self.core.stopping.load(Ordering::Acquire) {
                return Err(stopped());
            }
            state = if state.invalidation.processed < target
                || !state.invalidation.deferred.is_empty()
            {
                self.core.invalidation.wait(state).map_err(poisoned)?
            } else if target.is_some_and(|target| state.page_stores.precede(target)) {
                self.core.page_stored.wait(state).map_err(poisoned)?
            } else {
                break;
            };
        }
        state
            .invalidation
            .failure
            .take()
            .map_or(Ok(()), |error| Err(NativeMountError::Driver(error)))
    }

    /// Takes the operation of every request served so far.
    #[cfg(test)]
    pub(super) fn take_requests(&self) -> Vec<&'static str> {
        std::mem::take(
            &mut *self
                .core
                .requests
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, ProjectionState>, NativeMountError> {
        self.core
            .state
            .lock()
            .map_err(|_| NativeMountError::Driver("FUSE projection state is poisoned".to_owned()))
    }

    fn notifier(&self) -> Result<&Notifier, NativeMountError> {
        self.session
            .as_ref()
            .and(self.core.notifier.get())
            .ok_or_else(|| NativeMountError::Driver("session is stopped".to_owned()))
    }

    pub(super) fn start(
        request: &NativeMountRequest,
        source: Arc<dyn MountFilesystem>,
    ) -> Result<Self, NativeMountError> {
        // Sampled before the root so that a concurrent change can only make
        // it older than the facts it labels, never newer.
        let stamp = source.view_stamp();
        let root = source
            .lookup(&MountPath::root())
            .map_err(source_error)?
            .ok_or_else(|| NativeMountError::Driver("volume root is absent".to_owned()))?;
        if root.node.kind != MountNodeKind::Directory {
            return Err(NativeMountError::Driver(
                "volume root is not a directory".to_owned(),
            ));
        }
        let destination_metadata = request
            .destination
            .metadata()
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        let state = ProjectionState {
            defaults: AttributeDefaults {
                writable: request.writable,
                mount_uid: destination_metadata.uid(),
                mount_gid: destination_metadata.gid(),
            },
            next_inode: ROOT_INODE + 1,
            next_handle: 1,
            by_inode: HashMap::from([(
                ROOT_INODE,
                InodeEntry::new(MountPath::root(), root, stamp, 1),
            )]),
            inode_by_path: HashMap::from([(MountPath::root(), ROOT_INODE)]),
            inode_by_file: HashMap::from([(root.node.file_id, ROOT_INODE)]),
            files: HashMap::new(),
            implicit: HashMap::new(),
            streams: DirectoryStreams::default(),
            invalidation: InvalidationQueue::default(),
            page_stores: PageStores::default(),
        };
        let core = Arc::new(ProjectionCore::new(source, state));
        let observer: Weak<ProjectionCore> = Arc::downgrade(&core);
        core.source.observe_view(observer);
        // Even with `noatime`, a writable mount pays one GETATTR per file
        // after the kernel reads its pages from the daemon; small files'
        // pages arrive with their first open instead (`PAGE_STORE_LIMIT`).
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
        // Measured: a descriptor per thread does not change 8-way stat or
        // read throughput here, whose cost is in the source, not the queue.
        config.clone_fd = false;
        let filesystem = FuseProjection {
            core: Arc::clone(&core),
            skips_opendir: false,
            skips_open: false,
        };
        let session = fuser::Session::new(filesystem, &request.destination, &config)
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        // Callbacks store pages through the notifier from the first request.
        let notifier = session.notifier();
        let _ = core.notifier.set(notifier.clone());
        let session = session
            .spawn()
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        let invalidating = Arc::clone(&core);
        let invalidator = std::thread::Builder::new()
            .name("acyclic-fs-fuse-invalidate".to_owned())
            .spawn(move || run_invalidator(&invalidating, &notifier));
        let mut started = Self {
            session: Some(session),
            core,
            invalidator: None,
            shutdown_failed: false,
        };
        match invalidator {
            Ok(invalidator) => {
                started.invalidator = Some(invalidator);
                Ok(started)
            }
            Err(error) => {
                started.stop()?;
                Err(NativeMountError::Driver(error.to_string()))
            }
        }
    }

    pub(super) fn stop(&mut self) -> Result<(), NativeMountError> {
        if self.shutdown_failed {
            return Err(NativeMountError::Driver(
                "FUSE background session previously failed during shutdown".to_owned(),
            ));
        }
        self.core.stopping.store(true, Ordering::Release);
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
        if let Some(invalidator) = self.invalidator.take() {
            // Notified under the lock so the stop cannot fall between the
            // invalidator's check and its wait.
            {
                let _state = self.core.state.lock();
                self.core.invalidation.notify_all();
            }
            invalidator.join().map_err(|_| {
                NativeMountError::Driver("FUSE invalidator failed during shutdown".to_owned())
            })?;
        }
        Ok(())
    }
}

/// Attribute changes one `SETATTR` requests.
#[allow(clippy::struct_field_names)]
struct AttributeChanges {
    mode: Option<u32>,
    uid: Option<u32>,
    gid: Option<u32>,
    size: Option<u64>,
    atime: Option<TimeOrNow>,
    mtime: Option<TimeOrNow>,
    changed: Option<SystemTime>,
    crtime: Option<SystemTime>,
    flags: Option<u32>,
}

impl AttributeChanges {
    fn is_empty(&self) -> bool {
        self.mode.is_none()
            && self.uid.is_none()
            && self.gid.is_none()
            && self.size.is_none()
            && self.atime.is_none()
            && self.mtime.is_none()
            && self.changed.is_none()
            && self.crtime.is_none()
            && self.flags.is_none()
    }

    fn applied_to(&self, mut metadata: FileMetadata) -> Result<FileMetadata, i32> {
        if let Some(mode) = self.mode {
            metadata.posix_mode = MetadataField::Value(mode);
        }
        if let Some(uid) = self.uid {
            metadata.posix_uid = MetadataField::Value(uid);
        }
        if let Some(gid) = self.gid {
            metadata.posix_gid = MetadataField::Value(gid);
        }
        if let Some(atime) = self.atime {
            metadata.accessed_ns = MetadataField::Value(time_or_now_ns(atime)?);
        }
        if let Some(mtime) = self.mtime {
            metadata.modified_ns = MetadataField::Value(time_or_now_ns(mtime)?);
        }
        if let Some(changed) = self.changed {
            metadata.changed_ns = MetadataField::Value(system_time_ns(changed)?);
        }
        if let Some(created) = self.crtime {
            metadata.created_ns = MetadataField::Value(system_time_ns(created)?);
        }
        if let Some(flags) = self.flags {
            metadata.posix_flags = MetadataField::Value(u64::from(flags));
        }
        Ok(metadata)
    }
}

impl FuseProjection {
    fn source(&self) -> &dyn MountFilesystem {
        self.core.source.as_ref()
    }

    /// The name `name` under `parent`, as spelled and as recorded.
    fn child(&self, state: &ProjectionState, parent: u64, name: &OsStr) -> Result<ChildName, i32> {
        Ok(ChildName::new(
            self.source(),
            state.child_path(parent, name)?,
        ))
    }

    /// The recorded names of each node with several names, other than the
    /// name it was just resolved through, that the source still binds to it.
    ///
    /// A node with several names keeps one inode for all of them, while a
    /// host that reused a removed node's identity for a new file must not
    /// have the new file attached to the inode the kernel holds for the
    /// removed one. Once the view cannot vouch for a recorded name, only the
    /// source can tell the two apart: a name that still resolves to the same
    /// identity shows that the node the inode stands for still exists, and
    /// so is the node resolved now. Those names are resolved here, outside
    /// the state's lock, into `confirmed`; a node with one name has no other
    /// name to keep, and a node no inode stands for has no recorded name.
    ///
    /// Only a resolution that finds the name absent or naming another node
    /// counts against it. One that fails (a name the caller may not reach,
    /// say) leaves the name recorded as bound: the requested name resolved,
    /// so its own lookup must not fail for a name it did not ask for.
    fn confirm_names<'a>(
        &self,
        mut confirmed: ConfirmedNames,
        resolved: impl IntoIterator<Item = (&'a MountPath, MountNode)>,
    ) -> Result<ConfirmedNames, i32> {
        let source = self.source();
        let linked = resolved
            .into_iter()
            .filter(|(_, node)| node.link_count > 1)
            .collect::<Vec<_>>();
        if linked.is_empty() {
            return Ok(confirmed);
        }
        let unvouched = {
            let state = self.core.state()?;
            linked
                .iter()
                .flat_map(|(path, node)| {
                    state
                        .unvouched_names(source, node.file_id, path)
                        .into_iter()
                        .map(|name| (node.file_id, name))
                })
                .collect::<Vec<_>>()
        };
        for (file_id, name) in unvouched {
            match resolve_path(source, &name) {
                Ok((Some(found), stamp)) if found.node.file_id == file_id => {
                    confirmed.confirm(file_id, name, stamp);
                }
                Ok(_) => {}
                Err(_) => confirmed.confirm(file_id, name, None),
            }
        }
        Ok(confirmed)
    }

    fn lookup_entry(&self, parent: u64, name: &OsStr) -> Result<Entry, i32> {
        let source = self.source();
        let _names = self.core.names()?;
        let child = {
            let mut state = self.core.state()?;
            let child = self.child(&state, parent, name)?;
            if let Some((inode, lookup, stamp)) = state.cached_lookup(source, child.key()) {
                let attr = state.attr(inode, &lookup)?;
                let ttl = state.admit_entry(source, inode, &child, Some(stamp));
                return Ok(Entry { attr, ttl });
            }
            child
        };
        let (found, stamp) = resolve_path(source, &child.spelled)?;
        let confirmed = match found {
            Some(lookup) => {
                self.confirm_names(ConfirmedNames::default(), [(child.key(), lookup.node)])?
            }
            None => ConfirmedNames::default(),
        };
        let mut state = self.core.state()?;
        let Some(lookup) = found else {
            state.remove_path_cache(child.key());
            // A zero-inode LOOKUP response carries a negative-dentry TTL.
            // Plain ENOENT has no cache lifetime and makes compiler probes
            // traverse the same absent dependency paths thousands of times.
            let ttl = state
                .admit_negative(source, parent, &child, stamp)
                .ok_or(libc::ENOENT)?;
            let mut attr = state.node_attr(ROOT_INODE).map_err(|_| libc::ESTALE)?;
            attr.ino = INodeNo(0);
            return Ok(Entry { attr, ttl });
        };
        let inode = state.intern(
            source,
            child.key().clone(),
            &lookup,
            stamp,
            true,
            &confirmed,
        )?;
        let attr = state
            .attr(inode, &lookup)
            .inspect_err(|_| state.release_lookup_reference(inode, 1))?;
        let ttl = state.admit_entry(source, inode, &child, stamp);
        Ok(Entry { attr, ttl })
    }

    fn node_attributes(
        &self,
        inode: u64,
        handle: Option<u64>,
    ) -> Result<(FileAttr, Duration), i32> {
        let source = self.source();
        let _names = self.core.names()?;
        let facts = {
            let mut state = self.core.state()?;
            match state.node_facts(source, inode, handle)? {
                Ok(current) => {
                    let attr = state.attr(inode, &current.lookup)?;
                    let ttl = state.admit_attributes(source, inode, current.stamp);
                    return Ok((attr, ttl));
                }
                facts => facts,
            }
        };
        let resolved = resolve_node(source, facts)?;
        self.core.state()?.apply(source, inode, &resolved)
    }

    fn set_attributes(
        &self,
        inode: u64,
        handle: Option<u64>,
        changes: &AttributeChanges,
    ) -> Result<(FileAttr, Duration), i32> {
        let source = self.source();
        let _names = self.core.names()?;
        let (facts, open_file) = {
            let state = self.core.state()?;
            state.admit_write().map_err(|_| libc::EOPNOTSUPP)?;
            let open_file = handle
                .map(|handle| state.open_handle(inode, handle))
                .transpose()?;
            (state.node_facts(source, inode, handle)?, open_file)
        };
        let current = resolve_node(source, facts)?;
        let _claim = changes
            .size
            .map(|_| self.core.claim_content(current.lookup.node.file_id))
            .transpose()?;
        let resolved = if changes.is_empty() {
            current
        } else {
            let metadata = changes.applied_to(current.lookup.metadata)?;
            let stamp = source.view_stamp();
            match (&open_file, current.through) {
                (Some(open_file), _) => {
                    open_file
                        .set_attributes(metadata, changes.size)
                        .map_err(errno)?;
                    Resolved {
                        lookup: open_file.lookup().map_err(errno)?,
                        stamp,
                        through: None,
                    }
                }
                (None, Some(path)) => {
                    source
                        .set_attributes(&path, metadata, changes.size)
                        .map_err(errno)?;
                    resolve_node(
                        source,
                        Err(NodeFacts::Names {
                            file_id: Some(current.lookup.node.file_id),
                            paths: vec![path],
                        }),
                    )?
                }
                (None, None) => return Err(libc::ESTALE),
            }
        };
        let mut state = self.core.state()?;
        if !changes.is_empty()
            && let Some(handle) = handle
        {
            state.mark_handle_dirty(inode, handle)?;
        }
        state.apply(source, inode, &resolved)
    }

    /// Creates one node through `create` and interns it at its new name.
    fn create_node(
        &self,
        parent: u64,
        name: &OsStr,
        create: impl FnOnce(&MountPath) -> Result<MountLookup, MountSourceError>,
    ) -> Result<Entry, i32> {
        let source = self.source();
        let _names = self.core.names()?;
        let child = {
            let state = self.core.state()?;
            state.admit_write()?;
            self.child(&state, parent, name)?
        };
        let stamp = source.view_stamp();
        let lookup = create(&child.spelled).map_err(errno)?;
        // A node just created has no recorded name to confirm.
        let confirmed =
            self.confirm_names(ConfirmedNames::default(), [(child.key(), lookup.node)])?;
        let mut state = self.core.state()?;
        let inode = state.intern(
            source,
            child.key().clone(),
            &lookup,
            stamp,
            true,
            &confirmed,
        )?;
        let attr = state
            .attr(inode, &lookup)
            .inspect_err(|_| state.release_lookup_reference(inode, 1))?;
        let ttl = state.admit_entry(source, inode, &child, stamp);
        Ok(Entry { attr, ttl })
    }

    /// Opens the implicit handle of the file `lookup` names, when the kernel
    /// may hold it open without having asked, so removing its last name
    /// detaches it for the kernel's later I/O.
    fn hold_unopened(&self, lookup: Option<&MountLookup>) -> Result<(), i32> {
        if !self.skips_open {
            return Ok(());
        }
        let inode = self.core.state()?.unopened_referenced_inode(lookup);
        // The caller holds the names lock exclusively.
        inode.map_or(Ok(()), |inode| {
            self.reopen_implicit_named(inode, None, Access::Read)
                .map(|_| ())
        })
    }

    fn remove_name(&self, parent: u64, name: &OsStr) -> Result<(), i32> {
        let source = self.source();
        let _names = self.core.names_exclusive()?;
        let child = {
            let state = self.core.state()?;
            state.admit_write()?;
            self.child(&state, parent, name)?
        };
        let path = &child.spelled;
        let current = source.lookup(path).map_err(errno)?;
        self.hold_unopened(current.as_ref())?;
        let detaching = self.core.state()?.detaching_inode(current.as_ref());
        let detached = detaching
            .map(|inode| source.detach_file(path).map(|detached| (inode, detached)))
            .transpose()
            .map_err(errno)?;
        source
            .remove(path, current.map(|lookup| lookup.node.file_id))
            .map_err(errno)?;
        let mut state = self.core.state()?;
        if let Some((inode, detached)) = detached {
            state.retain_detached_handles(inode, &detached);
        }
        state.remove_path_cache(child.key());
        Ok(())
    }

    fn rename_name(
        &self,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        flags: u32,
    ) -> Result<(), i32> {
        let source = self.source();
        if flags & !RENAME_NOREPLACE != 0 {
            return Err(libc::EOPNOTSUPP);
        }
        let replace = flags & RENAME_NOREPLACE == 0;
        let _names = self.core.names_exclusive()?;
        let (from, to) = {
            let state = self.core.state()?;
            state.admit_write()?;
            (
                self.child(&state, parent, name)?,
                self.child(&state, new_parent, new_name)?,
            )
        };
        let replaced = if replace {
            source.lookup(&to.spelled).map_err(errno)?
        } else {
            None
        };
        self.hold_unopened(replaced.as_ref())?;
        let detaching = self.core.state()?.detaching_inode(replaced.as_ref());
        let detached = detaching
            .map(|inode| {
                source
                    .detach_file(&to.spelled)
                    .map(|detached| (inode, detached))
            })
            .transpose()
            .map_err(errno)?;
        source
            .rename(&from.spelled, &to.spelled, replace)
            .map_err(errno)?;
        let mut state = self.core.state()?;
        if let Some((inode, detached)) = detached {
            state.retain_detached_handles(inode, &detached);
        }
        // Renaming between spellings of one folded name moves nothing the
        // projection records.
        if from.key() != to.key() {
            // Whatever the projection still binds under the destination is
            // gone now: the rename replaced it, or it was already absent.
            state.invalidate_prefix(to.key());
            state.rename_prefix(from.key(), to.key());
        }
        Ok(())
    }

    fn link_name(&self, inode: u64, new_parent: u64, new_name: &OsStr) -> Result<Entry, i32> {
        let source = self.source();
        let _names = self.core.names()?;
        let (from, to, facts) = {
            let state = self.core.state()?;
            state.admit_write()?;
            (
                state.path(inode)?.clone(),
                self.child(&state, new_parent, new_name)?,
                state.node_facts(source, inode, None)?,
            )
        };
        let current = resolve_node(source, facts)?;
        let mut projected = current.lookup;
        projected.node.link_count = projected
            .node
            .link_count
            .checked_add(1)
            .ok_or(libc::EMLINK)?;
        source.hard_link(&from, &to.spelled).map_err(errno)?;
        let mut state = self.core.state()?;
        let attr = state.attr(inode, &projected)?;
        let entry = state.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        if entry.lookup.node.file_id != projected.node.file_id {
            return Err(libc::ESTALE);
        }
        if entry.binding(to.key()).is_none() {
            entry.bindings.try_reserve(1).map_err(|_| libc::ENOMEM)?;
            entry.bindings.push(Binding::new(to.key().clone(), None));
        }
        // Derived locally from the facts before the link, not read back.
        entry.lookup = projected;
        entry.facts = None;
        entry.lookup_references = entry.lookup_references.saturating_add(1);
        let ProjectionState {
            by_inode,
            inode_by_path,
            inode_by_file,
            ..
        } = &mut *state;
        bind_path(by_inode, inode_by_path, inode_by_file, to.key(), inode);
        let ttl = state.admit_entry(source, inode, &to, current.stamp);
        Ok(Entry { attr, ttl })
    }

    fn read_link(&self, inode: u64) -> Result<Bytes, i32> {
        let source = self.source();
        let _names = self.core.names()?;
        let path = self.core.state()?.path(inode)?.clone();
        let stamp = source.view_stamp();
        let target = source.read_link(&path).map_err(errno)?;
        // The kernel keeps the target it receives.
        self.core.state()?.admit_attributes(source, inode, stamp);
        Ok(target)
    }

    fn open_node(&self, inode: u64, flags: i32) -> Result<(u64, FopenFlags), i32> {
        self.open_node_storing(inode, flags, true)
    }

    /// Opens `inode` with `flags`; `storing` sends small files' pages to the
    /// kernel with the open.
    fn open_node_storing(
        &self,
        inode: u64,
        flags: i32,
        storing: bool,
    ) -> Result<(u64, FopenFlags), i32> {
        let _names = self.core.names()?;
        self.open_node_named(inode, flags, storing)
    }

    /// [`Self::open_node_storing`] for a caller that holds the names lock.
    fn open_node_named(
        &self,
        inode: u64,
        flags: i32,
        storing: bool,
    ) -> Result<(u64, FopenFlags), i32> {
        let source = self.source();
        let (path, file_id, stamp_before, binding_before, held) = {
            let state = self.core.state()?;
            admit_open(state.defaults.writable, flags)?;
            let node = state.node(inode)?;
            if node.kind != MountNodeKind::Regular {
                return Err(libc::EISDIR);
            }
            // Facts a read-only open can stand on without the source: pages
            // the kernel holds at the version these facts describe.
            let held = (!changes_content(flags) && flags & libc::O_DIRECT == 0)
                .then(|| state.by_inode.get(&inode))
                .flatten()
                .filter(|entry| entry.cached_content == Some(ContentVersion::of(&entry.lookup)))
                .and_then(|entry| Some((entry.lookup, entry.facts?)));
            (
                state.path(inode)?.clone(),
                node.file_id,
                source.view_stamp(),
                source.binding_epoch(),
                held,
            )
        };
        let _claim = changes_content(flags)
            .then(|| self.core.claim_content(file_id))
            .transpose()?;
        let held = held.filter(|(lookup, facts)| {
            lookup.node.file_id == file_id && source.unchanged_since(&path, Some(file_id), *facts)
        });
        let (open_file, refreshed, stamp, truncated) = match held {
            Some((lookup, facts)) => {
                let deferred: Arc<dyn MountOpenFile> = Arc::new(DeferredOpenFile {
                    source: Arc::clone(&self.core.source),
                    path: path.clone(),
                    opened: lookup,
                    file: Mutex::new(None),
                });
                (deferred, lookup, Some(facts), false)
            }
            None => self.open_source(source, &path, file_id, flags, stamp_before)?,
        };
        // Lazy promotion and O_TRUNC may write while opening, so pin the
        // external binding only after those operations have completed.
        let _binding = source
            .acquire_binding_lease(binding_before)
            .map_err(errno)?;
        let mut state = self.core.state()?;
        if state.path(inode).ok() != Some(&path) {
            return Err(libc::ESTALE);
        }
        let entry = state.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        if entry.lookup.node.file_id != file_id {
            return Err(libc::ESTALE);
        }
        entry.lookup = refreshed;
        entry.facts = stamp;
        if let Some(binding) = entry.binding_mut(&path) {
            binding.verified = stamp;
        }
        let (handle, open_flags, store) = state.admit_file_handle(
            source,
            inode,
            OpenedFile {
                path: &path,
                lookup: &refreshed,
                stamp,
                flags,
                open_file,
                dirty: truncated,
                stores_pages: storing,
            },
        )?;
        drop(state);
        Ok((handle, self.store_pages(store, open_flags)))
    }

    /// Opens `path` in the source for an open with `flags` and returns the
    /// file with its facts, the position they were read after, and whether
    /// the open truncated it.
    #[allow(clippy::type_complexity)]
    fn open_source(
        &self,
        source: &dyn MountFilesystem,
        path: &MountPath,
        file_id: crate::FileId,
        flags: i32,
        stamp_before: Option<ViewStamp>,
    ) -> Result<(Arc<dyn MountOpenFile>, MountLookup, Option<ViewStamp>, bool), i32> {
        // The inode may have outlived its path binding. Check the attached
        // file before O_TRUNC can mutate a replacement at that path.
        let (open_file, opened) = source.open_file_with_lookup(path).map_err(errno)?;
        if opened.node.file_id != file_id {
            return Err(libc::ESTALE);
        }
        let truncated = flags & libc::O_TRUNC != 0;
        if truncated {
            open_file.resize(0).map_err(errno)?;
        }
        // O_TRUNC (and a lazy-file promotion during open) legitimately
        // changes the node. Validate the attached handle instead of treating
        // that authored mutation as an external rebind.
        let unchanged = !truncated
            && stamp_before.is_some_and(|stamp| source.unchanged_since(path, Some(file_id), stamp));
        let (refreshed, stamp) = if unchanged {
            (opened, stamp_before)
        } else {
            let stamp = source.view_stamp();
            match open_file.lookup() {
                Ok(lookup) if lookup.node.file_id == file_id => (lookup, stamp),
                _ => return Err(libc::ESTALE),
            }
        };
        Ok((open_file, refreshed, stamp, truncated))
    }

    /// Completes `store`, if any, for an open replying with `open_flags`.
    ///
    /// The whole content reaches the kernel page cache before the open
    /// replies, and the open keeps it only while no change around the mount
    /// superseded it since it was read. A superseded store is dropped again
    /// as the invalidator would have, and any failure leaves the kernel to
    /// read the file on demand.
    fn store_pages(&self, store: Option<PageStore>, mut open_flags: FopenFlags) -> FopenFlags {
        let Some(store) = store else {
            return open_flags;
        };
        let stored = self.core.notifier.get().is_some_and(|notifier| {
            store
                .open_file
                .read_up_to(0, store.length)
                .is_ok_and(|bytes| notifier.store(INodeNo(store.inode), 0, &bytes).is_ok())
        });
        let mut state = self.core.state_to_finish();
        state.page_stores.land(store.file_id);
        let admitted = state.admissible(
            self.source(),
            &store.path,
            Some(store.file_id),
            Some(store.stamp),
        );
        if !admitted {
            state
                .invalidation
                .deferred
                .push(KernelCacheItem::Inode(store.inode));
        }
        self.core.page_stored.notify_all();
        drop(state);
        open_flags.set(FopenFlags::FOPEN_KEEP_CACHE, stored && admitted);
        open_flags
    }

    fn create_file(
        &self,
        request: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        flags: i32,
    ) -> Result<(Entry, u64, FopenFlags), i32> {
        let source = self.source();
        let _names = self.core.names()?;
        let (child, absent_since) = {
            let state = self.core.state()?;
            state.admit_write()?;
            let child = self.child(&state, parent, name)?;
            let absent_since = state.absent_since(parent, &child);
            (child, absent_since)
        };
        let path = &child.spelled;
        let stamp = source.view_stamp();
        let claim = |file_id| {
            changes_content(flags)
                .then(|| self.core.claim_content(file_id))
                .transpose()
        };
        // The kernel looks a name up before it creates it: an absence it was
        // told of that nothing has changed since needs no second lookup.
        let existing = match absent_since {
            Some(held) if source.unchanged_since(child.key(), None, held) => None,
            _ => source.lookup(path).map_err(errno)?,
        };
        let (lookup, open_file, dirty, _claim, confirmed) = if let Some(existing) = existing {
            {
                if flags & libc::O_EXCL != 0 {
                    return Err(libc::EEXIST);
                }
                if existing.node.kind != MountNodeKind::Regular {
                    return Err(libc::EISDIR);
                }
                // Confirmed before the open changes anything.
                let confirmed =
                    self.confirm_names(ConfirmedNames::default(), [(child.key(), existing.node)])?;
                let claimed = claim(existing.node.file_id)?;
                let open_file = source.open_file(path).map_err(errno)?;
                let opened = open_file.lookup().map_err(errno)?;
                if opened.node.file_id != existing.node.file_id {
                    return Err(libc::ESTALE);
                }
                if flags & libc::O_TRUNC == 0 {
                    (opened, open_file, false, claimed, confirmed)
                } else {
                    open_file.resize(0).map_err(errno)?;
                    let resized = open_file.lookup().map_err(errno)?;
                    (resized, open_file, true, claimed, confirmed)
                }
            }
        } else {
            let metadata = create_metadata(request, mode, S_IFREG);
            let created = source.create_file(path, metadata).map_err(errno)?;
            let claimed = claim(created.node.file_id)?;
            (
                created,
                source.open_created(path, &created).map_err(errno)?,
                true,
                claimed,
                // A file just created has no other name.
                ConfirmedNames::default(),
            )
        };
        let mut state = self.core.state()?;
        let inode = state.intern(
            source,
            child.key().clone(),
            &lookup,
            stamp,
            true,
            &confirmed,
        )?;
        let created = state.attr(inode, &lookup).and_then(|attr| {
            let opened = OpenedFile {
                path: child.key(),
                lookup: &lookup,
                stamp,
                flags,
                open_file,
                dirty,
                stores_pages: true,
            };
            let (handle, open_flags, store) = state.admit_file_handle(source, inode, opened)?;
            Ok((attr, handle, open_flags, store))
        });
        let (attr, handle, open_flags, store) =
            created.inspect_err(|_| state.release_lookup_reference(inode, 1))?;
        let ttl = state.admit_entry(source, inode, &child, stamp);
        drop(state);
        let open_flags = self.store_pages(store, open_flags);
        Ok((Entry { attr, ttl }, handle, open_flags))
    }

    /// Runs `operation` with the handle for I/O the kernel sent on `handle`:
    /// that handle, or for a file the kernel opened without asking (handle
    /// 0), the one this session holds for the inode, opened to write on its
    /// first change. A read of a file the session holds no handle for opens
    /// one for its own duration, so a file that is only read holds nothing
    /// open.
    fn with_handle<T>(
        &self,
        inode: u64,
        handle: u64,
        access: Access,
        operation: impl FnOnce(u64) -> Result<T, i32>,
    ) -> Result<T, i32> {
        if handle != 0 || !self.skips_open {
            return operation(handle);
        }
        let held = self.core.state()?.implicit.get(&inode).copied();
        match (held, access) {
            (Some(implicit), Access::Read) => operation(implicit.handle),
            (Some(implicit), Access::Write) if implicit.writes => operation(implicit.handle),
            (_, Access::Write) => operation(self.reopen_implicit(inode, held, access)?),
            (None, Access::Read) => {
                let (transient, _) = self.open_node_storing(inode, libc::O_RDONLY, false)?;
                let result = operation(transient);
                let _ = self.core.state()?.discard_file_handle(inode, transient);
                result
            }
        }
    }

    /// Opens `inode`'s implicit handle for `access` in place of `stale`,
    /// the one it held, if any. The kernel reads what it does not hold
    /// itself, so the open stores no pages.
    fn reopen_implicit(
        &self,
        inode: u64,
        stale: Option<Implicit>,
        access: Access,
    ) -> Result<u64, i32> {
        let _names = self.core.names()?;
        self.reopen_implicit_named(inode, stale, access)
    }

    /// [`Self::reopen_implicit`] for a caller that holds the names lock.
    fn reopen_implicit_named(
        &self,
        inode: u64,
        stale: Option<Implicit>,
        access: Access,
    ) -> Result<u64, i32> {
        let writes = access == Access::Write || stale.is_some_and(|stale| stale.writes);
        let flags = if writes { libc::O_RDWR } else { libc::O_RDONLY };
        let (opened, _) = self.open_node_named(inode, flags, false)?;
        let mut state = self.core.state()?;
        let current = state.implicit.get(&inode).map(|implicit| implicit.handle);
        if current.is_some() && current != stale.map(|stale| stale.handle) {
            // Another callback reopened it meanwhile.
            let _ = state.discard_file_handle(inode, opened);
            return current.ok_or(libc::ESTALE);
        }
        if let Some(stale) = state.implicit.insert(
            inode,
            Implicit {
                handle: opened,
                writes,
            },
        ) {
            let _ = state.discard_file_handle(inode, stale.handle);
        }
        Ok(opened)
    }

    /// Reads for I/O the kernel sent on `handle`. A held implicit handle
    /// serves every open the kernel made without asking, so when the file
    /// changed around the mount since it opened, it reopens once to read
    /// what those later opens see.
    fn read_through(&self, inode: u64, handle: u64, offset: u64, size: u32) -> Result<Bytes, i32> {
        let read = self.with_handle(inode, handle, Access::Read, |opened| {
            self.read_handle(inode, opened, offset, size)
        });
        match read {
            Err(libc::ESTALE) if handle == 0 && self.skips_open => {
                let Some(stale) = self.core.state()?.implicit.get(&inode).copied() else {
                    return Err(libc::ESTALE);
                };
                let reopened = self.reopen_implicit(inode, Some(stale), Access::Read)?;
                self.read_handle(inode, reopened, offset, size)
            }
            read => read,
        }
    }

    fn read_handle(&self, inode: u64, handle: u64, offset: u64, size: u32) -> Result<Bytes, i32> {
        let open_file = self.core.state()?.open_handle(inode, handle)?;
        let bytes = open_file.read_up_to(offset, size).map_err(errno)?;
        if self.core.stopping.load(Ordering::Acquire) {
            return Err(libc::ENODEV);
        }
        Ok(bytes)
    }

    fn write_handle(&self, inode: u64, handle: u64, offset: u64, data: &[u8]) -> Result<u32, i32> {
        let operation = {
            let state = self.core.state()?;
            state.admit_write()?;
            state.open_handle_operation(inode, handle)?
        };
        let _operation = operation.lock().map_err(|_| libc::EIO)?;
        let open_file = {
            let mut state = self.core.state()?;
            let file = state
                .files
                .get_mut(&handle)
                .filter(|file| file.inode == inode && Arc::ptr_eq(&file.operation, &operation))
                .ok_or(libc::ESTALE)?;
            file.dirty = true;
            Arc::clone(&file.open_file)
        };

        #[cfg(test)]
        pause_test_write();

        let length = u32::try_from(data.len()).unwrap_or(u32::MAX);
        open_file
            .write_range(offset, Bytes::copy_from_slice(data))
            .map_err(errno)?;
        // The write recorded its change to the node, so these facts are
        // current exactly until the next change.
        let stamp = self.source().view_stamp();
        let refreshed = open_file.lookup().ok();
        let mut state = self.core.state()?;
        let current = state.files.get(&handle).is_some_and(|file| {
            file.inode == inode
                && Arc::ptr_eq(&file.operation, &operation)
                && Arc::ptr_eq(&file.open_file, &open_file)
        });
        if current && let Some(lookup) = refreshed {
            state.apply_handle_facts(inode, lookup, stamp);
        }
        Ok(length)
    }

    fn flush_handle(&self, inode: u64, handle: u64, force: bool, release: bool) -> Result<(), i32> {
        let operation = self.core.state()?.open_handle_operation(inode, handle)?;
        let _operation = operation.lock().map_err(|_| libc::EIO)?;
        let should_flush = {
            let state = self.core.state()?;
            let file = state
                .files
                .get(&handle)
                .filter(|file| file.inode == inode && Arc::ptr_eq(&file.operation, &operation))
                .ok_or(libc::ESTALE)?;
            // fsync is an explicit durability request for the file, including
            // writes made through other descriptors. A close only flushes when
            // this handle was dirty and the source requires it.
            force || (file.dirty && self.source().flush_on_handle_close())
        };
        // The per-handle operation gate remains held, but unrelated FUSE
        // callbacks never wait on the state lock during durable IO.
        let flushed = if should_flush {
            self.source().flush().map_err(errno)
        } else {
            Ok(())
        };
        let mut state = self.core.state()?;
        let current = state
            .files
            .get(&handle)
            .is_some_and(|file| file.inode == inode && Arc::ptr_eq(&file.operation, &operation));
        if !current {
            return Err(libc::ESTALE);
        }
        if flushed.is_ok()
            && let Some(file) = state.files.get_mut(&handle)
        {
            file.dirty = false;
        }
        if release {
            let discarded = state.discard_file_handle(inode, handle);
            flushed.and(discarded)
        } else {
            flushed
        }
    }

    fn list_directory<L: DirectoryListing>(&self, inode: u64, offset: u64, listing: L) {
        let source = self.source();
        let listed = (|| {
            let names = self.core.names()?;
            let mut stream = self.core.state()?.directory_stream(source, inode, offset)?;
            self.position_directory_stream(&mut stream, offset)?;
            let expected = stream.binding_epoch;
            let lease = source
                .acquire_binding_lease(expected)
                .ok()
                .filter(|_| source.view_is_stable() && source.binding_epoch() == expected)
                .ok_or(libc::ESTALE)?;
            let state = self.core.state()?;
            Ok((names, lease, stream, state))
        })();
        match listed {
            Ok((_names, _lease, mut stream, mut state)) => {
                state.list_directory(source, inode, &mut stream, listing);
                state.streams.park(inode, stream);
                drop(state);
            }
            Err(error) => listing.error(Errno::from_i32(error)),
        }
    }

    /// Advances `stream` to `offset` and buffers source entries until it
    /// holds some or its listing is exhausted. Source paging dominates
    /// listing cost, so it runs outside the state lock; a lease pins the
    /// stream's binding instead, and each page is labelled with the view it
    /// was read in.
    fn position_directory_stream(
        &self,
        stream: &mut DirectoryStream,
        offset: u64,
    ) -> Result<(), i32> {
        let source = self.source();
        // The dot entries need no source page to skip.
        stream.emitted = stream.emitted.max(offset.min(2));
        loop {
            if stream.emitted < offset
                && let Some(_skipped) = stream.entries.pop_front()
            {
                stream.emitted += 1;
                continue;
            }
            if !stream.entries.is_empty() || stream.exhausted {
                return Ok(());
            }
            let lease = source
                .acquire_binding_lease(stream.binding_epoch)
                .map_err(|_| libc::ESTALE)?;
            let stamp = source.view_stamp();
            let page = source
                .read_directory(&stream.path, stream.cursor.as_deref(), DIRECTORY_PAGE_SIZE)
                .map_err(errno)?;
            if !source.view_is_stable() || source.binding_epoch() != stream.binding_epoch {
                return Err(libc::ESTALE);
            }
            stream.exhausted = page.next_cursor.is_none();
            stream.cursor = page.next_cursor;
            drop(lease);
            stream.confirmed = self.confirm_listed_names(&stream.path, &page.entries, stamp)?;
            stream.entries.extend(page.entries);
            stream.entries_stamp = stamp;
        }
    }

    /// [`Self::confirm_names`] for one listed page, read after `stamp`,
    /// whose entries also confirm each other: names the page lists for one
    /// node all name it. Its entries are emitted later, so every
    /// confirmation is buffered and holds only while the view vouches for
    /// its name since it was resolved.
    fn confirm_listed_names(
        &self,
        directory: &MountPath,
        entries: &[MountDirectoryEntry],
        stamp: Option<ViewStamp>,
    ) -> Result<ConfirmedNames, i32> {
        let source = self.source();
        let listed = entries
            .iter()
            .filter(|entry| entry.node.link_count > 1)
            .map(|entry| {
                let name = ChildName::new(source, directory.child(entry.name.clone()));
                (name.key().clone(), entry.node)
            })
            .collect::<Vec<_>>();
        let mut confirmed = self.confirm_names(
            ConfirmedNames::default(),
            listed.iter().map(|(path, node)| (path, *node)),
        )?;
        for (path, node) in listed {
            confirmed.confirm(node.file_id, path, stamp);
        }
        Ok(confirmed)
    }

    fn attributes_to_write(&self, inode: u64) -> Result<AttributeTarget, i32> {
        let state = self.core.state()?;
        state.admit_write()?;
        state.attribute_target(inode)
    }

    fn attributes_to_read(&self, inode: u64) -> Result<Option<AttributeTarget>, i32> {
        self.core.state()?.attributes_to_read(self.source(), inode)
    }

    fn write_named_attribute(
        &self,
        inode: u64,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
    ) -> Result<(), i32> {
        if position != 0 || flags & !(libc::XATTR_CREATE | libc::XATTR_REPLACE) != 0 {
            return Err(libc::EOPNOTSUPP);
        }
        let mode = match (
            flags & libc::XATTR_CREATE != 0,
            flags & libc::XATTR_REPLACE != 0,
        ) {
            (true, true) => return Err(libc::EINVAL),
            (true, false) => super::MountAttributeWriteMode::Create,
            (false, true) => super::MountAttributeWriteMode::Replace,
            (false, false) => super::MountAttributeWriteMode::Upsert,
        };
        let _names = self.core.names()?;
        let value = Bytes::copy_from_slice(value);
        match self.attributes_to_write(inode)? {
            AttributeTarget::Handle(open_file) => {
                open_file.write_attribute(name.as_bytes(), value, mode)
            }
            AttributeTarget::Path(path) => {
                self.source()
                    .write_attribute(&path, name.as_bytes(), value, mode)
            }
        }
        .map_err(errno)
    }

    fn read_named_attribute(&self, inode: u64, name: &OsStr) -> Result<Bytes, i32> {
        let _names = self.core.names()?;
        match self.attributes_to_read(inode)? {
            Some(AttributeTarget::Handle(open_file)) => open_file.read_attribute(name.as_bytes()),
            Some(AttributeTarget::Path(path)) => {
                self.source().read_attribute(&path, name.as_bytes())
            }
            None => Ok(None),
        }
        .map_err(|error| match error {
            MountSourceError::NotFound => libc::ENODATA,
            error => errno(error),
        })?
        .ok_or(libc::ENODATA)
    }

    fn list_named_attributes(&self, inode: u64) -> Result<Vec<u8>, i32> {
        let _names = self.core.names()?;
        let Some(target) = self.attributes_to_read(inode)? else {
            return Ok(Vec::new());
        };
        let mut cursor: Option<Vec<u8>> = None;
        let mut encoded = Vec::new();
        loop {
            let page = match &target {
                AttributeTarget::Handle(open_file) => {
                    open_file.list_attributes(cursor.as_deref(), ATTRIBUTE_PAGE_SIZE)
                }
                AttributeTarget::Path(path) => {
                    self.source()
                        .list_attributes(path, cursor.as_deref(), ATTRIBUTE_PAGE_SIZE)
                }
            }
            .map_err(errno)?;
            for name in page.names {
                let required = name.len().checked_add(1).ok_or(libc::EOVERFLOW)?;
                if encoded.len().saturating_add(required) > MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES {
                    return Err(libc::E2BIG);
                }
                encoded.try_reserve(required).map_err(|_| libc::ENOMEM)?;
                encoded.extend_from_slice(&name);
                encoded.push(0);
            }
            let Some(next) = page.next_cursor else {
                return Ok(encoded);
            };
            if cursor.as_ref() == Some(&next) {
                return Err(libc::EIO);
            }
            cursor = Some(next);
        }
    }

    fn remove_named_attribute(&self, inode: u64, name: &OsStr) -> Result<(), i32> {
        let _names = self.core.names()?;
        match self.attributes_to_write(inode)? {
            AttributeTarget::Handle(open_file) => open_file.remove_attribute(name.as_bytes()),
            AttributeTarget::Path(path) => self.source().remove_attribute(&path, name.as_bytes()),
        }
        .map_err(|error| match error {
            MountSourceError::NotFound => libc::ENODATA,
            error => errno(error),
        })
    }

    fn allocate(
        &self,
        inode: u64,
        handle: u64,
        offset: u64,
        length: u64,
        mode: i32,
    ) -> Result<(), i32> {
        const KEEP_SIZE: i32 = 0x01;
        const PUNCH_HOLE: i32 = 0x02;
        const ZERO_RANGE: i32 = 0x10;
        let open_file = {
            let state = self.core.state()?;
            state.admit_write()?;
            state.open_handle(inode, handle)?
        };
        if length == 0 {
            return Err(libc::EINVAL);
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
            return Err(libc::EOPNOTSUPP);
        };
        open_file
            .allocate_range(offset, length, operation)
            .map_err(errno)?;
        let stamp = self.source().view_stamp();
        let refreshed = open_file.lookup().ok();
        let mut state = self.core.state()?;
        state.mark_handle_dirty(inode, handle)?;
        if let Some(lookup) = refreshed {
            state.apply_handle_facts(inode, lookup, stamp);
        }
        Ok(())
    }

    fn seek(&self, inode: u64, handle: u64, offset: i64, whence: i32) -> Result<i64, i32> {
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let target = match whence {
            libc::SEEK_DATA => MountSeekTarget::Data,
            libc::SEEK_HOLE => MountSeekTarget::Hole,
            _ => return Err(libc::EINVAL),
        };
        let open_file = self.core.state()?.open_handle(inode, handle)?;
        let found = open_file
            .seek(offset, target)
            .map_err(errno)?
            .ok_or(libc::ENXIO)?;
        i64::try_from(found).map_err(|_| libc::EOVERFLOW)
    }

    #[allow(clippy::too_many_arguments)]
    fn copy_range(
        &self,
        source_inode: u64,
        source_handle: u64,
        source_offset: u64,
        destination_inode: u64,
        destination_handle: u64,
        destination_offset: u64,
        length: u64,
        flags: u64,
    ) -> Result<u32, i32> {
        let (source_open, destination_open) = {
            let state = self.core.state()?;
            state.admit_write()?;
            (
                state.open_handle(source_inode, source_handle)?,
                state.open_handle(destination_inode, destination_handle)?,
            )
        };
        u32::try_from(length).map_err(|_| libc::EOVERFLOW)?;
        if flags != 0 {
            return Err(libc::EOPNOTSUPP);
        }
        if length == 0 {
            return Ok(0);
        }
        let transferred = copy_open_range(
            self.source(),
            &source_open,
            source_offset,
            &destination_open,
            destination_offset,
            length,
        )?;
        let stamp = self.source().view_stamp();
        let refreshed = destination_open.lookup().ok();
        let mut state = self.core.state()?;
        state.mark_handle_dirty(destination_inode, destination_handle)?;
        if let Some(lookup) = refreshed {
            state.apply_handle_facts(destination_inode, lookup, stamp);
        }
        Ok(transferred)
    }
}

/// Copies one range between open files, cloning stable identities where the
/// source can and moving sparse chunks otherwise.
fn copy_open_range(
    source: &dyn MountFilesystem,
    source_open: &Arc<dyn MountOpenFile>,
    source_offset: u64,
    destination_open: &Arc<dyn MountOpenFile>,
    destination_offset: u64,
    length: u64,
) -> Result<u32, i32> {
    let source_lookup = source_open.lookup().map_err(errno)?;
    let destination_lookup = destination_open.lookup().map_err(errno)?;
    let source_size = source_lookup.node.logical_bytes;
    let copy_length = length.min(source_size.saturating_sub(source_offset));
    if copy_length == 0 {
        return Ok(0);
    }
    if source_lookup.node.link_count != 0 && destination_lookup.node.link_count != 0 {
        match source.clone_range_by_id(
            source_lookup.node.file_id,
            source_offset,
            destination_lookup.node.file_id,
            destination_offset,
            copy_length,
        ) {
            Ok(()) => return u32::try_from(copy_length).map_err(|_| libc::EOVERFLOW),
            Err(MountSourceError::Unsupported(_)) => {}
            Err(error) => return Err(errno(error)),
        }
    }
    let same_open_file = Arc::ptr_eq(source_open, destination_open);
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
    u32::try_from(transferred).map_err(|_| libc::EOVERFLOW)
}

/// Replies to one callback with its result.
macro_rules! respond {
    ($reply:ident, $result:expr, |$value:pat_param| $ok:expr) => {
        match $result {
            Ok($value) => $ok,
            Err(error) => $reply.error(Errno::from_i32(error)),
        }
    };
}

// Every callback attributes the view changes it causes to this session, so
// the invalidator never drops what the kernel applied as it made them.
impl Filesystem for FuseProjection {
    fn init(&mut self, request: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        let _ = request;
        // A kernel that lists without opening keeps every listing cached,
        // which only a source with view stamps can keep coherent.
        self.skips_opendir = config
            .capabilities()
            .contains(InitFlags::FUSE_NO_OPENDIR_SUPPORT)
            && ProjectionState::directory_open_flags(self.source())
                .contains(FopenFlags::FOPEN_CACHE_DIR | FopenFlags::FOPEN_KEEP_CACHE);
        self.skips_open = config
            .capabilities()
            .contains(InitFlags::FUSE_NO_OPEN_SUPPORT)
            && self.source().view_stamp().is_some()
            && !self.source().flush_on_handle_close();
        let supported = REQUESTED_CAPABILITIES & config.capabilities();
        config
            .add_capabilities(supported)
            .map_err(|unsupported| std::io::Error::other(format!("{unsupported:?}")))
    }

    fn lookup(&self, request: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let _ = request;
        let _request = self.core.request("lookup");
        respond!(reply, self.lookup_entry(parent.0, name), |entry| entry
            .reply(reply));
    }

    fn getattr(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: Option<FuseFileHandle>,
        reply: ReplyAttr,
    ) {
        let _ = request;
        let _request = self.core.request("getattr");
        respond!(
            reply,
            self.node_attributes(
                inode.0,
                fh.map(|handle| handle.0).filter(|handle| *handle != 0)
            ),
            |(attr, ttl)| reply.attr(&ttl, &attr)
        );
    }

    fn forget(&self, request: &Request, inode: INodeNo, nlookup: u64) {
        let _ = request;
        let _request = self.core.request("forget");
        self.core
            .state_to_finish()
            .release_lookup_reference(inode.0, nlookup);
    }

    fn readlink(&self, request: &Request, inode: INodeNo, reply: ReplyData) {
        let _ = request;
        let _request = self.core.request("readlink");
        respond!(reply, self.read_link(inode.0), |target| reply.data(&target));
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
        let _ = request;
        let _request = self.core.request("setattr");
        if bkuptime.is_some() {
            return reply.error(Errno::from_i32(libc::EOPNOTSUPP));
        }
        let changes = AttributeChanges {
            mode,
            uid,
            gid,
            size,
            atime,
            mtime,
            changed: chgtime.or(ctime),
            crtime,
            flags: flags.map(|flags| flags.bits()),
        };
        respond!(
            reply,
            self.set_attributes(
                inode.0,
                fh.map(|handle| handle.0).filter(|handle| *handle != 0),
                &changes
            ),
            |(attr, ttl)| reply.attr(&ttl, &attr)
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
        let _request = self.core.request("mkdir");
        let metadata = create_metadata(request, mode & !umask, S_IFDIR);
        respond!(
            reply,
            self.create_node(parent.0, name, |path| self
                .source()
                .create_directory(path, metadata)),
            |entry| entry.reply(reply)
        );
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
        let _request = self.core.request("mknod");
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
        respond!(
            reply,
            self.create_node(parent.0, name, |path| self
                .source()
                .create_special(path, kind, device, metadata)),
            |entry| entry.reply(reply)
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
        let _request = self.core.request("symlink");
        let metadata = create_metadata(request, 0o777, S_IFLNK);
        let target = Bytes::copy_from_slice(target.as_os_str().as_bytes());
        respond!(
            reply,
            self.create_node(parent.0, link_name, |path| self
                .source()
                .create_symbolic_link(path, target, metadata)),
            |entry| entry.reply(reply)
        );
    }

    fn unlink(&self, request: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let _ = request;
        let _request = self.core.request("unlink");
        respond!(reply, self.remove_name(parent.0, name), |()| reply.ok());
    }

    fn rmdir(&self, request: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let _ = request;
        let _request = self.core.request("rmdir");
        respond!(reply, self.remove_name(parent.0, name), |()| reply.ok());
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
        let _ = request;
        let _request = self.core.request("rename");
        respond!(
            reply,
            self.rename_name(parent.0, name, new_parent.0, new_name, flags.bits()),
            |()| reply.ok()
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
        let _ = request;
        let _request = self.core.request("link");
        respond!(
            reply,
            self.link_name(inode.0, new_parent.0, new_name),
            |entry| entry.reply(reply)
        );
    }

    fn open(&self, request: &Request, inode: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let _ = request;
        let _request = self.core.request("open");
        if self.skips_open {
            // `ENOSYS` tells the kernel to open every file without asking.
            return reply.error(Errno::from_i32(libc::ENOSYS));
        }
        respond!(reply, self.open_node(inode.0, flags.0), |(
            handle,
            flags,
        )| {
            reply.opened(FuseFileHandle(handle), flags);
        });
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
        let _request = self.core.request("read");
        respond!(
            reply,
            self.read_through(inode.0, fh.0, offset, size),
            |bytes| {
                reply.data(&bytes);
            }
        );
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
        let _request = self.core.request("write");
        respond!(
            reply,
            self.with_handle(inode.0, fh.0, Access::Write, |handle| {
                self.write_handle(inode.0, handle, offset, data)
            }),
            |written| {
                reply.written(written);
            }
        );
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
        let _request = self.core.request("fsync");
        let flushed = self.with_handle(inode.0, fh.0, Access::Read, |handle| {
            self.flush_handle(inode.0, handle, true, false)
        });
        respond!(reply, flushed, |()| {
            reply.ok();
        });
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
        let _request = self.core.request("flush");
        if self.skips_open && fh.0 == 0 {
            // A close flushes nothing for this source (see `skips_open`), and
            // `ENOSYS` tells the kernel to stop sending closes.
            return reply.error(Errno::from_i32(libc::ENOSYS));
        }
        respond!(
            reply,
            self.flush_handle(inode.0, fh.0, false, false),
            |()| {
                reply.ok();
            }
        );
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
        let _request = self.core.request("release");
        respond!(reply, self.flush_handle(inode.0, fh.0, false, true), |()| {
            reply.ok();
        });
    }

    fn opendir(&self, request: &Request, inode: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let _ = (request, inode, flags);
        let _request = self.core.request("opendir");
        // `ENOSYS` tells the kernel to list every directory without opening
        // it from now on, with its listing cached.
        if self.skips_opendir {
            return reply.error(Errno::ENOSYS);
        }
        reply.opened(
            FuseFileHandle(0),
            ProjectionState::directory_open_flags(self.source()),
        );
    }

    fn readdir(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        offset: u64,
        reply: ReplyDirectory,
    ) {
        let _ = (request, fh);
        let _request = self.core.request("readdir");
        self.list_directory(inode.0, offset, reply);
    }

    fn readdirplus(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        offset: u64,
        reply: ReplyDirectoryPlus,
    ) {
        let _ = (request, fh);
        let _request = self.core.request("readdirplus");
        self.list_directory(inode.0, offset, reply);
    }

    fn releasedir(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        let _ = (request, inode, fh, flags);
        let _request = self.core.request("releasedir");
        reply.ok();
    }

    fn fsyncdir(
        &self,
        request: &Request,
        inode: INodeNo,
        fh: FuseFileHandle,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        let _ = (request, inode, fh, datasync);
        let _request = self.core.request("fsyncdir");
        // Directory fsync is an explicit durability boundary too; never hold
        // the state lock across SDK publication.
        respond!(reply, self.source().flush().map_err(errno), |()| reply.ok());
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
        let _ = request;
        let _request = self.core.request("setxattr");
        respond!(
            reply,
            self.write_named_attribute(inode.0, name, value, flags, position),
            |()| reply.ok()
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
        let _ = request;
        let _request = self.core.request("getxattr");
        respond!(reply, self.read_named_attribute(inode.0, name), |value| {
            reply_xattr(reply, &value, size);
        });
    }

    fn listxattr(&self, request: &Request, inode: INodeNo, size: u32, reply: ReplyXattr) {
        let _ = request;
        let _request = self.core.request("listxattr");
        respond!(reply, self.list_named_attributes(inode.0), |encoded| {
            reply_xattr(reply, &encoded, size);
        });
    }

    fn removexattr(&self, request: &Request, inode: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let _ = request;
        let _request = self.core.request("removexattr");
        respond!(reply, self.remove_named_attribute(inode.0, name), |()| {
            reply.ok();
        });
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
        let _ = request;
        let _request = self.core.request("fallocate");
        respond!(
            reply,
            self.with_handle(inode.0, fh.0, Access::Write, |handle| {
                self.allocate(inode.0, handle, offset, length, mode)
            }),
            |()| reply.ok()
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
        let _ = request;
        let _request = self.core.request("lseek");
        let found = self.with_handle(inode.0, fh.0, Access::Read, |handle| {
            self.seek(inode.0, handle, offset, whence)
        });
        respond!(reply, found, |found| {
            reply.offset(found);
        });
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
        let _ = request;
        let _request = self.core.request("copy_file_range");
        let copied = self.with_handle(inode_in.0, fh_in.0, Access::Read, |handle_in| {
            self.with_handle(inode_out.0, fh_out.0, Access::Write, |handle_out| {
                self.copy_range(
                    inode_in.0,
                    handle_in,
                    offset_in,
                    inode_out.0,
                    handle_out,
                    offset_out,
                    length,
                    flags.bits(),
                )
            })
        });
        respond!(reply, copied, |written| reply.written(written));
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
        let _request = self.core.request("create");
        respond!(
            reply,
            self.create_file(request, parent.0, name, mode & !umask, flags),
            |(entry, handle, open_flags)| reply.created(
                &entry.ttl,
                &entry.attr,
                Generation(0),
                FuseFileHandle(handle),
                open_flags,
            )
        );
    }
}

/// Answers a size probe or returns `value` when it fits in `size`.
fn reply_xattr(reply: ReplyXattr, value: &[u8], size: u32) {
    if size == 0 {
        reply.size(u32::try_from(value.len()).unwrap_or(u32::MAX));
    } else if value.len() <= size as usize {
        reply.data(value);
    } else {
        reply.error(Errno::from_i32(libc::ERANGE));
    }
}

fn file_type(kind: MountNodeKind) -> Result<FileType, i32> {
    Ok(match kind {
        MountNodeKind::Regular => FileType::RegularFile,
        MountNodeKind::Directory => FileType::Directory,
        MountNodeKind::SymbolicLink => FileType::Symlink,
        MountNodeKind::Fifo => FileType::NamedPipe,
        MountNodeKind::Socket => FileType::Socket,
        MountNodeKind::CharacterDevice => FileType::CharDevice,
        MountNodeKind::BlockDevice => FileType::BlockDevice,
        MountNodeKind::Unsupported => return Err(libc::EOPNOTSUPP),
    })
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
    use super::super::{
        MountAttributePage, MountAttributeWriteMode, MountDirectoryPage, MountRangeAllocation,
        MountSourceError, MountViewLease, ViewObserver,
    };
    use super::{
        CachedContent, ConfirmedNames, InodeEntry, KernelCacheItem,
        MAXIMUM_NEGATIVE_ENTRIES_PER_DIRECTORY, MountFilesystem, MountLookup, MountNode,
        MountNodeKind, MountOpenFile, MountPath, MountSeekTarget, PAGE_STORE_LIMIT, PageStores,
        ROOT_INODE, ViewStamp, admit_open, cached_projected_lookup, changes_content,
        forget_unbound_names, intern_projected, replace_root_lookup, retire_reused_identity,
        stale_kernel_items,
    };
    use crate::FileId;
    use crate::kernel::{FileMetadata, MetadataField};
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicU64;
    use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
    use std::sync::{Arc, Mutex, Weak};
    use std::time::Duration;

    type MemorySource = super::super::CheckoutMountSource<
        crate::facade::MemoryAuthorityBackend,
        crate::facade::MemoryObjectBackend,
    >;

    fn regular(file_id: FileId, logical_bytes: u64) -> MountLookup {
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

    fn directory(file_id: FileId) -> MountLookup {
        let mut lookup = regular(file_id, 0);
        lookup.node.kind = MountNodeKind::Directory;
        lookup
    }

    fn name(value: &str) -> MountPath {
        MountPath::root().child(value.as_bytes().to_vec())
    }

    /// Distinct positions in increasing order.
    fn positions<const N: usize>() -> [ViewStamp; N] {
        let slot = AtomicU64::new(0);
        std::array::from_fn(|_| ViewStamp::record(&slot))
    }

    #[test]
    fn retained_pages_require_the_version_they_were_admitted_under() {
        let file_id = FileId::new();
        let mut entry = InodeEntry::new(name("file"), regular(file_id, 4), None, 1);
        let admitted = regular(file_id, 4);
        let resized = regular(file_id, 5);
        let mut touched = admitted;
        touched.metadata.modified_ns = MetadataField::Value(7);

        assert_eq!(
            entry.admit_cached_content(&admitted),
            CachedContent::Absent,
            "nothing is cached yet"
        );
        assert_eq!(
            entry.admit_cached_content(&admitted),
            CachedContent::Current
        );
        assert_eq!(
            entry.admit_cached_content(&resized),
            CachedContent::Superseded
        );
        assert_eq!(
            entry.admit_cached_content(&touched),
            CachedContent::Superseded
        );
        assert_eq!(entry.admit_cached_content(&touched), CachedContent::Current);
    }

    #[test]
    fn negative_entries_are_cacheable_only_while_tracked() {
        let [older, newer] = positions();
        let mut entry = InodeEntry::new(MountPath::root(), directory(FileId::new()), None, 1);
        for index in 0..MAXIMUM_NEGATIVE_ENTRIES_PER_DIRECTORY {
            assert!(entry.remember_negative_child(format!("absent-{index}").as_bytes(), newer));
        }
        assert!(entry.remember_negative_child(b"absent-0", older));
        assert!(!entry.remember_negative_child(b"untracked", newer));
        assert_eq!(
            entry.negative_children.len(),
            MAXIMUM_NEGATIVE_ENTRIES_PER_DIRECTORY
        );
        // A tracked entry keeps the oldest position the kernel may hold.
        assert_eq!(entry.negative_children[b"absent-0".as_slice()], older);
        assert!(entry.remember_negative_child(b"absent-0", newer));
        assert_eq!(entry.negative_children[b"absent-0".as_slice()], older);
    }

    #[test]
    fn each_name_keeps_its_own_verification() -> Result<(), i32> {
        let [first, second, third] = positions();
        let mut linked = regular(FileId::new(), 3);
        linked.node.link_count = 2;
        let mut next_inode = ROOT_INODE + 1;
        let mut by_inode = HashMap::new();
        let mut by_path = HashMap::new();
        let mut by_file = HashMap::new();
        let mut intern = |by_inode: &mut HashMap<u64, InodeEntry>, path: &str, stamp| {
            intern_projected(
                &mut next_inode,
                by_inode,
                &mut by_path,
                &mut by_file,
                name(path),
                &linked,
                stamp,
                true,
            )
        };
        let inode = intern(&mut by_inode, "alias", Some(first))?;
        assert_eq!(intern(&mut by_inode, "original", Some(second))?, inode);
        // Refreshing one name re-verifies only that name.
        assert_eq!(intern(&mut by_inode, "original", Some(third))?, inode);
        let entry = &by_inode[&inode];
        assert_eq!(
            entry.binding(&name("alias")).and_then(|b| b.verified),
            Some(first)
        );
        assert_eq!(
            entry.binding(&name("original")).and_then(|b| b.verified),
            Some(third)
        );
        assert_eq!(entry.facts, Some(third));

        // Reuse through a name is bounded by that name's own verification.
        let mut sampled = Vec::new();
        for path in ["alias", "original"] {
            cached_projected_lookup(&mut by_inode, &by_path, &name(path), |_, stamp| {
                sampled.push(stamp);
                false
            });
        }
        assert_eq!(sampled, [first, third]);
        let entry = by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        if let Some(binding) = entry.binding_mut(&name("alias")) {
            binding.verified = None;
        }
        assert!(
            cached_projected_lookup(&mut by_inode, &by_path, &name("alias"), |_, _| true).is_none(),
            "an unverified name is never reused"
        );
        Ok(())
    }

    /// A reused identity gets a fresh inode: its removed name neither counts
    /// against the new file's links, which would refuse the new name, nor
    /// stays in the kernel, and the old inode, which the kernel may still
    /// hold, names no file any more.
    /// A name joins its node's inode on the identity rule alone: facts that
    /// still count fewer links than the names shown bound (a link made in
    /// the source since they were read) never refuse it.
    #[test]
    fn a_name_joins_its_node_whatever_link_count_its_facts_carry() -> Result<(), i32> {
        let [before, after] = positions();
        let stale = regular(FileId::new(), 3);
        let mut next_inode = ROOT_INODE + 1;
        let mut by_inode = HashMap::from([(
            ROOT_INODE,
            InodeEntry::new(MountPath::root(), directory(FileId::new()), Some(before), 1),
        )]);
        let mut by_path = HashMap::from([(MountPath::root(), ROOT_INODE)]);
        let mut by_file = HashMap::new();
        let first = intern_projected(
            &mut next_inode,
            &mut by_inode,
            &mut by_path,
            &mut by_file,
            name("a"),
            &stale,
            Some(before),
            true,
        )?;
        let second = intern_projected(
            &mut next_inode,
            &mut by_inode,
            &mut by_path,
            &mut by_file,
            name("b"),
            &stale,
            Some(after),
            true,
        )?;
        assert_eq!(first, second);
        assert_eq!(by_inode[&first].bindings.len(), 2);
        Ok(())
    }

    #[test]
    fn a_reused_identity_gets_a_fresh_inode() -> Result<(), i32> {
        let [before, after] = positions();
        let reused = regular(FileId::new(), 3);
        let mut next_inode = ROOT_INODE + 1;
        let mut by_inode = HashMap::from([(
            ROOT_INODE,
            InodeEntry::new(MountPath::root(), directory(FileId::new()), Some(before), 1),
        )]);
        let mut by_path = HashMap::from([(MountPath::root(), ROOT_INODE)]);
        let mut by_file = HashMap::new();
        let old = intern_projected(
            &mut next_inode,
            &mut by_inode,
            &mut by_path,
            &mut by_file,
            name("removed"),
            &reused,
            Some(before),
            true,
        )?;
        if let Some(binding) = by_inode
            .get_mut(&old)
            .and_then(|entry| entry.binding_mut(&name("removed")))
        {
            binding.kernel = Some(before);
        }
        let forgotten = forget_unbound_names(
            &mut by_inode,
            &mut by_path,
            old,
            &name("created"),
            |path, _| *path != name("removed"),
        );
        assert!(matches!(
            forgotten.as_slice(),
            [KernelCacheItem::Entry { parent: ROOT_INODE, name }] if name == b"removed"
        ));
        retire_reused_identity(&by_inode, &mut by_file, old);
        let created = intern_projected(
            &mut next_inode,
            &mut by_inode,
            &mut by_path,
            &mut by_file,
            name("created"),
            &reused,
            Some(after),
            true,
        )?;
        assert_ne!(created, old, "the new file gets a fresh inode");
        assert_eq!(by_inode[&created].anchor(), &name("created"));
        assert!(
            by_inode[&old].bindings.is_empty(),
            "the old inode names no file"
        );
        assert_eq!(by_path.get(&name("created")), Some(&created));
        assert_eq!(by_file.get(&reused.node.file_id), Some(&created));
        Ok(())
    }

    #[test]
    fn lookup_cache_requires_an_unchanged_view() {
        let path = name("cached");
        let lookup = regular(FileId::from_bytes([3; 16]), 7);
        let [stamp] = positions();
        let mut by_inode =
            HashMap::from([(2, InodeEntry::new(path.clone(), lookup, Some(stamp), 0))]);
        let by_path = HashMap::from([(path.clone(), 2)]);

        assert!(cached_projected_lookup(&mut by_inode, &by_path, &path, |_, _| false).is_none());
        assert_eq!(by_inode[&2].lookup_references, 0);
        assert_eq!(
            cached_projected_lookup(&mut by_inode, &by_path, &path, |file_id, sampled| {
                file_id == lookup.node.file_id && sampled == stamp
            }),
            Some((2, lookup, stamp))
        );
        assert_eq!(by_inode[&2].lookup_references, 1);
    }

    #[test]
    fn page_stores_and_content_claims_exclude_each_other() {
        let [before, target, after] = positions();
        let (stored, claimed) = (FileId::new(), FileId::new());
        let mut stores = PageStores::default();
        assert!(stores.may_begin(stored));
        stores.begin(stored, before);
        assert!(!stores.may_begin(stored), "one store per file at a time");
        assert!(stores.precede(target), "the barrier waits for older reads");
        assert!(!stores.precede(before));

        stores.claim(claimed);
        stores.claim(claimed);
        assert!(!stores.may_begin(claimed));
        stores.release(claimed);
        assert!(!stores.may_begin(claimed), "every claim must be released");
        stores.release(claimed);
        assert!(stores.may_begin(claimed));

        stores.land(stored);
        assert!(!stores.precede(target));
        stores.begin(stored, after);
        assert!(!stores.precede(target), "a newer read supersedes nothing");
        stores.land(stored);
        stores.claim(stored);
        assert!(!stores.may_begin(stored));
    }

    #[test]
    fn only_content_changing_opens_claim() {
        assert!(!changes_content(libc::O_RDONLY));
        assert!(!changes_content(libc::O_RDONLY | libc::O_DIRECT));
        assert!(changes_content(libc::O_WRONLY));
        assert!(changes_content(libc::O_RDWR));
        assert!(changes_content(libc::O_RDONLY | libc::O_TRUNC));
    }

    #[test]
    fn invalidation_drops_exactly_the_superseded_kernel_items() -> Result<(), i32> {
        let [held, through] = positions();
        let mut next_inode = ROOT_INODE + 1;
        let mut by_inode = HashMap::from([(
            ROOT_INODE,
            InodeEntry::new(MountPath::root(), directory(FileId::new()), Some(held), 1),
        )]);
        let mut by_path = HashMap::from([(MountPath::root(), ROOT_INODE)]);
        let mut by_file = HashMap::new();
        let mut linked = regular(FileId::new(), 3);
        linked.node.link_count = 2;
        let mut intern = |by_inode: &mut HashMap<u64, InodeEntry>, path: &str| {
            intern_projected(
                &mut next_inode,
                by_inode,
                &mut by_path,
                &mut by_file,
                name(path),
                &linked,
                Some(held),
                true,
            )
        };
        let file = intern(&mut by_inode, "changed")?;
        assert_eq!(intern(&mut by_inode, "kept")?, file);
        let root = by_inode.get_mut(&ROOT_INODE).ok_or(libc::ESTALE)?;
        root.kernel = Some(held);
        assert!(root.remember_negative_child(b"appeared", held));
        assert!(root.remember_negative_child(b"still-absent", held));
        let entry = by_inode.get_mut(&file).ok_or(libc::ESTALE)?;
        entry.kernel = Some(held);
        for binding in &mut entry.bindings {
            binding.kernel = Some(held);
        }
        let changed = [name("changed"), name("appeared")];

        let mut items = stale_kernel_items(
            &mut by_inode,
            &by_path,
            |path, _, _| !changed.contains(path),
            through,
        );

        let entry = |parent: u64, name: &str| KernelCacheItem::Entry {
            parent,
            name: name.as_bytes().to_vec(),
        };
        let mut expected = vec![
            // The file's anchor is its first name, which changed.
            KernelCacheItem::Inode(file),
            entry(ROOT_INODE, "changed"),
            entry(ROOT_INODE, "appeared"),
        ];
        let key = |item: &KernelCacheItem| format!("{item:?}");
        items.sort_by_key(key);
        expected.sort_by_key(key);
        assert_eq!(items, expected);
        let root = &by_inode[&ROOT_INODE];
        assert_eq!(root.kernel, Some(held), "an unchanged inode stays held");
        assert!(
            root.negative_children
                .contains_key(b"still-absent".as_slice())
        );
        assert!(!root.negative_children.contains_key(b"appeared".as_slice()));
        let file = &by_inode[&file];
        // Pages an open handle reads after the drop postdate the check.
        assert_eq!(file.kernel, Some(through));
        assert_eq!(file.binding(&name("changed")).and_then(|b| b.kernel), None);
        assert_eq!(
            file.binding(&name("kept")).and_then(|b| b.kernel),
            Some(held)
        );
        Ok(())
    }

    #[test]
    fn directory_streams_wait_at_their_offset_and_evict_the_oldest() {
        let stream = |emitted| super::DirectoryStream {
            path: MountPath::root(),
            parent_inode: ROOT_INODE,
            binding_epoch: None,
            cursor: None,
            entries: std::collections::VecDeque::new(),
            entries_stamp: None,
            confirmed: ConfirmedNames::default(),
            exhausted: false,
            emitted,
            parked: 0,
        };
        let mut streams = super::DirectoryStreams::default();
        for offset in 0..super::MAXIMUM_DIRECTORY_STREAMS as u64 {
            streams.park(ROOT_INODE, stream(offset));
        }
        assert!(streams.take(ROOT_INODE, 3).is_some());
        assert!(
            streams.take(ROOT_INODE, 3).is_none(),
            "a stream resumes once"
        );
        streams.park(ROOT_INODE, stream(3));
        streams.park(ROOT_INODE, stream(1_000));
        assert!(
            streams.take(ROOT_INODE, 0).is_none(),
            "the longest-waiting stream makes room"
        );
        assert!(streams.take(ROOT_INODE, 3).is_some());
        assert!(streams.take(ROOT_INODE, 1_000).is_some());
        assert!(streams.take(ROOT_INODE + 1, 1).is_none());
    }

    /// A name confirmed bound at a position (a listed page, read before its
    /// entries are emitted) stops counting once the name changes, so a
    /// reused identity is never kept on the inode of the node it replaced;
    /// one confirmed without a position, by a source that cannot be watched,
    /// counts with the facts it was resolved with.
    /// A callback that leaves kernel items for the invalidator wakes it as it
    /// releases the state, whether it serves a request or finishes one (a
    /// page store landing), so the items drop, and a revalidation waiting on
    /// them returns, without a further change to the source.
    #[test]
    fn releasing_deferred_items_wakes_the_invalidator() -> Result<(), Box<dyn std::error::Error>> {
        use super::{
            AttributeDefaults, DirectoryStreams, InvalidationQueue, PageStores, ProjectionCore,
            ProjectionState,
        };
        use std::sync::PoisonError;
        let state = || ProjectionState {
            defaults: AttributeDefaults {
                writable: true,
                mount_uid: 0,
                mount_gid: 0,
            },
            next_inode: ROOT_INODE + 1,
            next_handle: 1,
            by_inode: HashMap::new(),
            inode_by_path: HashMap::new(),
            inode_by_file: HashMap::new(),
            files: HashMap::new(),
            implicit: HashMap::new(),
            streams: DirectoryStreams::default(),
            invalidation: InvalidationQueue::default(),
            page_stores: PageStores::default(),
        };
        for finishing in [false, true] {
            let (source, _) = shared_sources()?;
            let core = Arc::new(ProjectionCore::new(Arc::new(source), state()));
            let waiting = core.state.lock().map_err(|_| "poisoned")?;
            let invalidator = std::thread::spawn({
                let core = Arc::clone(&core);
                move || {
                    // Waits once: only a notification ends it before the
                    // deadline.
                    let state = core.state.lock().unwrap_or_else(PoisonError::into_inner);
                    let (state, waited) = core
                        .invalidation
                        .wait_timeout(state, std::time::Duration::from_secs(20))
                        .unwrap_or_else(PoisonError::into_inner);
                    (state.invalidation.deferred.len(), waited.timed_out())
                }
            });
            drop(waiting);
            // Let the invalidator sleep before the item is deferred.
            std::thread::sleep(std::time::Duration::from_millis(50));
            let mut state = if finishing {
                core.state_to_finish()
            } else {
                core.state().map_err(|_| "stopped")?
            };
            state
                .invalidation
                .deferred
                .push(KernelCacheItem::Inode(ROOT_INODE));
            drop(state);
            let (deferred, timed_out) = invalidator.join().map_err(|_| "invalidator panicked")?;
            assert_eq!((deferred, timed_out), (1, false), "finishing: {finishing}");
        }
        Ok(())
    }

    #[test]
    fn a_confirmation_at_a_position_lapses_when_its_name_changes()
    -> Result<(), Box<dyn std::error::Error>> {
        let (source, _) = shared_sources()?;
        let file = source.create_file(&name("a"), FileMetadata::default())?;
        source.hard_link(&name("a"), &name("b"))?;
        let file_id = file.node.file_id;
        let stamp = source.view_stamp().ok_or("a checkout carries stamps")?;

        let mut confirmed = ConfirmedNames::default();
        confirmed.confirm(file_id, name("b"), Some(stamp));
        confirmed.confirm(file_id, name("a"), None);
        assert!(confirmed.still_bound(&source, file_id, &name("b")));
        assert!(confirmed.still_bound(&source, file_id, &name("a")));
        assert!(!confirmed.still_bound(&source, FileId::new(), &name("b")));
        assert!(!confirmed.still_bound(&source, file_id, &name("c")));

        // The name changes after the page was read, before it is emitted.
        source.remove(&name("b"), Some(file_id))?;
        source.create_file(&name("b"), FileMetadata::default())?;
        assert!(!confirmed.still_bound(&source, file_id, &name("b")));
        // A sibling changing leaves another name's confirmation standing.
        let mut sibling = ConfirmedNames::default();
        let stamp = source.view_stamp().ok_or("a checkout carries stamps")?;
        sibling.confirm(file_id, name("a"), Some(stamp));
        source.create_file(&name("c"), FileMetadata::default())?;
        assert!(sibling.still_bound(&source, file_id, &name("a")));
        Ok(())
    }

    #[test]
    fn node_facts_follow_the_node_and_directory_facts_its_listing()
    -> Result<(), Box<dyn std::error::Error>> {
        let (source, _) = shared_sources()?;
        let file = source.create_file(&name("file"), FileMetadata::default())?;
        let directory = source.create_directory(&name("directory"), FileMetadata::default())?;
        let stamp = source.view_stamp().ok_or("a checkout carries stamps")?;
        let mut file_entry = InodeEntry::new(name("file"), file, Some(stamp), 1);
        let directory_entry = InodeEntry::new(name("directory"), directory, Some(stamp), 1);
        assert_eq!(file_entry.current_facts(&source), Some(stamp));
        assert!(file_entry.lacks_named_attributes(&source));

        // Names changing around a node leave its facts current...
        source.create_file(&name("sibling"), FileMetadata::default())?;
        source.hard_link(&name("sibling"), &name("alias"))?;
        assert_eq!(file_entry.current_facts(&source), Some(stamp));
        // ...but not a directory's, whose listing they change.
        source.create_file(
            &name("directory").child(b"child".to_vec()),
            FileMetadata::default(),
        )?;
        assert_eq!(directory_entry.current_facts(&source), None);

        // A change to the node supersedes its facts.
        source.write_range(&name("file"), 0, Bytes::from_static(b"changed"))?;
        assert_eq!(file_entry.current_facts(&source), None);
        assert!(!file_entry.lacks_named_attributes(&source));
        file_entry.facts = source.view_stamp();
        file_entry.lookup = source.lookup(&name("file"))?.ok_or("file")?;
        assert!(file_entry.lacks_named_attributes(&source));
        source.write_attribute(
            &name("file"),
            b"user.present",
            Bytes::from_static(b"value"),
            MountAttributeWriteMode::Upsert,
        )?;
        assert!(!file_entry.lacks_named_attributes(&source));
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
    fn root_inode_survives_checkout_identity_changes() -> Result<(), i32> {
        let old_file = FileId::from_bytes([1; 16]);
        let new_file = FileId::from_bytes([2; 16]);
        let mut by_inode = HashMap::from([(
            ROOT_INODE,
            InodeEntry::new(MountPath::root(), directory(old_file), None, 1),
        )]);
        let mut inode_by_file = HashMap::from([(old_file, ROOT_INODE)]);

        replace_root_lookup(&mut by_inode, &mut inode_by_file, directory(new_file))?;

        assert_eq!(by_inode[&ROOT_INODE].lookup.node.file_id, new_file);
        assert_eq!(by_inode[&ROOT_INODE].anchor(), &MountPath::root());
        assert_eq!(inode_by_file.get(&new_file), Some(&ROOT_INODE));
        assert!(!inode_by_file.contains_key(&old_file));
        assert_eq!(
            replace_root_lookup(&mut by_inode, &mut inode_by_file, regular(FileId::new(), 0)),
            Err(libc::EIO),
            "the root stays a directory"
        );
        Ok(())
    }

    #[test]
    fn enumerated_entries_receive_nonzero_inodes_without_fake_lookup_references() -> Result<(), i32>
    {
        let mut next_inode = ROOT_INODE + 1;
        let mut by_inode = HashMap::<u64, InodeEntry>::new();
        let mut inode_by_path = HashMap::new();
        let mut inode_by_file = HashMap::new();
        let lookup = regular(FileId::new(), 4);
        let path = name("preexisting.bin");

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
        let overflow = regular(FileId::new(), 4);
        assert_eq!(
            intern_projected(
                &mut next_inode,
                &mut by_inode,
                &mut inode_by_path,
                &mut inode_by_file,
                name("overflow.bin"),
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

    type MemoryCheckout =
        crate::Checkout<crate::facade::MemoryAuthorityBackend, crate::facade::MemoryObjectBackend>;

    /// A POSIX volume, whose names are all distinct.
    fn posix_config() -> crate::model::VolumeConfig {
        let mut config = crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral);
        config.profile = crate::model::FilesystemProfile::Posix;
        config
    }

    fn memory_checkout(
        config: crate::model::VolumeConfig,
        mode: crate::model::CheckoutMode,
        count: usize,
    ) -> Result<(Vec<MemoryCheckout>, crate::model::VolumeConfig), Box<dyn std::error::Error>> {
        use crate::model::GenerationSelector;
        let fs = crate::Fs::memory();
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let checkouts = runtime.block_on(async {
            let cancellation = crate::CancellationToken::new();
            let volume = fs
                .create_volume(config, crate::WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value;
            let mut checkouts = Vec::with_capacity(count);
            for _ in 0..count {
                checkouts.push(
                    volume
                        .checkout(
                            GenerationSelector::Head,
                            mode,
                            crate::WorkBudget::UNBOUNDED,
                            &cancellation,
                        )
                        .await?
                        .value,
                );
            }
            Ok::<_, crate::OperationFailure<crate::FsError>>(checkouts)
        })?;
        Ok((checkouts, config))
    }

    /// Independent checkouts of one volume, each behind its own source.
    fn tracking_sources() -> Result<(MemorySource, MemorySource), Box<dyn std::error::Error>> {
        use crate::model::{AccessMode, CheckoutMode, ConsistencyMode, MutationMode};
        let (checkouts, config) = memory_checkout(
            posix_config(),
            CheckoutMode {
                access: AccessMode::ReadWrite,
                consistency: ConsistencyMode::TrackingSafe,
                mutations: MutationMode::PrivateOverlay,
            },
            2,
        )?;
        let mut sources = checkouts.into_iter().map(|checkout| {
            MemorySource::new(
                Arc::new(super::super::SharedCheckout::new(checkout)),
                config,
            )
        });
        match (sources.next(), sources.next()) {
            (Some(reader), Some(writer)) => Ok((reader?, writer?)),
            _ => Err("two checkouts were requested".into()),
        }
    }

    /// Two sources over one shared checkout: changes through either are
    /// changes around a mount of the other.
    fn shared_sources() -> Result<(MemorySource, MemorySource), Box<dyn std::error::Error>> {
        shared_sources_of(posix_config())
    }

    fn shared_sources_of(
        config: crate::model::VolumeConfig,
    ) -> Result<(MemorySource, MemorySource), Box<dyn std::error::Error>> {
        use crate::model::{AccessMode, CheckoutMode, ConsistencyMode, MutationMode};
        let (checkouts, config) = memory_checkout(
            config,
            CheckoutMode {
                access: AccessMode::ReadWrite,
                consistency: ConsistencyMode::Pinned,
                mutations: MutationMode::PrivateOverlay,
            },
            1,
        )?;
        let checkout = checkouts.into_iter().next().ok_or("one checkout")?;
        let shared = Arc::new(super::super::SharedCheckout::new(checkout));
        Ok((
            MemorySource::new(Arc::clone(&shared), config)?,
            MemorySource::new(shared, config)?,
        ))
    }

    fn mount(
        source: Arc<dyn MountFilesystem>,
        volume_id: crate::VolumeId,
        destination: &std::path::Path,
    ) -> Result<crate::NativeMountSession, crate::NativeMountError> {
        crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id,
                destination: destination.to_path_buf(),
                writable: true,
            },
            source,
        )
    }

    fn listing(directory: &std::path::Path) -> std::io::Result<Vec<std::ffi::OsString>> {
        let mut names = std::fs::read_dir(directory)?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<Result<Vec<_>, _>>()?;
        names.sort();
        Ok(names)
    }

    #[test]
    #[ignore = "requires a live Linux FUSE mount"]
    fn linux_changes_around_the_mount_reach_every_kernel_cache()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mounted_source, around) = shared_sources()?;
        let mounted_source = Arc::new(mounted_source);
        let file = name("file");
        around.create_file(&file, FileMetadata::default())?;
        around.write_range(&file, 0, Bytes::from_static(b"first"))?;
        let temporary = tempfile::tempdir()?;
        let mut session = mount(
            Arc::clone(&mounted_source) as Arc<dyn MountFilesystem>,
            mounted_source.volume_id()?,
            temporary.path(),
        )?;
        let mounted = temporary.path().join("file");
        let added = temporary.path().join("added");
        // Cache data, a negative entry, and the listing in the kernel.
        assert_eq!(std::fs::read(&mounted)?, b"first");
        assert!(!added.exists());
        assert_eq!(listing(temporary.path())?, ["file"]);

        // Same length, same (unavailable) times: only invalidation can tell.
        around.write_range(&file, 0, Bytes::from_static(b"other"))?;
        around.create_file(&name("added"), FileMetadata::default())?;
        session.revalidate()?;
        assert_eq!(std::fs::read(&mounted)?, b"other");
        assert!(added.is_file());
        assert_eq!(listing(temporary.path())?, ["added", "file"]);

        // Without the barrier the change still arrives, promptly.
        around.remove(&name("added"), None)?;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while added.exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "a removal around the mount never reached the kernel"
            );
            std::thread::yield_now();
        }
        assert!(session.stop()?);
        Ok(())
    }

    #[test]
    #[ignore = "requires a live Linux FUSE mount"]
    fn linux_kernel_caches_answer_unchanged_files_until_they_change()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mounted_source, around) = shared_sources()?;
        let mounted_source = Arc::new(mounted_source);
        let small = name("small");
        around.create_file(&small, FileMetadata::default())?;
        around.write_range(&small, 0, Bytes::from_static(b"first"))?;
        let large = name("large");
        let large_bytes = (0..PAGE_STORE_LIMIT + 1)
            .map(|index| u8::try_from(index % 251).unwrap_or_default())
            .collect::<Vec<_>>();
        around.create_file(&large, FileMetadata::default())?;
        around.write_range(&large, 0, Bytes::from(large_bytes.clone()))?;
        let temporary = tempfile::tempdir()?;
        let mut session = mount(
            Arc::clone(&mounted_source) as Arc<dyn MountFilesystem>,
            mounted_source.volume_id()?,
            temporary.path(),
        )?;
        let mounted = temporary.path().join("small");
        // Every request but a close, which the kernel sends asynchronously.
        let requests = |session: &crate::NativeMountSession| {
            let mut requests = session.take_fuse_requests();
            requests.retain(|operation| *operation != "release");
            requests
        };
        assert_eq!(std::fs::metadata(&mounted)?.len(), 5);
        requests(&session);

        // The first open hands the kernel the whole file: nothing is read
        // page by page, so nothing marks the access time stale either.
        assert_eq!(std::fs::read(&mounted)?, b"first");
        assert_eq!(requests(&session), ["open"]);
        for _ in 0..3 {
            assert_eq!(std::fs::metadata(&mounted)?.len(), 5);
        }
        assert_eq!(requests(&session), Vec::<&str>::new());
        assert_eq!(std::fs::read(&mounted)?, b"first");
        assert_eq!(requests(&session), ["open"], "retained pages serve reads");

        // Same length, same (unavailable) times: only invalidation can tell.
        around.write_range(&small, 0, Bytes::from_static(b"other"))?;
        session.revalidate()?;
        assert_eq!(std::fs::metadata(&mounted)?.len(), 5);
        assert_ne!(
            requests(&session),
            Vec::<&str>::new(),
            "a change is fetched"
        );
        assert_eq!(std::fs::read(&mounted)?, b"other");
        assert!(requests(&session).contains(&"open"));
        assert_eq!(std::fs::metadata(&mounted)?.len(), 5);
        assert_eq!(requests(&session), Vec::<&str>::new());

        // A change through the mount reaches every later open and stat.
        std::fs::write(&mounted, b"third!")?;
        assert_eq!(std::fs::metadata(&mounted)?.len(), 6);
        assert_eq!(std::fs::read(&mounted)?, b"third!");

        // A file past the store limit is read on demand, exactly.
        assert_eq!(std::fs::read(temporary.path().join("large"))?, large_bytes);
        assert!(requests(&session).contains(&"read"));
        assert!(session.stop()?);
        Ok(())
    }

    #[test]
    #[ignore = "requires a live Linux FUSE mount"]
    fn linux_writes_keep_node_facts_exact() -> Result<(), Box<dyn std::error::Error>> {
        let (inner, _) = shared_sources()?;
        let volume_id = inner.volume_id()?;
        let counted = Arc::new(GatedSource::ungated(Arc::new(inner)));
        let temporary = tempfile::tempdir()?;
        let mut session = mount(
            Arc::clone(&counted) as Arc<dyn MountFilesystem>,
            volume_id,
            temporary.path(),
        )?;
        let written = temporary.path().join("written");
        std::fs::write(&written, b"first")?;
        let before = counted.reads().len();
        // The kernel asks whether each write must drop capabilities, and
        // stats after writes; the facts the writes returned answer both.
        for body in [b"second write".as_slice(), b"third".as_slice()] {
            let mut file = std::fs::OpenOptions::new().write(true).open(&written)?;
            std::io::Write::write_all(&mut file, body)?;
            drop(file);
            assert_eq!(std::fs::metadata(&written)?.len(), 12);
        }
        // The directory's own attributes change with each write's parent
        // update and are read again; the written file's never are.
        assert!(
            !counted.reads()[before..].contains(&b"written".to_vec()),
            "a stat or capability check after a write went back to the source"
        );
        assert_eq!(std::fs::read(&written)?, b"thirdd write");
        assert!(session.stop()?);
        Ok(())
    }

    #[test]
    #[ignore = "requires a live Linux FUSE mount"]
    fn linux_listings_resume_across_pages_and_readers() -> Result<(), Box<dyn std::error::Error>> {
        let (inner, around) = shared_sources()?;
        let volume_id = inner.volume_id()?;
        inner.create_directory(&name("many"), FileMetadata::default())?;
        let mut expected = (0..700)
            .map(|index| format!("entry-{index:04}"))
            .collect::<Vec<_>>();
        for entry in &expected {
            inner.create_file(
                &name("many").child(entry.as_bytes().to_vec()),
                FileMetadata::default(),
            )?;
        }
        let temporary = tempfile::tempdir()?;
        let mut session = mount(
            Arc::new(inner) as Arc<dyn MountFilesystem>,
            volume_id,
            temporary.path(),
        )?;
        let many = temporary.path().join("many");
        let names = |directory: &std::path::Path| -> std::io::Result<Vec<String>> {
            let mut names = listing(directory)?
                .into_iter()
                .map(|name| name.to_string_lossy().into_owned())
                .collect::<Vec<_>>();
            names.sort();
            Ok(names)
        };
        // Several readers list concurrently, each across many kernel pages.
        std::thread::scope(|scope| {
            let readers = (0..4)
                .map(|_| scope.spawn(|| names(&many)))
                .collect::<Vec<_>>();
            for reader in readers {
                let listed = reader.join().map_err(|_| "reader panicked")??;
                assert_eq!(listed, expected);
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        })?;
        // A change around the mount reaches the cached listing.
        around.create_file(
            &name("many").child(b"added".to_vec()),
            FileMetadata::default(),
        )?;
        session.revalidate()?;
        expected.push("added".to_owned());
        expected.sort();
        assert_eq!(names(&many)?, expected);
        assert!(session.stop()?);
        Ok(())
    }

    #[test]
    fn only_folding_volumes_with_case_variants_fold_names() -> Result<(), Box<dyn std::error::Error>>
    {
        use crate::model::{CaseSensitivity, FilesystemProfile, Lifecycle, VolumeConfig};
        let folds = |profile, case_sensitivity| -> Result<bool, Box<dyn std::error::Error>> {
            let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
            config.profile = profile;
            config.case_sensitivity = case_sensitivity;
            let (source, _) = shared_sources_of(config)?;
            let folded = source.folded_path(&name("Dir").child(b"MiXeD".to_vec()));
            assert_eq!(
                folded.is_some(),
                source.folded_path(&MountPath::root()).is_some(),
                "folding is a property of the volume, not of one path"
            );
            if let Some(folded) = folded {
                assert_eq!(folded, name("dir").child(b"mixed".to_vec()));
            }
            Ok(source.folded_path(&MountPath::root()).is_some())
        };
        assert!(folds(
            FilesystemProfile::Portable,
            CaseSensitivity::ProfileFolded
        )?);
        assert!(!folds(
            FilesystemProfile::Portable,
            CaseSensitivity::Sensitive
        )?);
        // POSIX names are opaque bytes, so they have no case variants.
        assert!(!folds(
            FilesystemProfile::Posix,
            CaseSensitivity::ProfileFolded
        )?);
        Ok(())
    }

    #[test]
    #[ignore = "requires a live Linux FUSE mount"]
    fn linux_folded_names_stay_exact_through_the_mount() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::MetadataExt as _;

        let mut config = crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral);
        config.case_sensitivity = crate::model::CaseSensitivity::ProfileFolded;
        let (source, _) = shared_sources_of(config)?;
        assert_eq!(
            source.folded_path(&name("Foo")),
            Some(name("foo")),
            "a folding volume keys every spelling alike"
        );
        let volume_id = source.volume_id()?;
        source.create_file(&name("foo"), FileMetadata::default())?;
        let temporary = tempfile::tempdir()?;
        let mut session = mount(Arc::new(source), volume_id, temporary.path())?;
        let at = |name: &str| temporary.path().join(name);
        let exists = |name: &str| at(name).exists();

        // Every spelling resolves to the one node.
        let inode = std::fs::metadata(at("foo"))?.ino();
        assert_eq!(std::fs::metadata(at("FOO"))?.ino(), inode);
        assert_eq!(std::fs::metadata(at("Foo"))?.ino(), inode);
        // A rename through one spelling reaches every other spelling at once.
        std::fs::rename(at("foo"), at("bar"))?;
        assert!(!exists("FOO") && !exists("Foo") && !exists("foo"));
        assert_eq!(std::fs::metadata(at("BAR"))?.ino(), inode);
        // So does a create after absences were looked up in other spellings.
        assert!(!exists("NEW") && !exists("New"));
        std::fs::write(at("new"), b"new")?;
        assert_eq!(std::fs::read(at("NEW"))?, b"new");
        assert_eq!(std::fs::read(at("New"))?, b"new");
        // And a removal through yet another spelling.
        std::fs::remove_file(at("NeW"))?;
        assert!(!exists("new") && !exists("NEW"));
        std::fs::remove_file(at("Bar"))?;
        assert!(!exists("bar") && !exists("BAR"));
        assert!(session.stop()?);
        Ok(())
    }

    #[test]
    #[ignore = "requires a live Linux FUSE mount"]
    fn linux_rebind_reaches_every_kernel_cache() -> Result<(), Box<dyn std::error::Error>> {
        let (reader, writer) = tracking_sources()?;
        let reader = Arc::new(reader);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let file = name("file");
        writer.create_file(&file, FileMetadata::default())?;
        writer.write_range(&file, 0, Bytes::from_static(b"first"))?;
        writer.sync()?;
        runtime.block_on(reader.advance_to_head_async())?;
        let temporary = tempfile::tempdir()?;
        let mut session = mount(
            Arc::clone(&reader) as Arc<dyn MountFilesystem>,
            reader.volume_id()?,
            temporary.path(),
        )?;
        let mounted = temporary.path().join("file");
        let added = temporary.path().join("added");
        assert_eq!(std::fs::read(&mounted)?, b"first");
        assert!(!added.exists());
        assert_eq!(listing(temporary.path())?, ["file"]);

        writer.write_range(&file, 0, Bytes::from_static(b"other"))?;
        writer.create_file(&name("added"), FileMetadata::default())?;
        writer.sync()?;
        runtime.block_on(reader.advance_to_head_async())?;
        session.revalidate()?;
        assert_eq!(std::fs::read(&mounted)?, b"other");
        assert!(added.is_file());
        assert_eq!(listing(temporary.path())?, ["added", "file"]);
        assert!(session.stop()?);
        Ok(())
    }

    /// Delegates to a source, blocking lookups of one name and directory
    /// creation of another until released.
    struct GatedSource {
        inner: Arc<dyn MountFilesystem>,
        gated_lookup: Vec<u8>,
        gated_directory: Vec<u8>,
        entered: SyncSender<()>,
        released: Mutex<Receiver<()>>,
        /// The final name of every lookup and attribute read that reached
        /// the source, in order.
        reads: Mutex<Vec<Vec<u8>>>,
    }

    impl GatedSource {
        fn ungated(inner: Arc<dyn MountFilesystem>) -> Self {
            let (entered, _) = sync_channel(0);
            let (_, released) = sync_channel(0);
            Self {
                inner,
                gated_lookup: Vec::new(),
                gated_directory: Vec::new(),
                entered,
                released: Mutex::new(released),
                reads: Mutex::new(Vec::new()),
            }
        }

        fn record_read(&self, path: &MountPath) {
            self.reads
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(path.components().last().cloned().unwrap_or_default());
        }

        fn reads(&self) -> Vec<Vec<u8>> {
            self.reads
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    impl GatedSource {
        fn pass(&self, path: &MountPath, gated: &[u8]) {
            if path.components().last().is_some_and(|last| last == gated) {
                let _ = self.entered.send(());
                let released = self
                    .released
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let _ = released.recv_timeout(Duration::from_secs(10));
            }
        }
    }

    impl MountFilesystem for GatedSource {
        fn supports_posix_named_attributes(&self) -> bool {
            self.inner.supports_posix_named_attributes()
        }
        fn flush_on_handle_close(&self) -> bool {
            self.inner.flush_on_handle_close()
        }
        fn view_is_stable(&self) -> bool {
            self.inner.view_is_stable()
        }
        fn view_stamp(&self) -> Option<ViewStamp> {
            self.inner.view_stamp()
        }
        fn node_unchanged_since(&self, file_id: FileId, stamp: ViewStamp) -> bool {
            self.inner.node_unchanged_since(file_id, stamp)
        }
        fn unchanged_since(
            &self,
            path: &MountPath,
            file_id: Option<FileId>,
            stamp: ViewStamp,
        ) -> bool {
            self.inner.unchanged_since(path, file_id, stamp)
        }
        fn observe_view(&self, observer: Weak<dyn ViewObserver>) {
            self.inner.observe_view(observer);
        }
        fn binding_epoch(&self) -> Option<u64> {
            self.inner.binding_epoch()
        }
        fn acquire_view_lease(&self) -> Result<Box<dyn MountViewLease>, MountSourceError> {
            self.inner.acquire_view_lease()
        }
        fn acquire_binding_lease(
            &self,
            expected_epoch: Option<u64>,
        ) -> Result<Box<dyn MountViewLease>, MountSourceError> {
            self.inner.acquire_binding_lease(expected_epoch)
        }
        fn lookup(&self, path: &MountPath) -> Result<Option<MountLookup>, MountSourceError> {
            self.record_read(path);
            self.pass(path, &self.gated_lookup);
            self.inner.lookup(path)
        }
        fn open_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
            self.inner.open_file(path)
        }
        fn detach_file(
            &self,
            path: &MountPath,
        ) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
            self.inner.detach_file(path)
        }
        fn read_link(&self, path: &MountPath) -> Result<Bytes, MountSourceError> {
            self.inner.read_link(path)
        }
        fn read_range(
            &self,
            path: &MountPath,
            offset: u64,
            length: u32,
        ) -> Result<Bytes, MountSourceError> {
            self.inner.read_range(path, offset, length)
        }
        fn seek(
            &self,
            path: &MountPath,
            offset: u64,
            target: MountSeekTarget,
        ) -> Result<Option<u64>, MountSourceError> {
            self.inner.seek(path, offset, target)
        }
        fn read_directory(
            &self,
            path: &MountPath,
            cursor: Option<&[u8]>,
            maximum_entries: u32,
        ) -> Result<MountDirectoryPage, MountSourceError> {
            self.inner.read_directory(path, cursor, maximum_entries)
        }
        fn create_file(
            &self,
            path: &MountPath,
            metadata: FileMetadata,
        ) -> Result<MountLookup, MountSourceError> {
            self.inner.create_file(path, metadata)
        }
        fn create_directory(
            &self,
            path: &MountPath,
            metadata: FileMetadata,
        ) -> Result<MountLookup, MountSourceError> {
            self.pass(path, &self.gated_directory);
            self.inner.create_directory(path, metadata)
        }
        fn create_symbolic_link(
            &self,
            path: &MountPath,
            target: Bytes,
            metadata: FileMetadata,
        ) -> Result<MountLookup, MountSourceError> {
            self.inner.create_symbolic_link(path, target, metadata)
        }
        fn create_special(
            &self,
            path: &MountPath,
            kind: MountNodeKind,
            device: Option<(u32, u32)>,
            metadata: FileMetadata,
        ) -> Result<MountLookup, MountSourceError> {
            self.inner.create_special(path, kind, device, metadata)
        }
        fn set_attributes(
            &self,
            path: &MountPath,
            metadata: FileMetadata,
            logical_bytes: Option<u64>,
        ) -> Result<(), MountSourceError> {
            self.inner.set_attributes(path, metadata, logical_bytes)
        }
        fn read_attribute(
            &self,
            path: &MountPath,
            name: &[u8],
        ) -> Result<Option<Bytes>, MountSourceError> {
            self.record_read(path);
            self.inner.read_attribute(path, name)
        }
        fn list_attributes(
            &self,
            path: &MountPath,
            cursor: Option<&[u8]>,
            maximum_entries: u32,
        ) -> Result<MountAttributePage, MountSourceError> {
            self.inner.list_attributes(path, cursor, maximum_entries)
        }
        fn write_attribute(
            &self,
            path: &MountPath,
            name: &[u8],
            value: Bytes,
            mode: MountAttributeWriteMode,
        ) -> Result<(), MountSourceError> {
            self.inner.write_attribute(path, name, value, mode)
        }
        fn remove_attribute(&self, path: &MountPath, name: &[u8]) -> Result<(), MountSourceError> {
            self.inner.remove_attribute(path, name)
        }
        fn write_range(
            &self,
            path: &MountPath,
            offset: u64,
            bytes: Bytes,
        ) -> Result<(), MountSourceError> {
            self.inner.write_range(path, offset, bytes)
        }
        fn resize(&self, path: &MountPath, logical_bytes: u64) -> Result<(), MountSourceError> {
            self.inner.resize(path, logical_bytes)
        }
        fn allocate_range(
            &self,
            path: &MountPath,
            offset: u64,
            length: u64,
            operation: MountRangeAllocation,
        ) -> Result<(), MountSourceError> {
            self.inner.allocate_range(path, offset, length, operation)
        }
        fn clone_range(
            &self,
            source: &MountPath,
            source_offset: u64,
            destination: &MountPath,
            destination_offset: u64,
            length: u64,
        ) -> Result<(), MountSourceError> {
            self.inner.clone_range(
                source,
                source_offset,
                destination,
                destination_offset,
                length,
            )
        }
        fn clone_range_by_id(
            &self,
            source_file_id: FileId,
            source_offset: u64,
            destination_file_id: FileId,
            destination_offset: u64,
            length: u64,
        ) -> Result<(), MountSourceError> {
            self.inner.clone_range_by_id(
                source_file_id,
                source_offset,
                destination_file_id,
                destination_offset,
                length,
            )
        }
        fn remove(
            &self,
            path: &MountPath,
            expected: Option<FileId>,
        ) -> Result<(), MountSourceError> {
            self.inner.remove(path, expected)
        }
        fn rename(
            &self,
            source: &MountPath,
            destination: &MountPath,
            replace: bool,
        ) -> Result<(), MountSourceError> {
            self.inner.rename(source, destination, replace)
        }
        fn hard_link(
            &self,
            source: &MountPath,
            destination: &MountPath,
        ) -> Result<(), MountSourceError> {
            self.inner.hard_link(source, destination)
        }
        fn flush(&self) -> Result<(), MountSourceError> {
            self.inner.flush()
        }
        fn capture_host_path(
            &self,
            source_root: &std::path::Path,
            path: &MountPath,
        ) -> Result<(), MountSourceError> {
            self.inner.capture_host_path(source_root, path)
        }
        fn capture_host_paths(
            &self,
            source_root: &std::path::Path,
            paths: &[MountPath],
        ) -> Result<(), MountSourceError> {
            self.inner.capture_host_paths(source_root, paths)
        }
        fn capture_host_subtree(
            &self,
            source_root: &std::path::Path,
            path: &MountPath,
        ) -> Result<(), MountSourceError> {
            self.inner.capture_host_subtree(source_root, path)
        }
    }

    /// Runs `operation` on a fresh thread and reports whether it finished
    /// within `limit`.
    fn finishes_within(limit: Duration, operation: impl FnOnce() + Send + 'static) -> bool {
        let (done, finished) = sync_channel(1);
        std::thread::spawn(move || {
            operation();
            let _ = done.send(());
        });
        finished.recv_timeout(limit).is_ok()
    }

    #[test]
    #[ignore = "requires a live Linux FUSE mount"]
    fn linux_callbacks_proceed_while_others_block_in_the_source()
    -> Result<(), Box<dyn std::error::Error>> {
        let (inner, _) = shared_sources()?;
        let volume_id = inner.volume_id()?;
        for directory in ["slow", "busy", "free"] {
            inner.create_directory(&name(directory), FileMetadata::default())?;
        }
        let present = name("free").child(b"present".to_vec());
        inner.create_file(&present, FileMetadata::default())?;
        inner.write_range(&present, 0, Bytes::from_static(b"present"))?;
        let (entered, entries) = sync_channel(4);
        let (release, released) = sync_channel(4);
        let gated = Arc::new(GatedSource {
            inner: Arc::new(inner),
            gated_lookup: b"blocked-lookup".to_vec(),
            gated_directory: b"blocked-mkdir".to_vec(),
            entered,
            released: Mutex::new(released),
            reads: Mutex::new(Vec::new()),
        });
        let temporary = tempfile::tempdir()?;
        let mut session = mount(gated, volume_id, temporary.path())?;
        let root = temporary.path().to_path_buf();

        // One lookup and one mkdir block inside the source, each in its own
        // directory so the kernel's directory locks keep the rest free.
        let slow = root.join("slow").join("blocked-lookup");
        let blocked_lookup = std::thread::spawn(move || slow.exists());
        let busy = root.join("busy").join("blocked-mkdir");
        let blocked_mkdir = std::thread::spawn(move || std::fs::create_dir(busy));
        for _ in 0..2 {
            entries.recv_timeout(Duration::from_secs(5))?;
        }
        // Every other callback that reads the source, including lookups,
        // opens, and listings, keeps making progress meanwhile. (A source
        // mutation would rightly wait for the view the blocked lookup holds.)
        let free = root.join("free");
        assert!(
            finishes_within(Duration::from_secs(5), move || {
                assert!(!free.join("absent").exists());
                assert_eq!(
                    std::fs::read(free.join("present")).ok().as_deref(),
                    Some(b"present".as_slice())
                );
                assert!(listing(&free).is_ok_and(|names| names == ["present"]));
            }),
            "a callback waited on another callback's source work"
        );
        for _ in 0..2 {
            release.send(())?;
        }
        assert!(!blocked_lookup.join().map_err(|_| "lookup panicked")?);
        blocked_mkdir.join().map_err(|_| "mkdir panicked")??;
        assert!(session.stop()?);
        Ok(())
    }
}
