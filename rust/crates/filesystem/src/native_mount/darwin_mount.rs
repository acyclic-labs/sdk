//! Self-contained macOS mount projection over loopback `NFSv4`.
//!
//! The embedded C transport translates NFS requests into this module's bounded
//! callbacks; the canonical Rust checkout remains the only filesystem state.

#![allow(unsafe_code)]

use super::{
    DriverStartFailure, MountAttributeWriteMode, MountDirectoryEntry, MountFilesystem, MountLookup,
    MountNodeKind, MountOpenFile, MountPath, MountRangeAllocation, MountSeekTarget,
    MountSourceError, NativeMountError, NativeMountRequest, ViewObserver, ViewOrigin, ViewStamp,
    metadata_or, system_time_ns,
};
use crate::FileId;
use crate::kernel::{FileMetadata, MetadataField};
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::fs::Metadata;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, PoisonError, RwLock, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};

const ROOT_INODE: u64 = 1;
const DIRECTORY_PAGE_SIZE: u32 = 256;
const ATTRIBUTE_PAGE_SIZE: u32 = 256;
const MAXIMUM_LOOKUP_CACHE_ENTRIES: usize = 65_536;
/// Most labelled objects a revalidation verifies against the source after
/// an unconfirmed fence; with more, waiting out the attribute timeout costs
/// less.
const MAXIMUM_VERIFIED_LABELS: usize = 4_096;
const MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES: usize = 1024 * 1024;
const MAXIMUM_CALLBACK_BYTES: usize = i32::MAX as usize;
const RENAME_NOREPLACE: u32 = 1;
const FALLOC_FL_KEEP_SIZE: c_int = 0x01;
const FALLOC_FL_PUNCH_HOLE: c_int = 0x02;
const FALLOC_FL_ZERO_RANGE: c_int = 0x10;
const DISKUTIL_UNMOUNT_TIMEOUT: Duration = Duration::from_secs(4);
const MOUNT_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(10);
const MOUNT_LOOP_EXIT_TIMEOUT: Duration = Duration::from_secs(4);
const DISKUTIL_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(2);
const DIRECT_UNMOUNT_TIMEOUT: Duration = Duration::from_secs(3);
/// How soon after its callback returns a reply is assumed to reach the NFS
/// client (see [`DarwinMountContext::revalidate`]).
const REPLY_DELIVERY_SLACK: Duration = Duration::from_millis(100);
const CALLBACK_DRAIN_POLL: Duration = Duration::from_millis(1);
mod mode {
    pub(super) const IFMT: u32 = libc::S_IFMT as u32;
    pub(super) const IFIFO: u32 = libc::S_IFIFO as u32;
    pub(super) const IFSOCK: u32 = libc::S_IFSOCK as u32;
    pub(super) const IFCHR: u32 = libc::S_IFCHR as u32;
    pub(super) const IFBLK: u32 = libc::S_IFBLK as u32;
    pub(super) const IFDIR: u32 = libc::S_IFDIR as u32;
    pub(super) const IFLNK: u32 = libc::S_IFLNK as u32;
    pub(super) const IFREG: u32 = libc::S_IFREG as u32;
}

#[derive(Clone, Copy)]
#[repr(C)]
struct NativeStat {
    inode: u64,
    logical_bytes: u64,
    blocks: u64,
    accessed_seconds: i64,
    accessed_nanoseconds: u32,
    modified_seconds: i64,
    modified_nanoseconds: u32,
    changed_seconds: i64,
    changed_nanoseconds: u32,
    created_seconds: i64,
    created_nanoseconds: u32,
    mode: u32,
    link_count: u32,
    uid: u32,
    gid: u32,
    device: u64,
    block_size: u32,
    flags: u32,
    /// NFS change attribute: see [`ChangeLabels`].
    change: u64,
}

#[repr(C)]
struct NativeTimes {
    accessed_seconds: i64,
    accessed_nanoseconds: i64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
}

type DirectoryFiller = unsafe extern "C" fn(
    *mut c_void,
    *const c_char,
    *const libc::stat,
    libc::off_t,
    c_int,
) -> c_int;

unsafe extern "C" {
    fn acyclic_fs_darwin_mount_session_new() -> *mut c_void;
    fn acyclic_fs_darwin_mount_session_free(session: *mut c_void);
    fn acyclic_fs_darwin_mount_run(
        session: *mut c_void,
        argc: c_int,
        argv: *const *const c_char,
        mountpoint: *const c_char,
        context: usize,
        mounted: unsafe extern "C" fn(*mut c_void),
        mounted_argument: *mut c_void,
    ) -> c_int;
    fn acyclic_fs_darwin_mount_interrupt(session: *mut c_void);
    fn acyclic_fs_darwin_mount_invalidate(session: *mut c_void, path: *const c_char) -> c_int;
    fn acyclic_fs_darwin_mount_attribute_timeout() -> u32;
    fn acyclic_fs_darwin_mount_fill_directory(
        buffer: *mut c_void,
        filler: DirectoryFiller,
        name: *const c_char,
        attributes: *const NativeStat,
        next_offset: i64,
    ) -> c_int;
}

struct FileHandle {
    file_id: FileId,
    /// Bound to the source file on first use: an open that is only closed,
    /// as a read served from the kernel's cache is, never opens the source.
    file: Option<Arc<dyn MountOpenFile>>,
    observation: Arc<Mutex<FileObservation>>,
    /// Ledger sequence of the latest mutation through this handle; zero
    /// until one completes.
    written: Arc<AtomicU64>,
}

struct FileObservation {
    stamp: Option<CacheStamp>,
    lookup: MountLookup,
}

/// When this mount sampled the source's view, before observing facts that
/// a later callback may reuse: the source's stamp, and how many changes the
/// source did not record the mount had been told of by then (see
/// [`DarwinMountContext::forget_view`]). Ordered by age.
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
struct CacheStamp {
    forgotten: u64,
    view: ViewStamp,
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct CacheEpochs {
    view: CacheStamp,
    binding: u64,
}

#[derive(Clone)]
struct DirectoryHandle {
    path: MountPath,
    binding_epoch: Option<u64>,
    epochs: Option<CacheEpochs>,
    cursor: Option<Vec<u8>>,
    entries: VecDeque<MountDirectoryEntry>,
    exhausted: bool,
    emitted: i64,
    revision: Option<u64>,
}

struct DirectoryCheckpoint {
    revision: u64,
    handle: DirectoryHandle,
}

impl DirectoryHandle {
    fn new(path: MountPath, binding_epoch: Option<u64>, epochs: Option<CacheEpochs>) -> Self {
        Self {
            path,
            binding_epoch,
            epochs,
            cursor: None,
            entries: VecDeque::new(),
            exhausted: false,
            emitted: 0,
            revision: None,
        }
    }

    fn rewind(&mut self) {
        self.cursor = None;
        self.entries.clear();
        self.exhausted = false;
        self.emitted = 0;
        self.revision = None;
    }

    /// Whether buffered pages still describe the directory: its binding is
    /// the same and nothing has changed its listing since they were read.
    fn is_current(&self, context: &DarwinMountContext) -> bool {
        match self.epochs {
            Some(epochs) => {
                context.source.view_is_stable()
                    && context.source.binding_epoch() == Some(epochs.binding)
                    && context.unchanged_since(&self.path, None, epochs.view)
            }
            None => context.cache_epochs().is_none(),
        }
    }

    const fn can_reuse_pages(&self) -> bool {
        self.epochs.is_some()
    }
}

struct DarwinMountContext {
    source: Arc<dyn MountFilesystem>,
    writable: bool,
    mount_uid: u32,
    mount_gid: u32,
    next_handle: AtomicU64,
    next_inode: AtomicU64,
    inodes: Mutex<HashMap<FileId, u64>>,
    /// Changes the source did not record that this mount was told of.
    forgotten: AtomicU64,
    lookups: Mutex<LookupCache>,
    files: RwLock<HashMap<u64, FileHandle>>,
    directories: Mutex<HashMap<u64, Arc<Mutex<DirectoryHandle>>>>,
    directory_checkpoints: Mutex<HashMap<MountPath, DirectoryCheckpoint>>,
    namespace_revision: AtomicU64,
    ledger: FlushLedger,
    changes: ChangeLabels,
    /// Attributes every change this mount's callbacks make to the mount.
    origin: ViewOrigin,
    callbacks: CallbackGate,
    around: AroundChanges,
}

/// Native callbacks in flight, in two generations, so a barrier can wait
/// for every callback that began before it without holding up later ones.
struct CallbackGate {
    generation: AtomicU64,
    /// Callbacks in flight that entered in an even or odd generation.
    even: AtomicU64,
    odd: AtomicU64,
    /// Serializes barriers, so each drains a generation before the next
    /// one reuses its slot.
    draining: Mutex<()>,
}

/// One callback's place in its generation, left when dropped.
struct CallbackPass<'a>(&'a AtomicU64);

impl CallbackGate {
    const fn new() -> Self {
        Self {
            generation: AtomicU64::new(0),
            even: AtomicU64::new(0),
            odd: AtomicU64::new(0),
            draining: Mutex::new(()),
        }
    }

    fn slot(&self, generation: u64) -> &AtomicU64 {
        if generation.is_multiple_of(2) {
            &self.even
        } else {
            &self.odd
        }
    }

    fn enter(&self) -> CallbackPass<'_> {
        loop {
            let generation = self.generation.load(Ordering::SeqCst);
            let active = self.slot(generation);
            active.fetch_add(1, Ordering::SeqCst);
            // A barrier that began in between may already have found this
            // slot idle; join the generation after it instead.
            if self.generation.load(Ordering::SeqCst) == generation {
                return CallbackPass(active);
            }
            active.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Returns once every callback that entered before this call has left.
    fn drain(&self) {
        let _draining = self.draining.lock().unwrap_or_else(PoisonError::into_inner);
        let active = self.slot(self.generation.fetch_add(1, Ordering::SeqCst));
        while active.load(Ordering::SeqCst) != 0 {
            std::thread::sleep(CALLBACK_DRAIN_POLL);
        }
    }
}

impl Drop for CallbackPass<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Changes made around the mount, which the NFS client learns of only when
/// its cached attributes expire.
struct AroundChanges {
    /// Changes another origin reported, plus views forgotten.
    made: AtomicU64,
    /// `made` as of the latest barrier that waited them out.
    settled: AtomicU64,
    /// Fences that could not confirm every change was reported.
    unconfirmed: AtomicU64,
    /// `unconfirmed` as of the latest barrier that verified or waited them
    /// out.
    verified: AtomicU64,
}

impl ViewObserver for DarwinMountContext {
    fn view_changed(&self, _position: ViewStamp, origin: ViewOrigin) {
        // The client learns of this mount's own changes from their replies.
        if origin != self.origin {
            self.around.made.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn view_unconfirmed(&self, _position: ViewStamp) {
        self.around.unconfirmed.fetch_add(1, Ordering::AcqRel);
    }
}

/// Uptime excluding sleep: the clock the NFS client stamps cached
/// attributes with (`microuptime`).
fn uptime() -> Duration {
    let mut now = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `now` is a valid, writable timespec; the clock always exists.
    unsafe { libc::clock_gettime(libc::CLOCK_UPTIME_RAW, &raw mut now) };
    Duration::new(
        u64::try_from(now.tv_sec).unwrap_or(0),
        u32::try_from(now.tv_nsec).unwrap_or(0),
    )
}

/// Orders completed native mutations against source flushes.
///
/// A mutating callback advances `completed` only after its source call
/// returns, so a flush that samples `completed` before it starts covers every
/// mutation up to that sample. The ledger starts dirty because the state the
/// mount opened over may be unpublished, and the first sync must cover it.
struct FlushLedger {
    completed: AtomicU64,
    flushed: AtomicU64,
    /// Serializes flushes: a waiter that a finished flush already covered
    /// returns without flushing again, so concurrent syncs share one flush.
    flushing: Mutex<()>,
}

impl FlushLedger {
    const fn new() -> Self {
        Self {
            completed: AtomicU64::new(1),
            flushed: AtomicU64::new(0),
            flushing: Mutex::new(()),
        }
    }

    /// Records one completed (or failed, possibly partial) mutation and
    /// returns its sequence.
    fn record(&self) -> u64 {
        self.completed.fetch_add(1, Ordering::AcqRel) + 1
    }

    fn latest(&self) -> u64 {
        self.completed.load(Ordering::Acquire)
    }

    /// Makes every mutation up to `required` durable, flushing the source
    /// only when no earlier successful flush already covers it.
    fn flush_through(&self, required: u64, source: &dyn MountFilesystem) -> Result<(), i32> {
        if self.flushed.load(Ordering::Acquire) >= required {
            return Ok(());
        }
        let _flushing = self.flushing.lock().map_err(|_| libc::EIO)?;
        if self.flushed.load(Ordering::Acquire) >= required {
            return Ok(());
        }
        let covered = self.latest();
        source.flush().map_err(|error| errno(&error))?;
        self.flushed.fetch_max(covered, Ordering::AcqRel);
        Ok(())
    }
}

/// Per-object NFS change attributes (RFC 7530 s5.4).
///
/// A node keeps its label while the source reports nothing that node's
/// attributes, listing, or name depend on changed since the label was
/// issued, or, across changes the source could not confirm it reported,
/// while the attributes observed again match those it was issued for;
/// otherwise it gets a fresh one. Labels come from one counter and are
/// never reissued, so a stale cached value can never match again, and a
/// write to one file leaves every other object's cached state valid.
struct ChangeLabels {
    next: AtomicU64,
    labels: Mutex<HashMap<FileId, IssuedLabel>>,
    /// Times the labels were dropped at capacity, after which a revalidation
    /// cannot tell what the client holds.
    cleared: AtomicU64,
}

#[derive(Clone)]
struct IssuedLabel {
    label: u64,
    /// Sampled before the attributes the label was issued for.
    stamp: CacheStamp,
    binding: Option<u64>,
    /// The attributes it was issued for, and the path they were observed at.
    observed: Observed,
}

/// Attributes a label was issued for, as observed at a path.
#[derive(Clone)]
struct Observed {
    path: MountPath,
    lookup: MountLookup,
}

impl Observed {
    /// Whether `lookup` shows the client what these attributes did. Access
    /// times are left out: no source reports reads, so no label follows them.
    ///
    /// A directory's label also covers its listing, which its attributes
    /// witness only through its change time: every entry created, removed,
    /// or renamed in it sets that time, and no caller can set it back. A
    /// directory whose change time is unknown is never taken as unchanged.
    fn matches(&self, lookup: &MountLookup) -> bool {
        let mut seen = *lookup;
        seen.metadata.accessed_ns = self.lookup.metadata.accessed_ns;
        seen == self.lookup
            && (lookup.node.kind != MountNodeKind::Directory
                || matches!(lookup.metadata.changed_ns, MetadataField::Value(_)))
    }
}

impl ChangeLabels {
    fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
            labels: Mutex::new(HashMap::new()),
            cleared: AtomicU64::new(0),
        }
    }

    /// The label for `file_id`'s attributes, `observed` after `stamp` was
    /// sampled under source binding `binding`; `unchanged` tells whether
    /// nothing they depend on changed since a stamp, and `reported_unchanged`
    /// whether nothing reported did. A source without stamps gets a fresh
    /// label every time, so its cached state is never trusted past a
    /// revalidation.
    fn label(
        &self,
        file_id: FileId,
        stamp: Option<CacheStamp>,
        binding: Option<u64>,
        observed: Observed,
        unchanged: impl FnOnce(CacheStamp) -> bool,
        reported_unchanged: impl FnOnce(CacheStamp) -> bool,
    ) -> u64 {
        let Some(stamp) = stamp else {
            return self.fresh();
        };
        // Labels only ever move to fresh values, so a panicked holder
        // cannot leave one that repeats.
        let mut labels = self.labels.lock().unwrap_or_else(PoisonError::into_inner);
        // The issued label names these attributes only if nothing changed
        // since the older of the two observations, so attributes observed
        // before a change never take a label issued after it.
        if let Some(issued) = labels.get_mut(&file_id)
            && issued.binding == binding
        {
            let since = issued.stamp.min(stamp);
            if unchanged(since) {
                return issued.label;
            }
            // Read again across changes the source could not confirm, with
            // the same result: the client's copy still holds.
            if issued.observed.matches(&observed.lookup) && reported_unchanged(since) {
                issued.stamp = issued.stamp.max(stamp);
                return issued.label;
            }
        }
        if labels.len() >= MAXIMUM_LOOKUP_CACHE_ENTRIES {
            labels.clear();
            self.cleared.fetch_add(1, Ordering::AcqRel);
        }
        let label = self.fresh();
        labels.insert(
            file_id,
            IssuedLabel {
                label,
                stamp,
                binding,
                observed,
            },
        );
        label
    }

    /// What the client may hold labels for: each labelled node with the
    /// attributes and path it was labelled at; `None` past
    /// [`MAXIMUM_VERIFIED_LABELS`].
    fn issued(&self) -> Option<Vec<(FileId, Observed)>> {
        let labels = self.labels.lock().unwrap_or_else(PoisonError::into_inner);
        (labels.len() <= MAXIMUM_VERIFIED_LABELS).then(|| {
            labels
                .iter()
                .map(|(file_id, issued)| (*file_id, issued.observed.clone()))
                .collect()
        })
    }

    fn fresh(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }
}

/// Lookups, each with the stamp it was resolved after.
struct LookupCache {
    entries: HashMap<MountPath, (Option<MountLookup>, CacheStamp)>,
}

impl DarwinMountContext {
    fn new(
        source: Arc<dyn MountFilesystem>,
        writable: bool,
        metadata: &Metadata,
        root_file_id: FileId,
    ) -> Self {
        Self {
            source,
            writable,
            mount_uid: metadata.uid(),
            mount_gid: metadata.gid(),
            next_handle: AtomicU64::new(1),
            next_inode: AtomicU64::new(ROOT_INODE + 1),
            inodes: Mutex::new(HashMap::from([(root_file_id, ROOT_INODE)])),
            forgotten: AtomicU64::new(0),
            lookups: Mutex::new(LookupCache {
                entries: HashMap::new(),
            }),
            files: RwLock::new(HashMap::new()),
            directories: Mutex::new(HashMap::new()),
            directory_checkpoints: Mutex::new(HashMap::new()),
            namespace_revision: AtomicU64::new(0),
            ledger: FlushLedger::new(),
            changes: ChangeLabels::new(),
            origin: ViewOrigin::new(),
            callbacks: CallbackGate::new(),
            around: AroundChanges {
                made: AtomicU64::new(0),
                settled: AtomicU64::new(0),
                unconfirmed: AtomicU64::new(0),
                verified: AtomicU64::new(0),
            },
        }
    }

    /// Shares this context, told of every change to its source's view.
    fn observing_source(self) -> Arc<Self> {
        let context = Arc::new(self);
        let observer: Weak<dyn ViewObserver> = Arc::<Self>::downgrade(&context);
        context.source.observe_view(observer);
        context
    }

    fn allocate_handle(&self) -> Result<u64, i32> {
        let handle = self.next_handle.fetch_add(1, Ordering::Relaxed);
        if handle == 0 || handle == u64::MAX {
            return Err(libc::EMFILE);
        }
        Ok(handle)
    }

    fn namespace_changed(&self) {
        self.namespace_revision.fetch_add(1, Ordering::Release);
    }

    /// Samples the view before observing facts a later callback may reuse;
    /// `None` when the source cannot invalidate exactly.
    fn cache_stamp(&self) -> Option<CacheStamp> {
        let forgotten = self.forgotten.load(Ordering::Acquire);
        Some(CacheStamp {
            forgotten,
            view: self.source.view_stamp()?,
        })
    }

    /// Whether facts about `path`, which resolved to `file_id` (`None`: to
    /// nothing), observed after `stamp` still describe the view.
    fn unchanged_since(
        &self,
        path: &MountPath,
        file_id: Option<FileId>,
        stamp: CacheStamp,
    ) -> bool {
        stamp.forgotten == self.forgotten.load(Ordering::Acquire)
            && self.source.unchanged_since(path, file_id, stamp.view)
    }

    /// [`Self::unchanged_since`], counting only changes the source reported.
    fn reported_unchanged_since(
        &self,
        path: &MountPath,
        file_id: Option<FileId>,
        stamp: CacheStamp,
    ) -> bool {
        stamp.forgotten == self.forgotten.load(Ordering::Acquire)
            && self
                .source
                .reported_unchanged_since(path, file_id, stamp.view)
    }

    fn cache_epochs(&self) -> Option<CacheEpochs> {
        if !self.source.view_is_stable() {
            return None;
        }
        Some(CacheEpochs {
            view: self.cache_stamp()?,
            binding: self.source.binding_epoch()?,
        })
    }

    /// Stops trusting every fact this mount observed so far, because a
    /// change the source did not record may have superseded any of them:
    /// cached lookups, handle observations, and directory pages all fail
    /// validation, and every object's next change attribute is fresh, so
    /// the client discards what it cached at its next revalidation.
    fn forget_view(&self) {
        self.forgotten.fetch_add(1, Ordering::AcqRel);
        self.namespace_changed();
        self.lookups
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entries
            .clear();
        self.around.made.fetch_add(1, Ordering::AcqRel);
    }

    /// Waits until the NFS client holds nothing that a change made around
    /// the mount before this call superseded. Changes made through the mount
    /// need no wait: the client learns of them from their replies.
    ///
    /// Without delegations, which a local-socket mount cannot take, the
    /// server cannot recall what the client cached. The client trusts
    /// cached attributes, names, and directory pages until the attribute
    /// timeout expires, measured in whole seconds of uptime from when each
    /// reply arrived, and then revalidates them against change attributes,
    /// which every such change moved (see [`ChangeLabels`]). Every callback
    /// that began before this call is waited out; the replies they sent
    /// are assumed to arrive within [`REPLY_DELIVERY_SLACK`], so all expire
    /// by the uptime second one timeout after that. The wait is at most the
    /// timeout plus the slack and averages half a second less.
    ///
    /// A fence that could not confirm every change was reported (macOS)
    /// needs no wait when nothing the client holds changed: each object it
    /// holds a label for is read again, and the wait follows only if one no
    /// longer matches what its label was issued for.
    fn revalidate(&self) {
        let unconfirmed = self.around.unconfirmed.load(Ordering::Acquire);
        if self.around.verified.load(Ordering::Acquire) < unconfirmed && !self.verify_labels() {
            self.around.made.fetch_add(1, Ordering::AcqRel);
        }
        self.around
            .verified
            .fetch_max(unconfirmed, Ordering::AcqRel);
        let made = self.around.made.load(Ordering::Acquire);
        if self.around.settled.load(Ordering::Acquire) >= made {
            return;
        }
        self.callbacks.drain();
        let arrived_by = uptime() + REPLY_DELIVERY_SLACK;
        // SAFETY: the bridge reports a constant.
        let timeout = u64::from(unsafe { acyclic_fs_darwin_mount_attribute_timeout() });
        let expired = Duration::from_secs(arrived_by.as_secs() + timeout);
        loop {
            let now = uptime();
            if now >= expired {
                break;
            }
            std::thread::sleep(expired - now);
        }
        self.around.settled.fetch_max(made, Ordering::AcqRel);
    }

    /// Whether every object the client holds a label for still shows what
    /// its label was issued for, read again from the source. False when any
    /// differs, or when what the client holds is unknown.
    fn verify_labels(&self) -> bool {
        let cleared = self.changes.cleared.load(Ordering::Acquire);
        let Some(issued) = self.changes.issued() else {
            return false;
        };
        issued.iter().all(|(file_id, observed)| {
            self.lookup(&observed.path)
                .is_ok_and(|current| current.node.file_id == *file_id && observed.matches(&current))
        }) && self.changes.cleared.load(Ordering::Acquire) == cleared
    }

    fn inode(&self, file_id: FileId) -> Result<u64, i32> {
        let mut inodes = self.inodes.lock().map_err(|_| libc::EIO)?;
        if let Some(inode) = inodes.get(&file_id) {
            return Ok(*inode);
        }
        let inode = self.next_inode.fetch_add(1, Ordering::Relaxed);
        if inode <= ROOT_INODE || inode == u64::MAX {
            return Err(libc::EOVERFLOW);
        }
        inodes.try_reserve(1).map_err(|_| libc::ENOMEM)?;
        inodes.insert(file_id, inode);
        Ok(inode)
    }

    fn lookup(&self, path: &MountPath) -> Result<MountLookup, i32> {
        // Sampled first: a change to anything the lookup reads records a
        // later position, so its result can never validate over the change.
        let stamp = self.cache_stamp();
        if stamp.is_some() {
            let cache = self.lookups.lock().map_err(|_| libc::EIO)?;
            if let Some((cached, cached_stamp)) = cache.entries.get(path).copied()
                && self.unchanged_since(
                    path,
                    cached.map(|lookup| lookup.node.file_id),
                    cached_stamp,
                )
            {
                return cached.ok_or(libc::ENOENT);
            }
        }
        let lookup = self.source.lookup(path).map_err(|error| errno(&error))?;
        if let Some(stamp) = stamp {
            self.remember_lookup(path, lookup, stamp)?;
        }
        lookup.ok_or(libc::ENOENT)
    }

    fn remember_current_lookup(
        &self,
        path: &MountPath,
        lookup: Option<MountLookup>,
    ) -> Result<(), i32> {
        let Some(stamp) = self.cache_stamp() else {
            return Ok(());
        };
        self.remember_lookup(path, lookup, stamp)?;
        Ok(())
    }

    fn remember_lookup(
        &self,
        path: &MountPath,
        lookup: Option<MountLookup>,
        stamp: CacheStamp,
    ) -> Result<(), i32> {
        let mut lookups = self.lookups.lock().map_err(|_| libc::EIO)?;
        if lookups.entries.len() >= MAXIMUM_LOOKUP_CACHE_ENTRIES {
            lookups.entries.clear();
        }
        lookups.entries.try_reserve(1).map_err(|_| libc::ENOMEM)?;
        lookups.entries.insert(path.clone(), (lookup, stamp));
        Ok(())
    }

    /// Opens a handle on the regular file at `path`. The source file is
    /// bound on first use (see [`Self::bind`]).
    fn open(&self, path: &MountPath, state: InitialHandleState) -> Result<u64, i32> {
        let written = match state {
            InitialHandleState::Clean => 0,
            InitialHandleState::Written => self.ledger.latest(),
        };
        // Sampled first, so a change after it cannot validate the lookup.
        let stamp = self.cache_stamp();
        let lookup = self.lookup(path)?;
        match lookup.node.kind {
            MountNodeKind::Regular => {}
            MountNodeKind::Directory => return Err(libc::EISDIR),
            _ => return Err(libc::EINVAL),
        }
        let handle = self.allocate_handle()?;
        let mut files = self.files.write().map_err(|_| libc::EIO)?;
        files.try_reserve(1).map_err(|_| libc::ENOMEM)?;
        files.insert(
            handle,
            FileHandle {
                file_id: lookup.node.file_id,
                file: None,
                observation: Arc::new(Mutex::new(FileObservation { stamp, lookup })),
                written: Arc::new(AtomicU64::new(written)),
            },
        );
        Ok(handle)
    }

    /// The source file behind `handle`, opened at `path` on first use.
    ///
    /// A handle names one file identity. Until a callback needs its source
    /// file, `path` (the kernel's current name for the handle's object) still
    /// names that identity unless something outside this mount removed or
    /// replaced it, which fails the handle as a stale NFS handle; a removal
    /// or replacement through this mount detaches its file first (see [`Self::unlink`]).
    fn bind(&self, path: &MountPath, handle: u64) -> Result<Arc<dyn MountOpenFile>, i32> {
        let file_id = {
            let files = self.files.read().map_err(|_| libc::EIO)?;
            let entry = files.get(&handle).ok_or(libc::ESTALE)?;
            if let Some(file) = &entry.file {
                return Ok(Arc::clone(file));
            }
            entry.file_id
        };
        let file = self.source.open_file(path).map_err(|error| errno(&error))?;
        if file.lookup().map_err(|error| errno(&error))?.node.file_id != file_id {
            return Err(libc::ESTALE);
        }
        let mut files = self.files.write().map_err(|_| libc::EIO)?;
        let entry = files.get_mut(&handle).ok_or(libc::ESTALE)?;
        Ok(Arc::clone(entry.file.get_or_insert(file)))
    }

    /// Runs `unlink`, which removes `path`, the name `lookup` resolved.
    /// When that is the last name of a regular file with open handles, the
    /// file is detached first and, once the name is gone, those handles use
    /// it, so an unlinked open file stays readable and writable.
    fn unlink(
        &self,
        path: &MountPath,
        lookup: MountLookup,
        unlink: impl FnOnce() -> Result<(), MountSourceError>,
    ) -> Result<(), i32> {
        let file_id = lookup.node.file_id;
        let has_open = lookup.node.kind == MountNodeKind::Regular
            && lookup.node.link_count == 1
            && self
                .files
                .read()
                .map_err(|_| libc::EIO)?
                .values()
                .any(|entry| entry.file_id == file_id);
        let detached = self.mutate(|| {
            let detached = has_open
                .then(|| self.source.detach_file(path))
                .transpose()?;
            unlink()?;
            Ok(detached)
        })?;
        if let Some(detached) = detached {
            for entry in self
                .files
                .write()
                .map_err(|_| libc::EIO)?
                .values_mut()
                .filter(|entry| entry.file_id == file_id)
            {
                entry.file = Some(Arc::clone(&detached));
            }
        }
        Ok(())
    }

    /// The open handle's file and written marker, or a path-bound file
    /// without a marker when `handle` is zero.
    fn file_target(&self, path: &MountPath, handle: u64) -> Result<FileTarget, i32> {
        if handle == 0 {
            let file = self.source.open_file(path).map_err(|error| errno(&error))?;
            return Ok(FileTarget {
                file,
                written: None,
            });
        }
        let file = self.bind(path, handle)?;
        let files = self.files.read().map_err(|_| libc::EIO)?;
        let entry = files.get(&handle).ok_or(libc::ESTALE)?;
        Ok(FileTarget {
            file,
            written: Some(Arc::clone(&entry.written)),
        })
    }

    fn file_or_open(&self, path: &MountPath, handle: u64) -> Result<Arc<dyn MountOpenFile>, i32> {
        self.file_target(path, handle).map(|target| target.file)
    }

    /// Runs one mutating callback and records its completion before the
    /// reply, including a failed attempt that may have applied partially, so
    /// a later sync covers it and the change attribute advances.
    fn mutate<T>(&self, mutation: impl FnOnce() -> Result<T, MountSourceError>) -> Result<T, i32> {
        self.admit_write()?;
        let result = mutation().map_err(|error| errno(&error));
        self.ledger.record();
        result
    }

    /// Mutates one regular file through `handle` (or a path-bound file when
    /// it is zero) and marks that handle written, so its close publishes
    /// when the source publishes on close.
    fn mutate_file<T>(
        &self,
        path: &MountPath,
        handle: u64,
        mutation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>,
    ) -> Result<T, i32> {
        self.admit_write()?;
        let target = self.file_target(path, handle)?;
        let result = mutation(target.file.as_ref()).map_err(|error| errno(&error));
        let sequence = self.ledger.record();
        if let Some(written) = target.written {
            written.fetch_max(sequence, Ordering::AcqRel);
        }
        result
    }

    /// The attributes of `handle`'s file, or of the file at `path` when
    /// `handle` is zero.
    fn lookup_handle(&self, path: &MountPath, handle: u64) -> Result<MountLookup, i32> {
        if handle == 0 {
            return self.lookup(path);
        }
        let observation = {
            let files = self.files.read().map_err(|_| libc::EIO)?;
            Arc::clone(&files.get(&handle).ok_or(libc::ESTALE)?.observation)
        };
        {
            let observation = observation.lock().map_err(|_| libc::EIO)?;
            if observation.stamp.is_some_and(|stamp| {
                self.unchanged_since(path, Some(observation.lookup.node.file_id), stamp)
            }) {
                return Ok(observation.lookup);
            }
        }
        let stamp = self.cache_stamp();
        let lookup = self
            .bind(path, handle)?
            .lookup()
            .map_err(|error| errno(&error))?;
        if stamp.is_some() {
            let mut observation = observation.lock().map_err(|_| libc::EIO)?;
            observation.stamp = stamp;
            observation.lookup = lookup;
        }
        Ok(lookup)
    }

    fn attributes(&self, path: &MountPath, handle: u64) -> Result<NativeStat, i32> {
        // Sampled before observing: a change after it cannot keep the label.
        let stamp = self.cache_stamp();
        let lookup = self.lookup_handle(path, handle)?;
        self.attributes_from_lookup(path, lookup, stamp)
    }

    /// Native attributes of `lookup`, observed at `path` after `stamp`.
    fn attributes_from_lookup(
        &self,
        path: &MountPath,
        lookup: MountLookup,
        stamp: Option<CacheStamp>,
    ) -> Result<NativeStat, i32> {
        let node = lookup.node;
        let file_kind = match node.kind {
            MountNodeKind::Regular => mode::IFREG,
            MountNodeKind::Directory => mode::IFDIR,
            MountNodeKind::SymbolicLink => mode::IFLNK,
            MountNodeKind::Fifo => mode::IFIFO,
            MountNodeKind::Socket => mode::IFSOCK,
            MountNodeKind::CharacterDevice => mode::IFCHR,
            MountNodeKind::BlockDevice => mode::IFBLK,
            MountNodeKind::Unsupported => return Err(libc::EOPNOTSUPP),
        };
        let default_mode = if self.writable { 0o755 } else { 0o555 };
        let mode = metadata_or(lookup.metadata.posix_mode, default_mode) & 0o7777;
        let (accessed_seconds, accessed_nanoseconds) = metadata_time(lookup.metadata.accessed_ns);
        let (modified_seconds, modified_nanoseconds) = metadata_time(lookup.metadata.modified_ns);
        let (changed_seconds, changed_nanoseconds) = metadata_time(lookup.metadata.changed_ns);
        let (created_seconds, created_nanoseconds) = metadata_time(lookup.metadata.created_ns);
        Ok(NativeStat {
            inode: self.inode(node.file_id)?,
            logical_bytes: node.logical_bytes,
            blocks: node.logical_bytes.div_ceil(512),
            accessed_seconds,
            accessed_nanoseconds,
            modified_seconds,
            modified_nanoseconds,
            changed_seconds,
            changed_nanoseconds,
            created_seconds,
            created_nanoseconds,
            mode: file_kind | mode,
            link_count: u32::try_from(node.link_count).unwrap_or(u32::MAX),
            uid: metadata_or(lookup.metadata.posix_uid, self.mount_uid),
            gid: metadata_or(lookup.metadata.posix_gid, self.mount_gid),
            device: node
                .device
                .map(|(major, minor)| {
                    super::device::join_device(major, minor)
                        .map(|device| u64::from(u32::from_ne_bytes(device.to_ne_bytes())))
                })
                .transpose()?
                .unwrap_or(0),
            block_size: 4096,
            flags: u32::try_from(metadata_or(lookup.metadata.posix_flags, 0)).unwrap_or(u32::MAX),
            change: self.changes.label(
                node.file_id,
                stamp,
                self.source.binding_epoch(),
                Observed {
                    path: path.clone(),
                    lookup,
                },
                |stamp| self.unchanged_since(path, Some(node.file_id), stamp),
                |stamp| self.reported_unchanged_since(path, Some(node.file_id), stamp),
            ),
        })
    }

    /// Whether the node at `path` may carry named attributes. A node's
    /// metadata records its named attributes; without that record it has
    /// none, which every source reads the same way, so the probes the macOS
    /// client makes on each new file (provenance, Finder info) need no
    /// source call.
    fn may_have_named_attributes(&self, path: &MountPath) -> Result<bool, i32> {
        Ok(self.lookup(path)?.metadata.named_attributes != MetadataField::Unavailable)
    }

    fn admit_write(&self) -> Result<(), i32> {
        self.writable.then_some(()).ok_or(libc::EROFS)
    }

    fn mutate_metadata(
        &self,
        path: &MountPath,
        handle: u64,
        mutate: impl FnOnce(&mut FileMetadata) -> Result<(), i32>,
    ) -> Result<(), i32> {
        let current = self.lookup_handle(path, handle)?;
        let mut metadata = current.metadata;
        mutate(&mut metadata)?;
        // Metadata of any node kind: only a handle names a regular file.
        if handle == 0 {
            self.mutate(|| self.source.set_attributes(path, metadata, None))
        } else {
            self.mutate_file(path, handle, |file| file.set_attributes(metadata, None))
        }
    }

    /// Publishes this handle's mutations when the source publishes on close.
    fn flush_on_close(&self, handle: u64) -> Result<(), i32> {
        if handle == 0 {
            // Named-attribute opens carry no handle; their writes are
            // recorded mutations that the next sync covers.
            return Ok(());
        }
        let written = self
            .files
            .read()
            .map_err(|_| libc::EIO)?
            .get(&handle)
            .ok_or(libc::ESTALE)?
            .written
            .load(Ordering::Acquire);
        if written == 0 || !self.source.flush_on_handle_close() {
            return Ok(());
        }
        self.ledger.flush_through(written, self.source.as_ref())
    }

    /// Publishes every mutation this mount has acknowledged.
    fn sync(&self) -> Result<(), i32> {
        self.ledger
            .flush_through(self.ledger.latest(), self.source.as_ref())
    }
}

struct FileTarget {
    file: Arc<dyn MountOpenFile>,
    written: Option<Arc<AtomicU64>>,
}

#[derive(Clone, Copy)]
enum InitialHandleState {
    Clean,
    /// Created by this callback: unpublished until a sync covers it.
    Written,
}

/// What a mount loop has reported: that the kernel accepted its mount, and
/// that the loop has returned. Mount and teardown block on these reports
/// rather than polling for their effects, so each wakes the moment the loop
/// reports; a poll's sleep is stretched far beyond its interval when the
/// system coalesces a background service's timers.
#[derive(Default)]
struct LoopEvents {
    state: Mutex<LoopState>,
    changed: Condvar,
}

#[derive(Clone, Copy, Debug, Default)]
struct LoopState {
    mounted: bool,
    exited: bool,
}

impl LoopEvents {
    fn report(&self, update: impl FnOnce(&mut LoopState)) {
        update(&mut self.state.lock().unwrap_or_else(PoisonError::into_inner));
        self.changed.notify_all();
    }

    fn current(&self) -> LoopState {
        *self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Waits up to `timeout` for a state `done` accepts, and returns the
    /// state reported by then.
    fn wait_until(&self, timeout: Duration, done: impl Fn(LoopState) -> bool) -> LoopState {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let (state, _) = self
            .changed
            .wait_timeout_while(state, timeout, |state| !done(*state))
            .unwrap_or_else(PoisonError::into_inner);
        *state
    }
}

/// Held by the mount loop thread; reports its return when dropped, so a
/// waiter also wakes if the thread unwinds.
struct ExitReport(Arc<LoopEvents>);

impl Drop for ExitReport {
    fn drop(&mut self) {
        self.0.report(|state| state.exited = true);
    }
}

/// Called by the bridge once the kernel has accepted the mount.
unsafe extern "C" fn report_mounted(events: *mut c_void) {
    // SAFETY: the loop thread's `ExitReport` keeps these events alive for
    // the whole native loop, which is the only caller.
    let events = unsafe { &*events.cast_const().cast::<LoopEvents>() };
    events.report(|state| state.mounted = true);
}

/// One process-owned high-level Darwin mount session.
pub(super) struct DarwinMountSession {
    resources: Option<Arc<DriverSessionResources>>,
    destination: PathBuf,
    thread: Option<JoinHandle<c_int>>,
    events: Arc<LoopEvents>,
    loop_finished_before_teardown: Option<bool>,
    loop_result: Option<MountLoopResult>,
    teardown_complete: bool,
}

#[derive(Debug)]
enum MountLoopResult {
    Exited(c_int),
    Panicked,
    TimedOut,
}

/// Both the session and mount loop own these pointers. A timed-out loop may
/// outlive the session, but neither pointer is freed until that loop exits.
struct DriverSessionResources {
    source_context: usize,
    driver_session: usize,
}

impl DriverSessionResources {
    fn context(&self) -> &DarwinMountContext {
        // SAFETY: the context outlives every owner of these resources.
        unsafe { &*(self.source_context as *const DarwinMountContext) }
    }
}

impl Drop for DriverSessionResources {
    fn drop(&mut self) {
        // SAFETY: the last owner has released these exact allocations, and
        // the mount loop retains an owner through every native callback.
        unsafe {
            acyclic_fs_darwin_mount_session_free(self.driver_session as *mut c_void);
            drop(Arc::from_raw(
                self.source_context as *const DarwinMountContext,
            ));
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UnmountEvidence {
    AlreadyUnmounted,
    Detached,
}

impl DarwinMountSession {
    pub(super) fn start(
        request: &NativeMountRequest,
        source: Arc<dyn MountFilesystem>,
    ) -> Result<Self, DriverStartFailure> {
        static STARTUP: OnceLock<Mutex<()>> = OnceLock::new();
        let _startup = STARTUP.get_or_init(|| Mutex::new(())).lock().map_err(|_| {
            NativeMountError::Driver("Darwin mount startup lock is poisoned".to_owned())
        })?;
        let root = source
            .lookup(&MountPath::root())
            .map_err(|error| source_error(&error))?
            .ok_or_else(|| NativeMountError::Driver("volume root is absent".to_owned()))?;
        if root.node.kind != MountNodeKind::Directory {
            return Err(
                NativeMountError::Driver("volume root is not a directory".to_owned()).into(),
            );
        }
        let destination_metadata = request
            .destination
            .metadata()
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        let parent_metadata = request
            .destination
            .parent()
            .and_then(|parent| parent.metadata().ok())
            .ok_or_else(|| NativeMountError::Driver("mount parent is unavailable".to_owned()))?;
        let supports_named_attributes = source.supports_posix_named_attributes();
        let context = DarwinMountContext::new(
            source,
            request.writable,
            &destination_metadata,
            root.node.file_id,
        )
        .observing_source();

        let destination = request.destination.clone();
        let destination_c = path_cstring(&destination).map_err(driver_errno)?;
        let options = mount_options(request.writable, supports_named_attributes);
        let arguments = fuse_arguments(&options);
        let argument_count = c_int::try_from(arguments.len())
            .map_err(|_| NativeMountError::Driver("too many Darwin mount arguments".to_owned()))?;
        // Transfer the Rust callback context only after every fallible argument
        // conversion has completed, then bind one C interrupt handle to this
        // exact loop. The bridge registry keeps native control state scoped to
        // this session, so independent mounts do not share an interrupt target.
        let source_context = Arc::into_raw(context) as usize;
        let driver_session = unsafe { acyclic_fs_darwin_mount_session_new() } as usize;
        if driver_session == 0 {
            unsafe { drop(Arc::from_raw(source_context as *const DarwinMountContext)) };
            return Err(NativeMountError::Driver(
                "Darwin mount session allocation failed".to_owned(),
            )
            .into());
        }
        let resources = Arc::new(DriverSessionResources {
            source_context,
            driver_session,
        });
        let loop_resources = Arc::clone(&resources);
        let events = Arc::new(LoopEvents::default());
        let exit_report = ExitReport(Arc::clone(&events));
        let thread = std::thread::Builder::new()
            .name("acyclic-fs-darwin-nfs".to_owned())
            .spawn(move || {
                let exit_report = exit_report;
                let pointers = arguments
                    .iter()
                    .map(|argument| argument.as_ptr())
                    .collect::<Vec<_>>();
                unsafe {
                    acyclic_fs_darwin_mount_run(
                        loop_resources.driver_session as *mut c_void,
                        argument_count,
                        pointers.as_ptr(),
                        destination_c.as_ptr(),
                        loop_resources.source_context,
                        report_mounted,
                        Arc::as_ptr(&exit_report.0).cast_mut().cast(),
                    )
                }
            })
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        Self {
            resources: Some(resources),
            destination,
            thread: Some(thread),
            events,
            loop_finished_before_teardown: None,
            loop_result: None,
            teardown_complete: false,
        }
        .await_mount(&parent_metadata)
    }

    fn await_mount(mut self, parent_metadata: &Metadata) -> Result<Self, DriverStartFailure> {
        let reported = self.events.wait_until(MOUNT_VISIBILITY_TIMEOUT, |state| {
            state.mounted || state.exited
        });
        if !reported.mounted {
            if !reported.exited {
                return Err(self.failed_start(NativeMountError::Driver(format!(
                    "Darwin mount did not become visible within {} seconds",
                    MOUNT_VISIBILITY_TIMEOUT.as_secs()
                ))));
            }
            let status = self.finish_thread();
            self.loop_finished_before_teardown = Some(true);
            let description = format!("{status:?}");
            self.loop_result = Some(status);
            return Err(self.failed_start(NativeMountError::Driver(format!(
                "Darwin mount exited before the mount became visible: {description}"
            ))));
        }
        // The loop reports only a mount the kernel accepted; the mount table
        // confirms that it is this destination's.
        match is_mounted(&self.destination, parent_metadata) {
            Ok(true) => {}
            Ok(false) => {
                return Err(self.failed_start(NativeMountError::Driver(
                    "Darwin mount reported a mount the mount table does not show".to_owned(),
                )));
            }
            Err(error) => return Err(self.failed_start(NativeMountError::Driver(error))),
        }
        // A successful NFS mount does not prove the caller can use it: macOS
        // privacy policy can allow metadata lookups while denying every open
        // on the mounted network volume. Opening the root reads no entries or
        // content, and fails before exposing an unusable child workspace.
        if let Err(error) = std::fs::File::open(&self.destination) {
            let message = if error.raw_os_error() == Some(libc::EPERM) {
                format!(
                    "macOS denied opening the mounted network volume: {error}; for SSH sessions, enable Remote Login → Allow full disk access for remote users"
                )
            } else {
                format!("Darwin mount root could not be opened: {error}")
            };
            return Err(self.failed_start(NativeMountError::Driver(message)));
        }
        Ok(self)
    }

    fn failed_start(mut self, error: NativeMountError) -> DriverStartFailure {
        match self.stop() {
            Ok(()) => error.into(),
            Err(cleanup) => {
                // The caller will preserve the destination fence. Do not let
                // Drop retry teardown without owning that fence: its result
                // could otherwise contradict the caller's ownership decision.
                std::mem::forget(self);
                DriverStartFailure::preserving_destination_fence(NativeMountError::Driver(format!(
                    "{error}; teardown failed: {cleanup}"
                )))
            }
        }
    }

    /// Marks one mount-relative path (leading `/` optional) changed by a
    /// projection change the source did not record, such as a removed
    /// route. NFS offers no server-initiated invalidation without
    /// delegations: this forgets everything the mount cached about the view
    /// (the change may reach beyond `path`), so the client drops its cached
    /// names, attributes, and data at its next revalidation, which
    /// [`Self::revalidate`] waits for and every open makes immediately.
    pub(super) fn invalidate(&self, path: &[u8]) -> Result<(), NativeMountError> {
        let resources = self.resources()?;
        let mut bytes = Vec::with_capacity(path.len() + 1);
        if path.first() != Some(&b'/') {
            bytes.push(b'/');
        }
        bytes.extend_from_slice(path);
        let path = CString::new(bytes)
            .map_err(|_| NativeMountError::Driver("path contains NUL".to_owned()))?;
        // Reject continuation snapshots before the external invalidation can
        // overlap a directory read, and make the client's next revalidation
        // discard what it cached.
        resources.context().forget_view();
        // SAFETY: `driver_session` is the live bridge session this struct
        // owns until teardown, and `path` is a NUL-terminated string.
        let status = unsafe {
            acyclic_fs_darwin_mount_invalidate(
                resources.driver_session as *mut c_void,
                path.as_ptr(),
            )
        };
        if status == 0 {
            Ok(())
        } else {
            Err(NativeMountError::Driver(format!(
                "invalidate returned {status}"
            )))
        }
    }

    /// See [`DarwinMountContext::revalidate`].
    pub(super) fn revalidate(&self) -> Result<(), NativeMountError> {
        let context = self.resources()?.context();
        // Every change made to the source before this call is recorded, and
        // so counted as made around the mount, before the wait begins.
        context
            .source
            .fence_changes()
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        context.revalidate();
        Ok(())
    }

    fn resources(&self) -> Result<&DriverSessionResources, NativeMountError> {
        self.resources
            .as_deref()
            .ok_or_else(|| NativeMountError::Driver("Darwin mount session has stopped".to_owned()))
    }

    #[allow(clippy::unnecessary_wraps)]
    pub(super) fn stop(&mut self) -> Result<(), NativeMountError> {
        if self.teardown_complete {
            return Ok(());
        }
        // Observe whether the provider loop failed independently before any
        // teardown action can make it exit. A detach closes the transport,
        // which ends the loop, so sampling after it races a successful detach
        // and misclassifies it as a pre-existing failure.
        if self.loop_result.is_none() {
            self.loop_finished_before_teardown =
                Some(self.thread.is_none() || self.events.current().exited);
        }
        let unmount = bounded_unmount(&self.destination);
        if self
            .loop_result
            .as_ref()
            .is_none_or(|result| matches!(result, MountLoopResult::TimedOut))
        {
            if let Some(resources) = &self.resources {
                unsafe {
                    acyclic_fs_darwin_mount_interrupt(resources.driver_session as *mut c_void);
                }
            }
            let join = self.finish_thread();
            self.loop_result = Some(join);
        }
        let unmount = unmount.map_err(NativeMountError::Driver)?;
        let loop_finished_before_teardown =
            self.loop_finished_before_teardown.ok_or_else(|| {
                NativeMountError::Driver("Darwin mount teardown state is absent".to_owned())
            })?;
        let join = self.loop_result.as_ref().ok_or_else(|| {
            NativeMountError::Driver("Darwin mount loop result is absent".to_owned())
        })?;
        let result = classify_detached_loop(join, unmount, loop_finished_before_teardown);
        if result.is_ok() {
            self.teardown_complete = true;
        }
        result
    }

    fn finish_thread(&mut self) -> MountLoopResult {
        self.finish_thread_within(MOUNT_LOOP_EXIT_TIMEOUT)
    }

    fn finish_thread_within(&mut self, timeout: Duration) -> MountLoopResult {
        if self.thread.is_none() {
            return MountLoopResult::Exited(0);
        }
        if !self.events.wait_until(timeout, |state| state.exited).exited {
            // Keep the join handle, callback resources, and destination
            // fence so a later stop can retry once callbacks have exited.
            return MountLoopResult::TimedOut;
        }
        // The loop has returned: joining waits only for its thread to end.
        let Some(thread) = self.thread.take() else {
            return MountLoopResult::Panicked;
        };
        let status = match thread.join() {
            Ok(status) => MountLoopResult::Exited(status),
            Err(_) => MountLoopResult::Panicked,
        };
        self.resources.take();
        status
    }
}

fn classify_detached_loop(
    join: &MountLoopResult,
    unmount: UnmountEvidence,
    loop_finished_before_teardown: bool,
) -> Result<(), NativeMountError> {
    match (join, unmount, loop_finished_before_teardown) {
        (MountLoopResult::Exited(0), _, false)
        | (MountLoopResult::Exited(0), UnmountEvidence::AlreadyUnmounted, true) => Ok(()),
        // An external forced detach is a mount-loss event, not a provider
        // failure.  The destination evidence distinguishes it from a
        // provider loop that died while its stale mount was still present.
        (MountLoopResult::Exited(0), _, true) => Err(NativeMountError::Driver(
            "Darwin mount loop exited before teardown began".to_owned(),
        )),
        // A detached namespace does not prove that in-flight callbacks have
        // finished. Publishing this destination now could race a late write.
        (MountLoopResult::TimedOut, _, _) => Err(NativeMountError::Driver(
            "Darwin mount callbacks have not finished; destination remains fenced".to_owned(),
        )),
        (MountLoopResult::Exited(status), _, _) => Err(NativeMountError::Driver(format!(
            "Darwin mount loop exited with status {status}"
        ))),
        (MountLoopResult::Panicked, _, _) => Err(NativeMountError::Driver(
            "Darwin mount loop thread panicked".to_owned(),
        )),
    }
}

impl Drop for DarwinMountSession {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

/// Only options the loopback NFS mount honors: `DarwinFUSE` rejects any
/// other rather than dropping it.
fn mount_options(writable: bool, supports_named_attributes: bool) -> Vec<&'static CStr> {
    let mut options = vec![c"nobrowse"];
    if !writable {
        options.push(c"ro");
    }
    if supports_named_attributes {
        options.push(c"namedattr");
    }
    options
}

fn fuse_arguments(options: &[&CStr]) -> Vec<CString> {
    let mut arguments = Vec::with_capacity(options.len().saturating_mul(2).saturating_add(1));
    arguments.push(c"acyclic-fs".to_owned());
    for option in options {
        arguments.push(c"-o".to_owned());
        arguments.push((*option).to_owned());
    }
    arguments
}

fn is_mounted(destination: &Path, _parent: &Metadata) -> Result<bool, String> {
    use std::ffi::CStr;
    use std::os::unix::ffi::OsStrExt as _;

    let mut mounts = std::ptr::null_mut::<libc::statfs>();
    // SAFETY: `getmntinfo` initializes `mounts` to an OS-owned array retained
    // until the next call; this function consumes it before returning.
    let count = unsafe { libc::getmntinfo(&raw mut mounts, libc::MNT_NOWAIT) };
    if count <= 0 || mounts.is_null() {
        return Err(format!(
            "getmntinfo failed while querying {}: {}",
            destination.display(),
            std::io::Error::last_os_error()
        ));
    }
    let count = usize::try_from(count)
        .map_err(|_| "getmntinfo returned an invalid mount count".to_owned())?;
    // SAFETY: a positive return value is the exact initialized array length.
    let mounts = unsafe { std::slice::from_raw_parts(mounts, count) };
    let normalized = destination
        .parent()
        .and_then(|parent| parent.canonicalize().ok())
        .and_then(|parent| destination.file_name().map(|name| parent.join(name)));
    Ok(mounts.iter().any(|mount| {
        // SAFETY: Darwin guarantees that `f_mntonname` is NUL terminated.
        let mounted_at = unsafe { CStr::from_ptr(mount.f_mntonname.as_ptr()) };
        mounted_at.to_bytes() == destination.as_os_str().as_bytes()
            || normalized
                .as_deref()
                .is_some_and(|path| mounted_at.to_bytes() == path.as_os_str().as_bytes())
    }))
}

/// Detaches `destination` directly, falling back to `diskutil unmount
/// force` only when the direct detach leaves it mounted.
fn bounded_unmount(destination: &Path) -> Result<UnmountEvidence, String> {
    let parent = destination
        .parent()
        .and_then(|path| path.metadata().ok())
        .ok_or_else(|| "mount parent disappeared during teardown".to_owned())?;
    if !is_mounted(destination, &parent)? {
        return Ok(UnmountEvidence::AlreadyUnmounted);
    }
    if bounded_direct_unmount(destination, &parent)? {
        return Ok(UnmountEvidence::Detached);
    }
    let mut child = Command::new("/usr/sbin/diskutil")
        .arg("unmount")
        .arg("force")
        .arg(destination)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| error.to_string())?;
    let deadline = Instant::now() + DISKUTIL_UNMOUNT_TIMEOUT;
    let status = loop {
        if let Some(status) = child.try_wait().map_err(|error| error.to_string())? {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            if !is_mounted(destination, &parent)? {
                return Ok(UnmountEvidence::Detached);
            }
            return Err(format!(
                "diskutil unmount exceeded its {}-second bound",
                DISKUTIL_UNMOUNT_TIMEOUT.as_secs()
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    if !is_mounted(destination, &parent)? {
        return Ok(UnmountEvidence::Detached);
    }
    if !status.success() {
        return Err(format!("diskutil unmount failed with status {status}"));
    }
    let verify_deadline = Instant::now() + DISKUTIL_VISIBILITY_TIMEOUT;
    while is_mounted(destination, &parent)? {
        if Instant::now() >= verify_deadline {
            return Err("Darwin mount remained visible after diskutil unmount".to_owned());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Ok(UnmountEvidence::Detached)
}

pub(super) fn recover_destination(destination: &Path) -> Result<(), NativeMountError> {
    bounded_unmount(destination)
        .map(|_| ())
        .map_err(NativeMountError::Driver)
}

/// Force-detaches with `unmount(2)`, as `umount -f` would without its
/// process launch, and reports whether the destination is detached. The
/// kernel detaches within the call, so its return is the answer; a helper
/// thread bounds a call that a wedged transport blocks, after which the
/// caller's fallback takes over.
fn bounded_direct_unmount(destination: &Path, parent: &Metadata) -> Result<bool, String> {
    let path = path_cstring(destination)
        .map_err(|error| std::io::Error::from_raw_os_error(error).to_string())?;
    let (detached, outcome) = std::sync::mpsc::sync_channel(1);
    std::thread::Builder::new()
        .name("acyclic-fs-darwin-unmount".to_owned())
        .spawn(move || {
            // SAFETY: `path` is a NUL-terminated path this thread owns.
            let status = unsafe { libc::unmount(path.as_ptr(), libc::MNT_FORCE) };
            let _ = detached.send(status);
        })
        .map_err(|error| error.to_string())?;
    // The mount table decides either way: a failed or still-blocked call
    // leaves the destination mounted for the fallback.
    let _ = outcome.recv_timeout(DIRECT_UNMOUNT_TIMEOUT);
    is_mounted(destination, parent).map(|mounted| !mounted)
}

fn context(address: usize) -> Result<&'static DarwinMountContext, i32> {
    if address == 0 {
        return Err(libc::ESTALE);
    }
    Ok(unsafe { &*(address as *const DarwinMountContext) })
}

/// Runs one native callback on the context at `address`. Every change it
/// makes is this mount's own, which the client learns of from the reply,
/// and it completes before any barrier that began after it (see
/// [`DarwinMountContext::revalidate`]). A panic fails it with `EIO`.
fn callback<T>(
    address: usize,
    operation: impl FnOnce(&'static DarwinMountContext) -> Result<T, i32>,
) -> Result<T, i32> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let context = context(address)?;
        let _callback = context.callbacks.enter();
        let _origin = context.origin.enter();
        operation(context)
    }))
    .unwrap_or(Err(libc::EIO))
}

fn ffi_status(
    address: usize,
    operation: impl FnOnce(&'static DarwinMountContext) -> Result<c_int, i32>,
) -> c_int {
    callback(address, operation).unwrap_or_else(|error| -error)
}

fn ffi_offset(
    address: usize,
    operation: impl FnOnce(&'static DarwinMountContext) -> Result<i64, i32>,
) -> i64 {
    callback(address, operation).unwrap_or_else(|error| -i64::from(error))
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_getattr(
    address: usize,
    path: *const c_char,
    handle: u64,
    result: *mut NativeStat,
) -> c_int {
    ffi_status(address, |context| {
        let attributes = context.attributes(&mount_path(path)?, handle)?;
        unsafe { result.write(attributes) };
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_open(
    address: usize,
    path: *const c_char,
    flags: c_int,
    handle: *mut u64,
) -> c_int {
    ffi_status(address, |context| {
        if flags & libc::O_ACCMODE != libc::O_RDONLY {
            context.admit_write()?;
        }
        unsafe { handle.write(context.open(&mount_path(path)?, InitialHandleState::Clean)?) };
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_create(
    address: usize,
    path: *const c_char,
    mode: u32,
    uid: u32,
    gid: u32,
    _flags: c_int,
    handle: *mut u64,
) -> c_int {
    ffi_status(address, |context| {
        let path = mount_path(path)?;
        let lookup = context.mutate(|| {
            context
                .source
                .create_file(&path, create_metadata(mode, mode::IFREG, uid, gid))
        })?;
        context.remember_current_lookup(&path, Some(lookup))?;
        context.namespace_changed();
        unsafe { handle.write(context.open(&path, InitialHandleState::Written)?) };
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_release(
    address: usize,
    _path: *const c_char,
    handle: u64,
) -> c_int {
    ffi_status(address, |context| {
        if handle == 0 {
            return Ok(0);
        }
        context
            .files
            .write()
            .map_err(|_| libc::EIO)?
            .remove(&handle)
            .ok_or(libc::ESTALE)?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_read(
    address: usize,
    path: *const c_char,
    handle: u64,
    buffer: *mut c_char,
    length: usize,
    offset: i64,
) -> c_int {
    ffi_status(address, |context| {
        let requested_length = bounded_length(length)?;
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let bytes = context
            .file_or_open(&mount_path(path)?, handle)?
            .read_up_to(offset, requested_length)
            .map_err(|error| errno(&error))?;
        if bytes.len() > usize::try_from(requested_length).unwrap_or(usize::MAX) {
            return Err(libc::EIO);
        }
        unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), buffer.cast(), bytes.len()) };
        c_int::try_from(bytes.len()).map_err(|_| libc::EOVERFLOW)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_write(
    address: usize,
    path: *const c_char,
    handle: u64,
    buffer: *const c_char,
    length: usize,
    offset: i64,
) -> c_int {
    ffi_status(address, |context| {
        let length = bounded_length(length)?;
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let bytes = unsafe { std::slice::from_raw_parts(buffer.cast::<u8>(), length as usize) };
        context.mutate_file(&mount_path(path)?, handle, |file| {
            file.write_range(offset, Bytes::copy_from_slice(bytes))
        })?;
        c_int::try_from(length).map_err(|_| libc::EOVERFLOW)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_truncate(
    address: usize,
    path: *const c_char,
    handle: u64,
    length: i64,
) -> c_int {
    ffi_status(address, |context| {
        let length = u64::try_from(length).map_err(|_| libc::EINVAL)?;
        context.mutate_file(&mount_path(path)?, handle, |file| file.resize(length))?;
        Ok(0)
    })
}

/// NFS CLOSE: publishes only what this handle wrote, and only for sources
/// whose close is a publication boundary.
#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_flush(address: usize, handle: u64) -> c_int {
    ffi_status(address, |context| {
        context.flush_on_close(handle)?;
        Ok(0)
    })
}

/// NFS COMMIT and stable WRITE. The client sends these for `fsync` and
/// for its close-to-open flush alike, so each keeps the `fsync` guarantee:
/// every acknowledged mutation is published once this returns.
#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_fsync(address: usize) -> c_int {
    ffi_status(address, |context| {
        context.sync()?;
        Ok(0)
    })
}

/// Whether every acknowledged write is already as durable as `fsync` would
/// make it, because the source's flush publishes nothing: WRITE replies are
/// then stable and the client never needs to COMMIT.
#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_durable_writes(address: usize) -> c_int {
    ffi_status(address, |context| {
        Ok(c_int::from(!context.source.flush_publishes()))
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_opendir(
    address: usize,
    path: *const c_char,
    handle: *mut u64,
) -> c_int {
    ffi_status(address, |context| {
        let path = mount_path(path)?;
        let binding_epoch = context.source.binding_epoch();
        let epochs = context.cache_epochs();

        let _binding_lease = context
            .source
            .acquire_binding_lease(binding_epoch)
            .map_err(|error| errno(&error))?;
        if context.lookup(&path)?.node.kind != MountNodeKind::Directory {
            return Err(libc::ENOTDIR);
        }
        let directory = DirectoryHandle::new(path, binding_epoch, epochs);
        if directory.can_reuse_pages() && !directory.is_current(context) {
            return Err(libc::ESTALE);
        }
        let allocated = context.allocate_handle()?;
        let mut directories = context.directories.lock().map_err(|_| libc::EIO)?;
        directories.try_reserve(1).map_err(|_| libc::ENOMEM)?;
        directories.insert(allocated, Arc::new(Mutex::new(directory)));
        unsafe { handle.write(allocated) };
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_readdir(
    address: usize,
    _path: *const c_char,
    buffer: *mut c_void,
    filler: DirectoryFiller,
    offset: i64,
    handle: u64,
) -> c_int {
    ffi_status(address, |context| {
        let directory = context
            .directories
            .lock()
            .map_err(|_| libc::EIO)?
            .get(&handle)
            .cloned()
            .ok_or(libc::ESTALE)?;
        let mut directory = directory.lock().map_err(|_| libc::EIO)?;
        let _binding_lease = context
            .source
            .acquire_binding_lease(directory.binding_epoch)
            .map_err(|error| errno(&error))?;
        if !directory.is_current(context) {
            return Err(libc::ESTALE);
        }
        if offset < 0 {
            return Err(libc::EINVAL);
        }
        if !directory.can_reuse_pages() {
            directory.rewind();
            context
                .directory_checkpoints
                .lock()
                .map_err(|_| libc::EIO)?
                .remove(&directory.path);
        } else if offset > 0 && directory.emitted == 0 {
            let checkpoints = context
                .directory_checkpoints
                .lock()
                .map_err(|_| libc::EIO)?;
            if let Some(checkpoint) = checkpoints.get(&directory.path)
                && checkpoint.revision == context.namespace_revision.load(Ordering::Acquire)
                && checkpoint.handle.emitted == offset
                && checkpoint.handle.binding_epoch == directory.binding_epoch
                && checkpoint.handle.is_current(context)
            {
                *directory = checkpoint.handle.clone();
            }
        }
        if offset < directory.emitted {
            directory.rewind();
        }
        advance_directory_to_offset(context, &mut directory, offset)?;
        while directory.emitted < 2 {
            let name = if directory.emitted == 0 { c"." } else { c".." };
            let next = directory.emitted + 1;
            let buffer_full = unsafe {
                acyclic_fs_darwin_mount_fill_directory(
                    buffer,
                    filler,
                    name.as_ptr(),
                    ptr::null(),
                    next,
                )
            };
            if buffer_full != 0 {
                finish_directory_page(context, &directory, true)?;
                return Ok(0);
            }
            directory.emitted = next;
        }
        loop {
            ensure_directory_page(context, &mut directory)?;
            let Some(entry) = directory.entries.front() else {
                finish_directory_page(context, &directory, false)?;
                context
                    .directory_checkpoints
                    .lock()
                    .map_err(|_| libc::EIO)?
                    .remove(&directory.path);
                return Ok(0);
            };
            let (name, attributes) = directory_entry(context, &directory, entry)?;
            let next = directory.emitted.checked_add(1).ok_or(libc::EOVERFLOW)?;
            let buffer_full = unsafe {
                acyclic_fs_darwin_mount_fill_directory(
                    buffer,
                    filler,
                    name.as_ptr(),
                    &raw const attributes,
                    next,
                )
            };
            if buffer_full != 0 {
                finish_directory_page(context, &directory, true)?;
                return Ok(0);
            }
            directory.entries.pop_front();
            directory.emitted = next;
        }
    })
}

/// One listed entry's native name and attributes. The page was read after
/// the directory's stamp was sampled, which therefore labels the entry.
fn directory_entry(
    context: &DarwinMountContext,
    directory: &DirectoryHandle,
    entry: &MountDirectoryEntry,
) -> Result<(CString, NativeStat), i32> {
    let name = CString::new(entry.name.as_slice()).map_err(|_| libc::EIO)?;
    let attributes = context.attributes_from_lookup(
        &directory.path.child(entry.name.clone()),
        MountLookup {
            node: entry.node,
            metadata: entry.metadata,
        },
        directory.epochs.map(|epochs| epochs.view),
    )?;
    Ok((name, attributes))
}

fn finish_directory_page(
    context: &DarwinMountContext,
    directory: &DirectoryHandle,
    checkpoint: bool,
) -> Result<(), i32> {
    // A mutation can happen after the last page read, while the native filler
    // copies entries into its buffer. Returning ESTALE discards that buffer
    // rather than exposing a listing assembled from different view epochs.
    if directory.can_reuse_pages() && !directory.is_current(context) {
        return Err(libc::ESTALE);
    }
    if checkpoint {
        checkpoint_directory(context, directory)?;
    }
    Ok(())
}

fn advance_directory_to_offset(
    context: &DarwinMountContext,
    directory: &mut DirectoryHandle,
    offset: i64,
) -> Result<(), i32> {
    while directory.emitted < offset {
        if directory.emitted < 2 {
            directory.emitted += 1;
            continue;
        }
        ensure_directory_page(context, directory)?;
        if directory.entries.pop_front().is_none() {
            return Err(libc::EINVAL);
        }
        directory.emitted += 1;
    }
    Ok(())
}

fn ensure_directory_page(
    context: &DarwinMountContext,
    directory: &mut DirectoryHandle,
) -> Result<(), i32> {
    if directory.can_reuse_pages() && !directory.is_current(context) {
        return Err(libc::ESTALE);
    }
    if directory.entries.is_empty() && !directory.exhausted {
        directory
            .revision
            .get_or_insert_with(|| context.namespace_revision.load(Ordering::Acquire));
        let page = context
            .source
            .read_directory(
                &directory.path,
                directory.cursor.as_deref(),
                DIRECTORY_PAGE_SIZE,
            )
            .map_err(|error| errno(&error))?;
        if directory.can_reuse_pages() && !directory.is_current(context) {
            return Err(libc::ESTALE);
        }
        if page.entries.is_empty() && page.next_cursor.is_some() {
            return Err(libc::EIO);
        }
        directory.cursor = page.next_cursor;
        directory.exhausted = directory.cursor.is_none();
        directory.entries = page.entries.into();
    }
    Ok(())
}

fn checkpoint_directory(
    context: &DarwinMountContext,
    directory: &DirectoryHandle,
) -> Result<(), i32> {
    const MAXIMUM_DIRECTORY_CHECKPOINTS: usize = 64;
    if !directory.can_reuse_pages() {
        return Ok(());
    }
    let mut checkpoints = context
        .directory_checkpoints
        .lock()
        .map_err(|_| libc::EIO)?;
    let current_revision = context.namespace_revision.load(Ordering::Acquire);
    let revision = directory.revision.unwrap_or(current_revision);
    if revision != current_revision {
        checkpoints.remove(&directory.path);
        return Ok(());
    }
    if checkpoints.len() >= MAXIMUM_DIRECTORY_CHECKPOINTS
        && !checkpoints.contains_key(&directory.path)
    {
        checkpoints.clear();
    }
    checkpoints.insert(
        directory.path.clone(),
        DirectoryCheckpoint {
            revision,
            handle: directory.clone(),
        },
    );
    Ok(())
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_releasedir(address: usize, handle: u64) -> c_int {
    ffi_status(address, |context| {
        context
            .directories
            .lock()
            .map_err(|_| libc::EIO)?
            .remove(&handle)
            .ok_or(libc::ESTALE)?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_mkdir(
    address: usize,
    path: *const c_char,
    mode: u32,
    uid: u32,
    gid: u32,
) -> c_int {
    ffi_status(address, |context| {
        let path = mount_path(path)?;
        context.mutate(|| {
            context
                .source
                .create_directory(&path, create_metadata(mode, mode::IFDIR, uid, gid))
        })?;
        context.namespace_changed();
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_remove(
    address: usize,
    path: *const c_char,
    directory: c_int,
) -> c_int {
    ffi_status(address, |context| {
        let path = mount_path(path)?;
        let lookup = context.lookup(&path)?;
        if (directory != 0) != (lookup.node.kind == MountNodeKind::Directory) {
            return Err(if directory == 0 {
                libc::EISDIR
            } else {
                libc::ENOTDIR
            });
        }
        context.unlink(&path, lookup, || {
            context.source.remove(&path, Some(lookup.node.file_id))
        })?;
        context.namespace_changed();
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_rename(
    address: usize,
    source: *const c_char,
    destination: *const c_char,
    flags: u32,
) -> c_int {
    ffi_status(address, |context| {
        if flags & !RENAME_NOREPLACE != 0 {
            return Err(libc::EOPNOTSUPP);
        }
        let (source, destination) = (mount_path(source)?, mount_path(destination)?);
        let replace = flags & RENAME_NOREPLACE == 0;
        let rename = || context.source.rename(&source, &destination, replace);
        // Replacing the destination unlinks it.
        match replace.then(|| context.lookup(&destination)) {
            Some(Ok(replaced)) => context.unlink(&destination, replaced, rename)?,
            None | Some(Err(libc::ENOENT)) => context.mutate(rename)?,
            Some(Err(error)) => return Err(error),
        }
        context.namespace_changed();

        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_link(
    address: usize,
    source: *const c_char,
    destination: *const c_char,
) -> c_int {
    ffi_status(address, |context| {
        let (source, destination) = (mount_path(source)?, mount_path(destination)?);
        context.mutate(|| context.source.hard_link(&source, &destination))?;
        context.namespace_changed();
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_symlink(
    address: usize,
    target: *const c_char,
    destination: *const c_char,
    uid: u32,
    gid: u32,
) -> c_int {
    ffi_status(address, |context| {
        let target = unsafe { CStr::from_ptr(target) }.to_bytes();
        let destination = mount_path(destination)?;
        context.mutate(|| {
            context.source.create_symbolic_link(
                &destination,
                Bytes::copy_from_slice(target),
                create_metadata(0o777, mode::IFLNK, uid, gid),
            )
        })?;
        context.namespace_changed();
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_readlink(
    address: usize,
    path: *const c_char,
    buffer: *mut c_char,
    length: usize,
) -> c_int {
    ffi_status(address, |context| {
        if length == 0 {
            return Err(libc::ERANGE);
        }
        let target = context
            .source
            .read_link(&mount_path(path)?)
            .map_err(|error| errno(&error))?;
        if target.len() >= length {
            return Err(libc::ENAMETOOLONG);
        }
        unsafe {
            ptr::copy_nonoverlapping(target.as_ptr(), buffer.cast(), target.len());
            buffer.add(target.len()).write(0);
        }
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_mknod(
    address: usize,
    path: *const c_char,
    mode: u32,
    device: u64,
    uid: u32,
    gid: u32,
) -> c_int {
    ffi_status(address, |context| {
        let kind = match mode & mode::IFMT {
            mode::IFIFO => MountNodeKind::Fifo,
            mode::IFSOCK => MountNodeKind::Socket,
            mode::IFCHR => MountNodeKind::CharacterDevice,
            mode::IFBLK => MountNodeKind::BlockDevice,
            _ => return Err(libc::EOPNOTSUPP),
        };
        let device = matches!(
            kind,
            MountNodeKind::CharacterDevice | MountNodeKind::BlockDevice
        )
        .then(|| super::device::split_device(device));
        let path = mount_path(path)?;
        context.mutate(|| {
            context.source.create_special(
                &path,
                kind,
                device,
                create_metadata(mode, mode & mode::IFMT, uid, gid),
            )
        })?;
        context.namespace_changed();
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_chmod(
    address: usize,
    path: *const c_char,
    mode: u32,
    handle: u64,
) -> c_int {
    ffi_status(address, |context| {
        context.mutate_metadata(&mount_path(path)?, handle, |metadata| {
            let kind = metadata_or(metadata.posix_mode, 0) & mode::IFMT;
            metadata.posix_mode = MetadataField::Value(kind | (mode & 0o7777));
            Ok(())
        })?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_chown(
    address: usize,
    path: *const c_char,
    uid: u32,
    gid: u32,
    handle: u64,
) -> c_int {
    ffi_status(address, |context| {
        context.mutate_metadata(&mount_path(path)?, handle, |metadata| {
            if uid != u32::MAX {
                metadata.posix_uid = MetadataField::Value(uid);
            }
            if gid != u32::MAX {
                metadata.posix_gid = MetadataField::Value(gid);
            }
            Ok(())
        })?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_utimens(
    address: usize,
    path: *const c_char,
    times: *const NativeTimes,
    handle: u64,
) -> c_int {
    ffi_status(address, |context| {
        let times = unsafe { times.as_ref() }.ok_or(libc::EINVAL)?;
        context.mutate_metadata(&mount_path(path)?, handle, |metadata| {
            update_time(
                &mut metadata.accessed_ns,
                times.accessed_seconds,
                times.accessed_nanoseconds,
            )?;
            update_time(
                &mut metadata.modified_ns,
                times.modified_seconds,
                times.modified_nanoseconds,
            )?;
            Ok(())
        })?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_getxattr(
    address: usize,
    path: *const c_char,
    name: *const c_char,
    value: *mut c_char,
    length: usize,
) -> c_int {
    ffi_status(address, |context| {
        let path = mount_path(path)?;
        if !context.may_have_named_attributes(&path)? {
            return Err(libc::ENOATTR);
        }
        let bytes = context
            .source
            .read_attribute(&path, c_bytes(name)?)
            .map_err(|error| errno(&error))?
            .ok_or(libc::ENOATTR)?;
        copy_variable_result(&bytes, value, length)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_setxattr(
    address: usize,
    path: *const c_char,
    name: *const c_char,
    value: *const c_char,
    length: usize,
    flags: c_int,
) -> c_int {
    ffi_status(address, |context| {
        if length > MAXIMUM_CALLBACK_BYTES {
            return Err(libc::E2BIG);
        }
        let mode = match flags {
            0 => MountAttributeWriteMode::Upsert,
            libc::XATTR_CREATE => MountAttributeWriteMode::Create,
            libc::XATTR_REPLACE => MountAttributeWriteMode::Replace,
            _ => return Err(libc::EINVAL),
        };
        let bytes = unsafe { std::slice::from_raw_parts(value.cast::<u8>(), length) };
        let (path, name) = (mount_path(path)?, c_bytes(name)?);
        context.mutate(|| {
            context
                .source
                .write_attribute(&path, name, Bytes::copy_from_slice(bytes), mode)
        })?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_listxattr(
    address: usize,
    path: *const c_char,
    list: *mut c_char,
    length: usize,
) -> c_int {
    ffi_status(address, |context| {
        let path = mount_path(path)?;
        if !context.may_have_named_attributes(&path)? {
            return copy_variable_result(&[], list, length);
        }
        let mut cursor = None;
        let mut encoded = Vec::new();
        loop {
            let page = context
                .source
                .list_attributes(&path, cursor.as_deref(), ATTRIBUTE_PAGE_SIZE)
                .map_err(|error| errno(&error))?;
            for name in page.names {
                if name.contains(&0) {
                    return Err(libc::EIO);
                }
                let required = encoded
                    .len()
                    .checked_add(name.len())
                    .and_then(|value| value.checked_add(1))
                    .ok_or(libc::EOVERFLOW)?;
                if required > MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES {
                    return Err(libc::E2BIG);
                }
                encoded.extend_from_slice(&name);
                encoded.push(0);
            }
            match page.next_cursor {
                Some(next) if cursor.as_ref() != Some(&next) => cursor = Some(next),
                Some(_) => return Err(libc::EIO),
                None => break,
            }
        }
        copy_variable_result(&encoded, list, length)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_removexattr(
    address: usize,
    path: *const c_char,
    name: *const c_char,
) -> c_int {
    ffi_status(address, |context| {
        let (path, name) = (mount_path(path)?, c_bytes(name)?);
        context.mutate(|| context.source.remove_attribute(&path, name))?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_lseek(
    address: usize,
    path: *const c_char,
    handle: u64,
    offset: i64,
    whence: c_int,
) -> i64 {
    ffi_offset(address, |context| {
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let target = match whence {
            libc::SEEK_DATA => MountSeekTarget::Data,
            libc::SEEK_HOLE => MountSeekTarget::Hole,
            _ => return Err(libc::EINVAL),
        };
        let result = context
            .file_or_open(&mount_path(path)?, handle)?
            .seek(offset, target)
            .map_err(|error| errno(&error))?
            .ok_or(libc::ENXIO)?;
        i64::try_from(result).map_err(|_| libc::EOVERFLOW)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_fallocate(
    address: usize,
    path: *const c_char,
    handle: u64,
    mode: c_int,
    offset: i64,
    length: i64,
) -> c_int {
    ffi_status(address, |context| {
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let length = u64::try_from(length).map_err(|_| libc::EINVAL)?;
        let (operation, permitted_flags) = if mode & FALLOC_FL_PUNCH_HOLE != 0 {
            (
                MountRangeAllocation::PunchHole,
                FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
            )
        } else if mode & FALLOC_FL_ZERO_RANGE != 0 {
            (
                MountRangeAllocation::ZeroRange {
                    extend: mode & FALLOC_FL_KEEP_SIZE == 0,
                },
                FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE,
            )
        } else {
            (
                MountRangeAllocation::Preallocate {
                    keep_size: mode & FALLOC_FL_KEEP_SIZE != 0,
                },
                FALLOC_FL_KEEP_SIZE,
            )
        };
        if mode & !permitted_flags != 0 {
            return Err(libc::EOPNOTSUPP);
        }
        context.mutate_file(&mount_path(path)?, handle, |file| {
            file.allocate_range(offset, length, operation)
        })?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_copy_file_range(
    address: usize,
    source_path: *const c_char,
    source_handle: u64,
    source_offset: i64,
    destination_path: *const c_char,
    destination_handle: u64,
    destination_offset: i64,
    length: usize,
    flags: c_int,
) -> i64 {
    ffi_offset(address, |context| {
        if flags != 0 {
            return Err(libc::EINVAL);
        }
        let source_offset = u64::try_from(source_offset).map_err(|_| libc::EINVAL)?;
        let destination_offset = u64::try_from(destination_offset).map_err(|_| libc::EINVAL)?;
        let length = u64::try_from(length).map_err(|_| libc::EOVERFLOW)?;
        let source = context.file_or_open(&mount_path(source_path)?, source_handle)?;
        let length = context.mutate_file(
            &mount_path(destination_path)?,
            destination_handle,
            |destination| {
                let source_lookup = source.lookup()?;
                let destination_lookup = destination.lookup()?;
                let length = length.min(
                    source_lookup
                        .node
                        .logical_bytes
                        .saturating_sub(source_offset),
                );
                context.source.clone_range_by_id(
                    source_lookup.node.file_id,
                    source_offset,
                    destination_lookup.node.file_id,
                    destination_offset,
                    length,
                )?;
                Ok(length)
            },
        )?;
        i64::try_from(length).map_err(|_| libc::EOVERFLOW)
    })
}

fn mount_path(path: *const c_char) -> Result<MountPath, i32> {
    let bytes = c_bytes(path)?;
    if bytes.first().copied() != Some(b'/') {
        return Err(libc::EINVAL);
    }
    let mut result = MountPath::root();
    for component in bytes.split(|byte| *byte == b'/').skip(1) {
        if component.is_empty() {
            continue;
        }
        if component == b"." || component == b".." {
            return Err(libc::EPERM);
        }
        result = result.child(component.to_vec());
    }
    Ok(result)
}

fn c_bytes<'a>(value: *const c_char) -> Result<&'a [u8], i32> {
    if value.is_null() {
        return Err(libc::EINVAL);
    }
    Ok(unsafe { CStr::from_ptr(value) }.to_bytes())
}

fn path_cstring(path: &Path) -> Result<CString, i32> {
    CString::new(path.as_os_str().as_bytes()).map_err(|_| libc::EINVAL)
}

fn bounded_length(length: usize) -> Result<u32, i32> {
    if length > MAXIMUM_CALLBACK_BYTES {
        return Err(libc::E2BIG);
    }
    u32::try_from(length).map_err(|_| libc::EOVERFLOW)
}

fn copy_variable_result(bytes: &[u8], output: *mut c_char, length: usize) -> Result<c_int, i32> {
    if length == 0 {
        return c_int::try_from(bytes.len()).map_err(|_| libc::EOVERFLOW);
    }
    if output.is_null() || bytes.len() > length {
        return Err(libc::ERANGE);
    }
    unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), output.cast(), bytes.len()) };
    c_int::try_from(bytes.len()).map_err(|_| libc::EOVERFLOW)
}

fn create_metadata(mode: u32, kind: u32, uid: u32, gid: u32) -> FileMetadata {
    let now = system_time_ns(SystemTime::now()).unwrap_or(i64::MAX);
    FileMetadata {
        posix_mode: MetadataField::Value((mode & 0o7777) | kind),
        posix_uid: MetadataField::Value(uid),
        posix_gid: MetadataField::Value(gid),
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

fn update_time(field: &mut MetadataField<i64>, seconds: i64, nanoseconds: i64) -> Result<(), i32> {
    if nanoseconds == libc::UTIME_OMIT {
        return Ok(());
    }
    if nanoseconds == libc::UTIME_NOW {
        *field = MetadataField::Value(system_time_ns(SystemTime::now())?);
        return Ok(());
    }
    if !(0..1_000_000_000).contains(&nanoseconds) {
        return Err(libc::EINVAL);
    }
    let nanos = i128::from(seconds)
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(i128::from(nanoseconds)))
        .ok_or(libc::EOVERFLOW)?;
    *field = MetadataField::Value(i64::try_from(nanos).map_err(|_| libc::EOVERFLOW)?);
    Ok(())
}

fn metadata_time(field: MetadataField<i64>) -> (i64, u32) {
    let nanos = match field {
        MetadataField::Unavailable => 0,
        MetadataField::Value(value) => value,
    };
    (
        nanos.div_euclid(1_000_000_000),
        u32::try_from(nanos.rem_euclid(1_000_000_000)).unwrap_or(0),
    )
}

fn errno(error: &MountSourceError) -> i32 {
    match error {
        MountSourceError::NotFound => libc::ENOENT,
        MountSourceError::AlreadyExists => libc::EEXIST,
        MountSourceError::Invalid(_) => libc::EINVAL,
        MountSourceError::Unsupported(_) => libc::EOPNOTSUPP,
        MountSourceError::Engine(_) => libc::EIO,
        MountSourceError::Stale => libc::ESTALE,
    }
}

fn source_error(error: &MountSourceError) -> NativeMountError {
    NativeMountError::Driver(error.to_string())
}

fn driver_errno(error: i32) -> NativeMountError {
    NativeMountError::Driver(std::io::Error::from_raw_os_error(error).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_mount::MountPublication;

    unsafe extern "C" {
        fn nfs4_test_read_reply() -> c_int;
        fn nfs4_test_durable_writes() -> c_int;
        fn nfs4_test_change_attribute() -> c_int;
        fn nfs4_test_access_rights() -> c_int;
        fn nfs4_test_verify_attributes() -> c_int;
        fn nfs4_test_node_identity() -> c_int;
        fn nfs4_test_release_open_files() -> c_int;
    }

    type TestResult = Result<(), Box<dyn std::error::Error>>;
    type MemorySource = crate::native_mount::CheckoutMountSource<
        crate::facade::MemoryAuthorityBackend,
        crate::facade::MemoryObjectBackend,
    >;

    /// A writable context over a fresh POSIX checkout.
    fn checkout_context(
        publication: MountPublication,
    ) -> Result<(Arc<MemorySource>, DarwinMountContext), Box<dyn std::error::Error>> {
        use crate::Fs;
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, Lifecycle};
        use crate::native_mount::SharedCheckout;

        let mut config = crate::model::VolumeConfig::portable(Lifecycle::Ephemeral);
        config.profile = FilesystemProfile::Posix;
        let fs = Fs::memory();
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let checkout = runtime.block_on(async {
            let cancellation = crate::CancellationToken::new();
            let volume = fs
                .create_volume(config, crate::WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value;
            volume
                .checkout(
                    GenerationSelector::Head,
                    CheckoutMode::tracking_transaction(),
                    crate::WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .map(|receipt| receipt.value)
        })?;
        let shared = Arc::new(SharedCheckout::with_publication(checkout, publication));
        let source = Arc::new(MemorySource::new(shared, config)?);
        let root_id = source
            .lookup(&MountPath::root())?
            .ok_or("root absent")?
            .node
            .file_id;
        let context = DarwinMountContext::new(
            Arc::clone(&source) as Arc<dyn MountFilesystem>,
            true,
            &std::fs::metadata(".")?,
            root_id,
        );
        Ok((source, context))
    }

    fn os(error: i32) -> std::io::Error {
        std::io::Error::from_raw_os_error(error)
    }

    #[test]
    fn close_publishes_only_what_its_handle_wrote() -> TestResult {
        let (source, context) = checkout_context(MountPublication::CloseAndSync)?;
        let epoch = || source.generation_id();
        let path = MountPath::root().child(b"published".to_vec());
        context
            .mutate(|| source.create_file(&path, FileMetadata::default()))
            .map_err(os)?;
        let created = context
            .open(&path, InitialHandleState::Written)
            .map_err(os)?;
        let before = epoch()?;
        context.flush_on_close(created).map_err(os)?;
        assert_ne!(epoch()?, before, "closing the creating handle publishes");

        let reader = context.open(&path, InitialHandleState::Clean).map_err(os)?;
        let before = epoch()?;
        context.flush_on_close(reader).map_err(os)?;
        context.sync().map_err(os)?;
        assert_eq!(epoch()?, before, "a clean close or sync publishes nothing");

        context
            .mutate_file(&path, reader, |file| {
                file.write_range(0, Bytes::from_static(b"through the handle"))
            })
            .map_err(os)?;
        let before = epoch()?;
        context.flush_on_close(reader).map_err(os)?;
        assert_ne!(epoch()?, before, "a written handle's close publishes");
        let before = epoch()?;
        context.flush_on_close(reader).map_err(os)?;
        context.sync().map_err(os)?;
        assert_eq!(epoch()?, before, "published writes are not published again");

        // NFS may write under a stateid that names no open handle: that
        // write does not dirty the handle, but a sync still covers it.
        context
            .mutate_file(&path, 0, |file| {
                file.write_range(0, Bytes::from_static(b"without a handle"))
            })
            .map_err(os)?;
        context.flush_on_close(reader).map_err(os)?;
        let before = epoch()?;
        context.sync().map_err(os)?;
        assert_ne!(
            epoch()?,
            before,
            "a sync publishes every acknowledged write"
        );
        Ok(())
    }

    #[test]
    fn manual_close_and_fsync_publish_nothing_until_sync() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let epoch = || source.generation_id();
        let path = MountPath::root().child(b"manual".to_vec());
        context
            .mutate(|| source.create_file(&path, FileMetadata::default()))
            .map_err(os)?;
        let created = context
            .open(&path, InitialHandleState::Written)
            .map_err(os)?;
        context
            .mutate_file(&path, created, |file| {
                file.write_range(0, Bytes::from_static(b"deferred"))
            })
            .map_err(os)?;
        let reader = context.open(&path, InitialHandleState::Clean).map_err(os)?;
        let before = epoch()?;
        context.flush_on_close(reader).map_err(os)?;
        context.flush_on_close(created).map_err(os)?;
        context.sync().map_err(os)?;
        assert_eq!(epoch()?, before, "manual close and fsync publish nothing");
        source.sync()?;
        assert_ne!(epoch()?, before, "an explicit mount sync publishes");
        Ok(())
    }

    #[test]
    fn change_labels_advance_exactly_for_objects_that_changed() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let label = |path: &MountPath| {
            context
                .attributes(path, 0)
                .map(|attributes| attributes.change)
                .map_err(os)
        };
        let root = MountPath::root();
        let first = root.child(b"first".to_vec());
        let second = root.child(b"second".to_vec());
        for path in [&first, &second] {
            context
                .mutate(|| source.create_file(path, FileMetadata::default()))
                .map_err(os)?;
        }
        let (root_label, first_label, second_label) =
            (label(&root)?, label(&first)?, label(&second)?);
        assert_eq!(
            label(&first)?,
            first_label,
            "an unchanged file keeps its label"
        );
        assert_ne!(first_label, second_label, "objects never share a label");

        context
            .mutate_file(&first, 0, |file| {
                file.write_range(0, Bytes::from_static(b"written"))
            })
            .map_err(os)?;
        assert!(
            label(&first)? > first_label,
            "a written file gets a new label"
        );
        assert_eq!(label(&second)?, second_label, "other files keep theirs");
        assert_eq!(
            label(&root)?,
            root_label,
            "a content write keeps the listing"
        );

        source.remove(&second, None)?;
        assert!(
            label(&root)? > root_label,
            "a namespace change relabels its directory"
        );

        let first_label = label(&first)?;
        context.forget_view();
        assert!(
            label(&first)? > first_label,
            "an invalidation relabels everything"
        );

        let unstamped = ChangeLabels::new();
        let lookup = source.lookup(&first)?.ok_or("file absent")?;
        let observed = || Observed {
            path: first.clone(),
            lookup,
        };
        assert_ne!(
            unstamped.label(
                lookup.node.file_id,
                None,
                None,
                observed(),
                |_| true,
                |_| true
            ),
            unstamped.label(
                lookup.node.file_id,
                None,
                None,
                observed(),
                |_| true,
                |_| true
            ),
            "a source without stamps never repeats a label"
        );
        Ok(())
    }

    /// After a fence that could not confirm every change was reported, each
    /// labelled object is read again: one that reads the same keeps its
    /// label and a revalidation returns without waiting out the client's
    /// attribute timeout, while one that differs makes it wait.
    #[test]
    fn an_unconfirmed_fence_waits_only_for_what_changed() -> TestResult {
        use super::super::view_ledger::ViewChange;
        use std::time::{Duration, Instant};

        let (source, context) = checkout_context(MountPublication::Manual)?;
        let context = Arc::new(context);
        let observer = Arc::downgrade(&context);
        let observer: std::sync::Weak<dyn ViewObserver> = observer;
        source.observe_view(observer);
        let path = MountPath::root().child(b"kept".to_vec());
        source.create_file(&path, FileMetadata::default())?;
        // The creation was made around the mount; take it as waited out.
        let made = context.around.made.load(Ordering::Acquire);
        context.around.settled.store(made, Ordering::Release);
        let label = context.attributes(&path, 0).map_err(os)?.change;

        source.record_projection_change(&ViewChange::Unconfirmed);
        let started = Instant::now();
        context.revalidate();
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "nothing changed, so nothing is waited out"
        );
        assert_eq!(context.attributes(&path, 0).map_err(os)?.change, label);

        // What the client holds no longer matches what the source reads.
        for issued in context
            .changes
            .labels
            .lock()
            .map_err(|_| "labels poisoned")?
            .values_mut()
        {
            issued.observed.lookup.node.logical_bytes += 1;
        }
        source.record_projection_change(&ViewChange::Unconfirmed);
        let before = context.around.made.load(Ordering::Acquire);
        context.revalidate();
        // The wait ends on a whole uptime second, so its length says little;
        // what it waited out does.
        let waited = context.around.made.load(Ordering::Acquire);
        assert!(
            waited > before && context.around.settled.load(Ordering::Acquire) >= waited,
            "a change the fence could not confirm is waited out"
        );
        Ok(())
    }

    #[test]
    fn attributes_observed_before_a_change_never_take_a_later_label() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let path = MountPath::root().child(b"raced".to_vec());
        let created = source.create_file(&path, FileMetadata::default())?;
        let file_id = created.node.file_id;
        let binding = source.binding_epoch();
        let unchanged = |stamp| context.unchanged_since(&path, Some(file_id), stamp);
        let reported = |stamp| context.reported_unchanged_since(&path, Some(file_id), stamp);
        let before = context.cache_stamp().ok_or("checkout has no view stamp")?;
        source.write_range(&path, 0, Bytes::from_static(b"changed"))?;
        let after = context.cache_stamp().ok_or("checkout has no view stamp")?;
        let written = source.lookup(&path)?.ok_or("file absent")?;
        let observed = |lookup| Observed {
            path: path.clone(),
            lookup,
        };

        let current = context.changes.label(
            file_id,
            Some(after),
            binding,
            observed(written),
            unchanged,
            reported,
        );
        assert_eq!(
            context.changes.label(
                file_id,
                Some(after),
                binding,
                observed(written),
                unchanged,
                reported
            ),
            current,
            "attributes observed after the change share its label"
        );
        assert!(
            context.changes.label(
                file_id,
                Some(before),
                binding,
                observed(created),
                unchanged,
                reported
            ) > current,
            "attributes observed before it get a label of their own"
        );
        Ok(())
    }

    #[test]
    fn forgetting_the_view_rereads_what_the_source_did_not_record() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let path = MountPath::root().child(b"unrecorded".to_vec());
        source.create_file(&path, FileMetadata::default())?;
        let handle = context.open(&path, InitialHandleState::Clean).map_err(os)?;
        let size = |handle| {
            context
                .attributes(&path, handle)
                .map(|attributes| attributes.logical_bytes)
                .map_err(os)
        };
        assert_eq!((size(0)?, size(handle)?), (0, 0));
        let label = context.attributes(&path, 0).map_err(os)?.change;

        // Stand in for a change the source reports to nobody: seed the
        // caches with a size the source no longer has.
        let stale = |lookup: &mut MountLookup| lookup.node.logical_bytes = 7;
        context
            .lookups
            .lock()
            .map_err(|_| "poisoned lookups")?
            .entries
            .values_mut()
            .filter_map(|(lookup, _)| lookup.as_mut())
            .for_each(stale);
        let files = context.files.read().map_err(|_| "poisoned handles")?;
        stale(
            &mut files
                .get(&handle)
                .ok_or("handle absent")?
                .observation
                .lock()
                .map_err(|_| "poisoned observation")?
                .lookup,
        );
        drop(files);
        assert_eq!((size(0)?, size(handle)?), (7, 7), "the caches were seeded");

        context.forget_view();
        assert_eq!(
            (size(0)?, size(handle)?),
            (0, 0),
            "neither the lookup cache nor a handle serves a forgotten fact"
        );
        let relabeled = context.attributes(&path, 0).map_err(os)?.change;
        assert!(
            relabeled > label,
            "the reread attributes take a fresh label"
        );
        assert_eq!(
            context.attributes(&path, handle).map_err(os)?.change,
            relabeled,
            "which then holds for the unchanged object"
        );
        Ok(())
    }

    #[test]
    #[allow(unsafe_code)]
    fn revalidation_waits_out_the_attribute_timeout_for_changes_around_the_mount() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let context = context.observing_source();
        // SAFETY: the bridge reports a constant.
        let timeout = u64::from(unsafe { acyclic_fs_darwin_mount_attribute_timeout() });
        let barrier = || {
            let began = uptime();
            context.revalidate();
            (began, uptime())
        };
        let (began, ended) = barrier();
        assert!(ended - began < REPLY_DELIVERY_SLACK, "an unchanged view");

        // SAFETY: the name is NUL-terminated and `context` outlives the call.
        let status = unsafe {
            acyclic_fs_darwin_mount_mkdir(
                Arc::as_ptr(&context) as usize,
                c"/own".as_ptr(),
                0o755,
                0,
                0,
            )
        };
        assert_eq!(status, 0);
        let (began, ended) = barrier();
        assert!(
            ended - began < REPLY_DELIVERY_SLACK,
            "the client learns of the mount's own changes from their replies"
        );

        let waits_out_the_timeout = || {
            let (began, ended) = barrier();
            assert!(
                ended.as_secs() >= (began + REPLY_DELIVERY_SLACK).as_secs() + timeout,
                "every attribute cached before the barrier expired"
            );
            let (began, ended) = barrier();
            assert!(ended - began < REPLY_DELIVERY_SLACK, "once per change");
        };
        source.create_file(
            &MountPath::root().child(b"around".to_vec()),
            FileMetadata::default(),
        )?;
        waits_out_the_timeout();
        context.forget_view();
        waits_out_the_timeout();
        Ok(())
    }

    #[test]
    fn a_barrier_waits_only_for_callbacks_that_began_before_it() -> TestResult {
        let gate = Arc::new(CallbackGate::new());
        let earlier = gate.enter();
        let (drained, finished) = std::sync::mpsc::channel();
        let barrier = std::thread::spawn({
            let gate = Arc::clone(&gate);
            move || {
                gate.drain();
                let _ = drained.send(());
            }
        });
        assert!(
            finished.recv_timeout(Duration::from_millis(50)).is_err(),
            "an earlier callback holds the barrier"
        );
        let later = gate.enter();
        drop(earlier);
        finished.recv_timeout(Duration::from_secs(5))?;
        drop(later);
        barrier.join().map_err(|_| "barrier panicked")?;
        Ok(())
    }

    #[test]
    #[allow(unsafe_code)]
    fn nfs_read_answers_short_reads_as_eof_in_place() {
        // SAFETY: the test hook owns all callback state.
        assert_eq!(unsafe { nfs4_test_read_reply() }, 0);
    }

    #[test]
    #[allow(unsafe_code)]
    fn nfs_fileids_name_nodes_so_hard_links_share_one() {
        // SAFETY: the test hook owns all callback state.
        assert_eq!(unsafe { nfs4_test_node_identity() }, 0);
    }

    #[test]
    #[allow(unsafe_code)]
    fn nfs_durable_writes_are_stable_without_a_sync() {
        // SAFETY: the test hook owns all callback state.
        assert_eq!(unsafe { nfs4_test_durable_writes() }, 0);
    }

    #[test]
    #[allow(unsafe_code)]
    fn only_a_publishing_source_leaves_writes_unstable() -> TestResult {
        for (publication, durable) in [
            (MountPublication::Manual, 1),
            (MountPublication::CloseAndSync, 0),
        ] {
            let (_, context) = checkout_context(publication)?;
            // SAFETY: `context` outlives the call it is passed to.
            let answer = unsafe {
                acyclic_fs_darwin_mount_durable_writes(std::ptr::from_ref(&context) as usize)
            };
            assert_eq!(answer, durable, "{publication:?}");
        }
        Ok(())
    }

    #[test]
    fn handles_open_their_source_file_only_when_used() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let path = MountPath::root().child(b"deferred".to_vec());
        let created = source.create_file(&path, FileMetadata::default())?;
        source.write_range(&path, 0, Bytes::from_static(b"kept"))?;
        let bound = |handle: u64| -> Result<bool, Box<dyn std::error::Error>> {
            let files = context.files.read().map_err(|_| "poisoned handles")?;
            Ok(files.get(&handle).ok_or("handle absent")?.file.is_some())
        };

        let closed_unused = context.open(&path, InitialHandleState::Clean).map_err(os)?;
        assert!(!bound(closed_unused)?, "an open binds nothing");
        assert_eq!(
            context
                .lookup_handle(&path, closed_unused)
                .map_err(os)?
                .node
                .file_id,
            created.node.file_id,
            "attributes need no source file"
        );
        assert!(!bound(closed_unused)?);

        let read = context.open(&path, InitialHandleState::Clean).map_err(os)?;
        let bytes = context
            .file_or_open(&path, read)
            .map_err(os)?
            .read_up_to(0, 16)?;
        assert_eq!(bytes.as_ref(), b"kept");
        assert!(bound(read)?, "the first read binds the source file");

        // A replacement outside this mount leaves an unbound handle stale,
        // as an NFS handle on a removed object is.
        let stale = context.open(&path, InitialHandleState::Clean).map_err(os)?;
        source.remove(&path, Some(created.node.file_id))?;
        source.create_file(&path, FileMetadata::default())?;
        assert_eq!(context.file_or_open(&path, stale).err(), Some(libc::ESTALE));
        Ok(())
    }

    #[test]
    #[allow(unsafe_code)]
    fn renaming_over_an_open_file_keeps_it_for_its_handles() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let (replaced, replacement) = (
            MountPath::root().child(b"replaced".to_vec()),
            MountPath::root().child(b"replacement".to_vec()),
        );
        let original = source.create_file(&replaced, FileMetadata::default())?;
        source.write_range(&replaced, 0, Bytes::from_static(b"original"))?;
        source.create_file(&replacement, FileMetadata::default())?;
        let handle = context
            .open(&replaced, InitialHandleState::Clean)
            .map_err(os)?;
        let rename_over = |from: &CStr| {
            // SAFETY: both names are NUL-terminated and `context` outlives the call.
            unsafe {
                acyclic_fs_darwin_mount_rename(
                    std::ptr::from_ref(&context) as usize,
                    from.as_ptr(),
                    c"/replaced".as_ptr(),
                    0,
                )
            }
        };
        assert_eq!(rename_over(c"/missing"), -libc::ENOENT);
        assert!(
            context
                .files
                .read()
                .map_err(|_| "poisoned handles")?
                .get(&handle)
                .ok_or("handle absent")?
                .file
                .is_none(),
            "a failed rename leaves the handle as it was"
        );
        assert_eq!(rename_over(c"/replacement"), 0);
        let file = context.file_or_open(&replaced, handle).map_err(os)?;
        assert_eq!(file.lookup()?.node.file_id, original.node.file_id);
        assert_eq!(file.read_up_to(0, 16)?.as_ref(), b"original");
        Ok(())
    }

    #[test]
    #[allow(unsafe_code)]
    fn named_attribute_probes_need_no_source_call_without_attributes() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let path = MountPath::root().child(b"plain".to_vec());
        source.create_file(&path, FileMetadata::default())?;
        assert!(!context.may_have_named_attributes(&path).map_err(os)?);
        // SAFETY: the names are NUL-terminated and `context` outlives the call.
        let probe = || unsafe {
            acyclic_fs_darwin_mount_getxattr(
                std::ptr::from_ref(&context) as usize,
                c"/plain".as_ptr(),
                c"com.apple.provenance".as_ptr(),
                ptr::null_mut(),
                0,
            )
        };
        assert_eq!(probe(), -libc::ENOATTR);
        source.write_attribute(
            &path,
            b"com.apple.provenance",
            Bytes::from_static(b"origin"),
            MountAttributeWriteMode::Upsert,
        )?;
        assert!(context.may_have_named_attributes(&path).map_err(os)?);
        assert_eq!(probe(), 6, "a present attribute is read from the source");
        Ok(())
    }

    #[test]
    #[allow(unsafe_code)]
    fn nfs_change_attribute_travels_with_its_attributes() {
        // SAFETY: the test hook owns all callback state.
        assert_eq!(unsafe { nfs4_test_change_attribute() }, 0);
    }

    #[test]
    #[allow(unsafe_code)]
    fn nfs_access_applies_the_callers_identity_to_mode_bits() {
        // SAFETY: the test hook owns all callback state.
        assert_eq!(unsafe { nfs4_test_access_rights() }, 0);
    }

    #[test]
    #[allow(unsafe_code)]
    fn nfs_verify_compares_current_attribute_values() {
        // SAFETY: the test hook owns all callback state.
        assert_eq!(unsafe { nfs4_test_verify_attributes() }, 0);
    }

    #[test]
    #[allow(unsafe_code)]
    fn nfs_teardown_releases_every_open_handle_once() {
        // SAFETY: the test hook owns all callback state.
        assert_eq!(unsafe { nfs4_test_release_open_files() }, 0);
    }

    #[test]
    fn content_changes_stamp_modification_and_status_times() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let path = MountPath::root().child(b"stamped".to_vec());
        let created = FileMetadata {
            modified_ns: MetadataField::Value(1),
            changed_ns: MetadataField::Value(1),
            ..create_metadata(0o644, mode::IFREG, 0, 0)
        };
        context
            .mutate(|| source.create_file(&path, created))
            .map_err(os)?;
        let times = || -> Result<_, Box<dyn std::error::Error>> {
            let metadata = source.lookup(&path)?.ok_or("file absent")?.metadata;
            Ok((metadata.modified_ns, metadata.changed_ns))
        };
        context
            .mutate_file(&path, 0, |file| {
                file.write_range(0, Bytes::from_static(b"stamped"))
            })
            .map_err(os)?;
        let (MetadataField::Value(modified), MetadataField::Value(changed)) = times()? else {
            return Err("write dropped a time".into());
        };
        assert!(modified > 1 && changed > 1, "a write stamps both times");
        context
            .mutate_file(&path, 0, |file| file.resize(1))
            .map_err(os)?;
        let (MetadataField::Value(resized), _) = times()? else {
            return Err("resize dropped a time".into());
        };
        assert!(resized >= modified, "a truncation stamps the time again");
        Ok(())
    }

    /// Mounts a fresh checkout for one live macOS test.
    #[allow(clippy::type_complexity)]
    fn live_mount() -> Result<
        (
            tempfile::TempDir,
            Arc<MemorySource>,
            crate::NativeMountSession,
        ),
        Box<dyn std::error::Error>,
    > {
        let (source, _) = checkout_context(MountPublication::Manual)?;
        let temporary = tempfile::tempdir()?;
        let mount = crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id: source.volume_id()?,
                destination: temporary.path().to_path_buf(),
                writable: true,
            },
            Arc::clone(&source) as Arc<dyn MountFilesystem>,
        )?;
        Ok((temporary, source, mount))
    }

    /// The local socket a live mount's server listens on, from the mount
    /// table's `<socket>:/` source.
    fn mount_socket(destination: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let destination = destination.canonicalize()?;
        let mut mounts = std::ptr::null_mut::<libc::statfs>();
        // SAFETY: `getmntinfo` points `mounts` at an OS-owned array of the
        // returned length, valid until the next call.
        let count = unsafe { libc::getmntinfo(&raw mut mounts, libc::MNT_NOWAIT) };
        // SAFETY: as above.
        let mounts = unsafe { std::slice::from_raw_parts(mounts, usize::try_from(count)?) };
        mounts
            .iter()
            .find_map(|mount| {
                // SAFETY: both names are NUL-terminated by the OS.
                let (on, from) = unsafe {
                    (
                        CStr::from_ptr(mount.f_mntonname.as_ptr()),
                        CStr::from_ptr(mount.f_mntfromname.as_ptr()),
                    )
                };
                (on.to_bytes() == destination.as_os_str().as_bytes())
                    .then(|| from.to_str().ok())
                    .flatten()
                    .and_then(|from| from.strip_prefix('<'))
                    .and_then(|from| from.split_once(">:"))
                    .map(|(socket, _)| PathBuf::from(socket))
            })
            .ok_or_else(|| "mount has no local socket source".into())
    }

    #[test]
    #[ignore = "requires a live macOS NFS mount"]
    fn macos_revalidation_shows_changes_made_around_the_mount() -> TestResult {
        let (temporary, source, mut mount) = live_mount()?;
        let (changed, created) = (
            temporary.path().join("changed"),
            temporary.path().join("created"),
        );
        let names = || -> std::io::Result<Vec<_>> {
            std::fs::read_dir(temporary.path())?
                .map(|entry| entry.map(|entry| entry.file_name()))
                .collect()
        };
        // Cache the file's attributes, the listing, and the absent name.
        std::fs::write(&changed, b"old")?;
        assert_eq!(std::fs::metadata(&changed)?.len(), 3);
        assert_eq!(names()?, ["changed"]);
        assert_eq!(
            std::fs::metadata(&created).err().map(|error| error.kind()),
            Some(std::io::ErrorKind::NotFound)
        );
        let began = Instant::now();
        mount.revalidate()?;
        assert!(
            began.elapsed() < REPLY_DELIVERY_SLACK,
            "the mount's own changes need no wait"
        );

        source.write_range(
            &MountPath::root().child(b"changed".to_vec()),
            0,
            Bytes::from_static(b"newer"),
        )?;
        source.create_file(
            &MountPath::root().child(b"created".to_vec()),
            FileMetadata::default(),
        )?;
        mount.revalidate()?;
        assert_eq!(std::fs::metadata(&changed)?.len(), 5);
        assert!(created.is_file(), "the absent name is looked up again");
        let mut listed = names()?;
        listed.sort();
        assert_eq!(listed, ["changed", "created"]);
        assert_eq!(std::fs::read(&changed)?, b"newer");
        assert!(mount.stop()?);
        Ok(())
    }

    #[test]
    #[ignore = "requires a live macOS NFS mount"]
    fn macos_server_answers_only_the_kernel() -> TestResult {
        use std::io::{Read as _, Write as _};
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

        let (temporary, _, mut mount) = live_mount()?;
        let socket = mount_socket(temporary.path())?;
        let directory = std::fs::metadata(socket.parent().ok_or("socket has no directory")?)?;
        assert_eq!(directory.permissions().mode() & 0o777, 0o700);
        // SAFETY: `getuid` has no preconditions.
        assert_eq!(directory.uid(), unsafe { libc::getuid() });

        // An NFS NULL call, as any local process could send it.
        let mut stream = std::os::unix::net::UnixStream::connect(&socket)?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        let call: [u32; 11] = [0x8000_0028, 1, 0, 2, 100_003, 4, 0, 0, 0, 0, 0];
        let bytes = call
            .iter()
            .flat_map(|word| word.to_be_bytes())
            .collect::<Vec<_>>();
        let _ = stream.write_all(&bytes);
        let mut reply = [0_u8; 4];
        assert_eq!(
            stream.read(&mut reply).unwrap_or(0),
            0,
            "the server closes a user process's connection unanswered"
        );
        std::fs::write(temporary.path().join("still-served"), b"kernel")?;
        assert_eq!(
            std::fs::read(temporary.path().join("still-served"))?,
            b"kernel"
        );
        assert!(mount.stop()?);
        Ok(())
    }

    #[test]
    #[ignore = "requires a live macOS NFS mount"]
    fn macos_locks_exclude_other_processes() -> TestResult {
        use std::os::fd::AsRawFd as _;

        let (temporary, _, mut mount) = live_mount()?;
        let path = temporary.path().join("locked");
        std::fs::write(&path, b"")?;
        // The NFS client keys locks by process, so another process contends.
        let contender = |operation: &str| {
            Command::new("/usr/bin/perl")
                .arg("-e")
                .arg(format!(
                    "use Fcntl qw(:flock :DEFAULT); open(F, '+<', $ARGV[0]) or die;                      exit({operation} ? 0 : 1)"
                ))
                .arg(&path)
                .status()
                .map(|status| status.success())
        };
        let flock = "flock(F, LOCK_EX | LOCK_NB)";
        let fcntl = "fcntl(F, F_SETLK, my $region = pack('q q l s s', 0, 0, 0, F_WRLCK, 0))";
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;
        // SAFETY: `file` owns a live descriptor for each call.
        assert_eq!(unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) }, 0);
        assert!(!contender(flock)?, "a held flock excludes another process");
        // SAFETY: as above.
        assert_eq!(unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) }, 0);
        assert!(contender(flock)?, "a released flock admits another process");
        let region = libc::flock {
            l_start: 0,
            l_len: 0,
            l_pid: 0,
            l_type: libc::c_short::try_from(libc::F_WRLCK)?,
            l_whence: libc::c_short::try_from(libc::SEEK_SET)?,
        };
        // SAFETY: `region` outlives the call on the live descriptor.
        assert_eq!(
            unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLK, &raw const region) },
            0
        );
        assert!(
            !contender(fcntl)?,
            "a held record lock excludes another process"
        );
        drop(file);
        assert!(contender(fcntl)?, "closing releases the record lock");
        assert!(mount.stop()?);
        Ok(())
    }

    #[test]
    #[ignore = "requires a live macOS NFS mount"]
    fn macos_mode_bits_deny_access() -> TestResult {
        use std::os::unix::fs::PermissionsExt as _;

        let (temporary, _, mut mount) = live_mount()?;
        let path = temporary.path().join("read-only");
        std::fs::write(&path, b"kept")?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444))?;
        assert_eq!(
            std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .err()
                .map(|error| error.kind()),
            Some(std::io::ErrorKind::PermissionDenied),
            "a read-only file refuses a writer"
        );
        let script = temporary.path().join("script");
        std::fs::write(&script, b"#!/bin/sh\nexit 0\n")?;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o644))?;
        assert_eq!(
            Command::new(&script)
                .status()
                .err()
                .map(|error| error.kind()),
            Some(std::io::ErrorKind::PermissionDenied),
            "a file without an execute bit does not run"
        );
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))?;
        assert!(Command::new(&script).status()?.success());
        assert_eq!(std::fs::read(&path)?, b"kept");
        assert!(mount.stop()?);
        Ok(())
    }

    #[test]
    fn directory_page_rejects_a_native_mutation_between_reads() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let root = MountPath::root();
        let mut directory =
            DirectoryHandle::new(root, source.binding_epoch(), context.cache_epochs());
        ensure_directory_page(&context, &mut directory)
            .map_err(std::io::Error::from_raw_os_error)?;
        source.create_file(
            &MountPath::root().child(b"after-page".to_vec()),
            FileMetadata::default(),
        )?;
        assert_eq!(
            finish_directory_page(&context, &directory, false),
            Err(libc::ESTALE)
        );
        assert_eq!(
            ensure_directory_page(&context, &mut directory),
            Err(libc::ESTALE)
        );
        Ok(())
    }

    #[test]
    fn directory_continuation_requires_its_listing_and_binding_unchanged() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let nested = MountPath::root().child(b"nested".to_vec());
        source.create_directory(&nested, FileMetadata::default())?;
        let epochs = context.cache_epochs().ok_or("checkout has no view stamp")?;
        let root = DirectoryHandle::new(MountPath::root(), Some(epochs.binding), Some(epochs));
        let directory = DirectoryHandle::new(nested.clone(), Some(epochs.binding), Some(epochs));
        let rebound = DirectoryHandle::new(
            nested.clone(),
            Some(epochs.binding + 1),
            Some(CacheEpochs {
                binding: epochs.binding + 1,
                ..epochs
            }),
        );
        assert!(root.is_current(&context));
        assert!(directory.is_current(&context));
        assert!(!rebound.is_current(&context));
        assert!(directory.can_reuse_pages());

        source.create_file(
            &MountPath::root().child(b"sibling".to_vec()),
            FileMetadata::default(),
        )?;
        assert!(!root.is_current(&context));
        assert!(directory.is_current(&context));
        source.create_file(&nested.child(b"child".to_vec()), FileMetadata::default())?;
        assert!(!directory.is_current(&context));

        let unrecorded =
            DirectoryHandle::new(nested, source.binding_epoch(), context.cache_epochs());
        assert!(unrecorded.is_current(&context));
        context.forget_view();
        assert!(
            !unrecorded.is_current(&context),
            "a forgotten view invalidates every page"
        );

        let epochless = DirectoryHandle::new(MountPath::root(), None, None);
        assert!(!epochless.is_current(&context));
        assert!(!epochless.can_reuse_pages());
        Ok(())
    }

    #[test]
    fn stalled_mount_loop_keeps_destination_fenced_until_callbacks_finish() {
        let (release, waiting) = std::sync::mpsc::channel::<()>();
        let (exited, complete) = std::sync::mpsc::channel::<()>();
        let events = Arc::new(LoopEvents::default());
        let exit_report = ExitReport(Arc::clone(&events));
        let thread = std::thread::spawn(move || {
            let _exit_report = exit_report;
            let _ = waiting.recv();
            let _ = exited.send(());
            0
        });
        let mut session = DarwinMountSession {
            resources: None,
            destination: PathBuf::new(),
            thread: Some(thread),
            events,
            loop_finished_before_teardown: None,
            loop_result: None,
            teardown_complete: false,
        };
        let start = Instant::now();
        let result = session.finish_thread_within(Duration::from_millis(20));
        assert!(matches!(result, MountLoopResult::TimedOut));
        assert!(classify_detached_loop(&result, UnmountEvidence::Detached, false).is_err());
        assert!(start.elapsed() < Duration::from_secs(1));
        assert!(session.thread.is_some());
        let _ = release.send(());
        assert!(complete.recv_timeout(Duration::from_secs(1)).is_ok());
        let result = session.finish_thread_within(Duration::from_secs(1));
        assert!(matches!(result, MountLoopResult::Exited(0)));
        assert!(classify_detached_loop(&result, UnmountEvidence::Detached, false).is_ok());
        assert!(session.thread.is_none());
        // This synthetic session has no destination or native driver to stop.
        session.teardown_complete = true;
    }

    #[test]
    fn mount_loop_reports_wake_waiters_and_survive_an_unwinding_loop() {
        const LONG: Duration = Duration::from_secs(60);
        let events = Arc::new(LoopEvents::default());
        let silent = events.wait_until(Duration::from_millis(20), |state| state.mounted);
        assert!(!silent.mounted && !silent.exited, "nothing was reported");

        let (proceed, proceeding) = std::sync::mpsc::channel::<()>();
        let exit_report = ExitReport(Arc::clone(&events));
        let thread = std::thread::spawn(move || {
            // SAFETY: `exit_report` keeps the events alive across the call,
            // as the mount loop thread does.
            unsafe { report_mounted(Arc::as_ptr(&exit_report.0).cast_mut().cast()) };
            let _ = proceeding.recv();
            std::panic::resume_unwind(Box::new("the loop unwinds instead of returning"));
        });
        let began = Instant::now();
        let mounted = events.wait_until(LONG, |state| state.mounted);
        assert!(mounted.mounted && !mounted.exited);
        let _ = proceed.send(());
        let exited = events.wait_until(LONG, |state| state.exited);
        assert!(exited.exited, "an unwinding loop still reports its exit");
        assert!(
            began.elapsed() < LONG / 2,
            "reports wake waiters, not timeouts"
        );
        assert!(thread.join().is_err());
    }
}
