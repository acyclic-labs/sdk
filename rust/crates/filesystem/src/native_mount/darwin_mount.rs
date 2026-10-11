//! Self-contained macOS mount projection over loopback `NFSv4`.
//!
//! The embedded C transport translates NFS requests into this module's bounded
//! callbacks; the canonical Rust checkout remains the only filesystem state.

#![allow(
    unsafe_code,
    reason = "drives the embedded C NFSv4 transport through FFI"
)]

use super::{
    ATTRIBUTE_PAGE_SIZE, DIRECTORY_PAGE_SIZE, DriverStartFailure,
    MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES, MountAttributeWriteMode, MountDirectoryEntry,
    MountFilesystem, MountLookup, MountNodeKind, MountOpenFile, MountPath, MountRangeAllocation,
    MountSeekTarget, MountSourceError, NativeMountError, NativeMountRequest, ViewObserver,
    ViewOrigin, ViewStamp, create_metadata, errno, metadata_or, source_error, system_time_ns,
};
use crate::FileId;
use crate::demand::{NativeDemandReadMeter, native_demand_error};
#[cfg(test)]
use crate::demand::DemandWork;
use crate::native_host::NativeHostWork as _;
use crate::kernel::{FileMetadata, MetadataField};
use bytes::Bytes;
use std::collections::VecDeque;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::fs::Metadata;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::process::Command;
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, PoisonError, RwLock, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};

const ROOT_INODE: u64 = 1;
const MAXIMUM_LOOKUP_CACHE_ENTRIES: usize = 65_536;
/// Most times a listing that has delivered no entry starts over in a view
/// that keeps moving before it fails.
const MAXIMUM_LISTING_RESTARTS: usize = 8;
/// Most labelled objects a revalidation verifies against the source after
/// an unconfirmed fence; with more, waiting out the attribute timeout costs
/// less.
const MAXIMUM_VERIFIED_LABELS: usize = 4_096;
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
/// Existing NFS cache grace policy after completed callbacks. The SDK exposes
/// neither transport delivery nor ACK; this duration is not evidence of either.
const CALLBACK_GRACE_INTERVAL: Duration = Duration::from_millis(100);
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
    file: Option<DarwinSourceFile>,
    observation: DarwinShared<Mutex<FileObservation>>,
    /// Ledger sequence of the latest mutation through this handle; zero
    /// until one completes.
    written: DarwinShared<AtomicU64>,
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

struct DirectoryHandle {
    path: MountPath,
    binding_epoch: Option<u64>,
    epochs: Option<CacheEpochs>,
    cursor: Option<Vec<u8>>,
    entries: VecDeque<MountDirectoryEntry>,
    exhausted: bool,
    emitted: i64,
    revision: Option<u64>,
    path_custody: Option<DarwinOwned<()>>,
    page_custody: Option<DarwinOwned<()>>,
    /// Exact producer custody, including raw source ownership, follows all page data.
    page_source_ownership: Option<crate::demand::SourcePayloadOwnership>,
}

struct DirectoryCheckpoint {
    revision: u64,
    handle: DirectoryHandle,
    key_custody: DarwinOwned<()>,
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
            path_custody: None,
            page_custody: None,
            page_source_ownership: None,
        }
    }

    fn rewind(&mut self) {
        self.clear_page();
        self.exhausted = false;
        self.emitted = 0;
        self.revision = None;
    }

    /// Whether buffered pages still describe the directory: its binding is
    /// the same and nothing has changed its listing since they were read.
    fn clear_page(&mut self) {
        drop(self.cursor.take());
        drop(std::mem::take(&mut self.entries));
        drop(self.page_custody.take());
        drop(self.page_source_ownership.take());
    }

    fn pop_entry(&mut self) -> bool {
        let Some(entry) = self.entries.pop_front() else { return false; };
        let bytes = entry.name.capacity() as u64;
        let source_ownership = self.page_source_ownership.as_mut()
            .filter(|ownership| ownership.has_native_custody())
            .map(|ownership| ownership.split_owned_payload(bytes)
                .expect("entry name belongs to the exact source page"));
        drop(entry);
        drop(source_ownership);
        if let Some(proof) = &mut self.page_custody { proof.retire_part(bytes); }
        true
    }

    fn clone_measured(&self, work: &DarwinCallbackWork<'_>) -> Result<Self, i32> {
        let path_owned = work.clone_path(&self.path)?;
        let mut page_custody = path_owned.alias_proof();
        let width = self.entries.len().checked_mul(std::mem::size_of::<MountDirectoryEntry>()).ok_or(libc::EOVERFLOW)? as u64;
        page_custody.claim_more(width, work)?;
        let mut entries = VecDeque::new();
        entries.try_reserve_exact(self.entries.len()).map_err(|error| work.native_error(NativeMountError::Allocation(error)))?;
        let extra = entries.capacity().checked_sub(self.entries.len())
            .and_then(|capacity| capacity.checked_mul(std::mem::size_of::<MountDirectoryEntry>()))
            .ok_or(libc::EOVERFLOW)? as u64;
        page_custody.claim_more(extra, work)?;
        let cursor = if let Some(cursor) = &self.cursor {
            let copied = crate::WorkCounters { bytes_copied: cursor.len() as u64, ..crate::WorkCounters::UNCHARGED };
            work.admit(copied)?;
            page_custody.claim_more(cursor.len() as u64, work)?;
            let mut value = Vec::new();
            value.try_reserve_exact(cursor.len()).map_err(|error| work.native_error(NativeMountError::Allocation(error)))?;
            page_custody.claim_more((value.capacity() - cursor.len()) as u64, work)?;
            work.copying(copied, || value.extend_from_slice(cursor))?;
            Some(value)
        } else { None };
        for entry in &self.entries {
            work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            let copied = crate::WorkCounters { bytes_copied: (entry.name.len() + std::mem::size_of::<MountDirectoryEntry>()) as u64, ..crate::WorkCounters::UNCHARGED };
            work.admit(copied)?;
            page_custody.claim_more(entry.name.len() as u64, work)?;
            let mut name = Vec::new();
            name.try_reserve_exact(entry.name.len()).map_err(|error| work.native_error(NativeMountError::Allocation(error)))?;
            page_custody.claim_more((name.capacity() - entry.name.len()) as u64, work)?;
            work.copying(copied, || {
                name.extend_from_slice(&entry.name);
                entries.push_back(MountDirectoryEntry { name, node: entry.node, metadata: entry.metadata, pin: entry.pin });
            })?;
        }
        let (path, path_custody) = path_owned.into_parts();
        Ok(Self {
            path, binding_epoch: self.binding_epoch, epochs: self.epochs, cursor, entries,
            exhausted: self.exhausted, emitted: self.emitted, revision: self.revision,
            path_custody: Some(path_custody), page_custody: Some(page_custody),
            page_source_ownership: None,
        })
    }

    #[cfg(test)]
    fn is_current(&self, context: &DarwinMountContext) -> bool {
        context.run_request(|work| self.is_current_measured(context, work)).expect("administrative directory validation")
    }

    fn is_current_measured(&self, context: &DarwinMountContext, work: &DarwinCallbackWork<'_>) -> Result<bool, i32> {
        Ok(match self.epochs {
            Some(epochs) => {
                context.source.view_is_stable_with_meter(work.native).map_err(|error| work.source_error(error))?
                    && context.source.binding_epoch_with_meter(work.native).map_err(|error| work.source_error(error))? == Some(epochs.binding)
                    && context.unchanged_since_measured(&self.path, None, epochs.view, work)?
            }
            None => context.cache_epochs_measured(work)?.is_none(),
        })
    }

    const fn can_reuse_pages(&self) -> bool {
        self.epochs.is_some()
    }

    /// Keeps the listing in the current view, or fails it with `ESTALE`. A
    /// listing that has delivered nothing but `.` and `..` (which no view
    /// changes) starts over in the view current now, as if just opened: a
    /// change to the source while a directory is being opened or first read
    /// never fails the listing. Only one already delivering entries from an
    /// older view fails, rather than mixing two views in one listing.
    fn keep_current_measured(&mut self, context: &DarwinMountContext, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
        for _ in 0..MAXIMUM_LISTING_RESTARTS {
            if !self.can_reuse_pages() || self.is_current_measured(context, work)? {
                return Ok(());
            }
            if self.emitted > 2 || context.source.binding_epoch_with_meter(work.native).map_err(|error| work.source_error(error))? != self.binding_epoch {
                return Err(libc::ESTALE);
            }
            self.epochs = context.cache_epochs_measured(work)?;
            self.clear_page();
            self.exhausted = false;
            self.revision = None;
        }
        Err(libc::ESTALE)
    }
}

/// Completed future requests, including ordinary misses, share the existing
/// diagnostic lock. Active lifecycle requests remain solely in their meter.
#[derive(Default)]
struct DarwinCallbackState {
    completed_future_request_work: crate::WorkCounters,
    first_fatal: Option<super::NativeMountCallbackFailure>,
}

fn darwin_verify_callback_guard(
    guard: Option<&Arc<dyn super::NativeMountCallbackGuard>>,
    work: Option<&super::NativeMountMeterWork>,
) -> Result<(), NativeMountError> {
    let Some(guard) = guard else { return Ok(()); };
    let work = work.ok_or(NativeMountError::ControlPending)?;
    super::adapter::CallbackRuntime::existing()?.verify_native_callback_guard(guard.as_ref(), work)
}

fn darwin_same_native_view(left: &super::NativeMountMeterWork, right: &super::NativeMountMeterWork) -> bool {
    Arc::ptr_eq(left.meter(), right.meter())
        && match (left.resource_allowance(), right.resource_allowance()) {
            (None, None) => true,
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
        && match (left.capture_origin(), right.capture_origin()) {
            (None, None) => true,
            (Some(left), Some(right)) => std::sync::Weak::ptr_eq(left, right),
            _ => false,
        }
        && match (left.invocation_view_state(), right.invocation_view_state()) {
            (None, None) => true,
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
}

fn darwin_native_source_original(
    source: &dyn MountFilesystem, candidate: Option<&super::NativeMountMeterWork>,
) -> Result<Option<super::NativeMountMeterWork>, NativeMountError> {
    let original = source.native_original_work()?;
    if let Some(original) = &original {
        if let Some(candidate) = candidate {
            if !darwin_same_native_view(original, candidate) {
                return Err(NativeMountError::ControlPending);
            }
        }
        original.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)?;
    }
    Ok(original)
}

struct DarwinMountContext {
    source: Arc<dyn MountFilesystem>,
    allocation_custody: Option<crate::demand::NativeMountAllocationCustody>,
    startup_failure: Mutex<Option<super::NativeMountCallbackFailure>>,
    handler_failure: Mutex<DarwinCallbackState>,
    handler_failed: AtomicBool,
    handler_budget: crate::WorkBudget,
    handler_cancellation: crate::CancellationToken,
    startup_meter: Mutex<super::NativeMountMeterActivation>,
    writable: bool,
    mount_uid: u32,
    mount_gid: u32,
    next_handle: AtomicU64,
    next_inode: AtomicU64,
    inodes: Mutex<DarwinMap<FileId, u64>>,
    /// Changes the source did not record that this mount was told of.
    forgotten: AtomicU64,
    lookups: Mutex<LookupCache>,
    files: RwLock<DarwinMap<u64, FileHandle>>,
    directories: Mutex<DarwinMap<u64, DarwinShared<Mutex<DirectoryHandle>>>>,
    directory_checkpoints: Mutex<DarwinMap<MountPath, DirectoryCheckpoint>>,
    namespace_revision: AtomicU64,
    ledger: FlushLedger,
    changes: ChangeLabels,
    /// Attributes every change this mount's callbacks make to the mount.
    origin: ViewOrigin,
    callbacks: CallbackGate,
    around: AroundChanges,
    native_allocation: Option<crate::NativeAllocation>,
    callback_guard: Option<Arc<dyn super::NativeMountCallbackGuard>>,
    /// Original accounting/lifetime view, not task or source authority.
    source_original_work: Option<super::NativeMountMeterWork>,
}

/// Native callbacks in flight, in two generations, so a barrier can wait
/// for every callback that began before it without holding up later ones.
struct CallbackGate {
    generation: AtomicU64,
    /// Callbacks in flight that entered in an even or odd generation.
    even: AtomicU64,
    odd: AtomicU64,
    /// Serializes barriers and retains an interrupted generation until its
    /// actual callbacks leave, before either slot can be reused.
    draining: Mutex<Option<u64>>,
}

/// One callback's place in its generation, left when dropped.
struct CallbackPass<'a>(&'a AtomicU64);

impl CallbackGate {
    const fn new() -> Self {
        Self {
            generation: AtomicU64::new(0),
            even: AtomicU64::new(0),
            odd: AtomicU64::new(0),
            draining: Mutex::new(None),
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

    /// Ordinary barriers use the same admitted engine, with explicit unlimited
    /// grants. A cancelled measured barrier retains the actual generation slot.
    #[cfg(test)]
    fn drain(&self) {
        let cancellation = crate::CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .expect("explicit unlimited callback phase");
        self.drain_measured(&work).expect("unlimited callback drain remains live");
    }

    #[cfg(test)]
    fn drain_measured(&self, work: &crate::NativeMaterializationWork) -> Result<(), NativeMountError> {
        self.drain_native(&work.native_mount_work())
    }
    fn drain_native(&self, work: &super::NativeMountMeterWork) -> Result<(), NativeMountError> {
        work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)?;
        let mut pending = self.draining.lock().unwrap_or_else(PoisonError::into_inner);
        // An interrupted previous barrier must finish before either slot can
        // be reused. It does not cover callbacks that entered before this retry.
        if let Some(generation) = *pending {
            self.wait_generation(generation, work)?;
            *pending = None;
        }
        let generation = self.generation.fetch_add(1, Ordering::SeqCst);
        *pending = Some(generation);
        self.wait_generation(generation, work)?;
        *pending = None;
        Ok(())
    }

    fn wait_generation(&self, generation: u64, work: &super::NativeMountMeterWork) -> Result<(), NativeMountError> {
        loop {
            work.native_materialization_work().charge(crate::WorkCounters { items_examined: 1,
                ..crate::WorkCounters::UNCHARGED })?;
            if self.slot(generation).load(Ordering::SeqCst) == 0 { return Ok(()); }
            darwin_native_attempt(work, false)?;
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
    fn allocation_custody(&self) -> Option<crate::demand::NativeMountAllocationCustody> {
        self.allocation_custody.clone()
    }

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

/// Uptime excluding sleep, the real clock used by the NFS client's cache.
/// Each actual clock query is admitted; a failed query is not expiry evidence.
fn uptime_measured<W: crate::native_host::NativeHostWork>(work: &mut W) -> Result<Duration, NativeMountError>
where NativeMountError: From<W::Error> {
    let mut now = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    darwin_stop_io(work, false, || {
        let result = unsafe { libc::clock_gettime(libc::CLOCK_UPTIME_RAW, &raw mut now) };
        if result == 0 { Ok(()) } else { Err(std::io::Error::last_os_error()) }
    })?;
    Ok(Duration::new(u64::try_from(now.tv_sec).unwrap_or(0), u32::try_from(now.tv_nsec).unwrap_or(0)))
}

#[cfg(test)]
fn uptime() -> Duration {
    let cancellation = crate::CancellationToken::new();
    uptime_measured(&mut DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation))
        .expect("the native uptime clock is available")
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
        let cancellation = crate::CancellationToken::new();
        super::run_native_mount_handler(crate::WorkBudget::UNBOUNDED, &cancellation, |native| {
            let work = DarwinCallbackWork::new(native);
            work.finish(self.flush_through_measured(required, source, &work))
        }).map(|receipt| receipt.value).map_err(|_| libc::EIO)
    }

    fn flush_through_measured(&self, required: u64, source: &dyn MountFilesystem, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
        if self.flushed.load(Ordering::Acquire) >= required {
            return Ok(());
        }
        let _flushing = self.flushing.lock().map_err(|_| libc::EIO)?;
        if self.flushed.load(Ordering::Acquire) >= required {
            return Ok(());
        }
        let covered = self.latest();
        source.flush_with_meter(work.native).map_err(|error| work.source_error(error))?;
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
    labels: Mutex<DarwinMap<FileId, IssuedLabel>>,
    /// Times the labels were dropped at capacity, after which a revalidation
    /// cannot tell what the client holds.
    cleared: AtomicU64,
}

struct IssuedLabel {
    label: u64,
    /// Sampled before the attributes the label was issued for.
    stamp: CacheStamp,
    binding: Option<u64>,
    /// The attributes it was issued for, and the path they were observed at.
    observed: Observed,
}

/// Attributes a label was issued for, as observed at a path.
struct Observed {
    path: MountPath,
    lookup: MountLookup,
    path_custody: Option<DarwinOwned<()>>,
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
            labels: Mutex::new(DarwinMap::new()),
            cleared: AtomicU64::new(0),
        }
    }

    /// The label for `file_id`'s attributes, `observed` after `stamp` was
    /// sampled under source binding `binding`; `unchanged` tells whether
    /// nothing they depend on changed since a stamp, and `reported_unchanged`
    /// whether nothing reported did. A source without stamps gets a fresh
    /// label every time, so its cached state is never trusted past a
    /// revalidation.
    #[cfg(test)]
    fn label(
        &self, file_id: FileId, stamp: Option<CacheStamp>, binding: Option<u64>, observed: Observed,
        unchanged: impl FnOnce(CacheStamp) -> bool, reported_unchanged: impl FnOnce(CacheStamp) -> bool,
    ) -> u64 {
        let cancellation = crate::CancellationToken::new();
        super::run_native_mount_handler(crate::WorkBudget::UNBOUNDED, &cancellation, |native| {
            let work = DarwinCallbackWork::new(native);
            work.finish(self.label_measured(file_id, stamp, binding, &observed.path, observed.lookup,
                |stamp| Ok(unchanged(stamp)), |stamp| Ok(reported_unchanged(stamp)), &work))
        }).expect("administrative label generation").value
    }

    fn label_measured(
        &self,
        file_id: FileId,
        stamp: Option<CacheStamp>,
        binding: Option<u64>,
        path: &MountPath,
        lookup: MountLookup,
        unchanged: impl FnOnce(CacheStamp) -> Result<bool, i32>,
        reported_unchanged: impl FnOnce(CacheStamp) -> Result<bool, i32>,
        work: &DarwinCallbackWork<'_>,
    ) -> Result<u64, i32> {
        let Some(stamp) = stamp else {
            return Ok(self.fresh());
        };
        // Labels only ever move to fresh values, so a panicked holder
        // cannot leave one that repeats.
        let mut labels = self.labels.lock().unwrap_or_else(PoisonError::into_inner);
        // The issued label names these attributes only if nothing changed
        // since the older of the two observations, so attributes observed
        // before a change never take a label issued after it.
        if let Some(issued) = labels.get_mut_measured(&file_id, work)?
            && issued.binding == binding
        {
            let since = issued.stamp.min(stamp);
            if unchanged(since)? {
                return Ok(issued.label);
            }
            // Read again across changes the source could not confirm, with
            // the same result: the client's copy still holds.
            if issued.observed.matches(&lookup) && reported_unchanged(since)? {
                issued.stamp = issued.stamp.max(stamp);
                return Ok(issued.label);
            }
        }
        if labels.len() >= MAXIMUM_LOOKUP_CACHE_ENTRIES {
            labels.clear();
            self.cleared.fetch_add(1, Ordering::AcqRel);
        }
        let path = work.clone_path(path)?;
        let (path, proof) = path.into_parts();
        let observed = Observed { path, lookup, path_custody: Some(proof) };
        let label = self.fresh();
        labels.insert_measured(
            file_id,
            IssuedLabel {
                label,
                stamp,
                binding,
                observed,
            },
            work,
        )?;
        Ok(label)
    }

    /// An exact caller-owned snapshot. Each path is cloned through the same
    /// component-by-component admission engine used by real callbacks.
    fn issued_with_meter(
        &self, native: &super::NativeMountMeterWork,
    ) -> Result<Option<DarwinOwned<Vec<(FileId, Observed)>>>, NativeMountError> {
        let work = DarwinCallbackWork::new(native);
        let labels = self.labels.lock().unwrap_or_else(PoisonError::into_inner);
        if labels.len() > MAXIMUM_VERIFIED_LABELS { return Ok(None); }
        if labels.is_empty() { return Ok(Some(DarwinOwned::empty(Vec::new()))); }
        let result = (|| {
            let bytes = labels.len().checked_mul(std::mem::size_of::<(FileId, Observed)>())
                .ok_or_else(|| work.native_error(crate::WorkError::Overflow.into()))? as u64;
            let mut rows = work.owned(bytes, || {
                let mut rows = Vec::new();
                rows.try_reserve_exact(labels.len())?;
                Ok(rows)
            })?;
            for (file_id, issued) in labels.iter() {
                work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                let path = work.clone_path(&issued.observed.path)?;
                let (path, proof) = path.into_parts();
                let copied = crate::WorkCounters { bytes_copied:
                    (std::mem::size_of::<FileId>() + std::mem::size_of::<MountLookup>()) as u64,
                    ..crate::WorkCounters::UNCHARGED };
                work.copying(copied, || rows.push((*file_id, Observed {
                    path, lookup: issued.observed.lookup, path_custody: Some(proof),
                })))?;
            }
            Ok(Some(rows))
        })();
        work.finish(result)
    }

    fn fresh(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }
}

/// Lookups, each with the stamp it was resolved after.
struct LookupCache {
    entries: DarwinMap<MountPath, (Option<MountLookup>, CacheStamp, DarwinOwned<()>)>,
}

impl LookupCache {
    fn clear(&mut self) {
        for (path, value) in self.entries.drain() { drop(path); drop(value); }
    }
}

impl Drop for LookupCache {
    fn drop(&mut self) { self.clear(); }
}

struct DarwinContextOwner {
    context: Arc<DarwinMountContext>,
    custody: crate::demand::NativeMountAllocationCustody,
}

pub(super) struct DarwinStartupMeterScope {
    context: Arc<DarwinMountContext>,
    _custody: Option<crate::demand::NativeMountAllocationCustody>,
    meter: super::NativeMountMeterWork,
    generation: u64,
}

impl DarwinStartupMeterScope {
    fn new(owner: &DarwinContextOwner, meter: &super::NativeMountMeterWork) -> Result<Self, NativeMountError> {
        Self::existing(Arc::clone(&owner.context), Some(owner.custody.clone()), meter)
    }
    fn existing(
        context: Arc<DarwinMountContext>, custody: Option<crate::demand::NativeMountAllocationCustody>,
        meter: &super::NativeMountMeterWork,
    ) -> Result<Self, NativeMountError> {
        let generation = context.startup_meter.lock().unwrap_or_else(PoisonError::into_inner).enter(meter)?;
        Ok(Self { context, _custody: custody, meter: meter.clone(), generation })
    }
    /// Aliases this operative SDK scope in the actual accepted I/O payload.
    /// Capture the returned owner in the accepted payload, not just its caller
    /// future: dropping a native completion does not drain accepted SDK I/O.
    pub(super) fn try_clone_for_io(&self) -> Result<Self, NativeMountError> {
        Self::existing(Arc::clone(&self.context), self._custody.clone(), &self.meter)
    }
}

impl Drop for DarwinStartupMeterScope {
    fn drop(&mut self) {
        self.context.startup_meter.lock().unwrap_or_else(PoisonError::into_inner).leave(&self.meter, self.generation);
    }
}


impl DarwinMountContext {
    fn new(
        source: Arc<dyn MountFilesystem>,
        writable: bool,
        metadata: &Metadata,
        root_file_id: FileId,
    ) -> Result<Self, NativeMountError> {
        Self::new_owner(source, writable, metadata.uid(), metadata.gid(),
            Vec::from([(root_file_id, ROOT_INODE)]),
            crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
    }

    fn new_owner(
        source: Arc<dyn MountFilesystem>, writable: bool, mount_uid: u32, mount_gid: u32,
        root_entries: Vec<(FileId, u64)>, handler_budget: crate::WorkBudget,
        handler_cancellation: &crate::CancellationToken,
    ) -> Result<Self, NativeMountError> {
        let source_original_work = darwin_native_source_original(source.as_ref(), None)?;
        let callback_guard = source.native_callback_guard();
        darwin_verify_callback_guard(callback_guard.as_ref(), source_original_work.as_ref())?;
        Ok(Self {
            source,
            allocation_custody: None,
            startup_failure: Mutex::new(None),
            handler_failure: Mutex::new(DarwinCallbackState::default()),
            handler_failed: AtomicBool::new(false),
            handler_budget,
            handler_cancellation: handler_cancellation.clone(),
            startup_meter: Mutex::new(super::NativeMountMeterActivation::default()),
            writable,
            mount_uid,
            mount_gid,
            next_handle: AtomicU64::new(1),
            next_inode: AtomicU64::new(ROOT_INODE + 1),
            inodes: Mutex::new(DarwinMap::from_seed(root_entries)),
            forgotten: AtomicU64::new(0),
            lookups: Mutex::new(LookupCache {
                entries: DarwinMap::new(),
            }),
            files: RwLock::new(DarwinMap::new()),
            directories: Mutex::new(DarwinMap::new()),
            directory_checkpoints: Mutex::new(DarwinMap::new()),
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
            native_allocation: None,
            callback_guard,
            source_original_work,
        })
    }

    fn callback_meter(&self) -> Result<Option<super::NativeMountMeterWork>, NativeMountError> {
        let active = self.startup_meter.lock().unwrap_or_else(PoisonError::into_inner).upgrade();
        let candidate = active.as_ref().or(self.source_original_work.as_ref());
        let current = darwin_native_source_original(self.source.as_ref(), candidate)?;
        if self.source_original_work.is_some() != current.is_some() {
            return Err(NativeMountError::ControlPending);
        }
        if let Some(original) = &self.source_original_work {
            let current = current.as_ref().expect("installed native original remains present");
            if !darwin_same_native_view(original, current) {
                return Err(NativeMountError::ControlPending);
            }
        }
        let selected = candidate.cloned();
        if let Some(work) = &selected {
            work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)?;
        }
        let current_guard = self.source.native_callback_guard();
        match (&self.callback_guard, &current_guard) {
            (None, None) => {}
            (Some(original), Some(current)) if Arc::ptr_eq(original, current) => {
                darwin_verify_callback_guard(Some(original), selected.as_ref())?;
            }
            _ => return Err(NativeMountError::ControlPending),
        }
        Ok(selected)
    }

    /// Shares this context, told of every change to its source's view.
    fn observing_source(self) -> Result<Arc<Self>, MountSourceError> {
        let context = Arc::new(self);
        let observer: Weak<dyn ViewObserver> = Arc::<Self>::downgrade(&context);
        context.source.observe_view(observer)?;
        Ok(context)
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
    fn cache_stamp_measured(&self, work: &DarwinCallbackWork<'_>) -> Result<Option<CacheStamp>, i32> {
        let forgotten = self.forgotten.load(Ordering::Acquire);
        Ok(self.source.view_stamp_with_meter(work.native).map_err(|error| work.source_error(error))?
            .map(|view| CacheStamp { forgotten, view }))
    }

    fn unchanged_since_measured(&self, path: &MountPath, file_id: Option<FileId>, stamp: CacheStamp, work: &DarwinCallbackWork<'_>) -> Result<bool, i32> {
        if stamp.forgotten != self.forgotten.load(Ordering::Acquire) { return Ok(false); }
        self.source.unchanged_since_with_meter(path, file_id, stamp.view, work.native)
            .map_err(|error| work.source_error(error))
    }

    fn reported_unchanged_since_measured(&self, path: &MountPath, file_id: Option<FileId>, stamp: CacheStamp, work: &DarwinCallbackWork<'_>) -> Result<bool, i32> {
        if stamp.forgotten != self.forgotten.load(Ordering::Acquire) { return Ok(false); }
        self.source.reported_unchanged_since_with_meter(path, file_id, stamp.view, work.native)
            .map_err(|error| work.source_error(error))
    }

    fn cache_epochs_measured(&self, work: &DarwinCallbackWork<'_>) -> Result<Option<CacheEpochs>, i32> {
        if !self.source.view_is_stable_with_meter(work.native).map_err(|error| work.source_error(error))? { return Ok(None); }
        let Some(view) = self.cache_stamp_measured(work)? else { return Ok(None); };
        let Some(binding) = self.source.binding_epoch_with_meter(work.native).map_err(|error| work.source_error(error))? else { return Ok(None); };
        Ok(Some(CacheEpochs { view, binding }))
    }

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
    /// that began before this call is waited out. Existing cache timing policy
    /// adds [`CALLBACK_GRACE_INTERVAL`] and one attribute timeout; callback
    /// completion and elapsed grace do not prove reply delivery or ACK.
    /// The wait is at most the timeout plus grace and averages half a second less.
    ///
    /// A fence that could not confirm every change was reported (macOS)
    /// needs no wait when nothing the client holds changed: each object it
    /// holds a label for is read again, and the wait follows only if one no
    /// longer matches what its label was issued for.
    #[cfg(test)]
    fn revalidate(&self) { self.revalidate_with_wait(); }

    /// Test-facing selective policy result through the same canonical engine.
    #[cfg(test)]
    fn revalidate_with_wait(&self) -> bool {
        let cancellation = crate::CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .expect("explicit unlimited native cache phase");
        self.revalidate_with_meter(&work.native_mount_work())
            .expect("explicit unlimited native cache barrier")
    }

    fn revalidate_with_meter(
        &self, native: &super::NativeMountMeterWork,
    ) -> Result<bool, NativeMountError> {
        native.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)?;
        let unconfirmed = self.around.unconfirmed.load(Ordering::Acquire);
        if self.around.verified.load(Ordering::Acquire) < unconfirmed
            && !self.verify_labels_with_meter(native)? {
            self.around.made.fetch_add(1, Ordering::AcqRel);
        }
        self.around.verified.fetch_max(unconfirmed, Ordering::AcqRel);
        let made = self.around.made.load(Ordering::Acquire);
        if self.around.settled.load(Ordering::Acquire) >= made { return Ok(false); }
        self.callbacks.drain_native(native)?;
        let materialization = native.native_materialization_work();
        let callback_grace_end = uptime_measured(&mut &materialization)? + CALLBACK_GRACE_INTERVAL;
        darwin_native_attempt(native, false)?;
        let timeout = u64::from(unsafe { acyclic_fs_darwin_mount_attribute_timeout() });
        let expired = Duration::from_secs(callback_grace_end.as_secs() + timeout);
        loop {
            let now = uptime_measured(&mut &materialization)?;
            if now >= expired { break; }
            darwin_native_attempt(native, false)?;
            std::thread::sleep((expired - now).min(Duration::from_millis(10)));
        }
        self.around.settled.fetch_max(made, Ordering::AcqRel);
        Ok(true)
    }

    /// Checks the same actual memo/source engine, preserving typed work and
    /// cancellation failures rather than treating denial as changed contents.
    fn verify_labels_with_meter(
        &self, native: &super::NativeMountMeterWork,
    ) -> Result<bool, NativeMountError> {
        let cleared = self.changes.cleared.load(Ordering::Acquire);
        let Some(issued) = self.changes.issued_with_meter(native)? else { return Ok(false); };
        for (file_id, observed) in issued.iter() {
            native.native_materialization_work().charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED,
            })?;
            match self.lookup_with_meter(&observed.path, native) {
                Ok(current) if current.node.file_id == *file_id && observed.matches(&current) => {}
                Ok(_) => return Ok(false),
                Err(error @ (MountSourceError::Work(_) | MountSourceError::Cancelled)) => return Err(error.into()),
                Err(_) => return Ok(false),
            }
        }
        Ok(self.changes.cleared.load(Ordering::Acquire) == cleared)
    }

    fn inode_measured(&self, file_id: FileId, work: &DarwinCallbackWork<'_>) -> Result<u64, i32> {
        let mut inodes = self.inodes.lock().map_err(|_| libc::EIO)?;
        if let Some(inode) = inodes.get_measured(&file_id, work)? {
            return Ok(*inode);
        }
        let inode = self.next_inode.fetch_add(1, Ordering::Relaxed);
        if inode <= ROOT_INODE || inode == u64::MAX {
            return Err(libc::EOVERFLOW);
        }
        inodes.insert_measured(file_id, inode, work)?;
        Ok(inode)
    }

    fn lookup_measured(&self, path: &MountPath, work: &DarwinCallbackWork<'_>) -> Result<MountLookup, i32> {
        // Sampled first: a change to anything the lookup reads records a
        // later position, so its result can never validate over the change.
        let stamp = self.cache_stamp_measured(work)?;
        if stamp.is_some() {
            let cache = self.lookups.lock().map_err(|_| libc::EIO)?;
            if let Some((cached, cached_stamp)) = cache.entries.get_measured(path, work)?.map(|(lookup, stamp, _)| (*lookup, *stamp))
                && self.unchanged_since_measured(
                    path,
                    cached.map(|lookup| lookup.node.file_id),
                    cached_stamp, work,
                )?
            {
                return cached.ok_or(libc::ENOENT);
            }
        }
        let lookup = self.source.lookup_with_meter(path, work.native).map_err(|error| work.source_error(error))?;
        if let Some(stamp) = stamp {
            self.remember_lookup_measured(path, lookup, stamp, work)?;
        }
        lookup.ok_or(libc::ENOENT)
    }

    fn remember_current_lookup_measured(
        &self,
        path: &MountPath,
        lookup: Option<MountLookup>, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
        let Some(stamp) = self.cache_stamp_measured(work)? else {
            return Ok(());
        };
        self.remember_lookup_measured(path, lookup, stamp, work)?;
        Ok(())
    }

    fn remember_lookup_measured(
        &self,
        path: &MountPath,
        lookup: Option<MountLookup>,
        stamp: CacheStamp, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
        let mut lookups = self.lookups.lock().map_err(|_| libc::EIO)?;
        if lookups.entries.len() >= MAXIMUM_LOOKUP_CACHE_ENTRIES {
            lookups.clear();
        }
        let path = work.clone_path(path)?;
        let (path, proof) = path.into_parts();
        if let Some((key, value)) = lookups.entries.remove_entry_measured(&path, work)? { drop(key); drop(value); }
        lookups.entries.insert_measured(path, (lookup, stamp, proof), work)?;
        Ok(())
    }

    /// Opens a handle on the regular file at `path`. The source file is
    /// bound on first use (see [`Self::bind`]).
    fn open_measured(&self, path: &MountPath, state: InitialHandleState, work: &DarwinCallbackWork<'_>) -> Result<u64, i32> {
        let written = match state {
            InitialHandleState::Clean => 0,
            InitialHandleState::Written => self.ledger.latest(),
        };
        // Sampled first, so a change after it cannot validate the lookup.
        let stamp = self.cache_stamp_measured(work)?;
        let lookup = self.lookup_measured(path, work)?;
        match lookup.node.kind {
            MountNodeKind::Regular => {}
            MountNodeKind::Directory => return Err(libc::EISDIR),
            _ => return Err(libc::EINVAL),
        }
        let handle = self.allocate_handle()?;
        let mut files = self.files.write().map_err(|_| libc::EIO)?;
        let observation = work.shared(|| Mutex::new(FileObservation { stamp, lookup }))?;
        let written = work.shared(|| AtomicU64::new(written))?;
        let file = FileHandle { file_id: lookup.node.file_id, file: None, observation, written };
        files.insert_measured(handle, file, work)?;
        Ok(handle)
    }

    /// The source file behind `handle`, opened at `path` on first use.
    ///
    /// A handle names one file identity. Until a callback needs its source
    /// file, `path` (the kernel's current name for the handle's object) still
    /// names that identity unless something outside this mount removed or
    /// replaced it, which fails the handle as a stale NFS handle; a removal
    /// or replacement through this mount detaches its file first (see [`Self::unlink`]).
    fn bind_measured(&self, path: &MountPath, handle: u64, work: &DarwinCallbackWork<'_>) -> Result<DarwinSourceFile, i32> {
        let file_id = {
            let files = self.files.read().map_err(|_| libc::EIO)?;
            let entry = files.get_measured(&handle, work)?.ok_or(libc::ESTALE)?;
            if let Some(file) = &entry.file {
                return Ok(file.clone());
            }
            entry.file_id
        };
        let file = work.retained_file(self.source.open_file_with_meter(path, work.native).map_err(|error| work.source_error(error))?)?;
        if file.lookup_with_meter(work.native).map_err(|error| work.source_error(error))?.node.file_id != file_id {
            return Err(libc::ESTALE);
        }
        let mut files = self.files.write().map_err(|_| libc::EIO)?;
        let entry = files.get_mut_measured(&handle, work)?.ok_or(libc::ESTALE)?;
        Ok(entry.file.get_or_insert(file).clone())
    }

    /// Runs `unlink`, which removes `path`, the name `lookup` resolved.
    /// When that is the last name of a regular file with open handles, the
    /// file is detached first and, once the name is gone, those handles use
    /// it, so an unlinked open file stays readable and writable.
    fn unlink_measured(
        &self,
        path: &MountPath,
        lookup: MountLookup,
        unlink: impl FnOnce() -> Result<(), MountSourceError>, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
        let file_id = lookup.node.file_id;
        let has_open = if lookup.node.kind == MountNodeKind::Regular && lookup.node.link_count == 1 {
            let files = self.files.read().map_err(|_| libc::EIO)?;
            let mut found = false;
            for entry in files.values() {
                work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                if entry.file_id == file_id { found = true; break; }
            }
            found
        } else { false };
        self.admit_write()?;
        let result = (|| {
            let detached = if has_open {
                Some(work.retained_file(self.source.detach_file_with_meter(path, work.native)
                    .map_err(|error| work.source_error(error))?)?)
            } else { None };
            unlink().map_err(|error| work.source_error(error))?;
            Ok(detached)
        })();
        self.ledger.record();
        let detached = result?;
        if let Some(detached) = detached {
            let mut files = self.files.write().map_err(|_| libc::EIO)?;
            for entry in files.values_mut() {
                work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                if entry.file_id == file_id {
                    let replacement = Some(detached.clone());
                    let old = work.copying(crate::WorkCounters { bytes_copied: std::mem::size_of::<Option<DarwinSourceFile>>() as u64, ..crate::WorkCounters::UNCHARGED }, || std::mem::replace(&mut entry.file, replacement))?;
                    drop(old);
                }
            }
        }
        Ok(())
    }

    /// The open handle's file and written marker, or a path-bound file
    /// without a marker when `handle` is zero.
    fn file_target_measured(&self, path: &MountPath, handle: u64, work: &DarwinCallbackWork<'_>) -> Result<FileTarget, i32> {
        if handle == 0 {
            let file = work.retained_file(self.source.open_file_with_meter(path, work.native).map_err(|error| work.source_error(error))?)?;
            return Ok(FileTarget {
                file,
                written: None,
            });
        }
        let file = self.bind_measured(path, handle, work)?;
        let files = self.files.read().map_err(|_| libc::EIO)?;
        let entry = files.get_measured(&handle, work)?.ok_or(libc::ESTALE)?;
        Ok(FileTarget {
            file,
            written: Some(entry.written.clone()),
        })
    }

    fn file_or_open_measured(&self, path: &MountPath, handle: u64, work: &DarwinCallbackWork<'_>) -> Result<DarwinSourceFile, i32> {
        self.file_target_measured(path, handle, work).map(|target| target.file)
    }
    /// Runs one mutating callback and records its completion before the
    /// reply, including a failed attempt that may have applied partially, so
    /// a later sync covers it and the change attribute advances.
    fn mutate_measured<T>(&self, mutation: impl FnOnce() -> Result<T, MountSourceError>, work: &DarwinCallbackWork<'_>) -> Result<T, i32> {
        self.admit_write()?;
        let result = mutation().map_err(|error| work.source_error(error));
        self.ledger.record();
        result
    }

    /// Mutates one regular file through `handle` (or a path-bound file when
    /// it is zero) and marks that handle written, so its close publishes
    /// when the source publishes on close.
    fn mutate_file_measured<T>(
        &self,
        path: &MountPath,
        handle: u64,
        mutation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>, work: &DarwinCallbackWork<'_>) -> Result<T, i32> {
        self.admit_write()?;
        let target = self.file_target_measured(path, handle, work)?;
        let result = mutation(&*target.file).map_err(|error| work.source_error(error));
        let sequence = self.ledger.record();
        if let Some(written) = target.written {
            written.fetch_max(sequence, Ordering::AcqRel);
        }
        result
    }

    /// The attributes of `handle`'s file, or of the file at `path` when
    /// `handle` is zero.
    fn lookup_handle_measured(&self, path: &MountPath, handle: u64, work: &DarwinCallbackWork<'_>) -> Result<MountLookup, i32> {
        if handle == 0 {
            return self.lookup_measured(path, work);
        }
        let observation = {
            let files = self.files.read().map_err(|_| libc::EIO)?;
            files.get_measured(&handle, work)?.ok_or(libc::ESTALE)?.observation.clone()
        };
        {
            let observation = observation.lock().map_err(|_| libc::EIO)?;
            if let Some(stamp) = observation.stamp
                && self.unchanged_since_measured(path, Some(observation.lookup.node.file_id), stamp, work)?
            {
                return Ok(observation.lookup);
            }
        }
        let stamp = self.cache_stamp_measured(work)?;
        let lookup = self.bind_measured(path, handle, work)?.lookup_with_meter(work.native).map_err(|error| work.source_error(error))?;
        if stamp.is_some() {
            let mut observation = observation.lock().map_err(|_| libc::EIO)?;
            observation.stamp = stamp;
            observation.lookup = lookup;
        }
        Ok(lookup)
    }

    fn attributes_measured(&self, path: &MountPath, handle: u64, work: &DarwinCallbackWork<'_>) -> Result<NativeStat, i32> {
        // Sampled before observing: a change after it cannot keep the label.
        let stamp = self.cache_stamp_measured(work)?;
        let lookup = self.lookup_handle_measured(path, handle, work)?;
        self.attributes_from_lookup_measured(path, lookup, stamp, work)
    }

    /// Native attributes of `lookup`, observed at `path` after `stamp`.
    fn attributes_from_lookup_measured(
        &self,
        path: &MountPath,
        lookup: MountLookup,
        stamp: Option<CacheStamp>, work: &DarwinCallbackWork<'_>) -> Result<NativeStat, i32> {
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
            inode: self.inode_measured(node.file_id, work)?,
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
            change: self.changes.label_measured(
                node.file_id, stamp,
                self.source.binding_epoch_with_meter(work.native).map_err(|error| work.source_error(error))?,
                path, lookup,
                |stamp| self.unchanged_since_measured(path, Some(node.file_id), stamp, work),
                |stamp| self.reported_unchanged_since_measured(path, Some(node.file_id), stamp, work),
                work,
            )?,
        })
    }

    /// Whether the node at `path` may carry named attributes. A node's
    /// metadata records its named attributes; without that record it has
    /// none, which every source reads the same way, so the probes the macOS
    /// client makes on each new file (provenance, Finder info) need no
    /// source call.
    fn may_have_named_attributes_measured(&self, path: &MountPath, work: &DarwinCallbackWork<'_>) -> Result<bool, i32> {
        Ok(self.lookup_measured(path, work)?.metadata.named_attributes != MetadataField::Unavailable)
    }

    fn admit_write(&self) -> Result<(), i32> {
        self.writable.then_some(()).ok_or(libc::EROFS)
    }

    fn mutate_metadata_measured(
        &self,
        path: &MountPath,
        handle: u64,
        mutate: impl FnOnce(&mut FileMetadata) -> Result<(), i32>, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
        let current = self.lookup_handle_measured(path, handle, work)?;
        let mut metadata = current.metadata;
        mutate(&mut metadata)?;
        // Metadata of any node kind: only a handle names a regular file.
        if handle == 0 {
            self.mutate_measured(|| self.source.set_attributes_with_meter(path, metadata, None, work.native), work)
        } else {
            self.mutate_file_measured(path, handle, |file| file.set_attributes_with_meter(metadata, None, work.native), work)
        }
    }

    /// Publishes this handle's mutations when the source publishes on close.
    fn flush_on_close_measured(&self, handle: u64, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
        if handle == 0 {
            // Named-attribute opens carry no handle; their writes are
            // recorded mutations that the next sync covers.
            return Ok(());
        }
        let written = self
            .files
            .read()
            .map_err(|_| libc::EIO)?
            .get_measured(&handle, work)?
            .ok_or(libc::ESTALE)?
            .written
            .load(Ordering::Acquire);
        if written == 0 || !self.source.flush_on_handle_close() {
            return Ok(());
        }
        self.ledger.flush_through_measured(written, self.source.as_ref(), work)
    }

    /// Publishes every mutation this mount has acknowledged.
    fn sync_measured(&self, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
        self.ledger
            .flush_through_measured(self.ledger.latest(), self.source.as_ref(), work)
    }
}

struct FileTarget {
    file: DarwinSourceFile,
    written: Option<DarwinShared<AtomicU64>>,
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
    _allocation: Option<crate::NativeAllocation>,
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

/// Caller-created argv and native destination buffers outlive the blocking
/// SDK loop. Their known payload is claimed, not opaque SDK allocator headers,
/// kernel allocations, or resident thread stacks.
struct DarwinStartupBuffers {
    destination: Option<CString>,
    arguments: Vec<CString>,
    pointers: Vec<usize>,
    allocation: crate::NativeAllocation,
}

impl DarwinStartupBuffers {
    #[cfg(test)]
    fn new(
        destination: &Path, writable: bool, named_attributes: bool, work: &crate::NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        Self::new_with_meter(destination, writable, named_attributes, &work.native_mount_work())
    }

    fn new_with_meter(
        destination: &Path, writable: bool, named_attributes: bool, native: &super::NativeMountMeterWork,
    ) -> Result<Self, NativeMountError> {
        let work = native.native_materialization_work();
        let allocation = work.claim(0)?;
        let mut owned = Self {
            destination: None, arguments: Vec::new(), pointers: Vec::new(), allocation,
        };
        let name = destination.as_os_str().as_bytes();
        owned.destination = Some(owned.string(name, &work)?);
        let options = [Some(c"nobrowse"), (!writable).then_some(c"ro"),
            named_attributes.then_some(c"namedattr")];
        let count = 1 + 2 * (1 + usize::from(!writable) + usize::from(named_attributes));
        let bytes = (count * std::mem::size_of::<CString>()) as u64;
        owned.allocation.grow(bytes)?;
        owned.arguments.try_reserve_exact(count)?;
        let argument = owned.string(c"acyclic-fs".to_bytes(), &work)?;
        owned.arguments.push(argument);
        for option in options.into_iter().flatten() {
            work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            let flag = owned.string(c"-o".to_bytes(), &work)?;
            owned.arguments.push(flag);
            let argument = owned.string(option.to_bytes(), &work)?;
            owned.arguments.push(argument);
        }
        let bytes = (count * std::mem::size_of::<usize>()) as u64;
        owned.allocation.grow(bytes)?;
        owned.pointers.try_reserve_exact(count)?;
        for argument in &owned.arguments {
            work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            let copied = crate::WorkCounters { bytes_copied: std::mem::size_of::<usize>() as u64,
                ..crate::WorkCounters::UNCHARGED };
            work.admit(copied)?;
            owned.pointers.push(argument.as_ptr() as usize);
            work.record_admitted(copied)?;
        }
        Ok(owned)
    }

    fn string(&mut self, value: &[u8], work: &crate::NativeMaterializationWork) -> Result<CString, NativeMountError> {
        for byte in value {
            work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            if *byte == 0 { return Err(std::io::Error::from_raw_os_error(libc::EINVAL).into()); }
        }
        let bytes = value.len().checked_add(1).ok_or(crate::WorkError::Overflow)? as u64;
        let copied = crate::WorkCounters { bytes_copied: value.len() as u64, ..crate::WorkCounters::UNCHARGED };
        work.admit(copied)?;
        self.allocation.grow(bytes)?;
        let mut vector = Vec::with_capacity(bytes as usize);
        vector.extend_from_slice(value);
        vector.push(0);
        // SAFETY: every input byte was validated above and one NUL was appended.
        let string = unsafe { CString::from_vec_with_nul_unchecked(vector) };
        work.record_admitted(copied)?;
        Ok(string)
    }
}

impl Drop for DarwinStartupBuffers {
    fn drop(&mut self) {
        drop(self.destination.take());
        drop(std::mem::take(&mut self.arguments));
        drop(std::mem::take(&mut self.pointers));
    }
}

/// One process-owned high-level Darwin mount session.
pub(super) struct DarwinMountSession {
    resources: Option<Arc<DriverSessionResources>>,
    destination: PathBuf,
    _destination_allocation: Option<crate::NativeAllocation>,
    thread: Option<JoinHandle<c_int>>,
    events: Arc<LoopEvents>,
    loop_finished_before_teardown: Option<bool>,
    loop_result: Option<MountLoopResult>,
    // Existing SDK panic allocation, retained even if later free is denied.
    loop_panic: Option<Mutex<Box<dyn std::any::Any + Send>>>,
    teardown_complete: bool,
    completed_callback_work: crate::WorkCounters,
    retired_callback_failure: Mutex<Option<super::NativeMountCallbackFailure>>,
    native_root_identity: Option<crate::NativeRootIdentity>,
    unmount: UnmountState,
}

#[derive(Debug, Clone, Copy)]
enum MountLoopResult {
    Exited(c_int),
    Panicked,
    TimedOut,
}

/// Allocation-free evidence from actual Darwin teardown controls. Formatting
/// occurs only when a caller displays the retained cause, never at a denied leaf.
#[derive(Debug)]
pub enum DarwinTeardownFailure {
    /// The provider had already exited while its mount still required detach.
    LoopExitedBeforeDetach(i32),
    /// Actual nonzero provider status after teardown began.
    LoopExitedAfterDetach(i32),
    /// The actual provider thread join reported panic.
    LoopPanicked,
    /// Actual callbacks did not settle within the existing liveness bound.
    LoopExitTimedOut,
    /// Actual diskutil process status.
    DiskutilExit(std::process::ExitStatus),
    /// The same retained diskutil process exceeded its original deadline.
    DiskutilTimedOut,
    /// A real mount-table probe still observed the namespace at the bound.
    MountStillVisible,
    /// Required actual teardown evidence has not been observed.
    MissingTeardownState,
}

impl std::fmt::Display for DarwinTeardownFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LoopExitedBeforeDetach(0) => formatter.write_str("Darwin mount loop exited before teardown began"),
            Self::LoopExitedBeforeDetach(status) | Self::LoopExitedAfterDetach(status) =>
                write!(formatter, "Darwin mount loop exited with status {status}"),
            Self::LoopPanicked => formatter.write_str("Darwin mount loop thread panicked"),
            Self::LoopExitTimedOut => formatter.write_str("Darwin mount callbacks have not finished; destination remains fenced"),
            Self::DiskutilExit(status) => write!(formatter, "diskutil unmount failed with status {status}"),
            Self::DiskutilTimedOut => formatter.write_str("diskutil unmount exceeded its 4-second bound"),
            Self::MountStillVisible => formatter.write_str("Darwin mount remained visible after diskutil unmount"),
            Self::MissingTeardownState => formatter.write_str("Darwin mount teardown evidence is absent"),
        }
    }
}

impl std::error::Error for DarwinTeardownFailure {}

/// Both the session and mount loop own these pointers. A timed-out loop may
/// outlive the session, but neither pointer is freed until that loop exits.
struct DriverSessionResources {
    source_context: usize,
    driver_session: usize,
    startup: Option<DarwinStartupBuffers>,
    context_custody: Option<crate::demand::NativeMountAllocationCustody>,
    _allocation: Option<crate::NativeAllocation>,
}

impl DriverSessionResources {
    fn context(&self) -> &DarwinMountContext {
        // SAFETY: the context outlives every owner of these resources.
        unsafe { &*(self.source_context as *const DarwinMountContext) }
    }

    fn context_owner(&self) -> Arc<DarwinMountContext> {
        let pointer = self.source_context as *const DarwinMountContext;
        // SAFETY: this resource still owns the exact Arc::into_raw strong
        // reference, so cloning it cannot resurrect a physically dropped Arc.
        unsafe { Arc::increment_strong_count(pointer); Arc::from_raw(pointer) }
    }
}

impl Drop for DriverSessionResources {
    fn drop(&mut self) {
        // SAFETY: the last owner has released these exact allocations, and
        // the mount loop retains an owner through every native callback.
        unsafe {
            if self.driver_session != 0 {
                acyclic_fs_darwin_mount_session_free(self.driver_session as *mut c_void);
            }
            if self.source_context != 0 {
                drop(Arc::from_raw(self.source_context as *const DarwinMountContext));
            }
        }
    }
}

struct DarwinLoopOwner {
    resources: Arc<DriverSessionResources>,
    events: ExitReport,
    original: super::NativeMountMeterWork,
    allocation: Option<crate::NativeAllocation>,
}

impl DarwinLoopOwner {
    fn run(self) -> c_int {
        let admission = darwin_native_attempt(&self.original, true).map_err(|cause| {
            super::NativeMountCallbackFailure { cause, work: self.original.meter().lock().work }
        });
        if let Err(failure) = admission {
            self.resources.context().handler_failed.store(true, Ordering::Release);
            *self.resources.context().startup_failure.lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(failure);
            return -libc::EIO;
        }
        let buffers = self.resources.startup.as_ref().expect("native argv retained");
        // The original queued owner retains all pointers and full policy until
        // this blocking SDK loop reaches its actual terminal.
        unsafe {
            acyclic_fs_darwin_mount_run(
                self.resources.driver_session as *mut c_void,
                buffers.arguments.len() as c_int,
                buffers.pointers.as_ptr().cast::<*const c_char>(),
                buffers.destination.as_ref().expect("native destination retained").as_ptr(),
                self.resources.source_context, report_mounted,
                Arc::as_ptr(&self.events.0).cast_mut().cast(),
            )
        }
    }
}

fn darwin_start_stat<W: crate::native_host::NativeHostWork>(
    path: &CStr, work: &mut W,
) -> Result<libc::stat, W::Error> {
    loop {
        let read = crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED };
        work.admit(read)?;
        work.record(read)?;
        let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
        if unsafe { libc::stat(path.as_ptr(), metadata.as_mut_ptr()) } == 0 {
            return Ok(unsafe { metadata.assume_init() });
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted { return Err(error.into()); }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UnmountEvidence {
    AlreadyUnmounted,
    Detached,
}

impl DarwinMountSession {
    /// Ordinary startup establishes global I/O services outside any measured
    /// request, then delegates to the same per-mount lifecycle engine.
    pub(super) fn start(
        request: &NativeMountRequest, source: Arc<dyn MountFilesystem>,
    ) -> Result<Self, DriverStartFailure> {
        acyclic_native_runtime::initialize_native_io_services().map_err(NativeMountError::from)?;
        let cancellation = crate::CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| NativeMountError::from(cause))?;
        Self::start_with_work(request, source, &work)
    }

    /// Every known native argument, callback owner and per-mount thread is
    /// admitted before construction. A partially started native session is
    /// returned with its actual resources, never forgotten or retried with
    /// an unbounded replacement invocation.
    pub(super) fn start_with_work(
        request: &NativeMountRequest, source: Arc<dyn MountFilesystem>, work: &crate::NativeMaterializationWork,
    ) -> Result<Self, DriverStartFailure> {
        Self::start_with_meter(request, source, &work.native_mount_work())
    }

    pub(super) fn start_with_meter(
        request: &NativeMountRequest, source: Arc<dyn MountFilesystem>,
        source_work: &super::NativeMountMeterWork,
    ) -> Result<Self, DriverStartFailure> {
            let meter = source_work.meter();
            darwin_native_source_original(source.as_ref(), Some(source_work)).map_err(NativeMountError::from)?;
            source_work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)
                .map_err(NativeMountError::from)?;
            static STARTUP: std::sync::LazyLock<Mutex<()>> = std::sync::LazyLock::new(|| Mutex::new(()));
            let _startup = STARTUP.lock().map_err(|_| NativeMountError::Lifecycle("Darwin mount startup lock is poisoned"))?;
            let root = source.lookup_with_meter(&MountPath::root(), source_work)
                .map_err(source_error)?
                .ok_or(NativeMountError::Lifecycle("volume root is absent"))?;
            if root.node.kind != MountNodeKind::Directory {
                return Err(NativeMountError::Lifecycle("volume root is not a directory").into());
            }
            let metadata = {
                let materialization = source_work.native_materialization_work();
                let mut host = &materialization;
                let metadata = crate::native_host::unix_name_with_work(
                    request.destination.as_os_str(), &mut host, darwin_start_stat,
                ).map_err(NativeMountError::from)?;
                let parent = request.destination.parent().ok_or(crate::DemandError::InvalidRequest)
                    .map_err(NativeMountError::from)?;
                crate::native_host::unix_name_with_work(parent.as_os_str(), &mut host, darwin_start_stat)
                    .map_err(NativeMountError::from)?;
                metadata
            };
            let owner = {
                let custody = crate::demand::NativeMountAllocationCustody::new_with_meter(source_work)
                    .map_err(NativeMountError::from)?;
                let materialization = source_work.native_materialization_work();
                let bytes = std::mem::size_of::<(FileId, u64)>() as u64;
                let mut root_allocation = materialization.claim(bytes).map_err(NativeMountError::from)?;
                let mut root_entries = Vec::new();
                root_entries.try_reserve_exact(1).map_err(NativeMountError::from)?;
                root_allocation.reconcile_capacity(
                    (root_entries.capacity() * std::mem::size_of::<(FileId, u64)>()) as u64,
                ).map_err(NativeMountError::from)?;
                let copied = crate::WorkCounters { bytes_copied: bytes, ..crate::WorkCounters::UNCHARGED };
                materialization.admit(copied).map_err(NativeMountError::from)?;
                root_entries.push((root.node.file_id, ROOT_INODE));
                materialization.record_admitted(copied).map_err(NativeMountError::from)?;
                let (budget, cancellation) = { let work = meter.lock(); (work.budget, work.cancellation().clone()) };
                let mut context = DarwinMountContext::new_owner(
                    source, request.writable, metadata.st_uid, metadata.st_gid, root_entries,
                    budget, &cancellation,
                )?;
                context.inodes.get_mut().unwrap_or_else(PoisonError::into_inner)
                    .storage.allocation = Some(root_allocation);
                context.allocation_custody = Some(custody.clone());
                context.native_allocation = Some(materialization.claim(
                    std::mem::size_of::<DarwinMountContext>() as u64,
                ).map_err(NativeMountError::from)?);
                DarwinContextOwner { context: Arc::new(context), custody }
            };
            let _startup_meter = DarwinStartupMeterScope::new(&owner, source_work)?;
            let observer: Weak<DarwinMountContext> = Arc::downgrade(&owner.context);
            owner.context.source.observe_view_with_meter(observer, source_work).map_err(source_error)?;
            let supports_named_attributes = owner.context.source.supports_posix_named_attributes();
            let buffers = DarwinStartupBuffers::new_with_meter(
                &request.destination, request.writable, supports_named_attributes, source_work,
            )?;
            let materialization = source_work.native_materialization_work();
            let bytes = request.destination.as_os_str().as_bytes().len() as u64;
            let copied = crate::WorkCounters { bytes_copied: bytes, ..crate::WorkCounters::UNCHARGED };
            materialization.admit(copied).map_err(NativeMountError::from)?;
            let destination_allocation = materialization.claim(bytes).map_err(NativeMountError::from)?;
            let destination = request.destination.clone();
            materialization.record_admitted(copied).map_err(NativeMountError::from)?;
            let events_allocation = materialization.claim(std::mem::size_of::<LoopEvents>() as u64)
                .map_err(NativeMountError::from)?;
            let events = Arc::new(LoopEvents { _allocation: Some(events_allocation), ..LoopEvents::default() });
            let resources_allocation = materialization.claim(std::mem::size_of::<DriverSessionResources>() as u64)
                .map_err(NativeMountError::from)?;
            let resources = Arc::new(DriverSessionResources {
                source_context: Arc::into_raw(owner.context) as usize, driver_session: 0,
                startup: Some(buffers), context_custody: Some(owner.custody),
                _allocation: Some(resources_allocation),
            });
            let mut started = Self {
                resources: Some(resources),
                destination, _destination_allocation: Some(destination_allocation),
                thread: None,
                events,
                loop_finished_before_teardown: None, loop_result: None,
                loop_panic: None,
                teardown_complete: true, unmount: UnmountState::default(),
                completed_callback_work: crate::WorkCounters::UNCHARGED,
                retired_callback_failure: Mutex::new(None),
                native_root_identity: None,
            };
            // Before this SDK leaf the only resources are known Rust buffers.
            darwin_native_attempt(source_work, true)?;
            let native = unsafe { acyclic_fs_darwin_mount_session_new() } as usize;
            if native == 0 {
                return Err(NativeMountError::from(std::io::Error::from_raw_os_error(libc::ENOMEM)).into());
            }
            Arc::get_mut(started.resources.as_mut().expect("new resources retained"))
                .expect("session has not yet started a loop").driver_session = native;
            started.teardown_complete = false;
            let result = started.spawn_loop(source_work).and_then(|()| started.await_mount_measured(source_work));
            match result {
                Ok(()) => Ok(started),
                Err(error) => Err(DriverStartFailure::retaining_driver(
                    error, super::DriverSession::DarwinMount(started),
                )),
            }
    }

    fn spawn_loop(&mut self, native: &super::NativeMountMeterWork) -> Result<(), NativeMountError> {
        let materialization = native.native_materialization_work();
        let resources = Arc::clone(self.resources.as_ref().expect("native resources retained"));
        let events = ExitReport(Arc::clone(&self.events));
        let mut job = DarwinLoopOwner { resources, events, original: native.clone(), allocation: None };
        let name = "acyclic-fs-darwin-nfs";
        let copied = crate::WorkCounters { bytes_copied: name.len() as u64, ..crate::WorkCounters::UNCHARGED };
        materialization.admit(copied)?;
        let payload = std::mem::size_of_val(&job) as u64;
        job.allocation = Some(materialization.claim(payload.checked_add(name.len() as u64)
            .ok_or(crate::WorkError::Overflow)?)?);
        let name = name.to_owned();
        materialization.record_admitted(copied)?;
        darwin_native_attempt(native, true)?;
        self.thread = Some(std::thread::Builder::new().name(name).spawn(move || job.run())?);
        Ok(())
    }

    fn await_mount_measured(&mut self, native: &super::NativeMountMeterWork) -> Result<(), NativeMountError> {
        let meter = native.meter();
        let deadline = Instant::now() + MOUNT_VISIBILITY_TIMEOUT;
        let reported = loop {
            native.native_materialization_work().charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED,
            })?;
            let state = self.events.current();
            if state.mounted || state.exited { break state; }
            meter.lock().check().map_err(NativeMountError::from)?;
            if Instant::now() >= deadline {
                return Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into());
            }
            darwin_native_attempt(native, false)?;
            self.events.wait_until(Duration::from_millis(10), |state| state.mounted || state.exited);
        };
        if let Some(error) = self.resources()?.context().startup_failure.lock()
            .unwrap_or_else(PoisonError::into_inner).take() {
            return Err(error.cause);
        }
        if !reported.mounted {
            self.loop_finished_before_teardown = Some(true);
            self.loop_result = Some(with_detach_meter(
                native,
                |detach| self.finish_thread_with_detach_meter(MOUNT_LOOP_EXIT_TIMEOUT, detach),
            )?);
            return Err(std::io::Error::from_raw_os_error(libc::EIO).into());
        }
        let materialization = native.native_materialization_work();
        if !is_mounted_measured(&self.destination, &mut &materialization)? {
            return Err(std::io::Error::from_raw_os_error(libc::EIO).into());
        }
        // Opening the NFS root can invoke measured source callbacks. Never hold
        // the work mutex across that real native call; reserve its actual close
        // first and record the observed open/close prefix on the same carrier.
        let cleanup = {
            let read = crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED };
            materialization.admit(read)?;
            let cleanup = materialization.reserve_cleanup_write()?;
            materialization.record_admitted(read)?;
            cleanup
        };
        let path = self.resources()?.startup.as_ref().expect("native inputs retained")
            .destination.as_ref().expect("native destination retained");
        let fd = unsafe { libc::open(path.as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC) };
        if fd < 0 {
            let error = std::io::Error::last_os_error();
            drop(cleanup);
            return Err(error.into());
        }
        use std::os::fd::{FromRawFd as _, IntoRawFd as _};
        // The existing successful privacy-open is also the authentic mounted
        // root witness. Reuse it instead of opening and probing a second root.
        let file = std::mem::ManuallyDrop::new(unsafe { std::fs::File::from_raw_fd(fd) });
        let mut host = &materialization;
        let identity = crate::native_host::file_identity_and_length_with_work(&file, &mut host);
        let (closed, recorded) = host.attempt_cleanup_write(cleanup, || {
            if unsafe { libc::close(fd) } == 0 { Ok(()) } else { Err(std::io::Error::last_os_error()) }
        });
        // The actual close above consumed the descriptor; never schedule a
        // hidden std::File drop/retry, even when its status was uncertain.
        let _ = std::mem::ManuallyDrop::into_inner(file).into_raw_fd();
        recorded.map_err(NativeMountError::from)?;
        let (identity, _) = identity?;
        closed?;
        self.native_root_identity = Some(identity);
        Ok(())
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
        resources.context().run_control(|native| {
            let work = DarwinCallbackWork::new(native);
            let result = (|| {
                let path = work.native_name_with_prefix(path, path.first() != Some(&b'/'))?;
                work.charge(crate::WorkCounters {
                    backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
                })?;
                // Reject continuation snapshots before this actual SDK attempt.
                resources.context().forget_view();
                // SAFETY: the borrowed resource owns the live SDK session and
                // the preadmitted native path remains owned through its return.
                let status = unsafe { acyclic_fs_darwin_mount_invalidate(
                    resources.driver_session as *mut c_void, path.as_ptr(),
                ) };
                if status == 0 { Ok(()) }
                else { Err(work.native_error(NativeMountError::Io(std::io::Error::from_raw_os_error(status)))) }
            })();
            work.finish(result)
        })
    }

    /// Revalidates through the same engine and unlimited explicit grants.
    pub(super) fn revalidate(&self) -> Result<(), NativeMountError> {
        let cancellation = crate::CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| NativeMountError::from(cause))?;
        self.revalidate_measured(&work)
    }

    /// Source fencing, each callback-generation probe, clock query and real
    /// expiry wait share the caller's original work and cancellation. A denied
    /// barrier retains its exact generation; no interrupted wait counts settled.
    pub(super) fn revalidate_measured(&self, work: &crate::NativeMaterializationWork) -> Result<(), NativeMountError> {
        self.revalidate_with_meter(&work.native_mount_work())
    }

    pub(super) fn revalidate_with_meter(
        &self, native: &super::NativeMountMeterWork,
    ) -> Result<(), NativeMountError> {
        native.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)?;
        let _active = self.begin_original_work(native)?;
        let context = &_active.context;
        context.source.fence_changes_with_meter(native).map_err(source_error)?;
        context.revalidate_with_meter(native).map(|_| ())
    }

    pub(super) fn begin_original_work(
        &self, native: &super::NativeMountMeterWork,
    ) -> Result<DarwinStartupMeterScope, NativeMountError> {
        native.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)?;
        let resources = self.resources()?;
        if resources.driver_session == 0 { return Err(NativeMountError::Lifecycle("session is stopped")); }
        DarwinStartupMeterScope::existing(resources.context_owner(), resources.context_custody.clone(), native)
    }

    /// Authentic mounted-root observation of this original driver epoch,
    /// without a fresh path probe or a claim that a transient fd stays open.
    pub(super) fn native_root_identity(&self) -> Option<crate::NativeRootIdentity> {
        if self.teardown_complete || self.unmount.detached { None } else { self.native_root_identity }
    }

    /// Moves the actual recorded callback packet; observing it does not clear
    /// the sticky failure evidence or revive an incomplete callback context.
    pub(super) fn take_callback_failure(&self) -> Option<super::NativeMountCallbackFailure> {
        if let Some(resources) = self.resources.as_deref() {
            let context = resources.context();
            context.startup_failure.lock().unwrap_or_else(PoisonError::into_inner).take()
                .or_else(|| context.handler_failure.lock().unwrap_or_else(PoisonError::into_inner).first_fatal.take())
        } else {
            self.retired_callback_failure.lock().unwrap_or_else(PoisonError::into_inner).take()
        }
    }

    /// Adds only real completed future request receipts. Active requests are
    /// already charged on their original lifecycle carrier and are not copied.
    pub(super) fn completed_callback_work(&self) -> crate::WorkCounters {
        self.resources.as_deref().map_or(self.completed_callback_work, |resources| {
            resources.context().handler_failure.lock().unwrap_or_else(PoisonError::into_inner)
                .completed_future_request_work
        })
    }

    fn resources(&self) -> Result<&DriverSessionResources, NativeMountError> {
        self.resources
            .as_deref()
            .ok_or(NativeMountError::Lifecycle("Darwin mount session has stopped"))
    }

    /// Uses the bounded engine with an unlimited canonical work ledger.
    pub(super) fn stop(&mut self) -> Result<(), NativeMountError> {
        if self.teardown_complete { return Ok(()); }
        acyclic_native_runtime::initialize_native_io_services().map_err(NativeMountError::from)?;
        let cancellation = crate::CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| NativeMountError::from(cause))?;
        self.stop_measured(&work)
    }

    /// Measures actual mount-table probes, native detach, process controls and
    /// loop teardown. A stopped observer never frees an accepted native path
    /// or releases the destination while a late detach could still affect it.
    pub(super) fn stop_measured(&mut self, work: &crate::NativeMaterializationWork) -> Result<(), NativeMountError> {
        if self.teardown_complete {
            return Ok(());
        }
        self.stop_with_meter(&work.native_mount_work())
    }

    /// Runs the same finite teardown producer as borrowed and ordinary stop.
    /// The caller's operative Arc remains canonical during SDK callbacks;
    /// native calls and waits never execute while its work mutex is held.
    pub(super) fn stop_with_meter(
        &mut self, native: &super::NativeMountMeterWork,
    ) -> Result<(), NativeMountError> {
        if self.teardown_complete { return Ok(()); }
        let _active = if self.resources.is_some() { Some(self.begin_original_work(native)?) } else { None };
        with_detach_meter(native, |meter| self.stop_with_detach_meter(meter, _active.as_ref()))
    }

    fn stop_with_detach_meter(
        &mut self, meter: &Arc<DetachReadMeter>, scope: Option<&DarwinStartupMeterScope>,
    ) -> Result<(), NativeMountError> {
        meter.with_native(|work| work.admit(crate::WorkCounters::UNCHARGED).map_err(NativeMountError::from))?;
        // Observe whether the provider loop failed independently before any
        // teardown action can make it exit. A detach closes the transport,
        // which ends the loop, so sampling after it races a successful detach
        // and misclassifies it as a pre-existing failure.
        if self.loop_finished_before_teardown.is_none() {
            if self.thread.is_some() {
                meter.with_native(|work| work.charge(crate::WorkCounters {
                    items_examined: 1, ..crate::WorkCounters::UNCHARGED,
                }).map_err(NativeMountError::from))?;
            }
            self.loop_finished_before_teardown =
                Some(self.thread.is_none() || self.events.current().exited);
        }
        let unmount = bounded_unmount_measured(&self.destination, &mut self.unmount, meter, scope);
        if matches!(&unmount, Err(NativeMountError::Work(_))
            | Err(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(_))))
            || meter.with_native(|work| work.admit(crate::WorkCounters::UNCHARGED).map_err(NativeMountError::from)).is_err() {
            return unmount.and_then(|_| meter.with_native(|work|
                work.admit(crate::WorkCounters::UNCHARGED).map_err(NativeMountError::from)));
        }
        if self
            .loop_result
            .as_ref()
            .is_none_or(|result| matches!(result, MountLoopResult::TimedOut))
        {
            if let Some(resources) = &self.resources {
                meter.attempt(true)?;
                // SAFETY: `resources` keeps the bridge session alive; interrupting it only
                // signals that session's loop.
                unsafe {
                    acyclic_fs_darwin_mount_interrupt(resources.driver_session as *mut c_void);
                }
            }
            let join = self.finish_thread_with_detach_meter(MOUNT_LOOP_EXIT_TIMEOUT, meter)?;
            self.loop_result = Some(join);
        }
        else if self.resources.is_some() {
            self.finish_thread_with_detach_meter(MOUNT_LOOP_EXIT_TIMEOUT, meter)?;
        }
        let unmount = unmount?;
        if let Some(task) = &mut self.unmount.direct {
            if poll_direct(task, meter)?.is_ready() { self.unmount.direct.take(); }
        }
        if self.unmount.direct.is_some() || self.unmount.diskutil.is_some() {
            return Err(NativeMountError::ControlPending);
        }
        let loop_finished_before_teardown = self.loop_finished_before_teardown.ok_or(
            NativeMountError::DarwinTeardown(DarwinTeardownFailure::MissingTeardownState),
        )?;
        let join = self.loop_result.as_ref().ok_or(
            NativeMountError::DarwinTeardown(DarwinTeardownFailure::MissingTeardownState),
        )?;
        if let Some(panic) = self.loop_panic.take() {
            return Err(NativeMountError::DriverPanicked {
                operation: "Darwin mount loop shutdown", panic,
            });
        }
        let result = classify_detached_loop(join, unmount, loop_finished_before_teardown);
        if result.is_ok() {
            self.teardown_complete = true;
        }
        result
    }

    /// Last actual mount-table absence evidence, not a new physical probe.
    /// This may coexist with retained callbacks or a still-running control;
    /// successful stop requires those owners to have settled separately.
    pub(super) fn namespace_detach_confirmed(&self) -> bool { self.unmount.detached }


    #[cfg(test)]
    fn finish_thread_within(&mut self, timeout: Duration) -> MountLoopResult {
        let cancellation = crate::CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .expect("explicit unlimited SDK finish phase");
        self.finish_thread_measured(timeout, &work).unwrap_or(MountLoopResult::TimedOut)
    }

    #[cfg(test)]
    fn finish_thread_measured(
        &mut self, timeout: Duration, work: &crate::NativeMaterializationWork,
    ) -> Result<MountLoopResult, NativeMountError> {
        with_detach_work(work, |meter| self.finish_thread_with_detach_meter(timeout, meter))
    }

    fn finish_thread_with_detach_meter(
        &mut self, timeout: Duration, meter: &Arc<DetachReadMeter>,
    ) -> Result<MountLoopResult, NativeMountError> {
        if self.thread.is_none() {
            if self.resources.is_some() {
                let cleanup = meter.with_native(|work| work.reserve_cleanup_write().map_err(NativeMountError::from))?;
                meter.with_host_work(|work| self.release_resources_measured(cleanup, work))?;
            }
            return Ok(self.loop_result.unwrap_or(MountLoopResult::Exited(0)));
        }
        let deadline = Instant::now() + timeout;
        loop {
            meter.with_native(|work| work.charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED,
            }).map_err(NativeMountError::from))?;
            if self.events.current().exited { break; }
            if Instant::now() >= deadline { return Ok(MountLoopResult::TimedOut); }
            meter.attempt(false)?;
            self.events.wait_until(Duration::from_millis(10), |state| state.exited);
        }
        let cleanup = meter.with_native(|work| {
            let read = crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED };
            work.admit(read)?;
            let cleanup = if self.resources.is_some() {
                Some(work.reserve_cleanup_write()?)
            } else { None };
            work.record_admitted(read)?;
            Ok(cleanup)
        })?;
        let thread = self.thread.take().expect("admitted mount loop remains owned");
        let status = match thread.join() {
            Ok(status) => MountLoopResult::Exited(status),
            Err(panic) => {
                self.loop_panic = Some(Mutex::new(panic));
                MountLoopResult::Panicked
            }
        };
        self.loop_result = Some(status);
        if let Some(cleanup) = cleanup {
            meter.with_host_work(|work| self.release_resources_measured(cleanup, work))?;
        }
        Ok(status)
    }

    /// Frees the real SDK session exactly once while its callback context is
    /// still owned, then moves the final receipts before physical context drop.
    fn release_resources_measured<W: crate::native_host::NativeHostWork>(
        &mut self, cleanup: W::CleanupWrite, work: &mut W,
    ) -> Result<(), NativeMountError> where NativeMountError: From<W::Error> {
        let Some(resources) = self.resources.as_mut().and_then(Arc::get_mut) else {
            work.cancel_cleanup_write(cleanup);
            return Err(NativeMountError::ControlPending);
        };
        let recorded = if resources.driver_session != 0 {
            let (_, recorded) = work.attempt_cleanup_write(cleanup, || {
                // SAFETY: the loop has joined, this is the sole resource owner,
                // and the context remains alive through final SDK callbacks.
                unsafe { acyclic_fs_darwin_mount_session_free(resources.driver_session as *mut c_void); }
                resources.driver_session = 0;
            });
            recorded
        } else {
            work.cancel_cleanup_write(cleanup);
            Ok(())
        };
        self.completed_callback_work = self.completed_callback_work();
        let failure = self.take_callback_failure();
        let retained = self.retired_callback_failure.get_mut().unwrap_or_else(PoisonError::into_inner);
        if retained.is_none() { *retained = failure; }
        drop(self.resources.take());
        recorded.map_err(NativeMountError::from)
    }
}

fn classify_detached_loop(
    join: &MountLoopResult, unmount: UnmountEvidence, loop_finished_before_teardown: bool,
) -> Result<(), NativeMountError> {
    let failure = match (join, unmount, loop_finished_before_teardown) {
        (MountLoopResult::Exited(0), _, false)
        | (MountLoopResult::Exited(0), UnmountEvidence::AlreadyUnmounted, true) => return Ok(()),
        (MountLoopResult::Exited(status), _, true) => DarwinTeardownFailure::LoopExitedBeforeDetach(*status),
        (MountLoopResult::Exited(status), _, false) => DarwinTeardownFailure::LoopExitedAfterDetach(*status),
        (MountLoopResult::TimedOut, _, _) => DarwinTeardownFailure::LoopExitTimedOut,
        (MountLoopResult::Panicked, _, _) => DarwinTeardownFailure::LoopPanicked,
    };
    Err(NativeMountError::DarwinTeardown(failure))
}

impl Drop for DarwinMountSession {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}


fn darwin_stop_work_error(error: crate::DemandError) -> NativeMountError {
    error.into()
}

/// Records an admitted SDK/native attempt at its boundary, including failures.
fn darwin_stop_admit<W: crate::native_host::NativeHostWork>(
    work: &mut W, write: bool,
) -> Result<(), NativeMountError> where NativeMountError: From<W::Error> {
    let delta = if write {
        crate::WorkCounters { backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED }
    } else {
        crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED }
    };
    work.admit(delta).map_err(NativeMountError::from)?;
    work.record(delta).map_err(NativeMountError::from)
}

fn darwin_native_attempt(native: &super::NativeMountMeterWork, write: bool) -> Result<(), NativeMountError> {
    native.native_materialization_work().charge(if write {
        crate::WorkCounters { backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED }
    } else {
        crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED }
    }).map_err(Into::into)
}

fn darwin_stop_io<T, W: crate::native_host::NativeHostWork>(
    work: &mut W, write: bool, call: impl FnOnce() -> std::io::Result<T>,
) -> Result<T, NativeMountError> where NativeMountError: From<W::Error> {
    darwin_stop_admit(work, write)?;
    call().map_err(NativeMountError::from)
}

/// The mount table is caller-owned, not getmntinfo's hidden reallocating array.
/// A full result retries the count/fill pair rather than trusting truncation.
fn is_mounted_measured<W: crate::native_host::NativeHostWork>(
    destination: &Path, work: &mut W,
) -> Result<bool, NativeMountError> where NativeMountError: From<W::Error> {
    let parent = destination.parent().ok_or(
        NativeMountError::Lifecycle("mount parent disappeared during teardown"),
    )?;
    let normalized = crate::native_host::unix_name_with_work(parent.as_os_str(), work, |name, work| {
        let delta = crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED };
        work.admit(delta)?;
        let cleanup = work.reserve_cleanup_write()?;
        work.record(delta)?;
        // SAFETY: name is terminated; the descriptor remains owned until its
        // separately reserved close, including cancellation and probe failure.
        let fd = unsafe { libc::open(name.as_ptr(), libc::O_EVTONLY | libc::O_CLOEXEC) };
        if fd < 0 {
            work.cancel_cleanup_write(cleanup);
            return Err(std::io::Error::last_os_error().into());
        }
        let result = (|| {
            work.admit(delta)?;
            work.record(delta)?;
            let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
            if unsafe { libc::fstat(fd, metadata.as_mut_ptr()) } != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            let size = libc::PATH_MAX as usize;
            let allocation = work.claim(size as u64)?;
            let mut path = Vec::<std::mem::MaybeUninit<u8>>::with_capacity(size);
            // The successful F_GETPATH call initializes its complete terminated
            // prefix, not the unused tail. Avoid zeroing that tail first.
            unsafe { path.set_len(size); }
            let result = (|| {
                work.admit(delta)?;
                work.record(delta)?;
                if unsafe { libc::fcntl(fd, libc::F_GETPATH, path.as_mut_ptr()) } < 0 {
                    return Err(std::io::Error::last_os_error().into());
                }
                let mut length = 0;
                for byte in &path {
                    let examined = crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED };
                    work.admit(examined)?;
                    work.record(examined)?;
                    // SAFETY: successful F_GETPATH writes every byte through
                    // its NUL terminator; no byte after that terminator is read.
                    if unsafe { byte.assume_init() } == 0 { break; }
                    length += 1;
                }
                if length == size { return Err(W::invalid_request()); }
                let name = destination.file_name().ok_or_else(W::invalid_request)?.as_bytes();
                let separator = usize::from(length != 0 && unsafe { path[length - 1].assume_init() } != b'/');
                let end = length.checked_add(separator).and_then(|length| length.checked_add(name.len()))
                    .ok_or(crate::WorkError::Overflow)?;
                if end > size { return Err(std::io::Error::from_raw_os_error(libc::ENAMETOOLONG).into()); }
                let copy = crate::WorkCounters { bytes_copied: name.len() as u64, ..crate::WorkCounters::UNCHARGED };
                work.admit(copy)?;
                if separator != 0 { path[length].write(b'/'); length += 1; }
                // SAFETY: end <= size, the destination is writable and the
                // separately borrowed filename cannot alias this new buffer.
                unsafe { ptr::copy_nonoverlapping(name.as_ptr(), path.as_mut_ptr().add(length).cast::<u8>(), name.len()); }
                work.record(copy)?;
                Ok(end)
            })();
            let end = match result {
                Ok(end) => end,
                Err(error) => {
                    drop(path);
                    work.release(allocation)?;
                    return Err(error);
                }
            };
            let mut path = std::mem::ManuallyDrop::new(path);
            // SAFETY: the prefix through end is initialized, u8 has the same
            // allocation layout as MaybeUninit<u8>, and ownership moves once.
            Ok((unsafe { Vec::from_raw_parts(path.as_mut_ptr().cast::<u8>(), end, path.capacity()) }, allocation))
        })();
        let (status, recorded) = work.attempt_cleanup_write(cleanup, || unsafe { libc::close(fd) });
        let close = recorded.and_then(|()| if status == 0 { Ok(()) } else { Err(std::io::Error::last_os_error().into()) });
        match (result, close) {
            (Ok(path), Ok(())) => Ok(path),
            (Ok(path), Err(error)) => {
                drop(path.0);
                work.release(path.1)?;
                Err(error)
            }
            (Err(error), _) => Err(error),
        }
    }).map_err(NativeMountError::from)?;
    let result = (|| {
        loop {
            let count = darwin_stop_io(work, false, || {
                let count = unsafe { libc::getfsstat(ptr::null_mut(), 0, libc::MNT_NOWAIT) };
                if count < 0 { Err(std::io::Error::last_os_error()) } else { Ok(count as usize) }
            })?;
            let capacity = count.checked_add(1).ok_or(crate::WorkError::Overflow)?;
            let bytes = capacity.checked_mul(std::mem::size_of::<libc::statfs>())
                .ok_or(crate::WorkError::Overflow)?;
            let native_bytes = c_int::try_from(bytes).map_err(|_| crate::WorkError::Overflow)?;
            let allocation = work.claim(bytes as u64).map_err(NativeMountError::from)?;
            let mut mounts = Vec::<std::mem::MaybeUninit<libc::statfs>>::with_capacity(capacity);
            let result = (|| {
                let count = darwin_stop_io(work, false, || {
                    let count = unsafe { libc::getfsstat(mounts.as_mut_ptr().cast(), native_bytes, libc::MNT_NOWAIT) };
                    if count < 0 { Err(std::io::Error::last_os_error()) } else { Ok(count as usize) }
                })?;
                if count >= capacity { return Ok(None); }
                // SAFETY: getfsstat initialized exactly these returned entries.
                unsafe { mounts.set_len(count); }
                for mount in &mounts {
                    let examined = crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED };
                    work.admit(examined).map_err(NativeMountError::from)?;
                    work.record(examined).map_err(NativeMountError::from)?;
                    let mount = unsafe { mount.assume_init_ref() };
                    let name = unsafe { CStr::from_ptr(mount.f_mntonname.as_ptr()) }.to_bytes();
                    if name == destination.as_os_str().as_bytes() || name == normalized.0 {
                        return Ok(Some(true));
                    }
                }
                Ok(Some(false))
            })();
            drop(mounts);
            work.release(allocation).map_err(NativeMountError::from)?;
            match result? { Some(mounted) => return Ok(mounted), None => {} }
        }
    })();
    drop(normalized.0);
    work.release(normalized.1).map_err(NativeMountError::from)?;
    result
}


#[derive(Default)]
struct UnmountState {
    direct: Option<DirectUnmountTask>,
    diskutil: Option<DiskutilProcess>,
    detached: bool,
}

/// One original work loan and its original physical buffer-retirement proof.
/// No second ledger exists; cancellation-free release remains valid after seal.
pub(crate) struct DetachReadMeter {
    work: Mutex<Option<super::NativeMountMeterWork>>,
    custody: crate::demand::NativeMountAllocationCustody,
}

/// Actual direct task plus its existing shared carrier. Field order drops the
/// native task and carrier Arc before the last outside allocation-custody alias.
struct DirectUnmountTask {
    task: acyclic_native_runtime::BlockingIoTask<std::io::Result<c_int>>,
    _meter: Arc<DetachReadMeter>,
    original: super::NativeMountMeterWork,
    _custody: super::NativeMountAllocationCustody,
}

struct DetachLoan<'a>(&'a DetachReadMeter);

impl Drop for DetachLoan<'_> {
    fn drop(&mut self) { self.0.seal(); }
}

impl DetachReadMeter {
    fn attempt(&self, write: bool) -> Result<(), NativeMountError> {
        let native = self.work.lock().unwrap_or_else(PoisonError::into_inner)
            .as_ref().cloned().ok_or(crate::DemandError::Cancelled)?;
        darwin_native_attempt(&native, write)
    }
    fn with_native<T>(
        &self, call: impl FnOnce(&crate::NativeMaterializationWork) -> Result<T, NativeMountError>,
    ) -> Result<T, NativeMountError> {
        let native = self.work.lock().unwrap_or_else(PoisonError::into_inner)
            .as_ref().cloned().ok_or(crate::DemandError::Cancelled)?;
        call(&native.native_materialization_work())
    }

    fn with_meter<T>(
        &self, call: impl FnOnce(&dyn acyclic_native_runtime::NativeReadMeter) -> std::io::Result<T>,
    ) -> std::io::Result<T> {
        let held = self.work.lock().unwrap_or_else(PoisonError::into_inner);
        let original = held.as_ref().cloned().ok_or(std::io::ErrorKind::Interrupted)?;
        drop(held);
        call(original.native_materialization_work().runtime_meter())
    }

    fn native_error(&self, error: std::io::Error) -> crate::DemandError {
        let original = self.work.lock().unwrap_or_else(PoisonError::into_inner)
            .as_ref().cloned();
        original.and_then(|meter| meter.meter().take_native_io_failure())
            .unwrap_or_else(|| native_demand_error(error))
    }

    fn with_host_work<T>(
        &self, call: impl FnOnce(&mut &crate::NativeMaterializationWork) -> Result<T, NativeMountError>,
    ) -> Result<T, NativeMountError> {
        let original = self.work.lock().unwrap_or_else(PoisonError::into_inner)
            .as_ref().cloned().ok_or(crate::DemandError::Cancelled)?;
        // The loan-state and original-work mutexes are both free while native
        // I/O runs or invokes a source callback on this same original Arc.
        let materialization = original.native_materialization_work();
        call(&mut &materialization)
    }

    fn seal(&self) {
        self.work.lock().unwrap_or_else(PoisonError::into_inner).take();
    }

}


/// Borrows the complete original phase while actual controls use it.
/// The original registration owns its real Weak; accepted physical tasks keep
/// full resource policy and custody beyond the caller's observation.
fn with_detach_work<T>(
    work: &crate::NativeMaterializationWork, call: impl FnOnce(&Arc<DetachReadMeter>) -> Result<T, NativeMountError>,
) -> Result<T, NativeMountError> {
    with_detach_meter(&work.native_mount_work(), call)
}

fn with_detach_meter<T>(
    native: &super::NativeMountMeterWork,
    call: impl FnOnce(&Arc<DetachReadMeter>) -> Result<T, NativeMountError>,
) -> Result<T, NativeMountError> {
    let custody = super::NativeMountAllocationCustody::new_with_meter(native)?;
    custody.claim_with_meter(std::mem::size_of::<DetachReadMeter>() as u64, native)?;
    let meter = Arc::new(DetachReadMeter {
        work: Mutex::new(Some(native.clone())), custody: custody.clone(),
    });
    custody.attach_known_control_with_meter(super::NativeMountControlAllocation::DarwinDetachMeter(
        Arc::downgrade(&meter)), native)?;
    let loan = DetachLoan(&meter);
    let result = call(&meter);
    drop(loan);
    drop(meter);
    drop(custody);
    result
}


struct DetachPath {
    path: Option<CString>,
    _allocation: crate::NativeAllocation,
}

impl Drop for DetachPath {
    fn drop(&mut self) {
        drop(self.path.take());
    }
}

fn poll_direct(
    task: &mut DirectUnmountTask, meter: &Arc<DetachReadMeter>,
) -> Result<std::task::Poll<std::io::Result<std::io::Result<c_int>>>, NativeMountError> {
    meter.with_native(|work| work.charge(crate::WorkCounters {
        items_examined: 1, ..crate::WorkCounters::UNCHARGED,
    }).map_err(NativeMountError::from))?;
    Ok(std::pin::Pin::new(&mut task.task).poll(&mut std::task::Context::from_waker(std::task::Waker::noop())))
}

// The physical path is destroyed before its original reserved operation and
// meter alias, including a native worker panic or an abandoned observer.
struct AcceptedDirectUnmount {
    path: DetachPath,
    operation: acyclic_native_runtime::OwnedNativeOperationReservation,
    _scope: Option<DarwinStartupMeterScope>,
}

/// Offloads the same actual libc detach through the existing measured service.
/// The bound stops observation, not the syscall: accepted work and its path
/// stay owned, and a later stop must retire this task before releasing a fence.
fn bounded_direct_unmount(
    destination: &Path, state: &mut UnmountState, meter: &Arc<DetachReadMeter>,
    scope: Option<&DarwinStartupMeterScope>,
) -> Result<(), NativeMountError> {
    if let Some(task) = &mut state.direct {
        if let std::task::Poll::Ready(result) = poll_direct(task, meter)? {
            let task = state.direct.take().expect("observed accepted task");
            return result.and_then(std::convert::identity).map(|_| ())
                .map_err(|error| darwin_stop_work_error(task.original.meter().take_native_io_failure()
                    .unwrap_or_else(|| native_demand_error(error))));
        }
        return Ok(());
    }
    let path = meter.with_native(|work| {
        let bytes = destination.as_os_str().as_bytes();
        for byte in bytes {
            work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            if *byte == 0 { return Err(std::io::Error::from_raw_os_error(libc::EINVAL).into()); }
        }
        let copied = crate::WorkCounters { bytes_copied: bytes.len() as u64, ..crate::WorkCounters::UNCHARGED };
        work.admit(copied)?;
        let length = bytes.len().checked_add(1).ok_or(crate::WorkError::Overflow)?;
        let mut allocation = work.claim(length as u64)?;
        let mut path = Vec::new();
        path.try_reserve_exact(length)?;
        allocation.reconcile_capacity(path.capacity() as u64)?;
        path.extend_from_slice(bytes);
        path.push(0);
        // SAFETY: the individually validated input has no NUL and exactly one
        // terminator was appended, so no second byte scan is necessary.
        let path = DetachPath { path: Some(unsafe { CString::from_vec_with_nul_unchecked(path) }),
            _allocation: allocation };
        work.record_admitted(copied)?;
        Ok(path)
    })?;
    let original = meter.work.lock().unwrap_or_else(PoisonError::into_inner)
        .as_ref().cloned().ok_or(crate::DemandError::Cancelled)?;
    let task_meter = original.native_materialization_work().runtime_meter_owner();
    let operation = acyclic_native_runtime::OwnedNativeOperationReservation::new(
        Arc::clone(&task_meter), acyclic_native_runtime::NativeOperationKind::Write, 1,
    ).map_err(|error| darwin_stop_work_error(original.meter().take_native_io_failure()
        .unwrap_or_else(|| native_demand_error(error))))?;
    let mut owned = AcceptedDirectUnmount {
        path, operation, _scope: scope.map(DarwinStartupMeterScope::try_clone_for_io).transpose()?,
    };
    let task = acyclic_native_runtime::run_blocking_io_measured(move || {
        let path = owned.path.path.as_ref().expect("owned detach path").as_ptr();
        let (result, recorded) = owned.operation.attempt(|| {
            // SAFETY: the accepted payload retains its terminated path through
            // this actual call, even after caller cancellation or loan seal.
            let status = unsafe { libc::unmount(path, libc::MNT_FORCE) };
            if status == 0 { Ok(status) } else { Err(std::io::Error::last_os_error()) }
        });
        drop(owned);
        result.and_then(|status| recorded.map(|()| status))
    }, task_meter).map_err(|error| darwin_stop_work_error(original.meter().take_native_io_failure()
        .unwrap_or_else(|| native_demand_error(error))))?;
    // Custody is installed before even the first readiness probe can be denied.
    state.direct = Some(DirectUnmountTask {
        task, _meter: Arc::clone(meter), original, _custody: meter.custody.clone(),
    });
    let deadline = Instant::now() + DIRECT_UNMOUNT_TIMEOUT;
    loop {
        match poll_direct(state.direct.as_mut().expect("accepted task retained"), meter)? {
            std::task::Poll::Ready(result) => {
                let task = state.direct.take().expect("observed accepted task");
                return result.and_then(std::convert::identity).map(|_| ())
                    .map_err(|error| darwin_stop_work_error(task.original.meter().take_native_io_failure()
                        .unwrap_or_else(|| native_demand_error(error))));
            }
            std::task::Poll::Pending => {}
        }
        meter.with_native(|work| work.admit(crate::WorkCounters::UNCHARGED).map_err(NativeMountError::from))?;
        if Instant::now() >= deadline { return Ok(()); }
        meter.attempt(false)?;
        std::thread::sleep(Duration::from_millis(10));
    }
}

struct DiskutilProcess {
    tree: Option<acyclic_native_runtime::ProcessTree>,
    deadline: Instant,
    failed_spawn: bool,
    _scope: Option<DarwinStartupMeterScope>,
}

fn spawn_diskutil(
    destination: &Path, custody: &mut Option<DiskutilProcess>, meter: &Arc<DetachReadMeter>,
    scope: Option<&DarwinStartupMeterScope>,
) -> Result<(), NativeMountError> {
    *custody = Some(DiskutilProcess {
        tree: None, deadline: Instant::now() + DISKUTIL_UNMOUNT_TIMEOUT, failed_spawn: false,
        _scope: scope.map(DarwinStartupMeterScope::try_clone_for_io).transpose()?,
    });
    let result = meter.with_host_work(|work| {
        crate::native_host::unix_name_with_work(destination.as_os_str(), work, |path, _work| {
            meter.with_meter(|native| acyclic_native_runtime::ProcessTree::spawn_closed_measured(
                c"/usr/sbin/diskutil", &[c"unmount", c"force", path], native,
                &mut custody.as_mut().expect("spawn custody").tree,
            )).map_err(|error| crate::materializer::native_materialization_work_error(meter.native_error(error)))
        }).map_err(NativeMountError::from)
    });
    if custody.as_ref().is_some_and(|child| child.tree.is_none()) { custody.take(); }
    if let Some(child) = custody { child.failed_spawn = result.is_err(); }
    result
}

impl DiskutilProcess {
    fn observe(&mut self, meter: &Arc<DetachReadMeter>) -> Result<Option<std::process::ExitStatus>, NativeMountError> {
        meter.with_meter(|native| self.tree.as_mut().expect("retained process tree").try_wait_measured(native))
            .map_err(|error| darwin_stop_work_error(meter.native_error(error)))
    }

    fn terminate(&mut self, meter: &Arc<DetachReadMeter>) -> Result<(), NativeMountError> {
        meter.with_meter(|native| self.tree.as_mut().expect("retained process tree").terminate_measured(native))
            .map_err(|error| darwin_stop_work_error(meter.native_error(error)))
    }
}

/// Keeps the original direct-then-diskutil ordering, with each retry and probe
/// admitted where it really occurs. Failed control retains the task or PID.
fn bounded_unmount_measured(
    destination: &Path, state: &mut UnmountState, meter: &Arc<DetachReadMeter>,
    scope: Option<&DarwinStartupMeterScope>,
) -> Result<UnmountEvidence, NativeMountError> {
    let mounted = meter.with_host_work(|work| is_mounted_measured(destination, work))?;
    state.detached = !mounted;
    if let Some(child) = &mut state.diskutil {
        if !mounted || child.failed_spawn {
            child.terminate(meter)?;
            state.diskutil.take();
        }
    }
    if !mounted {
        if let Some(task) = &mut state.direct {
            if poll_direct(task, meter)?.is_ready() { state.direct.take(); }
        }
        return Ok(UnmountEvidence::AlreadyUnmounted);
    }
    if state.diskutil.is_none() {
        match bounded_direct_unmount(destination, state, meter, scope) {
            Ok(()) | Err(NativeMountError::Io(_)) => {}
            Err(error) => return Err(error),
        }
        if !meter.with_host_work(|work| is_mounted_measured(destination, work))? {
            state.detached = true;
            return Ok(UnmountEvidence::Detached);
        }
        spawn_diskutil(destination, &mut state.diskutil, meter, scope)?;
    }
    let (status, timed_out) = loop {
        let child = state.diskutil.as_mut().expect("owned diskutil process");
        let status = child.observe(meter)?;
        let timed_out = Instant::now() >= child.deadline;
        if status.is_some() || timed_out {
            child.terminate(meter)?;
            break (status, timed_out);
        }
        meter.attempt(false)?;
        std::thread::sleep(Duration::from_millis(10));
    };
    state.diskutil.take();
    if !meter.with_host_work(|work| is_mounted_measured(destination, work))? {
        state.detached = true;
        return Ok(UnmountEvidence::Detached);
    }
    if timed_out {
        return Err(NativeMountError::DarwinTeardown(DarwinTeardownFailure::DiskutilTimedOut));
    }
    if let Some(status) = status.filter(|status| !status.success()) {
        return Err(NativeMountError::DarwinTeardown(DarwinTeardownFailure::DiskutilExit(status)));
    }
    let verify_deadline = Instant::now() + DISKUTIL_VISIBILITY_TIMEOUT;
    while meter.with_host_work(|work| is_mounted_measured(destination, work))? {
        if Instant::now() >= verify_deadline {
            return Err(NativeMountError::DarwinTeardown(DarwinTeardownFailure::MountStillVisible));
        }
        meter.attempt(false)?;
        std::thread::sleep(Duration::from_millis(10));
    }
    state.detached = true;
    Ok(UnmountEvidence::Detached)
}

/// Native recovery controls without a provider loop. The root recovery owner
/// must keep this value together with its original destination fence, and take
/// both on retry before admitting another owner of that destination.
#[derive(Default)]
pub(super) struct DarwinRecoveryControl(UnmountState);

impl DarwinRecoveryControl {
    pub(super) fn controls_pending(&self) -> bool {
        self.0.direct.is_some() || self.0.diskutil.is_some()
    }

    pub(super) fn namespace_detach_confirmed(&self) -> bool { self.0.detached }

    /// Runs the same admitted teardown leaves as session stop and retains
    /// accepted control identity on every failed or cancelled attempt.
    pub(super) fn recover_measured(
        &mut self, destination: &Path, work: &super::NativeMountMeterWork,
    ) -> Result<(), NativeMountError> {
        with_detach_meter(work, |meter| {
            bounded_unmount_measured(destination, &mut self.0, meter, None)?;
            if let Some(task) = &mut self.0.direct {
                if poll_direct(task, meter)?.is_ready() { self.0.direct.take(); }
            }
            if self.controls_pending() {
                return Err(NativeMountError::ControlPending);
            }
            Ok(())
        })
    }
}

fn context(address: usize) -> Result<&'static DarwinMountContext, i32> {
    if address == 0 {
        return Err(libc::ESTALE);
    }
    // SAFETY: nonzero addresses come only from the bridge, which passes back the
    // `Arc::into_raw` context that `DriverSessionResources` keeps alive until
    // the loop has exited.
    Ok(unsafe { &*(address as *const DarwinMountContext) })
}

/// The existing memo tables use exact-capacity backing, not opaque hash
/// buckets. Each real growth owns its allocating invocation's marker; moving
/// entries preserves the original owners of paths, pages, and source handles.
struct DarwinMap<K, V> {
    storage: DarwinOwned<Vec<(K, V)>>,
}

impl<K, V> Default for DarwinMap<K, V> {
    fn default() -> Self { Self::new() }
}

impl<K, V> DarwinMap<K, V> {
    fn new() -> Self { Self { storage: DarwinOwned::empty(Vec::new()) } }
    fn from_seed(entries: Vec<(K, V)>) -> Self { Self { storage: DarwinOwned::empty(entries) } }
    fn len(&self) -> usize { self.storage.len() }
    fn iter(&self) -> impl Iterator<Item = (&K, &V)> { self.storage.iter().map(|(key, value)| (key, value)) }
    fn values(&self) -> impl Iterator<Item = &V> { self.storage.iter().map(|(_, value)| value) }
    fn values_mut(&mut self) -> impl Iterator<Item = &mut V> { self.storage.iter_mut().map(|(_, value)| value) }
    fn clear(&mut self) { self.storage.clear(); }
    fn drain(&mut self) -> std::vec::Drain<'_, (K, V)> { self.storage.drain(..) }

    fn reserve_measured(&mut self, additional: usize, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
        let required = self.len().checked_add(additional).ok_or(libc::EOVERFLOW)?;
        if required <= self.storage.capacity() { return Ok(()); }
        let capacity = self.storage.capacity().checked_mul(2).unwrap_or(required).max(required);
        let bytes = capacity.checked_mul(std::mem::size_of::<(K, V)>()).ok_or(libc::EOVERFLOW)? as u64;
        let copied = crate::WorkCounters {
            bytes_copied: self.len().checked_mul(std::mem::size_of::<(K, V)>()).ok_or(libc::EOVERFLOW)? as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        work.admit(copied)?;
        let mut next = work.owned(bytes, || {
            let mut entries = Vec::new();
            entries.try_reserve_exact(capacity)?;
            Ok(entries)
        })?;
        work.copying(copied, || next.extend(self.storage.drain(..)))?;
        // The old creator, never this fresh invocation, retires its backing.
        drop(std::mem::replace(&mut self.storage, next));
        Ok(())
    }
}

impl<K: Ord, V> DarwinMap<K, V> {
    fn search_measured(&self, key: &K, work: &DarwinCallbackWork<'_>) -> Result<Result<usize, usize>, i32> {
        let (mut low, mut high) = (0usize, self.len());
        while low < high {
            work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            let middle = low + (high - low) / 2;
            match self.storage[middle].0.cmp(key) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return Ok(Ok(middle)),
            }
        }
        Ok(Err(low))
    }
    fn get_measured(&self, key: &K, work: &DarwinCallbackWork<'_>) -> Result<Option<&V>, i32> {
        Ok(self.search_measured(key, work)?.ok().map(|index| &self.storage[index].1))
    }
    fn get_mut_measured(&mut self, key: &K, work: &DarwinCallbackWork<'_>) -> Result<Option<&mut V>, i32> {
        let index = self.search_measured(key, work)?.ok();
        Ok(index.map(|index| &mut self.storage[index].1))
    }
    fn contains_key_measured(&self, key: &K, work: &DarwinCallbackWork<'_>) -> Result<bool, i32> {
        Ok(self.search_measured(key, work)?.is_ok())
    }
    fn insert_measured(&mut self, key: K, value: V, work: &DarwinCallbackWork<'_>) -> Result<Option<V>, i32> {
        match self.search_measured(&key, work)? {
            Ok(index) => {
                let copied = crate::WorkCounters { bytes_copied: std::mem::size_of::<V>() as u64, ..crate::WorkCounters::UNCHARGED };
                work.copying(copied, || std::mem::replace(&mut self.storage[index].1, value)).map(Some)
            }
            Err(index) => {
                self.reserve_measured(1, work)?;
                let rows = self.len().checked_sub(index).and_then(|rows| rows.checked_add(1)).ok_or(libc::EOVERFLOW)?;
                let copied = crate::WorkCounters { bytes_copied: rows.checked_mul(std::mem::size_of::<(K, V)>()).ok_or(libc::EOVERFLOW)? as u64, ..crate::WorkCounters::UNCHARGED };
                work.copying(copied, || self.storage.insert(index, (key, value)))?;
                Ok(None)
            }
        }
    }
    fn remove_entry_measured(&mut self, key: &K, work: &DarwinCallbackWork<'_>) -> Result<Option<(K, V)>, i32> {
        let Ok(index) = self.search_measured(key, work)? else { return Ok(None); };
        let rows = self.len().checked_sub(index + 1).ok_or(libc::EOVERFLOW)?;
        let copied = crate::WorkCounters { bytes_copied: rows.checked_mul(std::mem::size_of::<(K, V)>()).ok_or(libc::EOVERFLOW)? as u64, ..crate::WorkCounters::UNCHARGED };
        work.copying(copied, || self.storage.remove(index)).map(Some)
    }
    fn remove_measured(&mut self, key: &K, work: &DarwinCallbackWork<'_>) -> Result<Option<V>, i32> {
        Ok(self.remove_entry_measured(key, work)?.map(|(key, value)| { drop(key); value }))
    }
    #[cfg(test)]
    fn get(&self, key: &K) -> Option<&V> {
        self.storage.binary_search_by(|(found, _)| found.cmp(key)).ok().map(|index| &self.storage[index].1)
    }
}

/// Scratch still owned by this request; `into_chunk` moves its original
/// allocation claim to the source engine's existing content packet.
struct DarwinTransient<T> {
    value: Option<T>,
    meter: Arc<NativeDemandReadMeter>,
    bytes: u64,
    intrinsic: bool,
    allocation: Option<crate::NativeAllocation>,
}

impl<T> std::ops::Deref for DarwinTransient<T> {
    type Target = T;
    fn deref(&self) -> &T { self.value.as_ref().expect("native request scratch") }
}

impl<T> Drop for DarwinTransient<T> {
    fn drop(&mut self) {
        drop(self.value.take());
        if !self.intrinsic {
            self.meter.lock().release(self.bytes).expect("release physically dropped request scratch");
        }
    }
}

impl<T> std::ops::DerefMut for DarwinTransient<T> {
    fn deref_mut(&mut self) -> &mut T { self.value.as_mut().expect("native request scratch") }
}

impl DarwinTransient<Vec<u8>> {
    fn empty(work: &DarwinCallbackWork<'_>) -> Self {
        Self { value: Some(Vec::new()), meter: Arc::clone(work.native.meter()), bytes: 0, intrinsic: true, allocation: None }
    }

    fn reserve(&mut self, required: usize, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
        if required <= self.capacity() { return Ok(()); }
        let capacity = self.capacity().checked_mul(2).unwrap_or(required).max(required).min(MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES);
        let copied = crate::WorkCounters { bytes_copied: self.len() as u64, ..crate::WorkCounters::UNCHARGED };
        work.admit(copied)?;
        let allocation = work.native.native_materialization_work().claim(capacity as u64)
            .map_err(|error| work.native_error(error.into()))?;
        let mut next = DarwinTransient { value: Some(Vec::new()), meter: Arc::clone(work.native.meter()), bytes: capacity as u64, intrinsic: true, allocation: Some(allocation) };
        next.try_reserve_exact(capacity).map_err(|error| work.native_error(NativeMountError::Allocation(error)))?;
        let actual = next.capacity() as u64;
        next.allocation.as_mut().expect("owned transient capacity").reconcile_capacity(actual)
            .map_err(|error| work.native_error(error.into()))?;
        next.bytes = actual;
        work.copying(copied, || next.extend_from_slice(self))?;
        drop(std::mem::replace(self, next));
        Ok(())
    }
}

impl DarwinTransient<Bytes> {
    fn into_chunk(mut self) -> crate::kernel::OwnedSourceChunk {
        let bytes = self.value.take().expect("native request bytes");
        let logical_owned_bytes = std::mem::take(&mut self.bytes);
        if self.intrinsic {
            crate::kernel::OwnedSourceChunk::intrinsic(bytes, logical_owned_bytes, self.allocation.take())
        } else {
            crate::kernel::OwnedSourceChunk::claimed(bytes, logical_owned_bytes)
        }
    }
}

/// The existing SDK Arc owns its data before its full original allocation guard.
struct DarwinSharedValue<T> {
    value: T,
    _allocation: crate::NativeAllocation,
}

struct DarwinShared<T> {
    value: Arc<DarwinSharedValue<T>>,
}

impl<T> Clone for DarwinShared<T> {
    fn clone(&self) -> Self { Self { value: Arc::clone(&self.value) } }
}

impl<T> std::ops::Deref for DarwinShared<T> {
    type Target = T;
    fn deref(&self) -> &T { &self.value.value }
}

/// Source-created Arc payloads retain their source's physical owner.
struct DarwinSourceFile {
    value: Option<Arc<dyn MountOpenFile>>,
    custody: Option<super::NativeMountAllocationCustody>,
}

impl Clone for DarwinSourceFile {
    fn clone(&self) -> Self {
        Self { value: self.value.clone(), custody: self.custody.clone() }
    }
}

impl std::ops::Deref for DarwinSourceFile {
    type Target = dyn MountOpenFile;
    fn deref(&self) -> &Self::Target { self.value.as_deref().expect("retained source Arc") }
}

impl Drop for DarwinSourceFile {
    fn drop(&mut self) {
        drop(self.value.take());
        drop(self.custody.take());
    }
}

impl DarwinOwned<Arc<dyn MountOpenFile>> {
    fn into_source_file(mut self) -> DarwinSourceFile {
        DarwinSourceFile { value: self.value.take(), custody: self.custody.take() }
    }
}

struct DarwinOwned<T> {
    value: Option<T>,
    custody: Option<super::NativeMountAllocationCustody>,
    bytes: u64,
    allocation: Option<crate::NativeAllocation>,
}

impl<T> std::ops::Deref for DarwinOwned<T> {
    type Target = T;
    fn deref(&self) -> &T { self.value.as_ref().expect("owned native payload") }
}

impl<T> std::ops::DerefMut for DarwinOwned<T> {
    fn deref_mut(&mut self) -> &mut T { self.value.as_mut().expect("owned native payload") }
}

impl<T> DarwinOwned<T> {
    fn empty(value: T) -> Self { Self { value: Some(value), custody: None, bytes: 0, allocation: None } }

    fn claim_more(&mut self, bytes: u64, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
        if bytes == 0 { return Ok(()); }
        let mut allocation = work.native.native_materialization_work().claim(bytes)
            .map_err(|error| work.native_error(error.into()))?;
        if let Some(owned) = &mut self.allocation {
            owned.absorb(&mut allocation).map_err(|error| work.native_error(error.into()))?;
        } else {
            self.allocation = Some(allocation);
        }
        Ok(())
    }
    fn retire_part(&mut self, bytes: u64) {
        if bytes == 0 { return; }
        if let Some(allocation) = &mut self.allocation {
            allocation.release(bytes).expect("retire only actually dropped native capacity");
            return;
        }
        self.custody.as_ref().expect("registered native custody").retire(bytes)
            .expect("retire only actually dropped native buffer capacity");
        self.bytes = self.bytes.checked_sub(bytes).expect("retirement belongs to this payload");
    }

    fn alias_proof(&self) -> DarwinOwned<()> {
        DarwinOwned { value: Some(()), custody: self.custody.clone(), bytes: 0, allocation: None }
    }

    fn transfer_part<U>(&mut self, value: U, bytes: u64) -> DarwinOwned<U> {
        if let Some(allocation) = &mut self.allocation {
            let split = allocation.split_off(bytes).expect("transfer exact owned native capacity");
            return DarwinOwned { value: Some(value), custody: None, bytes: 0, allocation: Some(split) };
        }
        self.bytes = self.bytes.checked_sub(bytes).expect("transfer exact owned payload capacity");
        DarwinOwned { value: Some(value), custody: self.custody.clone(), bytes, allocation: None }
    }

    fn map<U>(mut self, transform: impl FnOnce(T) -> U) -> DarwinOwned<U> {
        let value = transform(self.value.take().expect("owned native payload"));
        let custody = self.custody.take();
        let bytes = std::mem::take(&mut self.bytes);
        DarwinOwned { value: Some(value), custody, bytes, allocation: self.allocation.take() }
    }

    fn into_parts(mut self) -> (T, DarwinOwned<()>) {
        let value = self.value.take().expect("owned native payload");
        let proof = DarwinOwned { value: Some(()), custody: self.custody.take(), bytes: self.bytes, allocation: self.allocation.take() };
        self.bytes = 0;
        (value, proof)
    }
}

impl<T> Drop for DarwinOwned<T> {
    fn drop(&mut self) {
        drop(self.value.take());
        if let Some(custody) = self.custody.take() {
            custody.retire(self.bytes).expect("retire only the physically dropped native payload");
        }
    }
}

impl DarwinCallbackWork<'_> {
    fn owned<T>(&self, bytes: u64, build: impl FnOnce() -> Result<T, NativeMountError>) -> Result<DarwinOwned<T>, i32> {
        if bytes == 0 {
            return build().map(DarwinOwned::empty).map_err(|error| self.native_error(error));
        }
        let allocation = self.native.native_materialization_work().claim(bytes)
            .map_err(|error| self.native_error(error.into()))?;
        let mut owned = DarwinOwned { value: None, custody: None, bytes: 0, allocation: Some(allocation) };
        owned.value = Some(build().map_err(|error| self.native_error(error))?);
        Ok(owned)
    }

    fn admitted<T>(&self, value: T, bytes: u64) -> Result<DarwinOwned<T>, i32> {
        if bytes == 0 { return Ok(DarwinOwned::empty(value)); }
        let registered = (|| {
            let custody = super::NativeMountAllocationCustody::new_with_meter(self.native)?;
            custody.retain_admitted_with_meter(bytes, self.native)?;
            Ok::<_, NativeMountError>(custody)
        })();
        match registered {
            Ok(custody) => Ok(DarwinOwned { value: Some(value), custody: Some(custody), bytes, allocation: None }),
            Err(error) => {
                drop(value);
                self.native.native_materialization_work().original_demand_work()
                    .with_original_leaf(|original| original.release(bytes))
                    .expect("original source allocation ledger")
                    .expect("dropped source-owned allocation");
                Err(self.native_error(error.into()))
            }
        }
    }

    fn source_chunk(&self, chunk: crate::kernel::OwnedSourceChunk) -> Result<DarwinTransient<Bytes>, i32> {
        let (bytes, logical_owned_bytes, intrinsic, allocation) = chunk.into_owned_parts();
        Ok(DarwinTransient { value: Some(bytes), meter: Arc::clone(self.native.meter()), bytes: logical_owned_bytes, intrinsic, allocation })
    }

    fn copy_bytes(&self, bytes: &[u8]) -> Result<DarwinTransient<Bytes>, i32> {
        if bytes.is_empty() {
            self.check()?;
            return Ok(DarwinTransient { value: Some(Bytes::new()), meter: Arc::clone(self.native.meter()), bytes: 0, intrinsic: false, allocation: None });
        }
        let delta = crate::WorkCounters { bytes_copied: bytes.len() as u64, ..crate::WorkCounters::UNCHARGED };
        let materialization = self.native.native_materialization_work();
        materialization.admit(delta).map_err(|error| self.native_error(error.into()))?;
        let mut allocation = materialization.claim(bytes.len() as u64).map_err(|error| self.native_error(error.into()))?;
        let mut value = Vec::new();
        value.try_reserve_exact(bytes.len()).map_err(|error| self.native_error(NativeMountError::Allocation(error)))?;
        allocation.reconcile_capacity(value.capacity() as u64).map_err(|error| self.native_error(error.into()))?;
        value.extend_from_slice(bytes);
        materialization.record_admitted(delta).map_err(|error| self.native_error(error.into()))?;
        let retained = allocation.bytes();
        Ok(DarwinTransient { value: Some(Bytes::from(value)), meter: Arc::clone(self.native.meter()), bytes: retained, intrinsic: true, allocation: Some(allocation) })
    }

    fn retained_file(&self, file: (Arc<dyn MountOpenFile>, u64)) -> Result<DarwinSourceFile, i32> {
        Ok(self.admitted(file.0, file.1)?.into_source_file())
    }

    fn shared<T>(&self, build: impl FnOnce() -> T) -> Result<DarwinShared<T>, i32> {
        let allocation = self.native.native_materialization_work()
            .claim(std::mem::size_of::<DarwinSharedValue<T>>() as u64)
            .map_err(|error| self.native_error(error.into()))?;
        Ok(DarwinShared { value: Arc::new(DarwinSharedValue { value: build(), _allocation: allocation }) })
    }

    fn native_name(&self, name: &[u8]) -> Result<DarwinOwned<CString>, i32> {
        self.native_name_with_prefix(name, false)
    }

    fn native_name_with_prefix(&self, name: &[u8], slash: bool) -> Result<DarwinOwned<CString>, i32> {
        for byte in name {
            self.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            if *byte == 0 { return Err(libc::EIO); }
        }
        let length = name.len().checked_add(usize::from(slash))
            .and_then(|length| length.checked_add(1)).ok_or(libc::EOVERFLOW)?;
        let copied = crate::WorkCounters { bytes_copied: length as u64, ..crate::WorkCounters::UNCHARGED };
        self.admit(copied)?;
        let mut value = self.owned(length as u64, || {
            let mut value = Vec::new();
            value.try_reserve_exact(length)?;
            Ok(value)
        })?;
        self.copying(copied, || {
            if slash { value.push(b'/'); }
            value.extend_from_slice(name); value.push(0);
        })?;
        // SAFETY: the charged scan rejected every interior NUL, and the
        // only terminator was appended above; no second uncharged scan.
        Ok(value.map(|value| unsafe { CString::from_vec_with_nul_unchecked(value) }))
    }

    fn clone_path(&self, path: &MountPath) -> Result<DarwinOwned<MountPath>, i32> {
        self.path_with_child(path, None)
    }

    fn path_with_child(&self, path: &MountPath, child: Option<&[u8]>) -> Result<DarwinOwned<MountPath>, i32> {
        let count = path.components().len().checked_add(usize::from(child.is_some())).ok_or(libc::EOVERFLOW)?;
        let bytes = count.checked_mul(std::mem::size_of::<Vec<u8>>()).ok_or(libc::EOVERFLOW)? as u64;
        let mut result = self.owned(bytes, || {
            let mut components = Vec::new();
            components.try_reserve_exact(count)?;
            Ok(MountPath { components })
        })?;
        for part in path.components().iter().map(Vec::as_slice).chain(child) {
            self.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            let copied = crate::WorkCounters { bytes_copied: part.len() as u64, ..crate::WorkCounters::UNCHARGED };
            self.admit(copied)?;
            result.claim_more(part.len() as u64, self)?;
            let mut component = Vec::new();
            component.try_reserve_exact(part.len()).map_err(|error| self.native_error(NativeMountError::Allocation(error)))?;
            self.copying(copied, || {
                component.extend_from_slice(part);
                result.components.push(component);
            })?;
        }
        Ok(result)
    }
}

fn darwin_source_errno(error: &MountSourceError) -> i32 {
    match error.semantic_error() {
        MountSourceError::NotFound => libc::ENOENT,
        MountSourceError::AlreadyExists => libc::EEXIST,
        MountSourceError::Invalid(_) | MountSourceError::NamespacePath(_) => libc::EINVAL,
        MountSourceError::Unsupported(_) | MountSourceError::NativeUnsupported => libc::EOPNOTSUPP,
        MountSourceError::Stale | MountSourceError::Filesystem(crate::FsError::SourceSnapshotStale) => libc::ESTALE,
        MountSourceError::Cancelled | MountSourceError::Filesystem(crate::FsError::Cancelled(_)) => libc::ECANCELED,
        MountSourceError::Allocation(_) => libc::ENOMEM,
        MountSourceError::Io(error) | MountSourceError::NativeRecordIo(error, _) =>
            error.raw_os_error().unwrap_or(libc::EIO),
        MountSourceError::NativeWork(error) => super::native_materialization_errno(error),
        MountSourceError::WorkspaceCause(error) => super::native_workspace_errno(error),
        MountSourceError::NativeWorkspaceCause(error) => super::native_workspace_errno(&error.source),
        MountSourceError::NativeText { kind, .. } | MountSourceError::NativeStaticText { kind, .. } =>
            super::native_source_text_errno(*kind),
        MountSourceError::Work(_) | MountSourceError::Engine(_) | MountSourceError::Filesystem(_)
        | MountSourceError::ServicesNotInitialized | MountSourceError::ServiceUnavailable(_)
        | MountSourceError::Retained(_) | MountSourceError::MutationFinalization { .. } => libc::EIO,
        MountSourceError::OriginalSnapshotUnavailable | MountSourceError::Indeterminate
        | MountSourceError::SelectedPublication(_) | MountSourceError::PublicationRetryLimit
        | MountSourceError::NativeRecordJson(_, _) | MountSourceError::NativeRecordLockTimeout(_, _)
        | MountSourceError::NativeRecordOwnershipConflict(_, _)
        | MountSourceError::NativeRecordMaterializationInProgress | MountSourceError::NativeRecordIntegrity
        | MountSourceError::NativeRecordContextTransaction(_, _) | MountSourceError::NativeRecordCorrupt(_, _) => libc::EIO,
        #[cfg(target_os = "macos")]
        MountSourceError::MacMetadata(_) => libc::EIO,
        #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
        MountSourceError::SourceWatchStartup(_) | MountSourceError::NativeWatchIo(_)
        | MountSourceError::NativeWatchBackend(_) | MountSourceError::NativeWatchOriginalIo(_)
        | MountSourceError::NativeWatchOriginalBackend(_)
        | MountSourceError::NativeWatchStartupFailed { .. }
        | MountSourceError::NativeWatchSourceStartupFailed { .. } => libc::EIO,
    }
}

fn darwin_handler_errno(error: &NativeMountError) -> i32 {
    match error {
        NativeMountError::CallbackDenied { .. } => libc::EACCES,
        NativeMountError::Source(error) => darwin_source_errno(error),
        NativeMountError::Materialization(error) => super::native_materialization_errno(error),
        NativeMountError::Demand(crate::DemandError::Mount(error)) => darwin_source_errno(error),
        NativeMountError::Io(error) => error.raw_os_error().unwrap_or(libc::EIO),
        NativeMountError::Demand(crate::DemandError::Cancelled) => libc::ECANCELED,
        NativeMountError::Allocation(_) => libc::ENOMEM,
        _ => libc::EIO,
    }
}

fn darwin_handler_is_fatal(error: &NativeMountError) -> bool {
    match error {
        NativeMountError::Work(_) | NativeMountError::Demand(crate::DemandError::Cancelled)
        | NativeMountError::ControlPending | NativeMountError::CallbackDenied { .. }
        | NativeMountError::DriverPanicked { .. } => true,
        NativeMountError::Materialization(error) => super::native_materialization_failure_is_fatal(error),
        NativeMountError::Source(error) | NativeMountError::Demand(crate::DemandError::Mount(error)) => {
            match error.semantic_error() {
                MountSourceError::NativeWork(error) => super::native_materialization_failure_is_fatal(error),
                MountSourceError::WorkspaceCause(error) => super::native_workspace_failure_is_fatal(error),
                MountSourceError::NativeWorkspaceCause(error) => super::native_workspace_failure_is_fatal(&error.source),
                MountSourceError::Cancelled | MountSourceError::Work(_)
                | MountSourceError::OriginalSnapshotUnavailable | MountSourceError::Indeterminate
                | MountSourceError::NativeRecordMaterializationInProgress
                | MountSourceError::Filesystem(crate::FsError::Cancelled(_) | crate::FsError::Work(_)) => true,
                #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
                MountSourceError::SourceWatchStartup(_)
                | MountSourceError::NativeWatchStartupFailed { .. }
                | MountSourceError::NativeWatchSourceStartupFailed { .. } => true,
                _ => false,
            }
        }
        _ => false,
    }
}

impl DarwinMountContext {
    /// Explicit controls use the installed handler's original grants, or the
    /// currently active original lifecycle meter, never an unbounded fallback.
    fn run_control(
        &self, operation: impl FnOnce(&super::NativeMountMeterWork) -> Result<(), NativeMountError>,
    ) -> Result<(), NativeMountError> {
        if self.handler_failed.load(Ordering::Acquire) {
            return Err(NativeMountError::Lifecycle("Darwin callback previously failed"));
        }
        let active = self.callback_meter();
        let result = if let Err(cause) = active {
            Err(super::NativeMountCallbackFailure {
                cause,
                work: self.source_original_work.as_ref()
                    .map_or(crate::WorkCounters::UNCHARGED, |work| work.meter().lock().work),
            })
        } else if let Some(meter) = &active.expect("original source view was validated") {
            super::run_native_mount_handler_on_meter(meter, operation).map_err(|cause|
                super::NativeMountCallbackFailure { cause, work: meter.meter().lock().work })
        } else {
            let result = super::run_native_mount_handler(self.handler_budget, &self.handler_cancellation, operation);
            let completed = match &result { Ok(receipt) => receipt.work, Err(failure) => failure.work };
            let overflow = self.handler_failure.lock().unwrap_or_else(PoisonError::into_inner)
                .completed_future_request_work.try_add_assign(&completed).err();
            match result {
                Ok(receipt) => match overflow {
                    None => Ok(receipt.value),
                    Some(error) => Err(super::NativeMountCallbackFailure { cause: error.into(), work: completed }),
                },
                Err(failure) => {
                    if let Some(error) = overflow {
                        self.handler_failed.store(true, Ordering::Release);
                        let mut state = self.handler_failure.lock().unwrap_or_else(PoisonError::into_inner);
                        if state.first_fatal.is_none() {
                            state.first_fatal = Some(super::NativeMountCallbackFailure { cause: error.into(), work: completed });
                        }
                    }
                    Err(failure)
                }
            }
        };
        let Err(failure) = result else { return Ok(()); };
        if !darwin_handler_is_fatal(&failure.cause) { return Err(failure.cause); }
        self.handler_failed.store(true, Ordering::Release);
        // Work/cancellation causes are inline and can be copied without a new
        // error envelope. The original SDK panic payload has only one owner.
        let returned = match &failure.cause {
            NativeMountError::Work(error) => Some(NativeMountError::Work(error.clone())),
            NativeMountError::Demand(crate::DemandError::Cancelled) =>
                Some(NativeMountError::Demand(crate::DemandError::Cancelled)),
            NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(error)) =>
                Some(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(error.clone()))),
            NativeMountError::Materialization(crate::NativeTreeMaterializationError::Cancelled) =>
                Some(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Cancelled)),
            NativeMountError::ControlPending => Some(NativeMountError::ControlPending),
            _ => None,
        };
        if active.is_some() {
            let mut retained = self.startup_failure.lock().unwrap_or_else(PoisonError::into_inner);
            if retained.is_none() { *retained = Some(failure); }
        } else {
            let mut retained = self.handler_failure.lock().unwrap_or_else(PoisonError::into_inner);
            if retained.first_fatal.is_none() { retained.first_fatal = Some(failure); }
        }
        Err(returned.unwrap_or(NativeMountError::Lifecycle("Darwin native control failed; original callback failure retained")))
    }
}

struct DarwinCallbackWork<'a> {
    native: &'a super::NativeMountMeterWork,
    failure: std::cell::RefCell<Option<NativeMountError>>,
}

impl DarwinCallbackWork<'_> {
    fn new(native: &super::NativeMountMeterWork) -> DarwinCallbackWork<'_> {
        DarwinCallbackWork { native, failure: std::cell::RefCell::new(None) }
    }

    fn source_error(&self, error: MountSourceError) -> i32 {
        let status = darwin_source_errno(&error);
        *self.failure.borrow_mut() = Some(error.into());
        status
    }

    fn native_error(&self, error: NativeMountError) -> i32 {
        let status = darwin_handler_errno(&error);
        *self.failure.borrow_mut() = Some(error);
        status
    }

    fn check(&self) -> Result<(), i32> {
        self.native.meter().lock().check().map_err(|error| self.native_error(error.into()))
    }

    fn charge(&self, delta: crate::WorkCounters) -> Result<(), i32> {
        self.native.native_materialization_work().charge(delta).map_err(|error| self.native_error(error.into()))
    }

    fn admit(&self, delta: crate::WorkCounters) -> Result<(), i32> {
        self.native.native_materialization_work().admit(delta).map_err(|error| self.native_error(error.into()))
    }

    /// Only local memory copies enter this closure; full original admission is
    /// retained without holding a work mutex across SDK/source/native waits.
    fn copying<T>(&self, delta: crate::WorkCounters, copy: impl FnOnce() -> T) -> Result<T, i32> {
        let work = self.native.native_materialization_work();
        work.admit(delta).map_err(|error| self.native_error(error.into()))?;
        let value = copy();
        work.record_admitted(delta).map_err(|error| self.native_error(error.into()))?;
        Ok(value)
    }
    fn finish<T>(&self, result: Result<T, i32>) -> Result<T, NativeMountError> {
        result.map_err(|status| self.failure.borrow_mut().take()
            .filter(|error| darwin_handler_errno(error) == status)
            .unwrap_or_else(|| NativeMountError::Io(std::io::Error::from_raw_os_error(status))))
    }
}

impl DarwinMountContext {
    fn run_request<T>(&self, operation: impl FnOnce(&DarwinCallbackWork<'_>) -> Result<T, i32>) -> Result<T, i32> {
        // The mutex protects only the weak scope snapshot, never a source/SDK wait.
        let active = match self.callback_meter() {
            Ok(active) => active,
            Err(cause) => {
                let status = darwin_handler_errno(&cause);
                if darwin_handler_is_fatal(&cause) {
                    self.handler_failed.store(true, Ordering::Release);
                    let work = self.source_original_work.as_ref()
                        .map_or(crate::WorkCounters::UNCHARGED, |work| work.meter().lock().work);
                    let mut retained = self.handler_failure.lock().unwrap_or_else(PoisonError::into_inner);
                    if retained.first_fatal.is_none() {
                        retained.first_fatal = Some(super::NativeMountCallbackFailure { cause, work });
                    }
                }
                return Err(status);
            }
        };
        if active.is_some() {
            let failure = self.startup_failure.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(error) = failure.as_ref().filter(|failure| darwin_handler_is_fatal(&failure.cause)) {
                return Err(darwin_handler_errno(&error.cause));
            }
        } else {
            let failure = self.handler_failure.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(failure) = failure.first_fatal.as_ref().filter(|failure| darwin_handler_is_fatal(&failure.cause)) {
                return Err(darwin_handler_errno(&failure.cause));
            }
        }
        if self.handler_failed.load(Ordering::Acquire) { return Err(libc::EIO); }
        let status = std::cell::Cell::new(None);
        let run = |native: &super::NativeMountMeterWork| {
            let work = DarwinCallbackWork::new(native);
            let result = work.check().and_then(|()| operation(&work));
            if let Err(error) = &result { status.set(Some(*error)); }
            work.finish(result)
        };
        if let Some(meter) = active {
            match super::run_native_mount_handler_on_meter(&meter, run) {
                Ok(value) => Ok(value),
                Err(error) => {
                    let status = status.get().unwrap_or_else(|| darwin_handler_errno(&error));
                    // Normal absent-name probes never poison constructor admission.
                    if darwin_handler_is_fatal(&error) {
                        self.handler_failed.store(true, Ordering::Release);
                        let mut failure = self.startup_failure.lock().unwrap_or_else(PoisonError::into_inner);
                        if failure.is_none() { *failure = Some(super::NativeMountCallbackFailure { cause: error, work: meter.meter().lock().work }); }
                    }
                    Err(status)
                }
            }
        } else {
            let result = super::run_native_mount_handler(self.handler_budget, &self.handler_cancellation, run);
            let completed = match &result {
                Ok(receipt) => &receipt.work,
                Err(failure) => &failure.work,
            };
            let mut retained = self.handler_failure.lock().unwrap_or_else(PoisonError::into_inner);
            let overflow = retained.completed_future_request_work.try_add_assign(completed).err();
            match result {
                Ok(receipt) => {
                    if let Some(error) = overflow {
                        self.handler_failed.store(true, Ordering::Release);
                        if retained.first_fatal.is_none() {
                            retained.first_fatal = Some(super::NativeMountCallbackFailure {
                                cause: error.into(), work: receipt.work,
                            });
                        }
                        return Err(libc::EIO);
                    }
                    Ok(receipt.value)
                }
                Err(failure) => {
                    let status = status.get().unwrap_or_else(|| darwin_handler_errno(&failure.cause));
                    if darwin_handler_is_fatal(&failure.cause) {
                        self.handler_failed.store(true, Ordering::Release);
                        if retained.first_fatal.is_none() { retained.first_fatal = Some(failure); }
                    } else if let Some(error) = overflow {
                        self.handler_failed.store(true, Ordering::Release);
                        if retained.first_fatal.is_none() {
                            retained.first_fatal = Some(super::NativeMountCallbackFailure {
                                cause: error.into(), work: failure.work,
                            });
                        }
                    }
                    Err(status)
                }
            }
        }
    }

    fn lookup(&self, path: &MountPath) -> Result<MountLookup, i32> {
        self.run_request(|work| self.lookup_measured(path, work))
    }

    fn remember_current_lookup(&self, path: &MountPath, lookup: Option<MountLookup>) -> Result<(), i32> {
        self.run_request(|work| self.remember_current_lookup_measured(path, lookup, work))
    }

    fn remember_lookup(&self, path: &MountPath, lookup: Option<MountLookup>, stamp: CacheStamp) -> Result<(), i32> {
        self.run_request(|work| self.remember_lookup_measured(path, lookup, stamp, work))
    }

    fn open(&self, path: &MountPath, state: InitialHandleState) -> Result<u64, i32> {
        self.run_request(|work| self.open_measured(path, state, work))
    }

    fn bind(&self, path: &MountPath, handle: u64) -> Result<DarwinSourceFile, i32> {
        self.run_request(|work| self.bind_measured(path, handle, work))
    }

    fn unlink(&self, path: &MountPath, lookup: MountLookup, unlink: impl FnOnce() -> Result<(), MountSourceError>) -> Result<(), i32> {
        self.run_request(|work| self.unlink_measured(path, lookup, unlink, work))
    }

    fn file_target(&self, path: &MountPath, handle: u64) -> Result<FileTarget, i32> {
        self.run_request(|work| self.file_target_measured(path, handle, work))
    }

    fn file_or_open(&self, path: &MountPath, handle: u64) -> Result<DarwinSourceFile, i32> {
        self.run_request(|work| self.file_or_open_measured(path, handle, work))
    }

    fn lookup_handle(&self, path: &MountPath, handle: u64) -> Result<MountLookup, i32> {
        self.run_request(|work| self.lookup_handle_measured(path, handle, work))
    }

    fn attributes(&self, path: &MountPath, handle: u64) -> Result<NativeStat, i32> {
        self.run_request(|work| self.attributes_measured(path, handle, work))
    }

    fn attributes_from_lookup(&self, path: &MountPath, lookup: MountLookup, stamp: Option<CacheStamp>) -> Result<NativeStat, i32> {
        self.run_request(|work| self.attributes_from_lookup_measured(path, lookup, stamp, work))
    }

    fn may_have_named_attributes(&self, path: &MountPath) -> Result<bool, i32> {
        self.run_request(|work| self.may_have_named_attributes_measured(path, work))
    }

    fn mutate_metadata(&self, path: &MountPath, handle: u64, mutate: impl FnOnce(&mut FileMetadata) -> Result<(), i32>) -> Result<(), i32> {
        self.run_request(|work| self.mutate_metadata_measured(path, handle, mutate, work))
    }

    fn flush_on_close(&self, handle: u64) -> Result<(), i32> {
        self.run_request(|work| self.flush_on_close_measured(handle, work))
    }

    fn sync(&self) -> Result<(), i32> {
        self.run_request(|work| self.sync_measured(work))
    }

    fn inode(&self, file_id: FileId) -> Result<u64, i32> {
        self.run_request(|work| self.inode_measured(file_id, work))
    }

    fn mutate<T>(&self, mutation: impl FnOnce() -> Result<T, MountSourceError>) -> Result<T, i32> {
        self.run_request(|work| self.mutate_measured(mutation, work))
    }

    fn mutate_file<T>(&self, path: &MountPath, handle: u64, mutation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>) -> Result<T, i32> {
        self.run_request(|work| self.mutate_file_measured(path, handle, mutation, work))
    }

    pub(super) fn lookup_with_meter(
        &self, path: &MountPath, native: &super::NativeMountMeterWork,
    ) -> Result<MountLookup, MountSourceError> {
        let work = DarwinCallbackWork::new(native);
        self.lookup_measured(path, &work).map_err(|status| {
            match work.failure.borrow_mut().take() {
                Some(NativeMountError::Source(error)) => error,
                Some(NativeMountError::Work(error)) => MountSourceError::Work(error),
                Some(NativeMountError::Demand(error)) => super::lazy::mount_demand_error(error),
                Some(NativeMountError::Io(error)) => MountSourceError::Io(error),
                Some(NativeMountError::Materialization(error)) => MountSourceError::NativeWork(error),
                Some(NativeMountError::Allocation(error)) => MountSourceError::Allocation(error),
                Some(NativeMountError::ServicesNotInitialized) => MountSourceError::ServicesNotInitialized,
                _ if status == libc::ENOENT => MountSourceError::NotFound,
                _ => MountSourceError::Io(std::io::Error::from_raw_os_error(status)),
            }
        })
    }
}

/// and it completes before any barrier that began after it (see
/// [`DarwinMountContext::revalidate`]). A panic fails it with `EIO`.
fn callback<T>(
    address: usize,
    span: &tracing::span::EnteredSpan,
    operation: impl FnOnce(&'static DarwinMountContext, &DarwinCallbackWork<'_>) -> Result<T, i32>,
) -> Result<T, i32> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let context = context(address)?;
        let _callback = context.callbacks.enter();
        let _origin = context.origin.enter();
        context.run_request(|work| operation(context, work))
    }))
    .unwrap_or(Err(libc::EIO))
    .inspect_err(|error| {
        span.record("errno", error);
    })
}

fn ffi_status(
    address: usize,
    span: &tracing::span::EnteredSpan,
    operation: impl FnOnce(&'static DarwinMountContext, &DarwinCallbackWork<'_>) -> Result<c_int, i32>,
) -> c_int {
    callback(address, span, operation).unwrap_or_else(|error| -error)
}

fn ffi_offset(
    address: usize,
    span: &tracing::span::EnteredSpan,
    operation: impl FnOnce(&'static DarwinMountContext, &DarwinCallbackWork<'_>) -> Result<i64, i32>,
) -> i64 {
    callback(address, span, operation).unwrap_or_else(|error| -i64::from(error))
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_getattr(
    address: usize,
    path: *const c_char,
    handle: u64,
    result: *mut NativeStat,
) -> c_int {
    let _span = callback_span!(TRACE, getattr, fh = handle);
    ffi_status(address, &_span, |context, work| {
        let attributes = context.attributes_measured(&mount_path_measured(path, work)?, handle, work)?;
        // SAFETY: the bridge passes a valid, writable `NativeStat` for the callback.
        work.copying(crate::WorkCounters { bytes_copied: std::mem::size_of::<NativeStat>() as u64, ..crate::WorkCounters::UNCHARGED }, || unsafe { result.write(attributes) })?;
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
    let _span = callback_span!(TRACE, open);
    ffi_status(address, &_span, |context, work| {
        if flags & libc::O_ACCMODE != libc::O_RDONLY {
            context.admit_write()?;
        }
        let opened = context.open_measured(&mount_path_measured(path, work)?, InitialHandleState::Clean, work)?;
        // SAFETY: the bridge passes a valid, writable handle slot for the callback.
        work.copying(crate::WorkCounters { bytes_copied: std::mem::size_of::<u64>() as u64, ..crate::WorkCounters::UNCHARGED }, || unsafe { handle.write(opened) })?;
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
    let _span = callback_span!(DEBUG, create);
    ffi_status(address, &_span, |context, work| {
        let path = mount_path_measured(path, work)?;
        let lookup = context.mutate_measured(|| {
            context.source.create_file_with_meter(&path, create_metadata(mode, mode::IFREG, uid, gid), work.native)
        }, work)?;
        context.remember_current_lookup_measured(&path, Some(lookup), work)?;
        context.namespace_changed();
        let opened = context.open_measured(&path, InitialHandleState::Written, work)?;
        // SAFETY: the bridge passes a valid, writable handle slot for the callback.
        work.copying(crate::WorkCounters { bytes_copied: std::mem::size_of::<u64>() as u64, ..crate::WorkCounters::UNCHARGED }, || unsafe { handle.write(opened) })?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_release(
    address: usize,
    _path: *const c_char,
    handle: u64,
) -> c_int {
    let _span = callback_span!(DEBUG, release, fh = handle);
    ffi_status(address, &_span, |context, work| {
        if handle == 0 {
            return Ok(0);
        }
        context
            .files
            .write()
            .map_err(|_| libc::EIO)?
            .remove_measured(&handle, work)?
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
    let _span = callback_span!(TRACE, read, fh = handle, offset = offset, len = length);
    ffi_status(address, &_span, |context, work| {
        let requested_length = bounded_length(length)?;
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let chunk = context.file_or_open_measured(&mount_path_measured(path, work)?, handle, work)?
            .read_up_to_with_meter(offset, requested_length, work.native)
            .map_err(|error| work.source_error(error))?;
        let bytes = work.source_chunk(chunk)?;
        if bytes.len() > usize::try_from(requested_length).unwrap_or(usize::MAX) {
            return Err(libc::EIO);
        }
        // SAFETY: the bridge buffer holds `length` writable bytes, `bytes` is no
        // longer than that (checked above), and Rust owns `bytes` separately.
        let copied = crate::WorkCounters { bytes_copied: bytes.len() as u64, ..crate::WorkCounters::UNCHARGED };
        work.copying(copied, || unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), buffer.cast(), bytes.len()) })?;
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
    let _span = callback_span!(TRACE, write, fh = handle, offset = offset, len = length);
    ffi_status(address, &_span, |context, work| {
        let length = bounded_length(length)?;
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        // SAFETY: the bridge forwards FUSE's `length`-byte write buffer, readable for
        // the callback; `bounded_length` capped that length.
        let bytes = unsafe { std::slice::from_raw_parts(buffer.cast::<u8>(), length as usize) };
        let copied = work.copy_bytes(bytes)?;
        context.mutate_file_measured(&mount_path_measured(path, work)?, handle, |file| {
            file.write_range_with_meter(offset, copied.into_chunk(), work.native)
        }, work)?;
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
    let _span = callback_span!(DEBUG, truncate, fh = handle, len = length);
    ffi_status(address, &_span, |context, work| {
        let length = u64::try_from(length).map_err(|_| libc::EINVAL)?;
        context.mutate_file_measured(&mount_path_measured(path, work)?, handle, |file| file.resize_with_meter(length, work.native), work)?;
        Ok(0)
    })
}

/// NFS CLOSE: publishes only what this handle wrote, and only for sources
/// whose close is a publication boundary.
#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_flush(address: usize, handle: u64) -> c_int {
    let _span = callback_span!(DEBUG, flush, fh = handle);
    ffi_status(address, &_span, |context, work| {
        context.flush_on_close_measured(handle, work)?;
        Ok(0)
    })
}

/// NFS COMMIT and stable WRITE. The client sends these for `fsync` and
/// for its close-to-open flush alike, so each keeps the `fsync` guarantee:
/// every acknowledged mutation is published once this returns.
#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_fsync(address: usize) -> c_int {
    let _span = callback_span!(DEBUG, fsync);
    ffi_status(address, &_span, |context, work| {
        context.sync_measured(work)?;
        Ok(0)
    })
}

/// Whether every acknowledged write is already as durable as `fsync` would
/// make it, because the source's flush publishes nothing: WRITE replies are
/// then stable and the client never needs to COMMIT.
#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_durable_writes(address: usize) -> c_int {
    let _span = callback_span!(TRACE, durable_writes);
    ffi_status(address, &_span, |context, work| {
        Ok(c_int::from(!context.source.flush_publishes()))
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_opendir(
    address: usize,
    path: *const c_char,
    handle: *mut u64,
) -> c_int {
    let _span = callback_span!(TRACE, opendir);
    ffi_status(address, &_span, |context, work| {
        let path = mount_path_measured(path, work)?;
        let binding_epoch = context.source.binding_epoch_with_meter(work.native).map_err(|error| work.source_error(error))?;
        let epochs = context.cache_epochs_measured(work)?;

        let _binding_lease = context.source.acquire_binding_lease_with_meter(binding_epoch, work.native)
            .map_err(|error| work.source_error(error))?;
        if context.lookup_measured(&path, work)?.node.kind != MountNodeKind::Directory {
            return Err(libc::ENOTDIR);
        }
        let (path, path_custody) = path.into_parts();
        let mut directory = DirectoryHandle::new(path, binding_epoch, epochs);
        directory.path_custody = Some(path_custody);
        directory.keep_current_measured(context, work)?;
        let allocated = context.allocate_handle()?;
        let directory = work.shared(|| Mutex::new(directory))?;
        let mut directories = context.directories.lock().map_err(|_| libc::EIO)?;
        directories.insert_measured(allocated, directory, work)?;
        // SAFETY: the bridge passes a valid, writable handle slot for the callback.
        work.copying(crate::WorkCounters { bytes_copied: std::mem::size_of::<u64>() as u64, ..crate::WorkCounters::UNCHARGED }, || unsafe { handle.write(allocated) })?;
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
    let _span = callback_span!(TRACE, readdir, fh = handle, offset = offset);
    ffi_status(address, &_span, |context, work| {
        let directory = context
            .directories
            .lock()
            .map_err(|_| libc::EIO)?
            .get_measured(&handle, work)?
            .cloned()
            .ok_or(libc::ESTALE)?;
        let mut directory = directory.lock().map_err(|_| libc::EIO)?;
        let _binding_lease = context.source.acquire_binding_lease_with_meter(directory.binding_epoch, work.native)
            .map_err(|error| work.source_error(error))?;
        directory.keep_current_measured(context, work)?;
        if offset < 0 {
            return Err(libc::EINVAL);
        }
        if !directory.can_reuse_pages() {
            directory.rewind();
            context
                .directory_checkpoints
                .lock()
                .map_err(|_| libc::EIO)?
                .remove_measured(&directory.path, work)?;
        } else if offset > 0 && directory.emitted == 0 {
            let checkpoints = context
                .directory_checkpoints
                .lock()
                .map_err(|_| libc::EIO)?;
            if let Some(checkpoint) = checkpoints.get_measured(&directory.path, work)?
                && checkpoint.revision == context.namespace_revision.load(Ordering::Acquire)
                && checkpoint.handle.emitted == offset
                && checkpoint.handle.binding_epoch == directory.binding_epoch
                && checkpoint.handle.is_current_measured(context, work)?
            {
                *directory = checkpoint.handle.clone_measured(work)?;
            }
        }
        if offset < directory.emitted {
            directory.rewind();
        }
        advance_directory_to_offset_measured(context, &mut directory, offset, work)?;
        while directory.emitted < 2 {
            let name = if directory.emitted == 0 { c"." } else { c".." };
            let next = directory.emitted + 1;
            // SAFETY: `buffer` and `filler` are FUSE's live readdir fill context for
            // this callback, and `name` outlives the synchronous call.
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
                finish_directory_page_measured(context, &directory, true, work)?;
                return Ok(0);
            }
            directory.emitted = next;
        }
        loop {
            ensure_directory_page_measured(context, &mut directory, work)?;
            let Some(entry) = directory.entries.front() else {
                finish_directory_page_measured(context, &directory, false, work)?;
                context
                    .directory_checkpoints
                    .lock()
                    .map_err(|_| libc::EIO)?
                    .remove_measured(&directory.path, work)?;
                return Ok(0);
            };
            let (name, attributes) = directory_entry_measured(context, &directory, entry, work)?;
            let next = directory.emitted.checked_add(1).ok_or(libc::EOVERFLOW)?;
            // SAFETY: `buffer` and `filler` are FUSE's live readdir fill context for
            // this callback; `name` and `attributes` outlive the synchronous call.
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
                finish_directory_page_measured(context, &directory, true, work)?;
                return Ok(0);
            }
            directory.pop_entry();
            directory.emitted = next;
        }
    })
}

/// One listed entry's native name and attributes. The page was read after
/// the directory's stamp was sampled, which therefore labels the entry.
fn directory_entry_measured(
    context: &DarwinMountContext,
    directory: &DirectoryHandle,
    entry: &MountDirectoryEntry, work: &DarwinCallbackWork<'_>) -> Result<(DarwinOwned<CString>, NativeStat), i32> {
    work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
    let name = work.native_name(&entry.name)?;
    let path = work.path_with_child(&directory.path, Some(&entry.name))?;
    let attributes = context.attributes_from_lookup_measured(
        &path,
        MountLookup {
            node: entry.node,
            metadata: entry.metadata,
        },
        directory.epochs.map(|epochs| epochs.view), work)?;
    Ok((name, attributes))
}

fn finish_directory_page_measured(
    context: &DarwinMountContext,
    directory: &DirectoryHandle,
    checkpoint: bool, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
    // A mutation can happen after the last page read, while the native filler
    // copies entries into its buffer. Returning ESTALE discards that buffer
    // rather than exposing a listing assembled from different view epochs.
    if directory.emitted > 2 && directory.can_reuse_pages() && !directory.is_current_measured(context, work)? {
        return Err(libc::ESTALE);
    }
    if checkpoint {
        checkpoint_directory_measured(context, directory, work)?;
    }
    Ok(())
}

fn advance_directory_to_offset_measured(
    context: &DarwinMountContext,
    directory: &mut DirectoryHandle,
    offset: i64, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
    while directory.emitted < offset {
        if directory.emitted < 2 {
            directory.emitted += 1;
            continue;
        }
        ensure_directory_page_measured(context, directory, work)?;
        if !directory.entries.is_empty() {
            work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        }
        if !directory.pop_entry() {
            return Err(libc::EINVAL);
        }
        directory.emitted += 1;
    }
    Ok(())
}

fn ensure_directory_page_measured(
    context: &DarwinMountContext,
    directory: &mut DirectoryHandle, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
    directory.keep_current_measured(context, work)?;
    for _ in 0..MAXIMUM_LISTING_RESTARTS {
        if !directory.entries.is_empty() || directory.exhausted {
            return Ok(());
        }
        let read_in = directory.epochs;
        directory
            .revision
            .get_or_insert_with(|| context.namespace_revision.load(Ordering::Acquire));
        let page = context.source.read_directory_with_meter(
                &directory.path,
                directory.cursor.as_deref(),
                DIRECTORY_PAGE_SIZE, work.native)
            .map_err(|error| work.source_error(error))?;
        directory.keep_current_measured(context, work)?;
        if directory.epochs != read_in {
            // Started over in a newer view; this page is from the older one.
            continue;
        }
        if page.entries().is_empty() && page.next_cursor().is_some() {
            return Err(libc::EIO);
        }
        directory.clear_page();
        let (entries, cursor, ownership) = page.into_parts();
        directory.cursor = cursor;
        directory.exhausted = directory.cursor.is_none();
        directory.entries = entries.into();
        directory.page_source_ownership = Some(ownership);
        return Ok(());
    }
    Err(libc::ESTALE)
}

#[cfg(test)]
fn finish_directory_page(context: &DarwinMountContext, directory: &DirectoryHandle, checkpoint: bool) -> Result<(), i32> {
    context.run_request(|work| finish_directory_page_measured(context, directory, checkpoint, work))
}

#[cfg(test)]
fn advance_directory_to_offset(context: &DarwinMountContext, directory: &mut DirectoryHandle, offset: i64) -> Result<(), i32> {
    context.run_request(|work| advance_directory_to_offset_measured(context, directory, offset, work))
}

#[cfg(test)]
fn ensure_directory_page(context: &DarwinMountContext, directory: &mut DirectoryHandle) -> Result<(), i32> {
    context.run_request(|work| ensure_directory_page_measured(context, directory, work))
}

fn checkpoint_directory_measured(
    context: &DarwinMountContext,
    directory: &DirectoryHandle, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
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
        checkpoints.remove_measured(&directory.path, work)?;
        return Ok(());
    }
    if checkpoints.len() >= MAXIMUM_DIRECTORY_CHECKPOINTS
        && !checkpoints.contains_key_measured(&directory.path, work)?
    {
        for (path, checkpoint) in checkpoints.drain() { drop(path); drop(checkpoint); }
    }
    let path = work.clone_path(&directory.path)?;
    let handle = directory.clone_measured(work)?;
    let (path, key_custody) = path.into_parts();
    if let Some((key, value)) = checkpoints.remove_entry_measured(&path, work)? { drop(key); drop(value); }
    checkpoints.insert_measured(path, DirectoryCheckpoint { revision, handle, key_custody }, work)?;
    Ok(())
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_releasedir(address: usize, handle: u64) -> c_int {
    let _span = callback_span!(TRACE, releasedir, fh = handle);
    ffi_status(address, &_span, |context, work| {
        context
            .directories
            .lock()
            .map_err(|_| libc::EIO)?
            .remove_measured(&handle, work)?
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
    let _span = callback_span!(TRACE, mkdir);
    ffi_status(address, &_span, |context, work| {
        let path = mount_path_measured(path, work)?;
        context.mutate_measured(|| {
            context.source.create_directory_with_meter(&path, create_metadata(mode, mode::IFDIR, uid, gid), work.native)
        }, work)?;
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
    let _span = callback_span!(TRACE, remove);
    ffi_status(address, &_span, |context, work| {
        let path = mount_path_measured(path, work)?;
        let lookup = context.lookup_measured(&path, work)?;
        if (directory != 0) != (lookup.node.kind == MountNodeKind::Directory) {
            return Err(if directory == 0 {
                libc::EISDIR
            } else {
                libc::ENOTDIR
            });
        }
        context.unlink_measured(&path, lookup, || {
            context.source.remove_with_meter(&path, Some(lookup.node.file_id), work.native)
        }, work)?;
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
    let _span = callback_span!(DEBUG, rename);
    ffi_status(address, &_span, |context, work| {
        if flags & !RENAME_NOREPLACE != 0 {
            return Err(libc::EOPNOTSUPP);
        }
        let (source, destination) = (mount_path_measured(source, work)?, mount_path_measured(destination, work)?);
        let replace = flags & RENAME_NOREPLACE == 0;
        let rename = || context.source.rename_with_meter(&source, &destination, replace, work.native);
        // Replacing the destination unlinks it.
        match replace.then(|| context.lookup_measured(&destination, work)) {
            Some(Ok(replaced)) => context.unlink_measured(&destination, replaced, rename, work)?,
            None | Some(Err(libc::ENOENT)) => context.mutate_measured(rename, work)?,
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
    let _span = callback_span!(TRACE, link);
    ffi_status(address, &_span, |context, work| {
        let (source, destination) = (mount_path_measured(source, work)?, mount_path_measured(destination, work)?);
        context.mutate_measured(|| context.source.hard_link_with_meter(&source, &destination, work.native), work)?;
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
    let _span = callback_span!(TRACE, symlink);
    ffi_status(address, &_span, |context, work| {
        // SAFETY: the bridge forwards FUSE's NUL-terminated link target, valid for
        // the callback.
        let target = c_bytes_measured(target, work)?;
        let target = work.copy_bytes(target)?;
        let destination = mount_path_measured(destination, work)?;
        context.mutate_measured(|| {
            context.source.create_symbolic_link_with_meter(
                &destination,
                target.into_chunk(),
                create_metadata(0o777, mode::IFLNK, uid, gid), work.native)
        }, work)?;
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
    let _span = callback_span!(TRACE, readlink);
    ffi_status(address, &_span, |context, work| {
        if length == 0 {
            return Err(libc::ERANGE);
        }
        let chunk = context.source.read_link_with_meter(&mount_path_measured(path, work)?, work.native)
            .map_err(|error| work.source_error(error))?;
        let target = work.source_chunk(chunk)?;
        if target.len() >= length {
            return Err(libc::ENAMETOOLONG);
        }
        // SAFETY: the bridge buffer holds `length` bytes and `target.len() < length`,
        // so the copy and its NUL terminator stay in bounds.
        let copied = crate::WorkCounters { bytes_copied: target.len() as u64 + 1, ..crate::WorkCounters::UNCHARGED };
        work.copying(copied, || unsafe {
            ptr::copy_nonoverlapping(target.as_ptr(), buffer.cast(), target.len());
            buffer.add(target.len()).write(0);
        })?;
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
    let _span = callback_span!(TRACE, mknod);
    ffi_status(address, &_span, |context, work| {
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
        let path = mount_path_measured(path, work)?;
        context.mutate_measured(|| {
            context.source.create_special_with_meter(
                &path,
                kind,
                device,
                create_metadata(mode, mode & mode::IFMT, uid, gid), work.native)
        }, work)?;
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
    let _span = callback_span!(DEBUG, chmod, fh = handle);
    ffi_status(address, &_span, |context, work| {
        context.mutate_metadata_measured(&mount_path_measured(path, work)?, handle, |metadata| {
            let kind = metadata_or(metadata.posix_mode, 0) & mode::IFMT;
            metadata.posix_mode = MetadataField::Value(kind | (mode & 0o7777));
            Ok(())
        }, work)?;
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
    let _span = callback_span!(DEBUG, chown, fh = handle);
    ffi_status(address, &_span, |context, work| {
        context.mutate_metadata_measured(&mount_path_measured(path, work)?, handle, |metadata| {
            if uid != u32::MAX {
                metadata.posix_uid = MetadataField::Value(uid);
            }
            if gid != u32::MAX {
                metadata.posix_gid = MetadataField::Value(gid);
            }
            Ok(())
        }, work)?;
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
    let _span = callback_span!(DEBUG, utimens, fh = handle);
    ffi_status(address, &_span, |context, work| {
        // SAFETY: the bridge passes null (rejected here) or a `NativeTimes` valid for
        // the callback.
        let times = unsafe { times.as_ref() }.ok_or(libc::EINVAL)?;
        context.mutate_metadata_measured(&mount_path_measured(path, work)?, handle, |metadata| {
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
        }, work)?;
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
    let _span = callback_span!(TRACE, getxattr);
    ffi_status(address, &_span, |context, work| {
        let path = mount_path_measured(path, work)?;
        if !context.may_have_named_attributes_measured(&path, work)? {
            return Err(libc::ENOATTR);
        }
        let chunk = context.source.read_attribute_with_meter(&path, c_bytes_measured(name, work)?, work.native)
            .map_err(|error| work.source_error(error))?
            .ok_or(libc::ENOATTR)?;
        let bytes = work.source_chunk(chunk)?;
        copy_variable_result_measured(&bytes, value, length, work)
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
    let _span = callback_span!(TRACE, setxattr);
    ffi_status(address, &_span, |context, work| {
        if length > MAXIMUM_CALLBACK_BYTES {
            return Err(libc::E2BIG);
        }
        let mode = match flags {
            0 => MountAttributeWriteMode::Upsert,
            libc::XATTR_CREATE => MountAttributeWriteMode::Create,
            libc::XATTR_REPLACE => MountAttributeWriteMode::Replace,
            _ => return Err(libc::EINVAL),
        };
        // SAFETY: the bridge forwards FUSE's `length`-byte attribute value, readable
        // for the callback.
        let bytes = unsafe { std::slice::from_raw_parts(value.cast::<u8>(), length) };
        let (path, name) = (mount_path_measured(path, work)?, c_bytes_measured(name, work)?);
        let copied = work.copy_bytes(bytes)?;
        context.mutate_measured(|| {
            context.source.write_attribute_with_meter(&path, name, copied.into_chunk(), mode, work.native)
        }, work)?;
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
    let _span = callback_span!(TRACE, listxattr);
    ffi_status(address, &_span, |context, work| {
        let path = mount_path_measured(path, work)?;
        if !context.may_have_named_attributes_measured(&path, work)? {
            return copy_variable_result_measured(&[], list, length, work);
        }
        let mut cursor: Option<(Vec<u8>, crate::NativeAllocation)> = None;
        let mut encoded = DarwinTransient::<Vec<u8>>::empty(work);
        loop {
            let mut page = context.source.list_attributes_with_meter(&path, cursor.as_ref().map(|cursor| cursor.0.as_slice()), ATTRIBUTE_PAGE_SIZE, work.native)
                .map_err(|error| work.source_error(error))?;
            for name in &page.0.names {
                for byte in name {
                    work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                    if *byte == 0 { return Err(libc::EIO); }
                }
                let required = encoded.len().checked_add(name.len()).and_then(|value| value.checked_add(1)).ok_or(libc::EOVERFLOW)?;
                if required > MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES { return Err(libc::E2BIG); }
                encoded.reserve(required, work)?;
                let copied = crate::WorkCounters { bytes_copied: name.len() as u64 + 1, ..crate::WorkCounters::UNCHARGED };
                work.copying(copied, || {
                    encoded.extend_from_slice(name);
                    encoded.push(0);
                })?;
            }
            match page.0.next_cursor.take() {
                Some(next) => {
                    let bytes = next.capacity() as u64;
                    let allocation = page.1.split_off(bytes).map_err(|error| work.native_error(error.into()))?;
                    let next = (next, allocation);
                    if let Some(cursor) = &cursor {
                        let mut equal = cursor.0.len() == next.0.len();
                        if equal {
                            for (left, right) in cursor.0.iter().zip(next.0.iter()) {
                                work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                                if left != right { equal = false; break; }
                            }
                        }
                        if equal { return Err(libc::EIO); }
                    }
                    cursor = Some(next);
                }
                None => break,
            }
        }
        copy_variable_result_measured(&encoded, list, length, work)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_removexattr(
    address: usize,
    path: *const c_char,
    name: *const c_char,
) -> c_int {
    let _span = callback_span!(TRACE, removexattr);
    ffi_status(address, &_span, |context, work| {
        let (path, name) = (mount_path_measured(path, work)?, c_bytes_measured(name, work)?);
        context.mutate_measured(|| context.source.remove_attribute_with_meter(&path, name, work.native), work)?;
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
    let _span = callback_span!(TRACE, lseek, fh = handle, offset = offset);
    ffi_offset(address, &_span, |context, work| {
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let target = match whence {
            libc::SEEK_DATA => MountSeekTarget::Data,
            libc::SEEK_HOLE => MountSeekTarget::Hole,
            _ => return Err(libc::EINVAL),
        };
        let result = context.file_or_open_measured(&mount_path_measured(path, work)?, handle, work)?
            .seek_with_meter(offset, target, work.native)
            .map_err(|error| work.source_error(error))?
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
    let _span = callback_span!(TRACE, fallocate, fh = handle, offset = offset, len = length);
    ffi_status(address, &_span, |context, work| {
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
        context.mutate_file_measured(&mount_path_measured(path, work)?, handle, |file| {
            file.allocate_range_with_meter(offset, length, operation, work.native)
        }, work)?;
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
    let _span = callback_span!(TRACE, copy_file_range, fh = source_handle);
    ffi_offset(address, &_span, |context, work| {
        if flags != 0 {
            return Err(libc::EINVAL);
        }
        let source_offset = u64::try_from(source_offset).map_err(|_| libc::EINVAL)?;
        let destination_offset = u64::try_from(destination_offset).map_err(|_| libc::EINVAL)?;
        let length = u64::try_from(length).map_err(|_| libc::EOVERFLOW)?;
        let source = context.file_or_open_measured(&mount_path_measured(source_path, work)?, source_handle, work)?;
        let length = context.mutate_file_measured(
            &mount_path_measured(destination_path, work)?,
            destination_handle,
            |destination| {
                let source_lookup = source.lookup_with_meter(work.native)?;
                let destination_lookup = destination.lookup_with_meter(work.native)?;
                let length = length.min(
                    source_lookup
                        .node
                        .logical_bytes
                        .saturating_sub(source_offset),
                );
                context.source.clone_range_by_id_with_meter(
                    source_lookup.node.file_id,
                    source_offset,
                    destination_lookup.node.file_id,
                    destination_offset,
                    length, work.native)?;
                Ok(length)
            }, work)?;
        i64::try_from(length).map_err(|_| libc::EOVERFLOW)
    })
}

fn next_native_component<'a>(bytes: &'a [u8], position: &mut usize, work: &DarwinCallbackWork<'_>) -> Result<Option<&'a [u8]>, i32> {
    while *position < bytes.len() {
        let start = *position;
        let mut end = start;
        while *position < bytes.len() {
            work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            let byte = bytes[*position];
            *position += 1;
            if byte == b'/' { end = *position - 1; break; }
            end = *position;
        }
        if end > start { return Ok(Some(&bytes[start..end])); }
    }
    Ok(None)
}

fn mount_path_measured(path: *const c_char, work: &DarwinCallbackWork<'_>) -> Result<DarwinOwned<MountPath>, i32> {
    let bytes = c_bytes_measured(path, work)?;
    if bytes.first().copied() != Some(b'/') { return Err(libc::EINVAL); }
    let mut count = 0usize;
    let mut position = 1;
    while let Some(component) = next_native_component(bytes, &mut position, work)? {
        if component == b"." || component == b".." { return Err(libc::EPERM); }
        count = count.checked_add(1).ok_or(libc::EOVERFLOW)?;
    }
    let allocated = count.checked_mul(std::mem::size_of::<Vec<u8>>()).ok_or(libc::EOVERFLOW)?;
    let mut path = work.owned(allocated as u64, || {
        let mut components = Vec::new();
        components.try_reserve_exact(count)?;
        Ok(MountPath { components })
    })?;
    position = 1;
    while let Some(component) = next_native_component(bytes, &mut position, work)? {
        let copied = crate::WorkCounters { bytes_copied: component.len() as u64, ..crate::WorkCounters::UNCHARGED };
        work.admit(copied)?;
        path.claim_more(component.len() as u64, work)?;
        let mut name = Vec::new();
        name.try_reserve_exact(component.len()).map_err(|error| work.native_error(NativeMountError::Allocation(error)))?;
        work.copying(copied, || {
            name.extend_from_slice(component);
            path.components.push(name);
        })?;
    }
    Ok(path)
}

fn c_bytes_measured<'a>(value: *const c_char, work: &DarwinCallbackWork<'_>) -> Result<&'a [u8], i32> {
    if value.is_null() { return Err(libc::EINVAL); }
    let mut length = 0usize;
    loop {
        work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        // SAFETY: bridge strings are terminated and remain live through this callback.
        if unsafe { value.add(length).read() } == 0 { break; }
        length = length.checked_add(1).ok_or(libc::EOVERFLOW)?;
    }
    // SAFETY: the admitted scan proved this exact live prefix precedes the terminator.
    Ok(unsafe { std::slice::from_raw_parts(value.cast(), length) })
}
    if value.is_null() {
        return Err(libc::EINVAL);
    }
    // SAFETY: callers pass only bridge strings, which FUSE NUL-terminates and keeps
    // valid for the callback; null was rejected above.
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

fn copy_variable_result_measured(bytes: &[u8], output: *mut c_char, length: usize, work: &DarwinCallbackWork<'_>) -> Result<c_int, i32> {
    if length == 0 {
        return c_int::try_from(bytes.len()).map_err(|_| libc::EOVERFLOW);
    }
    if output.is_null() || bytes.len() > length {
        return Err(libc::ERANGE);
    }
    // SAFETY: `output` is a non-null bridge buffer of `length` bytes and
    // `bytes` fits in it (checked above).
    let copied = crate::WorkCounters { bytes_copied: bytes.len() as u64, ..crate::WorkCounters::UNCHARGED };
    work.copying(copied, || unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), output.cast(), bytes.len()) })?;
    c_int::try_from(bytes.len()).map_err(|_| libc::EOVERFLOW)
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

fn driver_errno(error: i32) -> NativeMountError {
    NativeMountError::Io(std::io::Error::from_raw_os_error(error))
}

#[cfg(test)]
mod tests {

    #[test]
    fn filtered_callback_error_does_not_overwrite_parent_errno() {
        crate::obs::assert_filtered_callback_isolation(|span| {
            let status = super::ffi_status(0, span, |_| Ok(0));
            assert_eq!(status, -libc::ESTALE);
            -status
        });
    }
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

    #[test]
    fn native_startup_buffers_exact_capacity_denial_and_cancellation() -> TestResult {
        let cancellation = crate::CancellationToken::new();
        let path = Path::new("/native-buffer-test");
        let full = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let buffers = DarwinStartupBuffers::new(path, false, true, &full)?;
        assert_eq!(buffers.arguments.len(), 7);
        assert_eq!(buffers.pointers.len(), 7);
        assert!(buffers.arguments.iter().zip(&buffers.pointers)
            .all(|(argument, pointer)| argument.as_ptr() as usize == *pointer));
        let operations = full.receipt().allocation_operations;
        assert!(operations >= 10, "destination, argv and pointer buffers own actual allocation guards");
        assert_eq!(full.receipt().backend_read_operations, 0);
        assert_eq!(full.receipt().backend_write_operations, 0);
        let ceiling = full.receipt().peak_allocation_bytes;
        drop(buffers);
        full.admit(crate::WorkCounters::UNCHARGED)?;
        assert_eq!(full.live_allocation_bytes(), crate::NativeMaterializationWork::accounting_state_bytes());
        let exact = crate::NativeMaterializationWork::new(crate::WorkBudget {
            allocation_operations: operations, peak_allocation_bytes: ceiling, ..crate::WorkBudget::UNBOUNDED
        }, &cancellation).map_err(|(cause, _)| cause)?;
        drop(DarwinStartupBuffers::new(path, false, true, &exact)?);
        exact.admit(crate::WorkCounters::UNCHARGED)?;
        assert_eq!(exact.receipt().allocation_operations, operations);
        assert_eq!(exact.live_allocation_bytes(), crate::NativeMaterializationWork::accounting_state_bytes());
        let denied = crate::NativeMaterializationWork::new(crate::WorkBudget {
            allocation_operations: operations - 1, ..crate::WorkBudget::UNBOUNDED
        }, &cancellation).map_err(|(cause, _)| cause)?;
        assert!(matches!(DarwinStartupBuffers::new(path, false, true, &denied),
            Err(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(_)))));
        denied.admit(crate::WorkCounters::UNCHARGED)?;
        assert_eq!(denied.receipt().allocation_operations, operations - 1);
        assert_eq!(denied.live_allocation_bytes(), crate::NativeMaterializationWork::accounting_state_bytes());
        let cancelled = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let cancelled_prefix = cancelled.receipt();
        cancellation.cancel();
        assert!(matches!(DarwinStartupBuffers::new(path, false, true, &cancelled),
            Err(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Cancelled))));
        assert_eq!(cancelled.receipt(), cancelled_prefix);
        Ok(())
    }

    #[test]
    fn interrupted_callback_barrier_retains_slot_and_retry_covers_new_callbacks() -> TestResult {
        let gate = CallbackGate::new();
        let before = gate.enter();
        let cancellation = crate::CancellationToken::new();
        let denied = crate::NativeMaterializationWork::new(crate::WorkBudget { items_examined: 1,
            backend_read_operations: 0, ..crate::WorkBudget::UNBOUNDED }, &cancellation)
            .map_err(|(cause, _)| cause)?;
        assert!(matches!(gate.drain_measured(&denied), Err(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(_)))));
        assert_eq!(*gate.draining.lock().unwrap_or_else(PoisonError::into_inner), Some(0));
        assert_eq!(denied.receipt().items_examined, 1);
        assert_eq!(denied.receipt().backend_read_operations, 0);
        let after = gate.enter();
        drop(before);
        let retry = crate::NativeMaterializationWork::new(crate::WorkBudget { items_examined: 1,
            backend_read_operations: 0, ..crate::WorkBudget::UNBOUNDED }, &cancellation)
            .map_err(|(cause, _)| cause)?;
        assert!(matches!(gate.drain_measured(&retry), Err(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(_)))));
        assert_eq!(retry.receipt().items_examined, 1, "only the old retained slot was physically examined");
        assert_eq!(*gate.draining.lock().unwrap_or_else(PoisonError::into_inner), Some(1));
        drop(after);
        let exact = crate::NativeMaterializationWork::new(crate::WorkBudget { items_examined: 2,
            backend_read_operations: 0, ..crate::WorkBudget::UNBOUNDED }, &cancellation)
            .map_err(|(cause, _)| cause)?;
        gate.drain_measured(&exact)?;
        assert_eq!(exact.receipt().items_examined, 2, "retained slot and this retry's actual cutoff");
        assert_eq!(exact.receipt().backend_read_operations, 0);
        assert!(gate.draining.lock().unwrap_or_else(PoisonError::into_inner).is_none());
        Ok(())
    }

    #[test]
    fn denied_actual_expiry_clock_never_marks_external_changes_settled() -> TestResult {
        let (_, context) = checkout_context(MountPublication::Manual)?;
        context.around.made.store(1, Ordering::Release);
        let cancellation = crate::CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(crate::WorkBudget { backend_read_operations: 0,
            ..crate::WorkBudget::UNBOUNDED }, &cancellation).map_err(|(cause, _)| cause)?;
        let result = context.revalidate_with_meter(&original.native_mount_work());
        assert!(matches!(result, Err(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(_)))));
        assert_eq!(original.receipt().backend_read_operations, 0, "denied clock was not queried");
        assert_eq!(original.receipt().items_examined, 1, "actual idle callback generation was examined");
        assert_eq!(context.around.settled.load(Ordering::Acquire), 0);
        assert_eq!(context.around.made.load(Ordering::Acquire), 1);
        assert!(context.callbacks.draining.lock().unwrap_or_else(PoisonError::into_inner).is_none());
        let cancelled = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let cancelled_prefix = cancelled.receipt();
        cancellation.cancel();
        assert!(matches!(context.revalidate_with_meter(&cancelled.native_mount_work()),
            Err(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Cancelled))));
        assert_eq!(cancelled.receipt(), cancelled_prefix);
        assert_eq!(context.around.settled.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[test]
    #[ignore = "requires the native macOS mount service"]
    fn startup_sdk_resource_is_retained_when_actual_thread_spawn_is_denied() -> TestResult {
        acyclic_native_runtime::initialize_native_io_services()?;
        let (source, _) = checkout_context(MountPublication::Manual)?;
        let temporary = tempfile::tempdir()?;
        let request = NativeMountRequest { mount_id: crate::MountId::new(),
            volume_id: source.volume_id()?, destination: temporary.path().to_path_buf(), writable: true };
        let cancellation = crate::CancellationToken::new();
        let prefix = crate::NativeMaterializationWork::new(crate::WorkBudget { backend_write_operations: 1,
            ..crate::WorkBudget::UNBOUNDED }, &cancellation).map_err(|(cause, _)| cause)?;
        let mut failure = match DarwinMountSession::start_with_work(&request, source, &prefix) {
            Err(failure) => failure,
            Ok(mut session) => { session.stop()?; panic!("the real thread-spawn leaf exceeded its admitted budget"); }
        };
        assert!(matches!(failure.error, NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(_))));
        assert_eq!(prefix.receipt().backend_write_operations, 1, "only actual SDK session construction occurred");
        assert!(failure.preserve_destination_fence);
        let Some(super::super::DriverSession::DarwinMount(mut session)) = failure.retained_driver.take() else {
            panic!("the actual allocated SDK resource must remain in the returned driver");
        };
        assert!(session.thread.is_none());
        assert_ne!(session.resources()?.driver_session, 0);
        assert!(!session.teardown_complete);
        let denied = crate::NativeMaterializationWork::new(crate::WorkBudget { backend_write_operations: 2,
            ..crate::WorkBudget::UNBOUNDED }, &cancellation).map_err(|(cause, _)| cause)?;
        assert!(matches!(session.stop_measured(&denied),
            Err(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(_)))));
        assert_eq!(denied.receipt().backend_write_operations, 2, "real probe descriptor close and SDK interrupt");
        assert!(session.resources.is_some(), "denied SDK free does not relinquish resource custody");
        let cleanup = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        session.stop_measured(&cleanup)?;
        assert!(session.teardown_complete);
        assert!(session.resources.is_none());
        assert!(session.namespace_detach_confirmed());
        Ok(())
    }

    #[test]
    fn actual_native_payload_drop_retires_original_claim_after_observer_is_sealed() -> TestResult {
        let cancellation = crate::CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let mut retained = None;
        with_detach_work(&original, |meter| {
            let copied = b"retained native task bytes";
            meter.with_native(|work| {
                let allocation = work.claim(copied.len() as u64)?;
                let mut buffer = Vec::new();
                buffer.try_reserve_exact(copied.len())?;
                buffer.extend_from_slice(copied);
                retained = Some((buffer, Arc::clone(meter), allocation));
                Ok(())
            })
        })?;
        let (buffer, meter, allocation) = retained.take().ok_or("native payload owner absent")?;
        assert!(meter.work.lock().unwrap_or_else(PoisonError::into_inner).is_none());
        let live_before = original.live_allocation_bytes();
        let bytes = buffer.len() as u64;
        drop(buffer);
        drop(allocation);
        assert_eq!(original.live_allocation_bytes(), live_before - bytes,
            "sealed loan still has same original physical retirement proof");
        drop(meter);
        original.admit(crate::WorkCounters::UNCHARGED)?;
        assert!(original.live_allocation_bytes() >= std::mem::size_of::<DetachReadMeter>() as u64,
            "the real original known-control Weak still retains its Arc backing");
        Ok(())
    }

    #[test]
    fn mount_table_probe_denies_each_real_leaf_and_releases_native_buffers() -> TestResult {
        let temporary = tempfile::tempdir()?;
        let cancellation = crate::CancellationToken::new();
        let mut prefix = DemandWork::new(crate::WorkBudget {
            backend_read_operations: 4, backend_write_operations: 1,
            ..crate::WorkBudget::UNBOUNDED
        }, &cancellation);
        assert!(matches!(is_mounted_measured(temporary.path(), &mut prefix), Err(NativeMountError::Work(_))));
        assert_eq!(prefix.work.backend_read_operations, 4, "count call occurred, denied fill did not");
        assert_eq!(prefix.work.backend_write_operations, 1, "the owned parent descriptor really closed");
        assert_eq!(prefix.work.allocation_operations, 3, "CString, F_GETPATH buffer and mount array");
        assert_eq!(prefix.live_allocation_bytes(), 0);
        let parent_bytes = temporary.path().parent().ok_or("parent absent")?.as_os_str().len() as u64 + 1;
        let mut memory = DemandWork::new(crate::WorkBudget {
            peak_allocation_bytes: parent_bytes + libc::PATH_MAX as u64 - 1,
            ..crate::WorkBudget::UNBOUNDED
        }, &cancellation);
        assert!(matches!(is_mounted_measured(temporary.path(), &mut memory), Err(NativeMountError::Work(_))));
        assert_eq!(memory.work.backend_read_operations, 2, "F_GETPATH waits for actual buffer admission");
        assert_eq!(memory.work.backend_write_operations, 1);
        assert_eq!(memory.work.allocation_operations, 1);
        assert_eq!(memory.live_allocation_bytes(), 0);
        let mut exact = DemandWork::new(crate::WorkBudget {
            backend_read_operations: 5, backend_write_operations: 1,
            ..crate::WorkBudget::UNBOUNDED
        }, &cancellation);
        assert!(!is_mounted_measured(temporary.path(), &mut exact)?);
        assert_eq!(exact.work.backend_read_operations, 5);
        assert_eq!(exact.work.backend_write_operations, 1);
        assert_eq!(exact.live_allocation_bytes(), 0);
        cancellation.cancel();
        let mut cancelled = DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
        assert!(is_mounted_measured(temporary.path(), &mut cancelled).is_err());
        assert_eq!(cancelled.work, crate::WorkCounters::UNCHARGED);
        Ok(())
    }

    #[test]
    fn recovery_control_publishes_only_actual_namespace_absence_evidence() -> TestResult {
        let temporary = tempfile::tempdir()?;
        let cancellation = crate::CancellationToken::new();
        let mut control = DarwinRecoveryControl::default();
        assert!(!control.namespace_detach_confirmed());
        let denied = DemandWork::new(crate::WorkBudget {
            backend_read_operations: 0, ..crate::WorkBudget::UNBOUNDED
        }, &cancellation);
        let denied = super::super::NativeMountMeterWork::from_meter(Arc::new(NativeDemandReadMeter(Mutex::new(denied))));
        assert!(matches!(control.recover_measured(temporary.path(), &denied),
            Err(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(_)))));
        assert!(!control.namespace_detach_confirmed(), "denied work is not an absence proof");
        assert_eq!(denied.meter().lock().work.backend_read_operations, 0);
        assert_eq!(denied.meter().lock().work.backend_write_operations, 0);
        let exact = DemandWork::new(crate::WorkBudget {
            backend_read_operations: 5, backend_write_operations: 1,
            ..crate::WorkBudget::UNBOUNDED
        }, &cancellation);
        let exact = super::super::NativeMountMeterWork::from_meter(Arc::new(NativeDemandReadMeter(Mutex::new(exact))));
        control.recover_measured(temporary.path(), &exact)?;
        assert!(control.namespace_detach_confirmed());
        assert!(!control.controls_pending());
        assert_eq!(exact.meter().lock().work.backend_read_operations, 5);
        assert_eq!(exact.meter().lock().work.backend_write_operations, 1);
        assert_eq!(exact.meter().lock().live_allocation_bytes(),
            (std::mem::size_of::<DetachReadMeter>() + std::mem::size_of::<AtomicU64>()) as u64,
            "actual original known-control Weak and its marker still retain backing");
        Ok(())
    }

    #[test]
    fn direct_native_detach_uses_exact_task_carriers_and_original_cancellation() -> TestResult {
        // Inline admission makes queue contention irrelevant without replacing
        // either the measured task machinery or the actual unmount syscall.
        let _inline = acyclic_native_runtime::InlineBlocking::enter();
        let temporary = tempfile::tempdir()?;
        let cancellation = crate::CancellationToken::new();
        let mut state = UnmountState::default();
        let baseline = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let baseline_error = with_detach_work(&baseline, |meter|
            bounded_direct_unmount(temporary.path(), &mut state, meter, None))
            .expect_err("a real ordinary directory is not a mount to detach");
        assert!(matches!(baseline_error, NativeMountError::Io(_)));
        assert_eq!(baseline.receipt().backend_write_operations, 1);
        assert_eq!(baseline.receipt().backend_read_operations, 0);
        assert_eq!(baseline.live_allocation_bytes(),
            crate::NativeMaterializationWork::accounting_state_bytes()
                + (std::mem::size_of::<DetachReadMeter>() + std::mem::size_of::<AtomicU64>()) as u64);
        assert!(state.direct.is_none());
        let exact = crate::NativeMaterializationWork::new(crate::WorkBudget {
            backend_write_operations: 1, backend_read_operations: 0,
            allocation_operations: baseline.receipt().allocation_operations,
            peak_allocation_bytes: baseline.receipt().peak_allocation_bytes,
            items_examined: baseline.receipt().items_examined,
            bytes_copied: baseline.receipt().bytes_copied,
            ..crate::WorkBudget::UNBOUNDED
        }, &cancellation).map_err(|(cause, _)| cause)?;
        let exact_error = with_detach_work(&exact, |meter|
            bounded_direct_unmount(temporary.path(), &mut state, meter, None))
            .expect_err("the actual syscall must preserve its non-mount error");
        assert!(matches!(exact_error, NativeMountError::Io(_)));
        assert_eq!(exact.receipt(), baseline.receipt());
        let denied = crate::NativeMaterializationWork::new(crate::WorkBudget {
            backend_write_operations: 0, ..crate::WorkBudget::UNBOUNDED
        }, &cancellation).map_err(|(cause, _)| cause)?;
        assert!(matches!(with_detach_work(&denied, |meter|
            bounded_direct_unmount(temporary.path(), &mut state, meter, None)),
            Err(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(_)))));
        assert_eq!(denied.receipt().backend_write_operations, 0);
        assert_eq!(denied.live_allocation_bytes(),
            crate::NativeMaterializationWork::accounting_state_bytes()
                + (std::mem::size_of::<DetachReadMeter>() + std::mem::size_of::<AtomicU64>()) as u64);
        let cancelled = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let cancelled_prefix = cancelled.receipt();
        cancellation.cancel();
        assert!(with_detach_work(&cancelled, |meter|
            bounded_direct_unmount(temporary.path(), &mut state, meter, None)).is_err());
        assert_eq!(cancelled.receipt(), cancelled_prefix);
        Ok(())
    }

    #[test]
    #[ignore = "requires a live macOS NFS mount"]
    fn macos_measured_stop_keeps_real_driver_and_fence_after_denied_probe() -> TestResult {
        acyclic_native_runtime::initialize_native_io_services()?;
        let (temporary, _, mut mount) = live_mount()?;
        use std::os::unix::fs::MetadataExt as _;
        let mounted = std::fs::metadata(temporary.path())?;
        let authenticated = mount.native_root_identity().ok_or("mounted root observation absent")?;
        assert_eq!((authenticated.device, authenticated.object), (mounted.dev(), mounted.ino()));
        let cancellation = crate::CancellationToken::new();
        let failure = mount.stop_measured(crate::WorkBudget {
            backend_read_operations: 0, ..crate::WorkBudget::UNBOUNDED
        }, &cancellation).expect_err("first physical probe is denied");
        assert!(matches!(failure.0, NativeMountError::Work(_)));
        assert_eq!(failure.1.backend_read_operations, 0);
        assert_eq!(failure.1.backend_write_operations, 0);
        std::fs::write(temporary.path().join("still-served"), b"same live driver")?;
        assert_eq!(std::fs::read(temporary.path().join("still-served"))?, b"same live driver");
        let stopped = mount.stop_measured(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        assert!(stopped.value);
        assert!(stopped.work.backend_read_operations >= 5);
        assert!(stopped.work.backend_write_operations >= 2);
        assert!(mount.native_root_identity().is_none());
        Ok(())
    }

    /// A writable context over a fresh POSIX checkout.
    fn checkout_context(
        publication: MountPublication,
    ) -> Result<(Arc<MemorySource>, DarwinMountContext), Box<dyn std::error::Error>> {
        use crate::Fs;
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, Lifecycle};
        use crate::native_mount::SharedCheckout;
        acyclic_native_runtime::initialize_native_io_services()?;

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
        )?;
        Ok((source, context))
    }

    #[test]
    fn installed_native_original_without_resource_keeps_quota_and_cancellation() -> TestResult {
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, Lifecycle};
        acyclic_native_runtime::initialize_native_io_services()?;
        let mut config = crate::model::VolumeConfig::portable(Lifecycle::Ephemeral);
        config.profile = FilesystemProfile::Posix;
        let fs = crate::Fs::memory();
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let checkout = runtime.block_on(async {
            let cancellation = crate::CancellationToken::new();
            let volume = fs.create_volume(config, crate::WorkBudget::UNBOUNDED, &cancellation).await?.value;
            volume.checkout(GenerationSelector::Head, CheckoutMode::tracking_transaction(),
                crate::WorkBudget::UNBOUNDED, &cancellation).await.map(|receipt| receipt.value)
        })?;
        let shared = Arc::new(super::super::SharedCheckout::new(checkout));
        let ordinary = MemorySource::new(Arc::clone(&shared), config)?;
        let root_id = ordinary.lookup(&MountPath::root())?.ok_or("root absent")?.node.file_id;
        let cancellation = crate::CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(crate::WorkBudget {
            items_examined: 0, ..crate::WorkBudget::UNBOUNDED
        }, &cancellation).map_err(|(cause, _)| cause)?;
        let source = MemorySource::new_at_meter(
            shared, config, crate::kernel::NamespacePath::root(),
            original.claim(0)?, &original.native_mount_work(),
        )?;
        let source = source.into_arc_meter(&original.native_mount_work())?;
        let context = DarwinMountContext::new_owner(
            source, true, 0, 0, Vec::from([(root_id, ROOT_INODE)]),
            crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
        )?;
        assert!(context.startup_meter.lock().unwrap_or_else(PoisonError::into_inner).upgrade().is_none());
        let selected = context.callback_meter()?.ok_or("installed native original absent")?;
        assert!(selected.resource_allowance().is_none(), "membership is not a resource heuristic");
        assert!(Arc::ptr_eq(selected.meter(), original.native_mount_work().meter()));
        assert!(matches!(selected.native_materialization_work().charge(crate::WorkCounters {
            items_examined: 1, ..crate::WorkCounters::UNCHARGED
        }), Err(crate::NativeTreeMaterializationError::Work(_))));
        assert_eq!(context.handler_failure.lock().unwrap_or_else(PoisonError::into_inner)
            .completed_future_request_work, crate::WorkCounters::UNCHARGED);
        let prefix = original.receipt();
        cancellation.cancel();
        assert!(context.callback_meter().is_err());
        assert_eq!(original.receipt(), prefix, "revoked original performs no SDK work");
        Ok(())
    }

    fn os(error: i32) -> std::io::Error {
        std::io::Error::from_raw_os_error(error)
    }

    /// Complete finite policies reach the installed raw callbacks, not a
    /// substitute source operation or a fabricated failure producer.
    fn callback_policy() -> crate::WorkBudget {
        crate::WorkBudget {
            authority_records_read: 1_000_000, authority_records_appended: 1_000_000,
            authority_bytes_read: 16_000_000, authority_bytes_written: 16_000_000,
            object_probes: 1_000_000, backend_read_operations: 1_000_000,
            backend_write_operations: 1_000_000, durability_operations: 1_000_000,
            page_reads: 1_000_000, page_writes: 1_000_000,
            object_bytes_read: 16_000_000, object_bytes_written: 16_000_000,
            bytes_hashed: 16_000_000, bytes_copied: 16_000_000, bytes_encoded: 16_000_000,
            source_bytes_read: 16_000_000, source_path_components: 1_000_000,
            source_entries_visited: 1_000_000, output_bytes: 16_000_000,
            items_examined: 1_000_000, items_returned: 1_000_000,
            allocation_operations: 1_000_000, peak_allocation_bytes: 16_000_000,
            materializations: 1_000_000,
        }
    }

    fn callback_context(source: &Arc<MemorySource>, budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken) -> Result<Arc<DarwinMountContext>, Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let root_id = source.lookup(&MountPath::root())?.ok_or("root absent")?.node.file_id;
        let metadata = std::fs::metadata(".")?;
        // Administrative fixture setup; the policy/token installed for every
        // subsequent real callback are the exact incoming arguments.
        Ok(Arc::new(DarwinMountContext::new_owner(
            Arc::clone(source) as Arc<dyn MountFilesystem>, true, metadata.uid(), metadata.gid(),
            Vec::from([(root_id, ROOT_INODE)]), budget, cancellation,
        )?))
    }

    #[test]
    fn raw_getattr_preserves_exact_finite_path_scan_prefix_and_sticky_failure() -> TestResult {
        let (source, _) = checkout_context(MountPublication::Manual)?;
        let cancellation = crate::CancellationToken::new();
        let context = callback_context(&source, crate::WorkBudget { items_examined: 6, ..callback_policy() }, &cancellation)?;
        let path = CString::new("/proof")?;
        let mut output = std::mem::MaybeUninit::<NativeStat>::uninit();
        // SAFETY: context, terminated path, and writable result remain alive.
        let status = unsafe { acyclic_fs_darwin_mount_getattr(Arc::as_ptr(&context) as usize, path.as_ptr(), 0, output.as_mut_ptr()) };
        assert_eq!(status, -libc::EIO);
        let failure = context.handler_failure.lock().map_err(|_| "poisoned callback failure")?.first_fatal.take().ok_or("missing actual failure")?;
        assert!(matches!(failure.cause, NativeMountError::Work(crate::WorkError::BudgetExceeded { counter: "items_examined", .. })));
        assert_eq!(failure.work.items_examined, 6);
        assert_eq!(failure.work.bytes_copied, 0);
        assert!(context.handler_failed.load(Ordering::Acquire));
        let root = CString::new("/")?;
        // Draining diagnostics must not revive a physically incomplete context.
        assert_eq!(unsafe { acyclic_fs_darwin_mount_getattr(Arc::as_ptr(&context) as usize, root.as_ptr(), 0, output.as_mut_ptr()) }, -libc::EIO);
        Ok(())
    }

    #[test]
    fn raw_create_denies_actual_path_allocation_before_source_mutation() -> TestResult {
        let (source, _) = checkout_context(MountPublication::Manual)?;
        let cancellation = crate::CancellationToken::new();
        let context = callback_context(&source, crate::WorkBudget { allocation_operations: 0, ..callback_policy() }, &cancellation)?;
        let path = CString::new("/not-created")?;
        let mut handle = 0;
        // SAFETY: all native argument storage remains valid through the call.
        assert_eq!(unsafe { acyclic_fs_darwin_mount_create(Arc::as_ptr(&context) as usize,
            path.as_ptr(), 0o644, context.mount_uid, context.mount_gid, libc::O_RDWR, &mut handle) }, -libc::EIO);
        let failure = context.handler_failure.lock().map_err(|_| "poisoned callback failure")?.first_fatal.take().ok_or("missing allocation denial")?;
        assert!(matches!(failure.cause, NativeMountError::Work(_)));
        assert_eq!(failure.work.allocation_operations, 0);
        assert_eq!(failure.work.backend_write_operations, 0);
        assert_eq!(handle, 0);
        assert!(source.lookup(&MountPath::root().child(b"not-created".to_vec()))?.is_none());
        Ok(())
    }

    #[test]
    fn raw_getattr_uses_original_cancellation_and_does_not_poison_absence_probes() -> TestResult {
        let (source, _) = checkout_context(MountPublication::Manual)?;
        let cancellation = crate::CancellationToken::new();
        let context = callback_context(&source, callback_policy(), &cancellation)?;
        let path = CString::new("/later")?;
        let mut output = std::mem::MaybeUninit::<NativeStat>::uninit();
        let address = Arc::as_ptr(&context) as usize;
        // SAFETY: native storage and context are valid throughout each call.
        assert_eq!(unsafe { acyclic_fs_darwin_mount_getattr(address, path.as_ptr(), 0, output.as_mut_ptr()) }, -libc::ENOENT);
        let miss = context.handler_failure.lock().map_err(|_| "poisoned callback failure")?
            .completed_future_request_work;
        assert!(miss.items_examined > 0);
        assert!(context.handler_failure.lock().map_err(|_| "poisoned callback failure")?.first_fatal.is_none());
        assert!(!context.handler_failed.load(Ordering::Acquire));
        source.create_file(&MountPath::root().child(b"later".to_vec()), FileMetadata::default())?;
        assert_eq!(unsafe { acyclic_fs_darwin_mount_getattr(address, path.as_ptr(), 0, output.as_mut_ptr()) }, 0);
        cancellation.cancel();
        assert_eq!(unsafe { acyclic_fs_darwin_mount_getattr(address, path.as_ptr(), 0, output.as_mut_ptr()) }, -libc::ECANCELED);
        let failure = context.handler_failure.lock().map_err(|_| "poisoned callback failure")?.first_fatal.take().ok_or("missing original cancellation")?;
        assert!(matches!(failure.cause, NativeMountError::Demand(crate::DemandError::Cancelled)
            | NativeMountError::Materialization(crate::NativeTreeMaterializationError::Cancelled)
            | NativeMountError::Source(MountSourceError::Cancelled)));
        assert_eq!(failure.work.backend_read_operations, 0);
        assert!(context.handler_failure.lock().map_err(|_| "poisoned callback failure")?
            .completed_future_request_work.items_examined > miss.items_examined);
        Ok(())
    }

    #[test]
    fn actual_callback_panic_and_receipt_survive_physical_context_retirement() -> TestResult {
        let (source, _) = checkout_context(MountPublication::Manual)?;
        let cancellation = crate::CancellationToken::new();
        let context = callback_context(&source, callback_policy(), &cancellation)?;
        let status = context.run_request::<()>(|work| {
            work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            panic!("actual native callback panic");
        });
        assert_eq!(status, Err(libc::EIO));
        assert!(context.handler_failed.load(Ordering::Acquire));
        let completed = context.handler_failure.lock().map_err(|_| "poisoned callback failure")?
            .completed_future_request_work;
        assert_eq!(completed.items_examined, 1);
        let called = std::cell::Cell::new(false);
        assert_eq!(context.run_request(|_| { called.set(true); Ok(()) }), Err(libc::EIO));
        assert!(!called.get());
        let resources = Arc::new(DriverSessionResources {
            source_context: Arc::into_raw(context) as usize,
            driver_session: 0, startup: None, context_custody: None,
            _allocation: None,
        });
        let mut session = DarwinMountSession {
            resources: Some(resources),
            destination: PathBuf::new(), _destination_allocation: None,
            thread: None,
            events: Arc::new(LoopEvents::default()),
            loop_finished_before_teardown: Some(false),
            loop_result: Some(MountLoopResult::Exited(0)), teardown_complete: true,
            loop_panic: None,
            completed_callback_work: crate::WorkCounters::UNCHARGED,
            retired_callback_failure: Mutex::new(None), unmount: UnmountState::default(),
            native_root_identity: None,
        };
        let mut work = DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation);
        let cleanup = work.reserve_cleanup_write()?;
        session.release_resources_measured(cleanup, &mut work)?;
        assert!(session.resources.is_none());
        assert_eq!(work.work.backend_write_operations, 0, "the preexisting SDK pointer was already freed");
        assert_eq!(session.completed_callback_work(), completed);
        let failure = session.take_callback_failure().ok_or("missing retired actual panic")?;
        assert_eq!(failure.work.items_examined, 1);
        match failure.cause {
            NativeMountError::DriverPanicked { panic, .. } => {
                let payload = panic.into_inner().unwrap_or_else(PoisonError::into_inner);
                assert_eq!(payload.downcast_ref::<&str>(), Some(&"actual native callback panic"));
            }
            other => panic!("actual panic payload was replaced: {other:?}"),
        }
        assert!(session.take_callback_failure().is_none());
        assert_eq!(session.completed_callback_work(), completed);
        Ok(())
    }

    #[test]
    fn raw_getattr_uses_active_original_scope_instead_of_a_second_request_ledger() -> TestResult {
        let (source, _) = checkout_context(MountPublication::Manual)?;
        let cancellation = crate::CancellationToken::new();
        let context = callback_context(&source, crate::WorkBudget { items_examined: 0, ..callback_policy() }, &cancellation)?;
        let root = CString::new("/")?;
        let receipt = super::super::run_native_mount_handler(callback_policy(), &cancellation, |native| {
            let active = DarwinStartupMeterScope::existing(Arc::clone(&context), None, native)?;
            let mut output = std::mem::MaybeUninit::<NativeStat>::uninit();
            // SAFETY: the scope, context and argument storage remain alive.
            let status = unsafe { acyclic_fs_darwin_mount_getattr(Arc::as_ptr(&context) as usize, root.as_ptr(), 0, output.as_mut_ptr()) };
            // The original active loan is restored only after this is cleared.
            drop(active);
            if status != 0 { return Err(NativeMountError::Io(std::io::Error::from_raw_os_error(-status))); }
            Ok(())
        }).map_err(|failure| failure.cause)?;
        assert!(receipt.work.items_examined > 0);
        assert!(receipt.work.bytes_copied >= std::mem::size_of::<NativeStat>() as u64);
        let reports = context.handler_failure.lock().map_err(|_| "poisoned callback failure")?;
        assert!(reports.first_fatal.is_none());
        assert_eq!(reports.completed_future_request_work, crate::WorkCounters::UNCHARGED);
        Ok(())
    }

    #[test]
    fn materialization_errno_preserves_actual_sdk_absence_and_budget_failure_health() -> TestResult {
        for (kind, expected) in [
            (super::super::NativeSourceTextKind::Engine, libc::EIO),
            (super::super::NativeSourceTextKind::Invalid, libc::EINVAL),
            (super::super::NativeSourceTextKind::Unsupported, libc::EOPNOTSUPP),
        ] {
            let error: NativeMountError = MountSourceError::NativeStaticText { kind, text: "original" }.into();
            assert_eq!(darwin_handler_errno(&error), expected);
            assert!(!darwin_handler_is_fatal(&error));
        }
        let temporary = tempfile::tempdir()?;
        let io = std::fs::metadata(temporary.path().join("authentically-missing"))
            .expect_err("the actual SDK probe must report absence");
        let source = MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Io(io));
        assert_eq!(darwin_source_errno(&source), libc::ENOENT);
        let native: NativeMountError = source.into();
        assert_eq!(darwin_handler_errno(&native), libc::ENOENT);
        assert!(!darwin_handler_is_fatal(&native));
        let cancellation = crate::CancellationToken::new();
        let mut work = DemandWork::new(crate::WorkBudget {
            items_examined: 0, ..crate::WorkBudget::UNBOUNDED
        }, &cancellation);
        let error = work.charge(crate::WorkCounters {
            items_examined: 1, ..crate::WorkCounters::UNCHARGED
        }).expect_err("actual original work refuses its first item");
        let crate::DemandError::Work(error) = error else { return Err("lost actual Work cause".into()); };
        let native: NativeMountError =
            MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Work(error)).into();
        assert!(matches!(native, NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(_))));
        assert!(darwin_handler_is_fatal(&native));
        let reservation = Vec::<u8>::new().try_reserve_exact(usize::MAX)
            .expect_err("the real Vec reservation cannot represent this capacity");
        let allocation = NativeMountError::Materialization(
            crate::NativeTreeMaterializationError::AllocationFailed(reservation));
        assert_eq!(darwin_handler_errno(&allocation), libc::ENOMEM);
        assert!(darwin_handler_is_fatal(&allocation));
        Ok(())
    }

    #[test]
    fn owned_original_scope_rejects_other_meter_and_retains_real_sdk_callbacks_until_io_fork_drops() -> TestResult {
        let (source, _) = checkout_context(MountPublication::Manual)?;
        let cancellation = crate::CancellationToken::new();
        let context = callback_context(&source, crate::WorkBudget { items_examined: 0,
            ..callback_policy() }, &cancellation)?;
        let original = Arc::new(NativeDemandReadMeter(Mutex::new(
            DemandWork::new(callback_policy(), &cancellation))));
        let other = Arc::new(NativeDemandReadMeter(Mutex::new(
            DemandWork::new(callback_policy(), &cancellation))));
        let original = super::super::NativeMountMeterWork::from_meter(original);
        let other = super::super::NativeMountMeterWork::from_meter(other);
        let active = DarwinStartupMeterScope::existing(Arc::clone(&context), None, &original)?;
        assert!(matches!(DarwinStartupMeterScope::existing(Arc::clone(&context), None, &other),
            Err(NativeMountError::ControlPending)));
        assert_eq!(other.meter().lock().work, crate::WorkCounters::UNCHARGED);
        let accepted_io_scope = active.try_clone_for_io()?;
        drop(active);
        assert!(context.startup_meter.lock().unwrap_or_else(PoisonError::into_inner)
            .upgrade().is_some_and(|meter| Arc::ptr_eq(meter.meter(), original.meter())));
        let root = CString::new("/")?;
        let mut output = std::mem::MaybeUninit::<NativeStat>::uninit();
        // SAFETY: the actual retained context and output remain alive for this SDK callback.
        assert_eq!(unsafe { acyclic_fs_darwin_mount_getattr(Arc::as_ptr(&context) as usize,
            root.as_ptr(), 0, output.as_mut_ptr()) }, 0);
        assert!(original.meter().lock().work.items_examined > 0);
        assert_eq!(context.handler_failure.lock().unwrap_or_else(PoisonError::into_inner)
            .completed_future_request_work, crate::WorkCounters::UNCHARGED);
        drop(accepted_io_scope);
        assert!(context.startup_meter.lock().unwrap_or_else(PoisonError::into_inner).upgrade().is_none());
        assert_eq!(unsafe { acyclic_fs_darwin_mount_getattr(Arc::as_ptr(&context) as usize,
            root.as_ptr(), 0, output.as_mut_ptr()) }, -libc::EIO);
        assert!(context.handler_failed.load(Ordering::Acquire));
        assert_eq!(context.handler_failure.lock().unwrap_or_else(PoisonError::into_inner)
            .completed_future_request_work.items_examined, 0);
        Ok(())
    }
    #[test]
    fn raw_write_denies_the_actual_incoming_copy_without_touching_existing_contents() -> TestResult {
        let (source, _) = checkout_context(MountPublication::Manual)?;
        let path = MountPath::root().child(b"original".to_vec());
        source.create_file(&path, FileMetadata::default())?;
        source.write_range(&path, 0, Bytes::from_static(b"before"))?;
        let cancellation = crate::CancellationToken::new();
        let context = callback_context(&source, crate::WorkBudget { bytes_copied: 0, ..callback_policy() }, &cancellation)?;
        let native_path = CString::new("/original")?;
        let incoming = b"after";
        // SAFETY: context, terminated path and all readable input bytes remain alive.
        let status = unsafe { acyclic_fs_darwin_mount_write(Arc::as_ptr(&context) as usize,
            native_path.as_ptr(), 0, incoming.as_ptr().cast(), incoming.len(), 0) };
        assert_eq!(status, -libc::EIO);
        let failure = context.handler_failure.lock().map_err(|_| "poisoned callback failure")?.first_fatal.take().ok_or("missing actual copy denial")?;
        assert!(matches!(failure.cause, NativeMountError::Work(crate::WorkError::BudgetExceeded { counter: "bytes_copied", .. })));
        assert_eq!(failure.work.bytes_copied, 0);
        assert_eq!(failure.work.backend_write_operations, 0);
        assert_eq!(source.open_file(&path)?.read_up_to(0, 16)?.as_ref(), b"before");
        Ok(())
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
            path_custody: None,
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

        let (source, context) = checkout_context(MountPublication::Manual)?;
        let context = Arc::new(context);
        let observer = Arc::downgrade(&context);
        let observer: std::sync::Weak<dyn ViewObserver> = observer;
        source.observe_view(observer)?;
        let path = MountPath::root().child(b"kept".to_vec());
        source.create_file(&path, FileMetadata::default())?;
        // The creation was made around the mount; take it as waited out.
        let made = context.around.made.load(Ordering::Acquire);
        context.around.settled.store(made, Ordering::Release);
        let label = context.attributes(&path, 0).map_err(os)?.change;

        source.record_projection_change(&ViewChange::Unconfirmed);
        assert!(
            !context.revalidate_with_wait(),
            "unchanged labels do not require waiting out the attribute timeout"
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
        assert!(
            context.revalidate_with_wait(),
            "a change the fence could not confirm requires waiting out the attribute timeout"
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
            path_custody: None,
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
    fn revalidation_waits_out_the_attribute_timeout_for_changes_around_the_mount() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let context = context.observing_source()?;
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
    fn nfs_read_answers_short_reads_as_eof_in_place() {
        // SAFETY: the test hook owns all callback state.
        assert_eq!(unsafe { nfs4_test_read_reply() }, 0);
    }

    #[test]
    fn nfs_fileids_name_nodes_so_hard_links_share_one() {
        // SAFETY: the test hook owns all callback state.
        assert_eq!(unsafe { nfs4_test_node_identity() }, 0);
    }

    #[test]
    fn nfs_durable_writes_are_stable_without_a_sync() {
        // SAFETY: the test hook owns all callback state.
        assert_eq!(unsafe { nfs4_test_durable_writes() }, 0);
    }

    #[test]
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
    fn nfs_change_attribute_travels_with_its_attributes() {
        // SAFETY: the test hook owns all callback state.
        assert_eq!(unsafe { nfs4_test_change_attribute() }, 0);
    }

    #[test]
    fn nfs_access_applies_the_callers_identity_to_mode_bits() {
        // SAFETY: the test hook owns all callback state.
        assert_eq!(unsafe { nfs4_test_access_rights() }, 0);
    }

    #[test]
    fn nfs_verify_compares_current_attribute_values() {
        // SAFETY: the test hook owns all callback state.
        assert_eq!(unsafe { nfs4_test_verify_attributes() }, 0);
    }

    #[test]
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
    #[allow(
        clippy::type_complexity,
        reason = "the tuple keeps the temporary directory, source, and session alive together for one test"
    )]
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
        // macOS refuses socket options (EINVAL) once the peer has closed, as
        // this server may already have done; the read then ends at once.
        let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
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
            acyclic_native_runtime::process_status(Command::new("/usr/bin/perl")
                .arg("-e")
                .arg(format!(
                    "use Fcntl qw(:flock :DEFAULT); open(F, '+<', $ARGV[0]) or die;                      exit({operation} ? 0 : 1)"
                ))
                .arg(&path), std::time::Duration::from_secs(120))
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
        assert_eq!(
            // SAFETY: `region` outlives the call on the live descriptor.
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
            acyclic_native_runtime::process_status(
                &mut Command::new(&script),
                std::time::Duration::from_secs(120)
            )
            .err()
            .map(|error| error.kind()),
            Some(std::io::ErrorKind::PermissionDenied),
            "a file without an execute bit does not run"
        );
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))?;
        assert!(
            acyclic_native_runtime::process_status(
                &mut Command::new(&script),
                std::time::Duration::from_secs(120)
            )?
            .success()
        );
        assert_eq!(std::fs::read(&path)?, b"kept");
        assert!(mount.stop()?);
        Ok(())
    }

    #[test]
    fn directory_page_rejects_a_native_mutation_between_reads() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        source.create_file(
            &MountPath::root().child(b"in-page".to_vec()),
            FileMetadata::default(),
        )?;
        let root = MountPath::root();
        let mut directory =
            DirectoryHandle::new(root, source.binding_epoch(), context.cache_epochs());
        // `.`, `..`, and one entry delivered: a listing that handed out an
        // entry cannot continue in a later view.
        advance_directory_to_offset(&context, &mut directory, 3)
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
    fn a_listing_that_delivered_nothing_restarts_in_the_new_view() -> TestResult {
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
        // Only `.` and `..` were delivered: the listing starts again.
        assert_eq!(finish_directory_page(&context, &directory, false), Ok(()));
        assert_eq!(ensure_directory_page(&context, &mut directory), Ok(()));
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
    fn actual_loop_join_keeps_original_panic_after_quota_denial_and_reports_exact_sdk_prefix() -> TestResult {
        let temporary = tempfile::tempdir()?;
        let events = Arc::new(LoopEvents::default());
        let report = ExitReport(Arc::clone(&events));
        let thread = std::thread::spawn(move || -> c_int {
            let _report = report;
            panic!("original provider loop panic")
        });
        assert!(events.wait_until(Duration::from_secs(1), |state| state.exited).exited);
        let mut session = DarwinMountSession {
            resources: None,
            destination: temporary.path().to_path_buf(), _destination_allocation: None,
            thread: Some(thread),
            events, loop_finished_before_teardown: Some(true),
            loop_result: None, loop_panic: None, teardown_complete: false,
            completed_callback_work: crate::WorkCounters::UNCHARGED,
            retired_callback_failure: Mutex::new(None), native_root_identity: None,
            unmount: UnmountState::default(),
        };
        let cancellation = crate::CancellationToken::new();
        let denied = crate::NativeMaterializationWork::new(crate::WorkBudget {
            backend_read_operations: 0, ..crate::WorkBudget::UNBOUNDED
        }, &cancellation).map_err(|(cause, _)| cause)?;
        assert!(matches!(session.finish_thread_measured(Duration::from_secs(1), &denied),
            Err(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(_)))));
        assert!(session.thread.is_some());
        assert!(session.loop_panic.is_none(), "the actual SDK still owns its unconsumed panic");
        assert_eq!(denied.receipt().backend_read_operations, 0);
        let exact = crate::NativeMaterializationWork::new(crate::WorkBudget {
            backend_read_operations: 1, backend_write_operations: 0, items_examined: 1,
            ..crate::WorkBudget::UNBOUNDED
        }, &cancellation).map_err(|(cause, _)| cause)?;
        assert!(matches!(session.finish_thread_measured(Duration::from_secs(1), &exact)?,
            MountLoopResult::Panicked));
        assert_eq!(exact.receipt().backend_read_operations, 1);
        assert_eq!(exact.receipt().backend_write_operations, 0);
        assert_eq!(exact.receipt().items_examined, 1);
        assert!(session.thread.is_none());
        assert!(session.loop_panic.is_some(), "joined SDK payload remains owned before final detach classification");
        let native = super::super::NativeMountMeterWork::from_meter(Arc::new(
            NativeDemandReadMeter(Mutex::new(DemandWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)))));
        match session.stop_with_meter(&native) {
            Err(NativeMountError::DriverPanicked { panic, .. }) => {
                assert_eq!(panic.into_inner().unwrap_or_else(PoisonError::into_inner)
                    .downcast_ref::<&'static str>(), Some(&"original provider loop panic"));
            }
            _ => return Err("actual loop panic was replaced by a static description".into()),
        }
        assert!(session.namespace_detach_confirmed());
        assert!(session.loop_panic.is_none());
        // This actual joined SDK worker never installed a native namespace.
        session.teardown_complete = true;
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
            _destination_allocation: None,
            thread: Some(thread),
            events,
            loop_finished_before_teardown: None,
            loop_result: None,
            loop_panic: None,
            teardown_complete: false,
            completed_callback_work: crate::WorkCounters::UNCHARGED,
            retired_callback_failure: Mutex::new(None),
            native_root_identity: None,
            unmount: UnmountState::default(),
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
