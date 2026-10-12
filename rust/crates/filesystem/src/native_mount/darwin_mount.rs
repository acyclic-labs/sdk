//! Self-contained macOS mount projection over loopback `NFSv4`.

#![allow(
    unsafe_code,
    reason = "drives the embedded C NFSv4 transport through FFI"
)]

use super::{
    ATTRIBUTE_PAGE_SIZE, DIRECTORY_PAGE_SIZE, DriverStartFailure,
    MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES, MountAttributeWriteMode, MountDirectoryEntry,
    MountFilesystem, MountLookup, MountNodeKind, MountOpenFile, MountPath, MountRangeAllocation,
    MountSeekTarget, MountSourceError, NativeMountError, NativeMountRequest, ViewObserver,
    ViewOrigin, ViewStamp, create_metadata, metadata_or, source_error, system_time_ns,
};
use crate::FileId;
use crate::demand::{NativeDemandReadMeter, native_demand_error};
#[cfg(test)]
use crate::demand::DemandWork;
use crate::native_host::NativeHostWork as _;
use crate::kernel::{FileMetadata, MetadataField};
use acyclic_stream::OwnedBytes;
use std::collections::VecDeque;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::os::unix::ffi::OsStrExt;
#[cfg(test)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::process::Command;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError, RwLock, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};

const ROOT_INODE: u64 = 1;
const MAXIMUM_LOOKUP_CACHE_ENTRIES: usize = 65_536;
const MAXIMUM_LISTING_RESTARTS: usize = 8;
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
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(10);
const CALLBACK_GRACE_INTERVAL: Duration = Duration::from_millis(100);
const CALLBACK_DRAIN_POLL: Duration = Duration::from_millis(1);
const IFMT: u32 = libc::S_IFMT as u32;
const IFIFO: u32 = libc::S_IFIFO as u32;
const IFSOCK: u32 = libc::S_IFSOCK as u32;
const IFCHR: u32 = libc::S_IFCHR as u32;
const IFBLK: u32 = libc::S_IFBLK as u32;
const IFDIR: u32 = libc::S_IFDIR as u32;
const IFLNK: u32 = libc::S_IFLNK as u32;
const IFREG: u32 = libc::S_IFREG as u32;

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
    fn acyclic_fs_darwin_mount_session_allocation_size() -> usize;
    fn acyclic_fs_darwin_mount_session_free(session: *mut c_void);
    fn acyclic_fs_darwin_mount_run(
        session: *mut c_void,
        argc: c_int,
        argv: *const *const c_char,
        mountpoint: *const c_char,
        context: usize,
        mounted: unsafe extern "C" fn(*mut c_void),
        mounted_argument: *mut c_void,
        interrupted_before_namespace: *mut c_int,
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
    file: Option<DarwinFile>,
    observation: Arc<DarwinOwned<Mutex<FileObservation>>>,
    written: Arc<DarwinOwned<AtomicU64>>,
}

struct FileObservation {
    stamp: Option<CacheStamp>,
    lookup: MountLookup,
}

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

    fn clone(&self, work: &DarwinCallbackWork<'_>) -> Result<Self, i32> {
        let path_owned = work.path_with_child(&self.path, None)?;
        let mut page_custody = DarwinOwned::empty(());
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

    fn is_current(&self, context: &DarwinMountContext, work: &DarwinCallbackWork<'_>) -> Result<bool, i32> {
        Ok(match self.epochs {
            Some(epochs) => {
                context.source.view_is_stable_with_meter(work.native).map_err(|error| work.source_error(error))?
                    && context.source.binding_epoch_with_meter(work.native).map_err(|error| work.source_error(error))? == Some(epochs.binding)
                    && context.unchanged_since(&self.path, None, epochs.view, work)?
            }
            None => context.cache_epochs(work)?.is_none(),
        })
    }

    const fn can_reuse_pages(&self) -> bool {
        self.epochs.is_some()
    }

    fn keep_current(&mut self, context: &DarwinMountContext, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
        for _ in 0..MAXIMUM_LISTING_RESTARTS {
            if !self.can_reuse_pages() || self.is_current(context, work)? {
                return Ok(());
            }
            if self.emitted > 2 || context.source.binding_epoch_with_meter(work.native).map_err(|error| work.source_error(error))? != self.binding_epoch {
                return Err(libc::ESTALE);
            }
            self.epochs = context.cache_epochs(work)?;
            self.clear_page();
            self.exhausted = false;
            self.revision = None;
        }
        Err(libc::ESTALE)
    }
}

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
        && left.resource_allowance().map(Arc::as_ptr) == right.resource_allowance().map(Arc::as_ptr)
        && left.capture_origin().map(Weak::as_ptr) == right.capture_origin().map(Weak::as_ptr)
        && left.invocation_view_state().map(Arc::as_ptr) == right.invocation_view_state().map(Arc::as_ptr)
        && left.source_execution_view().map(Arc::as_ptr) == right.source_execution_view().map(Arc::as_ptr)
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
    forgotten: AtomicU64,
    lookups: Mutex<DarwinMap<MountPath, (Option<MountLookup>, CacheStamp, DarwinOwned<()>)>>,
    files: RwLock<DarwinMap<u64, FileHandle>>,
    directories: Mutex<DarwinMap<u64, Arc<DarwinOwned<Mutex<DirectoryHandle>>>>>,
    directory_checkpoints: Mutex<DarwinMap<MountPath, DirectoryCheckpoint>>,
    namespace_revision: AtomicU64,
    ledger: FlushLedger,
    changes: ChangeLabels,
    origin: ViewOrigin,
    callbacks: CallbackGate,
    around: AroundChanges,
    native_allocation: Option<crate::NativeAllocation>,
    callback_guard: Option<Arc<dyn super::NativeMountCallbackGuard>>,
    source_original_work: Option<super::NativeMountMeterWork>,
    allocation_custody: Option<crate::demand::NativeMountAllocationCustody>,
}

struct CallbackGate {
    generation: AtomicU64,
    even: AtomicU64,
    odd: AtomicU64,
    draining: Mutex<Option<u64>>,
}

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
            if self.generation.load(Ordering::SeqCst) == generation {
                return CallbackPass(active);
            }
            active.fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn drain(&self, work: &super::NativeMountMeterWork) -> Result<(), NativeMountError> {
        work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)?;
        let mut pending = self.draining.lock().unwrap_or_else(PoisonError::into_inner);
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

struct AroundChanges {
    made: AtomicU64,
    settled: AtomicU64,
    unconfirmed: AtomicU64,
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

fn uptime<W: crate::native_host::NativeHostWork>(work: &mut W) -> Result<Duration, NativeMountError>
where NativeMountError: From<W::Error> {
    let mut now = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    darwin_stop_io(work, false, || {
        let result = unsafe { libc::clock_gettime(libc::CLOCK_UPTIME_RAW, &raw mut now) };
        if result == 0 { Ok(()) } else { Err(std::io::Error::last_os_error()) }
    })?;
    Ok(Duration::new(u64::try_from(now.tv_sec).unwrap_or(0), u32::try_from(now.tv_nsec).unwrap_or(0)))
}

struct FlushLedger {
    completed: AtomicU64,
    flushed: AtomicU64,
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

    fn record(&self) -> u64 {
        self.completed.fetch_add(1, Ordering::AcqRel) + 1
    }

    fn latest(&self) -> u64 {
        self.completed.load(Ordering::Acquire)
    }

    fn flush_through(&self, required: u64, source: &dyn MountFilesystem, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
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

struct ChangeLabels {
    next: AtomicU64,
    labels: Mutex<DarwinMap<FileId, IssuedLabel>>,
    cleared: AtomicU64,
}

struct IssuedLabel {
    label: u64,
    stamp: CacheStamp,
    binding: Option<u64>,
    observed: Observed,
}

struct Observed {
    path: MountPath,
    lookup: MountLookup,
    path_custody: Option<DarwinOwned<()>>,
}

impl Observed {
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

    fn label(
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
        let mut labels = self.labels.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(issued) = labels.get_mut(&file_id, work)?
            && issued.binding == binding
        {
            let since = issued.stamp.min(stamp);
            if unchanged(since)? {
                return Ok(issued.label);
            }
            if issued.observed.matches(&lookup) && reported_unchanged(since)? {
                issued.stamp = issued.stamp.max(stamp);
                return Ok(issued.label);
            }
        }
        if labels.len() >= MAXIMUM_LOOKUP_CACHE_ENTRIES {
            labels.clear();
            self.cleared.fetch_add(1, Ordering::AcqRel);
        }
        let path = work.path_with_child(path, None)?;
        let (path, proof) = path.into_parts();
        let observed = Observed { path, lookup, path_custody: Some(proof) };
        let label = self.fresh();
        labels.insert(
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

    fn issued(
        &self, native: &super::NativeMountMeterWork,
    ) -> Result<Option<DarwinOwned<Vec<(FileId, Observed)>>>, NativeMountError> {
        let work = DarwinCallbackWork::new(native);
        let labels = self.labels.lock().unwrap_or_else(PoisonError::into_inner);
        if labels.len() > MAXIMUM_VERIFIED_LABELS { return Ok(None); }
        if labels.len() == 0 { return Ok(Some(DarwinOwned::empty(Vec::new()))); }
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
                let path = work.path_with_child(&issued.observed.path, None)?;
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

pub(super) struct DarwinStartupMeterScope {
    context: Arc<DarwinMountContext>,
    meter: super::NativeMountMeterWork,
    generation: u64,
}

impl DarwinStartupMeterScope {
    fn new(context: Arc<DarwinMountContext>, meter: &super::NativeMountMeterWork)
        -> Result<Self, NativeMountError> {
        let generation = context.startup_meter.lock().unwrap_or_else(PoisonError::into_inner).enter(meter)?;
        Ok(Self { context, meter: meter.clone(), generation })
    }
    /// Retains the original callback meter for accepted I/O.
    pub(super) fn try_clone_for_io(&self) -> Result<Self, NativeMountError> {
        Self::new(Arc::clone(&self.context), &self.meter)
    }
}

impl Drop for DarwinStartupMeterScope {
    fn drop(&mut self) {
        self.context.startup_meter.lock().unwrap_or_else(PoisonError::into_inner).leave(&self.meter, self.generation);
    }
}


impl DarwinMountContext {
    fn new(
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
            lookups: Mutex::new(DarwinMap::new()),
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

    fn cache_stamp(&self, work: &DarwinCallbackWork<'_>) -> Result<Option<CacheStamp>, i32> {
        let forgotten = self.forgotten.load(Ordering::Acquire);
        Ok(self.source.view_stamp_with_meter(work.native).map_err(|error| work.source_error(error))?
            .map(|view| CacheStamp { forgotten, view }))
    }

    fn unchanged_since(&self, path: &MountPath, file_id: Option<FileId>, stamp: CacheStamp, work: &DarwinCallbackWork<'_>) -> Result<bool, i32> {
        if stamp.forgotten != self.forgotten.load(Ordering::Acquire) { return Ok(false); }
        self.source.unchanged_since_with_meter(path, file_id, stamp.view, work.native)
            .map_err(|error| work.source_error(error))
    }

    fn reported_unchanged_since(&self, path: &MountPath, file_id: Option<FileId>, stamp: CacheStamp, work: &DarwinCallbackWork<'_>) -> Result<bool, i32> {
        if stamp.forgotten != self.forgotten.load(Ordering::Acquire) { return Ok(false); }
        self.source.reported_unchanged_since_with_meter(path, file_id, stamp.view, work.native)
            .map_err(|error| work.source_error(error))
    }

    fn cache_epochs(&self, work: &DarwinCallbackWork<'_>) -> Result<Option<CacheEpochs>, i32> {
        if !self.source.view_is_stable_with_meter(work.native).map_err(|error| work.source_error(error))? { return Ok(None); }
        let Some(view) = self.cache_stamp(work)? else { return Ok(None); };
        let Some(binding) = self.source.binding_epoch_with_meter(work.native).map_err(|error| work.source_error(error))? else { return Ok(None); };
        Ok(Some(CacheEpochs { view, binding }))
    }

    fn forget_view(&self) {
        self.forgotten.fetch_add(1, Ordering::AcqRel);
        self.namespace_changed();
        self.lookups
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        self.around.made.fetch_add(1, Ordering::AcqRel);
    }

    fn revalidate(
        &self, native: &super::NativeMountMeterWork,
    ) -> Result<bool, NativeMountError> {
        native.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)?;
        let unconfirmed = self.around.unconfirmed.load(Ordering::Acquire);
        if self.around.verified.load(Ordering::Acquire) < unconfirmed
            && !self.verify_labels(native)? {
            self.around.made.fetch_add(1, Ordering::AcqRel);
        }
        self.around.verified.fetch_max(unconfirmed, Ordering::AcqRel);
        let made = self.around.made.load(Ordering::Acquire);
        if self.around.settled.load(Ordering::Acquire) >= made { return Ok(false); }
        self.callbacks.drain(native)?;
        let materialization = native.native_materialization_work();
        let callback_grace_end = uptime(&mut &materialization)? + CALLBACK_GRACE_INTERVAL;
        darwin_native_attempt(native, false)?;
        let timeout = u64::from(unsafe { acyclic_fs_darwin_mount_attribute_timeout() });
        let expired = Duration::from_secs(callback_grace_end.as_secs() + timeout);
        loop {
            let now = uptime(&mut &materialization)?;
            if now >= expired { break; }
            darwin_native_attempt(native, false)?;
            std::thread::sleep((expired - now).min(Duration::from_millis(10)));
        }
        self.around.settled.fetch_max(made, Ordering::AcqRel);
        Ok(true)
    }

    fn verify_labels(
        &self, native: &super::NativeMountMeterWork,
    ) -> Result<bool, NativeMountError> {
        let cleared = self.changes.cleared.load(Ordering::Acquire);
        let Some(issued) = self.changes.issued(native)? else { return Ok(false); };
        for (file_id, observed) in issued.iter() {
            native.native_materialization_work().charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            })?;
            let work = DarwinCallbackWork::new(native);
            match work.finish(self.lookup(&observed.path, &work)) {
                Ok(current) if current.node.file_id == *file_id && observed.matches(&current) => {}
                Ok(_) => return Ok(false),
                Err(error) if darwin_handler_is_fatal(&error) => return Err(error),
                Err(_) => return Ok(false),
            }
        }
        Ok(self.changes.cleared.load(Ordering::Acquire) == cleared)
    }

    fn inode(&self, file_id: FileId, work: &DarwinCallbackWork<'_>) -> Result<u64, i32> {
        let mut inodes = self.inodes.lock().map_err(|_| libc::EIO)?;
        if let Some(inode) = inodes.get(&file_id, work)? {
            return Ok(*inode);
        }
        let inode = self.next_inode.fetch_add(1, Ordering::Relaxed);
        if inode <= ROOT_INODE || inode == u64::MAX {
            return Err(libc::EOVERFLOW);
        }
        inodes.insert(file_id, inode, work)?;
        Ok(inode)
    }

    fn lookup(&self, path: &MountPath, work: &DarwinCallbackWork<'_>) -> Result<MountLookup, i32> {
        let stamp = self.cache_stamp(work)?;
        if stamp.is_some() {
            let cache = self.lookups.lock().map_err(|_| libc::EIO)?;
            if let Some((cached, cached_stamp)) = cache.get(path, work)?.map(|(lookup, stamp, _)| (*lookup, *stamp))
                && self.unchanged_since(
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
            self.remember_lookup(path, lookup, stamp, work)?;
        }
        lookup.ok_or(libc::ENOENT)
    }

    fn remember_current_lookup(
        &self,
        path: &MountPath,
        lookup: Option<MountLookup>, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
        let Some(stamp) = self.cache_stamp(work)? else {
            return Ok(());
        };
        self.remember_lookup(path, lookup, stamp, work)?;
        Ok(())
    }

    fn remember_lookup(
        &self,
        path: &MountPath,
        lookup: Option<MountLookup>,
        stamp: CacheStamp, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
        let mut lookups = self.lookups.lock().map_err(|_| libc::EIO)?;
        if lookups.len() >= MAXIMUM_LOOKUP_CACHE_ENTRIES {
            lookups.clear();
        }
        let path = work.path_with_child(path, None)?;
        let (path, proof) = path.into_parts();
        if let Some((key, value)) = lookups.remove_entry(&path, work)? { drop(key); drop(value); }
        lookups.insert(path, (lookup, stamp, proof), work)?;
        Ok(())
    }

    fn open(&self, path: &MountPath, written: u64, work: &DarwinCallbackWork<'_>) -> Result<u64, i32> {
        // Sampled first, so a change after it cannot validate the lookup.
        let stamp = self.cache_stamp(work)?;
        let lookup = self.lookup(path, work)?;
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
        files.insert(handle, file, work)?;
        Ok(handle)
    }

    fn bind(&self, path: &MountPath, handle: u64, work: &DarwinCallbackWork<'_>) -> Result<DarwinFile, i32> {
        let file_id = {
            let files = self.files.read().map_err(|_| libc::EIO)?;
            let entry = files.get(&handle, work)?.ok_or(libc::ESTALE)?;
            if let Some(file) = &entry.file {
                return Ok(file.clone());
            }
            entry.file_id
        };
        let file = work.retained_file(self.source.open_file_with_meter(path, work.native).map_err(|error| work.source_error(error))?)?;
        if file.0.lookup_with_meter(work.native).map_err(|error| work.source_error(error))?.node.file_id != file_id {
            return Err(libc::ESTALE);
        }
        let mut files = self.files.write().map_err(|_| libc::EIO)?;
        let entry = files.get_mut(&handle, work)?.ok_or(libc::ESTALE)?;
        Ok(entry.file.get_or_insert(file).clone())
    }

    fn unlink(
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
        self.admit_write(work)?;
        let result: Result<Option<DarwinFile>, i32> = (|| {
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
                    let old = work.copying(crate::WorkCounters { bytes_copied: std::mem::size_of::<Option<DarwinFile>>() as u64, ..crate::WorkCounters::UNCHARGED }, || std::mem::replace(&mut entry.file, replacement))?;
                    drop(old);
                }
            }
        }
        self.admit_write(work)?;
        Ok(())
    }

    fn file_target(&self, path: &MountPath, handle: u64, work: &DarwinCallbackWork<'_>) -> Result<(DarwinFile, Option<Arc<DarwinOwned<AtomicU64>>>), i32> {
        if handle == 0 {
            let file = work.retained_file(self.source.open_file_with_meter(path, work.native).map_err(|error| work.source_error(error))?)?;
            return Ok((file, None));
        }
        let file = self.bind(path, handle, work)?;
        let files = self.files.read().map_err(|_| libc::EIO)?;
        let entry = files.get(&handle, work)?.ok_or(libc::ESTALE)?;
        Ok((file, Some(entry.written.clone())))
    }

    fn mutate<T>(&self, mutation: impl FnOnce() -> Result<T, MountSourceError>, work: &DarwinCallbackWork<'_>) -> Result<T, i32> {
        self.admit_write(work)?;
        let result = mutation().map_err(|error| work.source_error(error));
        self.ledger.record();
        result.and_then(|value| { self.admit_write(work)?; Ok(value) })
    }

    fn mutate_file<T>(
        &self,
        path: &MountPath,
        handle: u64,
        mutation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>, work: &DarwinCallbackWork<'_>) -> Result<T, i32> {
        self.admit_write(work)?;
        let target = self.file_target(path, handle, work)?;
        let result = mutation(target.0.0.as_ref()).map_err(|error| work.source_error(error));
        let sequence = self.ledger.record();
        if let Some(written) = target.1 {
            written.fetch_max(sequence, Ordering::AcqRel);
        }
        result.and_then(|value| { self.admit_write(work)?; Ok(value) })
    }

    fn lookup_handle(&self, path: &MountPath, handle: u64, work: &DarwinCallbackWork<'_>) -> Result<MountLookup, i32> {
        if handle == 0 {
            return self.lookup(path, work);
        }
        let observation = {
            let files = self.files.read().map_err(|_| libc::EIO)?;
            files.get(&handle, work)?.ok_or(libc::ESTALE)?.observation.clone()
        };
        {
            let observation = observation.lock().map_err(|_| libc::EIO)?;
            if let Some(stamp) = observation.stamp
                && self.unchanged_since(path, Some(observation.lookup.node.file_id), stamp, work)?
            {
                return Ok(observation.lookup);
            }
        }
        let stamp = self.cache_stamp(work)?;
        let lookup = self.bind(path, handle, work)?.0.lookup_with_meter(work.native).map_err(|error| work.source_error(error))?;
        if stamp.is_some() {
            let mut observation = observation.lock().map_err(|_| libc::EIO)?;
            observation.stamp = stamp;
            observation.lookup = lookup;
        }
        Ok(lookup)
    }

    fn attributes(&self, path: &MountPath, handle: u64, work: &DarwinCallbackWork<'_>) -> Result<NativeStat, i32> {
        // Sampled before observing: a change after it cannot keep the label.
        let stamp = self.cache_stamp(work)?;
        let lookup = self.lookup_handle(path, handle, work)?;
        self.attributes_from_lookup(path, lookup, stamp, work)
    }

    fn attributes_from_lookup(
        &self,
        path: &MountPath,
        lookup: MountLookup,
        stamp: Option<CacheStamp>, work: &DarwinCallbackWork<'_>) -> Result<NativeStat, i32> {
        let node = lookup.node;
        let file_kind = match node.kind {
            MountNodeKind::Regular => IFREG,
            MountNodeKind::Directory => IFDIR,
            MountNodeKind::SymbolicLink => IFLNK,
            MountNodeKind::Fifo => IFIFO,
            MountNodeKind::Socket => IFSOCK,
            MountNodeKind::CharacterDevice => IFCHR,
            MountNodeKind::BlockDevice => IFBLK,
            MountNodeKind::Unsupported => return Err(libc::EOPNOTSUPP),
        };
        let default_mode = if self.writable { 0o755 } else { 0o555 };
        let mode = metadata_or(lookup.metadata.posix_mode, default_mode) & 0o7777;
        let (accessed_seconds, accessed_nanoseconds) = metadata_time(lookup.metadata.accessed_ns);
        let (modified_seconds, modified_nanoseconds) = metadata_time(lookup.metadata.modified_ns);
        let (changed_seconds, changed_nanoseconds) = metadata_time(lookup.metadata.changed_ns);
        let (created_seconds, created_nanoseconds) = metadata_time(lookup.metadata.created_ns);
        Ok(NativeStat {
            inode: self.inode(node.file_id, work)?,
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
                node.file_id, stamp,
                self.source.binding_epoch_with_meter(work.native).map_err(|error| work.source_error(error))?,
                path, lookup,
                |stamp| self.unchanged_since(path, Some(node.file_id), stamp, work),
                |stamp| self.reported_unchanged_since(path, Some(node.file_id), stamp, work),
                work,
            )?,
        })
    }

    fn may_have_named_attributes(&self, path: &MountPath, work: &DarwinCallbackWork<'_>) -> Result<bool, i32> {
        Ok(self.lookup(path, work)?.metadata.named_attributes != MetadataField::Unavailable)
    }

    fn admit_write(&self, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
        self.writable.then_some(()).ok_or(libc::EROFS)?;
        if let Some(guard) = &self.callback_guard {
            super::adapter::CallbackRuntime::existing()
                .and_then(|runtime| runtime.verify_native_callback_write_guard(guard.as_ref(), work.native))
                .map_err(|error| work.native_error(error))?;
        }
        work.check()
    }

    fn mutate_metadata(
        &self,
        path: &MountPath,
        handle: u64,
        mutate: impl FnOnce(&mut FileMetadata) -> Result<(), i32>, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
        self.admit_write(work)?;
        let current = self.lookup_handle(path, handle, work)?;
        let mut metadata = current.metadata;
        mutate(&mut metadata)?;
        // Metadata of any node kind: only a handle names a regular file.
        if handle == 0 {
            self.mutate(|| self.source.set_attributes_with_meter(path, metadata, None, work.native), work)
        } else {
            self.mutate_file(path, handle, |file| file.set_attributes_with_meter(metadata, None, work.native), work)
        }
    }

    fn flush_on_close(&self, handle: u64, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
        if handle == 0 {
            return Ok(());
        }
        let written = self
            .files
            .read()
            .map_err(|_| libc::EIO)?
            .get(&handle, work)?
            .ok_or(libc::ESTALE)?
            .written
            .load(Ordering::Acquire);
        if written == 0 || !self.source.flush_on_handle_close() {
            return Ok(());
        }
        self.admit_write(work)?;
        self.ledger.flush_through(written, self.source.as_ref(), work)?;
        self.admit_write(work)
    }

    fn sync(&self, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
        self.admit_write(work)?;
        self.ledger.flush_through(self.ledger.latest(), self.source.as_ref(), work)?;
        self.admit_write(work)
    }
}

#[derive(Default)]
struct LoopEvents {
    state: Mutex<LoopState>,
    changed: Condvar,
    _allocation: Option<crate::NativeAllocation>,
}

#[derive(Clone, Copy, Debug, Default)]
struct LoopState {
    mounted: bool,
    provider_exited: bool,
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

    fn wait_until(&self, timeout: Duration, done: impl Fn(LoopState) -> bool) -> LoopState {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let (state, _) = self
            .changed
            .wait_timeout_while(state, timeout, |state| !done(*state))
            .unwrap_or_else(PoisonError::into_inner);
        *state
    }
}

struct ExitReport(Arc<LoopEvents>);

impl Drop for ExitReport {
    fn drop(&mut self) {
        self.0.report(|state| state.exited = true);
    }
}

unsafe extern "C" fn report_mounted(events: *mut c_void) {
    // SAFETY: ExitReport keeps events alive through the native loop.
    let events = unsafe { &*events.cast_const().cast::<LoopEvents>() };
    events.report(|state| state.mounted = true);
}

struct DarwinStartupBuffers {
    destination: Option<CString>,
    arguments: Vec<CString>,
    pointers: Vec<usize>,
    allocation: crate::NativeAllocation,
}

impl DarwinStartupBuffers {
    fn new(
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
    unmount: Arc<DarwinStopController>,
}

#[derive(Debug, Clone, Copy)]
enum MountLoopResult {
    Exited(c_int),
    Panicked,
    TimedOut,
}

/// Failure from Darwin teardown controls.
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
    /// The current mount table exceeds its genuinely prepaid birth buffer.
    MountTableCapacityExceeded,
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
            Self::MountTableCapacityExceeded => formatter.write_str("Darwin mount-table probe exceeds its prepaid birth capacity"),
        }
    }
}

impl std::error::Error for DarwinTeardownFailure {}

struct DriverSessionResources {
    source_context: usize,
    driver_session: AtomicUsize,
    native_control: Mutex<()>,
    sdk_allocation: Mutex<Option<crate::NativeAllocation>>,
    cleanup_credit: Mutex<crate::materializer::NativeWorkReservation>,
    interrupted: std::sync::atomic::AtomicBool,
    startup: Option<DarwinStartupBuffers>,
    _allocation: Option<crate::NativeAllocation>,
}

impl DriverSessionResources {
    fn new_sdk_session(
        original: &crate::NativeMaterializationWork,
    ) -> Result<(usize, crate::NativeAllocation), NativeMountError> {
        let bytes = unsafe { acyclic_fs_darwin_mount_session_allocation_size() } as u64;
        let allocation = original.allocation_work().claim_allocation(bytes, 1, crate::WorkCounters {
                backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
            })?;
        let pointer = unsafe { acyclic_fs_darwin_mount_session_new() } as usize;
        if pointer == 0 {
            return Err(std::io::Error::from_raw_os_error(libc::ENOMEM).into());
        }
        Ok((pointer, allocation))
    }

    fn reserve_cleanup_credit(
        original: &crate::NativeMaterializationWork,
    ) -> Result<crate::materializer::NativeWorkReservation, NativeMountError> {
        original.reserve_work(crate::WorkCounters {
            backend_read_operations: 1, backend_write_operations: 2,
            ..crate::WorkCounters::UNCHARGED
        }).map_err(NativeMountError::from)
    }

    fn interrupt_with_cleanup_wait(
        &self, wait: &crate::CancellationToken,
    ) -> Result<(), NativeMountError> {
        let _control = self.native_control.lock().unwrap_or_else(PoisonError::into_inner);
        if self.driver_session.load(Ordering::Acquire) == 0 { return Ok(()); }
        let mut credit = self.cleanup_credit.lock().unwrap_or_else(PoisonError::into_inner);
        if self.interrupted.load(Ordering::Acquire) { return Ok(()); }
        credit.cleanup_loan(wait).started_io(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(NativeMountError::from)?;
        self.interrupted.store(true, Ordering::Release);
        drop(credit);
        // SAFETY: this resource owner retains the exact bridge session; the
        // accepted signal targets only its existing loop and is issued once.
        unsafe {
            acyclic_fs_darwin_mount_interrupt(self.driver_session.load(Ordering::Acquire) as *mut c_void);
        }
        Ok(())
    }

    fn interrupt_accepted_cleanup(&self) {
        let _control = self.native_control.lock().unwrap_or_else(PoisonError::into_inner);
        let mut credit = self.cleanup_credit.lock().unwrap_or_else(PoisonError::into_inner);
        let pointer = self.driver_session.load(Ordering::Acquire);
        if pointer == 0 || self.interrupted.load(Ordering::Acquire) { return; }
        credit.started_drop_io(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        });
        self.interrupted.store(true, Ordering::Release);
        drop(credit);
        unsafe { acyclic_fs_darwin_mount_interrupt(pointer as *mut c_void); }
    }

    fn free_after_loop_quiescence(&self) {
        let _control = self.native_control.lock().unwrap_or_else(PoisonError::into_inner);
        let pointer = self.driver_session.load(Ordering::Acquire);
        if pointer == 0 { return; }
        self.cleanup_credit.lock().unwrap_or_else(PoisonError::into_inner)
            .started_drop_io(crate::WorkCounters {
                backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
            });
        unsafe { acyclic_fs_darwin_mount_session_free(pointer as *mut c_void); }
        self.driver_session.store(0, Ordering::Release);
        drop(self.sdk_allocation.lock().unwrap_or_else(PoisonError::into_inner).take());
    }


    fn start_join_with_cleanup_wait(
        &self, wait: &crate::CancellationToken,
    ) -> Result<(), NativeMountError> {
        self.cleanup_credit.lock().unwrap_or_else(PoisonError::into_inner)
            .cleanup_loan(wait).started_io(crate::WorkCounters {
                backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED
            }).map_err(NativeMountError::from)
    }

    fn context(&self) -> &DarwinMountContext {
        // SAFETY: the context outlives every owner of these resources.
        unsafe { &*(self.source_context as *const DarwinMountContext) }
    }

    fn context_owner(&self) -> Arc<DarwinMountContext> {
        let pointer = self.source_context as *const DarwinMountContext;
        // SAFETY: this resource owns the Arc::into_raw strong reference.
        unsafe { Arc::increment_strong_count(pointer); Arc::from_raw(pointer) }
    }

    fn free_with_cleanup_wait(
        &mut self, wait: &crate::CancellationToken,
    ) -> Result<(), NativeMountError> {
        let pointer = self.driver_session.load(Ordering::Acquire);
        if pointer == 0 { return Ok(()); }
        let credit = self.cleanup_credit.get_mut().unwrap_or_else(PoisonError::into_inner);
        let loan = credit.cleanup_loan(wait);
        loan.started_io(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(NativeMountError::from)?;
        // SAFETY: the loop is quiescent; context remains alive through SDK callbacks.
        unsafe { acyclic_fs_darwin_mount_session_free(pointer as *mut c_void); }
        self.driver_session.store(0, Ordering::Release);
        drop(self.sdk_allocation.get_mut().unwrap_or_else(PoisonError::into_inner).take());
        Ok(())
    }
}

impl Drop for DriverSessionResources {
    fn drop(&mut self) {
        // SAFETY: the loop retains an owner through every native callback.
        unsafe {
            let pointer = self.driver_session.load(Ordering::Acquire);
            if pointer != 0 {
                self.cleanup_credit.get_mut().unwrap_or_else(PoisonError::into_inner)
                    .started_drop_io(crate::WorkCounters {
                        backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
                    });
                acyclic_fs_darwin_mount_session_free(pointer as *mut c_void);
                self.driver_session.store(0, Ordering::Release);
            }
            drop(self.sdk_allocation.get_mut().unwrap_or_else(PoisonError::into_inner).take());
            if self.source_context != 0 {
                drop(Arc::from_raw(self.source_context as *const DarwinMountContext));
            }
        }
    }
}

struct DarwinStartupRetirementLoan {
    packet: Option<super::NativeMountRetirement>,
    controller: Arc<DarwinStopController>,
}

impl DarwinStartupRetirementLoan {
    fn restore(mut self) {
        let mut state = self.controller.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.retirement = self.packet.take();
    }
}

impl Drop for DarwinStartupRetirementLoan {
    fn drop(&mut self) {
        if let Some(packet) = self.packet.take() {
            let mut state = self.controller.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.retirement = Some(packet);
            state.start_aborted = true;
            self.controller.changed.notify_all();
        }
    }
}

fn run_darwin_loop(mut owned: (
    Arc<DriverSessionResources>, ExitReport, Arc<DarwinStopController>,
    Option<super::NativeMountMeterWork>, crate::NativeAllocation,
)) -> c_int {
    let (resources, events, controller, original, _) = &mut owned;
    let original = original.as_ref().expect("the born loop retains its original execution lease");
    let (mut state, mut gate) = {
        let mut state = controller.state.lock().unwrap_or_else(PoisonError::into_inner);
        let gate = state.start_gate_credit.take().expect("the loop's birth-paid startup wait");
        (state, gate)
    };
    if !state.start_ready && !state.start_aborted {
        gate.started_drop_io(crate::WorkCounters {
            backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED
        });
        state = controller.changed.wait_while(state, |state|
            !state.start_ready && !state.start_aborted).unwrap_or_else(PoisonError::into_inner);
    }
    let aborted = state.start_aborted;
    state.start_gate_credit = Some(gate);
    drop(state);
    let admission = if aborted { Err(super::NativeMountCallbackFailure {
        cause: NativeMountError::ControlPending, work: original.meter().lock().work,
    }) } else { darwin_native_attempt(original, true).map_err(|cause|
        super::NativeMountCallbackFailure { cause, work: original.meter().lock().work }) };
    let status = match admission {
        Err(failure) => {
            controller.state.lock().unwrap_or_else(PoisonError::into_inner).start_aborted = true;
            resources.context().handler_failed.store(true, Ordering::Release);
            *resources.context().startup_failure.lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(failure);
            -libc::EIO
        }
        Ok(()) => {
            let buffers = resources.startup.as_ref().expect("native argv retained");
            let mut interrupted_before_namespace = 0;
            let status = unsafe {
                acyclic_fs_darwin_mount_run(
                    resources.driver_session.load(Ordering::Acquire) as *mut c_void,
                    buffers.arguments.len() as c_int, buffers.pointers.as_ptr().cast::<*const c_char>(),
                    buffers.destination.as_ref().expect("native destination retained").as_ptr(),
                    resources.source_context, report_mounted,
                    Arc::as_ptr(&events.0).cast_mut().cast(), &mut interrupted_before_namespace,
                )
            };
            if interrupted_before_namespace != 0 {
                controller.state.lock().unwrap_or_else(PoisonError::into_inner).start_aborted = true;
            }
            status
        }
    };
    events.0.report(|state| state.provider_exited = true);
    drop(owned.3.take());
    owned.2.finish_retirement_after_loop(&owned.0);
    status
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
impl DarwinMountSession {
    pub(super) fn start_into<F>(
        request: &NativeMountRequest, source: Arc<dyn MountFilesystem>,
        source_work: &super::NativeMountMeterWork, out: &mut Option<Self>,
        retirement: &mut Option<super::NativeMountRetirement>, prepare: F,
    ) -> Result<(), DriverStartFailure>
    where F: FnOnce(&mut super::NativeMountRetirement) -> Result<(), NativeMountError> {
            if out.is_some() { return Err(NativeMountError::ControlPending.into()); }
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
            let context = {
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
                let mut context = DarwinMountContext::new(
                    source, request.writable, metadata.st_uid, metadata.st_gid, root_entries,
                    budget, &cancellation,
                )?;
                context.inodes.get_mut().unwrap_or_else(PoisonError::into_inner)
                    .storage.allocation = Some(root_allocation);
                context.allocation_custody = Some(custody.clone());
                context.native_allocation = Some(materialization.claim(
                    std::mem::size_of::<DarwinMountContext>() as u64,
                ).map_err(NativeMountError::from)?);
                Arc::new(context)
            };
            let _startup_meter = DarwinStartupMeterScope::new(Arc::clone(&context), source_work)?;
            let observer: Weak<DarwinMountContext> = Arc::downgrade(&context);
            context.source.observe_view_with_meter(observer, source_work).map_err(source_error)?;
            let supports_named_attributes = context.source.supports_posix_named_attributes();
            let buffers = DarwinStartupBuffers::new(
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
            let cleanup_credit = DriverSessionResources::reserve_cleanup_credit(&materialization)?;
            let resources = Arc::new(DriverSessionResources {
                source_context: Arc::into_raw(context) as usize, driver_session: AtomicUsize::new(0),
                cleanup_credit: Mutex::new(cleanup_credit),
                native_control: Mutex::new(()),
                sdk_allocation: Mutex::new(None),
                interrupted: std::sync::atomic::AtomicBool::new(false),
                startup: Some(buffers),
                _allocation: Some(resources_allocation),
            });
            let prepared_direct = prepare_direct_unmount(&resources, source_work)?;
            let cleanup_meter = Some(Arc::clone(&prepared_direct._meter));
            let state = UnmountState {
                prepared_direct: Some(prepared_direct), cleanup_meter,
                cleanup_plan: Some(DarwinCleanupPlan::new(&destination, &materialization)?),
                prepared_diskutil: Some(PreparedDiskutilProcess::new(&materialization)?),
                ..UnmountState::default()
            };
            let unmount = DarwinStopController::new(state, &materialization)?;
            let started = Self {
                resources: Some(resources),
                destination, _destination_allocation: Some(destination_allocation),
                thread: None,
                events,
                loop_finished_before_teardown: None, loop_result: None,
                loop_panic: None,
                teardown_complete: true, unmount,
                completed_callback_work: crate::WorkCounters::UNCHARGED,
                retired_callback_failure: Mutex::new(None),
                native_root_identity: None,
            };
            *out = Some(started);
            let started = out.as_mut().expect("logical native driver installed before SDK birth");
            let (builder, owned) = started.prepare_loop(source_work)?;
            darwin_native_attempt(source_work, true)?;
            started.thread = Some(builder.spawn(move || run_darwin_loop(owned))?);
            started.teardown_complete = false;
            let mut packet_loan = {
                let mut state = started.unmount.state.lock().unwrap_or_else(PoisonError::into_inner);
                state.retirement = retirement.take();
                DarwinStartupRetirementLoan {
                    packet: state.retirement.take(), controller: Arc::clone(&started.unmount),
                }
            };
            if let Some(packet) = packet_loan.packet.as_mut() {
                prepare(packet)?;
            }
            packet_loan.restore();
            // The same existing loop now owns the entire retirement packet.
            let (native, allocation) = match DriverSessionResources::new_sdk_session(&materialization) {
                Ok(owned) => owned,
                Err(error) => { started.abort_prepared_start(); return Err(error.into()); }
            };
            *started.resources.as_ref().expect("new resources retained").sdk_allocation.lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(allocation);
            started.resources.as_ref().expect("new resources retained").driver_session.store(native, Ordering::Release);
            started.unmount.state.lock().unwrap_or_else(PoisonError::into_inner).start_ready = true;
            started.unmount.changed.notify_all();
            started.await_mount(source_work).map_err(DriverStartFailure::from)
    }

    fn abort_prepared_start(&self) {
        self.unmount.state.lock().unwrap_or_else(PoisonError::into_inner).start_aborted = true;
        self.unmount.changed.notify_all();
    }

    fn prepare_loop(
        &self, native: &super::NativeMountMeterWork,
    ) -> Result<(std::thread::Builder, (
        Arc<DriverSessionResources>, ExitReport, Arc<DarwinStopController>,
        Option<super::NativeMountMeterWork>, crate::NativeAllocation,
    )), NativeMountError> {
        let materialization = native.native_materialization_work();
        let data = (
            Arc::clone(self.resources.as_ref().expect("native resources retained")),
            ExitReport(Arc::clone(&self.events)), Arc::clone(&self.unmount), Some(native.clone()),
        );
        let name = "acyclic-fs-darwin-nfs";
        let copied = crate::WorkCounters { bytes_copied: name.len() as u64, ..crate::WorkCounters::UNCHARGED };
        materialization.admit(copied)?;
        let payload = std::mem::size_of::<(
            Arc<DriverSessionResources>, ExitReport, Arc<DarwinStopController>,
            Option<super::NativeMountMeterWork>, crate::NativeAllocation,
        )>() as u64;
        let allocation = materialization.claim(payload.checked_add(name.len() as u64)
            .ok_or(crate::WorkError::Overflow)?)?;
        let owned = (data.0, data.1, data.2, data.3, allocation);
        let name = name.to_owned();
        materialization.record_admitted(copied)?;
        Ok((std::thread::Builder::new().name(name), owned))
    }

    fn await_mount(&mut self, native: &super::NativeMountMeterWork) -> Result<(), NativeMountError> {
        let meter = native.meter();
        let deadline = Instant::now() + MOUNT_VISIBILITY_TIMEOUT;
        let reported = loop {
            native.native_materialization_work().charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
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
            {
                let mut state = self.unmount.state.lock().unwrap_or_else(PoisonError::into_inner);
                drop(state.prepared_direct.take());
                drop(state.prepared_diskutil.take());
            }
            self.loop_finished_before_teardown = Some(true);
            self.loop_result = Some(with_detach_meter(
                native,
                |detach| self.finish_thread_with_detach_meter(MOUNT_LOOP_EXIT_TIMEOUT, detach, None),
            )?);
            return Err(std::io::Error::from_raw_os_error(libc::EIO).into());
        }
        let materialization = native.native_materialization_work();
        if !is_mounted(&self.destination, &mut &materialization, None, None)? {
            return Err(std::io::Error::from_raw_os_error(libc::EIO).into());
        }
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
        let file = std::mem::ManuallyDrop::new(unsafe { std::fs::File::from_raw_fd(fd) });
        let mut host = &materialization;
        let identity = crate::native_host::file_identity_and_length_with_work(&file, &mut host);
        let (closed, recorded) = host.attempt_cleanup_write(cleanup, || {
            if unsafe { libc::close(fd) } == 0 { Ok(()) } else { Err(std::io::Error::last_os_error()) }
        });
        let _ = std::mem::ManuallyDrop::into_inner(file).into_raw_fd();
        recorded.map_err(NativeMountError::from)?;
        let (identity, _) = identity?;
        closed?;
        self.native_root_identity = Some(identity);
        Ok(())
    }

    /// Invalidates one mount-relative path.
    pub(super) fn invalidate(&self, path: &[u8]) -> Result<(), NativeMountError> {
        let resources = self.resources()?;
        let context = resources.context();
        if context.handler_failed.load(Ordering::Acquire) {
            return Err(NativeMountError::Lifecycle("Darwin callback previously failed"));
        }
        context.run(context.callback_meter(), |native| {
            let work = DarwinCallbackWork::new(native);
            let result = (|| {
                let path = work.native_name(path, path.first() != Some(&b'/'))?;
                work.charge(crate::WorkCounters {
                    backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
                })?;
                // Reject continuation snapshots before this actual SDK attempt.
                resources.context().forget_view();
                // SAFETY: session and terminated path stay owned through this call.
                let status = unsafe { acyclic_fs_darwin_mount_invalidate(
                    resources.driver_session.load(Ordering::Acquire) as *mut c_void, path.as_ptr(),
                ) };
                if status == 0 { Ok(()) }
                else { Err(work.native_error(NativeMountError::Io(std::io::Error::from_raw_os_error(status)))) }
            })();
            work.finish(result)
        })
    }

    /// Revalidates the source and callback caches.
    pub(super) fn revalidate(&self) -> Result<(), NativeMountError> {
        let cancellation = crate::CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| NativeMountError::from(cause))?;
        self.revalidate_with_meter(&work.native_mount_work())
    }

    /// Revalidates the source and callback caches on the supplied meter.


    pub(super) fn revalidate_with_meter(
        &self, native: &super::NativeMountMeterWork,
    ) -> Result<(), NativeMountError> {
        native.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)?;
        let _active = self.begin_original_work(native)?;
        let context = &_active.context;
        context.source.fence_changes_with_meter(native).map_err(source_error)?;
        context.revalidate(native).map(|_| ())
    }

    pub(super) fn begin_original_work(
        &self, native: &super::NativeMountMeterWork,
    ) -> Result<DarwinStartupMeterScope, NativeMountError> {
        native.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)?;
        let resources = self.resources()?;
        if resources.driver_session.load(Ordering::Acquire) == 0 { return Err(NativeMountError::Lifecycle("session is stopped")); }
        DarwinStartupMeterScope::new(resources.context_owner(), native)
    }

    /// Returns the authenticated mounted-root identity.
    pub(super) fn native_root_identity(&self) -> Option<crate::NativeRootIdentity> {
        if self.teardown_complete || self.namespace_detach_confirmed() { None } else { self.native_root_identity }
    }

    /// Takes the retained callback failure.
    pub(super) fn take_callback_failure(&self) -> Option<super::NativeMountCallbackFailure> {
        if let Some(resources) = self.resources.as_deref() {
            let context = resources.context();
            context.startup_failure.lock().unwrap_or_else(PoisonError::into_inner).take()
                .or_else(|| context.handler_failure.lock().unwrap_or_else(PoisonError::into_inner).first_fatal.take())
        } else {
            self.retired_callback_failure.lock().unwrap_or_else(PoisonError::into_inner).take()
        }
    }

    /// Returns work from completed callback requests.
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

    /// Uses the constructor's original wait without replacing its cancellation.
    pub(super) fn stop(&mut self) -> Result<(), NativeMountError> {
        let meter = self.unmount.state.lock().unwrap_or_else(PoisonError::into_inner)
            .cleanup_meter.as_ref().cloned().ok_or(NativeMountError::ControlPending)?;
        let wait = meter.with_native(|work| Ok(work.cancellation().clone()))?;
        self.stop_with_cleanup_wait(&wait)
    }

    /// Stops using the original cleanup credit and supplied wait cancellation.
    pub(super) fn stop_with_cleanup_wait(
        &mut self, wait: &crate::CancellationToken,
    ) -> Result<(), NativeMountError> {
        wait.check().map_err(|_| NativeMountError::from(crate::DemandError::Cancelled))?;
        if self.teardown_complete {
            let mut state = self.unmount.state.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(retirement) = &mut state.retirement { retirement.retire_with_cleanup_wait(wait)?; }
            return Ok(());
        }
        let controller = Arc::clone(&self.unmount);
        let (meter, mut observer) = {
            let mut state = controller.state.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(cause) = state.failure.take() { return Err(cause); }
            let meter = state.cleanup_meter.as_ref().cloned().ok_or(NativeMountError::ControlPending)?;
            let observer = state.observer_credit.take().ok_or(NativeMountError::ControlPending)?;
            controller.retirement_requested.store(true, Ordering::Release);
            controller.changed.notify_all();
            (meter, observer)
        };
        let result = {
            let loan = observer.cleanup_loan(wait);
            self.stop_with_detach_meter(&meter, None, Some(&loan))
        };
        controller.state.lock().unwrap_or_else(PoisonError::into_inner).observer_credit = Some(observer);
        controller.changed.notify_all();
        result
    }

    pub(super) fn stop_with_meter(
        &mut self, native: &super::NativeMountMeterWork,
    ) -> Result<(), NativeMountError> {
        self.stop_with_cleanup_wait(native.native_materialization_work().cancellation())
    }

    fn stop_with_detach_meter(
        &mut self, meter: &Arc<DetachReadMeter>, scope: Option<&DarwinStartupMeterScope>,
        cleanup: Option<&crate::materializer::NativeCleanupLoan<'_>>,
    ) -> Result<(), NativeMountError> {
        detach_check(meter, cleanup)?;
        if self.loop_finished_before_teardown.is_none() {
            self.loop_finished_before_teardown = Some(if self.thread.is_none() {
                true
            } else {
                detach_observe(meter, cleanup, || self.events.current().provider_exited)?
            });
        }
        let controller = Arc::clone(&self.unmount);
        let unmount = {
            let mut state = controller.state.lock().unwrap_or_else(PoisonError::into_inner);
            if state.detached && state.direct.is_none() && state.diskutil.is_none()
                && self.resources.as_ref().is_none_or(|resources| resources.driver_session.load(Ordering::Acquire) == 0)
                && state.retirement.as_ref().is_none_or(super::NativeMountRetirement::is_retired)
            {
                Ok(false)
            } else {
                let mut plan = state.cleanup_plan.take().ok_or(NativeMountError::ControlPending)?;
                let result = {
                    let wait = cleanup.and_then(|loan| loan.cancellation());
                    let loan = match wait {
                        Some(wait) => plan.credit.cleanup_loan(wait),
                        None => plan.credit.accepted_cleanup_loan(),
                    };
                    bounded_unmount(
                        &self.destination, &mut state, meter, self.resources.as_ref(), scope,
                        Some(&mut plan.scratch), Some(&loan),
                    )
                };
                state.cleanup_plan = Some(plan);
                result
            }
        }?;
        controller.changed.notify_all();
        detach_check(meter, cleanup)?;
        if self
            .loop_result
            .as_ref()
            .is_none_or(|result| matches!(result, MountLoopResult::TimedOut))
        {
            if let Some(resources) = &self.resources {
                let wait = detach_wait(meter, cleanup)?.ok_or(NativeMountError::ControlPending)?;
                if !self.events.current().provider_exited { resources.interrupt_with_cleanup_wait(&wait)?; }
            }
            let join = self.finish_thread_with_detach_meter(MOUNT_LOOP_EXIT_TIMEOUT, meter, cleanup)?;
            self.loop_result = Some(join);
        }
        else if self.resources.is_some() {
            self.finish_thread_with_detach_meter(MOUNT_LOOP_EXIT_TIMEOUT, meter, cleanup)?;
        }
        let mut state = controller.state.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(task) = &mut state.direct {
            if poll_direct(task, meter, cleanup)?.is_ready() { state.direct.take(); }
        }
        if state.direct.is_some() || state.diskutil.is_some() {
            return Err(NativeMountError::ControlPending);
        }
        let loop_finished_before_teardown = self.loop_finished_before_teardown.expect("teardown observed the provider before detach");
        let join = self.loop_result.as_ref().expect("teardown observed the provider join");
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

    /// Returns the last confirmed namespace detach.
    pub(super) fn namespace_detach_confirmed(&self) -> bool {
        self.unmount.state.lock().unwrap_or_else(PoisonError::into_inner).detached
    }
    /// Returns retained destination and source ownership.
    pub(super) fn retirement_ownership(&self) -> (bool, bool) {
        let state = self.unmount.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.retirement.as_ref().map_or((false, false), |packet| (
            packet.destination_guard.is_some(),
            packet.retained_source.is_some() || packet.startup_source_watch.is_some(),
        ))
    }


    fn finish_thread_with_detach_meter(
        &mut self, timeout: Duration, meter: &Arc<DetachReadMeter>,
        cleanup: Option<&crate::materializer::NativeCleanupLoan<'_>>,
    ) -> Result<MountLoopResult, NativeMountError> {
        if self.thread.is_none() {
            if self.resources.is_some() {
                let wait = detach_wait(meter, cleanup)?.ok_or(NativeMountError::ControlPending)?;
                self.release_resources_with_cleanup_wait(&wait)?;
            }
            return Ok(self.loop_result.unwrap_or(MountLoopResult::Exited(0)));
        }
        let deadline = Instant::now() + timeout;
        loop {
            let (exited, failure) = detach_observe(meter, cleanup, || (
                self.events.current().exited,
                self.unmount.state.lock().unwrap_or_else(PoisonError::into_inner).failure.take(),
            ))?;
            if exited { break; }
            if let Some(cause) = failure { return Err(cause); }
            if Instant::now() >= deadline { return Ok(MountLoopResult::TimedOut); }
            detach_started_read(meter, cleanup)?;
            self.events.wait_until(STOP_POLL_INTERVAL, |state| state.exited);
        }
        let wait = detach_wait(meter, cleanup)?.ok_or(NativeMountError::ControlPending)?;
        if let Some(resources) = &self.resources {
            resources.start_join_with_cleanup_wait(&wait)?;
        } else {
            detach_started_read(meter, cleanup)?;
        }
        let thread = self.thread.take().expect("admitted mount loop remains owned");
        let status = match thread.join() {
            Ok(status) => MountLoopResult::Exited(status),
            Err(panic) => {
                self.loop_panic = Some(Mutex::new(panic));
                MountLoopResult::Panicked
            }
        };
        self.loop_result = Some(status);
        if self.resources.is_some() {
            self.release_resources_with_cleanup_wait(&wait)?;
        }
        Ok(status)
    }

    fn release_resources_with_cleanup_wait(
        &mut self, wait: &crate::CancellationToken,
    ) -> Result<(), NativeMountError> {
        let Some(resources) = self.resources.as_mut().and_then(Arc::get_mut) else {
            return Err(NativeMountError::ControlPending);
        };
        resources.free_with_cleanup_wait(wait)?;
        self.completed_callback_work = self.completed_callback_work();
        let failure = self.take_callback_failure();
        let retained = self.retired_callback_failure.get_mut().unwrap_or_else(PoisonError::into_inner);
        if retained.is_none() { *retained = failure; }
        drop(self.resources.take());
        let mut state = self.unmount.state.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(retirement) = &mut state.retirement { retirement.retire_with_cleanup_wait(wait)?; }
        Ok(())
    }
}

fn classify_detached_loop(
    join: &MountLoopResult, was_mounted: bool, loop_finished_before_teardown: bool,
) -> Result<(), NativeMountError> {
    let failure = match (join, was_mounted, loop_finished_before_teardown) {
        (MountLoopResult::Exited(0), _, false)
        | (MountLoopResult::Exited(0), false, true) => return Ok(()),
        (MountLoopResult::Exited(status), _, true) => DarwinTeardownFailure::LoopExitedBeforeDetach(*status),
        (MountLoopResult::Exited(status), _, false) => DarwinTeardownFailure::LoopExitedAfterDetach(*status),
        (MountLoopResult::TimedOut, _, _) => DarwinTeardownFailure::LoopExitTimedOut,
        (MountLoopResult::Panicked, _, _) => DarwinTeardownFailure::LoopPanicked,
    };
    Err(NativeMountError::DarwinTeardown(failure))
}

impl Drop for DarwinMountSession {
    fn drop(&mut self) {
        if self.teardown_complete { return; }
        self.unmount.retirement_requested.store(true, Ordering::Release);
        self.unmount.changed.notify_all();
        if !self.events.current().provider_exited {
            if let Some(resources) = &self.resources { resources.interrupt_accepted_cleanup(); }
        }
    }
}


fn detach_check(
    meter: &Arc<DetachReadMeter>, cleanup: Option<&crate::materializer::NativeCleanupLoan<'_>>,
) -> Result<(), NativeMountError> {
    match cleanup {
        Some(loan) => loan.check_wait().map_err(NativeMountError::from),
        None => meter.with_native(|work| work.admit(crate::WorkCounters::UNCHARGED).map_err(NativeMountError::from)),
    }
}

fn detach_wait(
    meter: &Arc<DetachReadMeter>, cleanup: Option<&crate::materializer::NativeCleanupLoan<'_>>,
) -> Result<Option<crate::CancellationToken>, NativeMountError> {
    detach_check(meter, cleanup)?;
    match cleanup {
        Some(loan) => Ok(loan.cancellation().cloned()),
        None => meter.with_native(|work| Ok(Some(work.cancellation().clone()))),
    }
}

fn detach_started_read(
    meter: &Arc<DetachReadMeter>, cleanup: Option<&crate::materializer::NativeCleanupLoan<'_>>,
) -> Result<(), NativeMountError> {
    match cleanup {
        Some(loan) => loan.started_io(crate::WorkCounters {
            backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(NativeMountError::from),
        None => meter.attempt(false),
    }
}

fn detach_observe<T>(
    meter: &Arc<DetachReadMeter>, cleanup: Option<&crate::materializer::NativeCleanupLoan<'_>>,
    operation: impl FnOnce() -> T,
) -> Result<T, NativeMountError> {
    let delta = crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED };
    meter.with_native(|mut work| darwin_probe_cpu(&mut work, cleanup, delta, operation))
}


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
    call().map_err(NativeMountError::Io)
}

struct DarwinStopScratch {
    parent: DetachPath,
    normalized: Vec<std::mem::MaybeUninit<u8>>,
    mounts: Vec<std::mem::MaybeUninit<libc::statfs>>,
    _normalized_allocation: crate::NativeAllocation,
    _mounts_allocation: crate::NativeAllocation,
}

struct DarwinCleanupPlan {
    scratch: DarwinStopScratch,
    credit: crate::materializer::NativeWorkReservation,
}

fn closed_control_visits(timeout: Duration, interval: Duration) -> Result<u64, NativeMountError> {
    let interval = interval.as_nanos();
    let visits = timeout.as_nanos().checked_add(interval - 1)
        .ok_or(crate::WorkError::Overflow)? / interval;
    u64::try_from(visits.checked_add(1).ok_or(crate::WorkError::Overflow)?)
        .map_err(|_| crate::WorkError::Overflow.into())
}

fn stop_mount_probe_maximum() -> Result<u64, NativeMountError> {
    closed_control_visits(DISKUTIL_VISIBILITY_TIMEOUT, STOP_POLL_INTERVAL)?
        .checked_add(3).ok_or_else(|| crate::WorkError::Overflow.into())
}

impl DarwinCleanupPlan {
    fn new(
        destination: &Path, original: &crate::NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        let parent = destination.parent().ok_or(NativeMountError::Lifecycle(
            "mount parent disappeared before driver birth",
        ))?;
        let parent = DetachPath::new(parent, original)?;
        original.charge(crate::WorkCounters {
            backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED
        })?;
        let count = unsafe { libc::getfsstat(ptr::null_mut(), 0, libc::MNT_NOWAIT) };
        if count < 0 { return Err(std::io::Error::last_os_error().into()); }
        // This driver's one future namespace and one truncation sentinel.
        let capacity = (count as usize).checked_add(1).and_then(|count| count.checked_add(1))
            .ok_or(crate::WorkError::Overflow)?;
        let bytes = capacity.checked_mul(std::mem::size_of::<libc::statfs>())
            .ok_or(crate::WorkError::Overflow)?;
        c_int::try_from(bytes).map_err(|_| crate::WorkError::Overflow)?;
        let mut mounts_allocation = original.claim(bytes as u64)?;
        let mut mounts = Vec::new();
        mounts.try_reserve_exact(capacity)?;
        let bytes = mounts.capacity().checked_mul(std::mem::size_of::<libc::statfs>())
            .ok_or(crate::WorkError::Overflow)?;
        c_int::try_from(bytes).map_err(|_| crate::WorkError::Overflow)?;
        mounts_allocation.reconcile_capacity(bytes as u64)?;
        let mut normalized_allocation = original.claim(libc::PATH_MAX as u64)?;
        let mut normalized = Vec::new();
        normalized.try_reserve_exact(libc::PATH_MAX as usize)?;
        normalized_allocation.reconcile_capacity(normalized.capacity() as u64)?;
        // MaybeUninit storage is allocated, not spuriously zero-filled.
        unsafe { normalized.set_len(libc::PATH_MAX as usize); }
        let probes = stop_mount_probe_maximum()?;
        let direct = closed_control_visits(DIRECT_UNMOUNT_TIMEOUT, STOP_POLL_INTERVAL)?;
        let diskutil = closed_control_visits(DISKUTIL_UNMOUNT_TIMEOUT, STOP_POLL_INTERVAL)?;
        let visibility = closed_control_visits(DISKUTIL_VISIBILITY_TIMEOUT, STOP_POLL_INTERVAL)?;
        let mount_loop = closed_control_visits(MOUNT_LOOP_EXIT_TIMEOUT, STOP_POLL_INTERVAL)?;
        let waits = direct.checked_add(diskutil).and_then(|n| n.checked_add(visibility))
            .and_then(|n| n.checked_add(mount_loop)).ok_or(crate::WorkError::Overflow)?;
        let probe_items = (libc::PATH_MAX as u64).checked_add(mounts.capacity() as u64)
            .and_then(|n| n.checked_mul(probes)).ok_or(crate::WorkError::Overflow)?;
        let name = destination.file_name().ok_or(NativeMountError::Lifecycle(
            "mount destination has no name",
        ))?.as_bytes();
        let credit = original.reserve_work(crate::WorkCounters {
            // Every probe has exactly open, fstat, F_GETPATH, count and fill.
            backend_read_operations: probes.checked_mul(5).and_then(|n| n.checked_add(waits))
                .ok_or(crate::WorkError::Overflow)?,
            backend_write_operations: probes,
            items_examined: probe_items.checked_add(waits).and_then(|n| n.checked_add(3))
                .ok_or(crate::WorkError::Overflow)?,
            bytes_copied: probes.checked_mul(name.len() as u64).ok_or(crate::WorkError::Overflow)?,
            ..crate::WorkCounters::UNCHARGED
        })?;
        Ok(Self { scratch: DarwinStopScratch {
            parent, normalized, mounts,
            _normalized_allocation: normalized_allocation, _mounts_allocation: mounts_allocation,
        }, credit })
    }
}

enum DarwinProbeBuffer<'a, T, A> {
    Owned(Vec<std::mem::MaybeUninit<T>>, Option<A>),
    Borrowed(&'a mut Vec<std::mem::MaybeUninit<T>>),
}

impl<T, A> DarwinProbeBuffer<'_, T, A> {
    fn data(&mut self) -> &mut Vec<std::mem::MaybeUninit<T>> {
        match self { Self::Owned(data, _) => data, Self::Borrowed(data) => data }
    }

    fn ensure<W: crate::native_host::NativeHostWork<Allocation = A>>(
        &mut self, count: usize, work: &mut W,
    ) -> Result<(), NativeMountError> where NativeMountError: From<W::Error> {
        match self {
            Self::Borrowed(data) => {
                if data.capacity() < count {
                    return Err(NativeMountError::DarwinTeardown(DarwinTeardownFailure::MountTableCapacityExceeded));
                }
            }
            Self::Owned(data, allocation) => {
                if data.capacity() >= count { return Ok(()); }
                drop(std::mem::take(data));
                if let Some(allocation) = allocation.take() { work.release(allocation)?; }
                let bytes = count.checked_mul(std::mem::size_of::<T>()).ok_or(crate::WorkError::Overflow)?;
                *allocation = Some(work.claim(bytes as u64)?);
                data.try_reserve_exact(count).map_err(W::allocation_failed)?;
                let actual = data.capacity().checked_mul(std::mem::size_of::<T>())
                    .ok_or(crate::WorkError::Overflow)?;
                work.reconcile_allocation(allocation.as_mut().expect("buffer has original custody"), actual as u64)?;
            }
        }
        Ok(())
    }

    fn clear<W: crate::native_host::NativeHostWork<Allocation = A>>(
        &mut self, work: &mut W,
    ) -> Result<(), NativeMountError> where NativeMountError: From<W::Error> {
        if let Self::Owned(data, allocation) = self {
            drop(std::mem::take(data));
            if let Some(allocation) = allocation.take() { work.release(allocation)?; }
        }
        Ok(())
    }

    fn release<W: crate::native_host::NativeHostWork<Allocation = A>>(
        mut self, work: &mut W,
    ) -> Result<(), NativeMountError> where NativeMountError: From<W::Error> {
        self.clear(work)
    }
}

fn darwin_probe_start<W: crate::native_host::NativeHostWork>(
    work: &mut W, cleanup: Option<&crate::materializer::NativeCleanupLoan<'_>>,
) -> Result<(), NativeMountError> where NativeMountError: From<W::Error> {
    let read = crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED };
    if let Some(cleanup) = cleanup { cleanup.started_io(read)?; }
    else { work.started_io(read)?; }
    Ok(())
}

fn darwin_probe_cpu<T, W: crate::native_host::NativeHostWork>(
    work: &mut W, cleanup: Option<&crate::materializer::NativeCleanupLoan<'_>>,
    delta: crate::WorkCounters, call: impl FnOnce() -> T,
) -> Result<T, NativeMountError> where NativeMountError: From<W::Error> {
    if let Some(cleanup) = cleanup {
        let mut credit = cleanup.admit_completed(delta)?;
        let value = call();
        credit.record_completed(delta)?;
        Ok(value)
    } else {
        work.admit(delta)?;
        let value = call();
        work.record(delta)?;
        Ok(value)
    }
}

enum DarwinProbeClose<A> {
    Original(A),
    Accepted(crate::materializer::NativeWorkReservation),
}

fn normalize_mount_probe<W: crate::native_host::NativeHostWork>(
    destination: &Path, name: &CStr, normalized: &mut DarwinProbeBuffer<'_, u8, W::Allocation>,
    work: &mut W, cleanup: Option<&crate::materializer::NativeCleanupLoan<'_>>,
) -> Result<usize, NativeMountError> where NativeMountError: From<W::Error> {
    let close = if let Some(cleanup) = cleanup {
        DarwinProbeClose::Accepted(cleanup.admit_completed(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        })?)
    } else { DarwinProbeClose::Original(work.reserve_cleanup_write()?) };
    if let Err(error) = darwin_probe_start(work, cleanup) {
        if let DarwinProbeClose::Original(close) = close { work.cancel_cleanup_write(close); }
        return Err(error);
    }
    let fd = unsafe { libc::open(name.as_ptr(), libc::O_EVTONLY | libc::O_CLOEXEC) };
    if fd < 0 {
        if let DarwinProbeClose::Original(close) = close { work.cancel_cleanup_write(close); }
        return Err(std::io::Error::last_os_error().into());
    }
    let result = (|| {
        darwin_probe_start(work, cleanup)?;
        let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
        if unsafe { libc::fstat(fd, metadata.as_mut_ptr()) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let size = libc::PATH_MAX as usize;
        normalized.ensure(size, work)?;
        let path = normalized.data();
        // SAFETY: capacity is admitted, and MaybeUninit requires no writes.
        unsafe { path.set_len(size); }
        darwin_probe_start(work, cleanup)?;
        if unsafe { libc::fcntl(fd, libc::F_GETPATH, path.as_mut_ptr()) } < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let mut length = 0;
        for byte in path.iter() {
            let is_end = darwin_probe_cpu(work, cleanup, crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            }, || unsafe { byte.assume_init() } == 0)?;
            if is_end { break; }
            length += 1;
        }
        if length == size { return Err(NativeMountError::from(W::invalid_request())); }
        let name = destination.file_name().ok_or_else(W::invalid_request)?.as_bytes();
        let separator = usize::from(length != 0 && unsafe { path[length - 1].assume_init() } != b'/');
        let end = length.checked_add(separator).and_then(|length| length.checked_add(name.len()))
            .ok_or(crate::WorkError::Overflow)?;
        if end > size { return Err(std::io::Error::from_raw_os_error(libc::ENAMETOOLONG).into()); }
        darwin_probe_cpu(work, cleanup, crate::WorkCounters {
            bytes_copied: name.len() as u64, ..crate::WorkCounters::UNCHARGED
        }, || {
            if separator != 0 { path[length].write(b'/'); length += 1; }
            // SAFETY: the checked initialized prefix fits this original buffer.
            unsafe { ptr::copy_nonoverlapping(name.as_ptr(), path.as_mut_ptr().add(length).cast::<u8>(), name.len()); }
        })?;
        Ok(end)
    })();
    let close = match close {
        DarwinProbeClose::Original(close) => {
            let (status, recorded) = work.attempt_cleanup_write(close, || unsafe { libc::close(fd) });
            recorded.map_err(NativeMountError::from).and_then(|()| if status == 0 { Ok(()) }
                else { Err(std::io::Error::last_os_error().into()) })
        }
        DarwinProbeClose::Accepted(mut close) => {
            close.started_drop_io(crate::WorkCounters {
                backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
            });
            if unsafe { libc::close(fd) } == 0 { Ok(()) } else { Err(std::io::Error::last_os_error().into()) }
        }
    };
    match (result, close) { (Ok(end), Ok(())) => Ok(end), (Err(error), _) | (_, Err(error)) => Err(error) }
}
fn is_mounted<W: crate::native_host::NativeHostWork>(
    destination: &Path, work: &mut W, scratch: Option<&mut DarwinStopScratch>,
    cleanup: Option<&crate::materializer::NativeCleanupLoan<'_>>,
) -> Result<bool, NativeMountError> where NativeMountError: From<W::Error> {
    let parent = destination.parent().ok_or(NativeMountError::Lifecycle(
        "mount parent disappeared during teardown",
    ))?;
    let (parent_name, mut normalized, mut mounts, bounded) = match scratch {
        Some(scratch) => (
            Some(scratch.parent.path.as_c_str()),
            DarwinProbeBuffer::Borrowed(&mut scratch.normalized),
            DarwinProbeBuffer::Borrowed(&mut scratch.mounts), true,
        ),
        None => (
            None, DarwinProbeBuffer::Owned(Vec::new(), None),
            DarwinProbeBuffer::Owned(Vec::new(), None), false,
        ),
    };
    let result = (|| {
        let end = if let Some(parent_name) = parent_name {
            normalize_mount_probe(destination, parent_name, &mut normalized, work, cleanup)?
        } else {
            crate::native_host::unix_name_with_work(parent.as_os_str(), work, |name, work| {
                Ok(normalize_mount_probe(destination, name, &mut normalized, work, cleanup))
            })??
        };
        // Only F_GETPATH's initialized prefix and the copied filename are read.
        let normalized_name = unsafe {
            std::slice::from_raw_parts(normalized.data().as_ptr().cast::<u8>(), end)
        };
        loop {
            darwin_probe_start(work, cleanup)?;
            let count = unsafe { libc::getfsstat(ptr::null_mut(), 0, libc::MNT_NOWAIT) };
            if count < 0 { return Err(std::io::Error::last_os_error().into()); }
            let required = (count as usize).checked_add(1).ok_or(crate::WorkError::Overflow)?;
            mounts.ensure(required, work)?;
            let capacity = mounts.data().capacity();
            let bytes = capacity.checked_mul(std::mem::size_of::<libc::statfs>())
                .ok_or(crate::WorkError::Overflow)?;
            let native_bytes = c_int::try_from(bytes).map_err(|_| crate::WorkError::Overflow)?;
            darwin_probe_start(work, cleanup)?;
            let count = unsafe { libc::getfsstat(mounts.data().as_mut_ptr().cast(), native_bytes, libc::MNT_NOWAIT) };
            if count < 0 { return Err(std::io::Error::last_os_error().into()); }
            if count as usize >= capacity {
                if bounded {
                    return Err(NativeMountError::DarwinTeardown(DarwinTeardownFailure::MountTableCapacityExceeded));
                }
                mounts.clear(work)?;
                continue;
            }
            let table = mounts.data();
            // SAFETY: this successful fill initialized exactly count entries.
            unsafe { table.set_len(count as usize); }
            for mount in table.iter() {
                let matches = darwin_probe_cpu(work, cleanup, crate::WorkCounters {
                    items_examined: 1, ..crate::WorkCounters::UNCHARGED
                }, || {
                    let mount = unsafe { mount.assume_init_ref() };
                    let name = unsafe { CStr::from_ptr(mount.f_mntonname.as_ptr()) }.to_bytes();
                    name == destination.as_os_str().as_bytes() || name == normalized_name
                })?;
                if matches { return Ok(true); }
            }
            return Ok(false);
        }
    })();
    let release_mounts = mounts.release(work);
    let release_normalized = normalized.release(work);
    match result {
        Err(error) => Err(error),
        Ok(value) => { release_mounts?; release_normalized?; Ok(value) }
    }
}


struct DarwinStopController {
    state: Mutex<UnmountState>,
    changed: Condvar,
    retirement_requested: std::sync::atomic::AtomicBool,
    _allocation: Option<crate::NativeAllocation>,
}

impl DarwinStopController {
    fn new(
        mut state: UnmountState,
        original: &crate::NativeMaterializationWork,
    ) -> Result<Arc<Self>, NativeMountError> {
        let allocation = original.claim(std::mem::size_of::<Self>() as u64)?;
        if let Some(plan) = &mut state.cleanup_plan {
            let visits = closed_control_visits(MOUNT_LOOP_EXIT_TIMEOUT, STOP_POLL_INTERVAL)?;
            state.observer_credit = Some(plan.credit.partition_work(crate::WorkCounters {
                backend_read_operations: visits,
                items_examined: visits.checked_add(2).ok_or(crate::WorkError::Overflow)?,
                ..crate::WorkCounters::UNCHARGED
            })?);
        }
        state.start_gate_credit = Some(original.reserve_work(crate::WorkCounters {
            backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED
        })?);
        state.failure_wait_credit = Some(original.reserve_work(crate::WorkCounters {
            backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED
        })?);
        Ok(Arc::new(Self {
            state: Mutex::new(state), changed: Condvar::new(),
            retirement_requested: std::sync::atomic::AtomicBool::new(false), _allocation: Some(allocation),
        }))
    }

    fn retire_once_after_loop(
        state: &mut UnmountState, resources: &Arc<DriverSessionResources>,
    ) -> Result<(), NativeMountError> {
        let meter = state.cleanup_meter.as_ref().cloned().ok_or(NativeMountError::ControlPending)?;
        if !state.detached || state.direct.is_some() || state.diskutil.is_some() {
            let destination = resources.startup.as_ref().and_then(|startup| startup.destination.as_ref())
                .ok_or(NativeMountError::ControlPending)?;
            let destination = Path::new(std::ffi::OsStr::from_bytes(destination.as_bytes()));
            let mut plan = state.cleanup_plan.take().ok_or(NativeMountError::ControlPending)?;
            let result = {
                let loan = plan.credit.accepted_cleanup_loan();
                bounded_unmount(destination, state, &meter, Some(resources), None,
                    Some(&mut plan.scratch), Some(&loan))
            };
            state.cleanup_plan = Some(plan);
            result?;
        }
        if state.direct.is_some() || state.diskutil.is_some() { return Err(NativeMountError::ControlPending); }
        drop(state.prepared_direct.take());
        drop(state.prepared_diskutil.take());
        resources.free_after_loop_quiescence();
        if let Some(retirement) = &mut state.retirement {
            retirement.retire_accepted_cleanup()?;
            if !retirement.is_retired() { return Err(NativeMountError::ControlPending); }
        }
        Ok(())
    }

    fn finish_retirement_after_loop(&self, resources: &Arc<DriverSessionResources>) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        self.retirement_requested.store(false, Ordering::Release);
        match Self::retire_once_after_loop(&mut state, resources) {
            Ok(()) => return,
            Err(cause) => { state.failure = Some(cause); }
        }
        let mut pending_wait = state.failure_wait_credit.take().expect("the loop's birth-paid retained-owner wait");
        pending_wait.started_drop_io(crate::WorkCounters {
            backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED
        });
        state = self.changed.wait_while(state, |state| {
            if !self.retirement_requested.swap(false, Ordering::AcqRel) { return true; }
            match Self::retire_once_after_loop(state, resources) {
                Ok(()) => false,
                Err(cause) => { if state.failure.is_none() { state.failure = Some(cause); } true }
            }
        }).unwrap_or_else(PoisonError::into_inner);
        state.failure_wait_credit = Some(pending_wait);
        drop(state);
    }
}

#[derive(Default)]
struct UnmountState {
    direct: Option<DirectUnmountTask>,
    prepared_direct: Option<DirectUnmountTask>,
    cleanup_plan: Option<DarwinCleanupPlan>,
    cleanup_meter: Option<Arc<DetachReadMeter>>,
    prepared_diskutil: Option<PreparedDiskutilProcess>,
    diskutil: Option<DiskutilProcess>,
    observer_credit: Option<crate::materializer::NativeWorkReservation>,
    retirement: Option<super::NativeMountRetirement>,
    failure: Option<NativeMountError>,
    start_gate_credit: Option<crate::materializer::NativeWorkReservation>,
    failure_wait_credit: Option<crate::materializer::NativeWorkReservation>,
    start_ready: bool,
    start_aborted: bool,
    detached: bool,
}

/// Retains the original detach work and allocation.
pub(crate) struct DetachReadMeter {
    work: Mutex<Option<super::NativeMountMeterWork>>,
    custody: crate::demand::NativeMountAllocationCustody,
}

struct DirectUnmountTask {
    task: Option<acyclic_native_runtime::NativeCompletion<std::io::Result<c_int>>>,
    submission_failure: Option<std::io::Error>,
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

    fn seal(&self) {
        self.work.lock().unwrap_or_else(PoisonError::into_inner).take();
    }

}


fn with_detach_meter<T>(
    native: &super::NativeMountMeterWork,
    call: impl FnOnce(&Arc<DetachReadMeter>) -> Result<T, NativeMountError>,
) -> Result<T, NativeMountError> {
    let meter = new_detach_meter(native)?;
    let custody = meter.custody.clone();
    let loan = DetachLoan(&meter);
    let result = call(&meter);
    drop(loan);
    drop(meter);
    drop(custody);
    result
}

fn new_detach_meter(
    native: &super::NativeMountMeterWork,
) -> Result<Arc<DetachReadMeter>, NativeMountError> {
    let custody = super::NativeMountAllocationCustody::new_with_meter(native)?;
    custody.claim_with_meter(std::mem::size_of::<DetachReadMeter>() as u64, native)?;
    let meter = Arc::new(DetachReadMeter {
        work: Mutex::new(Some(native.clone())), custody: custody.clone(),
    });
    custody.attach_known_control_with_meter(Arc::downgrade(&meter) as _, native)?;
    Ok(meter)
}


struct DetachPath {
    path: CString,
    _allocation: crate::NativeAllocation,
}

impl DetachPath {
    fn new(
        destination: &Path, work: &crate::NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
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
        let path = Self {
            // SAFETY: the scan rejected interior NULs; one terminator was appended.
            path: unsafe { CString::from_vec_with_nul_unchecked(path) }, _allocation: allocation,
        };
        work.record_admitted(copied)?;
        Ok(path)
    }
}

enum DirectUnmountPath {
    Driver(Arc<DriverSessionResources>),
    Recovery(DetachPath),
}

impl DirectUnmountPath {
    fn as_ptr(&self) -> *const c_char {
        match self {
            Self::Driver(resources) => resources.startup.as_ref().expect("retained native startup")
                .destination.as_ref().expect("retained native destination").as_ptr(),
            Self::Recovery(path) => path.path.as_ptr(),
        }
    }

    fn unmount(
        self, operation: acyclic_native_runtime::OwnedNativeOperationReservation,
        scope: Option<DarwinStartupMeterScope>, original_work: crate::NativeMaterializationWork,
    ) -> std::io::Result<c_int> {
        let mut owned = (self, operation, scope, original_work);
        let path = owned.0.as_ptr();
        let (result, recorded) = owned.1.attempt(|| {
            // SAFETY: owned retains the terminated path through this syscall.
            let status = unsafe { libc::unmount(path, libc::MNT_FORCE) };
            if status == 0 { Ok(status) } else { Err(std::io::Error::last_os_error()) }
        });
        drop(owned);
        result.and_then(|status| recorded.map(|()| status))
    }

}

fn poll_direct(
    task: &mut DirectUnmountTask, meter: &Arc<DetachReadMeter>,
    cleanup: Option<&crate::materializer::NativeCleanupLoan<'_>>,
) -> Result<std::task::Poll<std::io::Result<std::io::Result<c_int>>>, NativeMountError> {
    let visit = crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED };
    let mut credit = if let Some(cleanup) = cleanup {
        Some(cleanup.admit_completed(visit)?)
    } else {
        meter.with_native(|work| work.charge(visit).map_err(NativeMountError::from))?;
        None
    };
    let failed = task.submission_failure.is_some();
    let result = if failed { std::task::Poll::Pending }
    else if let Some(loan) = cleanup.filter(|loan| loan.cancellation().is_none()) {
        loan.started_io(crate::WorkCounters {
            backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED
        })?;
        let accepted = task.task.take().ok_or(NativeMountError::ControlPending)?;
        std::task::Poll::Ready(accepted.wait_measured(None, std::future::pending(), || {}))
    } else {
        std::pin::Pin::new(task.task.as_mut().ok_or(NativeMountError::ControlPending)?)
            .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    };
    if let Some(credit) = &mut credit { credit.record_completed(visit)?; }
    if failed { return Err(NativeMountError::ControlPending); }
    match result {
        std::task::Poll::Ready(Err(error)) => {
            task.submission_failure = Some(error);
            Err(NativeMountError::ControlPending)
        }
        result => Ok(result),
    }
}

fn prepare_direct_unmount(
    resources: &Arc<DriverSessionResources>, native: &super::NativeMountMeterWork,
) -> Result<DirectUnmountTask, NativeMountError> {
    let original = native.native_materialization_work();
    let credit = original.reserve_work(crate::WorkCounters {
        backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
    })?;
    let credit = crate::materializer::NativeOwnedCleanupCredit::from_reserved(credit)?;
    let task_meter = credit.runtime_meter_owner();
    let operation = acyclic_native_runtime::OwnedNativeOperationReservation::new(
        Arc::clone(&task_meter), acyclic_native_runtime::NativeOperationKind::Write, 1,
    ).map_err(|error| NativeMountError::from(native.meter().take_native_io_failure()
        .unwrap_or_else(|| native_demand_error(error))))?;
    let metadata = original.allocation_work().native_mount_work();
    let meter = new_detach_meter(&metadata)?;
    let path = DirectUnmountPath::Driver(Arc::clone(resources));
    let original_work = original.allocation_work();
    let task = acyclic_native_runtime::prepare_blocking_io_measured(
        move || path.unmount(operation, None, original_work), task_meter,
    ).map_err(|error| NativeMountError::from(native.meter().take_native_io_failure()
        .unwrap_or_else(|| native_demand_error(error))))?;
    Ok(DirectUnmountTask {
        task: Some(task), submission_failure: None, _meter: Arc::clone(&meter),
        original: metadata, _custody: meter.custody.clone(),
    })
}

fn bounded_direct_unmount(
    destination: &Path, state: &mut UnmountState, meter: &Arc<DetachReadMeter>,
    resources: Option<&Arc<DriverSessionResources>>,
    scope: Option<&DarwinStartupMeterScope>,
    cleanup: Option<&crate::materializer::NativeCleanupLoan<'_>>,
) -> Result<(), NativeMountError> {
    if let Some(task) = &mut state.direct {
        if let std::task::Poll::Ready(result) = poll_direct(task, meter, cleanup)? {
            let task = state.direct.take().expect("observed accepted task");
            return result.and_then(std::convert::identity).map(|_| ())
                .map_err(|error| NativeMountError::from(task.original.meter().take_native_io_failure()
                    .unwrap_or_else(|| native_demand_error(error))));
        }
        return Ok(());
    }
    detach_check(meter, cleanup)?;
    if let Some(prepared) = state.prepared_direct.take() { state.direct = Some(prepared); }
    if state.direct.is_none() && resources.is_some() { return Ok(()); }
    if state.direct.is_none() {
        let path = DirectUnmountPath::Recovery(
            meter.with_native(|work| DetachPath::new(destination, work))?,
        );
    let original = meter.work.lock().unwrap_or_else(PoisonError::into_inner)
        .as_ref().cloned().ok_or(crate::DemandError::Cancelled)?;
    let task_meter = original.native_materialization_work().runtime_meter_owner();
    let operation = acyclic_native_runtime::OwnedNativeOperationReservation::new(
        Arc::clone(&task_meter), acyclic_native_runtime::NativeOperationKind::Write, 1,
    ).map_err(|error| NativeMountError::from(original.meter().take_native_io_failure()
        .unwrap_or_else(|| native_demand_error(error))))?;
    let scope = scope.map(DarwinStartupMeterScope::try_clone_for_io).transpose()?;
    let original_work = original.native_materialization_work().allocation_work();
    let task = acyclic_native_runtime::run_blocking_io_measured(
        move || path.unmount(operation, scope, original_work), task_meter,
    ).map_err(|error| NativeMountError::from(original.meter().take_native_io_failure()
        .unwrap_or_else(|| native_demand_error(error))))?;
    // Custody is installed before even the first readiness probe can be denied.
    state.direct = Some(DirectUnmountTask {
        task: Some(task), submission_failure: None, _meter: Arc::clone(meter),
        original, _custody: meter.custody.clone(),
    });
    }
    let deadline = Instant::now() + DIRECT_UNMOUNT_TIMEOUT;
    loop {
        match poll_direct(state.direct.as_mut().expect("accepted task retained"), meter, cleanup)? {
            std::task::Poll::Ready(result) => {
                let task = state.direct.take().expect("observed accepted task");
                return result.and_then(std::convert::identity).map(|_| ())
                    .map_err(|error| NativeMountError::from(task.original.meter().take_native_io_failure()
                        .unwrap_or_else(|| native_demand_error(error))));
            }
            std::task::Poll::Pending => {}
        }
        detach_check(meter, cleanup)?;
        if Instant::now() >= deadline { return Ok(()); }
        detach_started_read(meter, cleanup)?;
        std::thread::sleep(STOP_POLL_INTERVAL);
    }
}

struct PreparedDiskutilProcess {
    frame: Option<acyclic_native_runtime::PreparedClosedProcess>,
    credit: Arc<crate::materializer::NativeOwnedCleanupCredit>,
    original_work: crate::NativeMaterializationWork,
}

impl PreparedDiskutilProcess {
    fn admitted_work() -> Result<crate::WorkCounters, NativeMountError> {
        let observations = closed_control_visits(DISKUTIL_UNMOUNT_TIMEOUT, STOP_POLL_INTERVAL)?;
        let termination = closed_control_visits(
            acyclic_native_runtime::ProcessTree::TERMINATION_TIMEOUT,
            acyclic_native_runtime::ProcessTree::TERMINATION_POLL_INTERVAL,
        )?;
        let arguments = 3usize;
        let pointer_slots = arguments.checked_add(2).ok_or(crate::WorkError::Overflow)?;
        let pointer_bytes = pointer_slots.checked_mul(std::mem::size_of::<*mut libc::c_char>())
            .ok_or(crate::WorkError::Overflow)?;
        Ok(crate::WorkCounters {
            backend_read_operations: termination.checked_mul(2).and_then(|n|
                termination.checked_mul(3).and_then(|group| n.checked_add(group)))
                .and_then(|n| n.checked_add(observations)).and_then(|n| n.checked_add(1))
                .ok_or(crate::WorkError::Overflow)?,
            backend_write_operations: observations.checked_add(2 + 2 + [0, 1, 2].len() as u64)
                .and_then(|n| n.checked_add(2)).and_then(|n| n.checked_add(1))
                .ok_or(crate::WorkError::Overflow)?,
            items_examined: (pointer_slots - 1) as u64,
            bytes_copied: pointer_bytes as u64,
            ..crate::WorkCounters::UNCHARGED
        })
    }

    fn new(original: &crate::NativeMaterializationWork) -> Result<Self, NativeMountError> {
        let credit = original.reserve_work(Self::admitted_work()?)?;
        let credit = crate::materializer::NativeOwnedCleanupCredit::from_reserved(credit)?;
        let original_work = original.allocation_work();
        let frame = acyclic_native_runtime::ProcessTree::prepare_closed_process_measured(
            3, original_work.runtime_meter_owner(),
        ).map_err(|error| NativeMountError::from(original.runtime_error(error)))?;
        Ok(Self { frame: Some(frame), credit, original_work })
    }
}

struct DiskutilProcess {
    tree: Option<acyclic_native_runtime::ProcessTree>,
    deadline: Instant,
    failed_spawn: bool,
    _scope: Option<DarwinStartupMeterScope>,
    owned_credit: Option<Arc<crate::materializer::NativeOwnedCleanupCredit>>,
    _original_work: Option<crate::NativeMaterializationWork>,
}

fn spawn_diskutil(
    destination: &Path, state: &mut UnmountState, meter: &Arc<DetachReadMeter>,
    resources: Option<&Arc<DriverSessionResources>>, scope: Option<&DarwinStartupMeterScope>,
) -> Result<(), NativeMountError> {
    if let Some(resources) = resources {
        let prepared = state.prepared_diskutil.as_mut().ok_or(NativeMountError::ControlPending)?;
        if prepared.frame.is_none() { return Err(NativeMountError::ControlPending); }
        let path = resources.startup.as_ref().and_then(|startup| startup.destination.as_ref())
            .ok_or(NativeMountError::ControlPending)?;
        state.diskutil = Some(DiskutilProcess {
            tree: None, deadline: Instant::now() + DISKUTIL_UNMOUNT_TIMEOUT, failed_spawn: false,
            _scope: None, owned_credit: Some(Arc::clone(&prepared.credit)),
            _original_work: Some(prepared.original_work.clone()),
        });
        let native = prepared.credit.runtime_meter_owner();
        let result = acyclic_native_runtime::ProcessTree::spawn_closed_prepared(
            c"/usr/sbin/diskutil", &[c"unmount", c"force", path.as_c_str()], &mut prepared.frame,
            native.as_ref(), &mut state.diskutil.as_mut().expect("spawn custody installed").tree,
        ).map_err(|error| NativeMountError::from(prepared.credit.original_work().runtime_error(error)));
        if state.diskutil.as_ref().is_some_and(|child| child.tree.is_none()) {
            state.diskutil.take();
        } else {
            drop(state.prepared_diskutil.take());
        }
        if let Some(child) = &mut state.diskutil { child.failed_spawn = result.is_err(); }
        return result;
    }
    state.diskutil = Some(DiskutilProcess {
        tree: None, deadline: Instant::now() + DISKUTIL_UNMOUNT_TIMEOUT, failed_spawn: false,
        _scope: scope.map(DarwinStartupMeterScope::try_clone_for_io).transpose()?,
        owned_credit: None, _original_work: None,
    });
    let result = meter.with_native(|mut work| {
        crate::native_host::unix_name_with_work(destination.as_os_str(), &mut work, |path, _work| {
            meter.with_meter(|native| acyclic_native_runtime::ProcessTree::spawn_closed_measured(
                c"/usr/sbin/diskutil", &[c"unmount", c"force", path], native,
                &mut state.diskutil.as_mut().expect("spawn custody").tree,
            )).map_err(|error| crate::materializer::native_materialization_work_error(meter.native_error(error)))
        }).map_err(NativeMountError::from)
    });
    if state.diskutil.as_ref().is_some_and(|child| child.tree.is_none()) { state.diskutil.take(); }
    if let Some(child) = &mut state.diskutil { child.failed_spawn = result.is_err(); }
    result
}

impl DiskutilProcess {
    fn observe(&mut self, meter: &Arc<DetachReadMeter>) -> Result<Option<std::process::ExitStatus>, NativeMountError> {
        if let Some(credit) = &self.owned_credit {
            return self.tree.as_mut().expect("retained process tree")
                .try_wait_measured(credit.runtime_meter_owner().as_ref())
                .map_err(|error| NativeMountError::from(credit.original_work().runtime_error(error)));
        }
        meter.with_meter(|native| self.tree.as_mut().expect("retained process tree").try_wait_measured(native))
            .map_err(|error| NativeMountError::from(meter.native_error(error)))
    }

    fn terminate(&mut self, meter: &Arc<DetachReadMeter>) -> Result<(), NativeMountError> {
        if let Some(credit) = &self.owned_credit {
            return self.tree.as_mut().expect("retained process tree")
                .terminate_measured(credit.runtime_meter_owner().as_ref())
                .map_err(|error| NativeMountError::from(credit.original_work().runtime_error(error)));
        }
        meter.with_meter(|native| self.tree.as_mut().expect("retained process tree").terminate_measured(native))
            .map_err(|error| NativeMountError::from(meter.native_error(error)))
    }
}

fn bounded_unmount(
    destination: &Path, state: &mut UnmountState, meter: &Arc<DetachReadMeter>,
    resources: Option<&Arc<DriverSessionResources>>,
    scope: Option<&DarwinStartupMeterScope>,
    mut scratch: Option<&mut DarwinStopScratch>, cleanup: Option<&crate::materializer::NativeCleanupLoan<'_>>,
) -> Result<bool, NativeMountError> {
    let mounted = meter.with_native(|mut work| is_mounted(destination, &mut work, scratch.as_deref_mut(), cleanup))?;
    state.detached = !mounted;
    if mounted && state.start_aborted {
        return Err(NativeMountError::DestinationBusy);
    }
    if let Some(child) = &mut state.diskutil {
        if !mounted || child.failed_spawn {
            detach_check(meter, cleanup)?;
            child.terminate(meter)?;
            state.diskutil.take();
        }
    }
    if !mounted {
        drop(state.prepared_direct.take());
        drop(state.prepared_diskutil.take());
        if let Some(task) = &mut state.direct {
            if poll_direct(task, meter, cleanup)?.is_ready() { state.direct.take(); }
        }
        return Ok(false);
    }
    if state.diskutil.is_none() {
        match bounded_direct_unmount(destination, state, meter, resources, scope, cleanup) {
            Ok(()) | Err(NativeMountError::Io(_)) => {}
            Err(error) => return Err(error),
        }
        if !meter.with_native(|mut work| is_mounted(destination, &mut work, scratch.as_deref_mut(), cleanup))? {
            state.detached = true;
            drop(state.prepared_diskutil.take());
            return Ok(true);
        }
        detach_check(meter, cleanup)?;
        spawn_diskutil(destination, state, meter, resources, scope)?;
    }
    let (status, timed_out) = loop {
        let child = state.diskutil.as_mut().expect("owned diskutil process");
        let status = detach_observe(meter, cleanup, || child.observe(meter))??;
        let timed_out = Instant::now() >= child.deadline;
        if status.is_some() || timed_out {
            detach_check(meter, cleanup)?;
            child.terminate(meter)?;
            break (status, timed_out);
        }
        detach_started_read(meter, cleanup)?;
        std::thread::sleep(STOP_POLL_INTERVAL);
    };
    state.diskutil.take();
    if !meter.with_native(|mut work| is_mounted(destination, &mut work, scratch.as_deref_mut(), cleanup))? {
        state.detached = true;
        return Ok(true);
    }
    if timed_out {
        return Err(NativeMountError::DarwinTeardown(DarwinTeardownFailure::DiskutilTimedOut));
    }
    if let Some(status) = status.filter(|status| !status.success()) {
        return Err(NativeMountError::DarwinTeardown(DarwinTeardownFailure::DiskutilExit(status)));
    }
    let verify_deadline = Instant::now() + DISKUTIL_VISIBILITY_TIMEOUT;
    while meter.with_native(|mut work| is_mounted(destination, &mut work, scratch.as_deref_mut(), cleanup))? {
        if Instant::now() >= verify_deadline {
            return Err(NativeMountError::DarwinTeardown(DarwinTeardownFailure::MountStillVisible));
        }
        detach_started_read(meter, cleanup)?;
        std::thread::sleep(STOP_POLL_INTERVAL);
    }
    state.detached = true;
    Ok(true)
}

/// Retains native crash-recovery controls.
#[derive(Default)]
pub(super) struct DarwinRecoveryControl(UnmountState);

impl DarwinRecoveryControl {
    pub(super) fn controls_pending(&self) -> bool {
        self.0.direct.is_some() || self.0.diskutil.is_some()
    }

    pub(super) fn namespace_detach_confirmed(&self) -> bool { self.0.detached }

    /// Drains accepted controls without probing or changing the namespace.
    pub(super) fn retire_controls_with_work(
        &mut self, original: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let mut failure = None;
        if self.0.direct.is_some() {
            let deadline = Instant::now() + DIRECT_UNMOUNT_TIMEOUT;
            loop {
                original.charge(crate::WorkCounters {
                    items_examined: 1, ..crate::WorkCounters::UNCHARGED
                })?;
                let task = self.0.direct.as_mut().expect("accepted direct task retained");
                let completed = if let Some(error) = task.submission_failure.take() {
                    Some(Err(error))
                } else {
                    let accepted = task.task.as_mut().ok_or(NativeMountError::ControlPending)?;
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        std::pin::Pin::new(accepted)
                            .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                    })) {
                        Ok(std::task::Poll::Pending) => None,
                        Ok(std::task::Poll::Ready(result)) => Some(result),
                        Err(panic) => {
                            // Panic follows physical unwind of the accepted payload.
                            failure = Some(NativeMountError::DriverPanicked {
                                operation: "Darwin administrative direct detach",
                                panic: Mutex::new(panic),
                            });
                            drop(self.0.direct.take());
                            break;
                        }
                    }
                };
                if let Some(result) = completed {
                    let task = self.0.direct.take().expect("physically completed direct task");
                    if let Err(error) = result.and_then(std::convert::identity) {
                        failure = Some(NativeMountError::from(
                            task.original.meter().take_native_io_failure()
                                .unwrap_or_else(|| native_demand_error(error)),
                        ));
                    }
                    drop(task);
                    break;
                }
                if Instant::now() >= deadline { return Err(NativeMountError::ControlPending); }
                original.charge(crate::WorkCounters {
                    backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED
                })?;
                std::thread::sleep(STOP_POLL_INTERVAL);
            }
        }
        if let Some(child) = &mut self.0.diskutil {
            let result = child.tree.as_mut().ok_or(NativeMountError::ControlPending)?
                .terminate_measured(original.runtime_meter())
                .map_err(|error| NativeMountError::from(original.runtime_error(error)));
            if let Err(error) = result { return Err(failure.unwrap_or(error)); }
            drop(self.0.diskutil.take());
        }
        if self.controls_pending() { return Err(failure.unwrap_or(NativeMountError::ControlPending)); }
        match failure { Some(error) => Err(error), None => Ok(()) }
    }

    /// Performs crash recovery on the supplied meter.
    pub(super) fn recover_measured(
        &mut self, destination: &Path, work: &super::NativeMountMeterWork,
    ) -> Result<(), NativeMountError> {
        with_detach_meter(work, |meter| {
            bounded_unmount(destination, &mut self.0, meter, None, None, None, None)?;
            if let Some(task) = &mut self.0.direct {
                if poll_direct(task, meter, None)?.is_ready() { self.0.direct.take(); }
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
    // SAFETY: the bridge address stays owned by DriverSessionResources through loop exit.
    Ok(unsafe { &*(address as *const DarwinMountContext) })
}

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

    fn reserve(&mut self, additional: usize, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
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
    fn search(&self, key: &K, work: &DarwinCallbackWork<'_>) -> Result<Result<usize, usize>, i32> {
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
    fn get(&self, key: &K, work: &DarwinCallbackWork<'_>) -> Result<Option<&V>, i32> {
        Ok(self.search(key, work)?.ok().map(|index| &self.storage[index].1))
    }
    fn get_mut(&mut self, key: &K, work: &DarwinCallbackWork<'_>) -> Result<Option<&mut V>, i32> {
        let index = self.search(key, work)?.ok();
        Ok(index.map(|index| &mut self.storage[index].1))
    }
    fn contains_key(&self, key: &K, work: &DarwinCallbackWork<'_>) -> Result<bool, i32> {
        Ok(self.search(key, work)?.is_ok())
    }
    fn insert(&mut self, key: K, value: V, work: &DarwinCallbackWork<'_>) -> Result<Option<V>, i32> {
        match self.search(&key, work)? {
            Ok(index) => {
                let copied = crate::WorkCounters { bytes_copied: std::mem::size_of::<V>() as u64, ..crate::WorkCounters::UNCHARGED };
                work.copying(copied, || std::mem::replace(&mut self.storage[index].1, value)).map(Some)
            }
            Err(index) => {
                self.reserve(1, work)?;
                let rows = self.len().checked_sub(index).and_then(|rows| rows.checked_add(1)).ok_or(libc::EOVERFLOW)?;
                let copied = crate::WorkCounters { bytes_copied: rows.checked_mul(std::mem::size_of::<(K, V)>()).ok_or(libc::EOVERFLOW)? as u64, ..crate::WorkCounters::UNCHARGED };
                work.copying(copied, || self.storage.insert(index, (key, value)))?;
                Ok(None)
            }
        }
    }
    fn remove_entry(&mut self, key: &K, work: &DarwinCallbackWork<'_>) -> Result<Option<(K, V)>, i32> {
        let Ok(index) = self.search(key, work)? else { return Ok(None); };
        let rows = self.len().checked_sub(index + 1).ok_or(libc::EOVERFLOW)?;
        let copied = crate::WorkCounters { bytes_copied: rows.checked_mul(std::mem::size_of::<(K, V)>()).ok_or(libc::EOVERFLOW)? as u64, ..crate::WorkCounters::UNCHARGED };
        work.copying(copied, || self.storage.remove(index)).map(Some)
    }
    fn remove(&mut self, key: &K, work: &DarwinCallbackWork<'_>) -> Result<Option<V>, i32> {
        Ok(self.remove_entry(key, work)?.map(|(key, value)| { drop(key); value }))
    }

}

struct DarwinBytes {
    value: Option<OwnedBytes>,
    meter: Arc<NativeDemandReadMeter>,
    bytes: u64,
    intrinsic: bool,
}

impl std::ops::Deref for DarwinBytes {
    type Target = OwnedBytes;
    fn deref(&self) -> &OwnedBytes { self.value.as_ref().expect("native request scratch") }
}

impl Drop for DarwinBytes {
    fn drop(&mut self) {
        drop(self.value.take());
        if !self.intrinsic {
            self.meter.lock().release(self.bytes).expect("release physically dropped request scratch");
        }
    }
}

impl DarwinBytes {
    fn into_chunk(mut self) -> crate::kernel::OwnedSourceChunk {
        let bytes = self.value.take().expect("native request bytes");
        let logical_owned_bytes = std::mem::take(&mut self.bytes);
        if self.intrinsic {
            crate::kernel::OwnedSourceChunk::intrinsic(bytes, logical_owned_bytes)
        } else {
            crate::kernel::OwnedSourceChunk::claimed(bytes, logical_owned_bytes)
        }
    }
}

type DarwinFile = (Arc<dyn MountOpenFile>, Option<super::NativeMountAllocationCustody>);

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

impl DarwinOwned<Vec<u8>> {
    fn reserve_bytes(&mut self, required: usize, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
        if required <= self.capacity() { return Ok(()); }
        let capacity = self.capacity().checked_mul(2).unwrap_or(required).max(required).min(MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES);
        let copied = crate::WorkCounters { bytes_copied: self.len() as u64, ..crate::WorkCounters::UNCHARGED };
        work.admit(copied)?;
        let mut next = work.owned(capacity as u64, || {
            let mut value = Vec::new();
            value.try_reserve_exact(capacity)?;
            Ok(value)
        })?;
        let actual = next.capacity() as u64;
        next.allocation.as_mut().expect("owned transient capacity").reconcile_capacity(actual)
            .map_err(|error| work.native_error(error.into()))?;
        work.copying(copied, || next.extend_from_slice(self))?;
        drop(std::mem::replace(self, next));
        Ok(())
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
        let registered = super::NativeMountAllocationCustody::from_admitted_with_meter(bytes, self.native);
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

    fn source_chunk(&self, chunk: crate::kernel::OwnedSourceChunk) -> DarwinBytes {
        let (bytes, logical_owned_bytes, intrinsic) = chunk.into_owned_parts();
        DarwinBytes { value: Some(bytes), meter: Arc::clone(self.native.meter()), bytes: logical_owned_bytes, intrinsic }
    }

    fn copy_bytes(&self, bytes: &[u8]) -> Result<DarwinBytes, i32> {
        if bytes.is_empty() {
            self.check()?;
            return Ok(DarwinBytes { value: Some(OwnedBytes::new()), meter: Arc::clone(self.native.meter()), bytes: 0, intrinsic: false });
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
        let value = OwnedBytes::from_vec_with_guard(
            value, allocation, materialization.runtime_meter_owner(),
        ).map_err(|failure| match failure.into_error() {
            acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                self.native_error(materialization.runtime_error(error).into())
            }
            acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized
            => {
                unreachable!("guarded vector conversion only fails original admission")
            }
        })?;
        Ok(DarwinBytes { value: Some(value), meter: Arc::clone(self.native.meter()), bytes: retained, intrinsic: true })
    }

    fn retained_file(&self, file: (Arc<dyn MountOpenFile>, u64)) -> Result<DarwinFile, i32> {
        let mut owned = self.admitted(file.0, file.1)?;
        Ok((owned.value.take().expect("retained source Arc"), owned.custody.take()))
    }

    fn shared<T>(&self, build: impl FnOnce() -> T) -> Result<Arc<DarwinOwned<T>>, i32> {
        Ok(Arc::new(self.owned(std::mem::size_of::<DarwinOwned<T>>() as u64, || Ok(build()))?))
    }

    fn native_name(&self, name: &[u8], slash: bool) -> Result<DarwinOwned<CString>, i32> {
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
        // SAFETY: the scan rejected interior NULs; one terminator was appended.
        Ok(value.map(|value| unsafe { CString::from_vec_with_nul_unchecked(value) }))
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
        MountSourceError::WorkspacePrefixFailure { source, .. } => super::native_workspace_errno(source),
        MountSourceError::NativeCallbackDenied { .. } => libc::EACCES,
        MountSourceError::NativeWorkspaceCause(error) => super::native_workspace_errno(&error.source),
        MountSourceError::NativeText { kind, .. } | MountSourceError::NativeStaticText { kind, .. } =>
            super::native_source_text_errno(*kind),
        _ => libc::EIO,
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
                MountSourceError::WorkspacePrefixFailure { source, accounting } =>
                    super::native_workspace_failure_is_fatal(source)
                        || super::native_workspace_failure_is_fatal(accounting),
                MountSourceError::NativeCallbackDenied { .. } => false,
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
    fn run<T>(
        &self, active: Result<Option<super::NativeMountMeterWork>, NativeMountError>,
        operation: impl FnOnce(&super::NativeMountMeterWork) -> Result<T, NativeMountError>,
    ) -> Result<T, NativeMountError> {
        let had_original = matches!(active, Ok(Some(_)));
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
                    if let Some(error) = overflow.filter(|_| !darwin_handler_is_fatal(&failure.cause)) {
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
        let failure = match result { Ok(value) => return Ok(value), Err(failure) => failure };
        if !darwin_handler_is_fatal(&failure.cause) { return Err(failure.cause); }
        self.handler_failed.store(true, Ordering::Release);
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
        if had_original {
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
    fn run_request<T>(&self, authored_write: bool, operation: impl FnOnce(&DarwinCallbackWork<'_>) -> Result<T, i32>) -> Result<T, i32> {
        let active = self.callback_meter();
        if let Ok(meter) = &active {
            if meter.is_some() {
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
        }
        let status = std::cell::Cell::new(active.as_ref().err().map(darwin_handler_errno));
        self.run(active, |native| {
            let work = DarwinCallbackWork::new(native);
            let result = (|| {
                work.check()?;
                if authored_write { self.admit_write(&work)?; }
                let value = operation(&work)?;
                if authored_write { self.admit_write(&work)?; }
                Ok(value)
            })();
            if let Err(error) = &result { status.set(Some(*error)); }
            work.finish(result)
        }).map_err(|error| status.get().unwrap_or_else(|| darwin_handler_errno(&error)))
    }
}

fn ffi<T: From<i32>>(
    address: usize, span: &tracing::span::EnteredSpan, authored_write: bool,
    operation: impl FnOnce(&'static DarwinMountContext, &DarwinCallbackWork<'_>) -> Result<T, i32>,
) -> T {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let context = context(address)?;
        let _callback = context.callbacks.enter();
        let _origin = context.origin.enter();
        context.run_request(authored_write, |work| operation(context, work))
    })).unwrap_or(Err(libc::EIO)).unwrap_or_else(|error| {
        span.record("errno", error);
        T::from(-error)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_getattr(
    address: usize,
    path: *const c_char,
    handle: u64,
    result: *mut NativeStat,
) -> c_int {
    let _span = callback_span!(TRACE, getattr, fh = handle);
    ffi(address, &_span, false, |context, work| {
        let attributes = context.attributes(&*mount_path(path, work)?, handle, work)?;
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
    ffi(address, &_span, flags & libc::O_ACCMODE != libc::O_RDONLY, |context, work| {
        let opened = context.open(&*mount_path(path, work)?, 0, work)?;
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
    ffi(address, &_span, true, |context, work| {
        let path = mount_path(path, work)?;
        let lookup = context.mutate(|| {
            context.source.create_file_with_meter(&path, create_metadata(mode, IFREG, uid, gid), work.native)
        }, work)?;
        context.remember_current_lookup(&path, Some(lookup), work)?;
        context.namespace_changed();
        let opened = context.open(&path, context.ledger.latest(), work)?;
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
    ffi(address, &_span, false, |context, work| {
        if handle == 0 {
            return Ok(0);
        }
        context
            .files
            .write()
            .map_err(|_| libc::EIO)?
            .remove(&handle, work)?
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
    ffi(address, &_span, false, |context, work| {
        let requested_length = bounded_length(length)?;
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let chunk = context.file_target(&*mount_path(path, work)?, handle, work)?.0.0
            .read_up_to_with_meter(offset, requested_length, work.native)
            .map_err(|error| work.source_error(error))?;
        let bytes = work.source_chunk(chunk);
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
    ffi(address, &_span, true, |context, work| {
        let length = bounded_length(length)?;
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        // SAFETY: the bridge forwards FUSE's `length`-byte write buffer, readable for
        // the callback; `bounded_length` capped that length.
        let bytes = unsafe { std::slice::from_raw_parts(buffer.cast::<u8>(), length as usize) };
        let copied = work.copy_bytes(bytes)?;
        context.mutate_file(&*mount_path(path, work)?, handle, |file| {
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
    ffi(address, &_span, true, |context, work| {
        let length = u64::try_from(length).map_err(|_| libc::EINVAL)?;
        context.mutate_file(&*mount_path(path, work)?, handle, |file| file.resize_with_meter(length, work.native), work)?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_flush(address: usize, handle: u64) -> c_int {
    let _span = callback_span!(DEBUG, flush, fh = handle);
    ffi(address, &_span, true, |context, work| {
        context.flush_on_close(handle, work)?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_fsync(address: usize) -> c_int {
    let _span = callback_span!(DEBUG, fsync);
    ffi(address, &_span, true, |context, work| {
        context.sync(work)?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_durable_writes(address: usize) -> c_int {
    let _span = callback_span!(TRACE, durable_writes);
    ffi(address, &_span, false, |context, _work| {
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
    ffi(address, &_span, false, |context, work| {
        let path = mount_path(path, work)?;
        let binding_epoch = context.source.binding_epoch_with_meter(work.native).map_err(|error| work.source_error(error))?;
        let epochs = context.cache_epochs(work)?;

        let _binding_lease = context.source.acquire_binding_lease_with_meter(binding_epoch, work.native)
            .map_err(|error| work.source_error(error))?;
        if context.lookup(&path, work)?.node.kind != MountNodeKind::Directory {
            return Err(libc::ENOTDIR);
        }
        let (path, path_custody) = path.into_parts();
        let mut directory = DirectoryHandle::new(path, binding_epoch, epochs);
        directory.path_custody = Some(path_custody);
        directory.keep_current(context, work)?;
        let allocated = context.allocate_handle()?;
        let directory = work.shared(|| Mutex::new(directory))?;
        let mut directories = context.directories.lock().map_err(|_| libc::EIO)?;
        directories.insert(allocated, directory, work)?;
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
    ffi(address, &_span, false, |context, work| {
        let directory = context
            .directories
            .lock()
            .map_err(|_| libc::EIO)?
            .get(&handle, work)?
            .cloned()
            .ok_or(libc::ESTALE)?;
        let mut directory = directory.lock().map_err(|_| libc::EIO)?;
        let _binding_lease = context.source.acquire_binding_lease_with_meter(directory.binding_epoch, work.native)
            .map_err(|error| work.source_error(error))?;
        directory.keep_current(context, work)?;
        if offset < 0 {
            return Err(libc::EINVAL);
        }
        if !directory.can_reuse_pages() {
            directory.rewind();
            context
                .directory_checkpoints
                .lock()
                .map_err(|_| libc::EIO)?
                .remove(&directory.path, work)?;
        } else if offset > 0 && directory.emitted == 0 {
            let checkpoints = context
                .directory_checkpoints
                .lock()
                .map_err(|_| libc::EIO)?;
            if let Some(checkpoint) = checkpoints.get(&directory.path, work)?
                && checkpoint.revision == context.namespace_revision.load(Ordering::Acquire)
                && checkpoint.handle.emitted == offset
                && checkpoint.handle.binding_epoch == directory.binding_epoch
                && checkpoint.handle.is_current(context, work)?
            {
                *directory = checkpoint.handle.clone(work)?;
            }
        }
        if offset < directory.emitted {
            directory.rewind();
        }
        advance_directory_to_offset(context, &mut directory, offset, work)?;
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
                finish_directory_page(context, &directory, true, work)?;
                return Ok(0);
            }
            directory.emitted = next;
        }
        loop {
            ensure_directory_page(context, &mut directory, work)?;
            let Some(entry) = directory.entries.front() else {
                finish_directory_page(context, &directory, false, work)?;
                context
                    .directory_checkpoints
                    .lock()
                    .map_err(|_| libc::EIO)?
                    .remove(&directory.path, work)?;
                return Ok(0);
            };
            let (name, attributes) = directory_entry(context, &directory, entry, work)?;
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
                finish_directory_page(context, &directory, true, work)?;
                return Ok(0);
            }
            directory.pop_entry();
            directory.emitted = next;
        }
    })
}

fn directory_entry(
    context: &DarwinMountContext,
    directory: &DirectoryHandle,
    entry: &MountDirectoryEntry, work: &DarwinCallbackWork<'_>) -> Result<(DarwinOwned<CString>, NativeStat), i32> {
    work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
    let name = work.native_name(&entry.name, false)?;
    let path = work.path_with_child(&directory.path, Some(&entry.name))?;
    let attributes = context.attributes_from_lookup(
        &path,
        MountLookup {
            node: entry.node,
            metadata: entry.metadata,
        },
        directory.epochs.map(|epochs| epochs.view), work)?;
    Ok((name, attributes))
}

fn finish_directory_page(
    context: &DarwinMountContext,
    directory: &DirectoryHandle,
    checkpoint: bool, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
    if directory.emitted > 2 && directory.can_reuse_pages() && !directory.is_current(context, work)? {
        return Err(libc::ESTALE);
    }
    if checkpoint {
        checkpoint_directory(context, directory, work)?;
    }
    Ok(())
}

fn advance_directory_to_offset(
    context: &DarwinMountContext,
    directory: &mut DirectoryHandle,
    offset: i64, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
    while directory.emitted < offset {
        if directory.emitted < 2 {
            directory.emitted += 1;
            continue;
        }
        ensure_directory_page(context, directory, work)?;
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

fn ensure_directory_page(
    context: &DarwinMountContext,
    directory: &mut DirectoryHandle, work: &DarwinCallbackWork<'_>) -> Result<(), i32> {
    directory.keep_current(context, work)?;
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
        directory.keep_current(context, work)?;
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

fn checkpoint_directory(
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
        checkpoints.remove(&directory.path, work)?;
        return Ok(());
    }
    if checkpoints.len() >= MAXIMUM_DIRECTORY_CHECKPOINTS
        && !checkpoints.contains_key(&directory.path, work)?
    {
        for (path, checkpoint) in checkpoints.drain() { drop(path); drop(checkpoint); }
    }
    let path = work.path_with_child(&directory.path, None)?;
    let handle = directory.clone(work)?;
    let (path, key_custody) = path.into_parts();
    if let Some((key, value)) = checkpoints.remove_entry(&path, work)? { drop(key); drop(value); }
    checkpoints.insert(path, DirectoryCheckpoint { revision, handle, key_custody }, work)?;
    Ok(())
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_releasedir(address: usize, handle: u64) -> c_int {
    let _span = callback_span!(TRACE, releasedir, fh = handle);
    ffi(address, &_span, false, |context, work| {
        context
            .directories
            .lock()
            .map_err(|_| libc::EIO)?
            .remove(&handle, work)?
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
    ffi(address, &_span, true, |context, work| {
        let path = mount_path(path, work)?;
        context.mutate(|| {
            context.source.create_directory_with_meter(&path, create_metadata(mode, IFDIR, uid, gid), work.native)
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
    ffi(address, &_span, true, |context, work| {
        let path = mount_path(path, work)?;
        let lookup = context.lookup(&path, work)?;
        if (directory != 0) != (lookup.node.kind == MountNodeKind::Directory) {
            return Err(if directory == 0 {
                libc::EISDIR
            } else {
                libc::ENOTDIR
            });
        }
        context.unlink(&path, lookup, || {
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
    ffi(address, &_span, true, |context, work| {
        if flags & !RENAME_NOREPLACE != 0 {
            return Err(libc::EOPNOTSUPP);
        }
        let (source, destination) = (mount_path(source, work)?, mount_path(destination, work)?);
        let replace = flags & RENAME_NOREPLACE == 0;
        let rename = || context.source.rename_with_meter(&source, &destination, replace, work.native);
        // Replacing the destination unlinks it.
        match replace.then(|| context.lookup(&destination, work)) {
            Some(Ok(replaced)) => context.unlink(&destination, replaced, rename, work)?,
            None | Some(Err(libc::ENOENT)) => context.mutate(rename, work)?,
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
    ffi(address, &_span, true, |context, work| {
        let (source, destination) = (mount_path(source, work)?, mount_path(destination, work)?);
        context.mutate(|| context.source.hard_link_with_meter(&source, &destination, work.native), work)?;
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
    ffi(address, &_span, true, |context, work| {
        // SAFETY: the bridge forwards FUSE's NUL-terminated link target, valid for
        // the callback.
        let target = c_bytes(target, work)?;
        let target = work.copy_bytes(target)?;
        let destination = mount_path(destination, work)?;
        context.mutate(|| {
            context.source.create_symbolic_link_with_meter(
                &destination,
                target.into_chunk(),
                create_metadata(0o777, IFLNK, uid, gid), work.native)
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
    ffi(address, &_span, false, |context, work| {
        if length == 0 {
            return Err(libc::ERANGE);
        }
        let chunk = context.source.read_link_with_meter(&*mount_path(path, work)?, work.native)
            .map_err(|error| work.source_error(error))?;
        let target = work.source_chunk(chunk);
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
    ffi(address, &_span, true, |context, work| {
        let kind = match mode & IFMT {
            IFIFO => MountNodeKind::Fifo,
            IFSOCK => MountNodeKind::Socket,
            IFCHR => MountNodeKind::CharacterDevice,
            IFBLK => MountNodeKind::BlockDevice,
            _ => return Err(libc::EOPNOTSUPP),
        };
        let device = matches!(
            kind,
            MountNodeKind::CharacterDevice | MountNodeKind::BlockDevice
        )
        .then(|| super::device::split_device(device));
        let path = mount_path(path, work)?;
        context.mutate(|| {
            context.source.create_special_with_meter(
                &path,
                kind,
                device,
                create_metadata(mode, mode & IFMT, uid, gid), work.native)
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
    ffi(address, &_span, true, |context, work| {
        context.mutate_metadata(&*mount_path(path, work)?, handle, |metadata| {
            let kind = metadata_or(metadata.posix_mode, 0) & IFMT;
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
    ffi(address, &_span, true, |context, work| {
        context.mutate_metadata(&*mount_path(path, work)?, handle, |metadata| {
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
    ffi(address, &_span, true, |context, work| {
        // SAFETY: the bridge passes null (rejected here) or a `NativeTimes` valid for
        // the callback.
        let times = unsafe { times.as_ref() }.ok_or(libc::EINVAL)?;
        context.mutate_metadata(&*mount_path(path, work)?, handle, |metadata| {
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
    ffi(address, &_span, false, |context, work| {
        let path = mount_path(path, work)?;
        if !context.may_have_named_attributes(&path, work)? {
            return Err(libc::ENOATTR);
        }
        let chunk = context.source.read_attribute_with_meter(&path, c_bytes(name, work)?, work.native)
            .map_err(|error| work.source_error(error))?
            .ok_or(libc::ENOATTR)?;
        let bytes = work.source_chunk(chunk);
        copy_variable_result(&bytes, value, length, work)
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
    ffi(address, &_span, true, |context, work| {
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
        let (path, name) = (mount_path(path, work)?, c_bytes(name, work)?);
        let copied = work.copy_bytes(bytes)?;
        context.mutate(|| {
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
    ffi(address, &_span, false, |context, work| {
        let path = mount_path(path, work)?;
        if !context.may_have_named_attributes(&path, work)? {
            return copy_variable_result(&[], list, length, work);
        }
        let mut cursor: Option<(Vec<u8>, crate::NativeAllocation)> = None;
        let mut encoded = DarwinOwned::empty(Vec::new());
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
                encoded.reserve_bytes(required, work)?;
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
        copy_variable_result(&encoded, list, length, work)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_removexattr(
    address: usize,
    path: *const c_char,
    name: *const c_char,
) -> c_int {
    let _span = callback_span!(TRACE, removexattr);
    ffi(address, &_span, true, |context, work| {
        let (path, name) = (mount_path(path, work)?, c_bytes(name, work)?);
        context.mutate(|| context.source.remove_attribute_with_meter(&path, name, work.native), work)?;
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
    ffi(address, &_span, false, |context, work| {
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let target = match whence {
            libc::SEEK_DATA => MountSeekTarget::Data,
            libc::SEEK_HOLE => MountSeekTarget::Hole,
            _ => return Err(libc::EINVAL),
        };
        let result = context.file_target(&*mount_path(path, work)?, handle, work)?.0.0
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
    ffi(address, &_span, true, |context, work| {
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
        context.mutate_file(&*mount_path(path, work)?, handle, |file| {
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
    ffi(address, &_span, true, |context, work| {
        if flags != 0 {
            return Err(libc::EINVAL);
        }
        let source_offset = u64::try_from(source_offset).map_err(|_| libc::EINVAL)?;
        let destination_offset = u64::try_from(destination_offset).map_err(|_| libc::EINVAL)?;
        let length = u64::try_from(length).map_err(|_| libc::EOVERFLOW)?;
        let source = context.file_target(&*mount_path(source_path, work)?, source_handle, work)?.0;
        let length = context.mutate_file(
            &*mount_path(destination_path, work)?,
            destination_handle,
            |destination| {
                let source_lookup = source.0.lookup_with_meter(work.native)?;
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

fn mount_path(path: *const c_char, work: &DarwinCallbackWork<'_>) -> Result<DarwinOwned<MountPath>, i32> {
    let bytes = c_bytes(path, work)?;
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

fn c_bytes<'a>(value: *const c_char, work: &DarwinCallbackWork<'_>) -> Result<&'a [u8], i32> {
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

fn bounded_length(length: usize) -> Result<u32, i32> {
    if length > MAXIMUM_CALLBACK_BYTES {
        return Err(libc::E2BIG);
    }
    u32::try_from(length).map_err(|_| libc::EOVERFLOW)
}

fn copy_variable_result(bytes: &[u8], output: *mut c_char, length: usize, work: &DarwinCallbackWork<'_>) -> Result<c_int, i32> {
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

#[cfg(test)]
mod tests {

    #[test]
    fn filtered_callback_error_does_not_overwrite_parent_errno() {
        crate::obs::assert_filtered_callback_isolation(|span| {
            let status = super::ffi(0, span, false, |_, _| Ok(0));
            assert_eq!(status, -libc::ESTALE);
            -status
        });
    }
    use super::*;
    use bytes::Bytes;
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
    fn interrupted_callback_barrier_retains_slot_and_retry_covers_new_callbacks() -> TestResult {
        let gate = CallbackGate::new();
        let before = gate.enter();
        let cancellation = crate::CancellationToken::new();
        let denied = crate::NativeMaterializationWork::new(crate::WorkBudget { items_examined: 1,
            backend_read_operations: 0, ..crate::WorkBudget::UNBOUNDED }, &cancellation)
            .map_err(|(cause, _)| cause)?;
        assert!(matches!(gate.drain(&denied.native_mount_work()), Err(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(_)))));
        let after = gate.enter();
        drop(before);
        let retry = crate::NativeMaterializationWork::new(crate::WorkBudget { items_examined: 1,
            backend_read_operations: 0, ..crate::WorkBudget::UNBOUNDED }, &cancellation)
            .map_err(|(cause, _)| cause)?;
        assert!(matches!(gate.drain(&retry.native_mount_work()), Err(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(_)))));
        drop(after);
        let exact = crate::NativeMaterializationWork::new(crate::WorkBudget { items_examined: 2,
            backend_read_operations: 0, ..crate::WorkBudget::UNBOUNDED }, &cancellation)
            .map_err(|(cause, _)| cause)?;
        gate.drain(&exact.native_mount_work())?;
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
        let result = context.revalidate(&original.native_mount_work());
        assert!(matches!(result, Err(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(_)))));
        assert_eq!(original.receipt().backend_read_operations, 0, "denied clock was not queried");
        assert_eq!(context.around.settled.load(Ordering::Acquire), 0);
        assert_eq!(context.around.made.load(Ordering::Acquire), 1);
        assert!(context.callbacks.draining.lock().unwrap_or_else(PoisonError::into_inner).is_none());
        let cancelled = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let cancelled_prefix = cancelled.receipt();
        cancellation.cancel();
        assert!(matches!(context.revalidate(&cancelled.native_mount_work()),
            Err(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Cancelled))));
        assert_eq!(cancelled.receipt(), cancelled_prefix);
        assert_eq!(context.around.settled.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[test]
    #[ignore = "requires the native macOS mount service"]
    fn existing_preborn_loop_retains_cleanup_when_actual_sdk_birth_is_denied() -> TestResult {
        acyclic_native_runtime::initialize_native_io_services()?;
        let (source, _) = checkout_context(MountPublication::Manual)?;
        let temporary = tempfile::tempdir()?;
        let request = NativeMountRequest { mount_id: crate::MountId::new(),
            volume_id: source.volume_id()?, destination: temporary.path().to_path_buf(), writable: true };
        let cancellation = crate::CancellationToken::new();
        let birth_writes = stop_mount_probe_maximum()?
            .checked_add(PreparedDiskutilProcess::admitted_work()?.backend_write_operations)
            .and_then(|n| n.checked_add(4)).ok_or(crate::WorkError::Overflow)?;
        let prefix = crate::NativeMaterializationWork::new(crate::WorkBudget { backend_write_operations: birth_writes,
            ..crate::WorkBudget::UNBOUNDED }, &cancellation).map_err(|(cause, _)| cause)?;
        let mut driver = None;
        let mut retirement = None;
        let failure = DarwinMountSession::start_into(&request, source, &prefix.native_mount_work(),
            &mut driver, &mut retirement, |_| Ok(())).expect_err("SDK construction is denied");
        assert!(matches!(failure.error, NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(_))));
        let mut session = driver.ok_or("allocated SDK resource was not retained")?;
        assert!(session.thread.is_some());
        assert_eq!(session.resources()?.driver_session.load(Ordering::Acquire), 0);
        assert!(!session.teardown_complete);
        let denied = crate::CancellationToken::new();
        denied.cancel();
        assert!(session.stop_with_cleanup_wait(&denied).is_err());
        assert!(session.resources.is_some(), "denied wait does not relinquish resource custody");
        cancellation.cancel();
        let cleanup = crate::CancellationToken::new();
        assert!(matches!(session.stop_with_cleanup_wait(&cleanup),
            Err(NativeMountError::DarwinTeardown(
                DarwinTeardownFailure::LoopExitedBeforeDetach(_) | DarwinTeardownFailure::LoopExitedAfterDetach(_)))));
        assert!(session.resources.is_none());
        assert!(prefix.receipt().backend_write_operations >= 2,
            "only the real existing thread and actual probe closes are added, never null SDK interrupt/free");
        assert!(session.namespace_detach_confirmed());
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
        Ok(())
    }

    #[test]
    fn administrative_direct_retirement_preserves_payload_until_actual_completion() -> TestResult {
        acyclic_native_runtime::initialize_native_io_services()?;
        let _inline = acyclic_native_runtime::InlineBlocking::enter();
        let temporary = tempfile::tempdir()?;
        let cancellation = crate::CancellationToken::new();
        let creator = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, &cancellation,
        ).map_err(|(cause, _)| cause)?;
        let native = creator.native_mount_work();
        let meter = new_detach_meter(&native)?;
        let path = DirectUnmountPath::Recovery(DetachPath::new(temporary.path(), &creator)?);
        let task_meter = creator.runtime_meter_owner();
        let operation = acyclic_native_runtime::OwnedNativeOperationReservation::new(
            Arc::clone(&task_meter), acyclic_native_runtime::NativeOperationKind::Write, 1,
        ).map_err(|error| creator.runtime_error(error))?;
        let original_work = creator.allocation_work();
        let accepted = acyclic_native_runtime::prepare_blocking_io_measured(
            move || path.unmount(operation, None, original_work), task_meter,
        ).map_err(|error| creator.runtime_error(error))?;
        let mut control = DarwinRecoveryControl::default();
        control.0.direct = Some(DirectUnmountTask {
            task: Some(accepted), submission_failure: None, _meter: Arc::clone(&meter),
            original: native, _custody: meter.custody.clone(),
        });
        meter.seal();
        let retained_bytes = creator.live_allocation_bytes();
        let denied = crate::NativeMaterializationWork::new(crate::WorkBudget {
            items_examined: 0, ..crate::WorkBudget::UNBOUNDED
        }, &cancellation).map_err(|(cause, _)| cause)?;
        let denied_receipt = denied.receipt();
        assert!(matches!(control.retire_controls_with_work(&denied),
            Err(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(_)))));
        assert_eq!(denied.receipt(), denied_receipt);
        assert!(control.controls_pending());
        assert_eq!(creator.live_allocation_bytes(), retained_bytes, "the real task still owns its original path");
        assert_eq!(creator.receipt().backend_write_operations, 0, "denied observation never runs the syscall");

        let retry = crate::NativeMaterializationWork::new(crate::WorkBudget {
            items_examined: 1, backend_read_operations: 0, backend_write_operations: 0,
            allocation_operations: 1,
            peak_allocation_bytes: crate::NativeMaterializationWork::accounting_state_bytes(),
            ..crate::WorkBudget::UNBOUNDED
        }, &cancellation).map_err(|(cause, _)| cause)?;
        assert!(matches!(control.retire_controls_with_work(&retry), Err(NativeMountError::Io(_))),
            "the actual unmount of a plain directory retains its original native failure");
        assert!(!control.controls_pending());
        assert!(!control.namespace_detach_confirmed(), "completed native task is not absence evidence");
        assert!(creator.live_allocation_bytes() < retained_bytes, "physical path dropped before original AL");
        assert_eq!(retry.receipt().backend_write_operations, 0);
        Ok(())
    }

    fn checkout(
        publication: MountPublication,
    ) -> Result<(Arc<super::super::SharedCheckout<
        crate::facade::MemoryAuthorityBackend, crate::facade::MemoryObjectBackend,
    >>, crate::model::VolumeConfig), Box<dyn std::error::Error>> {
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
        Ok((Arc::new(super::super::SharedCheckout::with_publication(checkout, publication)), config))
    }

    fn checkout_context(
        publication: MountPublication,
    ) -> Result<(Arc<MemorySource>, DarwinMountContext), Box<dyn std::error::Error>> {
        let (shared, config) = checkout(publication)?;
        let source = Arc::new(MemorySource::new(shared, config)?);
        let root_id = source
            .lookup(&MountPath::root())?
            .ok_or("root absent")?
            .node
            .file_id;
        let metadata = std::fs::metadata(".")?;
        let context = DarwinMountContext::new(
            Arc::clone(&source) as Arc<dyn MountFilesystem>, true, metadata.uid(), metadata.gid(),
            Vec::from([(root_id, ROOT_INODE)]), crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )?;
        Ok((source, context))
    }

    fn os(error: i32) -> std::io::Error {
        std::io::Error::from_raw_os_error(error)
    }

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
        Ok(Arc::new(DarwinMountContext::new(
            Arc::clone(source) as Arc<dyn MountFilesystem>, true, metadata.uid(), metadata.gid(),
            Vec::from([(root_id, ROOT_INODE)]), budget, cancellation,
        )?))
    }

    #[derive(Debug, thiserror::Error)]
    #[error("installed callback owner has no authored write permission")]
    struct DarwinWriteAuthorityDenied;

    struct DarwinAuthoredGuard {
        original: crate::NativeMaterializationWork,
        writable: AtomicBool,
    }

    impl super::super::NativeMountCallbackGuard for DarwinAuthoredGuard {
        fn check<'a>(
            &'a self, work: &'a super::super::NativeMountMeterWork,
        ) -> Result<super::super::NativeMountCallbackFuture<'a>, NativeMountError> {
            if !self.original.shares_ledger(&work.native_materialization_work()) {
                return Err(NativeMountError::ControlPending);
            }
            super::super::NativeMountCallbackFuture::new(work, || async move {
                work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)?;
                Ok(())
            })
        }

        fn check_write<'a>(
            &'a self, work: &'a super::super::NativeMountMeterWork,
        ) -> Result<super::super::NativeMountCallbackFuture<'a>, NativeMountError> {
            if !self.original.shares_ledger(&work.native_materialization_work()) {
                return Err(NativeMountError::ControlPending);
            }
            if !self.writable.load(Ordering::Acquire) {
                return Err(super::super::NativeMountCallbackReservation::prepare(work)?
                    .into_error(DarwinWriteAuthorityDenied));
            }
            super::super::NativeMountCallbackFuture::new_guarded(work, move || {
                async move {
                    if self.writable.load(Ordering::Acquire) { Ok(()) }
                    else { Err(DarwinWriteAuthorityDenied) }
                }
            })
        }
    }

    fn authored_guard_context(
        writable: bool,
    ) -> Result<(Arc<MemorySource>, Arc<DarwinMountContext>, Arc<DarwinAuthoredGuard>, crate::NativeMaterializationWork),
        Box<dyn std::error::Error>> {
        let (shared, config) = checkout(MountPublication::Manual)?;
        let ordinary = Arc::new(MemorySource::new(Arc::clone(&shared), config)?);
        let root_id = ordinary.lookup(&MountPath::root())?.ok_or("root absent")?.node.file_id;
        let original = crate::NativeMaterializationWork::new(
            callback_policy(), &crate::CancellationToken::new(),
        ).map_err(|(cause, _)| cause)?;
        let guard = Arc::new(DarwinAuthoredGuard {
            original: original.clone(), writable: AtomicBool::new(writable),
        });
        let source = MemorySource::new_at_meter(
            shared, config, crate::kernel::NamespacePath::new(Vec::new(), config.limits)?,
            original.claim(0)?, &original.native_mount_work(), Some(&original),
        )?.with_native_callback_guard(guard.clone());
        let source = source.into_arc_meter(&original.native_mount_work())?;
        let context = Arc::new(DarwinMountContext::new(
            source, true, 0, 0, Vec::from([(root_id, ROOT_INODE)]),
            callback_policy(), &crate::CancellationToken::new(),
        )?);
        Ok((ordinary, context, guard, original))
    }

    #[test]
    fn raw_authored_write_denies_read_guard_before_payload_or_source_effect() -> TestResult {
        let (source, context, _guard, original) = authored_guard_context(false)?;
        let root = c"/";
        let mut attributes = std::mem::MaybeUninit::<NativeStat>::uninit();
        let address = Arc::as_ptr(&context) as usize;
        // SAFETY: real context and native output/path storage remain alive.
        assert_eq!(unsafe { acyclic_fs_darwin_mount_getattr(address, root.as_ptr(), 0, attributes.as_mut_ptr()) }, 0);
        let path = c"/denied-by-role";
        let mut handle = 0;
        let before = original.receipt();
        assert_eq!(unsafe { acyclic_fs_darwin_mount_create(address, path.as_ptr(), 0o644, 0, 0,
            libc::O_RDWR, &mut handle) }, -libc::EACCES);
        assert_eq!(handle, 0);
        let after = original.receipt();
        assert_eq!(after.items_examined, before.items_examined, "no authored path/provider frame is examined");
        assert_eq!(after.bytes_copied, before.bytes_copied, "no authored payload is copied");
        assert_eq!(after.backend_write_operations, before.backend_write_operations);
        assert!(source.lookup(&MountPath::root().child(b"denied-by-role".to_vec()))?.is_none());
        assert_eq!(context.ledger.latest(), 0);
        let failure = context.startup_failure.lock().map_err(|_| "poisoned original failure")?
            .take().ok_or("missing original typed guard denial")?;
        assert!(matches!(failure.cause, NativeMountError::CallbackDenied { .. }));
        assert_eq!(failure.work, after, "failure remains on the installed original work");
        Ok(())
    }

    #[test]
    fn authored_post_effect_revocation_retains_actual_mutation_and_typed_cause() -> TestResult {
        let (source, context, guard, original) = authored_guard_context(true)?;
        let path = MountPath::root().child(b"already-created".to_vec());
        let result = context.run_request(true, |work| {
            context.mutate(|| context.source.create_file_with_meter(
                &path, create_metadata(0o644, IFREG, 0, 0), work.native,
            ), work)?;
            guard.writable.store(false, Ordering::Release);
            Ok(())
        });
        assert_eq!(result, Err(libc::EACCES), "post-effect denial prevents successful SDK publication");
        assert!(source.lookup(&path)?.is_some(), "real partial effect is not rolled back or hidden");
        assert!(context.ledger.latest() > 0, "actual mutation remains covered by the original publication ledger");
        let failure = context.startup_failure.lock().map_err(|_| "poisoned original failure")?
            .take().ok_or("missing post-effect typed cause")?;
        assert!(matches!(failure.cause, NativeMountError::CallbackDenied { .. }));
        assert_eq!(failure.work, original.receipt());
        assert_eq!(context.handler_failure.lock().map_err(|_| "poisoned completed work")?
            .completed_future_request_work, crate::WorkCounters::UNCHARGED);
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
        let status = context.run_request::<()>(false, |work| {
            work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            panic!("actual native callback panic");
        });
        assert_eq!(status, Err(libc::EIO));
        assert!(context.handler_failed.load(Ordering::Acquire));
        let completed = context.handler_failure.lock().map_err(|_| "poisoned callback failure")?
            .completed_future_request_work;
        let called = std::cell::Cell::new(false);
        assert_eq!(context.run_request(false, |_| { called.set(true); Ok(()) }), Err(libc::EIO));
        assert!(!called.get());
        let work = crate::NativeMaterializationWork::new(crate::WorkBudget {
            backend_write_operations: 2, ..crate::WorkBudget::UNBOUNDED
        }, &cancellation).map_err(|(cause, _)| cause)?;
        let cleanup_credit = DriverSessionResources::reserve_cleanup_credit(&work)?;
        let resources = Arc::new(DriverSessionResources {
            source_context: Arc::into_raw(context) as usize,
            driver_session: AtomicUsize::new(0), startup: None,
            cleanup_credit: Mutex::new(cleanup_credit),
            native_control: Mutex::new(()),
            sdk_allocation: Mutex::new(None),
            interrupted: std::sync::atomic::AtomicBool::new(false),
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
            retired_callback_failure: Mutex::new(None),
            unmount: DarwinStopController::new(UnmountState::default(), &work)?,
            native_root_identity: None,
        };
        session.release_resources_with_cleanup_wait(&cancellation)?;
        assert!(session.resources.is_none());
        assert_eq!(session.completed_callback_work(), completed);
        let failure = session.take_callback_failure().ok_or("missing retired actual panic")?;
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
    fn actual_sdk_prebirth_interrupt_returns_without_creating_a_namespace() -> TestResult {
        let cancellation = crate::CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let (_, mut context) = checkout_context(MountPublication::Manual)?;
        context.native_allocation = Some(work.claim(std::mem::size_of::<DarwinMountContext>() as u64)?);
        let context = Arc::new(context);
        let temporary = tempfile::tempdir()?;
        let startup = DarwinStartupBuffers::new(
            temporary.path(), true, context.source.supports_posix_named_attributes(), &work.native_mount_work(),
        )?;
        let cleanup_credit = DriverSessionResources::reserve_cleanup_credit(&work)?;
        let allocation = work.claim(std::mem::size_of::<DriverSessionResources>() as u64)?;
        let (driver_session, sdk_allocation) = DriverSessionResources::new_sdk_session(&work)?;
        let resources = Arc::new(DriverSessionResources {
            source_context: Arc::into_raw(context) as usize,
            driver_session: AtomicUsize::new(driver_session), native_control: Mutex::new(()),
            sdk_allocation: Mutex::new(Some(sdk_allocation)), cleanup_credit: Mutex::new(cleanup_credit),
            interrupted: std::sync::atomic::AtomicBool::new(false), startup: Some(startup),
            _allocation: Some(allocation),
        });
        resources.interrupt_accepted_cleanup();
        let events = Arc::new(LoopEvents {
            _allocation: Some(work.claim(std::mem::size_of::<LoopEvents>() as u64)?),
            ..LoopEvents::default()
        });
        let startup = resources.startup.as_ref().expect("the actual original SDK arguments");
        let mut interrupted_before_namespace = 0;
        work.charge(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        })?;
        let status = unsafe {
            acyclic_fs_darwin_mount_run(
                driver_session as *mut c_void, startup.arguments.len() as c_int,
                startup.pointers.as_ptr().cast::<*const c_char>(),
                startup.destination.as_ref().expect("the actual original destination").as_ptr(),
                resources.source_context, report_mounted, Arc::as_ptr(&events).cast_mut().cast(),
                &mut interrupted_before_namespace,
            )
        };
        assert_eq!(status, 0);
        assert_eq!(interrupted_before_namespace, 1, "the real SDK gate consumed its persistent original stop");
        assert!(!events.current().mounted, "no mounted callback may acknowledge a namespace");
        assert!(!is_mounted(temporary.path(), &mut &work, None, None)?);
        cancellation.cancel();
        drop(resources);
        drop(events);
        assert_eq!(work.live_allocation_bytes(), 0, "actual SDK/context/argument data precede their paid guards");
        assert!(cancellation.is_cancelled());
        Ok(())
    }

    #[test]
    fn actual_sdk_controls_use_birth_credit_only_after_owner_quiescence_and_wait_admission() -> TestResult {
        let original_cancel = crate::CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(crate::WorkBudget {
            backend_read_operations: 1, backend_write_operations: 3, ..crate::WorkBudget::UNBOUNDED
        }, &original_cancel).map_err(|(cause, _)| cause)?;
        let cleanup_credit = DriverSessionResources::reserve_cleanup_credit(&work)?;
        assert_eq!(work.receipt().backend_write_operations, 0, "reservation is not a physical attempt");
        let (driver_session, sdk_allocation) = DriverSessionResources::new_sdk_session(&work)?;
        let mut resources = Arc::new(DriverSessionResources {
            source_context: 0, driver_session: AtomicUsize::new(driver_session),
            cleanup_credit: Mutex::new(cleanup_credit),
            native_control: Mutex::new(()),
            sdk_allocation: Mutex::new(Some(sdk_allocation)),
            interrupted: std::sync::atomic::AtomicBool::new(false),
            startup: None, _allocation: None,
        });
        let owner = Arc::clone(&resources);
        let (release, waiting) = std::sync::mpsc::channel::<()>();
        let thread = std::thread::spawn(move || {
            let _ = waiting.recv();
            drop(owner);
        });
        original_cancel.cancel();
        assert!(Arc::get_mut(&mut resources).is_none(), "a real outstanding owner prevents SDK free");
        let control_wait = crate::CancellationToken::new();
        resources.interrupt_with_cleanup_wait(&control_wait)?;
        resources.interrupt_with_cleanup_wait(&control_wait)?;
        release.send(())?;
        resources.start_join_with_cleanup_wait(&control_wait)?;
        thread.join().map_err(|_| "resource-owner fixture panicked")?;
        let stopped_wait = crate::CancellationToken::new();
        stopped_wait.cancel();
        let retained = Arc::get_mut(&mut resources).ok_or("owner did not quiesce")?;
        assert!(matches!(retained.free_with_cleanup_wait(&stopped_wait),
            Err(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Cancelled))));
        assert_eq!(retained.driver_session.load(Ordering::Acquire), driver_session, "wait refusal retains the actual SDK pointer");
        let retry_wait = crate::CancellationToken::new();
        retained.free_with_cleanup_wait(&retry_wait)?;
        assert_eq!(retained.driver_session.load(Ordering::Acquire), 0);
        assert!(work.cancellation().is_cancelled(), "cleanup cannot replace original cancellation");
        drop(resources);
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
        let active = DarwinStartupMeterScope::new(Arc::clone(&context), &original)?;
        assert!(matches!(DarwinStartupMeterScope::new(Arc::clone(&context), &other),
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
        context.run_request(true, |work| context.mutate(|| source.create_file(&path, FileMetadata::default()), work))
            .map_err(os)?;
        let created = context.run_request(false, |work| context.open(&path, context.ledger.latest(), work))
            .map_err(os)?;
        let before = epoch()?;
        context.run_request(true, |work| context.flush_on_close(created, work)).map_err(os)?;
        assert_ne!(epoch()?, before, "closing the creating handle publishes");

        let reader = context.run_request(false, |work| context.open(&path, 0, work)).map_err(os)?;
        let before = epoch()?;
        context.run_request(true, |work| context.flush_on_close(reader, work)).map_err(os)?;
        context.run_request(true, |work| context.sync(work)).map_err(os)?;
        assert_eq!(epoch()?, before, "a clean close or sync publishes nothing");

        context.run_request(true, |work| context.mutate_file(&path, reader, |file| {
                file.write_range(0, Bytes::from_static(b"through the handle"))
            }, work))
            .map_err(os)?;
        let before = epoch()?;
        context.run_request(true, |work| context.flush_on_close(reader, work)).map_err(os)?;
        assert_ne!(epoch()?, before, "a written handle's close publishes");
        let before = epoch()?;
        context.run_request(true, |work| context.flush_on_close(reader, work)).map_err(os)?;
        context.run_request(true, |work| context.sync(work)).map_err(os)?;
        assert_eq!(epoch()?, before, "published writes are not published again");

        context.run_request(true, |work| context.mutate_file(&path, 0, |file| {
                file.write_range(0, Bytes::from_static(b"without a handle"))
            }, work))
            .map_err(os)?;
        context.run_request(true, |work| context.flush_on_close(reader, work)).map_err(os)?;
        let before = epoch()?;
        context.run_request(true, |work| context.sync(work)).map_err(os)?;
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
        context.run_request(true, |work| context.mutate(|| source.create_file(&path, FileMetadata::default()), work))
            .map_err(os)?;
        let created = context.run_request(false, |work| context.open(&path, context.ledger.latest(), work))
            .map_err(os)?;
        context.run_request(true, |work| context.mutate_file(&path, created, |file| {
                file.write_range(0, Bytes::from_static(b"deferred"))
            }, work))
            .map_err(os)?;
        let reader = context.run_request(false, |work| context.open(&path, 0, work)).map_err(os)?;
        let before = epoch()?;
        context.run_request(true, |work| context.flush_on_close(reader, work)).map_err(os)?;
        context.run_request(true, |work| context.flush_on_close(created, work)).map_err(os)?;
        context.run_request(true, |work| context.sync(work)).map_err(os)?;
        assert_eq!(epoch()?, before, "manual close and fsync publish nothing");
        source.sync()?;
        assert_ne!(epoch()?, before, "an explicit mount sync publishes");
        Ok(())
    }

    #[test]
    fn change_labels_advance_exactly_for_objects_that_changed() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let label = |path: &MountPath| {
            context.run_request(false, |work| context.attributes(path, 0, work))
                .map(|attributes| attributes.change)
                .map_err(os)
        };
        let root = MountPath::root();
        let first = root.child(b"first".to_vec());
        let second = root.child(b"second".to_vec());
        for path in [&first, &second] {
            context.run_request(true, |work| context.mutate(|| source.create_file(path, FileMetadata::default()), work))
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

        context.run_request(true, |work| context.mutate_file(&first, 0, |file| {
                file.write_range(0, Bytes::from_static(b"written"))
            }, work))
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
        assert_ne!(
            context.run_request(false, |work| unstamped.label(lookup.node.file_id, None, None,
                &first, lookup, |_| Ok(true), |_| Ok(true), work)).map_err(os)?,
            context.run_request(false, |work| unstamped.label(lookup.node.file_id, None, None,
                &first, lookup, |_| Ok(true), |_| Ok(true), work)).map_err(os)?,
        );
        Ok(())
    }

    #[test]
    fn an_unconfirmed_fence_waits_only_for_what_changed() -> TestResult {
        use super::super::view_ledger::ViewChange;

        let (source, context) = checkout_context(MountPublication::Manual)?;
        let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .map_err(|(cause, _)| cause)?;
        let context = Arc::new(context);
        let observer = Arc::downgrade(&context);
        let observer: std::sync::Weak<dyn ViewObserver> = observer;
        source.observe_view(observer);
        let path = MountPath::root().child(b"kept".to_vec());
        source.create_file(&path, FileMetadata::default())?;
        // The creation was made around the mount; take it as waited out.
        let made = context.around.made.load(Ordering::Acquire);
        context.around.settled.store(made, Ordering::Release);
        let label = context.run_request(false, |work| context.attributes(&path, 0, work)).map_err(os)?.change;

        source.record_projection_change(&ViewChange::Unconfirmed);
        assert!(
            !context.revalidate(&work.native_mount_work())?,
            "unchanged labels do not require waiting out the attribute timeout"
        );
        assert_eq!(context.run_request(false, |work| context.attributes(&path, 0, work)).map_err(os)?.change, label);

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
            context.revalidate(&work.native_mount_work())?,
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
        let before = context.run_request(false, |work| context.cache_stamp(work)).map_err(os)?
            .ok_or("checkout has no view stamp")?;
        source.write_range(&path, 0, Bytes::from_static(b"changed"))?;
        let after = context.run_request(false, |work| context.cache_stamp(work)).map_err(os)?
            .ok_or("checkout has no view stamp")?;
        let written = source.lookup(&path)?.ok_or("file absent")?;
        let current = context.run_request(false, |work| context.changes.label(file_id,
            Some(after), binding, &path, written,
            |stamp| context.unchanged_since(&path, Some(file_id), stamp, work),
            |stamp| context.reported_unchanged_since(&path, Some(file_id), stamp, work), work)).map_err(os)?;
        assert_eq!(context.run_request(false, |work| context.changes.label(file_id,
            Some(after), binding, &path, written,
            |stamp| context.unchanged_since(&path, Some(file_id), stamp, work),
            |stamp| context.reported_unchanged_since(&path, Some(file_id), stamp, work), work)).map_err(os)?, current);
        assert!(context.run_request(false, |work| context.changes.label(file_id,
            Some(before), binding, &path, created,
            |stamp| context.unchanged_since(&path, Some(file_id), stamp, work),
            |stamp| context.reported_unchanged_since(&path, Some(file_id), stamp, work), work)).map_err(os)? > current);
        Ok(())
    }

    #[test]
    fn forgetting_the_view_rereads_what_the_source_did_not_record() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let path = MountPath::root().child(b"unrecorded".to_vec());
        source.create_file(&path, FileMetadata::default())?;
        let handle = context.run_request(false, |work| context.open(&path, 0, work)).map_err(os)?;
        let size = |handle| {
            context.run_request(false, |work| context.attributes(&path, handle, work))
                .map(|attributes| attributes.logical_bytes)
                .map_err(os)
        };
        assert_eq!((size(0)?, size(handle)?), (0, 0));
        let label = context.run_request(false, |work| context.attributes(&path, 0, work)).map_err(os)?.change;

        let stale = |lookup: &mut MountLookup| lookup.node.logical_bytes = 7;
        context
            .lookups
            .lock()
            .map_err(|_| "poisoned lookups")?
            .values_mut()
            .filter_map(|(lookup, _, _)| lookup.as_mut())
            .for_each(stale);
        let files = context.files.read().map_err(|_| "poisoned handles")?;
        stale(
            &mut context.run_request(false, |work| files.get(&handle, work)).map_err(os)?
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
        let relabeled = context.run_request(false, |work| context.attributes(&path, 0, work)).map_err(os)?.change;
        assert!(
            relabeled > label,
            "the reread attributes take a fresh label"
        );
        assert_eq!(
            context.run_request(false, |work| context.attributes(&path, handle, work)).map_err(os)?.change,
            relabeled,
            "which then holds for the unchanged object"
        );
        Ok(())
    }

    #[test]
    fn revalidation_waits_out_the_attribute_timeout_for_changes_around_the_mount() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let context = Arc::new(context);
        context.source.observe_view(Arc::<DarwinMountContext>::downgrade(&context));
        // SAFETY: the bridge reports a constant.
        let timeout = u64::from(unsafe { acyclic_fs_darwin_mount_attribute_timeout() });
        let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .map_err(|(cause, _)| cause)?;
        let barrier = || {
            let began = uptime(&mut &work).unwrap();
            context.revalidate(&work.native_mount_work()).unwrap();
            (began, uptime(&mut &work).unwrap())
        };
        let (began, ended) = barrier();
        assert!(ended - began < CALLBACK_GRACE_INTERVAL, "an unchanged view");

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
            ended - began < CALLBACK_GRACE_INTERVAL,
            "the client learns of the mount's own changes from their replies"
        );

        let waits_out_the_timeout = || {
            let (began, ended) = barrier();
            assert!(
                ended.as_secs() >= (began + CALLBACK_GRACE_INTERVAL).as_secs() + timeout,
                "every attribute cached before the barrier expired"
            );
            let (began, ended) = barrier();
            assert!(ended - began < CALLBACK_GRACE_INTERVAL, "once per change");
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
                let cancellation = crate::CancellationToken::new();
                let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
                    .expect("unlimited callback barrier");
                gate.drain(&work.native_mount_work()).expect("callback barrier remains live");
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
            Ok(context.run_request(false, |work| files.get(&handle, work)).map_err(os)?.ok_or("handle absent")?.file.is_some())
        };

        let closed_unused = context.run_request(false, |work| context.open(&path, 0, work)).map_err(os)?;
        assert!(!bound(closed_unused)?, "an open binds nothing");
        assert_eq!(
            context.run_request(false, |work| context.lookup_handle(&path, closed_unused, work))
                .map_err(os)?
                .node
                .file_id,
            created.node.file_id,
            "attributes need no source file"
        );
        assert!(!bound(closed_unused)?);

        let read = context.run_request(false, |work| context.open(&path, 0, work)).map_err(os)?;
        let bytes = context.run_request(false, |work| context.file_target(&path, read, work).map(|target| target.0))
            .map_err(os)?.0
            .read_up_to(0, 16)?;
        assert_eq!(bytes.as_ref(), b"kept");
        assert!(bound(read)?, "the first read binds the source file");

        let stale = context.run_request(false, |work| context.open(&path, 0, work)).map_err(os)?;
        source.remove(&path, Some(created.node.file_id))?;
        source.create_file(&path, FileMetadata::default())?;
        assert_eq!(context.run_request(false, |work| context.file_target(&path, stale, work).map(|target| target.0)).err(), Some(libc::ESTALE));
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
        let handle = context.run_request(false, |work| context.open(&replaced, 0, work))
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
            context.run_request(false, |work| {
                let files = context.files.read().map_err(|_| libc::EIO)?;
                Ok(files.get(&handle, work)?.ok_or(libc::ESTALE)?.file.is_none())
            }).map_err(os)?,
            "a failed rename leaves the handle as it was"
        );
        assert_eq!(rename_over(c"/replacement"), 0);
        let file = context.run_request(false, |work| context.file_target(&replaced, handle, work).map(|target| target.0)).map_err(os)?;
        assert_eq!(file.0.lookup()?.node.file_id, original.node.file_id);
        assert_eq!(file.0.read_up_to(0, 16)?.as_ref(), b"original");
        Ok(())
    }

    #[test]
    fn named_attribute_probes_need_no_source_call_without_attributes() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let path = MountPath::root().child(b"plain".to_vec());
        source.create_file(&path, FileMetadata::default())?;
        assert!(!context.run_request(false, |work| context.may_have_named_attributes(&path, work)).map_err(os)?);
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
        assert!(context.run_request(false, |work| context.may_have_named_attributes(&path, work)).map_err(os)?);
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
            ..create_metadata(0o644, IFREG, 0, 0)
        };
        context.run_request(true, |work| context.mutate(|| source.create_file(&path, created), work))
            .map_err(os)?;
        let times = || -> Result<_, Box<dyn std::error::Error>> {
            let metadata = source.lookup(&path)?.ok_or("file absent")?.metadata;
            Ok((metadata.modified_ns, metadata.changed_ns))
        };
        context.run_request(true, |work| context.mutate_file(&path, 0, |file| {
                file.write_range(0, Bytes::from_static(b"stamped"))
            }, work))
            .map_err(os)?;
        let (MetadataField::Value(modified), MetadataField::Value(changed)) = times()? else {
            return Err("write dropped a time".into());
        };
        assert!(modified > 1 && changed > 1, "a write stamps both times");
        context.run_request(true, |work| context.mutate_file(&path, 0, |file| file.resize(1), work))
            .map_err(os)?;
        let (MetadataField::Value(resized), _) = times()? else {
            return Err("resize dropped a time".into());
        };
        assert!(resized >= modified, "a truncation stamps the time again");
        Ok(())
    }

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
            began.elapsed() < CALLBACK_GRACE_INTERVAL,
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
            DirectoryHandle::new(root, source.binding_epoch(), context.run_request(false, |work| context.cache_epochs(work)).map_err(os)?);
        context.run_request(false, |work| advance_directory_to_offset(&context, &mut directory, 3, work))
            .map_err(std::io::Error::from_raw_os_error)?;
        source.create_file(
            &MountPath::root().child(b"after-page".to_vec()),
            FileMetadata::default(),
        )?;
        assert_eq!(
            context.run_request(false, |work| finish_directory_page(&context, &directory, false, work)),
            Err(libc::ESTALE)
        );
        assert_eq!(
            context.run_request(false, |work| ensure_directory_page(&context, &mut directory, work)),
            Err(libc::ESTALE)
        );
        Ok(())
    }

    #[test]
    fn a_listing_that_delivered_nothing_restarts_in_the_new_view() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let root = MountPath::root();
        let mut directory =
            DirectoryHandle::new(root, source.binding_epoch(), context.run_request(false, |work| context.cache_epochs(work)).map_err(os)?);
        context.run_request(false, |work| ensure_directory_page(&context, &mut directory, work))
            .map_err(std::io::Error::from_raw_os_error)?;
        source.create_file(
            &MountPath::root().child(b"after-page".to_vec()),
            FileMetadata::default(),
        )?;
        // Only `.` and `..` were delivered: the listing starts again.
        assert_eq!(context.run_request(false, |work| finish_directory_page(&context, &directory, false, work)), Ok(()));
        assert_eq!(context.run_request(false, |work| ensure_directory_page(&context, &mut directory, work)), Ok(()));
        Ok(())
    }

    #[test]
    fn directory_continuation_requires_its_listing_and_binding_unchanged() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let nested = MountPath::root().child(b"nested".to_vec());
        source.create_directory(&nested, FileMetadata::default())?;
        let epochs = context.run_request(false, |work| context.cache_epochs(work)).map_err(os)?.ok_or("checkout has no view stamp")?;
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
        assert!(context.run_request(false, |work| root.is_current(&context, work)).map_err(os)?);
        assert!(context.run_request(false, |work| directory.is_current(&context, work)).map_err(os)?);
        assert!(!context.run_request(false, |work| rebound.is_current(&context, work)).map_err(os)?);
        assert!(directory.can_reuse_pages());

        source.create_file(
            &MountPath::root().child(b"sibling".to_vec()),
            FileMetadata::default(),
        )?;
        assert!(!context.run_request(false, |work| root.is_current(&context, work)).map_err(os)?);
        assert!(context.run_request(false, |work| directory.is_current(&context, work)).map_err(os)?);
        source.create_file(&nested.child(b"child".to_vec()), FileMetadata::default())?;
        assert!(!context.run_request(false, |work| directory.is_current(&context, work)).map_err(os)?);

        let unrecorded =
            DirectoryHandle::new(nested, source.binding_epoch(), context.run_request(false, |work| context.cache_epochs(work)).map_err(os)?);
        assert!(context.run_request(false, |work| unrecorded.is_current(&context, work)).map_err(os)?);
        context.forget_view();
        assert!(
            !context.run_request(false, |work| unrecorded.is_current(&context, work)).map_err(os)?,
            "a forgotten view invalidates every page"
        );

        let epochless = DirectoryHandle::new(MountPath::root(), None, None);
        assert!(!context.run_request(false, |work| epochless.is_current(&context, work)).map_err(os)?);
        assert!(!epochless.can_reuse_pages());
        Ok(())
    }

    #[test]
    fn stalled_mount_loop_keeps_destination_fenced_until_callbacks_finish() -> TestResult {
        let cancellation = crate::CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let unmount = DarwinStopController::new(UnmountState::default(), &original)?;
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
            unmount,
        };
        let start = Instant::now();
        let result = with_detach_meter(&original.native_mount_work(), |meter|
            session.finish_thread_with_detach_meter(Duration::from_millis(20), meter, None))?;
        assert!(matches!(result, MountLoopResult::TimedOut));
        assert!(classify_detached_loop(&result, true, false).is_err());
        assert!(start.elapsed() < Duration::from_secs(1));
        assert!(session.thread.is_some());
        let _ = release.send(());
        assert!(complete.recv_timeout(Duration::from_secs(1)).is_ok());
        let result = with_detach_meter(&original.native_mount_work(), |meter|
            session.finish_thread_with_detach_meter(Duration::from_secs(1), meter, None))?;
        assert!(matches!(result, MountLoopResult::Exited(0)));
        assert!(classify_detached_loop(&result, true, false).is_ok());
        assert!(session.thread.is_none());
        // This synthetic session has no destination or native driver to stop.
        session.teardown_complete = true;
        Ok(())
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
