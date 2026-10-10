//! Audited Windows `ProjFS` FFI with exact close-boundary authored capture.
//!
//! `ProjFS` does not expose write byte ranges, so modified and newly created
//! files are streamed from final host state only after the corresponding file
//! handle closes. Rename and hard-link notifications preserve stable identity,
//! and every acknowledged authored notification seals the checkout generation.
//!
//! Bounded detach uses the caller's one work ledger at the actual SDK stop,
//! capture, native probe, enumeration, and physical deletion boundaries.
//! Unbounded stop is the same engine. Denial retains the live context or the
//! stopped projection's authenticated cleanup custody, never another root.

#![allow(
    unsafe_code,
    unsafe_op_in_unsafe_fn,
    reason = "audited Windows ProjFS FFI"
)]

use super::provider_stack;
use super::{
    DIRECTORY_PAGE_SIZE, DriverStartFailure, MountContentPin, MountFilesystem, MountLookup,
    MountNode, MountNodeKind, MountPath, MountSourceError, NativeMountError, NativeMountRequest,
    ViewObserver, ViewOrigin, ViewStamp,
};
use crate::FileId;
use crate::kernel::{FileMetadata, MetadataField};
use crate::native_host::HostRoot;
use crate::demand::DemandWork;
use crate::native_host::NativeHostWork as _;
use std::collections::{HashMap, HashSet};
use std::ffi::c_void;
use std::mem::size_of;
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use windows::Win32::Storage::FileSystem::{
    FILE_ATTRIBUTE_ARCHIVE, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_NORMAL,
    FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS, FILE_ATTRIBUTE_REPARSE_POINT,
};
use windows::Win32::Storage::ProjectedFileSystem::{
    PRJ_CALLBACK_DATA, PRJ_CALLBACKS, PRJ_DIR_ENTRY_BUFFER_HANDLE, PRJ_EXT_INFO_TYPE_SYMLINK,
    PRJ_EXTENDED_INFO, PRJ_EXTENDED_INFO_0, PRJ_EXTENDED_INFO_0_0, PRJ_FILE_BASIC_INFO,
    PRJ_FLAG_USE_NEGATIVE_PATH_CACHE, PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT, PRJ_NOTIFICATION,
    PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED,
    PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED,
    PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION, PRJ_NOTIFICATION_FILE_OPENED,
    PRJ_NOTIFICATION_FILE_OVERWRITTEN, PRJ_NOTIFICATION_FILE_RENAMED,
    PRJ_NOTIFICATION_HARDLINK_CREATED, PRJ_NOTIFICATION_MAPPING, PRJ_NOTIFICATION_NEW_FILE_CREATED,
    PRJ_NOTIFICATION_PARAMETERS, PRJ_NOTIFICATION_PRE_RENAME, PRJ_NOTIFICATION_PRE_SET_HARDLINK,
    PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_DELETED, PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_MODIFIED,
    PRJ_NOTIFY_FILE_HANDLE_CLOSED_NO_MODIFICATION, PRJ_NOTIFY_FILE_OPENED,
    PRJ_NOTIFY_FILE_OVERWRITTEN, PRJ_NOTIFY_FILE_RENAMED, PRJ_NOTIFY_HARDLINK_CREATED,
    PRJ_NOTIFY_NEW_FILE_CREATED, PRJ_NOTIFY_PRE_RENAME, PRJ_NOTIFY_PRE_SET_HARDLINK,
    PRJ_NOTIFY_TYPES, PRJ_PLACEHOLDER_INFO, PRJ_STARTVIRTUALIZING_OPTIONS,
    PRJ_UPDATE_ALLOW_READ_ONLY, PRJ_UPDATE_ALLOW_TOMBSTONE, PRJ_UPDATE_TYPES, PrjDeleteFile,
    PrjFileNameCompare, PrjMarkDirectoryAsPlaceholder, PrjStartVirtualizing, PrjStopVirtualizing,
    PrjWritePlaceholderInfo, PrjWritePlaceholderInfo2,
};
use windows::core::{GUID, HRESULT, HSTRING, PCWSTR};

const HR_OK: HRESULT = HRESULT(0);
const HR_FILE_NOT_FOUND: HRESULT = HRESULT(0x8007_0002_u32.cast_signed());
const HR_PATH_NOT_FOUND: HRESULT = HRESULT(0x8007_0003_u32.cast_signed());
const HR_ALREADY_EXISTS: HRESULT = HRESULT(0x8007_00b7_u32.cast_signed());
const HR_FILE_EXISTS: HRESULT = HRESULT(0x8007_0050_u32.cast_signed());
const HR_DIRECTORY: HRESULT = HRESULT(0x8007_010b_u32.cast_signed());
const HR_NOT_SAME_DEVICE: HRESULT = HRESULT(0x8007_0011_u32.cast_signed());
const HR_INVALID_DATA: HRESULT = HRESULT(0x8007_000d_u32.cast_signed());
const HR_NOT_SUPPORTED: HRESULT = HRESULT(0x8007_0032_u32.cast_signed());
const HR_UNEXPECTED: HRESULT = HRESULT(0x8000_ffff_u32.cast_signed());
const HR_FILE_INVALID: HRESULT = HRESULT(0x8007_03ee_u32.cast_signed());
const HR_IO_DEVICE: HRESULT = HRESULT(0x8007_045d_u32.cast_signed());
const HR_VIRTUALIZATION_INVALID_OPERATION: HRESULT = HRESULT(0x8007_0181_u32.cast_signed());
const HR_SHARING_VIOLATION: HRESULT = HRESULT(0x8007_0020_u32.cast_signed());
const HR_DIRECTORY_NOT_EMPTY: HRESULT = HRESULT(0x8007_0091_u32.cast_signed());

/// Every notification the provider observes, for every file, for its whole
/// life. A file created or renamed through the mount keeps this exact set:
/// narrowing it would hide a later write, metadata edit, or delete of that
/// name once its first capture had completed.
const FILE_NOTIFICATIONS: PRJ_NOTIFY_TYPES = PRJ_NOTIFY_TYPES(
    PRJ_NOTIFY_NEW_FILE_CREATED.0
        | PRJ_NOTIFY_FILE_OVERWRITTEN.0
        | PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_MODIFIED.0
        | PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_DELETED.0
        | PRJ_NOTIFY_FILE_HANDLE_CLOSED_NO_MODIFICATION.0
        | PRJ_NOTIFY_FILE_OPENED.0
        | PRJ_NOTIFY_PRE_RENAME.0
        | PRJ_NOTIFY_FILE_RENAMED.0
        | PRJ_NOTIFY_PRE_SET_HARDLINK.0
        | PRJ_NOTIFY_HARDLINK_CREATED.0,
);
const NOTIFICATION_ROOT: [u16; 1] = [0];
/// Largest read from the source while writing a file; bounds peak memory.
const WRITE_CHUNK_BYTES: u32 = 1 << 20;
/// Bounds the provider's memo of read-only close bindings.
const MAXIMUM_CACHED_BINDINGS: usize = 16_384;

/// One entry of a source listing, as this provider writes it.
struct ProjectedEntry {
    // ProjFS compares NUL-terminated UTF-16 names, not SDK byte cursors.
    name: Vec<u16>,
    /// The entry's exact path component.
    component: Vec<u8>,
    /// What the entry is written from.
    facts: EntryFacts,
    /// The basic information written for this entry.
    info: PRJ_FILE_BASIC_INFO,
    symlink_target: Option<bytes::Bytes>,
    /// Whether a link names a directory: a Windows link says so when made.
    directory_link: bool,
}

/// A real heap owner, not a second allocation ledger. The original operation
/// retains only the custody's weak physical-drop proof. Cloning that proof is
/// free; only the actual buffer or payload constructor admits new storage.
struct RetainedBuffer<T> {
    value: Option<T>,
    custody: super::NativeMountAllocationCustody,
    bytes: u64,
}

impl<T> RetainedBuffer<T> {
    fn transferred(
        value: T, bytes: u64, custody: super::NativeMountAllocationCustody, work: &mut DemandWork,
    ) -> Result<Self, NativeMountError> {
        if let Err(error) = custody.retain_admitted(bytes, work) {
            drop(value);
            work.release(bytes).map_err(detach_work_error)?;
            return Err(error);
        }
        Ok(Self { value: Some(value), custody, bytes })
    }

}

impl<T> std::ops::Deref for RetainedBuffer<T> {
    type Target = T;
    fn deref(&self) -> &T { self.value.as_ref().expect("live retained buffer") }
}

impl<T> std::ops::DerefMut for RetainedBuffer<T> {
    fn deref_mut(&mut self) -> &mut T { self.value.as_mut().expect("live retained buffer") }
}

impl<T> Drop for RetainedBuffer<T> {
    fn drop(&mut self) {
        drop(self.value.take());
        self.custody.retire(self.bytes).expect("only actually freed buffer bytes retire");
    }
}

/// Sized Arc payloads use their real Rust destructor as the lifetime boundary,
/// not SDK/allocator/RSS estimates. Each last-strong-reference payload owns its
/// own alias of the original operation's physical-drop proof.
struct RetainedPayload<T> {
    value: std::mem::ManuallyDrop<T>,
    custody: super::NativeMountAllocationCustody,
    bytes: u64,
}

impl<T> std::ops::Deref for RetainedPayload<T> {
    type Target = T;
    fn deref(&self) -> &T { &self.value }
}

impl<T> std::ops::DerefMut for RetainedPayload<T> {
    fn deref_mut(&mut self) -> &mut T { &mut self.value }
}

impl<T> Drop for RetainedPayload<T> {
    fn drop(&mut self) {
        // SAFETY: this is the sole last-strong-reference payload destructor.
        unsafe { std::mem::ManuallyDrop::drop(&mut self.value); }
        self.custody.retire(self.bytes).expect("actual Arc payload dropped before retirement");
    }
}

impl<T: ViewObserver> ViewObserver for RetainedPayload<T> {
    fn view_changed(&self, position: ViewStamp, origin: ViewOrigin) {
        self.value.view_changed(position, origin);
    }
}

fn retained_arc<T>(
    value: T, custody: &super::NativeMountAllocationCustody, work: &mut DemandWork,
) -> Result<Arc<RetainedPayload<T>>, NativeMountError> {
    let bytes = size_of::<RetainedPayload<T>>() as u64;
    custody.claim(bytes, work)?;
    Ok(Arc::new(RetainedPayload { value: std::mem::ManuallyDrop::new(value), custody: custody.clone(), bytes }))
}

fn retained_box<T>(
    value: T, custody: &super::NativeMountAllocationCustody, work: &mut DemandWork,
) -> Result<RetainedBuffer<Box<T>>, NativeMountError> {
    let bytes = size_of::<T>() as u64;
    custody.claim(bytes, work)?;
    Ok(RetainedBuffer { value: Some(Box::new(value)), custody: custody.clone(), bytes })
}

struct Runtime {
    source: Arc<dyn MountFilesystem>,
    root: PathBuf,
    metadata_root: Arc<HostRoot>,
    writable: bool,
    metadata_baselines: Arc<Mutex<HashMap<u128, OpenMetadataState>>>,
    projection: Arc<Mutex<ProjectionCache>>,
    /// Everything written into the projection, kept current as the source
    /// changes.
    projector: Arc<Projector>,
    metadata_probes: Arc<Mutex<HashMap<MountPath, usize>>>,
    /// The admitted retry snapshot doubles as borrowed metadata suppression;
    /// no second path clone or guessed hash-table allocation is needed.
    capture_probes: std::sync::RwLock<Vec<(MountPath, u64, bool)>>,
    flush_gate: Mutex<()>,
    /// Present only while the sole SDK stop synchronously drains callbacks.
    stop_meter: Mutex<Option<Arc<crate::demand::NativeDemandReadMeter>>>,
    /// Held for a file while one handle's metadata edits are read from the
    /// source and written back. `ProjFS` names each handle apart, so handles
    /// to one file close concurrently; each applies only what it changed, on
    /// what the one before it left. Keyed by the file's host identity, which
    /// every spelling of its name shares.
    metadata_edits: Arc<FileLocks>,
    post_operation_failure: Arc<Mutex<PostOperationFailures>>,
    callbacks: CallbackGate,
}

struct StopMeterScope<'a>(&'a Runtime);

impl Drop for StopMeterScope<'_> {
    fn drop(&mut self) {
        let _ = lock_recover(&self.0.stop_meter).take();
    }
}

struct CallbackGate {
    shards: Box<[Mutex<()>]>,
    active: Mutex<usize>,
    idle: Condvar,
}

struct ActiveCallback<'a>(&'a CallbackGate);

impl Drop for ActiveCallback<'_> {
    fn drop(&mut self) {
        let mut active = lock_recover(&self.0.active);
        *active = active.saturating_sub(1);
        if *active == 0 {
            self.0.idle.notify_all();
        }
    }
}

#[derive(Default)]
struct PostOperationFailures {
    pending_captures: HashMap<MountPath, PendingCapture>,
    next_capture_serial: u64,
    unreplayable: Option<String>,
    /// A callback whose provider controls could not be admitted never ran its
    /// authored handler. Its exact failure permanently fences root deletion.
    provider_control_failure: Option<provider_stack::ProviderFailure>,
}

#[derive(Clone)]
struct PendingCapture {
    serial: u64,
    error: String,
    subtree: bool,
}

impl PostOperationFailures {
    fn queue_capture(&mut self, path: MountPath, error: String) {
        self.next_capture_serial = self.next_capture_serial.wrapping_add(1);
        let subtree = self
            .pending_captures
            .get(&path)
            .is_some_and(|capture| capture.subtree);
        self.pending_captures.insert(
            path,
            PendingCapture {
                serial: self.next_capture_serial,
                error,
                subtree,
            },
        );
    }

    fn queue_subtree(&mut self, path: &MountPath, error: String) {
        self.queue_capture(path.clone(), error);
        if let Some(capture) = self.pending_captures.get_mut(path) {
            capture.subtree = true;
        }
    }

    fn rename_pending_captures(&mut self, source: &MountPath, destination: &MountPath) {
        let moved = self
            .pending_captures
            .iter()
            .filter_map(|(path, capture)| {
                projfs_path_suffix(path, source).map(|suffix| {
                    let destination = suffix.iter().fold(destination.clone(), |path, component| {
                        path.child(component.clone())
                    });
                    (path.clone(), destination, capture.clone())
                })
            })
            .collect::<Vec<_>>();
        for (source, destination, capture) in moved {
            self.pending_captures.remove(&source);
            if let Some(existing) = self.pending_captures.get_mut(&destination) {
                existing.subtree |= capture.subtree;
                existing.serial = existing.serial.max(capture.serial);
            } else {
                self.pending_captures.insert(destination, capture);
            }
        }
    }
}

fn projfs_path_suffix<'a>(path: &'a MountPath, prefix: &MountPath) -> Option<&'a [Vec<u8>]> {
    let suffix = path.components().get(prefix.components().len()..)?;
    path.components()
        .iter()
        .zip(prefix.components())
        .all(|(left, right)| same_projfs_name(left, right))
        .then_some(suffix)
}

fn same_projfs_name(left: &[u8], right: &[u8]) -> bool {
    if left == right {
        return true;
    }
    matches!(compare_projfs_name(left, right), Some(0))
}

fn compare_projfs_name(left: &[u8], right: &[u8]) -> Option<i32> {
    let (Some(mut left), Some(mut right)) = (decode_utf16_name(left), decode_utf16_name(right))
    else {
        return None;
    };
    left.push(0);
    right.push(0);
    // SAFETY: both owned buffers are live, NUL-terminated UTF-16 names.
    Some(unsafe {
        PrjFileNameCompare(
            PCWSTR::from_raw(left.as_ptr()),
            PCWSTR::from_raw(right.as_ptr()),
        )
    })
}

impl CallbackGate {
    fn start() -> Result<Self, NativeMountError> {
        Self::start_with_work(&mut DemandWork::new(
            crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
        ))
    }

    fn start_with_work(work: &mut DemandWork) -> Result<Self, NativeMountError> {
        const SHARDS: usize = 64;
        let bytes = (SHARDS * size_of::<Mutex<()>>()) as u64;
        work.allocate(bytes).map_err(detach_work_error)?;
        let mut shards = Vec::new();
        if let Err(error) = shards.try_reserve_exact(SHARDS) {
            drop(shards);
            work.release(bytes).map_err(detach_work_error)?;
            return Err(NativeMountError::Io(std::io::Error::new(std::io::ErrorKind::OutOfMemory, error)));
        }
        for _ in 0..SHARDS {
            shards.push(Mutex::new(()));
        }
        Ok(Self {
            shards: shards.into_boxed_slice(),
            active: Mutex::new(0),
            idle: Condvar::new(),
        })
    }

    /// Waits on the existing callback controls without assigning a guessed
    /// cost to the foreign callbacks that the SDK happens to drain.
    fn drain_measured(&self, work: &mut DemandWork) -> Result<(), NativeMountError> {
        let mut active = lock_recover(&self.active);
        while *active != 0 {
            work.check().map_err(detach_work_error)?;
            active = self.idle.wait_timeout(active, std::time::Duration::from_millis(10))
                .unwrap_or_else(std::sync::PoisonError::into_inner).0;
        }
        work.check().map_err(detach_work_error)
    }

    fn call_observed<T>(
        &self,
        key: u128,
        operation: impl FnOnce() -> T,
        observe: impl FnOnce(&T),
    ) -> Option<T> {
        let count = u128::try_from(self.shards.len()).ok()?;
        let shard = usize::try_from(key % count).ok()?;
        {
            let mut active = lock_recover(&self.active);
            *active = active.checked_add(1)?;
        }
        let _active = ActiveCallback(self);
        let _serial = lock_recover(self.shards.get(shard)?);
        let result = operation();
        observe(&result);
        Some(result)
    }
}

#[cfg(test)]
fn flush_callback_gate(
    callbacks: &CallbackGate,
    failure: Arc<Mutex<PostOperationFailures>>,
    capture: impl Fn(&[(MountPath, u64, bool)]) -> Result<(), MountSourceError>,
) -> Result<(), NativeMountError> {
    flush_callback_gate_measured(callbacks, failure.as_ref(), None, &mut DemandWork::new(
        crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
    ), |paths, _work| capture(paths).map_err(detach_source_error))
}

/// Snapshots only replayable failures, retaining their serials until a
/// successful capture proves exactly those writes were joined. Buffer claims
/// live until the buffers actually drop, including every denied replay.
fn flush_callback_gate_measured(
    callbacks: &CallbackGate,
    failure: &Mutex<PostOperationFailures>,
    probes: Option<&std::sync::RwLock<Vec<(MountPath, u64, bool)>>>,
    work: &mut DemandWork,
    mut capture: impl FnMut(&[(MountPath, u64, bool)], &mut DemandWork) -> Result<(), NativeMountError>,
) -> Result<(), NativeMountError> {
    callbacks.drain_measured(work)?;
    if let Some(error) = lock_recover(failure).provider_control_failure {
        return Err(error.into_mount_error());
    }
    let mut pending = Vec::new();
    let mut live = 0_u64;
    let result = (|| {
        {
            let failure = lock_recover(failure);
            let bytes = (failure.pending_captures.len() as u64)
                .checked_mul(size_of::<(MountPath, u64, bool)>() as u64)
                .ok_or(crate::WorkError::Overflow)?;
            work.allocate(bytes).map_err(detach_work_error)?;
            live = bytes;
            pending.try_reserve_exact(failure.pending_captures.len())
                .map_err(|error| NativeMountError::Io(std::io::Error::new(std::io::ErrorKind::OutOfMemory, error)))?;
            for (path, capture) in &failure.pending_captures {
                work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })
                    .map_err(detach_work_error)?;
                let (path, bytes) = clone_mount_path_measured(path, work).map_err(detach_work_error)?;
                live = live.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
                pending.push((path, capture.serial, capture.subtree));
            }
        }
        let mut sorting_failure = None;
        // The unstable sort uses no temporary allocation. Each actual
        // comparison is admitted before examining those two canonical paths.
        pending.sort_unstable_by(|(left, ..), (right, ..)| {
            if sorting_failure.is_some() {
                return std::cmp::Ordering::Equal;
            }
            if let Err(error) = work.charge(crate::WorkCounters { items_examined: 2, ..crate::WorkCounters::UNCHARGED }) {
                sorting_failure = Some(error);
                return std::cmp::Ordering::Equal;
            }
            left.components().len().cmp(&right.components().len())
                .then_with(|| left.components().cmp(right.components()))
        });
        if let Some(error) = sorting_failure {
            return Err(detach_work_error(error));
        }
        // Host capture reads inside the virtualization root; release the
        // failure lock so nested native notifications can finish.
        if let Some(probes) = probes {
            *probes.write().unwrap_or_else(std::sync::PoisonError::into_inner) = std::mem::take(&mut pending);
            let held = probes.read().unwrap_or_else(std::sync::PoisonError::into_inner);
            let captured = capture(&held, work);
            drop(held);
            pending = std::mem::take(&mut *probes.write().unwrap_or_else(std::sync::PoisonError::into_inner));
            captured?;
        } else {
            capture(&pending, work)?;
        }
        {
            let mut failure = lock_recover(failure);
            for (path, serial, _) in &pending {
                work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })
                    .map_err(detach_work_error)?;
                if failure.pending_captures.get(path).map(|capture| capture.serial) == Some(*serial) {
                    failure.pending_captures.remove(path);
                }
            }
        }
        callbacks.drain_measured(work)?;
        let failure = lock_recover(failure);
        let error = failure.unreplayable.as_deref().or_else(|| {
            failure.pending_captures.values().next().map(|capture| capture.error.as_str())
        });
        if let Some(error) = error {
            const PREFIX: &str = "ProjFS post-operation capture failed; mount cannot publish: ";
            let bytes = PREFIX.len().checked_add(error.len()).ok_or(crate::WorkError::Overflow)? as u64;
            let copied = crate::WorkCounters { bytes_copied: bytes, ..crate::WorkCounters::UNCHARGED };
            work.admit(copied).map_err(detach_work_error)?;
            work.allocate(bytes).map_err(detach_work_error)?;
            let mut detail = String::new();
            if let Err(error) = detail.try_reserve_exact(bytes as usize) {
                work.release(bytes).map_err(detach_work_error)?;
                return Err(NativeMountError::Io(std::io::Error::new(std::io::ErrorKind::OutOfMemory, error)));
            }
            detail.push_str(PREFIX);
            detail.push_str(error);
            work.record(copied).map_err(detach_work_error)?;
            // The returned error now owns this diagnostic allocation.
            return Err(NativeMountError::Driver(detail));
        }
        Ok(())
    })();
    drop(pending);
    work.release(live).map_err(detach_work_error)?;
    result
}

fn clone_mount_path_measured(path: &MountPath, work: &mut DemandWork) -> Result<(MountPath, u64), crate::DemandError> {
    clone_mount_components_measured(path.components(), work)
}

fn clone_mount_components_measured(source: &[Vec<u8>], work: &mut DemandWork) -> Result<(MountPath, u64), crate::DemandError> {
    let bytes = (source.len() as u64).checked_mul(size_of::<Vec<u8>>() as u64)
        .ok_or(crate::WorkError::Overflow)?;
    work.allocate(bytes)?;
    let mut components = Vec::new();
    let mut live = bytes;
    let result = (|| {
        components.try_reserve_exact(source.len())
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::OutOfMemory, error))?;
        for component in source {
            let copied = crate::WorkCounters { bytes_copied: component.len() as u64, items_examined: 1, ..crate::WorkCounters::UNCHARGED };
            work.admit(copied)?;
            work.allocate(component.len() as u64)?;
            live = live.checked_add(component.len() as u64).ok_or(crate::WorkError::Overflow)?;
            let mut owned = Vec::new();
            owned.try_reserve_exact(component.len())
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::OutOfMemory, error))?;
            owned.extend_from_slice(component);
            components.push(owned);
            work.record(copied)?;
        }
        Ok(())
    })();
    match result {
        Ok(()) => Ok((MountPath { components }, live)),
        Err(error) => {
            drop(components);
            work.release(live)?;
            Err(error)
        }
    }
}

fn defer_host_capture(failure: &Mutex<PostOperationFailures>, path: MountPath) {
    lock_recover(failure).queue_capture(path, "host capture pending".to_owned());
}

/// The source view one read began in. It is sampled before the read, so any
/// later change to what the read depended on is recorded after it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReadBasis {
    stamp: ViewStamp,
    binding: u64,
}

impl ReadBasis {
    /// `None` while the view is moving or for a source that cannot version it.
    fn sample(source: &dyn MountFilesystem) -> Option<Self> {
        if !source.view_is_stable() {
            return None;
        }
        Some(Self {
            stamp: source.view_stamp()?,
            binding: source.binding_epoch()?,
        })
    }

    /// Whether a read that found `file_id` (`None`: nothing) at `path` still
    /// describes the source: no rebind, and nothing it depended on changed.
    fn still_describes(
        self,
        source: &dyn MountFilesystem,
        path: &MountPath,
        file_id: Option<FileId>,
    ) -> bool {
        source.binding_epoch() == Some(self.binding)
            && source.unchanged_since(path, file_id, self.stamp)
    }

    /// As [`Self::still_describes`], except across changes the source could
    /// not confirm it reported: then only a read again can tell.
    fn reported_still_describes(
        self,
        source: &dyn MountFilesystem,
        path: &MountPath,
        file_id: Option<FileId>,
    ) -> bool {
        source.binding_epoch() == Some(self.binding)
            && source.reported_unchanged_since(path, file_id, self.stamp)
    }
}

/// Whether what the projection wrote at a path is gone.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Release {
    /// Gone, or the user's and kept.
    Done,
    /// Held open by another process: tried again later.
    Busy,
}

/// What the projection wrote at a path that `ProjFS` does not manage for
/// it: the projection replaces or removes it only while it is still
/// exactly what was written.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Owned {
    /// A symbolic link to the target with this digest: a link placeholder
    /// turns full once read, and `ReFS` takes only ordinary links.
    Link(u64),
    /// An ordinary file the projection wrote, as it left it. `ProjFS` drops
    /// a time set through a writable handle to a placeholder it manages, so
    /// no file is one.
    File(HostWindowsMetadata),
}

/// What writing one entry into the projection placed there.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Placed {
    /// Nothing: something is there already, or its directory is gone.
    Nothing,
    /// Written with nothing the projection owns in it: a link placeholder,
    /// which `ProjFS` manages, or an ordinary directory.
    Unowned,
    /// What `ProjFS` does not manage for the projection.
    Owned(Owned),
    /// Nothing yet: the source changed while its content was read. Its
    /// directory is listed again.
    Retry,
}

impl Placed {
    const fn owned(self) -> Option<Owned> {
        match self {
            Self::Owned(owned) => Some(owned),
            Self::Nothing | Self::Unowned | Self::Retry => None,
        }
    }
}

/// What one path was written from: the node, its metadata, and the content
/// its file holds.
#[derive(Clone, Copy, PartialEq, Eq)]
struct EntryFacts {
    lookup: MountLookup,
    pin: Option<MountContentPin>,
}

impl EntryFacts {
    /// Whether what was written from `self` is also what `current` would
    /// write. Access times move with every read and are not written.
    fn describe(&self, current: &Self) -> bool {
        let unaccessed = |facts: &Self| {
            let mut lookup = facts.lookup;
            lookup.metadata.accessed_ns = MetadataField::Unavailable;
            (lookup, facts.pin)
        };
        unaccessed(self) == unaccessed(current)
    }
}

/// Source facts the provider memoizes between callbacks.
///
/// Each fact keeps the basis it was read in and is reused only while the
/// source proves that nothing it depended on has changed since.
#[derive(Default)]
struct ProjectionCache {
    bindings: HashMap<MountPath, ReadOnlyBinding>,
}

impl ProjectionCache {
    fn clear(&mut self) {
        self.bindings.clear();
    }

    fn remember_binding(&mut self, path: MountPath, binding: ReadOnlyBinding) {
        if self.bindings.len() >= MAXIMUM_CACHED_BINDINGS {
            self.bindings.clear();
        }
        self.bindings.insert(path, binding);
    }
}

/// What the projection wrote at one path, as a source read wrote it.
#[derive(Clone, Copy, PartialEq, Eq)]
struct WrittenEntry {
    file_id: FileId,
    /// What is held here that `ProjFS` does not manage; `None` for what it
    /// does.
    owned: Option<Owned>,
    /// What it was written from; `None` for one known only to be gone.
    facts: Option<EntryFacts>,
    /// The view it was read in; `None` when the source cannot version it
    /// or was moving, which no later change is needed to supersede.
    basis: Option<ReadBasis>,
}

impl WrittenEntry {
    /// Whether a change since it was written may have made it wrong.
    fn superseded(&self, source: &dyn MountFilesystem, path: &MountPath) -> bool {
        self.basis
            .is_none_or(|basis| !basis.still_describes(source, path, Some(self.file_id)))
    }

    /// Whether only changes the source could not confirm it reported
    /// superseded it, so reading it again tells whether it still holds.
    fn unconfirmed(&self, source: &dyn MountFilesystem, path: &MountPath) -> bool {
        self.facts.is_some()
            && self.basis.is_some_and(|basis| {
                basis.reported_still_describes(source, path, Some(self.file_id))
            })
    }
}

/// The files a rename through the mount carried unchanged, as the source
/// holds them at their new paths, read in `basis`.
#[derive(Default)]
struct Carried {
    basis: Option<ReadBasis>,
    files: Vec<(MountPath, EntryFacts)>,
}

impl Carried {
    /// Reads each of `current`, the files written beneath `from` that held
    /// what the source did before it renamed `from` to `to`, at its new path.
    fn read(
        source: &dyn MountFilesystem,
        from: &MountPath,
        to: &MountPath,
        current: &[MountPath],
    ) -> Self {
        let basis = ReadBasis::sample(source);
        let files = current
            .iter()
            .filter_map(|path| {
                let moved = rebased(path, from, to)?;
                let (lookup, pin) = source.lookup_pinned(&moved).ok().flatten()?;
                (lookup.node.kind != MountNodeKind::Directory)
                    .then_some((moved, EntryFacts { lookup, pin }))
            })
            .collect();
        Self { basis, files }
    }
}

/// A directory this provider created as an ordinary directory, and what it
/// wrote into it.
struct Materialized {
    /// The view its listing was read in; `None` when the source cannot
    /// version it or was moving.
    basis: Option<ReadBasis>,
    /// Every name written or found here, and whether it is a directory: the
    /// base a changed listing is reconciled against.
    names: HashMap<Vec<u8>, bool>,
}

/// Most threads that write one tree at once.
const MATERIALIZE_WORKERS: usize = 8;

/// A [`Projection`] several workers write through at once.
struct SharedProjection<'a>(Projection<'a>);

// SAFETY: the source is `Sync`, the root a shared path, and the context an
// opaque token `ProjFS` accepts from any thread for the projection's life,
// which the scoped workers do not outlive.
unsafe impl Sync for SharedProjection<'_> {}

/// Directories waiting to be written, and how many are being written.
struct MaterializeQueue {
    state: Mutex<MaterializeProgress>,
    changed: Condvar,
}

struct MaterializeProgress {
    pending: Vec<MountPath>,
    writing: usize,
    failure: Option<String>,
}

impl MaterializeQueue {
    fn new(pending: Vec<MountPath>) -> Self {
        Self {
            state: Mutex::new(MaterializeProgress {
                pending,
                writing: 0,
                failure: None,
            }),
            changed: Condvar::new(),
        }
    }

    /// The next directory to write; `None` once none is left or one failed.
    fn next(&self) -> Option<MountPath> {
        let mut state = lock_recover(&self.state);
        loop {
            if state.failure.is_some() {
                return None;
            }
            if let Some(directory) = state.pending.pop() {
                state.writing += 1;
                return Some(directory);
            }
            if state.writing == 0 {
                return None;
            }
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    /// Records how writing one directory went, and what it found.
    fn done(&self, written: Result<Vec<MountPath>, String>) {
        let mut state = lock_recover(&self.state);
        state.writing -= 1;
        match written {
            Ok(found) => state.pending.extend(found),
            Err(error) => {
                state.failure.get_or_insert(error);
            }
        }
        self.changed.notify_all();
    }

    fn finish(self) -> Result<(), String> {
        self.state
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .failure
            .map_or(Ok(()), Err)
    }
}

/// Where this provider writes: the source it projects, its live context,
/// and the projection's root on the host.
#[derive(Clone, Copy)]
struct Projection<'a> {
    source: &'a dyn MountFilesystem,
    context: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT,
    root: &'a std::path::Path,
    /// Host edits not yet captured: a file holding one is the user's until
    /// the source has it.
    captures: &'a Mutex<PostOperationFailures>,
    /// Whether the mount is still starting, which nothing can read yet: a
    /// file is written at its name directly, with no staged name first.
    starting: bool,
}

/// The projection this provider writes, and the worker that keeps it
/// current.
///
/// `ProjFS` never lets a directory it projected be renamed, and drops a
/// time set through a writable handle to a placeholder it manages, so
/// nothing is projected on demand: every directory is created as an
/// ordinary directory and every file written as an ordinary file before
/// anything can list it (`ProjFS` asks the provider nothing about an
/// ordinary directory). Only links are placeholders.
///
/// Every path written is kept with the basis it was written in, and once a
/// change supersedes it the worker writes it again from the source, or
/// removes it where the source names nothing. Every directory is kept with
/// the names it holds, and once its source listing changes the worker
/// reconciles three ways: a name the source gained is written unless
/// something is there already, and a directory the source lost is emptied
/// of what was written into it and removed if nothing else is left. What
/// the user creates, changes or deletes through the mount is authored state
/// and stays: a file is replaced or removed only while it is exactly as
/// written.
///
/// A superseded path is pending from the moment the worker finds it until
/// it is replaced, released, or fails, including while the worker is
/// replacing it, so revalidation never returns while one still describes a
/// superseded source. One another process holds open cannot be replaced
/// until the handle closes: it stays pending, retried as handles close and
/// every [`HELD_RETRY`], and revalidation waits for it at most
/// [`HELD_SETTLE_LIMIT`] once every change has been served, then fails
/// naming it.
struct Projector {
    state: Mutex<ProjectorState>,
    /// Held by whatever writes directories into the projection, so a
    /// listing is written and recorded as one step.
    tree: Mutex<()>,
    /// Held for a path while what is written there is replaced or removed.
    paths: KeyedLocks<MountPath>,
    changed: Condvar,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
}

/// How often a path held open is tried again.
const HELD_RETRY: std::time::Duration = std::time::Duration::from_millis(20);
/// How long revalidation waits for paths held open, once every
/// change has been served.
const HELD_SETTLE_LIMIT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Default)]
struct ProjectorState {
    written: HashMap<MountPath, WrittenEntry>,
    /// Superseded paths not yet replaced, released, or failed.
    pending: HashMap<MountPath, WrittenEntry>,
    /// Every directory created in the projection.
    directories: HashMap<MountPath, Materialized>,
    /// Directories to list again whatever their basis says.
    stale: HashSet<MountPath>,
    /// Pending paths a listing waits on: when one is settled, its parent is
    /// listed again.
    relist: HashSet<MountPath>,
    /// Directories the source removed that still held a file another
    /// process had open: removed once nothing beneath them is pending.
    vacated: HashSet<MountPath>,
    /// Whether the worker's pass in progress took stale directories it has
    /// not listed yet.
    relisting: bool,
    /// Whether a handle closed since pending paths were last tried.
    retry: bool,
    /// Latest change around the mount the source reported.
    notified: Option<ViewStamp>,
    /// Latest change everything written was checked against.
    processed: Option<ViewStamp>,
    /// First path the worker could not replace since the last
    /// revalidation.
    failure: Option<String>,
    stopping: bool,
}

impl ProjectorState {
    /// Records what one round of replacing pending paths came to.
    /// Records what replacing `attempted` at `path` came to: settled, and
    /// `current` written in its place.
    fn record_attempt(
        &mut self,
        path: &MountPath,
        attempted: &WrittenEntry,
        current: Option<WrittenEntry>,
    ) {
        // Superseded again meanwhile, it stays pending as that.
        if self.pending.get(path) == Some(attempted) {
            self.pending.remove(path);
            if self.relist.remove(path)
                && let Some(parent) = path.parent()
            {
                self.stale.insert(parent);
            }
        }
        if let Some(current) = current {
            // Written again meanwhile, from a later basis.
            let entry = self.written.entry(path.clone()).or_insert(current);
            if entry.basis.map(|basis| basis.stamp) < current.basis.map(|basis| basis.stamp) {
                *entry = current;
            }
        }
    }
}

impl ViewObserver for Projector {
    fn view_changed(&self, position: ViewStamp, _origin: ViewOrigin) {
        let mut state = lock_recover(&self.state);
        state.notified = state.notified.max(Some(position));
        self.changed.notify_all();
    }
}

/// What replacing one superseded path came to.
enum Replaced {
    /// Written again from the source.
    Rewritten(Box<WrittenEntry>),
    /// Deleted, or no longer the projection's.
    Released,
    /// Open elsewhere; pending until it can be replaced.
    Busy,
}

/// `path` moved from beneath `from` to beneath `to`, when it was there.
fn rebased(path: &MountPath, from: &MountPath, to: &MountPath) -> Option<MountPath> {
    projfs_path_suffix(path, from).map(|suffix| {
        suffix
            .iter()
            .fold(to.clone(), |path, component| path.child(component.clone()))
    })
}

impl Projector {
    fn new() -> Self {
        Self {
            state: Mutex::new(ProjectorState::default()),
            tree: Mutex::new(()),
            paths: KeyedLocks::default(),
            changed: Condvar::new(),
            worker: Mutex::new(None),
        }
    }

    /// Keeps what was written at `path` in view `basis`.
    fn written(
        &self,
        path: MountPath,
        facts: EntryFacts,
        owned: Option<Owned>,
        basis: Option<ReadBasis>,
    ) {
        lock_recover(&self.state).written.insert(
            path,
            WrittenEntry {
                file_id: facts.lookup.node.file_id,
                owned,
                facts: Some(facts),
                basis,
            },
        );
    }

    /// Writes everything the source holds beneath `path`, the root or a
    /// directory already created, before anything can list it.
    fn materialize(&self, projection: Projection<'_>, path: MountPath) -> Result<(), String> {
        let _tree = lock_recover(&self.tree);
        self.materialize_locked(projection, vec![path])
    }

    fn materialize_locked(
        &self,
        projection: Projection<'_>,
        directories: Vec<MountPath>,
    ) -> Result<(), String> {
        if directories.is_empty() {
            return Ok(());
        }
        // Each entry is written independently of the others: several
        // workers write a tree in a fraction of the time one does.
        let workers = std::thread::available_parallelism()
            .map_or(1, usize::from)
            .min(MATERIALIZE_WORKERS);
        let queue = MaterializeQueue::new(directories);
        let shared = SharedProjection(projection);
        std::thread::scope(|scope| {
            for _ in 1..workers {
                scope.spawn(|| self.materialize_from(&shared, &queue));
            }
            self.materialize_from(&shared, &queue);
        });
        queue.finish()
    }

    /// Writes directories from `queue` until it is drained or failed.
    fn materialize_from(&self, projection: &SharedProjection<'_>, queue: &MaterializeQueue) {
        while let Some(directory) = queue.next() {
            let written = self.materialize_directory(projection.0, directory);
            queue.done(written);
        }
    }

    /// Writes what the source holds in `directory` and records it; returns
    /// the directories found there, which are written next.
    fn materialize_directory(
        &self,
        projection: Projection<'_>,
        directory: MountPath,
    ) -> Result<Vec<MountPath>, String> {
        let basis = ReadBasis::sample(projection.source);
        let entries = match source_listing(projection.source, &directory) {
            Ok(entries) => entries,
            // Gone from the source since its parent was listed.
            Err(MountSourceError::NotFound) => return Ok(Vec::new()),
            // The source changed while it was listed: written with nothing
            // known in it, it is listed again.
            Err(MountSourceError::Stale) => {
                let mut state = lock_recover(&self.state);
                state.directories.insert(
                    directory.clone(),
                    Materialized {
                        basis: None,
                        names: HashMap::new(),
                    },
                );
                state.stale.insert(directory);
                // Written while the mount starts, off the worker's thread.
                self.changed.notify_all();
                return Ok(Vec::new());
            }
            Err(error) => return Err(format!("listing {directory:?}: {error}")),
        };
        let mut names = HashMap::with_capacity(entries.len());
        let mut found = Vec::new();
        let mut written = Vec::with_capacity(entries.len());
        let mut retry = false;
        for entry in &entries {
            let child = directory.child(entry.component.clone());
            let is_directory = entry.info.IsDirectory;
            let placed = write_entry(projection, &child, entry)?;
            if placed == Placed::Retry {
                // Left unknown, so listing again writes it.
                retry = true;
                continue;
            }
            if is_directory {
                found.push(child);
            } else if placed != Placed::Nothing {
                written.push((child, entry.facts, placed.owned()));
            }
            names.insert(entry.component.clone(), is_directory);
        }
        let mut state = lock_recover(&self.state);
        if retry {
            state.stale.insert(directory.clone());
            self.changed.notify_all();
        }
        for (path, facts, owned) in written {
            state.written.insert(
                path,
                WrittenEntry {
                    file_id: facts.lookup.node.file_id,
                    owned,
                    facts: Some(facts),
                    basis,
                },
            );
        }
        state
            .directories
            .insert(directory, Materialized { basis, names });
        Ok(found)
    }

    /// Lists `directory` again and reconciles what it holds with the names
    /// it was last written with (see [`Projector`]). A file the source
    /// lost is replaced, so deleted unless modified.
    fn reconcile(&self, projection: Projection<'_>, directory: &MountPath) -> Result<(), String> {
        let _tree = lock_recover(&self.tree);
        let Some(known) = lock_recover(&self.state)
            .directories
            .get(directory)
            .map(|materialized| materialized.names.clone())
        else {
            return Ok(());
        };
        let basis = ReadBasis::sample(projection.source);
        let entries = match source_listing(projection.source, directory) {
            Ok(entries) => entries,
            Err(MountSourceError::NotFound) => {
                self.remove_tree(projection, directory);
                return Ok(());
            }
            // The source changed while it was listed: listed again.
            Err(MountSourceError::Stale) => {
                lock_recover(&self.state).stale.insert(directory.clone());
                return Ok(());
            }
            Err(error) => return Err(format!("listing {directory:?}: {error}")),
        };
        let mut names = HashMap::with_capacity(entries.len());
        let mut created = Vec::new();
        for entry in &entries {
            let is_directory = entry.info.IsDirectory;
            let child = directory.child(entry.component.clone());
            names.insert(entry.component.clone(), is_directory);
            // A name nothing was listed at can still hold what is pending
            // there: that is released like any file written before.
            let written_before = known.get(&entry.component).copied().or_else(|| {
                lock_recover(&self.state)
                    .pending
                    .contains_key(&child)
                    .then_some(false)
            });
            match written_before.as_ref() {
                Some(&was_directory) if was_directory == is_directory => continue,
                // Now another kind: what was written for the old one goes.
                Some(&true) => self.remove_tree(projection, &child),
                Some(&false) => {
                    let (owned, released) = self.release_recorded(projection, &child);
                    if released? == Release::Busy {
                        // Tried again as pending, at the retry interval; this
                        // listing stays unrecorded, and is listed again once
                        // the old entry is gone.
                        let mut state = lock_recover(&self.state);
                        let written = state.written.remove(&child).unwrap_or(WrittenEntry {
                            file_id: FileId::from_bytes([0; 16]),
                            owned,
                            facts: None,
                            basis: None,
                        });
                        state.relist.insert(child.clone());
                        state.pending.entry(child).or_insert(written);
                        return Ok(());
                    }
                }
                None => {}
            }
            let placed = write_entry(projection, &child, entry)?;
            if placed == Placed::Retry {
                // Left unknown, so listing again writes it.
                names.remove(entry.component.as_slice());
                lock_recover(&self.state).stale.insert(directory.clone());
                continue;
            }
            if is_directory {
                created.push(child);
            } else if placed != Placed::Nothing {
                self.written(child, entry.facts, placed.owned(), basis);
            }
        }
        for (name, was_directory) in &known {
            if names.contains_key(name) {
                continue;
            }
            let child = directory.child(name.clone());
            if *was_directory {
                self.remove_tree(projection, &child);
            } else {
                // Replaced like any superseded path: deleted unless
                // modified, and tried again while held open.
                let mut state = lock_recover(&self.state);
                // Already pending, it is replaced as what it was written as.
                if !state.pending.contains_key(&child) {
                    let placeholder = state.written.remove(&child).unwrap_or(WrittenEntry {
                        file_id: FileId::from_bytes([0; 16]),
                        owned: None,
                        facts: None,
                        basis: None,
                    });
                    state.pending.insert(child, placeholder);
                }
            }
        }
        lock_recover(&self.state)
            .directories
            .insert(directory.clone(), Materialized { basis, names });
        self.materialize_locked(projection, created)
    }

    /// Releases what was written at `path`, or superseded and pending since,
    /// under the path's lock. Once released, nothing of it is recorded: a
    /// pending record left behind would release whatever is written there
    /// next.
    fn release_recorded(
        &self,
        projection: Projection<'_>,
        path: &MountPath,
    ) -> (Option<Owned>, Result<Release, String>) {
        self.paths.with(path, || {
            let owned = {
                let state = lock_recover(&self.state);
                state
                    .written
                    .get(path)
                    .or_else(|| state.pending.get(path))
                    .and_then(|written| written.owned)
            };
            let released = release(projection, path, owned);
            if released == Ok(Release::Done) {
                let mut state = lock_recover(&self.state);
                state.written.remove(path);
                state.pending.remove(path);
                state.relist.remove(path);
            }
            (owned, released)
        })
    }

    /// Deletes what was written beneath `directory` that `ProjFS` still
    /// holds untouched, and each directory left empty; forgets all of it.
    fn remove_tree(&self, projection: Projection<'_>, directory: &MountPath) {
        let mut removed = {
            let mut state = lock_recover(&self.state);
            let beneath = state
                .directories
                .keys()
                .filter(|path| projfs_path_suffix(path, directory).is_some())
                .cloned()
                .collect::<Vec<_>>();
            beneath
                .into_iter()
                .filter_map(|path| state.directories.remove(&path).map(|entry| (path, entry)))
                .collect::<Vec<_>>()
        };
        // Deepest first, so each directory holds nothing written by the time
        // it is removed.
        removed.sort_by_key(|(path, _)| std::cmp::Reverse(path.components().len()));
        let mut held = false;
        for (path, materialized) in removed {
            for (name, is_directory) in materialized.names {
                if !is_directory {
                    let child = path.child(name);
                    let (owned, released) = self.release_recorded(projection, &child);
                    if released == Ok(Release::Busy) {
                        // Held open: tried again as pending.
                        held = true;
                        let mut state = lock_recover(&self.state);
                        let written = state.written.remove(&child).unwrap_or(WrittenEntry {
                            file_id: FileId::from_bytes([0; 16]),
                            owned,
                            facts: None,
                            basis: None,
                        });
                        state.pending.entry(child).or_insert(written);
                    }
                }
            }
            if held {
                lock_recover(&self.state).vacated.insert(path);
            } else {
                remove_vacated(projection, &path);
            }
        }
    }

    /// Removes each vacated directory nothing pending is beneath any more,
    /// unless the source made it a directory again, projected once more.
    fn clear_vacated(&self, projection: Projection<'_>) {
        let mut cleared = {
            let mut state = lock_recover(&self.state);
            let state = &mut *state;
            let beneath = |directory: &MountPath, path: &MountPath| {
                projfs_path_suffix(path, directory).is_some()
            };
            let cleared = state
                .vacated
                .iter()
                .filter(|directory| !state.pending.keys().any(|path| beneath(directory, path)))
                .cloned()
                .collect::<Vec<_>>();
            state
                .vacated
                .retain(|directory| !cleared.contains(directory));
            cleared
                .into_iter()
                .filter(|directory| {
                    !state
                        .directories
                        .keys()
                        .any(|path| beneath(directory, path))
                })
                .collect::<Vec<_>>()
        };
        cleared.sort_by_key(|path| std::cmp::Reverse(path.components().len()));
        for directory in cleared {
            remove_vacated(projection, &directory);
        }
    }

    /// The files written at or beneath `from` that still hold what the
    /// source does: a rename of `from` carries them unchanged.
    fn current_beneath(&self, source: &dyn MountFilesystem, from: &MountPath) -> Vec<MountPath> {
        lock_recover(&self.state)
            .written
            .iter()
            .filter(|(path, written)| {
                projfs_path_suffix(path, from).is_some()
                    && written.facts.is_some()
                    && !written.superseded(source, path)
            })
            .map(|(path, _)| path.clone())
            .collect()
    }

    /// Follows a rename through the mount: what was written moves with it,
    /// and what it `carried` unchanged is recorded as the source now holds
    /// it, so the rename alone never has it written again.
    fn renamed(&self, from: &MountPath, to: &MountPath, carried: Carried) {
        let mut state = lock_recover(&self.state);
        let state = &mut *state;
        let moved = state
            .directories
            .keys()
            .filter_map(|path| rebased(path, from, to).map(|moved| (path.clone(), moved)))
            .collect::<Vec<_>>();
        for (old, new) in moved {
            if let Some(materialized) = state.directories.remove(&old) {
                state.directories.insert(new, materialized);
            }
        }
        for projector in [&mut state.written, &mut state.pending] {
            let moved = projector
                .keys()
                .filter_map(|path| rebased(path, from, to).map(|moved| (path.clone(), moved)))
                .collect::<Vec<_>>();
            for (old, new) in moved {
                if let Some(written) = projector.remove(&old) {
                    projector.insert(new, written);
                }
            }
        }
        let is_directory = state.directories.contains_key(to);
        if let (Some(parent), Some(name)) = (from.parent(), from.components().last())
            && let Some(materialized) = state.directories.get_mut(&parent)
        {
            materialized.names.remove(name.as_slice());
        }
        if let (Some(parent), Some(name)) = (to.parent(), to.components().last())
            && let Some(materialized) = state.directories.get_mut(&parent)
        {
            materialized.names.insert(name.clone(), is_directory);
        }
        for (path, facts) in carried.files {
            let written = state.written.remove(&path);
            let pending = state.pending.remove(&path);
            state.written.insert(
                path,
                WrittenEntry {
                    file_id: facts.lookup.node.file_id,
                    owned: written.or(pending).and_then(|written| written.owned),
                    facts: Some(facts),
                    basis: carried.basis,
                },
            );
        }
    }

    /// Has the worker list `path`'s directory again and write `path` anew
    /// unless something is there.
    fn invalidate(&self, path: &MountPath) {
        let mut state = lock_recover(&self.state);
        state.written.remove(path);
        if let (Some(parent), Some(name)) = (path.parent(), path.components().last()) {
            if let Some(materialized) = state.directories.get_mut(&parent) {
                materialized.names.remove(name.as_slice());
            }
            state.stale.insert(parent);
        }
        self.changed.notify_all();
    }

    /// Has the worker check everything written against the source: what a
    /// source that reports none of its changes needs before anyone looks.
    fn invalidate_all(&self) {
        let mut state = lock_recover(&self.state);
        let state = &mut *state;
        state.stale.extend(state.directories.keys().cloned());
        let written = std::mem::take(&mut state.written);
        state.pending.extend(written);
        self.changed.notify_all();
    }

    /// Starts the worker that keeps the projection at `root` in `context`
    /// current.
    fn attach(
        self: &Arc<Self>,
        source: Arc<dyn MountFilesystem>,
        context: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT,
        root: PathBuf,
        captures: Arc<Mutex<PostOperationFailures>>,
    ) -> Result<(), NativeMountError> {
        let projector = Arc::clone(self);
        let observed = Arc::clone(&source);
        let context = SendContext(context);
        let worker = std::thread::Builder::new()
            .name("acyclic-projfs-projector".to_owned())
            .spawn(move || {
                let context = context;
                projector.run(Projection {
                    source: source.as_ref(),
                    context: context.0,
                    root: &root,
                    captures: &captures,
                    starting: false,
                });
            })
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        *lock_recover(&self.worker) = Some(worker);
        let projector = Arc::downgrade(self);
        let observer: std::sync::Weak<dyn ViewObserver> = projector;
        observed.observe_view(observer);
        Ok(())
    }

    fn run(&self, projection: Projection<'_>) {
        let source = projection.source;
        let mut state = lock_recover(&self.state);
        loop {
            // Waits for a change, or, while paths are pending, for a
            // handle to close or the retry interval, whichever comes first.
            loop {
                if state.stopping {
                    return;
                }
                if state.processed < state.notified || state.retry || !state.stale.is_empty() {
                    break;
                }
                if state.pending.is_empty() {
                    state = self
                        .changed
                        .wait(state)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                } else {
                    let (next, waited) = self
                        .changed
                        .wait_timeout(state, HELD_RETRY)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    state = next;
                    if waited.timed_out() {
                        break;
                    }
                }
            }
            state.retry = false;
            state.relisting = !state.stale.is_empty();
            let through = state.notified;
            // Directories whose listing may have changed, parents first.
            let mut listings = state
                .directories
                .iter()
                .filter(|(path, directory)| {
                    directory
                        .basis
                        .is_none_or(|basis| !basis.still_describes(source, path, None))
                })
                .map(|(path, _)| path.clone())
                .collect::<Vec<_>>();
            listings.extend(state.stale.drain());
            listings.sort_by_key(|path| path.components().len());
            listings.dedup();
            let superseded = state
                .written
                .iter()
                .filter(|(path, written)| written.superseded(source, path))
                .map(|(path, written)| (path.clone(), *written))
                .collect::<Vec<_>>();
            let mut unconfirmed = Vec::new();
            for (path, written) in superseded {
                state.written.remove(&path);
                if written.unconfirmed(source, &path) {
                    unconfirmed.push((path, written));
                } else {
                    state.pending.insert(path, written);
                }
            }
            drop(state);
            self.confirm(source, unconfirmed);
            let mut failure = None;
            for directory in listings {
                if let Err(error) = self.reconcile(projection, &directory) {
                    failure.get_or_insert(error);
                }
            }
            self.replace_pending(projection, &mut failure);
            self.clear_vacated(projection);
            state = lock_recover(&self.state);
            state.relisting = false;
            state.processed = state.processed.max(through);
            if let Some(failure) = failure {
                state.failure.get_or_insert(failure);
            }
            self.changed.notify_all();
        }
    }

    /// Replaces every pending path, each under its path's lock with
    /// the record current there, and records the outcome.
    fn replace_pending(&self, projection: Projection<'_>, failure: &mut Option<String>) {
        // Attempted as a snapshot: each stays pending until its outcome is
        // recorded.
        let mut attempted = lock_recover(&self.state)
            .pending
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        attempted.sort_by_key(|path| std::cmp::Reverse(path.components().len()));
        for path in attempted {
            // Replaced and recorded under the path's lock, so no conversion
            // comes between: made an ordinary file since, it is replaced as
            // that.
            let outcome = self.paths.with(&path, || {
                let Some(written) = lock_recover(&self.state).pending.get(&path).copied() else {
                    return Ok(());
                };
                let replaced = replace_written(projection, &path, &written);
                let mut state = lock_recover(&self.state);
                match replaced {
                    Ok(Replaced::Rewritten(current)) => {
                        state.record_attempt(&path, &written, Some(*current));
                        Ok(())
                    }
                    Ok(Replaced::Released) => {
                        state.record_attempt(&path, &written, None);
                        Ok(())
                    }
                    Ok(Replaced::Busy) => Ok(()),
                    Err(error) => {
                        state.record_attempt(&path, &written, None);
                        Err(error)
                    }
                }
            });
            if let Err(error) = outcome {
                failure.get_or_insert(error);
            }
        }
    }

    /// Reads each path only unconfirmed changes superseded again:
    /// one the source would still write as it is holds from now on, and
    /// any other is replaced.
    fn confirm(&self, source: &dyn MountFilesystem, unconfirmed: Vec<(MountPath, WrittenEntry)>) {
        for (path, written) in unconfirmed {
            let basis = ReadBasis::sample(source);
            let current = source.lookup_pinned(&path);
            let mut state = lock_recover(&self.state);
            match current {
                Ok(Some((lookup, pin)))
                    if written
                        .facts
                        .is_some_and(|facts| facts.describe(&EntryFacts { lookup, pin })) =>
                {
                    // Written again meanwhile, from a later read.
                    state
                        .written
                        .entry(path)
                        .or_insert(WrittenEntry { basis, ..written });
                }
                _ => {
                    state.pending.insert(path, written);
                }
            }
        }
    }

    /// Tries the paths held open again, as one of their handles
    /// closed.
    fn retry(&self) {
        let mut state = lock_recover(&self.state);
        if !state.pending.is_empty() {
            state.retry = true;
            self.changed.notify_all();
        }
    }

    /// Waits until every change reported so far has been served and no
    /// superseded path or invalidated directory remains; fails,
    /// naming one, when one is still held open [`HELD_SETTLE_LIMIT`]
    /// after every change was served.
    fn settle(&self) -> Result<(), NativeMountError> {
        let mut state = lock_recover(&self.state);
        let target = state.notified;
        let mut deadline = None;
        while !state.stopping
            && (state.processed < target
                || state.relisting
                || !state.stale.is_empty()
                || !state.pending.is_empty()
                || !state.vacated.is_empty())
        {
            if state.processed < target || state.relisting || !state.stale.is_empty() {
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                continue;
            }
            let deadline =
                *deadline.get_or_insert_with(|| std::time::Instant::now() + HELD_SETTLE_LIMIT);
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                let held = state.pending.keys().next().cloned();
                return Err(NativeMountError::Driver(format!(
                    "a superseded placeholder is held open and cannot be replaced: {held:?}"
                )));
            }
            state = self
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
        state
            .failure
            .take()
            .map_or(Ok(()), |failure| Err(NativeMountError::Driver(failure)))
    }

    /// Signals the existing worker and keeps its join handle in custody if
    /// cancellation arrives before completion. Waiting allocates no envelope.
    fn finish_measured(&self, work: &mut DemandWork) -> Result<(), NativeMountError> {
        work.check().map_err(detach_work_error)?;
        lock_recover(&self.state).stopping = true;
        self.changed.notify_all();
        loop {
            work.check().map_err(detach_work_error)?;
            {
                let mut worker = lock_recover(&self.worker);
                if worker.as_ref().is_none_or(std::thread::JoinHandle::is_finished) {
                    if let Some(worker) = worker.take() {
                        let _ = worker.join();
                    }
                    return Ok(());
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}

/// Writes `entry` at `path`: an ordinary directory for a directory, an
/// ordinary file for a file, and a placeholder for a link. Whatever is
/// already there (the user's, or written before) stays, and is reported as
/// not written.
fn write_entry(
    projection: Projection<'_>,
    path: &MountPath,
    entry: &ProjectedEntry,
) -> Result<Placed, String> {
    let host = host_relative_path(path).map_err(|error| error.to_string())?;
    if entry.info.IsDirectory {
        return match std::fs::create_dir(projection.root.join(&host)) {
            Ok(()) => Ok(Placed::Unowned),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::AlreadyExists
                        | std::io::ErrorKind::NotFound
                        | std::io::ErrorKind::NotADirectory
                ) =>
            {
                Ok(Placed::Nothing)
            }
            Err(error) => Err(format!("creating directory {path:?}: {error}")),
        };
    }
    if entry.symlink_target.is_none() {
        return write_ordinary(projection, path, &host, entry, None);
    }
    let mut placeholder = link_placeholder(entry.info);
    if entry.directory_link {
        placeholder.FileBasicInfo.IsDirectory = true;
        placeholder.FileBasicInfo.FileAttributes |= FILE_ATTRIBUTE_DIRECTORY.0;
    }
    let symlink = match entry.symlink_target.as_deref().map(symlink_extended) {
        Some(Some(target)) => Some(target),
        Some(None) => return Err(format!("link at {path:?} is unrepresentable")),
        None => None,
    };
    let relative = HSTRING::from(host.as_os_str());
    let size = u32::try_from(size_of::<PRJ_PLACEHOLDER_INFO>()).unwrap_or(u32::MAX);
    // SAFETY: the name, placeholder and link target outlive these
    // synchronous calls on a live context.
    let result = unsafe {
        if let Some((_target, extended)) = symlink.as_ref() {
            PrjWritePlaceholderInfo2(
                projection.context,
                &relative,
                &raw const placeholder,
                size,
                Some(extended),
            )
        } else {
            PrjWritePlaceholderInfo(projection.context, &relative, &raw const placeholder, size)
        }
    };
    match result {
        Ok(()) => Ok(symlink.map_or(Placed::Unowned, |(target, _)| {
            Placed::Owned(Owned::Link(link_digest(target.iter().copied())))
        })),
        // Something is there already, or its directory is no directory:
        // what the user made stays.
        Err(error)
            if [
                HR_ALREADY_EXISTS,
                HR_FILE_EXISTS,
                HR_FILE_NOT_FOUND,
                HR_PATH_NOT_FOUND,
                HR_DIRECTORY,
            ]
            .contains(&error.code()) =>
        {
            Ok(Placed::Nothing)
        }
        // `ReFS` refuses link placeholders: an ordinary link stands in.
        Err(error) if error.code() == HR_NOT_SUPPORTED && symlink.is_some() => {
            let target = symlink.as_ref().map(|(target, _)| target);
            write_link(projection, path, &host, target, entry.directory_link)
        }
        Err(error) => Err(format!(
            "writing placeholder {path:?}: {}",
            driver_error(&error)
        )),
    }
}

/// Writes an ordinary symbolic link to `target` at `path`, for a volume that
/// refuses link placeholders. Windows lets an unprivileged process create
/// links only in Developer Mode; elsewhere the projection cannot hold one.
fn write_link(
    projection: Projection<'_>,
    path: &MountPath,
    host: &std::path::Path,
    target: Option<&HSTRING>,
    directory: bool,
) -> Result<Placed, String> {
    use windows::Win32::Storage::FileSystem::{
        CreateSymbolicLinkW, SYMBOLIC_LINK_FLAG_ALLOW_UNPRIVILEGED_CREATE,
        SYMBOLIC_LINK_FLAG_DIRECTORY, SYMBOLIC_LINK_FLAGS,
    };

    let target = target.ok_or_else(|| format!("link at {path:?} has no target"))?;
    let name = HSTRING::from(projection.root.join(host).as_os_str());
    let mut flags = SYMBOLIC_LINK_FLAG_ALLOW_UNPRIVILEGED_CREATE.0;
    if directory {
        flags |= SYMBOLIC_LINK_FLAG_DIRECTORY.0;
    }
    // SAFETY: both names outlive this synchronous call.
    let created = unsafe { CreateSymbolicLinkW(&name, target, SYMBOLIC_LINK_FLAGS(flags)) };
    if !created {
        let error = std::io::Error::last_os_error();
        return match error.kind() {
            // Something is there already, or its directory is gone.
            std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::NotFound => Ok(Placed::Nothing),
            _ => Err(format!(
                "writing link {path:?}: {error} (this volume has no link projector, so \
                 projecting a link needs the right to create one: Developer Mode)"
            )),
        };
    }
    Ok(Placed::Owned(Owned::Link(link_digest(
        target.iter().copied(),
    ))))
}

/// A digest of a link's target, which says whether a link is still the
/// one written.
fn link_digest(target: impl IntoIterator<Item = u16>) -> u64 {
    use std::hash::{Hash as _, Hasher as _};

    let mut hasher = std::hash::DefaultHasher::new();
    target.into_iter().for_each(|unit| unit.hash(&mut hasher));
    hasher.finish()
}

/// Deletes what the projection wrote at `path` unless the user made it
/// their own: what `ProjFS` manages, or what it does not.
fn release(
    projection: Projection<'_>,
    path: &MountPath,
    owned: Option<Owned>,
) -> Result<Release, String> {
    match owned {
        None => delete_unowned(projection, path),
        Some(Owned::Link(digest)) => release_link(projection, path, digest),
        Some(Owned::File(state)) => release_file(projection, path, state),
    }
}

/// Deletes the ordinary file the projection made at `path` while it is
/// still `state`. It is opened so no writer can hold it, checked on that
/// handle, and deleted through it: whatever takes its name meanwhile, or
/// writes it, is the user's.
fn release_file(
    projection: Projection<'_>,
    path: &MountPath,
    state: HostWindowsMetadata,
) -> Result<Release, String> {
    // Its metadata cannot prove its content unchanged; an edit the source
    // has not captured yet can. Once captured, the source holds it.
    if has_pending_capture(projection.captures, path) {
        return Ok(Release::Busy);
    }
    let host = host_relative_path(path).map_err(|error| error.to_string())?;
    match delete_file_while(&projection.root.join(&host), state) {
        Ok(true) => {}
        Ok(false) => return Ok(Release::Done),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Release::Done),
        Err(error) if error.raw_os_error() == Some(SHARING_VIOLATION) => {
            return Ok(Release::Busy);
        }
        Err(error) => return Err(format!("removing {path:?}: {error}")),
    }
    clear_tombstone(projection, path, &HSTRING::from(host.as_os_str()))
}

/// Deletes the link to the target with `digest` the projection wrote at
/// `path` unless the user replaced it.
fn release_link(
    projection: Projection<'_>,
    path: &MountPath,
    digest: u64,
) -> Result<Release, String> {
    let host = host_relative_path(path).map_err(|error| error.to_string())?;
    let relative = HSTRING::from(host.as_os_str());
    // An untouched link placeholder is the projection's own.
    match prj_delete(projection, &relative, PRJ_UPDATE_ALLOW_READ_ONLY) {
        Ok(()) => return Ok(Release::Done),
        Err(error) if error.code() == HR_SHARING_VIOLATION => return Ok(Release::Busy),
        // Gone: at most a tombstone of an earlier try is left, cleared below.
        Err(error) if [HR_FILE_NOT_FOUND, HR_PATH_NOT_FOUND].contains(&error.code()) => {}
        // Read since, it is a full file, as one the user made would be; or
        // an ordinary link (`ReFS`), which `ProjFS` does not manage.
        Err(error) if error.code() == HR_VIRTUALIZATION_INVALID_OPERATION => {
            match delete_link(&projection.root.join(&host), digest) {
                Ok(true) => {}
                // Replaced, or pointed elsewhere: the user's.
                Ok(false) => return Ok(Release::Done),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(Release::Done);
                }
                Err(error) if error.raw_os_error() == Some(SHARING_VIOLATION) => {
                    return Ok(Release::Busy);
                }
                Err(error) => return Err(format!("removing link {path:?}: {error}")),
            }
        }
        Err(error) => {
            return Err(format!("deleting link {path:?}: {}", driver_error(&error)));
        }
    }
    clear_tombstone(projection, path, &relative)
}

/// Clears the tombstone deleting what the projection wrote at `path`
/// directly leaves, which would hide what is written there next. Only a
/// tombstone goes: whatever the user made since stays.
fn clear_tombstone(
    projection: Projection<'_>,
    path: &MountPath,
    relative: &HSTRING,
) -> Result<Release, String> {
    match prj_delete(projection, relative, PRJ_UPDATE_ALLOW_TOMBSTONE) {
        Ok(()) => Ok(Release::Done),
        Err(error) if error.code() == HR_SHARING_VIOLATION => Ok(Release::Busy),
        Err(error)
            if [
                HR_FILE_NOT_FOUND,
                HR_PATH_NOT_FOUND,
                HR_VIRTUALIZATION_INVALID_OPERATION,
            ]
            .contains(&error.code()) =>
        {
            Ok(Release::Done)
        }
        Err(error) => Err(format!(
            "clearing link tombstone {path:?}: {}",
            driver_error(&error)
        )),
    }
}

/// `ERROR_SHARING_VIOLATION`, as an I/O error reports it.
const SHARING_VIOLATION: i32 = 32;

/// `PrjDeleteFile` at `relative`, allowed what `update` allows.
fn prj_delete(
    projection: Projection<'_>,
    relative: &HSTRING,
    update: PRJ_UPDATE_TYPES,
) -> windows::core::Result<()> {
    // SAFETY: the name outlives this synchronous call on a live context.
    unsafe { PrjDeleteFile(projection.context, relative, Some(update), None) }
}

/// The ordinary file `file` names as the projection compares it: its
/// identity, size, attributes and times. The archive bit is not the file's
/// own: the file system sets it at every change, the projection's
/// included, and drops the normal bit as it does. An edit is told by what
/// it changes, and by the capture it awaits.
fn handle_state(file: &std::fs::File) -> std::io::Result<HostWindowsMetadata> {
    use std::os::windows::fs::MetadataExt as _;

    let metadata = file.metadata()?;
    let ticks = |value: u64| i64::try_from(value).map_err(std::io::Error::other);
    Ok(HostWindowsMetadata {
        identity: crate::NativeRootIdentity::from_file(file)?,
        links: None,
        size: metadata.file_size(),
        attributes: metadata.file_attributes()
            & !(FILE_ATTRIBUTE_ARCHIVE.0 | FILE_ATTRIBUTE_NORMAL.0),
        created: ticks(metadata.creation_time())?,
        modified: ticks(metadata.last_write_time())?,
    })
}

/// Reads or sets the basic information of the file `file` names.
fn basic_information(
    file: &std::fs::File,
    set: Option<&windows::Win32::Storage::FileSystem::FILE_BASIC_INFO>,
) -> std::io::Result<windows::Win32::Storage::FileSystem::FILE_BASIC_INFO> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::{
        FILE_BASIC_INFO, FileBasicInfo, GetFileInformationByHandleEx, SetFileInformationByHandle,
    };

    let handle = HANDLE(file.as_raw_handle());
    let size = u32::try_from(size_of::<FILE_BASIC_INFO>()).unwrap_or(u32::MAX);
    if let Some(set) = set {
        // SAFETY: a live handle and a correctly sized input structure.
        unsafe {
            SetFileInformationByHandle(handle, FileBasicInfo, std::ptr::from_ref(set).cast(), size)
        }?;
        return Ok(*set);
    }
    let mut information = FILE_BASIC_INFO::default();
    // SAFETY: a live handle and a correctly sized output structure.
    unsafe {
        GetFileInformationByHandleEx(handle, FileBasicInfo, (&raw mut information).cast(), size)
    }?;
    Ok(information)
}

/// Removes the directory the projection made at `path` if nothing is left
/// in it: what the user left in it keeps it.
fn remove_vacated(projection: Projection<'_>, path: &MountPath) {
    if let Ok(host) = host_relative_path(path)
        && !host.as_os_str().is_empty()
    {
        let _ = std::fs::remove_dir(projection.root.join(host));
    }
}

/// Writes `entry`, a regular file, at `path` as an ordinary file: the
/// content the listing named, its times and attributes, all under a staged
/// name, then given its own name at once, so no reader sees it partly
/// written. With `replacing`, it takes the place of the file the projection
/// left there in that state, and only while it is still that and holds no
/// edit the source has not captured; else what is there is the user's
/// ([`Placed::Nothing`]) or is held open ([`Placed::Retry`]). `ProjFS` drops
/// a time set through a writable handle to a placeholder it manages, so no
/// file of the projection is one.
fn write_ordinary(
    projection: Projection<'_>,
    path: &MountPath,
    host: &std::path::Path,
    entry: &ProjectedEntry,
    replacing: Option<HostWindowsMetadata>,
) -> Result<Placed, String> {
    let failed = |error: std::io::Error| format!("writing {path:?}: {error}");
    let (Some(parent), Some(name)) = (host.parent(), host.file_name()) else {
        return Err(format!("{path:?} names no file"));
    };
    let target = projection.root.join(host);
    // Named already: the user's, or what the source moved there.
    if replacing.is_none() && std::fs::symlink_metadata(&target).is_ok() {
        return Ok(Placed::Nothing);
    }
    let directory = match cap_std::fs::Dir::open_ambient_dir(
        projection.root.join(parent),
        cap_std::ambient_authority(),
    ) {
        Ok(directory) => directory,
        // Its directory is gone, or no directory.
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
            ) =>
        {
            return Ok(Placed::Nothing);
        }
        Err(error) => return Err(failed(error)),
    };
    if projection.starting && replacing.is_none() {
        return write_directly(projection, path, &target, entry);
    }
    // Staged beside the projection, so no listing of it shows a staged name.
    let staged = crate::native_host::StagedWindowsFile::create(staging_directory(projection.root)?)
        .map_err(failed)?;
    let mut file = staged.open_writer(false).map_err(failed)?;
    if !write_listed_content(projection, path, entry, &mut file)? {
        return Ok(Placed::Retry);
    }
    basic_information(&file, Some(&ordinary_basic_information(entry))).map_err(failed)?;
    let published = match replacing {
        None => staged.publish_in(&directory, name),
        Some(state) => {
            // What the user made, or edited without its capture yet, stays.
            if has_pending_capture(projection.captures, path) {
                return Ok(Placed::Retry);
            }
            match held_while(&target, state) {
                Ok(Some(_held)) => staged.publish_over_in(&directory, name),
                Ok(None) => return Ok(Placed::Nothing),
                Err(error) => Err(error),
            }
        }
    };
    match published {
        Ok(()) => handle_state(&file)
            .map(|state| Placed::Owned(Owned::File(state)))
            .map_err(failed),
        // Something is there already, or its directory is gone.
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::NotFound
            ) =>
        {
            Ok(Placed::Nothing)
        }
        Err(error) if error.raw_os_error() == Some(SHARING_VIOLATION) => Ok(Placed::Retry),
        Err(error) => Err(failed(error)),
    }
}

/// The times and attributes `entry` names, for its ordinary file. A time
/// left zero is not set: the moment of writing stays.
fn ordinary_basic_information(
    entry: &ProjectedEntry,
) -> windows::Win32::Storage::FileSystem::FILE_BASIC_INFO {
    use windows::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_RECALL_ON_OPEN, FILE_ATTRIBUTE_REPARSE_POINT, FILE_BASIC_INFO,
    };

    let attributes = entry.info.FileAttributes
        & !(FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS.0
            | FILE_ATTRIBUTE_RECALL_ON_OPEN.0
            | FILE_ATTRIBUTE_REPARSE_POINT.0);
    FILE_BASIC_INFO {
        CreationTime: entry.info.CreationTime,
        LastAccessTime: entry.info.LastAccessTime,
        LastWriteTime: entry.info.LastWriteTime,
        ChangeTime: entry.info.ChangeTime,
        FileAttributes: if attributes == 0 {
            FILE_ATTRIBUTE_NORMAL.0
        } else {
            attributes
        },
    }
}

/// Writes `entry` at `target` directly, while the mount is starting.
fn write_directly(
    projection: Projection<'_>,
    path: &MountPath,
    target: &std::path::Path,
    entry: &ProjectedEntry,
) -> Result<Placed, String> {
    let failed = |error: std::io::Error| format!("writing {path:?}: {error}");
    let mut file = match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(target)
    {
        Ok(file) => file,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::AlreadyExists
                    | std::io::ErrorKind::NotFound
                    | std::io::ErrorKind::NotADirectory
            ) =>
        {
            return Ok(Placed::Nothing);
        }
        Err(error) => return Err(failed(error)),
    };
    let written = write_listed_content(projection, path, entry, &mut file).and_then(|complete| {
        if !complete {
            return Ok(Placed::Retry);
        }
        basic_information(&file, Some(&ordinary_basic_information(entry)))
            .and_then(|_| handle_state(&file))
            .map(|state| Placed::Owned(Owned::File(state)))
            .map_err(failed)
    });
    if !matches!(written, Ok(Placed::Owned(_))) {
        drop(file);
        let _ = std::fs::remove_file(target);
    }
    written
}

/// The directory beside the projection at `root` where its files are
/// written before they take their names in it; made on first use. Only a
/// plain directory on the projection's volume serves, checked on the handle
/// it is then used through: a reparse point would lead elsewhere, and hard
/// links cross no other volume.
fn staging_directory(root: &std::path::Path) -> Result<cap_std::fs::Dir, String> {
    use std::os::windows::fs::{MetadataExt as _, OpenOptionsExt as _};
    use windows::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
    };

    let staging = staging_path(root);
    match std::fs::create_dir(&staging) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(format!("making {}: {error}", staging.display())),
    }
    let open = |path: &std::path::Path| {
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags((FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT).0)
            .open(path)
    };
    let failed = |error: std::io::Error| format!("opening {}: {error}", staging.display());
    let directory = open(&staging).map_err(failed)?;
    let metadata = directory.metadata().map_err(failed)?;
    let volume =
        |file: &std::fs::File| crate::NativeRootIdentity::from_file(file).map(|id| id.device);
    let same_volume = volume(&directory).map_err(failed)?
        == volume(&open(root).map_err(failed)?).map_err(failed)?;
    if !metadata.is_dir()
        || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0
        || !same_volume
    {
        return Err(format!(
            "{} is not a directory this projection can stage in",
            staging.display()
        ));
    }
    Ok(cap_std::fs::Dir::from_std_file(directory))
}

/// Where the projection at `root` stages its files: a hidden sibling on
/// the same volume, which hard links can cross.
fn staging_path(root: &std::path::Path) -> PathBuf {
    let name = root
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    root.with_file_name(format!(".{name}.acyclic-staging"))
}

/// Writes the content the listing named for `entry` at `path` to `file`;
/// `false` when the source changed or lost it since.
fn write_listed_content(
    projection: Projection<'_>,
    path: &MountPath,
    entry: &ProjectedEntry,
    file: &mut std::fs::File,
) -> Result<bool, String> {
    use std::io::Write as _;

    let size = u64::try_from(entry.info.FileSize).unwrap_or(0);
    let mut failure = None;
    let mut delivered = 0_u64;
    let mut sink = |_offset: u64, bytes: bytes::Bytes| {
        delivered += u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        file.write_all(&bytes).map_err(|error| {
            failure = Some(error);
            MountSourceError::Engine("writing the projected file failed".to_owned())
        })
    };
    let read = match entry.facts.pin {
        Some(pin) => {
            projection
                .source
                .read_pinned(path, pin, 0, size, WRITE_CHUNK_BYTES, &mut sink)
        }
        None => (|| {
            let mut offset = 0;
            while offset < size {
                let count = u32::try_from(size - offset)
                    .unwrap_or(u32::MAX)
                    .min(WRITE_CHUNK_BYTES);
                let bytes = projection.source.read_range(path, offset, count)?;
                if bytes.is_empty() {
                    return Err(MountSourceError::Stale);
                }
                offset += u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                sink(offset, bytes)?;
            }
            Ok(())
        })(),
    };
    match (read, failure) {
        (_, Some(error)) => Err(format!("writing {path:?}: {error}")),
        // Shorter or longer than listed: not the content the listing named.
        (Ok(()), None) => Ok(delivered == size),
        (Err(MountSourceError::Stale | MountSourceError::NotFound), None) => Ok(false),
        (Err(error), None) => Err(format!("reading {path:?}: {error}")),
    }
}

/// The ordinary file at `path`, held so no writer can share it, if it is
/// still `state`; `None` when something else is there.
fn held_while(
    path: &std::path::Path,
    state: HostWindowsMetadata,
) -> std::io::Result<Option<std::fs::File>> {
    use std::os::windows::fs::OpenOptionsExt as _;
    use windows::Win32::Storage::FileSystem::{
        DELETE, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_READ_ATTRIBUTES,
        FILE_SHARE_DELETE, FILE_SHARE_READ,
    };

    let file = match std::fs::OpenOptions::new()
        .access_mode(DELETE.0 | FILE_READ_ATTRIBUTES.0)
        .share_mode((FILE_SHARE_READ | FILE_SHARE_DELETE).0)
        .custom_flags((FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS).0)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    Ok((handle_state(&file)? == state).then_some(file))
}

/// Deletes the symbolic link at `path` if it points to the target with
/// `digest`. The link itself is opened, not followed, and the object checked
/// is the one deleted, whatever takes its name meanwhile. `false` when
/// something else is there.
fn delete_link(path: &std::path::Path, digest: u64) -> std::io::Result<bool> {
    use std::os::windows::fs::OpenOptionsExt as _;
    use windows::Win32::Storage::FileSystem::{
        DELETE, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_READ_ATTRIBUTES,
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };

    let link = std::fs::OpenOptions::new()
        .access_mode(DELETE.0 | FILE_READ_ATTRIBUTES.0)
        .share_mode((FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE).0)
        .custom_flags((FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS).0)
        .open(path)?;
    let target = reparse_data(&link)?.and_then(|data| symbolic_link_target(&data));
    if target.is_none_or(|target| link_digest(target) != digest) {
        return Ok(false);
    }
    delete_through(&link)?;
    Ok(true)
}

/// Deletes the ordinary file at `path` if it is still `state`, opened so no
/// writer can hold it and not followed; `false` when something else is there.
fn delete_file_while(path: &std::path::Path, state: HostWindowsMetadata) -> std::io::Result<bool> {
    let Some(file) = held_while(path, state)? else {
        return Ok(false);
    };
    delete_through(&file)?;
    Ok(true)
}

/// Deletes the file `file` names, through that handle. A POSIX delete takes
/// the name at once, also while another process holds it; delete-on-close
/// serves where that is refused.
fn delete_through(file: &std::fs::File) -> std::io::Result<()> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::{
        FILE_DISPOSITION_FLAG_DELETE, FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE,
        FILE_DISPOSITION_FLAG_POSIX_SEMANTICS, FILE_DISPOSITION_INFO, FILE_DISPOSITION_INFO_EX,
        FILE_DISPOSITION_INFO_EX_FLAGS, FileDispositionInfo, FileDispositionInfoEx,
        SetFileInformationByHandle,
    };

    let handle = HANDLE(file.as_raw_handle());
    // A POSIX delete takes the name at once, also while another process
    // holds the link; delete-on-close serves where that is refused.
    let posix = FILE_DISPOSITION_INFO_EX {
        Flags: FILE_DISPOSITION_INFO_EX_FLAGS(
            FILE_DISPOSITION_FLAG_DELETE.0
                | FILE_DISPOSITION_FLAG_POSIX_SEMANTICS.0
                | FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE.0,
        ),
    };
    let on_close = FILE_DISPOSITION_INFO { DeleteFile: true };
    // SAFETY: a live handle opened with delete access, and correctly sized
    // dispositions that outlive these synchronous calls.
    unsafe {
        SetFileInformationByHandle(
            handle,
            FileDispositionInfoEx,
            (&raw const posix).cast(),
            u32::try_from(size_of::<FILE_DISPOSITION_INFO_EX>()).unwrap_or(u32::MAX),
        )
        .or_else(|_| {
            SetFileInformationByHandle(
                handle,
                FileDispositionInfo,
                (&raw const on_close).cast(),
                u32::try_from(size_of::<FILE_DISPOSITION_INFO>()).unwrap_or(u32::MAX),
            )
        })
    }?;
    Ok(())
}

/// The target a symbolic link's reparse `data` names, as `read_link` reads
/// it; `None` for any other reparse point.
fn symbolic_link_target(data: &[u8]) -> Option<Vec<u16>> {
    const SYMLINK: u32 = 0xA000_000C;
    const PATH_BUFFER: usize = 20;
    let field = |at: usize| {
        data.get(at..)
            .and_then(<[u8]>::first_chunk)
            .map(|bytes| usize::from(u16::from_le_bytes(*bytes)))
    };
    if u32::from_le_bytes(*data.first_chunk()?) != SYMLINK {
        return None;
    }
    let (offset, length) = (field(8)?, field(10)?);
    let name = data.get(PATH_BUFFER + offset..PATH_BUFFER + offset + length)?;
    let mut units = decode_utf16_name(name)?;
    // An absolute target is stored as an NT path.
    let unc = r"\??\UNC\".encode_utf16().collect::<Vec<_>>();
    let nt = r"\??\".encode_utf16().collect::<Vec<_>>();
    if units.starts_with(&unc) {
        units.splice(..unc.len(), r"\\".encode_utf16());
    } else if units.starts_with(&nt) {
        units.drain(..nt.len());
    }
    Some(units)
}

/// Deletes what `ProjFS` manages at `path` unless the user modified it.
fn delete_unowned(projection: Projection<'_>, path: &MountPath) -> Result<Release, String> {
    let host = host_relative_path(path).map_err(|error| error.to_string())?;
    let relative = HSTRING::from(host.as_os_str());
    match prj_delete(projection, &relative, PRJ_UPDATE_ALLOW_READ_ONLY) {
        Ok(()) => Ok(Release::Done),
        Err(error) if error.code() == HR_SHARING_VIOLATION => Ok(Release::Busy),
        Err(error)
            if [
                HR_FILE_NOT_FOUND,
                HR_PATH_NOT_FOUND,
                HR_VIRTUALIZATION_INVALID_OPERATION,
                HR_DIRECTORY_NOT_EMPTY,
            ]
            .contains(&error.code()) =>
        {
            Ok(Release::Done)
        }
        Err(error) => Err(format!(
            "deleting placeholder {path:?}: {}",
            driver_error(&error)
        )),
    }
}

/// Writes what is at `path` again from the source, or deletes it
/// where the source names nothing there any more (or a directory, which the
/// directory's reconcile writes).
fn replace_written(
    projection: Projection<'_>,
    path: &MountPath,
    written: &WrittenEntry,
) -> Result<Replaced, String> {
    let source = projection.source;
    let relative = HSTRING::from(
        host_relative_path(path)
            .map_err(|error| error.to_string())?
            .as_os_str(),
    );
    let basis = ReadBasis::sample(source);
    let current = match source.lookup_pinned(path) {
        Ok(found) => found,
        Err(MountSourceError::NotFound) => None,
        Err(error) => return Err(error.to_string()),
    };
    rewrite(projection, path, &relative, written.owned, current, basis)
}

/// Writes what the source holds at `path` now in place of what was written
/// there: an ordinary file replaced in one step, anything else removed,
/// unless the user made it their own, and written anew.
fn rewrite(
    projection: Projection<'_>,
    path: &MountPath,
    relative: &HSTRING,
    owned: Option<Owned>,
    current: Option<(MountLookup, Option<MountContentPin>)>,
    basis: Option<ReadBasis>,
) -> Result<Replaced, String> {
    if let Some(Owned::File(state)) = owned
        && let Some((lookup, pin)) = current
        && lookup.node.kind == MountNodeKind::Regular
    {
        return replace_ordinary(projection, path, state, lookup, pin, basis);
    }
    if owned.is_some() {
        if release(projection, path, owned)? == Release::Busy {
            return Ok(Replaced::Busy);
        }
    } else {
        // SAFETY: the name outlives this synchronous call on a live context.
        match unsafe {
            PrjDeleteFile(
                projection.context,
                relative,
                Some(PRJ_UPDATE_ALLOW_READ_ONLY),
                None,
            )
        } {
            Ok(()) => {}
            Err(error) if [HR_FILE_NOT_FOUND, HR_PATH_NOT_FOUND].contains(&error.code()) => {}
            Err(error) if error.code() == HR_SHARING_VIOLATION => return Ok(Replaced::Busy),
            // Authored since: the user's.
            Err(error)
                if [HR_VIRTUALIZATION_INVALID_OPERATION, HR_DIRECTORY_NOT_EMPTY]
                    .contains(&error.code()) =>
            {
                return Ok(Replaced::Released);
            }
            Err(error) => return Err(driver_error(&error).to_string()),
        }
    }
    let Some((lookup, pin)) =
        current.filter(|(lookup, _)| lookup.node.kind != MountNodeKind::Directory)
    else {
        return Ok(Replaced::Released);
    };
    let (Some(parent), Some(component)) = (path.parent(), path.components().last()) else {
        return Ok(Replaced::Released);
    };
    let Some(entry) = projected_entry(projection.source, &parent, component.clone(), lookup, pin)
        .map_err(|error| error.to_string())?
    else {
        return Ok(Replaced::Released);
    };
    Ok(match write_entry(projection, path, &entry)? {
        Placed::Nothing => Replaced::Released,
        Placed::Retry => Replaced::Busy,
        placed => Replaced::Rewritten(Box::new(WrittenEntry {
            file_id: lookup.node.file_id,
            owned: placed.owned(),
            facts: Some(entry.facts),
            basis,
        })),
    })
}

/// Puts what the source now holds at `path` in place of the ordinary file
/// the projection left there in `state`, in one step no reader sees between.
fn replace_ordinary(
    projection: Projection<'_>,
    path: &MountPath,
    state: HostWindowsMetadata,
    lookup: MountLookup,
    pin: Option<MountContentPin>,
    basis: Option<ReadBasis>,
) -> Result<Replaced, String> {
    let (Some(parent), Some(component)) = (path.parent(), path.components().last()) else {
        return Ok(Replaced::Released);
    };
    let Some(entry) = projected_entry(projection.source, &parent, component.clone(), lookup, pin)
        .map_err(|error| error.to_string())?
    else {
        return Ok(Replaced::Released);
    };
    let host = host_relative_path(path).map_err(|error| error.to_string())?;
    Ok(
        match write_ordinary(projection, path, &host, &entry, Some(state))? {
            Placed::Owned(owned) => Replaced::Rewritten(Box::new(WrittenEntry {
                file_id: lookup.node.file_id,
                owned: Some(owned),
                facts: Some(entry.facts),
                basis,
            })),
            Placed::Retry => Replaced::Busy,
            Placed::Nothing | Placed::Unowned => Replaced::Released,
        },
    )
}

/// A virtualization context moved to the projection worker. The handle is
/// an opaque token `ProjFS` accepts from any thread.
struct SendContext(PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT);

// SAFETY: see `SendContext`; its lifetime is bounded by `Projector::finish`.
unsafe impl Send for SendContext {}

fn has_pending_capture(failure: &Mutex<PostOperationFailures>, path: &MountPath) -> bool {
    let failure = lock_recover(failure);
    failure.unreplayable.is_some()
        || failure.pending_captures.iter().any(|(pending, capture)| {
            pending == path || capture.subtree && projfs_path_suffix(path, pending).is_some()
        })
}

struct DetachedProjection {
    root: PathBuf,
    identity: crate::NativeRootIdentity,
}

/// One process-owned `ProjFS` virtualization context.
pub(super) struct ProjFsSession {
    context: Option<PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT>,
    runtime: Option<Box<Runtime>>,
    detached: Option<DetachedProjection>,
    /// The authenticated projection was removed, but its empty replacement
    /// has not yet been created. A denied replacement keeps this custody.
    destination_removed: bool,
}

// SAFETY: ProjFS contexts are opaque handles. The provider synchronizes callback
// dispatch and `stop` consumes the session before dropping callback state.
unsafe impl Send for ProjFsSession {}

impl ProjFsSession {
    pub(super) fn start(
        request: &NativeMountRequest,
        source: Arc<dyn MountFilesystem>,
    ) -> Result<Self, DriverStartFailure> {
        reject_stale_projection(&request.destination)?;
        let callback_gate = CallbackGate::start()?;
        let metadata_root = Arc::new(
            HostRoot::open(&request.destination)
                .map_err(|error| NativeMountError::Driver(error.to_string()))?,
        );
        let root = HSTRING::from(request.destination.as_os_str());
        let root_ptr = PCWSTR::from_raw(root.as_ptr());
        let guid = guid_from_key(u128::from_le_bytes(request.mount_id.into_bytes()));
        // SAFETY: the destination was admitted as an existing empty directory;
        // all pointers remain valid for this synchronous call.
        unsafe { PrjMarkDirectoryAsPlaceholder(root_ptr, PCWSTR::null(), None, &raw const guid) }
            .map_err(|error| {
            let error = driver_error(&error);
            NativeMountError::Driver(format!(
                "PrjMarkDirectoryAsPlaceholder failed for {}: {error}",
                request.destination.display()
            ))
        })?;

        let mut runtime = Box::new(Runtime {
            source,
            root: request.destination.clone(),
            metadata_root,
            writable: request.writable,
            metadata_baselines: Arc::new(Mutex::new(HashMap::new())),
            projection: Arc::new(Mutex::new(ProjectionCache::default())),
            projector: Arc::new(Projector::new()),
            metadata_probes: Arc::new(Mutex::new(HashMap::new())),
            capture_probes: std::sync::RwLock::new(Vec::new()),
            flush_gate: Mutex::new(()),
            stop_meter: Mutex::new(None),
            metadata_edits: Arc::new(FileLocks::default()),
            post_operation_failure: Arc::new(Mutex::new(PostOperationFailures::default())),
            callbacks: callback_gate,
        });
        let context_ptr = (&raw mut *runtime).cast::<c_void>();
        let callbacks = callbacks();
        // Only pre-operation callbacks that can veto are subscribed: a delete
        // is never refused, and it is captured when its handle closes.
        let mut notification_mapping = PRJ_NOTIFICATION_MAPPING {
            NotificationBitMask: FILE_NOTIFICATIONS,
            // ProjFS requires a pointer to an empty UTF-16 string for the
            // virtualization root. An empty HSTRING may be represented by a
            // null handle, which is not the same contract as `L""`.
            NotificationRoot: PCWSTR::from_raw(NOTIFICATION_ROOT.as_ptr()),
        };
        // Nothing is ever projected on demand (see `Projector`), so every
        // absence `ProjFS` caches is one it may.
        let mut options = PRJ_STARTVIRTUALIZING_OPTIONS {
            Flags: PRJ_FLAG_USE_NEGATIVE_PATH_CACHE,
            ..PRJ_STARTVIRTUALIZING_OPTIONS::default()
        };
        if request.writable {
            options.NotificationMappings = &raw mut notification_mapping;
            options.NotificationMappingsCount = 1;
        }
        // SAFETY: `runtime` is heap-stable until `PrjStopVirtualizing` has
        // synchronously drained every callback in `stop`.
        let context = unsafe {
            PrjStartVirtualizing(
                root_ptr,
                &raw const callbacks,
                Some(context_ptr.cast_const()),
                Some(&raw const options),
            )
        };
        let context = match context {
            Ok(context) => context,
            Err(error) => {
                let start_error = NativeMountError::Driver(format!(
                    "PrjStartVirtualizing failed for {}: {}",
                    request.destination.display(),
                    driver_error(&error)
                ));
                let root_identity = runtime.metadata_root.identity();
                drop(runtime);
                return Err(roll_back_start(
                    &request.destination,
                    root_identity,
                    start_error,
                ));
            }
        };
        let written = runtime.attach_workers(context).and_then(|()| {
            runtime
                .projector
                .materialize(
                    Projection {
                        source: runtime.source.as_ref(),
                        context,
                        root: &runtime.root,
                        captures: &runtime.post_operation_failure,
                        starting: true,
                    },
                    MountPath::root(),
                )
                .map_err(|error| {
                    NativeMountError::Driver(format!("projecting the source failed: {error}"))
                })
                // What changed while it was listed is written by the worker:
                // the mount opens only once all of it is there.
                .and_then(|()| runtime.projector.settle())
        });
        if let Err(error) = written {
            runtime.finish_workers();
            // SAFETY: the sole owner stops the context it just started.
            unsafe { PrjStopVirtualizing(context) };
            let root_identity = runtime.metadata_root.identity();
            drop(runtime);
            return Err(roll_back_start(&request.destination, root_identity, error));
        }
        Ok(Self {
            context: Some(context),
            runtime: Some(runtime),
            detached: None,
            destination_removed: false,
        })
    }

    pub(super) fn stop(&mut self) -> Result<(), NativeMountError> {
        if self.context.is_none() && self.runtime.is_none() && self.detached.is_none() {
            return Ok(());
        }
        self.stop_measured(&mut DemandWork::new(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        ))
    }

    /// Detaches this exact driver using the caller's admission and allocation
    /// ledger. A denied boundary retains the context or cleanup custody; a
    /// failed authored capture never permits removal of the projection.
    pub(super) fn stop_measured(&mut self, work: &mut DemandWork) -> Result<(), NativeMountError> {
        work.check().map_err(detach_work_error)?;
        if self.runtime.is_none() && self.detached.is_none() {
            return Ok(());
        }
        if let Some(context) = self.context {
            self.flush_callbacks_measured(work)?;
            let stop = crate::WorkCounters {
                backend_write_operations: 1,
                ..crate::WorkCounters::UNCHARGED
            };
            // Refuse before stopping the projection worker as well as before
            // entering the real SDK detach boundary.
            work.admit(stop).map_err(detach_work_error)?;
            if let Some(runtime) = self.runtime.as_ref() {
                runtime.finish_workers_measured(work)?;
            }
            work.admit(stop).map_err(detach_work_error)?;
            let runtime = self.runtime.as_ref().expect("live SDK callback runtime");
            let context_state = &mut self.context;
            super::adapter::BorrowedNativeDemandMeter::new(work).map_err(detach_source_error)?
                .run(|meter| {
                    // Withhold this actual SDK stop operation while concurrently
                    // drained callbacks admit their own real provider controls.
                    let reservation = {
                        let mut work = meter.lock();
                        work.reserve_cleanup_write().map_err(detach_work_error)?
                    };
                    *lock_recover(&runtime.stop_meter) = Some(Arc::clone(meter));
                    let scope = StopMeterScope(runtime);
                    let checked = { meter.lock().check() };
                    if let Err(error) = checked {
                        use acyclic_native_runtime::NativeReadMeter as _;
                        meter.cancel_cleanup_write(reservation);
                        return Err(detach_work_error(error));
                    }
                    // SAFETY: runtime and the original meter loan remain owned
                    // through this sole synchronous SDK callback drain.
                    unsafe { PrjStopVirtualizing(context) };
                    *context_state = None;
                    let recorded = {
                        use acyclic_native_runtime::NativeReadMeter as _;
                        meter.record_cleanup_write(reservation)
                            .map_err(|error| detach_work_error(crate::demand::native_demand_error(error)))
                    };
                    // Remove the runtime alias before the loan restores the
                    // original invocation; each scoped TLS fiber has retired.
                    drop(scope);
                    recorded
                })?;
        }
        if self.runtime.is_some() {
            // SDK stop can drain notifications admitted after the first barrier.
            self.flush_callbacks_measured(work)?;
            let mut runtime = self.runtime.take().expect("captured stopped runtime");
            let identity = runtime.metadata_root.identity();
            let root = std::mem::take(&mut runtime.root);
            // No callback can use this runtime after the SDK drain and final
            // capture. Release its old root handle before delete-on-close can
            // finish; move the preexisting path into cleanup custody, no clone.
            drop(runtime);
            self.detached = Some(DetachedProjection { root, identity });
        }
        let detached = self.detached.as_ref().expect("authenticated cleanup custody");
        if !self.destination_removed {
            if !remove_authenticated_destination_measured(&detached.root, detached.identity, work)? {
                return Err(NativeMountError::Lifecycle("stopped ProjFS root no longer carries its projection tag; preserved"));
            }
            self.destination_removed = true;
        }
        recreate_destination_measured(&detached.root, work)?;
        let _ = self.detached.take();
        self.destination_removed = false;
        Ok(())
    }

    /// Evidence that the sole `PrjStopVirtualizing` call completed, not that
    /// the destination currently exists, is empty, or is no longer a reparse
    /// point. Physical cleanup can still retain an authenticated root.
    pub(super) fn namespace_detach_confirmed(&self) -> bool {
        self.context.is_none()
    }

    /// The identity still held by live callbacks or stopped cleanup custody.
    /// It authenticates only this driver's owned root, not a later path query.
    pub(super) fn cleanup_root_identity(&self) -> Option<crate::NativeRootIdentity> {
        self.detached.as_ref().map(|detached| detached.identity)
            .or_else(|| self.runtime.as_ref().map(|runtime| runtime.metadata_root.identity()))
    }

    pub(super) fn flush_callbacks(&self) -> Result<(), NativeMountError> {
        self.flush_callbacks_measured(&mut DemandWork::new(
            crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
        ))
    }

    pub(super) fn flush_callbacks_measured(&self, work: &mut DemandWork) -> Result<(), NativeMountError> {
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            NativeMountError::Lifecycle("ProjFS callback runtime is stopped")
        })?;
        work.check().map_err(detach_work_error)?;
        let _flush = loop {
            work.check().map_err(detach_work_error)?;
            match runtime.flush_gate.try_lock() {
                Ok(held) => break held,
                Err(std::sync::TryLockError::Poisoned(error)) => break error.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
            }
        };
        flush_callback_gate_measured(
            &runtime.callbacks,
            runtime.post_operation_failure.as_ref(),
            Some(&runtime.capture_probes),
            work,
            |paths, work| {
                if paths.is_empty() {
                    return Ok(());
                }
                let envelope = (paths.len() as u64).checked_mul(size_of::<MountPath>() as u64)
                    .ok_or(crate::WorkError::Overflow)?;
                work.allocate(envelope).map_err(detach_work_error)?;
                let mut exact = Vec::new();
                let mut live = envelope;
                let result = (|| {
                    exact.try_reserve_exact(paths.len())
                        .map_err(|error| NativeMountError::Io(std::io::Error::new(std::io::ErrorKind::OutOfMemory, error)))?;
                    for (path, _, subtree) in paths {
                        work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })
                            .map_err(detach_work_error)?;
                        let kind = match with_mount_wide_path_measured(path, work, |name, work| {
                            runtime.metadata_root.stat_wide_measured(name, work)
                        }) {
                            Ok(_) => {
                                capture_missing_host_ancestors_measured(
                                    runtime.source.as_ref(), &runtime.root, path,
                                    runtime.metadata_root.identity(), work,
                                )?;
                                Some(*subtree)
                            }
                            Err(crate::DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                                let mut invocation = super::NativeMountWork { work };
                                match runtime.source.lookup_with_work(path, &mut invocation) {
                                    Err(MountSourceError::NotFound) | Ok(None) => None,
                                    Ok(Some(lookup)) => Some(lookup.node.kind == MountNodeKind::Directory),
                                    Err(error) => return Err(detach_source_error(error)),
                                }
                            }
                            Err(error) => return Err(detach_work_error(error)),
                        };
                        match kind {
                            Some(true) => {
                                runtime.source.capture_host_subtree_with_work(
                                    &runtime.root, path, runtime.metadata_root.identity(),
                                    &mut super::NativeMountWork { work },
                                ).map_err(detach_source_error)?;
                            }
                            Some(false) => {
                                let (path, bytes) = clone_mount_path_measured(path, work).map_err(detach_work_error)?;
                                live = live.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
                                exact.push(path);
                            }
                            None => {}
                        }
                    }
                    if !exact.is_empty() {
                        runtime.source.capture_host_paths_with_work(
                            &runtime.root, &exact, runtime.metadata_root.identity(),
                            &mut super::NativeMountWork { work },
                        ).map_err(detach_source_error)?;
                    }
                    Ok(())
                })();
                drop(exact);
                work.release(live).map_err(detach_work_error)?;
                result
            },
        )
    }

    /// Makes a source-side change at one mount-relative path (leading `/`
    /// optional) visible: the provider forgets memoized source facts, and an
    /// unmodified file at `path` is replaced by what is written again from
    /// the source (settled by [`Self::revalidate`]). Locally modified state
    /// at `path` is authored and stays.
    pub(super) fn invalidate(&self, path: &[u8]) -> Result<(), NativeMountError> {
        let (runtime, context) = self.live()?;
        let text = std::str::from_utf8(path)
            .map_err(|_| NativeMountError::Driver("invalidation path is not UTF-8".to_owned()))?;
        let relative = text
            .split('/')
            .filter(|component| !component.is_empty())
            .collect::<PathBuf>();
        // The change is outside the source's own record of changes, so no
        // memoized fact can prove itself current.
        lock_recover(runtime.projection.as_ref()).clear();
        if relative.as_os_str().is_empty() {
            return Ok(());
        }
        let relative = HSTRING::from(relative.as_os_str());
        let mount_path = path_from(PCWSTR::from_raw(relative.as_ptr())).ok_or_else(|| {
            NativeMountError::Driver("invalidation path is not a mount path".to_owned())
        })?;
        // SAFETY: the relative UTF-16 name remains live for this synchronous
        // call and the context belongs to this mounted runtime.
        match unsafe { PrjDeleteFile(context, &relative, Some(PRJ_UPDATE_ALLOW_READ_ONLY), None) } {
            Err(error)
                if ![
                    HR_FILE_NOT_FOUND,
                    HR_PATH_NOT_FOUND,
                    HR_VIRTUALIZATION_INVALID_OPERATION,
                ]
                .contains(&error.code()) =>
            {
                Err(driver_error(&error))
            }
            _ => {
                runtime.projector.invalidate(&mount_path);
                Ok(())
            }
        }
    }

    /// Waits until every change made to the source so far is written into
    /// the projection: each superseded path written again, each
    /// changed directory reconciled. A source that reports none of its
    /// changes has everything checked.
    pub(super) fn revalidate(&self) -> Result<(), NativeMountError> {
        let (runtime, _context) = self.live()?;
        if runtime.source.view_stamp().is_none() {
            runtime.projector.invalidate_all();
        }
        // Every change made to the source before this call is recorded, and
        // so notified, before the projection settles.
        runtime
            .source
            .fence_changes()
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        runtime.projector.settle()
    }

    fn live(&self) -> Result<(&Runtime, PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT), NativeMountError> {
        self.runtime
            .as_deref()
            .zip(self.context)
            .ok_or_else(|| NativeMountError::Driver("ProjFS session is stopped".to_owned()))
    }
}

impl Runtime {
    /// Starts the worker that keeps the projection current in `context`.
    fn attach_workers(
        &self,
        context: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT,
    ) -> Result<(), NativeMountError> {
        self.projector.attach(
            Arc::clone(&self.source),
            context,
            self.root.clone(),
            Arc::clone(&self.post_operation_failure),
        )
    }

    /// Stops the preexisting worker and removes only its now-empty staging
    /// directory. Joining borrows the worker; it does not invent SDK-internal
    /// callback or operating-system costs.
    fn finish_workers(&self) {
        let _ = self.finish_workers_measured(&mut DemandWork::new(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        ));
    }

    fn finish_workers_measured(&self, work: &mut DemandWork) -> Result<(), NativeMountError> {
        work.check().map_err(detach_work_error)?;
        self.projector.finish_measured(work)?;
        remove_staging_directory_measured(&self.root, work).map_err(detach_work_error)
    }
}

/// Reports one typed source failure as its distinct Win32 status. Only the
/// failure class crosses the `ProjFS` boundary; engine detail stays inside.
fn source_hresult(error: &MountSourceError) -> HRESULT {
    match error {
        MountSourceError::NotFound => HR_FILE_NOT_FOUND,
        MountSourceError::AlreadyExists => HR_ALREADY_EXISTS,
        MountSourceError::Invalid(_) => HR_INVALID_DATA,
        MountSourceError::Unsupported(_) => HR_NOT_SUPPORTED,
        MountSourceError::Engine(_) | MountSourceError::Work(_) => HR_IO_DEVICE,
        MountSourceError::Stale | MountSourceError::Cancelled => HR_FILE_INVALID,
    }
}

/// The placeholder `ProjFS` persists for a link described by `info`.
fn link_placeholder(info: PRJ_FILE_BASIC_INFO) -> PRJ_PLACEHOLDER_INFO {
    PRJ_PLACEHOLDER_INFO {
        FileBasicInfo: info,
        ..PRJ_PLACEHOLDER_INFO::default()
    }
}

fn reject_stale_projection(destination: &std::path::Path) -> Result<(), NativeMountError> {
    // An empty directory can still be a crash-left ProjFS root containing
    // hidden tombstones or metadata. Never mark it again: startup rollback
    // could otherwise delete the only remaining authored state.
    match reparse_tag(destination)?.0 {
        Some(windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS) => {
            Err(NativeMountError::Driver(format!(
                "stale ProjFS root at {} may contain unpublished authored state; preserved for recovery",
                destination.display()
            )))
        }
        Some(tag) => Err(NativeMountError::Driver(format!(
            "destination has non-ProjFS reparse tag 0x{tag:08x}"
        ))),
        None => Ok(()),
    }
}

/// Recovery owns only a capability it actually opened, never an invented
/// virtualization context from another process. A denied reparse probe keeps
/// that capability and its learned identity for the same owner's next attempt.
#[derive(Default)]
pub(super) struct ProjFsRecoveryControl {
    directory: Option<cap_std::fs::Dir>,
    identity: Option<crate::NativeRootIdentity>,
}

impl ProjFsRecoveryControl {
    /// Binds the constructor's actual root observation, independently of the
    /// sibling fence file identity. No new capability or allocation is made.
    pub(super) fn bind_root_identity(&mut self, actual: crate::NativeRootIdentity) {
        debug_assert!(self.directory.is_none(), "bind before acquiring recovery controls");
        self.identity = Some(actual);
    }

    /// Corroborates this owner's exact root using the original caller ledger.
    /// Preserving recovery never deletes cache or calls an SDK detach. Cache-
    /// only recovery retains the old fail-closed rule: a ProjFS reparse root
    /// may contain unjoined authored files, metadata or tombstones even empty.
    pub(super) fn recover_measured(
        &mut self,
        destination: &std::path::Path,
        disposable: bool,
        work: &mut DemandWork,
    ) -> Result<(), NativeMountError> {
        use windows::Win32::Storage::FileSystem::{FILE_READ_ATTRIBUTES, SYNCHRONIZE};
        match physical_attributes_measured(destination, work).map_err(detach_work_error)? {
            None => {
                let _ = self.directory.take();
                return Ok(());
            }
            Some(attributes) if attributes & FILE_ATTRIBUTE_DIRECTORY.0 != 0 => {}
            Some(_) => return Err(NativeMountError::InvalidDestination),
        }
        let retained = self.directory.is_some();
        if !retained {
            let parent = crate::native_host::open_root_directory_with_work(
                destination_parent(destination).map_err(detach_work_error)?, work,
            ).map_err(detach_work_error)?;
            let leaf = destination.file_name().ok_or(NativeMountError::InvalidDestination)?;
            let file = crate::native_host::with_wide_path_with_work(
                std::path::Path::new(leaf), work, |name, work| {
                    crate::native_host::open_windows_leaf_handle_with_work(
                        &parent, name, FILE_READ_ATTRIBUTES | SYNCHRONIZE, Some(true), work,
                    )
                },
            ).map_err(detach_work_error)?;
            self.directory = Some(cap_std::fs::Dir::from_std_file(file));
        }
        let directory = self.directory.as_ref().expect("actual recovery root capability");
        let identity = if !retained || self.identity.is_none() {
            let observed = crate::native_host::directory_identity_with_work(directory, work)
                .map_err(detach_work_error)?;
            if self.identity.is_some_and(|identity| identity != observed) {
                // This freshly observed replacement is not owned by recovery.
                // Drop its probe capability rather than associating it with
                // the retained proof for the original root.
                let _ = self.directory.take();
                return Err(NativeMountError::Demand(crate::DemandError::StaleSource));
            }
            self.identity = Some(observed);
            observed
        } else {
            self.identity.expect("retained capability has its original identity")
        };
        if retained && crate::native_host::root_path_identity_measured(destination, work)
            .map_err(detach_work_error)? != identity
        {
            return Err(NativeMountError::Demand(crate::DemandError::StaleSource));
        }
        if disposable {
            if let Some(tag) = reparse_tag_held_measured(directory, work).map_err(detach_work_error)? {
                return Err(NativeMountError::UnsafeRecoveryDestination { reparse_tag: Some(tag) });
            }
        }
        let _ = self.directory.take();
        Ok(())
    }

    /// Recovery did not call or observe an SDK stop. Neither a dead provider's
    /// reparse tag nor this control's lack of an SDK handle proves that the
    /// current public namespace is detached; callers inspect it separately.
    pub(super) const fn namespace_detach_confirmed(&self) -> bool {
        false
    }

    /// Reports actual retained native capability custody after a failed probe.
    pub(super) fn controls_pending(&self) -> bool {
        self.directory.is_some()
    }
}


pub(super) fn quarantine_crashed_destination(
    destination: &std::path::Path,
) -> Result<Option<std::path::PathBuf>, NativeMountError> {
    let metadata = match std::fs::symlink_metadata(destination) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(NativeMountError::Driver(error.to_string())),
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(NativeMountError::InvalidDestination);
    }
    let (tag, expected_identity) = reparse_tag(destination)?;
    match tag {
        Some(windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS) => {}
        Some(tag) => {
            return Err(NativeMountError::Driver(format!(
                "destination has non-ProjFS reparse tag 0x{tag:08x}"
            )));
        }
        None => {
            let mut entries = std::fs::read_dir(destination)
                .map_err(|error| NativeMountError::Driver(error.to_string()))?;
            if entries.next().is_none() {
                return Ok(None);
            }
        }
    }

    let parent = destination
        .parent()
        .ok_or(NativeMountError::InvalidDestination)?;
    let preserved = parent.join(format!(
        ".acyclic-residue-{}",
        uuid::Uuid::new_v4().simple()
    ));
    if preserved
        .try_exists()
        .map_err(|error| NativeMountError::Driver(error.to_string()))?
    {
        return Err(NativeMountError::Driver(
            "native recovery residue name collided; retry recovery".to_owned(),
        ));
    }
    std::fs::rename(destination, &preserved).map_err(|error| {
        NativeMountError::Driver(format!(
            "cannot preserve crashed ProjFS destination {}: {error}",
            destination.display()
        ))
    })?;
    let (_, moved_identity) = reparse_tag(&preserved)?;
    if moved_identity != expected_identity {
        return Err(NativeMountError::Driver(format!(
            "recovered ProjFS root identity changed; residue retained at {}",
            preserved.display()
        )));
    }
    Ok(Some(preserved))
}

/// Returns `destination` to the empty directory a failed start found,
/// reporting `error`; a destination that cannot be returned stays fenced.
fn roll_back_start(
    destination: &std::path::Path,
    root_identity: crate::NativeRootIdentity,
    error: NativeMountError,
) -> DriverStartFailure {
    let cleanup = remove_authenticated_destination(destination, root_identity).and_then(|()| {
        std::fs::create_dir(destination)
            .map_err(|error| NativeMountError::Driver(error.to_string()))
    });
    match cleanup {
        Ok(()) => error.into(),
        Err(cleanup) => DriverStartFailure::preserving_destination_fence(NativeMountError::Driver(
            format!("{error}; ProjFS startup rollback failed: {cleanup}"),
        )),
    }
}

fn detach_work_error(error: crate::DemandError) -> NativeMountError {
    match error {
        crate::DemandError::Work(error) => NativeMountError::Work(error),
        crate::DemandError::Io(error) => NativeMountError::Io(error),
        error => NativeMountError::Demand(error),
    }
}

fn detach_source_error(error: MountSourceError) -> NativeMountError {
    error.into()
}

/// Admits exactly one actual native call and records even its failed attempt.
/// Cancellation after the call cannot erase that executed prefix.
fn detach_attempt<T>(
    work: &mut DemandWork,
    write: bool,
    operation: impl FnOnce() -> std::io::Result<T>,
) -> Result<T, crate::DemandError> {
    let delta = if write {
        crate::WorkCounters { backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED }
    } else {
        crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED }
    };
    work.admit(delta)?;
    let result = operation();
    work.record(delta)?;
    result.map_err(Into::into)
}

fn destination_parent(destination: &std::path::Path) -> Result<&std::path::Path, crate::DemandError> {
    let parent = destination.parent().ok_or(crate::DemandError::InvalidRequest)?;
    Ok(if parent.as_os_str().is_empty() { std::path::Path::new(".") } else { parent })
}

fn recreate_destination_measured(
    destination: &std::path::Path, work: &mut DemandWork,
) -> Result<(), NativeMountError> {
    let parent = crate::native_host::open_root_directory_with_work(
        destination_parent(destination).map_err(detach_work_error)?, work,
    ).map_err(detach_work_error)?;
    let leaf = destination.file_name().ok_or(NativeMountError::InvalidDestination)?;
    // FILE_CREATE never adopts a directory that somebody else substituted.
    crate::native_host::with_wide_path_with_work(std::path::Path::new(leaf), work, |name, work| {
        crate::native_host::create_windows_new_directory_with_work(&parent, name, work)
    }).map_err(detach_work_error)?;
    Ok(())
}

fn remove_staging_directory_measured(
    root: &std::path::Path, work: &mut DemandWork,
) -> Result<(), crate::DemandError> {
    // Reproduce staging_path's lossy spelling without unadmitted temporary
    // strings or paths. Only this empty sibling is removed, never its contents.
    let leaf = root.file_name().unwrap_or_default();
    let mut units = 1_usize + ".acyclic-staging".len();
    for scalar in char::decode_utf16(leaf.encode_wide()) {
        work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        units = units.checked_add(scalar.unwrap_or(char::REPLACEMENT_CHARACTER).len_utf16())
            .ok_or(crate::WorkError::Overflow)?;
    }
    let bytes = (units as u64).checked_mul(2).ok_or(crate::WorkError::Overflow)?;
    let delta = crate::WorkCounters { bytes_encoded: bytes, ..crate::WorkCounters::UNCHARGED };
    work.admit(delta)?;
    work.allocate(bytes)?;
    let mut name = Vec::new();
    let result = (|| {
        name.try_reserve_exact(units)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::OutOfMemory, error))?;
        name.push(u16::from(b'.'));
        for scalar in char::decode_utf16(leaf.encode_wide()) {
            let mut encoded = [0; 2];
            name.extend_from_slice(scalar.unwrap_or(char::REPLACEMENT_CHARACTER).encode_utf16(&mut encoded));
        }
        name.extend(".acyclic-staging".encode_utf16());
        work.record(delta)?;
        let parent = crate::native_host::open_root_directory_with_work(destination_parent(root)?, work)?;
        match crate::native_host::remove_windows_leaf_with_work(&parent, &name, true, work) {
            Err(crate::DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            result => result,
        }
    })();
    drop(name);
    work.release(bytes)?;
    result
}

fn remove_authenticated_destination(
    destination: &std::path::Path,
    expected_identity: crate::NativeRootIdentity,
) -> Result<(), NativeMountError> {
    remove_authenticated_destination_measured(destination, expected_identity, &mut DemandWork::new(
        crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
    )).map(|_| ())
}

/// Observes the physical name without opening it through the stopped provider.
/// The owned absolute, NUL-terminated native buffer stays admitted through the
/// real metadata call and drops before its claim is released.
fn physical_attributes_measured(
    path: &std::path::Path, work: &mut DemandWork,
) -> Result<Option<u32>, crate::DemandError> {
    use windows::Win32::Storage::FileSystem::{GetFileAttributesW, INVALID_FILE_ATTRIBUTES};
    let name = crate::native_host::normalize_root_wide_measured(path, work)?;
    let bytes = name.len() as u64 * 2;
    let result = detach_attempt(work, false, || {
        // SAFETY: this exact native spelling includes its final NUL.
        let attributes = unsafe { GetFileAttributesW(PCWSTR::from_raw(name.as_ptr())) };
        if attributes == INVALID_FILE_ATTRIBUTES {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(attributes)
        }
    });
    drop(name);
    work.release(bytes)?;
    match result {
        Ok(attributes) => Ok(Some(attributes)),
        Err(crate::DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// Holds the authenticated root for the entire walk. Every child is opened
/// relative to that capability with reparses not followed; replacement of the
/// public path therefore cannot redirect cleanup into another tree.
fn remove_authenticated_destination_measured(
    destination: &std::path::Path,
    expected_identity: crate::NativeRootIdentity,
    work: &mut DemandWork,
) -> Result<bool, NativeMountError> {
    use windows::Win32::Storage::FileSystem::{DELETE, FILE_READ_ATTRIBUTES, SYNCHRONIZE};
    match physical_attributes_measured(destination, work).map_err(detach_work_error)? {
        None => return Ok(true),
        Some(attributes) if attributes & FILE_ATTRIBUTE_DIRECTORY.0 != 0 => {}
        Some(_) => return Err(NativeMountError::InvalidDestination),
    }
    let parent = crate::native_host::open_root_directory_with_work(
        destination_parent(destination).map_err(detach_work_error)?, work,
    ).map_err(detach_work_error)?;
    let leaf = destination.file_name().ok_or(NativeMountError::InvalidDestination)?;
    let file = match crate::native_host::with_wide_path_with_work(std::path::Path::new(leaf), work, |name, work| {
        crate::native_host::open_windows_leaf_handle_with_work(
            &parent, name, DELETE | FILE_READ_ATTRIBUTES | SYNCHRONIZE, Some(true), work,
        )
    }) {
        Ok(file) => file,
        Err(error) => return Err(detach_work_error(error)),
    };
    let directory = cap_std::fs::Dir::from_std_file(file);
    let identity = crate::native_host::directory_identity_with_work(&directory, work).map_err(detach_work_error)?;
    if identity != expected_identity {
        return Err(NativeMountError::Demand(crate::DemandError::StaleSource));
    }
    match reparse_tag_held_measured(&directory, work).map_err(detach_work_error)? {
        Some(windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS) => {}
        Some(tag) => return Err(NativeMountError::UnsafeRecoveryDestination { reparse_tag: Some(tag) }),
        None => return Ok(false),
    }
    remove_projection_tree(&directory, Some(expected_identity), work).map_err(detach_work_error)?;
    drop(directory);
    // A successful disposition can still be delete-on-close. Do not claim the
    // root is removed until its own handle dropped and this actual name probe
    // confirms absence; another directory substituted meanwhile is preserved.
    if physical_attributes_measured(destination, work).map_err(detach_work_error)?.is_some() {
        return Err(NativeMountError::Lifecycle("authenticated ProjFS root deletion is still pending; namespace preserved"));
    }
    Ok(true)
}

/// Removes only this owner's originally observed, currently empty physical
/// root. NT disposition enforces emptiness atomically; no tree walker or path
/// deletion can accidentally erase authored residue or a substituted root.
pub(super) fn remove_empty_recovery_destination_measured(
    destination: &std::path::Path,
    expected: Option<crate::NativeRootIdentity>,
    work: &mut DemandWork,
) -> Result<(), NativeMountError> {
    use windows::Win32::Storage::FileSystem::{DELETE, FILE_READ_ATTRIBUTES, SYNCHRONIZE};
    match physical_attributes_measured(destination, work).map_err(detach_work_error)? {
        None => return Ok(()),
        Some(attributes) if attributes & FILE_ATTRIBUTE_DIRECTORY.0 != 0 => {}
        Some(_) => return Err(NativeMountError::InvalidDestination),
    }
    let expected = expected.ok_or(NativeMountError::Demand(crate::DemandError::StaleSource))?;
    let parent = crate::native_host::open_root_directory_with_work(
        destination_parent(destination).map_err(detach_work_error)?, work,
    ).map_err(detach_work_error)?;
    let leaf = destination.file_name().ok_or(NativeMountError::InvalidDestination)?;
    let file = crate::native_host::with_wide_path_with_work(
        std::path::Path::new(leaf), work, |name, work| {
            crate::native_host::open_windows_leaf_handle_with_work(
                &parent, name, DELETE | FILE_READ_ATTRIBUTES | SYNCHRONIZE, Some(true), work,
            )
        },
    ).map_err(detach_work_error)?;
    let directory = cap_std::fs::Dir::from_std_file(file);
    if crate::native_host::directory_identity_with_work(&directory, work)
        .map_err(detach_work_error)? != expected
    {
        return Err(NativeMountError::Demand(crate::DemandError::StaleSource));
    }
    if let Some(tag) = reparse_tag_held_measured(&directory, work).map_err(detach_work_error)? {
        return Err(NativeMountError::UnsafeRecoveryDestination { reparse_tag: Some(tag) });
    }
    // Both actual disposition attempts refuse a nonempty directory. Foreign
    // children are never enumerated, opened, followed, or deleted here.
    delete_through_measured(&directory, work).map_err(detach_work_error)?;
    drop(directory);
    if physical_attributes_measured(destination, work).map_err(detach_work_error)?.is_some() {
        return Err(NativeMountError::Lifecycle("empty recovery root deletion is still pending; namespace preserved"));
    }
    Ok(())
}

/// Visits physical entries explicitly instead of hiding recursive traversal in
/// remove_dir_all. Retry enumeration, authentication probes, and both possible
/// disposition calls each receive their own admission at the real boundary.
fn remove_projection_tree(
    directory: &cap_std::fs::Dir,
    expected_root: Option<crate::NativeRootIdentity>,
    work: &mut DemandWork,
) -> Result<(), crate::DemandError> {
    const PATIENCE: std::time::Duration = std::time::Duration::from_secs(5);
    let deadline = std::time::Instant::now() + PATIENCE;
    loop {
        work.check()?;
        match remove_directories_in(directory, work) {
            Err(crate::DemandError::Io(error)) if refused_for_now(&error) => {}
            result => result?,
        }
        if let Some(expected) = expected_root {
            if crate::native_host::directory_identity_with_work(directory, work)? != expected
                || reparse_tag_held_measured(directory, work)?
                    != Some(windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS)
            {
                return Err(crate::DemandError::StaleSource);
            }
        }
        match delete_through_measured(directory, work) {
            Ok(()) => return Ok(()),
            Err(crate::DemandError::Io(error)) if refused_for_now(&error) && std::time::Instant::now() < deadline => {
                work.check()?;
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Err(error) => return Err(error),
        }
    }
}

/// Whether a real delete was refused by a transient holder or draining filter.
fn refused_for_now(error: &std::io::Error) -> bool {
    matches!(error.raw_os_error(), Some(2 | 3 | 5 | 32 | 145 | 369))
}

fn remove_directories_in(
    directory: &cap_std::fs::Dir, work: &mut DemandWork,
) -> Result<(), crate::DemandError> {
    use windows::Win32::Storage::FileSystem::{DELETE, FILE_READ_ATTRIBUTES, SYNCHRONIZE};
    let (mut entries, buffer) = crate::native_host::read_dir_held_with_work(directory, work)?;
    let result = (|| {
        while let Some((entry, allocation)) = entries.next_wide_with_work(work)? {
            let result = (|| {
                let file = match crate::native_host::open_windows_leaf_handle_with_work(
                    directory, &entry.name, DELETE | FILE_READ_ATTRIBUTES | SYNCHRONIZE, None, work,
                ) {
                    Ok(file) => file,
                    Err(crate::DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                    Err(error) => return Err(error),
                };
                let attributes = held_attributes_measured(&file, work)?;
                // A directory link is a leaf, never a recursively followed tree.
                if attributes & FILE_ATTRIBUTE_DIRECTORY.0 != 0
                    && attributes & FILE_ATTRIBUTE_REPARSE_POINT.0 == 0
                {
                    remove_projection_tree(&cap_std::fs::Dir::from_std_file(file), None, work)
                } else if attributes & FILE_ATTRIBUTE_DIRECTORY.0 != 0
                    && reparse_tag_held_measured(&file, work)?
                        == Some(windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS)
                {
                    remove_projection_tree(&cap_std::fs::Dir::from_std_file(file), None, work)
                } else {
                    delete_through_measured(&file, work)
                }
            })();
            drop(entry);
            work.release(allocation)?;
            result?;
        }
        Ok(())
    })();
    drop(entries);
    work.release(buffer)?;
    result
}

fn held_attributes_measured(
    file: &impl std::os::windows::io::AsRawHandle, work: &mut DemandWork,
) -> Result<u32, crate::DemandError> {
    use windows::Win32::Storage::FileSystem::{BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle};
    detach_attempt(work, false, || {
        let mut information = BY_HANDLE_FILE_INFORMATION::default();
        // SAFETY: this identity-bearing handle and output storage remain live.
        unsafe { GetFileInformationByHandle(windows::Win32::Foundation::HANDLE(file.as_raw_handle()), &raw mut information) }?;
        Ok(information.dwFileAttributes)
    })
}

fn delete_through_measured(
    file: &impl std::os::windows::io::AsRawHandle, work: &mut DemandWork,
) -> Result<(), crate::DemandError> {
    use windows::Win32::Storage::FileSystem::{
        FILE_DISPOSITION_FLAG_DELETE, FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE,
        FILE_DISPOSITION_FLAG_POSIX_SEMANTICS, FILE_DISPOSITION_INFO, FILE_DISPOSITION_INFO_EX,
        FILE_DISPOSITION_INFO_EX_FLAGS, FileDispositionInfo, FileDispositionInfoEx, SetFileInformationByHandle,
    };
    let handle = windows::Win32::Foundation::HANDLE(file.as_raw_handle());
    let posix = FILE_DISPOSITION_INFO_EX { Flags: FILE_DISPOSITION_INFO_EX_FLAGS(
        FILE_DISPOSITION_FLAG_DELETE.0 | FILE_DISPOSITION_FLAG_POSIX_SEMANTICS.0
            | FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE.0,
    ) };
    let result = detach_attempt(work, true, || {
        // SAFETY: the DELETE handle and correctly sized disposition are live.
        unsafe { SetFileInformationByHandle(handle, FileDispositionInfoEx, (&raw const posix).cast(), size_of::<FILE_DISPOSITION_INFO_EX>() as u32) }
            .map_err(Into::into)
    });
    match result {
        Err(crate::DemandError::Io(_)) => {
            let on_close = FILE_DISPOSITION_INFO { DeleteFile: true };
            detach_attempt(work, true, || {
                // SAFETY: same held handle; this fallback is separately admitted.
                unsafe { SetFileInformationByHandle(handle, FileDispositionInfo, (&raw const on_close).cast(), size_of::<FILE_DISPOSITION_INFO>() as u32) }
                    .map_err(Into::into)
            })
        }
        result => result,
    }
}

fn reparse_tag_held_measured(
    file: &impl std::os::windows::io::AsRawHandle, work: &mut DemandWork,
) -> Result<Option<u32>, crate::DemandError> {
    use windows::Win32::Foundation::{ERROR_NOT_A_REPARSE_POINT, ERROR_FILE_SYSTEM_VIRTUALIZATION_UNAVAILABLE, HANDLE};
    use windows::Win32::Storage::FileSystem::MAXIMUM_REPARSE_DATA_BUFFER_SIZE;
    use windows::Win32::System::IO::DeviceIoControl;
    use windows::Win32::System::Ioctl::FSCTL_GET_REPARSE_POINT;
    let bytes = u64::from(MAXIMUM_REPARSE_DATA_BUFFER_SIZE);
    work.allocate(bytes)?;
    let mut output = Vec::new();
    let result = (|| {
        output.try_reserve_exact(bytes as usize)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::OutOfMemory, error))?;
        output.resize(bytes as usize, 0_u8);
        let mut returned = 0_u32;
        match detach_attempt(work, false, || {
            // SAFETY: the held handle and exact-sized mutable buffer stay live.
            unsafe { DeviceIoControl(HANDLE(file.as_raw_handle()), FSCTL_GET_REPARSE_POINT, None, 0,
                Some(output.as_mut_ptr().cast()), MAXIMUM_REPARSE_DATA_BUFFER_SIZE, Some(&raw mut returned), None) }
                .map_err(Into::into)
        }) {
            Ok(()) if returned >= 4 => Ok(Some(u32::from_le_bytes(output[..4].try_into().expect("four tag bytes")))),
            Ok(()) => Err(std::io::ErrorKind::InvalidData.into()),
            Err(crate::DemandError::Io(error)) if error.raw_os_error() == Some(ERROR_NOT_A_REPARSE_POINT.0 as i32) => Ok(None),
            Err(crate::DemandError::Io(error)) if error.raw_os_error() == Some(ERROR_FILE_SYSTEM_VIRTUALIZATION_UNAVAILABLE.0 as i32) =>
                Ok(Some(windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS)),
            Err(error) => Err(error),
        }
    })();
    drop(output);
    work.release(bytes)?;
    result
}

fn reparse_tag(
    path: &std::path::Path,
) -> Result<(Option<u32>, crate::NativeRootIdentity), NativeMountError> {
    use std::os::windows::fs::OpenOptionsExt;
    use windows::Win32::Foundation::ERROR_FILE_SYSTEM_VIRTUALIZATION_UNAVAILABLE;
    use windows::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_DELETE,
        FILE_SHARE_READ, FILE_SHARE_WRITE,
    };

    let file = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS.0 | FILE_FLAG_OPEN_REPARSE_POINT.0)
        .open(path)
        .map_err(|error| NativeMountError::Driver(error.to_string()))?;
    let identity = crate::NativeRootIdentity::from_file(&file)
        .map_err(|error| NativeMountError::Driver(error.to_string()))?;
    match reparse_data(&file) {
        Ok(None) => Ok((None, identity)),
        Ok(Some(data)) => {
            let tag = data.first_chunk().copied().ok_or_else(|| {
                NativeMountError::Driver("reparse response omitted its tag".to_owned())
            })?;
            Ok((Some(u32::from_le_bytes(tag)), identity))
        }
        // A stopped ProjFS provider can leave an authenticated placeholder
        // root even when the filter declines to return its reparse payload.
        Err(error)
            if error.raw_os_error()
                == i32::try_from(ERROR_FILE_SYSTEM_VIRTUALIZATION_UNAVAILABLE.0).ok() =>
        {
            Ok((
                Some(windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS),
                identity,
            ))
        }
        Err(error) => Err(NativeMountError::Driver(error.to_string())),
    }
}

/// The reparse data `file` carries; `None` when it is no reparse point.
fn reparse_data(file: &std::fs::File) -> std::io::Result<Option<Vec<u8>>> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Win32::Foundation::{ERROR_NOT_A_REPARSE_POINT, HANDLE};
    use windows::Win32::Storage::FileSystem::MAXIMUM_REPARSE_DATA_BUFFER_SIZE;
    use windows::Win32::System::IO::DeviceIoControl;
    use windows::Win32::System::Ioctl::FSCTL_GET_REPARSE_POINT;

    let capacity = usize::try_from(MAXIMUM_REPARSE_DATA_BUFFER_SIZE)
        .map_err(|_| std::io::Error::other("reparse buffer size overflow"))?;
    let mut output = vec![0_u8; capacity];
    let mut returned = 0_u32;
    // SAFETY: the handle and output buffer remain valid for the synchronous
    // control call; the output length exactly matches the allocated buffer.
    let result = unsafe {
        DeviceIoControl(
            HANDLE(file.as_raw_handle()),
            FSCTL_GET_REPARSE_POINT,
            None,
            0,
            Some(output.as_mut_ptr().cast()),
            MAXIMUM_REPARSE_DATA_BUFFER_SIZE,
            Some(&raw mut returned),
            None,
        )
    };
    match result {
        Ok(()) => {
            output.truncate(usize::try_from(returned).unwrap_or(capacity));
            Ok(Some(output))
        }
        Err(error) if error.code() == ERROR_NOT_A_REPARSE_POINT.to_hresult() => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn driver_error(error: &windows::core::Error) -> NativeMountError {
    NativeMountError::Driver(error.to_string())
}

/// One lock per key, apart from every other key's.
struct KeyedLocks<K>(Mutex<HashMap<K, Arc<Mutex<()>>>>);

impl<K> Default for KeyedLocks<K> {
    fn default() -> Self {
        Self(Mutex::new(HashMap::new()))
    }
}

/// One lock per file, keyed by its host identity.
type FileLocks = KeyedLocks<crate::NativeRootIdentity>;

impl<K: Eq + std::hash::Hash + Clone> KeyedLocks<K> {
    /// Runs `edit` while no other edit under `key` runs.
    fn with<T>(&self, key: &K, edit: impl FnOnce() -> T) -> T {
        let lock = Arc::clone(lock_recover(&self.0).entry(key.clone()).or_default());
        let result = {
            let _held = lock_recover(&lock);
            edit()
        };
        let mut locks = lock_recover(&self.0);
        // Kept only while another edit holds or waits for it.
        if Arc::strong_count(&lock) == 2 {
            locks.remove(key);
        }
        result
    }
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

unsafe fn runtime<'a>(
    data: *const PRJ_CALLBACK_DATA,
) -> Option<(&'a PRJ_CALLBACK_DATA, &'a Runtime)> {
    let data = data.as_ref()?;
    let pointer = data.InstanceContext.cast::<Runtime>();
    pointer.as_ref().map(|value| (data, value))
}

fn path_from(pointer: PCWSTR) -> Option<MountPath> {
    if pointer.is_null() {
        return Some(MountPath::root());
    }
    let mut length = 0_usize;
    // SAFETY: ProjFS supplies a NUL-terminated UTF-16 callback path.
    unsafe {
        while *pointer.0.add(length) != 0 {
            length = length.checked_add(1)?;
        }
        let wide = std::slice::from_raw_parts(pointer.0, length);
        let host_path = PathBuf::from(std::ffi::OsString::from_wide(wide));
        let mut path = MountPath::root();
        for component in host_path.components() {
            let std::path::Component::Normal(component) = component else {
                return None;
            };
            let bytes = component
                .encode_wide()
                .flat_map(u16::to_le_bytes)
                .collect::<Vec<_>>();
            path = path.child(bytes);
        }
        Some(path)
    }
}

unsafe fn empty_destination(pointer: PCWSTR) -> bool {
    pointer.is_null() || *pointer.0 == 0
}

fn decode_utf16_name(bytes: &[u8]) -> Option<Vec<u16>> {
    Some(crate::kernel::types::utf16le_units(bytes)?.collect())
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct HostWindowsMetadata {
    identity: crate::NativeRootIdentity,
    links: Option<u32>,
    size: u64,
    attributes: u32,
    created: i64,
    modified: i64,
}

#[derive(Clone, Copy)]
struct ReadOnlyBinding {
    host: HostWindowsMetadata,
    file_id: FileId,
    basis: ReadBasis,
}

#[derive(Clone, Copy)]
struct OpenMetadataState {
    baseline: HostWindowsMetadata,
    open_handles: usize,
}

enum MetadataClose {
    Pending,
    Final(Option<HostWindowsMetadata>),
}

fn host_windows_metadata(
    root: &HostRoot,
    path: &MountPath,
) -> Result<HostWindowsMetadata, MountSourceError> {
    use cap_std::fs::MetadataExt as _;
    /// A `ProjFS` placeholder's own tag, which describes it without redirecting.
    const IO_REPARSE_TAG_PROJFS: u32 = 0x9000_001C;

    let host_path = host_relative_path(path)?;
    // One query answers a name that no reparse point redirects, without
    // opening every component on the way.
    if let Some(information) = root
        .stat_information_by_name(&host_path)
        .map_err(|error| MountSourceError::Engine(error.to_string()))?
        && (information.ReparseTag == 0 || information.ReparseTag == IO_REPARSE_TAG_PROJFS)
    {
        return Ok(HostWindowsMetadata {
            identity: crate::NativeRootIdentity {
                device: root.identity().device,
                object: u64::try_from(information.FileId).map_err(|_| {
                    MountSourceError::Invalid("Windows file identity is negative".to_owned())
                })?,
            },
            links: Some(information.NumberOfLinks),
            size: u64::try_from(information.EndOfFile).map_err(|_| {
                MountSourceError::Invalid("Windows file size is negative".to_owned())
            })?,
            attributes: information.FileAttributes,
            created: information.CreationTime,
            modified: information.LastWriteTime,
        });
    }
    let metadata = root
        .symlink_metadata_held(&host_path)
        .map_err(|error| MountSourceError::Engine(error.to_string()))?;
    Ok(HostWindowsMetadata {
        identity: crate::NativeRootIdentity::from_metadata(&metadata)
            .map_err(|error| MountSourceError::Engine(error.to_string()))?,
        links: cap_primitives::fs::_WindowsByHandle::number_of_links(&metadata),
        size: metadata.len(),
        attributes: metadata.file_attributes(),
        created: i64::try_from(metadata.creation_time()).map_err(|_| {
            MountSourceError::Invalid("Windows creation time exceeds i64".to_owned())
        })?,
        modified: i64::try_from(metadata.last_write_time())
            .map_err(|_| MountSourceError::Invalid("Windows write time exceeds i64".to_owned()))?,
    })
}

fn host_relative_path(path: &MountPath) -> Result<PathBuf, MountSourceError> {
    let mut host_path = PathBuf::new();
    for component in path.components() {
        let name = decode_utf16_name(component).ok_or_else(|| {
            MountSourceError::Invalid("ProjFS path component is malformed".to_owned())
        })?;
        host_path.push(std::ffi::OsString::from_wide(&name));
    }
    Ok(host_path)
}

/// Encodes only this invocation's owned relative native-name buffer.
fn with_mount_wide_path_measured<T>(
    path: &MountPath,
    work: &mut DemandWork,
    operation: impl FnOnce(&[u16], &mut DemandWork) -> Result<T, crate::DemandError>,
) -> Result<T, crate::DemandError> {
    let mut units = path.components().len().saturating_sub(1);
    for component in path.components() {
        work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        crate::kernel::types::utf16le_units(component).ok_or(crate::DemandError::InvalidRequest)?;
        units = units.checked_add(component.len() / 2).ok_or(crate::WorkError::Overflow)?;
    }
    let bytes = (units as u64).checked_mul(2).ok_or(crate::WorkError::Overflow)?;
    let encoded = crate::WorkCounters { bytes_encoded: bytes, ..crate::WorkCounters::UNCHARGED };
    work.admit(encoded)?;
    work.allocate(bytes)?;
    let mut name = Vec::new();
    let result = (|| {
        name.try_reserve_exact(units)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::OutOfMemory, error))?;
        for (index, component) in path.components().iter().enumerate() {
            if index != 0 {
                name.push(u16::from(b'\\'));
            }
            name.extend(crate::kernel::types::utf16le_units(component).expect("validated component"));
        }
        work.record(encoded)?;
        operation(&name, work)
    })();
    drop(name);
    work.release(bytes)?;
    result
}

fn capture_missing_host_ancestors_measured(
    source: &dyn MountFilesystem,
    root: &std::path::Path,
    path: &MountPath,
    identity: crate::NativeRootIdentity,
    work: &mut DemandWork,
) -> Result<(), NativeMountError> {
    for count in 1..path.components().len() {
        let (parent, bytes) = clone_mount_components_measured(&path.components()[..count], work)
            .map_err(detach_work_error)?;
        let result = (|| {
            if source.lookup_with_work(&parent, &mut super::NativeMountWork { work })
                .map_err(detach_source_error)?.is_none()
            {
                source.capture_host_paths_with_work(
                    root, std::slice::from_ref(&parent), identity, &mut super::NativeMountWork { work },
                ).map_err(detach_source_error)?;
            }
            Ok(())
        })();
        drop(parent);
        work.release(bytes).map_err(detach_work_error)?;
        result?;
    }
    Ok(())
}

struct MetadataProbeGuard<'a> {
    probes: &'a Mutex<HashMap<MountPath, usize>>,
    path: MountPath,
}

impl<'a> MetadataProbeGuard<'a> {
    fn enter(probes: &'a Mutex<HashMap<MountPath, usize>>, path: &MountPath) -> Self {
        let mut active = lock_recover(probes);
        *active.entry(path.clone()).or_default() += 1;
        Self {
            probes,
            path: path.clone(),
        }
    }

    fn is_active(probes: &Mutex<HashMap<MountPath, usize>>, path: &MountPath) -> bool {
        lock_recover(probes)
            .keys()
            .any(|active| matches!(projfs_path_suffix(active, path), Some([])))
    }
}

impl Drop for MetadataProbeGuard<'_> {
    fn drop(&mut self) {
        let mut active = lock_recover(self.probes);
        let Some(count) = active.get_mut(&self.path) else {
            return;
        };
        *count -= 1;
        if *count == 0 {
            active.remove(&self.path);
        }
    }
}

fn probe_host_windows_metadata(
    root: &HostRoot,
    path: &MountPath,
    probes: &Mutex<HashMap<MountPath, usize>>,
) -> Result<HostWindowsMetadata, MountSourceError> {
    let _guard = MetadataProbeGuard::enter(probes, path);
    host_windows_metadata(root, path)
}

fn close_metadata_handle(
    baselines: &Mutex<HashMap<u128, OpenMetadataState>>,
    file_id: u128,
) -> MetadataClose {
    let mut baselines = lock_recover(baselines);
    let Some(state) = baselines.get_mut(&file_id) else {
        return MetadataClose::Final(None);
    };
    if state.open_handles > 1 {
        state.open_handles -= 1;
        MetadataClose::Pending
    } else {
        MetadataClose::Final(baselines.remove(&file_id).map(|state| state.baseline))
    }
}

fn record_metadata_open(
    baselines: &Mutex<HashMap<u128, OpenMetadataState>>,
    file_id: u128,
    baseline: HostWindowsMetadata,
) -> Result<(), MountSourceError> {
    let mut baselines = lock_recover(baselines);
    if let Some(state) = baselines.get_mut(&file_id) {
        if state.open_handles == 0 {
            state.open_handles = 1;
        } else {
            state.open_handles = state.open_handles.checked_add(1).ok_or_else(|| {
                MountSourceError::Invalid("ProjFS open-handle count overflow".to_owned())
            })?;
        }
    } else {
        baselines.insert(
            file_id,
            OpenMetadataState {
                baseline,
                open_handles: 1,
            },
        );
    }
    Ok(())
}

fn refresh_metadata_baseline(
    baselines: &Mutex<HashMap<u128, OpenMetadataState>>,
    file_id: u128,
    baseline: HostWindowsMetadata,
) {
    let mut baselines = lock_recover(baselines);
    baselines
        .entry(file_id)
        .and_modify(|state| state.baseline = baseline)
        .or_insert(OpenMetadataState {
            baseline,
            open_handles: 0,
        });
}

fn metadata_changed_since_open(
    baseline: HostWindowsMetadata,
    current: HostWindowsMetadata,
) -> bool {
    let attributes_changed =
        !crate::native_host::same_windows_attributes(baseline.attributes, current.attributes);
    attributes_changed
        || baseline.identity != current.identity
        || baseline.created != current.created
        || baseline.modified != current.modified
}

fn capture_changed_windows_metadata(
    source: &dyn MountFilesystem,
    path: &MountPath,
    mut metadata: FileMetadata,
    baseline: HostWindowsMetadata,
    current: HostWindowsMetadata,
) -> Result<(), MountSourceError> {
    let attributes_changed =
        !crate::native_host::same_windows_attributes(baseline.attributes, current.attributes);
    if attributes_changed {
        metadata.windows_attributes = MetadataField::Value(current.attributes);
    }
    if baseline.created != current.created {
        metadata.created_ns = MetadataField::Value(unix_nanoseconds(current.created)?);
    }
    if baseline.modified != current.modified {
        metadata.modified_ns = MetadataField::Value(unix_nanoseconds(current.modified)?);
    }
    source
        .set_attributes(path, metadata, None)
        .and_then(|()| source.flush())
}

/// Keys one GUID by its little-endian wire bytes.
fn guid_key(guid: &GUID) -> u128 {
    let mut bytes = [0_u8; 16];
    bytes[0..4].copy_from_slice(&guid.data1.to_le_bytes());
    bytes[4..6].copy_from_slice(&guid.data2.to_le_bytes());
    bytes[6..8].copy_from_slice(&guid.data3.to_le_bytes());
    bytes[8..16].copy_from_slice(&guid.data4);
    u128::from_le_bytes(bytes)
}

/// Inverse of [`guid_key`].
fn guid_from_key(key: u128) -> GUID {
    let bytes = key.to_le_bytes();
    GUID::from_values(
        u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
        u16::from_le_bytes([bytes[4], bytes[5]]),
        u16::from_le_bytes([bytes[6], bytes[7]]),
        [
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ],
    )
}

fn file_id(data: &PRJ_CALLBACK_DATA) -> u128 {
    guid_key(&data.FileId)
}

fn basic(node: MountNode, metadata: Option<FileMetadata>) -> Option<PRJ_FILE_BASIC_INFO> {
    let (directory, default_attributes, size) = match node.kind {
        MountNodeKind::Directory => (true, FILE_ATTRIBUTE_DIRECTORY.0, 0),
        MountNodeKind::Regular => (false, FILE_ATTRIBUTE_NORMAL.0, node.logical_bytes),
        MountNodeKind::SymbolicLink => (false, FILE_ATTRIBUTE_REPARSE_POINT.0, node.logical_bytes),
        MountNodeKind::Fifo
        | MountNodeKind::Socket
        | MountNodeKind::CharacterDevice
        | MountNodeKind::BlockDevice
        | MountNodeKind::Unsupported => return None,
    };
    let attributes = metadata.map_or(default_attributes, |metadata| {
        let exact = match metadata.windows_attributes {
            MetadataField::Unavailable => default_attributes,
            MetadataField::Value(value) => value,
        };
        if directory {
            exact | FILE_ATTRIBUTE_DIRECTORY.0
        } else if exact == 0 {
            FILE_ATTRIBUTE_NORMAL.0
        } else {
            exact
        }
    });
    Some(PRJ_FILE_BASIC_INFO {
        IsDirectory: directory,
        FileSize: i64::try_from(size).ok()?,
        CreationTime: metadata
            .and_then(|value| windows_time(value.created_ns))
            .unwrap_or(0),
        LastAccessTime: metadata
            .and_then(|value| windows_time(value.accessed_ns))
            .unwrap_or(0),
        LastWriteTime: metadata
            .and_then(|value| windows_time(value.modified_ns))
            .unwrap_or(0),
        ChangeTime: metadata
            .and_then(|value| windows_time(value.changed_ns))
            .unwrap_or(0),
        FileAttributes: attributes,
    })
}

fn unix_nanoseconds(windows_ticks: i64) -> Result<i64, MountSourceError> {
    crate::native_host::filetime_to_unix_nanoseconds(windows_ticks).ok_or_else(|| {
        MountSourceError::Invalid("Windows timestamp exceeds i64 nanoseconds".to_owned())
    })
}

fn symlink_extended(target: &[u8]) -> Option<(HSTRING, PRJ_EXTENDED_INFO)> {
    let target = HSTRING::from_wide(&decode_utf16_name(target)?);
    let extended = PRJ_EXTENDED_INFO {
        InfoType: PRJ_EXT_INFO_TYPE_SYMLINK,
        NextInfoOffset: 0,
        Anonymous: PRJ_EXTENDED_INFO_0 {
            Symlink: PRJ_EXTENDED_INFO_0_0 {
                TargetName: PCWSTR::from_raw(target.as_ptr()),
            },
        },
    };
    Some((target, extended))
}

/// Presented placeholder times round toward the past: a projection must not
/// fail because its source keeps finer timestamps than Windows can name.
fn windows_time(field: MetadataField<i64>) -> Option<i64> {
    let MetadataField::Value(unix_nanoseconds) = field else {
        return None;
    };
    Some(crate::native_host::unix_nanoseconds_to_filetime(
        unix_nanoseconds,
    ))
}

// Every directory beneath the root is an ordinary one, and the root holds
// on disk everything the source lists: `ProjFS` merges an enumeration with
// what is on disk, so the provider enumerates nothing and names nothing.
unsafe fn start_directory(
    _callback_data: *const PRJ_CALLBACK_DATA,
    _enumeration_id: *const GUID,
) -> HRESULT {
    HR_OK
}

unsafe fn end_directory(
    _callback_data: *const PRJ_CALLBACK_DATA,
    _enumeration_id: *const GUID,
) -> HRESULT {
    HR_OK
}

unsafe fn get_directory(
    _callback_data: *const PRJ_CALLBACK_DATA,
    _enumeration_id: *const GUID,
    _search_expression: PCWSTR,
    _buffer: PRJ_DIR_ENTRY_BUFFER_HANDLE,
) -> HRESULT {
    HR_OK
}

unsafe fn placeholder(_callback_data: *const PRJ_CALLBACK_DATA) -> HRESULT {
    HR_FILE_NOT_FOUND
}

/// What the projection writes for the source's entry `component` in
/// `directory`; `None` for one Windows cannot represent.
fn projected_entry(
    source: &dyn MountFilesystem,
    directory: &MountPath,
    component: Vec<u8>,
    lookup: MountLookup,
    pin: Option<MountContentPin>,
) -> Result<Option<ProjectedEntry>, MountSourceError> {
    let Some(info) = basic(lookup.node, Some(lookup.metadata)) else {
        return Ok(None);
    };
    let Some(mut name) = decode_utf16_name(&component).filter(|name| !name.contains(&0)) else {
        return Ok(None);
    };
    name.push(0);
    let symlink_target = if lookup.node.kind == MountNodeKind::SymbolicLink {
        let target = source.read_link(&directory.child(component.clone()))?;
        if symlink_extended(&target).is_none() {
            return Ok(None);
        }
        Some(target)
    } else {
        None
    };
    let directory_link = symlink_target.as_deref().is_some_and(|target| {
        links_to_directory(
            source,
            directory,
            lookup.metadata.windows_attributes,
            target,
        )
    });
    Ok(Some(ProjectedEntry {
        name,
        component,
        facts: EntryFacts { lookup, pin },
        info,
        symlink_target,
        directory_link,
    }))
}

/// Whether the link in `directory` to `target` names a directory, which a
/// Windows link must say when it is made: as its own attributes recorded,
/// else as its target resolves, in the source or, outside it, on the host.
fn links_to_directory(
    source: &dyn MountFilesystem,
    directory: &MountPath,
    attributes: MetadataField<u32>,
    target: &[u8],
) -> bool {
    if let MetadataField::Value(attributes) = attributes {
        return attributes & FILE_ATTRIBUTE_DIRECTORY.0 != 0;
    }
    decode_utf16_name(target)
        .is_some_and(|target| resolves_to_directory(source, directory, &target, 0))
}

/// Whether `target`, read from a link in `directory`, names a directory,
/// following at most a few links further.
fn resolves_to_directory(
    source: &dyn MountFilesystem,
    directory: &MountPath,
    target: &[u16],
    hops: u8,
) -> bool {
    const MOST_HOPS: u8 = 8;
    let text = String::from_utf16_lossy(target);
    if text.starts_with(['/', '\\']) || text.get(1..2) == Some(":") {
        return std::path::Path::new(&text).is_dir();
    }
    let mut path = directory.clone();
    for part in text.split(['/', '\\']) {
        match part {
            "" | "." => {}
            ".." => match path.parent() {
                Some(parent) => path = parent,
                None => return false,
            },
            name => path = path.child(name.encode_utf16().flat_map(u16::to_le_bytes).collect()),
        }
    }
    match source.lookup(&path) {
        Ok(Some(lookup)) if lookup.node.kind == MountNodeKind::Directory => true,
        Ok(Some(lookup)) if lookup.node.kind == MountNodeKind::SymbolicLink && hops < MOST_HOPS => {
            let (Some(parent), Ok(next)) = (path.parent(), source.read_link(&path)) else {
                return false;
            };
            decode_utf16_name(&next)
                .is_some_and(|next| resolves_to_directory(source, &parent, &next, hops + 1))
        }
        _ => false,
    }
}

/// The source's listing of `path`, as this provider writes it. An entry
/// Windows cannot represent (a socket, say) is left out, as is a name that
/// case-folds to one listed before it: Windows can hold only one of them.
fn source_listing(
    source: &dyn MountFilesystem,
    path: &MountPath,
) -> Result<Vec<ProjectedEntry>, MountSourceError> {
    // Pinned only while the listing is read, so it is one view's.
    let lease = source.acquire_view_lease()?;
    let mut cursor = None;
    let mut seen_cursors = HashSet::new();
    let mut entries = Vec::new();
    loop {
        let page = source.read_directory(path, cursor.as_deref(), DIRECTORY_PAGE_SIZE)?;
        entries
            .try_reserve(page.entries.len())
            .map_err(|_| MountSourceError::Engine("listing exceeds memory".to_owned()))?;
        for entry in page.entries {
            let lookup = MountLookup {
                node: entry.node,
                metadata: entry.metadata,
            };
            if let Some(projected) = projected_entry(source, path, entry.name, lookup, entry.pin)? {
                entries.push(projected);
            }
        }
        match page.next_cursor {
            Some(next) if !seen_cursors.insert(next.clone()) => {
                return Err(MountSourceError::Invalid(
                    "directory listing repeated a cursor".to_owned(),
                ));
            }
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    drop(lease);
    entries.sort_by(|left, right| {
        // SAFETY: both NUL-terminated name buffers remain live for this call.
        unsafe {
            PrjFileNameCompare(
                PCWSTR::from_raw(left.name.as_ptr()),
                PCWSTR::from_raw(right.name.as_ptr()),
            )
        }
        .cmp(&0)
    });
    entries.dedup_by(|later, earlier| {
        // SAFETY: as above.
        unsafe {
            PrjFileNameCompare(
                PCWSTR::from_raw(later.name.as_ptr()),
                PCWSTR::from_raw(earlier.name.as_ptr()),
            ) == 0
        }
    });
    Ok(entries)
}

/// `ProjFS` asks only a placeholder for its content, and no file of the
/// projection is one.
unsafe fn file_data(
    _callback_data: *const PRJ_CALLBACK_DATA,
    _byte_offset: u64,
    _length: u32,
) -> HRESULT {
    HR_UNEXPECTED
}

unsafe fn query_name(callback_data: *const PRJ_CALLBACK_DATA) -> HRESULT {
    let Some((data, runtime)) = runtime(callback_data) else {
        return HR_UNEXPECTED;
    };
    let Some(path) = path_from(data.FilePathName) else {
        return HR_INVALID_DATA;
    };
    match runtime.source.lookup(&path) {
        Ok(Some(_)) => HR_OK,
        Ok(None) => HR_FILE_NOT_FOUND,
        Err(error) => source_hresult(&error),
    }
}

fn record_post_operation_failure(
    failure: &Mutex<PostOperationFailures>,
    notification: PRJ_NOTIFICATION,
    path: &MountPath,
    retry_path: Option<&MountPath>,
    result: &Result<(), MountSourceError>,
) {
    if !matches!(
        notification,
        PRJ_NOTIFICATION_NEW_FILE_CREATED
            | PRJ_NOTIFICATION_FILE_OVERWRITTEN
            | PRJ_NOTIFICATION_FILE_RENAMED
            | PRJ_NOTIFICATION_HARDLINK_CREATED
            | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED
            | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED
    ) {
        return;
    }
    let mut failure = lock_recover(failure);
    match (retry_path, result) {
        // A successful callback does not prove that an earlier deferred host
        // capture for this path has happened. In particular, a close after a
        // rename may be a no-op while the renamed new file is still pending.
        (_, Ok(())) => {}
        (Some(retry_path), Err(error)) => {
            failure.queue_capture(retry_path.clone(), error.to_string());
        }
        (None, Err(error)) => {
            if failure.unreplayable.is_none() {
                failure.unreplayable = Some(format!("{notification:?} at {path:?}: {error}"));
            }
        }
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "notification admission and its exact authored operation share one callback boundary"
)]
unsafe fn notification(
    callback_data: *const PRJ_CALLBACK_DATA,
    is_directory: bool,
    notification: PRJ_NOTIFICATION,
    destination_filename: PCWSTR,
    operation_parameters: *mut PRJ_NOTIFICATION_PARAMETERS,
) -> HRESULT {
    let Some((data, runtime)) = runtime(callback_data) else {
        return HR_UNEXPECTED;
    };
    if !runtime.writable {
        return HR_NOT_SUPPORTED;
    }
    let Some(path) = path_from(data.FilePathName) else {
        return HR_INVALID_DATA;
    };
    // A closed handle may have held a superseded path open.
    if matches!(
        notification,
        PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION
            | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED
            | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED
    ) {
        runtime.projector.retry();
    }
    // SAFETY: `runtime` accepted `callback_data` above as the live ProjFS
    // callback record.
    if unsafe { (*callback_data).TriggeringProcessId } == std::process::id()
        && matches!(
            notification,
            PRJ_NOTIFICATION_FILE_OPENED | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION
        )
        && (MetadataProbeGuard::is_active(runtime.metadata_probes.as_ref(), &path)
            || runtime.capture_probes.read().unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter().any(|(active, ..)| matches!(projfs_path_suffix(active, &path), Some([]))))
    {
        return HR_OK;
    }
    let file_id = file_id(data);
    // SAFETY: ProjFS passes these names as null or NUL-terminated strings
    // valid for the callback.
    let source_is_external = unsafe { empty_destination(data.FilePathName) };
    // SAFETY: as above.
    let destination_is_external = unsafe { empty_destination(destination_filename) };
    if notification == PRJ_NOTIFICATION_PRE_SET_HARDLINK
        && (source_is_external
            || destination_is_external
            || path_from(destination_filename).is_none())
    {
        // A cross-root hard link would let writes outside the projection
        // mutate an admitted file without another ProjFS callback.
        return HR_NOT_SAME_DEVICE;
    }
    let invalid_rename = source_is_external && destination_is_external
        || is_directory && (source_is_external || destination_is_external)
        || !destination_is_external && path_from(destination_filename).is_none();
    if notification == PRJ_NOTIFICATION_PRE_RENAME && invalid_rename {
        // A directory crossing needs a deferred recursive boundary after the
        // source process has finished moving descendants. Reject it until the
        // provider can prove that boundary instead of admitting a partial tree.
        return HR_NOT_SAME_DEVICE;
    }
    if notification == PRJ_NOTIFICATION_PRE_RENAME
        || notification == PRJ_NOTIFICATION_PRE_SET_HARDLINK
    {
        return HR_OK;
    }
    if notification == PRJ_NOTIFICATION_NEW_FILE_CREATED {
        // The physical file is already present. One final-state capture at
        // the next operation boundary covers every write before it; the file
        // keeps every notification, because a later boundary must still see
        // its writes, metadata edits, and deletion.
        if is_directory {
            lock_recover(runtime.post_operation_failure.as_ref())
                .queue_subtree(&path, "new directory awaiting capture".to_owned());
        } else {
            defer_host_capture(runtime.post_operation_failure.as_ref(), path);
        }
        keep_file_notifications(notification, operation_parameters);
        return HR_OK;
    }
    if !is_directory && notification == PRJ_NOTIFICATION_FILE_OVERWRITTEN {
        keep_file_notifications(notification, operation_parameters);
        return HR_OK;
    }
    let destination = (!destination_is_external)
        .then(|| path_from(destination_filename))
        .flatten();
    if path.components().is_empty() && notification == PRJ_NOTIFICATION_FILE_OPENED {
        return HR_OK;
    }
    if notification == PRJ_NOTIFICATION_HARDLINK_CREATED {
        let Some(destination) = destination else {
            lock_recover(runtime.post_operation_failure.as_ref()).unreplayable = Some(format!(
                "{path:?}: hard-link destination is invalid after host completion"
            ));
            return HR_INVALID_DATA;
        };
        // The link already exists physically. Both names must be captured in
        // one later batch to recover their shared identity, but enqueueing the
        // two paths cannot fail and does not touch checkout state.
        defer_host_capture(runtime.post_operation_failure.as_ref(), path);
        defer_host_capture(runtime.post_operation_failure.as_ref(), destination);
        return HR_OK;
    }
    let source = Arc::clone(&runtime.source);
    let metadata_baselines = Arc::clone(&runtime.metadata_baselines);
    let projection = Arc::clone(&runtime.projection);
    let metadata_probes = Arc::clone(&runtime.metadata_probes);
    let metadata_edits = Arc::clone(&runtime.metadata_edits);
    let metadata_root = Arc::clone(&runtime.metadata_root);
    let post_operation_failure = Arc::clone(&runtime.post_operation_failure);
    let operation_failures = Arc::clone(&post_operation_failure);
    let projector = Arc::clone(&runtime.projector);
    let failure_path = path.clone();
    let retry_path = Arc::new(Mutex::new(None));
    let operation_retry_path = Arc::clone(&retry_path);
    // ProjFS reports every notification at the file's current name, also for
    // a handle that renamed it, so no per-handle name is remembered.
    let operation = move || {
        if path.components().is_empty()
            && (notification == PRJ_NOTIFICATION_FILE_OPENED
                || notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION)
        {
            return Ok(());
        }
        if notification == PRJ_NOTIFICATION_FILE_OPENED {
            if has_pending_capture(operation_failures.as_ref(), &path) {
                // A boundary will capture this path's final state; its close
                // needs no baseline to compare against.
                return Ok(());
            }
            let baseline = probe_host_windows_metadata(
                metadata_root.as_ref(),
                &path,
                metadata_probes.as_ref(),
            )?;
            record_metadata_open(metadata_baselines.as_ref(), file_id, baseline)
        } else if notification == PRJ_NOTIFICATION_NEW_FILE_CREATED
            || notification == PRJ_NOTIFICATION_FILE_OVERWRITTEN
        {
            if is_directory {
                defer_host_capture(operation_failures.as_ref(), path);
                Ok(())
            } else {
                Ok(())
            }
        } else if notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED
            || notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED
        {
            let close = close_metadata_handle(metadata_baselines.as_ref(), file_id);
            let final_close = matches!(close, MetadataClose::Final(_));
            let result = if notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED {
                // The delete removed exactly this name; other links remain.
                lock_recover(operation_failures.as_ref())
                    .pending_captures
                    .remove(&path);
                *lock_recover(operation_retry_path.as_ref()) = Some(path.clone());
                match source.lookup(&path)? {
                    Some(_) => source.remove(&path, None),
                    None => Ok(()),
                }
            } else {
                defer_host_capture(operation_failures.as_ref(), path.clone());
                Ok(())
            };
            if result.is_ok()
                && !final_close
                && notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED
            {
                let baseline = probe_host_windows_metadata(
                    metadata_root.as_ref(),
                    &path,
                    metadata_probes.as_ref(),
                )?;
                refresh_metadata_baseline(metadata_baselines.as_ref(), file_id, baseline);
            }
            result
        } else if notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION {
            let baseline = match close_metadata_handle(metadata_baselines.as_ref(), file_id) {
                MetadataClose::Pending => return Ok(()),
                MetadataClose::Final(baseline) => baseline,
            };
            if has_pending_capture(operation_failures.as_ref(), &path) {
                // The next boundary captures this path's final host state,
                // metadata included, after this close.
                return Ok(());
            }
            let capture_path = path;
            let Some(baseline) = baseline else {
                // Opened while a capture was pending, which a boundary has
                // since taken: capture again so no edit through this handle
                // after that boundary is missed.
                defer_host_capture(operation_failures.as_ref(), capture_path);
                return Ok(());
            };
            *lock_recover(operation_retry_path.as_ref()) = Some(capture_path.clone());
            let host = probe_host_windows_metadata(
                metadata_root.as_ref(),
                &capture_path,
                metadata_probes.as_ref(),
            )?;
            if baseline.identity != host.identity
                || baseline.links != host.links
                || baseline.size != host.size
            {
                lock_recover(projection.as_ref())
                    .bindings
                    .remove(&capture_path);
                defer_host_capture(operation_failures.as_ref(), capture_path);
                return Ok(());
            }
            let cached_binding = lock_recover(projection.as_ref())
                .bindings
                .get(&capture_path)
                .copied();
            if !metadata_changed_since_open(baseline, host)
                && !has_pending_capture(operation_failures.as_ref(), &capture_path)
                && let Some(binding) = cached_binding
                && binding.host == host
                && host.links == Some(1)
                && let Ok(_view_lease) = source.acquire_view_lease()
                && binding.basis.still_describes(
                    source.as_ref(),
                    &capture_path,
                    Some(binding.file_id),
                )
            {
                return Ok(());
            }
            let basis = ReadBasis::sample(source.as_ref());
            let lookup = source.lookup(&capture_path);
            match lookup {
                Ok(Some(_)) if metadata_changed_since_open(baseline, host) => {
                    metadata_edits.with(&host.identity, || {
                        let Some(lookup) = source.lookup(&capture_path)? else {
                            defer_host_capture(operation_failures.as_ref(), capture_path);
                            return Ok(());
                        };
                        capture_changed_windows_metadata(
                            source.as_ref(),
                            &capture_path,
                            lookup.metadata,
                            baseline,
                            host,
                        )
                    })
                }
                Ok(Some(lookup)) => {
                    if let Some(basis) = basis
                        && !has_pending_capture(operation_failures.as_ref(), &capture_path)
                        && host.links == Some(1)
                    {
                        lock_recover(projection.as_ref()).remember_binding(
                            capture_path,
                            ReadOnlyBinding {
                                host,
                                file_id: lookup.node.file_id,
                                basis,
                            },
                        );
                    }
                    Ok(())
                }
                Ok(None) => {
                    lock_recover(projection.as_ref())
                        .bindings
                        .remove(&capture_path);
                    defer_host_capture(operation_failures.as_ref(), capture_path);
                    Ok(())
                }
                Err(error) => Err(error),
            }
        } else if notification == PRJ_NOTIFICATION_FILE_RENAMED {
            // No listing is reconciled while the source has the rename and
            // the projection's records do not yet.
            let _tree = lock_recover(&projector.tree);
            let current = projector.current_beneath(source.as_ref(), &path);
            let result = handle_rename_source(
                source.as_ref(),
                &path,
                source_is_external,
                destination_is_external,
                is_directory,
                destination.clone(),
                operation_failures.as_ref(),
            );
            if result.is_ok()
                && !source_is_external
                && !destination_is_external
                && let Some(destination) = destination.as_ref()
            {
                lock_recover(operation_failures.as_ref())
                    .rename_pending_captures(&path, destination);
                let carried = Carried::read(source.as_ref(), &path, destination, &current);
                projector.renamed(&path, destination, carried);
            }
            result
        } else {
            Err(MountSourceError::Unsupported(
                "ProjFS emitted an unadmitted write notification".to_owned(),
            ))
        }
    };
    let result = runtime
        .callbacks
        .call_observed(file_id, operation, move |result| {
            let retry_path = lock_recover(retry_path.as_ref());
            record_post_operation_failure(
                post_operation_failure.as_ref(),
                notification,
                &failure_path,
                retry_path.as_ref(),
                result,
            );
        });
    keep_file_notifications(notification, operation_parameters);
    match result {
        Some(Ok(())) => HR_OK,
        Some(Err(error)) => source_hresult(&error),
        None => HR_UNEXPECTED,
    }
}

/// Keeps [`FILE_NOTIFICATIONS`] for a file whose notification set `ProjFS`
/// lets the provider replace on this callback.
fn keep_file_notifications(
    notification: PRJ_NOTIFICATION,
    operation_parameters: *mut PRJ_NOTIFICATION_PARAMETERS,
) {
    if operation_parameters.is_null() {
        return;
    }
    // SAFETY: ProjFS supplies the writable union member matching each of
    // these notifications for the callback's duration.
    unsafe {
        match notification {
            PRJ_NOTIFICATION_NEW_FILE_CREATED | PRJ_NOTIFICATION_FILE_OVERWRITTEN => {
                (*operation_parameters).PostCreate.NotificationMask = FILE_NOTIFICATIONS;
            }
            PRJ_NOTIFICATION_FILE_RENAMED => {
                (*operation_parameters).FileRenamed.NotificationMask = FILE_NOTIFICATIONS;
            }
            _ => {}
        }
    }
}

fn handle_rename_source(
    source_fs: &dyn MountFilesystem,
    source: &MountPath,
    source_is_external: bool,
    destination_is_external: bool,
    is_directory: bool,
    destination: Option<MountPath>,
    post_operation_failures: &Mutex<PostOperationFailures>,
) -> Result<(), MountSourceError> {
    if source_is_external {
        let destination = destination
            .ok_or_else(|| MountSourceError::Invalid("rename destination is invalid".to_owned()))?;
        if is_directory {
            lock_recover(post_operation_failures).queue_subtree(
                &destination,
                "imported directory awaiting capture".to_owned(),
            );
        } else {
            defer_host_capture(post_operation_failures, destination);
        }
        return Ok(());
    }
    if destination_is_external {
        // The source vanished from the projection. Capturing that exact
        // now-missing path records its deletion without reading outside.
        defer_host_capture(post_operation_failures, source.clone());
        return Ok(());
    }
    let destination = destination
        .ok_or_else(|| MountSourceError::Invalid("rename destination is invalid".to_owned()))?;
    if source_fs.lookup(source)?.is_none() {
        // A full file created inside ProjFS can be renamed before its final
        // close notification has captured it into the SDK. The host rename
        // already happened, so import the destination instead of attempting
        // to rename a binding that does not yet exist in the source view.
        if is_directory {
            lock_recover(post_operation_failures).queue_subtree(
                &destination,
                "renamed directory awaiting capture".to_owned(),
            );
        } else {
            defer_host_capture(post_operation_failures, destination);
        }
        return Ok(());
    }
    source_fs.rename(source, &destination, true)?;
    source_fs.flush()
}

unsafe extern "system" fn cancel(_callback_data: *const PRJ_CALLBACK_DATA) {}

/// The `ProjFS` entry point for `callback`: it runs the callback on the
/// provider's stack, not on the stack of the thread `ProjFS` calls from.
macro_rules! on_provider_stack {
    ($level:ident, $callback:ident($data:ident: $data_type:ty $(, $argument:ident: $type:ty)*)) => {{
        unsafe extern "system" fn entry($data: $data_type $(, $argument: $type)*) -> HRESULT {
            let span = callback_span!($level, $callback);
            // SAFETY: the callback data and its heap-stable runtime remain
            // owned until the SDK drain has returned every callback.
            let callback_runtime = unsafe { runtime($data) }.map(|(_, runtime)| runtime);
            let meter = callback_runtime.and_then(|runtime| {
                lock_recover(&runtime.stop_meter).as_ref().map(Arc::clone)
            });
            // No finite-scope ordinary fallback: denial is retained as an
            // unreplayable typed control failure before the handler can run.
            let operation = || unsafe { $callback($data $(, $argument)*) };
            let invoked = match meter.as_ref() {
                Some(meter) => provider_stack::run_with_meter(Some(meter), operation),
                None => provider_stack::run(operation)
                    .map_err(|error| provider_stack::ProviderFailure::Native(error.code())),
            };
            let result = match invoked {
                Ok(result) => result,
                Err(error) => {
                    if meter.is_some() && let Some(runtime) = callback_runtime {
                        lock_recover(runtime.post_operation_failure.as_ref())
                            .provider_control_failure.get_or_insert(error);
                    }
                    error.hresult()
                }
            };
            if result.is_err() {
                span.record("errno", result.0);
            }
            result
        }
        entry
    }};
}

fn callbacks() -> PRJ_CALLBACKS {
    PRJ_CALLBACKS {
        StartDirectoryEnumerationCallback: Some(on_provider_stack!(TRACE, start_directory(
            callback_data: *const PRJ_CALLBACK_DATA,
            enumeration_id: *const GUID
        ))),
        EndDirectoryEnumerationCallback: Some(on_provider_stack!(TRACE, end_directory(
            callback_data: *const PRJ_CALLBACK_DATA,
            enumeration_id: *const GUID
        ))),
        GetDirectoryEnumerationCallback: Some(on_provider_stack!(TRACE, get_directory(
            callback_data: *const PRJ_CALLBACK_DATA,
            enumeration_id: *const GUID,
            search_expression: PCWSTR,
            buffer: PRJ_DIR_ENTRY_BUFFER_HANDLE
        ))),
        GetPlaceholderInfoCallback: Some(on_provider_stack!(TRACE, placeholder(
            callback_data: *const PRJ_CALLBACK_DATA
        ))),
        GetFileDataCallback: Some(on_provider_stack!(TRACE, file_data(
            callback_data: *const PRJ_CALLBACK_DATA,
            byte_offset: u64,
            length: u32
        ))),
        QueryFileNameCallback: Some(on_provider_stack!(TRACE, query_name(
            callback_data: *const PRJ_CALLBACK_DATA
        ))),
        NotificationCallback: Some(on_provider_stack!(DEBUG, notification(
            callback_data: *const PRJ_CALLBACK_DATA,
            is_directory: bool,
            kind: PRJ_NOTIFICATION,
            destination_filename: PCWSTR,
            operation_parameters: *mut PRJ_NOTIFICATION_PARAMETERS
        ))),
        CancelCommandCallback: Some(cancel),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CallbackGate, PostOperationFailures, ReadBasis, flush_callback_gate,
        record_post_operation_failure,
        remove_authenticated_destination, source_hresult,
    };
    use crate::kernel::FileMetadata;
    use crate::model::{
        AccessMode, CheckoutMode, ConsistencyMode, GenerationSelector, Lifecycle, MutationMode,
        VolumeConfig,
    };
    use crate::native_mount::adapter::{CheckoutMountSource, SharedCheckout};
    use crate::native_mount::{MountFilesystem, MountPath, MountSourceError};
    use crate::{
        CancellationToken, Fs, IdempotencyKey, LocalOptions, MountOptions, MountPublication,
        WorkBudget,
    };
    use bytes::Bytes;
    use std::collections::HashSet;
    use std::sync::{Arc, Barrier, Mutex};
    use windows::Win32::Storage::ProjectedFileSystem::{
        PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED, PRJ_NOTIFICATION_PRE_DELETE,
        PRJ_VIRTUALIZATION_INSTANCE_INFO, PrjGetVirtualizationInstanceInfo,
        PrjMarkDirectoryAsPlaceholder, PrjStartVirtualizing, PrjStopVirtualizing,
    };
    use windows::core::{GUID, HSTRING, PCWSTR};

    /// Files are staged only in a plain directory beside the projection:
    /// a link in its place, which would lead elsewhere, is refused.
    #[test]
    fn staging_refuses_a_link_in_place_of_its_directory() -> Result<(), Box<dyn std::error::Error>>
    {
        let parent = tempfile::tempdir()?;
        let root = parent.path().join("mount");
        std::fs::create_dir(&root)?;
        assert!(super::staging_directory(&root).is_ok());
        std::fs::remove_dir(super::staging_path(&root))?;
        let elsewhere = tempfile::tempdir()?;
        std::os::windows::fs::symlink_dir(elsewhere.path(), super::staging_path(&root))?;
        assert!(super::staging_directory(&root).is_err());
        Ok(())
    }

    #[test]
    fn recovery_read_denial_does_not_probe_or_take_native_root_controls()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        let authored = root.path().join("authored");
        std::fs::write(&authored, b"preserve")?;
        let token = crate::CancellationToken::new();
        let mut work = crate::demand::DemandWork::new(
            crate::WorkBudget { backend_read_operations: 0, ..crate::WorkBudget::UNBOUNDED },
            &token,
        );
        let mut control = super::ProjFsRecoveryControl::default();
        assert!(matches!(
            control.recover_measured(root.path(), true, &mut work),
            Err(super::NativeMountError::Work(_)),
        ));
        assert_eq!(work.work.backend_read_operations, 0);
        assert_eq!(work.work.backend_write_operations, 0);
        assert_eq!(work.live_allocation_bytes(), 0);
        assert!(!control.controls_pending());
        assert!(!control.namespace_detach_confirmed());
        assert_eq!(std::fs::read(&authored)?, b"preserve");
        Ok(())
    }

    #[test]
    fn recovery_reparse_buffer_denial_retains_real_capability_for_retry()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        let authored = root.path().join("authored");
        std::fs::write(&authored, b"preserve")?;
        let token = crate::CancellationToken::new();
        let mut control = super::ProjFsRecoveryControl::default();
        let mut observation = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &token);
        control.recover_measured(root.path(), false, &mut observation)?;
        assert!(!control.controls_pending());
        assert!(!control.namespace_detach_confirmed());
        assert!(observation.work.backend_read_operations > 0);
        assert_eq!(observation.work.backend_write_operations, 0);

        // Preserve mode observed the same concrete path/open/identity prefix.
        // This exact allocation quota permits that prefix but not the next
        // actual DeviceIoControl reparse-data buffer in disposable mode.
        let mut bounded = crate::demand::DemandWork::new(
            crate::WorkBudget {
                allocation_operations: observation.work.allocation_operations,
                ..crate::WorkBudget::UNBOUNDED
            },
            &token,
        );
        assert!(matches!(
            control.recover_measured(root.path(), true, &mut bounded),
            Err(super::NativeMountError::Work(_)),
        ));
        assert!(control.controls_pending());
        assert_eq!(bounded.work.allocation_operations, observation.work.allocation_operations);
        assert_eq!(bounded.work.backend_read_operations, observation.work.backend_read_operations);
        assert_eq!(bounded.work.backend_write_operations, 0);
        assert_eq!(bounded.live_allocation_bytes(), 0);
        assert_eq!(std::fs::read(&authored)?, b"preserve");

        let mut retry = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &token);
        control.recover_measured(root.path(), true, &mut retry)?;
        assert!(!control.controls_pending());
        assert!(!control.namespace_detach_confirmed());
        assert_eq!(retry.work.backend_write_operations, 0);
        assert_eq!(retry.live_allocation_bytes(), 0);
        assert_eq!(std::fs::read(&authored)?, b"preserve");
        Ok(())
    }

    #[test]
    fn recovery_retained_capability_never_adopts_a_replaced_root()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let parent = tempfile::tempdir()?;
        let root = parent.path().join("root");
        let residue = parent.path().join("residue");
        std::fs::create_dir(&root)?;
        std::fs::write(root.join("original"), b"original")?;
        let token = crate::CancellationToken::new();
        let mut control = super::ProjFsRecoveryControl::default();
        let mut observation = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &token);
        control.recover_measured(&root, false, &mut observation)?;
        let mut bounded = crate::demand::DemandWork::new(
            crate::WorkBudget {
                allocation_operations: observation.work.allocation_operations,
                ..crate::WorkBudget::UNBOUNDED
            },
            &token,
        );
        assert!(matches!(
            control.recover_measured(&root, true, &mut bounded),
            Err(super::NativeMountError::Work(_)),
        ));
        assert!(control.controls_pending());
        std::fs::rename(&root, &residue)?;
        std::fs::create_dir(&root)?;
        std::fs::write(root.join("replacement"), b"replacement")?;
        let mut retry = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &token);
        assert!(matches!(
            control.recover_measured(&root, true, &mut retry),
            Err(super::NativeMountError::Demand(crate::DemandError::StaleSource)),
        ));
        assert!(control.controls_pending());
        assert_eq!(retry.work.backend_write_operations, 0);
        assert_eq!(retry.live_allocation_bytes(), 0);
        assert_eq!(std::fs::read(root.join("replacement"))?, b"replacement");
        assert_eq!(std::fs::read(residue.join("original"))?, b"original");
        Ok(())
    }

    #[test]
    fn empty_recovery_cleanup_admits_actual_disposition_but_never_recurses()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let parent = tempfile::tempdir()?;
        let token = crate::CancellationToken::new();
        let baseline = parent.path().join("baseline");
        std::fs::create_dir(&baseline)?;
        let baseline_identity = crate::capture_root_identity(&baseline)?;
        let mut observed = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &token);
        super::remove_empty_recovery_destination_measured(
            &baseline, Some(baseline_identity), &mut observed,
        )?;
        assert!(!baseline.exists());
        let actual_dispositions = observed.work.backend_write_operations;
        assert!((1..=2).contains(&actual_dispositions), "one actual POSIX disposition and only its supported fallback");

        let root = parent.path().join("root");
        std::fs::create_dir(&root)?;
        let identity = crate::capture_root_identity(&root)?;
        let mut denied = crate::demand::DemandWork::new(
            crate::WorkBudget { backend_write_operations: 0, ..crate::WorkBudget::UNBOUNDED },
            &token,
        );
        assert!(matches!(
            super::remove_empty_recovery_destination_measured(&root, Some(identity), &mut denied),
            Err(super::NativeMountError::Work(_)),
        ));
        assert_eq!(denied.work.backend_write_operations, 0);
        assert_eq!(denied.live_allocation_bytes(), 0);
        assert!(root.is_dir());
        let mut exact = crate::demand::DemandWork::new(
            crate::WorkBudget {
                backend_write_operations: actual_dispositions,
                ..crate::WorkBudget::UNBOUNDED
            },
            &token,
        );
        super::remove_empty_recovery_destination_measured(&root, Some(identity), &mut exact)?;
        assert_eq!(exact.work.backend_write_operations, actual_dispositions);
        assert_eq!(exact.live_allocation_bytes(), 0);
        assert!(!root.exists());

        std::fs::create_dir(&root)?;
        std::fs::write(root.join("authored"), b"must not be recursively removed")?;
        let identity = crate::capture_root_identity(&root)?;
        let mut nonempty = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &token);
        assert!(matches!(
            super::remove_empty_recovery_destination_measured(&root, Some(identity), &mut nonempty),
            Err(super::NativeMountError::Io(_)),
        ));
        assert_eq!(nonempty.live_allocation_bytes(), 0);
        assert_eq!(std::fs::read(root.join("authored"))?, b"must not be recursively removed");
        Ok(())
    }

    #[test]
    fn empty_recovery_cleanup_refuses_root_created_after_absent_constructor_observation()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        let token = crate::CancellationToken::new();
        let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &token);
        assert!(matches!(
            super::remove_empty_recovery_destination_measured(root.path(), None, &mut work),
            Err(super::NativeMountError::Demand(crate::DemandError::StaleSource)),
        ));
        assert_eq!(work.work.backend_write_operations, 0);
        assert_eq!(work.live_allocation_bytes(), 0);
        assert!(root.path().is_dir());
        Ok(())
    }

    #[test]
    fn projfs_name_order_differs_from_sdk_cursor_order() {
        let name = windows_name;
        assert_eq!(
            super::compare_projfs_name(&name("Alpha"), &name("alpha")),
            Some(0)
        );
        assert!(name("Alpha") < name("alpha"));
        assert!(super::compare_projfs_name(&name("before"), &name("later")) < Some(0));
        assert!(super::compare_projfs_name(&name("later"), &name("before")) > Some(0));
        let mut projected = ["éclair", "Zebra", "entry000", "Alpha"];
        projected.sort_unstable_by(|left, right| {
            super::compare_projfs_name(&name(left), &name(right))
                .expect("valid Windows name")
                .cmp(&0)
        });
        assert_eq!(projected, ["Alpha", "entry000", "Zebra", "éclair"]);
        assert_ne!(
            ["Alpha", "Zebra", "entry000", "éclair"],
            projected,
            "SDK cursor order differs from the ProjFS merge order"
        );
    }

    type MemorySource = CheckoutMountSource<
        crate::facade::MemoryAuthorityBackend,
        crate::facade::MemoryObjectBackend,
    >;

    fn windows_name(name: &str) -> Vec<u8> {
        name.encode_utf16().flat_map(u16::to_le_bytes).collect()
    }

    fn windows_path(name: &str) -> MountPath {
        MountPath::root().child(windows_name(name))
    }

    /// One writable Windows-profile checkout, not yet mounted.
    async fn windows_checkout_source() -> Result<Arc<MemorySource>, Box<dyn std::error::Error>> {
        let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
        config.profile = crate::model::FilesystemProfile::Windows;
        let fs = Fs::memory();
        let cancellation = CancellationToken::new();
        let volume = fs
            .create_volume(config, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        let checkout = volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode {
                    access: AccessMode::ReadWrite,
                    consistency: ConsistencyMode::Pinned,
                    mutations: MutationMode::PrivateOverlay,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        Ok(Arc::new(CheckoutMountSource::new(
            Arc::new(SharedCheckout::new(checkout)),
            config,
        )?))
    }

    /// Mounts `source` writable at a fresh destination.
    fn mount_source(
        source: &Arc<MemorySource>,
    ) -> Result<
        (
            tempfile::TempDir,
            std::path::PathBuf,
            crate::NativeMountSession,
        ),
        Box<dyn std::error::Error>,
    > {
        let root = tempfile::tempdir()?;
        let destination = root.path().join("projection");
        std::fs::create_dir(&destination)?;
        let session = crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id: source.volume_id()?,
                destination: destination.clone(),
                writable: true,
            },
            Arc::clone(source) as Arc<dyn MountFilesystem>,
        )?;
        Ok((root, destination, session))
    }

    fn mount_driver_source(
        source: &Arc<MemorySource>,
    ) -> Result<(tempfile::TempDir, std::path::PathBuf, super::ProjFsSession), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let destination = root.path().join("projection");
        std::fs::create_dir(&destination)?;
        let session = super::ProjFsSession::start(
            &crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id: source.volume_id()?,
                destination: destination.clone(),
                writable: true,
            },
            Arc::clone(source) as Arc<dyn MountFilesystem>,
        ).map_err(|failure| failure.error)?;
        Ok((root, destination, session))
    }

    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn measured_stop_denies_the_actual_sdk_boundary_without_stopping_the_context()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = windows_checkout_source().await?;
        let (_root, destination, mut session) = mount_driver_source(&source)?;
        let mut work = crate::demand::DemandWork::new(
            WorkBudget { backend_write_operations: 0, ..WorkBudget::UNBOUNDED },
            &CancellationToken::new(),
        );
        let error = session.stop_measured(&mut work).expect_err("SDK detach must be admitted");
        assert!(matches!(error, crate::NativeMountError::Work(crate::WorkError::BudgetExceeded {
            counter: "backend_write_operations", ..
        })));
        assert_eq!(work.work.backend_write_operations, 0);
        assert_eq!(work.live_allocation_bytes(), 0);
        assert!(session.context.is_some());
        assert!(session.runtime.is_some());
        assert!(destination.is_dir());
        let mut info = PRJ_VIRTUALIZATION_INSTANCE_INFO::default();
        // SAFETY: denial retained the actual live SDK context.
        unsafe { PrjGetVirtualizationInstanceInfo(session.context.expect("live context"), &raw mut info) }?;
        session.stop()?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn measured_stop_records_the_sdk_prefix_and_retains_cleanup_custody()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = windows_checkout_source().await?;
        let (_root, destination, mut session) = mount_driver_source(&source)?;
        // Force one real staging-directory disposition before the SDK stop.
        drop(super::staging_directory(&destination).map_err(std::io::Error::other)?);
        let mut work = crate::demand::DemandWork::new(
            WorkBudget { backend_write_operations: 2, ..WorkBudget::UNBOUNDED },
            &CancellationToken::new(),
        );
        let error = session.stop_measured(&mut work).expect_err("physical root delete has no remaining write");
        assert!(matches!(error, crate::NativeMountError::Work(crate::WorkError::BudgetExceeded {
            counter: "backend_write_operations", ..
        })));
        assert_eq!(work.work.backend_write_operations, 2, "staging disposition and SDK stop really ran");
        assert!(work.work.backend_read_operations > 0, "actual root identity, reparse and enumeration probes ran");
        assert_eq!(work.live_allocation_bytes(), 0);
        assert!(session.context.is_none(), "SDK context is not stopped twice on retry");
        assert!(session.runtime.is_none(), "drained callback state no longer pins the deleted root");
        assert!(session.detached.is_some(), "authenticated root custody survives the denied deletion");
        assert!(!session.destination_removed);
        assert!(destination.is_dir());
        assert!(!super::staging_path(&destination).exists());
        session.stop()?;
        assert!(destination.is_dir());
        assert_eq!(std::fs::read_dir(&destination)?.count(), 0);
        assert!(session.runtime.is_none());
        assert!(session.detached.is_none());
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn measured_stop_cancellation_preserves_the_actual_driver_and_namespace()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = windows_checkout_source().await?;
        let seed = windows_path("seed.txt");
        source.create_file(&seed, FileMetadata::default())?;
        source.write_range(&seed, 0, Bytes::from_static(b"seed"))?;
        let (_root, destination, mut session) = mount_driver_source(&source)?;
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        session.stop_measured(&mut work).expect_err("original cancellation must win");
        assert_eq!(work.work, crate::WorkCounters::UNCHARGED);
        assert!(session.context.is_some());
        assert!(session.runtime.is_some());
        assert_eq!(std::fs::read(destination.join("seed.txt"))?, b"seed");
        session.stop()?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn measured_capture_denial_preserves_the_authored_namespace_before_sdk_stop()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = windows_checkout_source().await?;
        let (_root, destination, mut session) = mount_driver_source(&source)?;
        let path = windows_path("authored.txt");
        std::fs::write(destination.join("authored.txt"), b"only-host-copy")?;
        let runtime = session.runtime.as_ref().expect("live callback runtime");
        runtime.post_operation_failure.lock().expect("capture state")
            .queue_capture(path.clone(), "authored close awaiting capture".to_owned());
        let serial = runtime.post_operation_failure.lock().expect("capture state").pending_captures[&path].serial;
        let mut work = crate::demand::DemandWork::new(
            WorkBudget { source_bytes_read: 0, ..WorkBudget::UNBOUNDED },
            &CancellationToken::new(),
        );
        let error = session.stop_measured(&mut work).expect_err("host capture must use the caller's exact byte allowance");
        assert!(matches!(error, crate::NativeMountError::Work(_)));
        assert_eq!(work.work.source_bytes_read, 0);
        assert!(!session.namespace_detach_confirmed());
        assert!(session.context.is_some());
        let runtime = session.runtime.as_ref().expect("capture failure retains callbacks");
        assert_eq!(runtime.post_operation_failure.lock().expect("capture state").pending_captures[&path].serial, serial);
        assert_eq!(std::fs::read(destination.join("authored.txt"))?, b"only-host-copy");
        assert!(source.lookup(&path)?.is_none(), "a failed capture has no partial checkout candidate");
        session.stop()?;
        assert!(session.namespace_detach_confirmed());
        assert!(session.cleanup_root_identity().is_none());
        assert_eq!(source.read_range(&path, 0, 14)?, b"only-host-copy"[..]);
        assert_eq!(std::fs::read_dir(&destination)?.count(), 0);
        Ok(())
    }

    #[test]
    fn measured_root_recreation_has_one_exact_create_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let parent = tempfile::tempdir()?;
        let destination = parent.path().join("projection");
        let mut denied = crate::demand::DemandWork::new(
            WorkBudget { backend_write_operations: 0, ..WorkBudget::UNBOUNDED },
            &CancellationToken::new(),
        );
        let error = super::recreate_destination_measured(&destination, &mut denied)
            .expect_err("create-only boundary is denied");
        assert!(matches!(error, crate::NativeMountError::Work(_)));
        assert_eq!(denied.work.backend_write_operations, 0);
        assert_eq!(denied.live_allocation_bytes(), 0);
        assert!(!destination.exists());
        let mut admitted = crate::demand::DemandWork::new(
            WorkBudget { backend_write_operations: 1, ..WorkBudget::UNBOUNDED },
            &CancellationToken::new(),
        );
        super::recreate_destination_measured(&destination, &mut admitted)?;
        assert_eq!(admitted.work.backend_write_operations, 1);
        assert_eq!(admitted.live_allocation_bytes(), 0);
        assert!(destination.is_dir());
        std::fs::write(destination.join("do-not-adopt"), b"authored")?;
        let mut repeated = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
        super::recreate_destination_measured(&destination, &mut repeated)
            .expect_err("FILE_CREATE must not adopt another root");
        assert_eq!(std::fs::read(destination.join("do-not-adopt"))?, b"authored");
        assert_eq!(repeated.work.backend_write_operations, 1);
        assert_eq!(repeated.live_allocation_bytes(), 0);
        Ok(())
    }

    #[test]
    fn measured_physical_walk_keeps_an_exact_deleted_prefix_when_the_root_write_is_denied()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::windows::fs::OpenOptionsExt as _;
        use windows::Win32::Storage::FileSystem::{
            DELETE, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_GENERIC_READ,
            FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
        };
        let parent = tempfile::tempdir()?;
        let destination = parent.path().join("projection");
        std::fs::create_dir(&destination)?;
        std::fs::write(destination.join("child"), b"projected")?;
        let file = std::fs::OpenOptions::new()
            .access_mode(DELETE.0 | FILE_GENERIC_READ.0)
            .share_mode((FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE).0)
            .custom_flags((FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS).0)
            .open(&destination)?;
        let directory = cap_std::fs::Dir::from_std_file(file);
        let mut work = crate::demand::DemandWork::new(
            WorkBudget { backend_write_operations: 1, ..WorkBudget::UNBOUNDED },
            &CancellationToken::new(),
        );
        let error = super::remove_projection_tree(&directory, None, &mut work)
            .expect_err("the actual root disposition is denied after its child was removed");
        assert!(matches!(error, crate::DemandError::Work(_)));
        assert_eq!(work.work.backend_write_operations, 1);
        assert!(work.work.backend_read_operations > 0);
        assert!(work.work.items_examined > 0);
        assert!(work.work.peak_allocation_bytes >= 64 * 1024, "the real native enumeration buffer was admitted");
        assert_eq!(work.live_allocation_bytes(), 0);
        assert!(destination.is_dir());
        assert!(!destination.join("child").exists(), "the receipt preserves actual completed prefix work");
        super::remove_projection_tree(
            &directory, None,
            &mut crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()),
        )?;
        drop(directory);
        assert!(!destination.exists());
        Ok(())
    }

    #[test]
    fn measured_callback_snapshot_denial_preserves_the_original_capture_serial() {
        let callbacks = CallbackGate::start().expect("callback gate");
        let failures = Mutex::new(PostOperationFailures::default());
        let path = windows_path("authored.txt");
        failures.lock().expect("failure state").queue_capture(path.clone(), "unjoined write".to_owned());
        let serial = failures.lock().expect("failure state").pending_captures[&path].serial;
        let mut work = crate::demand::DemandWork::new(
            WorkBudget { peak_allocation_bytes: 0, ..WorkBudget::UNBOUNDED },
            &CancellationToken::new(),
        );
        let error = super::flush_callback_gate_measured(&callbacks, &failures, None, &mut work, |_, _| {
            panic!("a denied snapshot must never enter source capture")
        }).expect_err("snapshot envelope is admitted before allocation");
        assert!(matches!(error, crate::NativeMountError::Work(_)));
        assert_eq!(work.work.allocation_operations, 0);
        assert_eq!(work.live_allocation_bytes(), 0);
        assert_eq!(failures.lock().expect("failure state").pending_captures[&path].serial, serial);
    }

    #[test]
    fn measured_callback_capture_failure_preserves_typed_work_and_releases_its_controls() {
        let callbacks = CallbackGate::start().expect("callback gate");
        let failures = Mutex::new(PostOperationFailures::default());
        let path = windows_path("authored.txt");
        failures.lock().expect("failure state").queue_capture(path.clone(), "unjoined write".to_owned());
        let serial = failures.lock().expect("failure state").pending_captures[&path].serial;
        let probes = std::sync::RwLock::new(Vec::new());
        let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
        let expected = crate::WorkError::BudgetExceeded {
            counter: "object_bytes_read", observed: 7, maximum: 6,
        };
        let error = super::flush_callback_gate_measured(&callbacks, &failures, Some(&probes), &mut work, |paths, work| {
            assert_eq!(paths[0].0, path);
            assert_eq!(probes.read().expect("borrowed probes").len(), 1);
            work.charge(crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED })
                .map_err(super::detach_work_error)?;
            Err(super::detach_source_error(MountSourceError::Work(expected)))
        }).expect_err("typed source admission failure must fence cleanup");
        assert!(matches!(error, crate::NativeMountError::Work(error) if error == expected));
        assert_eq!(work.work.backend_read_operations, 1);
        assert!(work.work.peak_allocation_bytes > 0);
        assert_eq!(work.live_allocation_bytes(), 0);
        assert!(probes.read().expect("retired controls").is_empty());
        assert_eq!(failures.lock().expect("failure state").pending_captures[&path].serial, serial);
    }

    #[test]
    fn measured_callback_drain_observes_original_cancellation_while_controls_stay_live() {
        let callbacks = CallbackGate::start().expect("callback gate");
        let entered = Barrier::new(2);
        let release = Barrier::new(2);
        let cancellation = CancellationToken::new();
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                callbacks.call_observed(0, || {
                    entered.wait();
                    release.wait();
                }, |_| {}).expect("callback admitted");
            });
            entered.wait();
            let cancel = scope.spawn(|| {
                std::thread::sleep(std::time::Duration::from_millis(20));
                cancellation.cancel();
            });
            let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
            callbacks.drain_measured(&mut work).expect_err("cancellation must interrupt the real gate wait");
            assert_eq!(work.work, crate::WorkCounters::UNCHARGED, "borrowed controls have no invented cost");
            assert_eq!(*callbacks.active.lock().expect("live callback count"), 1);
            release.wait();
            worker.join().expect("callback finishes");
            cancel.join().expect("canceller finishes");
            callbacks.drain_measured(&mut crate::demand::DemandWork::new(
                WorkBudget::UNBOUNDED, &CancellationToken::new(),
            )).expect("later invocation drains the same controls");
        });
    }

    #[test]
    fn rejected_provider_controls_keep_the_original_typed_failure_and_root_fence() {
        let callbacks = CallbackGate::start().expect("callback gate");
        let expected = crate::WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes", observed: 16, maximum: 0,
        };
        let failures = Mutex::new(PostOperationFailures {
            provider_control_failure: Some(super::provider_stack::ProviderFailure::Work(expected)),
            ..PostOperationFailures::default()
        });
        for _ in 0..2 {
            let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
            let error = super::flush_callback_gate_measured(&callbacks, &failures, None, &mut work, |_, _| {
                panic!("an unexecuted authored handler cannot be forgotten by a later flush")
            }).expect_err("unreplayable provider controls retain namespace custody");
            assert!(matches!(error, crate::NativeMountError::Work(error) if error == expected));
            assert_eq!(work.work, crate::WorkCounters::UNCHARGED, "retained failure is borrowed, not newly executed");
            assert!(failures.lock().expect("retained control fault").provider_control_failure.is_some());
        }
    }

    fn sorted_names(directory: &std::path::Path) -> std::io::Result<Vec<String>> {
        let mut names = std::fs::read_dir(directory)?
            .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
            .collect::<std::io::Result<Vec<_>>>()?;
        names.sort();
        Ok(names)
    }

    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn stopped_projection_cannot_publish_a_late_open_writer()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::io::{Seek as _, Write as _};
        use std::os::windows::fs::OpenOptionsExt as _;
        use windows::Win32::Storage::FileSystem::{FILE_SHARE_READ, FILE_SHARE_WRITE};

        let source = windows_checkout_source().await?;
        let seed = windows_path("seed.txt");
        source.create_file(&seed, FileMetadata::default())?;
        source.write_range(&seed, 0, Bytes::from_static(b"seed"))?;
        let (_root, destination, mut session) = mount_source(&source)?;
        let projected = destination.join("seed.txt");
        assert_eq!(std::fs::read(&projected)?, b"seed");

        // Excluding FILE_SHARE_DELETE models a background compiler or language
        // server that outlives the host tool boundary. ProjFS can stop, but the
        // authenticated projection cannot be reclaimed while this handle is
        // alive, so an in-place native-view transition must not be published.
        let mut late = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .share_mode(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0)
            .open(&projected)?;
        let stop = session
            .stop()
            .expect_err("an open non-delete-sharing handle must fence cleanup");
        assert!(
            matches!(stop, crate::NativeMountError::Io(_)),
            "unexpected cleanup fence: {stop}"
        );

        late.rewind()?;
        late.write_all(b"late")?;
        late.flush()?;
        drop(late);

        // The provider is already stopped, so no callback can authenticate or
        // publish this write. A later physical view must quarantine the old
        // epoch rather than pretending that the late write joined the SDK.
        assert_eq!(source.read_range(&seed, 0, 4)?, b"seed"[..]);
        session.stop()?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn metadata_edit_after_a_boundary_through_a_handle_opened_before_it_is_captured()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = windows_checkout_source().await?;
        let (root, destination, mut session) = mount_source(&source)?;
        let file = destination.join("late.txt");
        std::fs::write(root.path().join("seed"), b"late")?;
        // ProjFS notifies the provider only of other processes' I/O.
        let copied = acyclic_native_runtime::process_status(
            std::process::Command::new("cmd.exe")
                .args(["/D", "/C", "copy /Y seed projection\\late.txt > nul"])
                .current_dir(root.path()),
            std::time::Duration::from_secs(120),
        )?;
        assert!(copied.success());
        // The file is still awaiting capture when this handle opens; the
        // boundary below captures it, and only then is its time rewritten.
        let script = "$ErrorActionPreference = 'Stop'; Add-Type -Namespace Probe -Name Kernel -MemberDefinition \
            '[DllImport(\"kernel32.dll\")] public static extern bool SetFileTime(\
            Microsoft.Win32.SafeHandles.SafeFileHandle h, IntPtr c, IntPtr a, ref long w);'; \
            $h = [IO.File]::Open($env:ACYCLIC_FS_TEST_FILE, 'Open', 'ReadWrite', 'ReadWrite,Delete'); \
            New-Item -ItemType File $env:ACYCLIC_FS_TEST_READY | Out-Null; \
            while (-not (Test-Path $env:ACYCLIC_FS_TEST_GO)) { Start-Sleep -Milliseconds 10 }; \
            $w = [DateTimeOffset]::FromUnixTimeSeconds(978307200).UtcDateTime.ToFileTimeUtc(); \
            if (-not [Probe.Kernel]::SetFileTime($h.SafeFileHandle, [IntPtr]::Zero, [IntPtr]::Zero, [ref]$w)) { exit 2 }; \
            $h.Close()";
        let ready = root.path().join("ready");
        let go = root.path().join("go");
        let mut editor = acyclic_native_runtime::spawn_process_tree(
            std::process::Command::new("powershell.exe")
                .args(["-NoProfile", "-NonInteractive", "-Command", script])
                .env("ACYCLIC_FS_TEST_FILE", &file)
                .env("ACYCLIC_FS_TEST_READY", &ready)
                .env("ACYCLIC_FS_TEST_GO", &go),
        )?;
        while !ready.exists() {
            assert!(editor.try_wait()?.is_none(), "editor exited early");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        session.flush_callbacks()?;
        std::fs::write(&go, b"")?;
        assert!(
            editor.wait(std::time::Duration::from_secs(30))?.success(),
            "metadata edit failed"
        );
        session.flush_callbacks()?;
        let captured = source
            .lookup(&windows_path("late.txt"))?
            .ok_or("late.txt was not captured")?;
        assert_eq!(
            captured.metadata.modified_ns,
            crate::kernel::MetadataField::Value(978_307_200_000_000_000),
            "the edit made after the boundary was lost"
        );
        session.stop()?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn write_through_a_handle_opened_before_a_rename_lands_at_the_new_name()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = windows_checkout_source().await?;
        let seed = windows_path("seed.txt");
        source.create_file(&seed, FileMetadata::default())?;
        source.write_range(&seed, 0, Bytes::from_static(b"seed"))?;
        let (_root, destination, mut session) = mount_source(&source)?;
        // ProjFS notifies the provider only of other processes' I/O. One
        // handle stays open across the rename and writes afterwards.
        let script = "$seed = Join-Path $env:ACYCLIC_FS_TEST_ROOT 'seed.txt';
            $moved = Join-Path $env:ACYCLIC_FS_TEST_ROOT 'moved.txt';
            $h = [IO.File]::Open($seed, 'Open', 'ReadWrite', 'ReadWrite,Delete');
            [IO.File]::Move($seed, $moved);
            $h.Seek(0, 'End') | Out-Null; $h.WriteByte(33); $h.Close()";
        let output = acyclic_native_runtime::process_output(
            std::process::Command::new("powershell.exe")
                .args(["-NoProfile", "-NonInteractive", "-Command", script])
                .env("ACYCLIC_FS_TEST_ROOT", &destination),
            std::time::Duration::from_secs(120),
            8 * 1024 * 1024,
        )?;
        assert!(output.status.success(), "external edit failed: {output:?}");
        session.flush_callbacks()?;
        assert!(source.lookup(&seed)?.is_none());
        let moved = windows_path("moved.txt");
        assert_eq!(source.read_range(&moved, 0, 5)?.as_ref(), b"seed!");
        session.stop()?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn created_and_renamed_files_stay_observed_after_capture()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = windows_checkout_source().await?;
        let (_root, destination, mut session) = mount_source(&source)?;
        // ProjFS notifies the provider only of other processes' I/O.
        let external = |command: &str| -> Result<(), Box<dyn std::error::Error>> {
            let output = acyclic_native_runtime::process_output(
                std::process::Command::new("cmd.exe")
                    .args(["/D", "/C", command])
                    .current_dir(&destination),
                std::time::Duration::from_secs(120),
                8 * 1024 * 1024,
            )?;
            assert!(output.status.success(), "{command} failed: {output:?}");
            Ok(())
        };
        let content = |name: &str| -> Result<Vec<u8>, MountSourceError> {
            let path = windows_path(name);
            let length = source.lookup(&path)?.ok_or(MountSourceError::NotFound)?;
            let length = u32::try_from(length.node.logical_bytes)
                .map_err(|_| MountSourceError::Invalid("test file too large".to_owned()))?;
            Ok(source.read_range(&path, 0, length)?.to_vec())
        };

        external("echo first> created.txt")?;
        session.flush_callbacks()?;
        assert_eq!(content("created.txt")?, b"first\r\n");
        // Each change after the first capture is its own later boundary.
        external("echo second> created.txt")?;
        session.flush_callbacks()?;
        assert_eq!(content("created.txt")?, b"second\r\n");
        external("ren created.txt renamed.txt")?;
        session.flush_callbacks()?;
        external("echo third> renamed.txt")?;
        session.flush_callbacks()?;
        assert_eq!(content("renamed.txt")?, b"third\r\n");
        external("mklink /H linked.txt renamed.txt > nul && del renamed.txt")?;
        session.flush_callbacks()?;
        assert!(source.lookup(&windows_path("created.txt"))?.is_none());
        assert!(source.lookup(&windows_path("renamed.txt"))?.is_none());
        assert_eq!(content("linked.txt")?, b"third\r\n");
        external("del linked.txt")?;
        session.flush_callbacks()?;
        assert!(source.lookup(&windows_path("linked.txt"))?.is_none());
        session.stop()?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn listing_follows_source_and_mount_changes() -> Result<(), Box<dyn std::error::Error>> {
        let source = windows_checkout_source().await?;
        source.create_file(&windows_path("a.txt"), FileMetadata::default())?;
        let (_root, destination, mut session) = mount_source(&source)?;
        assert_eq!(sorted_names(&destination)?, ["a.txt"]);
        assert_eq!(sorted_names(&destination)?, ["a.txt"]);

        // A source-side create advances the source view, which the
        // projection follows once revalidated.
        source.create_file(&windows_path("b.txt"), FileMetadata::default())?;
        session.revalidate()?;
        assert_eq!(sorted_names(&destination)?, ["a.txt", "b.txt"]);

        // Creates and removals through the mount stay exact before and after
        // the provider captures them into the source. ProjFS notifies the
        // provider only of other processes' I/O.
        let external = acyclic_native_runtime::process_output(
            std::process::Command::new("cmd.exe")
                .args(["/D", "/C", "echo c> c.txt && del b.txt"])
                .current_dir(&destination),
            std::time::Duration::from_secs(120),
            8 * 1024 * 1024,
        )?;
        assert!(
            external.status.success(),
            "external edit failed: {external:?}"
        );
        assert_eq!(sorted_names(&destination)?, ["a.txt", "c.txt"]);
        session.flush_callbacks()?;
        assert_eq!(sorted_names(&destination)?, ["a.txt", "c.txt"]);
        assert!(source.lookup(&windows_path("b.txt"))?.is_none());
        assert!(source.lookup(&windows_path("c.txt"))?.is_some());
        session.stop()?;
        Ok(())
    }

    /// A link is projected with its target, as a placeholder or, where the
    /// volume refuses link placeholders, as an ordinary link, and follows a
    /// source that points it elsewhere.
    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn a_projected_link_follows_its_source_target() -> Result<(), Box<dyn std::error::Error>>
    {
        let source = windows_checkout_source().await?;
        let target = |name: &str| Bytes::from(windows_name(name));
        let link = windows_path("link");
        source.create_symbolic_link(&link, target("first.txt"), FileMetadata::default())?;
        let (_root, destination, mut session) = mount_source(&source)?;
        assert_eq!(
            std::fs::read_link(destination.join("link"))?,
            std::path::Path::new("first.txt")
        );
        source.remove(&link, None)?;
        source.create_symbolic_link(&link, target("second.txt"), FileMetadata::default())?;
        session.revalidate()?;
        assert_eq!(sorted_names(&destination)?, ["link"]);
        assert_eq!(
            std::fs::read_link(destination.join("link"))?,
            std::path::Path::new("second.txt")
        );
        source.remove(&link, None)?;
        session.revalidate()?;
        assert!(sorted_names(&destination)?.is_empty());
        session.stop()?;
        Ok(())
    }

    /// Runs `script` in another process: `ProjFS` notifies the provider only
    /// of other processes' I/O.
    fn powershell(script: &str) -> std::io::Result<acyclic_native_runtime::ProcessTree> {
        acyclic_native_runtime::spawn_process_tree(
            std::process::Command::new("powershell.exe").args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                script,
            ]),
        )
    }

    fn succeeded(
        mut child: acyclic_native_runtime::ProcessTree,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let result = child.wait(std::time::Duration::from_secs(120));
        child.terminate()?;
        let status = result?;
        if status.success() {
            Ok(())
        } else {
            Err(format!("external process failed: {status}").into())
        }
    }

    /// A time set through a writable handle stays, on disk and in the
    /// source, for empty files and others, also while another process reads
    /// the files: every file of the projection is an ordinary file.
    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn a_time_set_through_a_writable_handle_stays() -> Result<(), Box<dyn std::error::Error>>
    {
        const FILES: usize = 120;
        const TICKS: i64 = crate::native_host::UNIX_EPOCH_FILETIME + 1_700_000_000 * 10_000_000;
        let source = windows_checkout_source().await?;
        for index in 0..FILES {
            let path = windows_path(&format!("f{index}.txt"));
            source.create_file(&path, FileMetadata::default())?;
            source.write_range(&path, 0, Bytes::from(vec![b'x'; 4096 * (index % 2)]))?;
        }
        let (_root, destination, mut session) = mount_source(&source)?;
        let directory = destination.display();
        let reader = powershell(&format!(
            r"1..10 | % {{ 0..{last} | % {{ try {{ [void][IO.File]::ReadAllBytes('{directory}\f' + $_ + '.txt') }} catch {{}} }} }}",
            last = FILES - 1
        ))?;
        let editor = powershell(&format!(
            r#"Add-Type -TypeDefinition 'using System; using System.Runtime.InteropServices; using Microsoft.Win32.SafeHandles; public static class T {{ [DllImport("kernel32.dll", SetLastError=true)] public static extern bool SetFileTime(SafeFileHandle h, IntPtr c, IntPtr a, ref long w); }}'
0..{last} | % {{
  $path = '{directory}\f' + $_ + '.txt'
  if ($_ % 3 -eq 0) {{ [void][IO.File]::ReadAllBytes($path) }}
  # Refused while the reader holds the file without sharing writes, as any
  # editor would be: tried again; failing otherwise.
  $file = $null
  for ($try = 0; $file -eq $null; $try++) {{
    try {{ $file = [IO.File]::Open($path, 'Open', 'ReadWrite', 'ReadWrite') }}
    catch [IO.IOException] {{ if ($try -ge 500) {{ exit 2 }}; Start-Sleep -Milliseconds 10 }}
  }}
  $time = [long]{TICKS}
  if (-not [T]::SetFileTime($file.SafeFileHandle, [IntPtr]::Zero, [IntPtr]::Zero, [ref]$time)) {{ exit 1 }}
  $file.Close()
}}"#,
            last = FILES - 1
        ))?;
        succeeded(editor)?;
        succeeded(reader)?;
        session.flush_callbacks()?;
        session.revalidate()?;
        let wanted = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
        let mut lost = Vec::new();
        for index in 0..FILES {
            let on_disk =
                std::fs::metadata(destination.join(format!("f{index}.txt")))?.modified()?;
            let recorded = source
                .lookup(&windows_path(&format!("f{index}.txt")))?
                .ok_or("file gone from the source")?
                .metadata
                .modified_ns;
            if on_disk != wanted
                || recorded != crate::kernel::MetadataField::Value(1_700_000_000_000_000_000)
            {
                lost.push(index);
            }
        }
        assert!(lost.is_empty(), "times lost: {lost:?}");
        session.stop()?;
        Ok(())
    }

    /// A file another process opened follows its source.
    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn an_opened_file_follows_its_source() -> Result<(), Box<dyn std::error::Error>> {
        let source = windows_checkout_source().await?;
        let tracked = windows_path("tracked.txt");
        source.create_file(&tracked, FileMetadata::default())?;
        source.write_range(&tracked, 0, Bytes::from_static(b"before"))?;
        let (_root, destination, mut session) = mount_source(&source)?;
        let projected = destination.join("tracked.txt");
        // Opened as editors do, sharing write access.
        succeeded(powershell(&format!(
            "[IO.File]::Open('{}', 'Open', 'Read', 'ReadWrite').Close()",
            projected.display()
        ))?)?;
        assert_eq!(super::reparse_tag(&projected)?.0, None, "a placeholder");
        session.flush_callbacks()?;
        source.write_range(&tracked, 0, Bytes::from_static(b"after!"))?;
        session.revalidate()?;
        assert_eq!(std::fs::read(&projected)?, b"after!");
        session.stop()?;
        Ok(())
    }

    /// What another process writes to a projected file stays, and reaches
    /// the source.
    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn an_opened_file_the_user_wrote_stays() -> Result<(), Box<dyn std::error::Error>> {
        let source = windows_checkout_source().await?;
        let kept = windows_path("kept.txt");
        source.create_file(&kept, FileMetadata::default())?;
        source.write_range(&kept, 0, Bytes::from_static(b"source"))?;
        let (_root, destination, mut session) = mount_source(&source)?;
        let projected = destination.join("kept.txt");
        succeeded(powershell(&format!(
            "[IO.File]::AppendAllText('{}', ' and user')",
            projected.display()
        ))?)?;
        session.flush_callbacks()?;
        session.revalidate()?;
        assert_eq!(std::fs::read(&projected)?, b"source and user");
        assert_eq!(
            source.read_range(&kept, 0, 15)?.as_ref(),
            b"source and user"
        );
        session.stop()?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn a_projected_directory_link_leads_into_its_directory()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::windows::fs::FileTypeExt as _;

        let source = windows_checkout_source().await?;
        let folder = windows_path("folder");
        source.create_directory(&folder, FileMetadata::default())?;
        let inner = folder.child(windows_name("inner.txt"));
        source.create_file(&inner, FileMetadata::default())?;
        source.write_range(&inner, 0, Bytes::from_static(b"inner"))?;
        source.create_symbolic_link(
            &windows_path("folder-link"),
            Bytes::from(windows_name("folder")),
            FileMetadata::default(),
        )?;
        let (_root, destination, mut session) = mount_source(&source)?;
        let link = destination.join("folder-link");
        assert!(
            std::fs::symlink_metadata(&link)?
                .file_type()
                .is_symlink_dir()
        );
        assert_eq!(std::fs::read(link.join("inner.txt"))?, b"inner");
        session.stop()?;
        Ok(())
    }

    /// A directory the source puts in place of a projected link replaces it.
    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn a_directory_replaces_a_projected_link() -> Result<(), Box<dyn std::error::Error>> {
        let source = windows_checkout_source().await?;
        let name = windows_path("name");
        source.create_symbolic_link(
            &name,
            Bytes::from(windows_name("target.txt")),
            FileMetadata::default(),
        )?;
        let (_root, destination, mut session) = mount_source(&source)?;
        source.remove(&name, None)?;
        source.create_directory(&name, FileMetadata::default())?;
        session.revalidate()?;
        assert!(std::fs::symlink_metadata(destination.join("name"))?.is_dir());
        session.stop()?;
        Ok(())
    }

    /// A link another process holds open while the source puts a directory
    /// in its place gives way to that directory once it is closed.
    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn a_directory_replaces_a_link_held_open_once_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::windows::fs::OpenOptionsExt as _;
        use windows::Win32::Storage::FileSystem::{
            FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ,
        };

        let source = windows_checkout_source().await?;
        let name = windows_path("name");
        source.create_symbolic_link(
            &name,
            Bytes::from(windows_name("target.txt")),
            FileMetadata::default(),
        )?;
        let (_root, destination, mut session) = mount_source(&source)?;
        let projected = destination.join("name");
        // Shares no delete access, so nothing can delete it while held.
        let held = std::fs::OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ.0)
            .custom_flags((FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS).0)
            .open(&projected)?;
        source.remove(&name, None)?;
        source.create_directory(&name, FileMetadata::default())?;
        let inner = name.child(windows_name("inner.txt"));
        source.create_file(&inner, FileMetadata::default())?;
        source.write_range(&inner, 0, Bytes::from_static(b"inner"))?;
        let closer = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(300));
            drop(held);
        });
        session.revalidate()?;
        closer.join().map_err(|_| "closer panicked")?;
        let held_by = || format!("the mount holds {:?}", sorted_names(&destination));
        let kind = std::fs::symlink_metadata(&projected)
            .map_err(|error| format!("the directory: {error}; {}", held_by()))?;
        assert!(kind.is_dir(), "not a directory: {kind:?}");
        let inner = std::fs::read(projected.join("inner.txt")).map_err(|error| {
            format!("its file: {error}; it holds {:?}", sorted_names(&projected))
        })?;
        assert_eq!(inner, b"inner");
        session.stop()?;
        Ok(())
    }

    /// A link another process holds open goes once it is closed: its
    /// removal is tried again, not given up.
    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn a_link_held_open_goes_once_closed() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::windows::fs::OpenOptionsExt as _;
        use windows::Win32::Storage::FileSystem::{
            FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ,
        };

        let source = windows_checkout_source().await?;
        let link = windows_path("link");
        source.create_symbolic_link(
            &link,
            Bytes::from(windows_name("target.txt")),
            FileMetadata::default(),
        )?;
        let (_root, destination, mut session) = mount_source(&source)?;
        let projected = destination.join("link");
        // Shares no delete access, so nothing can delete it while held.
        let held = std::fs::OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ.0)
            .custom_flags((FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS).0)
            .open(&projected)?;
        source.remove(&link, None)?;
        let closer = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(300));
            drop(held);
        });
        session.revalidate()?;
        closer.join().map_err(|_| "closer panicked")?;
        assert!(sorted_names(&destination)?.is_empty());
        session.stop()?;
        Ok(())
    }

    /// What the user put in place of a projected link stays when the source
    /// removes the link.
    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn a_link_the_user_replaced_stays() -> Result<(), Box<dyn std::error::Error>> {
        let source = windows_checkout_source().await?;
        let link = windows_path("link");
        source.create_symbolic_link(
            &link,
            Bytes::from(windows_name("target.txt")),
            FileMetadata::default(),
        )?;
        let (_root, destination, mut session) = mount_source(&source)?;
        let projected = destination.join("link");
        assert_eq!(
            std::fs::read_link(&projected)?,
            std::path::Path::new("target.txt")
        );
        std::fs::remove_file(&projected)?;
        std::fs::create_dir(&projected)?;
        source.remove(&link, None)?;
        session.revalidate()?;
        assert!(std::fs::symlink_metadata(&projected)?.is_dir());
        session.stop()?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn paths_the_source_gains_appear_once_revalidated()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = windows_checkout_source().await?;
        let (_root, destination, mut session) = mount_source(&source)?;
        let absent = |name: &str| {
            std::fs::metadata(destination.join(name))
                .err()
                .map(|error| error.kind())
        };

        assert_eq!(absent("listed.txt"), Some(std::io::ErrorKind::NotFound));
        source.create_file(&windows_path("listed.txt"), FileMetadata::default())?;
        source.write_range(
            &windows_path("listed.txt"),
            0,
            Bytes::from_static(b"listed"),
        )?;
        // A path absent before appears, listed and opened alike.
        session.revalidate()?;
        assert_eq!(sorted_names(&destination)?, ["listed.txt"]);
        assert_eq!(std::fs::read(destination.join("listed.txt"))?, b"listed");

        assert_eq!(
            absent("invalidated.txt"),
            Some(std::io::ErrorKind::NotFound)
        );
        source.create_file(&windows_path("invalidated.txt"), FileMetadata::default())?;
        session.invalidate(b"/invalidated.txt")?;
        session.revalidate()?;
        assert_eq!(
            std::fs::metadata(destination.join("invalidated.txt"))?.len(),
            0
        );
        session.stop()?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn advanced_view_reveals_a_path_absent_before() -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        let engine = Fs::local(LocalOptions::new(root.path().join("state"))).await?;
        let workspace = engine.create_workspace("advance-projfs").await?;
        let path = root.path().join("mount");
        std::fs::create_dir(&path)?;
        let mount = workspace
            .mount(
                &path,
                MountOptions::read_write().publication(MountPublication::Manual),
            )
            .await?;
        let added = path.join("added.txt");
        assert_eq!(
            std::fs::metadata(&added).err().map(|error| error.kind()),
            Some(std::io::ErrorKind::NotFound)
        );
        let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
        transaction
            .create_file(
                "/added.txt",
                Bytes::from_static(b"added"),
                FileMetadata::default(),
            )
            .await?;
        transaction.commit().await?;
        mount.advance_to_head().await?;
        assert_eq!(std::fs::read(&added)?, b"added");
        mount.unmount().await?;
        Ok(())
    }

    /// Revalidation waits while a superseded path is pending, which
    /// it is until its outcome is recorded, even once every change has been
    /// served, and returns as soon as it is settled.
    #[test]
    fn edits_of_one_file_run_one_at_a_time() {
        use super::{FileLocks, lock_recover};
        use std::sync::atomic::{AtomicUsize, Ordering};

        let locks = FileLocks::default();
        let file = crate::NativeRootIdentity {
            device: 1,
            object: 2,
        };
        let running = AtomicUsize::new(0);
        let most = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..50 {
                        locks.with(&file, || {
                            let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                            most.fetch_max(now, Ordering::SeqCst);
                            std::thread::yield_now();
                            running.fetch_sub(1, Ordering::SeqCst);
                        });
                    }
                });
            }
        });
        assert_eq!(most.load(Ordering::SeqCst), 1);
        // No lock outlives the edits that used it.
        assert!(lock_recover(&locks.0).is_empty());
    }

    #[test]
    fn settling_waits_for_every_pending_placeholder() -> Result<(), Box<dyn std::error::Error>> {
        use super::{Projector, WrittenEntry};
        use crate::FileId;
        use crate::native_mount::ViewStamp;

        let projector = Arc::new(Projector::new());
        let path = MountPath::root().child(b"held".to_vec());
        {
            let mut state = super::lock_recover(&projector.state);
            state.pending.insert(
                path.clone(),
                WrittenEntry {
                    file_id: FileId::new(),
                    owned: None,
                    facts: None,
                    basis: Some(ReadBasis {
                        stamp: ViewStamp::current(),
                        binding: 0,
                    }),
                },
            );
        }
        let settled = std::thread::spawn({
            let projector = Arc::clone(&projector);
            move || projector.settle()
        });
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert!(
            !settled.is_finished(),
            "settled while a placeholder is pending"
        );
        super::lock_recover(&projector.state).pending.remove(&path);
        projector.changed.notify_all();
        // Settling ends once nothing is pending.
        settled
            .join()
            .map_err(|_| "settle panicked")?
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    /// A superseded path another handle holds open is replaced once
    /// that handle closes; revalidation never returns while it still
    /// describes the superseded source.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn a_held_placeholder_is_replaced_before_revalidation_returns()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::native::NativeDemandSource;

        let root = tempfile::tempdir()?;
        let source_root = root.path().join("source");
        std::fs::create_dir(&source_root)?;
        std::fs::write(source_root.join("held.txt"), b"old")?;
        let demand = NativeDemandSource::open(
            &source_root,
            crate::model::FilesystemProfile::Windows,
            crate::model::VolumeLimits::default(),
        )
        .await?;
        let lazy = crate::LazyWorkspace::attach_with_config(
            &Fs::memory(),
            "held-placeholder",
            Arc::new(demand),
            crate::MemoryLazyWorkspaceStore::default(),
            VolumeConfig::native(Lifecycle::Ephemeral),
        )
        .await?;
        let destination = root.path().join("mount");
        std::fs::create_dir(&destination)?;
        let mount = Arc::new(
            lazy.mount(
                &destination,
                MountOptions::read_write().publication(MountPublication::Manual),
            )
            .await?,
        );
        let held_path = destination.join("held.txt");
        assert_eq!(std::fs::metadata(&held_path)?.len(), 3);
        let held = std::fs::File::open(&held_path)?;
        std::fs::write(source_root.join("held.txt"), b"much newer")?;
        let revalidating = {
            let mount = Arc::clone(&mount);
            std::thread::spawn(move || mount.revalidate().map_err(|error| error.to_string()))
        };
        std::thread::sleep(std::time::Duration::from_millis(300));
        // Either replaced despite the handle, and current already, or
        // replaced once it closes; never current only in name.
        if !revalidating.is_finished() {
            drop(held);
        }
        revalidating
            .join()
            .map_err(|_| "revalidation panicked")?
            .map_err(|error| error.to_string())?;
        assert_eq!(std::fs::metadata(&held_path)?.len(), 10);
        assert_eq!(std::fs::read(&held_path)?, b"much newer");
        mount.unmount().await?;
        Ok(())
    }

    /// A read-only source file is written read-only too; a change to
    /// the source still replaces it, so revalidation never leaves it stale.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn a_read_only_placeholder_is_replaced_when_its_source_changes()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::native::NativeDemandSource;

        let root = tempfile::tempdir()?;
        let source_root = root.path().join("source");
        std::fs::create_dir(&source_root)?;
        let source_file = source_root.join("locked.txt");
        let set_read_only = |read_only: bool| -> std::io::Result<()> {
            let mut permissions = std::fs::metadata(&source_file)?.permissions();
            permissions.set_readonly(read_only);
            std::fs::set_permissions(&source_file, permissions)
        };
        std::fs::write(&source_file, b"old")?;
        set_read_only(true)?;
        let demand = NativeDemandSource::open(
            &source_root,
            crate::model::FilesystemProfile::Windows,
            crate::model::VolumeLimits::default(),
        )
        .await?;
        let lazy = crate::LazyWorkspace::attach_with_config(
            &Fs::memory(),
            "read-only-placeholder",
            Arc::new(demand),
            crate::MemoryLazyWorkspaceStore::default(),
            VolumeConfig::native(Lifecycle::Ephemeral),
        )
        .await?;
        let destination = root.path().join("mount");
        std::fs::create_dir(&destination)?;
        let mount = lazy
            .mount(
                &destination,
                MountOptions::read_write().publication(MountPublication::Manual),
            )
            .await?;
        let projected = destination.join("locked.txt");
        assert_eq!(std::fs::metadata(&projected)?.len(), 3);
        assert!(std::fs::metadata(&projected)?.permissions().readonly());

        set_read_only(false)?;
        std::fs::write(&source_file, b"much newer")?;
        set_read_only(true)?;
        mount.revalidate()?;
        assert_eq!(std::fs::metadata(&projected)?.len(), 10);
        assert_eq!(std::fs::read(&projected)?, b"much newer");
        set_read_only(false)?;
        mount.unmount().await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn a_file_holds_only_the_content_its_listing_named()
    -> Result<(), Box<dyn std::error::Error>> {
        // A file is written from a lookup, or from the listing that
        // projected its name; either names exactly the listed content.
        for listed_first in [false, true] {
            writes_only_promised_content(listed_first).await?;
        }
        Ok(())
    }

    async fn writes_only_promised_content(
        listed_first: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::native::NativeDemandSource;
        use std::io::Write as _;

        let root = tempfile::tempdir()?;
        let source_root = root.path().join("source");
        std::fs::create_dir(&source_root)?;
        std::fs::write(source_root.join("changed.txt"), b"before")?;
        std::fs::write(source_root.join("stable.txt"), b"stable")?;
        let demand = NativeDemandSource::open(
            &source_root,
            crate::model::FilesystemProfile::Windows,
            crate::model::VolumeLimits::default(),
        )
        .await?;
        let lazy = crate::LazyWorkspace::attach_with_config(
            &Fs::memory(),
            "pinned-projfs",
            Arc::new(demand),
            crate::MemoryLazyWorkspaceStore::default(),
            VolumeConfig::native(Lifecycle::Ephemeral),
        )
        .await?;
        let destination = root.path().join("mount");
        std::fs::create_dir(&destination)?;
        let mount = lazy
            .mount(
                &destination,
                MountOptions::read_write().publication(MountPublication::Manual),
            )
            .await?;
        if listed_first {
            assert_eq!(std::fs::read_dir(&destination)?.count(), 2);
        }
        // Both are written.
        assert_eq!(std::fs::metadata(destination.join("changed.txt"))?.len(), 6);
        assert_eq!(std::fs::metadata(destination.join("stable.txt"))?.len(), 6);

        // Same length, new version: only the pin can tell the bytes apart.
        let mut changed = std::fs::OpenOptions::new()
            .write(true)
            .open(source_root.join("changed.txt"))?;
        changed.write_all(b"after!")?;
        changed.set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1 << 30))?;
        drop(changed);

        // The file holds the version the listing named, or was already
        // written again from the new one: never a mix.
        let read = std::fs::read(destination.join("changed.txt"))?;
        assert!(
            read == b"before" || read == b"after!",
            "a file holds content no listing named: {read:?}"
        );
        assert_eq!(std::fs::read(destination.join("stable.txt"))?, b"stable");
        mount.revalidate()?;
        assert_eq!(std::fs::read(destination.join("changed.txt"))?, b"after!");
        mount.unmount().await?;
        Ok(())
    }

    /// A directory projected from the source renames like any other (a
    /// directory `ProjFS` projected on demand never could), and a file moved
    /// with it, never opened before, still reads its content: `ProjFS` asks
    /// for it by the path it was written at.
    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn projected_directories_rename_and_what_moved_with_them_reads()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::native::NativeDemandSource;

        let root = tempfile::tempdir()?;
        let source_root = root.path().join("source");
        std::fs::create_dir_all(source_root.join("pkg").join("sub"))?;
        std::fs::write(source_root.join("pkg").join("shared.rs"), b"shared")?;
        std::fs::write(source_root.join("pkg").join("sub").join("deep.rs"), b"deep")?;
        let demand = NativeDemandSource::open(
            &source_root,
            crate::model::FilesystemProfile::Windows,
            crate::model::VolumeLimits::default(),
        )
        .await?;
        let engine = Fs::local(LocalOptions::new(root.path().join("state"))).await?;
        let lazy = crate::LazyWorkspace::attach_with_config(
            &engine,
            "renamed-projfs",
            Arc::new(demand),
            crate::LocalCoreStateStore::open_owned(root.path().join("core"))?,
            VolumeConfig::native(Lifecycle::Ephemeral),
        )
        .await?;
        let destination = root.path().join("mount");
        std::fs::create_dir(&destination)?;
        let mount = lazy
            .mount(
                &destination,
                MountOptions::read_write().publication(MountPublication::Manual),
            )
            .await?;
        let listed = |path: &std::path::Path| {
            sorted_names(path).map_err(|error| {
                format!(
                    "listing {}: {error}; the mount holds {:?}",
                    path.display(),
                    sorted_names(&destination)
                )
            })
        };
        assert_eq!(listed(&destination.join("pkg"))?, ["shared.rs", "sub"]);
        // ProjFS reports only other processes' I/O to its provider.
        let moved = acyclic_native_runtime::process_output(
            std::process::Command::new("cmd.exe")
                .args(["/D", "/C", "move pkg lib"])
                .current_dir(&destination),
            std::time::Duration::from_secs(120),
            8 * 1024 * 1024,
        )?;
        assert!(moved.status.success(), "move failed: {moved:?}");
        let read = |path: &std::path::Path| {
            std::fs::read(path).map_err(|error| format!("reading {}: {error}", path.display()))
        };
        assert_eq!(listed(&destination)?, ["lib"]);
        assert_eq!(read(&destination.join("lib").join("shared.rs"))?, b"shared");
        assert_eq!(
            read(&destination.join("lib").join("sub").join("deep.rs"))?,
            b"deep"
        );
        mount.sync().await?;
        assert!(lazy.lookup("/lib/sub/deep.rs").await.is_ok());
        assert!(lazy.lookup("/pkg").await.is_err());
        mount.unmount().await?;
        Ok(())
    }

    /// A live projection lists a lazy workspace's unauthored source entries.
    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn a_lazy_workspace_lists_through_the_projection()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::native::NativeDemandSource;

        let root = tempfile::tempdir()?;
        let source_root = root.path().join("source");
        std::fs::create_dir(&source_root)?;
        std::fs::write(source_root.join("a.txt"), b"a")?;
        std::fs::write(source_root.join("b.rs"), b"b")?;
        let demand = NativeDemandSource::open(
            &source_root,
            crate::model::FilesystemProfile::Windows,
            crate::model::VolumeLimits::default(),
        )
        .await?;
        let engine = Fs::local(LocalOptions::new(root.path().join("state"))).await?;
        let lazy = crate::LazyWorkspace::attach_with_config(
            &engine,
            "listed-projfs",
            Arc::new(demand),
            crate::LocalCoreStateStore::open_owned(root.path().join("core"))?,
            VolumeConfig::native(Lifecycle::Ephemeral),
        )
        .await?;
        let destination = root.path().join("mount");
        std::fs::create_dir(&destination)?;
        let mount = lazy
            .mount(
                &destination,
                MountOptions::read_write().publication(MountPublication::Manual),
            )
            .await?;
        assert_eq!(sorted_names(&destination)?, ["a.txt", "b.rs"]);
        mount.unmount().await?;
        Ok(())
    }

    #[test]
    fn source_failures_keep_distinct_statuses() {
        let statuses = [
            MountSourceError::NotFound,
            MountSourceError::AlreadyExists,
            MountSourceError::Invalid(String::new()),
            MountSourceError::Unsupported(String::new()),
            MountSourceError::Engine(String::new()),
            MountSourceError::Stale,
        ]
        .iter()
        .map(source_hresult)
        .collect::<HashSet<_>>();
        assert_eq!(statuses.len(), 6);
        assert!(!statuses.contains(&super::HR_UNEXPECTED));
    }

    #[test]
    #[ignore = "requires a host that permits starting a ProjFS provider"]
    fn minimal_virtualization_starts() -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let root_identity = crate::capture_root_identity(root.path())?;
        let wide = HSTRING::from(root.path().as_os_str());
        let guid = GUID::from_values(0, 0, 0, [0; 8]);
        // SAFETY: all referenced values remain live for these synchronous calls.
        unsafe {
            PrjMarkDirectoryAsPlaceholder(
                PCWSTR::from_raw(wide.as_ptr()),
                PCWSTR::null(),
                None,
                &raw const guid,
            )
        }?;
        // No Acyclic source, notification mappings, or mount setup participates.
        let callbacks = super::callbacks();
        // SAFETY: `wide` and `callbacks` outlive the synchronous start call.
        let context = unsafe {
            PrjStartVirtualizing(
                PCWSTR::from_raw(wide.as_ptr()),
                &raw const callbacks,
                None,
                None,
            )
        };
        match context {
            Ok(context) => {
                // SAFETY: the successful call returned the sole live context.
                unsafe { PrjStopVirtualizing(context) };
                // A crash leaves the designation in place. The provider must
                // be able to restart without marking or clearing that root.
                // SAFETY: `wide` and `callbacks` outlive the synchronous start call.
                let restarted = unsafe {
                    PrjStartVirtualizing(
                        PCWSTR::from_raw(wide.as_ptr()),
                        &raw const callbacks,
                        None,
                        None,
                    )
                }?;
                let mut instance = PRJ_VIRTUALIZATION_INSTANCE_INFO::default();
                // SAFETY: `restarted` is live and `instance` is a writable output.
                unsafe { PrjGetVirtualizationInstanceInfo(restarted, &raw mut instance) }?;
                assert_eq!(instance.InstanceID, guid);
                // SAFETY: `restarted` is the sole live context from the restart.
                unsafe { PrjStopVirtualizing(restarted) };
            }
            Err(error) => {
                remove_authenticated_destination(root.path(), root_identity)?;
                return Err(error.into());
            }
        }
        remove_authenticated_destination(root.path(), root_identity)?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn rustc_links_object_files_created_inside_projection()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let engine = Fs::local(LocalOptions::new(root.path().join("state"))).await?;
        let workspace = engine.create_workspace("rustc-projfs").await?;
        let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
        transaction
            .create_file(
                "/acyclic-workflow.rs",
                Bytes::from_static(b"fn main() { println!(\"ok\"); }\n"),
                crate::kernel::FileMetadata::default(),
            )
            .await?;
        transaction.commit().await?;
        let path = root.path().join("mount");
        std::fs::create_dir(&path)?;
        let mount = workspace
            .mount(
                &path,
                MountOptions::read_write().publication(MountPublication::Manual),
            )
            .await?;
        let compiler_path = path.clone();
        let output = tokio::task::spawn_blocking(move || {
            acyclic_native_runtime::process_output(
                std::process::Command::new("rustc")
                    .current_dir(compiler_path)
                    .args([
                        "--edition",
                        "2021",
                        "acyclic-workflow.rs",
                        "-o",
                        "acyclic-workflow-bin.exe",
                    ]),
                std::time::Duration::from_secs(120),
                8 * 1024 * 1024,
            )
        })
        .await??;
        assert!(
            output.status.success(),
            "rustc failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        mount.sync().await?;
        let executable = path.join("acyclic-workflow-bin.exe");
        let execution = tokio::task::spawn_blocking(move || {
            acyclic_native_runtime::process_output(
                &mut std::process::Command::new(executable),
                std::time::Duration::from_secs(120),
                8 * 1024 * 1024,
            )
        })
        .await??;
        assert!(execution.status.success());
        assert_eq!(execution.stdout, b"ok\n");
        mount.unmount().await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn external_metadata_only_change_survives_remount()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let engine = Fs::local(LocalOptions::new(root.path().join("state"))).await?;
        let workspace = engine.create_workspace("metadata-projfs").await?;
        let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
        transaction
            .create_file(
                "/metadata.txt",
                Bytes::from_static(b"unchanged contents"),
                crate::kernel::FileMetadata::default(),
            )
            .await?;
        transaction.commit().await?;

        let first = root.path().join("first-mount");
        std::fs::create_dir(&first)?;
        let mount = workspace
            .mount(
                &first,
                MountOptions::read_write().publication(MountPublication::Manual),
            )
            .await?;
        let projected = first.join("metadata.txt");
        for _ in 0..3 {
            assert_eq!(std::fs::read(&projected)?, b"unchanged contents");
        }
        let status = tokio::task::spawn_blocking(move || {
            acyclic_native_runtime::process_status(
                std::process::Command::new("attrib")
                    .arg("+R")
                    .arg(projected),
                std::time::Duration::from_secs(120),
            )
        })
        .await??;
        assert!(status.success(), "attrib failed with {status}");
        mount.sync().await?;
        mount.unmount().await?;

        let second = root.path().join("second-mount");
        std::fs::create_dir(&second)?;
        let remount = workspace
            .mount(
                &second,
                MountOptions::read_write().publication(MountPublication::Manual),
            )
            .await?;
        assert!(
            std::fs::metadata(second.join("metadata.txt"))?
                .permissions()
                .readonly(),
            "metadata-only edits must survive synchronization and remount"
        );
        remount.unmount().await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn a_file_renamed_before_it_is_read_stays_as_renamed()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::windows::fs::MetadataExt as _;

        let root = tempfile::tempdir()?;
        let engine = Fs::local(LocalOptions::new(root.path().join("state"))).await?;
        let workspace = engine.create_workspace("rename-unread-projfs").await?;
        let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
        transaction
            .create_file(
                "/before.txt",
                Bytes::from_static(b"unread"),
                crate::kernel::FileMetadata::default(),
            )
            .await?;
        transaction.commit().await?;
        let destination = root.path().join("mount");
        std::fs::create_dir(&destination)?;
        let mount = workspace
            .mount(
                &destination,
                MountOptions::read_write().publication(MountPublication::Manual),
            )
            .await?;
        // ProjFS notifies the provider only of other processes' I/O.
        let external = acyclic_native_runtime::process_output(
            std::process::Command::new("cmd.exe")
                .args(["/D", "/C", "ren before.txt after.txt"])
                .current_dir(&destination),
            std::time::Duration::from_secs(120),
            8 * 1024 * 1024,
        )?;
        assert!(external.status.success(), "rename failed: {external:?}");
        mount.sync().await?;
        // The rename moved the written file; nothing writes it again,
        // which would take it away from under a reader for a moment.
        let renamed = destination.join("after.txt");
        assert_eq!(
            std::fs::metadata(&renamed)?.file_attributes()
                & super::FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS.0,
            0
        );
        assert_eq!(std::fs::read(&renamed)?, b"unread");
        mount.unmount().await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn projected_file_rename_and_replacement_survive_remount()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let engine = Fs::local(LocalOptions::new(root.path().join("state"))).await?;
        let workspace = engine.create_workspace("replace-projfs").await?;
        let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
        transaction
            .create_file(
                "/original.txt",
                Bytes::from_static(b"original"),
                crate::kernel::FileMetadata::default(),
            )
            .await?;
        transaction.commit().await?;

        let first = root.path().join("first-mount");
        std::fs::create_dir(&first)?;
        let mount = workspace
            .mount(
                &first,
                MountOptions::read_write().publication(MountPublication::Manual),
            )
            .await?;
        let original = first.join("original.txt");
        for _ in 0..3 {
            assert_eq!(std::fs::read(&original)?, b"original");
        }
        // ProjFS notifies the provider only of other processes' I/O.
        let external = acyclic_native_runtime::process_output(
            std::process::Command::new("cmd.exe")
                .args([
                    "/D",
                    "/C",
                    "ren original.txt renamed.txt && echo replacement> original.txt",
                ])
                .current_dir(&first),
            std::time::Duration::from_secs(120),
            8 * 1024 * 1024,
        )?;
        assert!(
            external.status.success(),
            "external edit failed: {external:?}"
        );
        mount.sync().await?;
        mount.unmount().await?;

        let second = root.path().join("second-mount");
        std::fs::create_dir(&second)?;
        let remount = workspace
            .mount(
                &second,
                MountOptions::read_write().publication(MountPublication::Manual),
            )
            .await?;
        assert_eq!(
            std::fs::read(second.join("renamed.txt")).expect("renamed path after remount"),
            b"original"
        );
        assert_eq!(
            std::fs::read(second.join("original.txt")).expect("replacement after remount"),
            b"replacement\r\n"
        );
        remount.unmount().await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn native_watcher_observes_both_projected_rename_paths()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::watch::{NativeWatch, NativeWatchOptions, WatchBatch, WatchChange};
        use std::time::{Duration, Instant};

        let root = tempfile::tempdir()?;
        let engine = Fs::local(LocalOptions::new(root.path().join("state"))).await?;
        let workspace = engine.create_workspace("watch-projfs").await?;
        let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
        transaction
            .create_file(
                "/original.txt",
                Bytes::from_static(b"original"),
                crate::kernel::FileMetadata::default(),
            )
            .await?;
        transaction.commit().await?;

        let destination = root.path().join("projection");
        std::fs::create_dir(&destination)?;
        let mount = workspace
            .mount(
                &destination,
                MountOptions::read_write().publication(MountPublication::Manual),
            )
            .await?;
        let original = destination.join("original.txt");
        assert_eq!(std::fs::read(&original)?, b"original");
        let mut watcher = NativeWatch::open(
            &destination,
            NativeWatchOptions::new(crate::model::VolumeLimits::default()),
        )?;
        watcher.begin_rescan()?;
        assert!(matches!(
            watcher.finish_rescan()?,
            WatchBatch::Changes { .. }
        ));

        let external = acyclic_native_runtime::process_output(
            std::process::Command::new("cmd.exe")
                .arg("/D")
                .arg("/C")
                .arg("ren original.txt renamed.txt && echo replacement> original.txt")
                .current_dir(&destination),
            std::time::Duration::from_secs(120),
            8 * 1024 * 1024,
        )?;
        assert!(
            external.status.success(),
            "external projected rename failed: {}",
            String::from_utf8_lossy(&external.stderr)
        );
        let limits = crate::model::VolumeLimits::default();
        let expected_path =
            |path| -> Result<crate::kernel::NamespacePath, Box<dyn std::error::Error>> {
                let portable = crate::path::PortablePath::parse(path, limits)?;
                crate::kernel::NamespacePath::from_portable_in_profile(
                    &portable,
                    crate::model::FilesystemProfile::Windows,
                    limits,
                )
                .map_err(Into::into)
            };
        let original_path = expected_path("/original.txt")?;
        let renamed_path = expected_path("/renamed.txt")?;
        let mut seen = std::collections::BTreeSet::new();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match watcher
                .poll(128, WorkBudget::UNBOUNDED, &CancellationToken::new())?
                .value
            {
                WatchBatch::Changes { changes, .. } => {
                    for change in changes {
                        match change {
                            WatchChange::Created(path)
                            | WatchChange::Modified(path)
                            | WatchChange::Arrived(path)
                            | WatchChange::MetadataChanged(path)
                            | WatchChange::Removed(path) => {
                                seen.insert(path);
                            }
                            WatchChange::Renamed { from, to } => {
                                seen.insert(from);
                                seen.insert(to);
                            }
                        }
                    }
                }
                WatchBatch::RescanRequired { reason, .. } => {
                    return Err(format!("watcher invalidated: {reason}").into());
                }
            }
            if seen.contains(&original_path) && seen.contains(&renamed_path) {
                break;
            }
            if Instant::now() >= deadline {
                return Err(format!("watcher missed a projected rename path: {seen:?}").into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        mount.unmount().await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn nested_new_directory_hardlink_and_rename_publish()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let engine = Fs::local(LocalOptions::new(root.path().join("state"))).await?;
        let workspace = engine.create_workspace("nested-projfs").await?;
        let path = root.path().join("mount");
        std::fs::create_dir(&path)?;
        let mount = workspace
            .mount(
                &path,
                MountOptions::read_write().publication(MountPublication::Manual),
            )
            .await?;
        let nested = path.join("target").join("debug").join("deps");
        std::fs::create_dir_all(&nested)?;
        let source = nested.join("unit.o");
        std::fs::write(&source, b"object")?;
        std::fs::hard_link(&source, nested.join("linked.o"))?;
        let still_open = nested.join("open.o");
        let mut handle = std::fs::File::create(&still_open)?;
        std::io::Write::write_all(&mut handle, b"open object")?;
        std::fs::hard_link(&still_open, nested.join("linked-open.o"))?;
        drop(handle);
        std::fs::rename(&nested, path.join("renamed-deps"))?;
        mount.sync().await?;
        std::fs::remove_dir_all(path.join("renamed-deps"))?;
        mount.sync().await?;
        mount.unmount().await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn enumeration_survives_concurrent_projected_writes()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let engine = Fs::local(LocalOptions::new(root.path().join("state"))).await?;
        let workspace = engine.create_workspace("enumeration-rebase").await?;
        let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
        for index in 0..300 {
            transaction
                .create_file(
                    &format!("/entry{index:03}"),
                    Bytes::from_static(b"initial"),
                    crate::kernel::FileMetadata::default(),
                )
                .await?;
        }
        for name in ["Alpha", "Zebra", "éclair"] {
            transaction
                .create_file(
                    &format!("/{name}"),
                    Bytes::from_static(b"initial"),
                    crate::kernel::FileMetadata::default(),
                )
                .await?;
        }
        transaction.commit().await?;
        let path = root.path().join("mount");
        std::fs::create_dir(&path)?;
        let mount = workspace
            .mount(
                &path,
                MountOptions::read_write().publication(MountPublication::Manual),
            )
            .await?;
        let initial = std::fs::read_dir(&path)?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<std::io::Result<Vec<_>>>()?;
        let initial_count = initial.len();
        let barrier = Arc::new(Barrier::new(2));
        let writer_barrier = Arc::clone(&barrier);
        let writer_path = path.join("entry000");
        let writer = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            writer_barrier.wait();
            for index in 0..32 {
                std::fs::write(&writer_path, format!("update-{index}"))?;
            }
            Ok(())
        });
        let enumeration = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            barrier.wait();
            for _ in 0..64 {
                let names = std::fs::read_dir(&path)?
                    .map(|entry| entry.map(|entry| entry.file_name()))
                    .collect::<std::io::Result<Vec<_>>>()?;
                let entries = names.iter().cloned().collect::<HashSet<_>>();
                assert_eq!(
                    names.len(),
                    303,
                    "projected entries changed or duplicated: {:?}",
                    names
                        .iter()
                        .filter(|name| names.iter().filter(|other| other == name).count() > 1)
                        .collect::<HashSet<_>>()
                );
                assert_eq!(entries.len(), 303, "projected entries were duplicated");
                for index in 0..300 {
                    assert!(entries.contains(std::ffi::OsStr::new(&format!("entry{index:03}"))));
                }
                for name in ["Alpha", "Zebra", "éclair"] {
                    assert!(entries.contains(std::ffi::OsStr::new(name)));
                }
            }
            Ok(())
        });
        let (written, enumerated) = tokio::join!(writer, enumeration);
        let synchronized = mount.sync().await;
        let unmounted = mount.unmount().await;
        assert_eq!(
            initial_count, 303,
            "static projected enumeration is incomplete"
        );
        written??;
        enumerated??;
        synchronized?;
        unmounted?;
        Ok(())
    }

    #[test]
    #[ignore = "requires an enabled ProjFS filter"]
    fn stale_projection_is_preserved_for_recovery() {
        let root = tempfile::tempdir().expect("root");
        let root_identity = crate::capture_root_identity(root.path()).expect("root identity");
        let wide = HSTRING::from(root.path().as_os_str());
        let guid = GUID::from_values(0, 0, 0, [0; 8]);
        // SAFETY: pointers remain live for this synchronous call.
        unsafe {
            PrjMarkDirectoryAsPlaceholder(
                PCWSTR::from_raw(wide.as_ptr()),
                PCWSTR::null(),
                None,
                &raw const guid,
            )
        }
        .expect("mark projection root");
        std::fs::write(root.path().join("authored"), b"preserve").expect("authored crash residue");
        let token = crate::CancellationToken::new();
        let mut control = super::ProjFsRecoveryControl::default();
        control.bind_root_identity(root_identity);
        let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, &token);
        assert!(matches!(
            control.recover_measured(root.path(), true, &mut work),
            Err(super::NativeMountError::UnsafeRecoveryDestination { reparse_tag: Some(_) }),
        ));
        assert!(control.controls_pending(), "actual probe capability remains owned after refusal");
        assert!(!control.namespace_detach_confirmed(), "no SDK stop was observed");
        assert!(work.work.backend_read_operations > 0);
        assert_eq!(work.work.backend_write_operations, 0);
        assert_eq!(work.live_allocation_bytes(), 0);
        control.recover_measured(root.path(), false, &mut crate::demand::DemandWork::new(
            crate::WorkBudget::UNBOUNDED, &token,
        )).expect("preserving recovery corroborates root without deleting authored residue");
        assert!(!control.controls_pending());
        assert_eq!(std::fs::read(root.path().join("authored")).expect("preserved residue"), b"preserve");
        remove_authenticated_destination(root.path(), root_identity).expect("test cleanup");
    }

    #[test]
    #[ignore = "requires an enabled ProjFS filter"]
    fn projection_cleanup_rejects_a_replaced_root_identity() {
        let original = tempfile::tempdir().expect("original root");
        let replacement = tempfile::tempdir().expect("replacement root");
        let original_identity =
            crate::capture_root_identity(original.path()).expect("original identity");
        let replacement_identity =
            crate::capture_root_identity(replacement.path()).expect("replacement identity");
        let wide = HSTRING::from(replacement.path().as_os_str());
        let guid = GUID::from_values(0, 0, 0, [0; 8]);
        // SAFETY: pointers remain live for this synchronous call.
        unsafe {
            PrjMarkDirectoryAsPlaceholder(
                PCWSTR::from_raw(wide.as_ptr()),
                PCWSTR::null(),
                None,
                &raw const guid,
            )
        }
        .expect("mark replacement projection root");
        let rejected = remove_authenticated_destination(replacement.path(), original_identity)
            .expect_err("different root must not be removed");
        assert!(matches!(rejected, super::NativeMountError::Demand(crate::DemandError::StaleSource)));
        assert!(replacement.path().exists());
        remove_authenticated_destination(replacement.path(), replacement_identity)
            .expect("remove test-owned projection");
    }

    #[test]
    fn failed_post_operation_capture_retries_at_sync() {
        let executor = CallbackGate::start().expect("callback gate");
        let failure = Arc::new(Mutex::new(PostOperationFailures::default()));
        let path = MountPath::root().child(b"changed".to_vec());
        let capture_error = Err(MountSourceError::Invalid("capture failed".to_owned()));
        record_post_operation_failure(
            failure.as_ref(),
            PRJ_NOTIFICATION_PRE_DELETE,
            &path,
            None,
            &capture_error,
        );
        assert!(
            failure
                .lock()
                .expect("failure state")
                .pending_captures
                .is_empty()
        );
        record_post_operation_failure(
            failure.as_ref(),
            PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED,
            &path,
            Some(&path),
            &capture_error,
        );
        let error = flush_callback_gate(&executor, Arc::clone(&failure), |_| {
            Err(MountSourceError::Invalid("transient failure".to_owned()))
        })
        .expect_err("failed retry must fence publication");
        assert!(error.to_string().contains("transient failure"));
        flush_callback_gate(&executor, Arc::clone(&failure), |_| Ok(()))
            .expect("successful retry clears failure");
        assert!(
            failure
                .lock()
                .expect("failure state")
                .pending_captures
                .is_empty()
        );
    }

    #[test]
    fn newer_capture_is_not_cleared_by_an_older_flush() {
        let executor = CallbackGate::start().expect("callback gate");
        let failure = Arc::new(Mutex::new(PostOperationFailures::default()));
        let path = MountPath::root().child(b"changed".to_vec());
        failure
            .lock()
            .expect("failure state")
            .queue_capture(path.clone(), "first write".to_owned());
        let during_capture = Arc::clone(&failure);
        let error = flush_callback_gate(&executor, Arc::clone(&failure), move |paths| {
            during_capture
                .lock()
                .expect("failure state")
                .queue_capture(paths[0].0.clone(), "second write".to_owned());
            Ok(())
        })
        .expect_err("the newer write remains pending");
        assert!(error.to_string().contains("second write"));
        flush_callback_gate(&executor, Arc::clone(&failure), |_| Ok(()))
            .expect("a later flush captures the newer write");
    }

    #[test]
    fn successful_close_does_not_erase_renamed_pending_capture() {
        let failure = Mutex::new(PostOperationFailures::default());
        let source = MountPath::root().child(b"save".to_vec());
        let destination = MountPath::root().child(b"final".to_vec());
        {
            let mut pending = failure.lock().expect("pending capture");
            pending.queue_capture(source.clone(), "new file".to_owned());
            pending.rename_pending_captures(&source, &destination);
        }
        record_post_operation_failure(
            &failure,
            PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED,
            &source,
            Some(&destination),
            &Ok(()),
        );
        assert!(
            failure
                .lock()
                .expect("pending capture")
                .pending_captures
                .contains_key(&destination)
        );
    }

    #[test]
    fn rename_retries_failed_capture_at_destination() {
        let executor = CallbackGate::start().expect("callback gate");
        let failure = Arc::new(Mutex::new(PostOperationFailures::default()));
        let source = MountPath::root().child(b"old".to_vec());
        let destination = MountPath::root().child(b"new".to_vec());
        let nested = source.child(b"nested".to_vec());
        let nested_destination = destination.child(b"nested".to_vec());
        let unrelated = MountPath::root().child(b"other".to_vec());
        {
            let mut failures = failure.lock().expect("failure state");
            failures.queue_capture(source.clone(), "old capture failed".to_owned());
            failures.queue_capture(nested, "nested capture failed".to_owned());
            failures.queue_capture(unrelated.clone(), "other capture failed".to_owned());
            failures.queue_subtree(&destination, "renamed directory".to_owned());
            failures.rename_pending_captures(&source, &destination);
            assert!(!failures.pending_captures.contains_key(&source));
            assert!(failures.pending_captures[&destination].subtree);
        }
        let captured = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&captured);
        flush_callback_gate(&executor, Arc::clone(&failure), move |paths| {
            observed
                .lock()
                .expect("captured paths")
                .extend(paths.iter().map(|(path, _, _)| path.clone()));
            Ok(())
        })
        .expect("renamed captures recover");
        let captured = captured.lock().expect("captured paths");
        assert!(captured.contains(&destination));
        assert!(captured.contains(&nested_destination));
        assert!(captured.contains(&unrelated));
        assert!(!captured.contains(&source));
    }

    #[test]
    fn later_path_capture_keeps_pending_subtree_scope() {
        let path = MountPath::root().child(b"imported-directory".to_vec());
        let mut failures = PostOperationFailures::default();
        failures.queue_subtree(&path, "rename pending".to_owned());
        failures.queue_capture(path.clone(), "close pending".to_owned());
        assert!(failures.pending_captures[&path].subtree);
    }

    #[test]
    fn renamed_directory_capture_precedes_its_children() {
        let executor = CallbackGate::start().expect("callback gate");
        let failure = Arc::new(Mutex::new(PostOperationFailures::default()));
        let parent = MountPath::root().child(b"renamed".to_vec());
        let child = parent.child(b"child".to_vec());
        {
            let mut failures = failure.lock().expect("failure state");
            failures.queue_capture(child.clone(), "child write".to_owned());
            failures.queue_subtree(&parent, "directory rename".to_owned());
        }
        let observed = Arc::new(Mutex::new(Vec::new()));
        let captures = Arc::clone(&observed);
        flush_callback_gate(&executor, Arc::clone(&failure), move |paths| {
            captures
                .lock()
                .expect("capture order")
                .extend(paths.iter().map(|(path, _, subtree)| (path.clone(), *subtree)));
            Ok(())
        })
        .expect("parent and child capture");
        assert_eq!(
            *observed.lock().expect("capture order"),
            vec![(parent, true), (child, false)]
        );
    }

    #[test]
    fn rename_matches_windows_case_spelling() {
        let source = windows_path("Old");
        let callback_source = windows_path("old");
        let destination = windows_path("New");
        let mut failures = PostOperationFailures::default();
        failures.queue_capture(source.clone(), "capture failed".to_owned());
        failures.rename_pending_captures(&callback_source, &destination);
        assert!(!failures.pending_captures.contains_key(&source));
        assert!(failures.pending_captures.contains_key(&destination));
    }

    #[test]
    fn callback_admitted_after_first_stop_barrier_prevents_cleanup() {
        let executor = CallbackGate::start().expect("callback gate");
        let failure = Arc::new(Mutex::new(PostOperationFailures::default()));
        flush_callback_gate(&executor, Arc::clone(&failure), |_| Ok(())).expect("first barrier");
        let observed = Arc::clone(&failure);
        let path = MountPath::root().child(b"late-object".to_vec());
        let result = executor
            .call_observed(
                0,
                || Err::<(), _>(MountSourceError::Invalid("late capture failed".to_owned())),
                move |result| {
                    record_post_operation_failure(
                        observed.as_ref(),
                        PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED,
                        &path,
                        Some(&path),
                        result,
                    );
                },
            )
            .expect("callback worker");
        assert!(result.is_err());
        assert!(
            flush_callback_gate(&executor, Arc::clone(&failure), |_| {
                Err(MountSourceError::Invalid("late capture failed".to_owned()))
            })
            .expect_err("second barrier must prevent cleanup")
            .to_string()
            .contains("late capture failed")
        );
    }

}
