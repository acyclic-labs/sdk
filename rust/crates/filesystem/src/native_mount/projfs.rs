//! Audited Windows `ProjFS` FFI with exact close-boundary authored capture.
//!
//! `ProjFS` does not expose write byte ranges, so modified and newly created
//! files are streamed from final host state only after the corresponding file
//! handle closes. Rename and hard-link notifications preserve stable identity,
//! and every acknowledged authored notification seals the checkout generation.

#![allow(unsafe_code, unsafe_op_in_unsafe_fn)]

use super::provider_stack;
use super::{
    ContentSink, DriverStartFailure, MountContentPin, MountFilesystem, MountLookup, MountNode,
    MountNodeKind, MountPath, MountSourceError, NativeMountError, NativeMountRequest, ViewObserver,
    ViewOrigin, ViewStamp,
};
use crate::FileId;
use crate::kernel::{FileMetadata, MetadataField};
use crate::native_host::HostRoot;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::ffi::c_void;
use std::mem::size_of;
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock};
use windows::Win32::Storage::FileSystem::{
    FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS,
    FILE_ATTRIBUTE_REPARSE_POINT,
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
    PRJ_NOTIFY_TYPES, PRJ_PLACEHOLDER_INFO, PRJ_PLACEHOLDER_VERSION_INFO,
    PRJ_STARTVIRTUALIZING_OPTIONS, PRJ_UPDATE_ALLOW_READ_ONLY, PRJ_UPDATE_ALLOW_TOMBSTONE,
    PRJ_UPDATE_TYPES, PrjAllocateAlignedBuffer, PrjDeleteFile, PrjFileNameCompare,
    PrjFreeAlignedBuffer, PrjMarkDirectoryAsPlaceholder, PrjStartVirtualizing, PrjStopVirtualizing,
    PrjUpdateFileIfNeeded, PrjWriteFileData, PrjWritePlaceholderInfo, PrjWritePlaceholderInfo2,
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
const HR_OUT_OF_MEMORY: HRESULT = HRESULT(0x8007_000e_u32.cast_signed());
const HR_UNEXPECTED: HRESULT = HRESULT(0x8000_ffff_u32.cast_signed());
const HR_FILE_INVALID: HRESULT = HRESULT(0x8007_03ee_u32.cast_signed());
const HR_IO_DEVICE: HRESULT = HRESULT(0x8007_045d_u32.cast_signed());
const HR_VIRTUALIZATION_INVALID_OPERATION: HRESULT = HRESULT(0x8007_0181_u32.cast_signed());
const HR_SHARING_VIOLATION: HRESULT = HRESULT(0x8007_0020_u32.cast_signed());
const HR_DIRECTORY_NOT_EMPTY: HRESULT = HRESULT(0x8007_0091_u32.cast_signed());

const DIRECTORY_PAGE_SIZE: u32 = 256;
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
/// Largest hydration unit. A multiple of every sector size, so each chunk
/// but the last keeps `PrjWriteFileData` aligned; bounds peak memory.
const HYDRATION_CHUNK_BYTES: u32 = 1 << 20;
/// Bounds the provider's memo of read-only close bindings.
const MAXIMUM_CACHED_BINDINGS: usize = 16_384;
/// Marks a placeholder whose `ContentID` carries a [`MountContentPin`].
const CONTENT_PIN_PROVIDER: &[u8; 16] = b"acyclic-fs-pin-1";

/// One entry of a source listing, as this provider writes it.
struct ProjectedEntry {
    // ProjFS compares NUL-terminated UTF-16 names, not SDK byte cursors.
    name: Vec<u16>,
    /// The entry's exact path component.
    component: Vec<u8>,
    /// What the placeholder is written from.
    facts: PlaceholderFacts,
    /// The basic information, and with the pin exactly the placeholder
    /// written for this entry.
    info: PRJ_FILE_BASIC_INFO,
    symlink_target: Option<bytes::Bytes>,
    /// Whether a link names a directory: a Windows link says so when made.
    directory_link: bool,
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
    placeholders: Arc<Placeholders>,
    metadata_probes: Arc<Mutex<HashMap<MountPath, usize>>>,
    /// Held while one handle's metadata edits are read from the source and
    /// written back. `ProjFS` names each handle apart, so handles to one file
    /// close concurrently; each applies only what it changed, on what the
    /// one before it left.
    metadata_edits: Arc<Mutex<()>>,
    post_operation_failure: Arc<Mutex<PostOperationFailures>>,
    callbacks: CallbackGate,
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
        const SHARDS: usize = 64;
        let mut shards = Vec::new();
        shards
            .try_reserve_exact(SHARDS)
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        for _ in 0..SHARDS {
            shards.push(Mutex::new(()));
        }
        Ok(Self {
            shards: shards.into_boxed_slice(),
            active: Mutex::new(0),
            idle: Condvar::new(),
        })
    }

    fn call<T>(&self, operation: impl FnOnce() -> T) -> Option<T> {
        self.drain()?;
        Some(operation())
    }

    fn drain(&self) -> Option<()> {
        let mut active = lock_recover(&self.active);
        while *active != 0 {
            active = self
                .idle
                .wait(active)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        Some(())
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

fn flush_callback_gate(
    callbacks: &CallbackGate,
    failure: Arc<Mutex<PostOperationFailures>>,
    capture: impl Fn(&[(MountPath, bool)]) -> Result<(), MountSourceError>,
) -> Result<(), NativeMountError> {
    let pending_failure = Arc::clone(&failure);
    let pending = callbacks
        .call(move || {
            lock_recover(pending_failure.as_ref())
                .pending_captures
                .iter()
                .map(|(path, capture)| (path.clone(), capture.serial, capture.subtree))
                .collect::<Vec<_>>()
        })
        .ok_or_else(|| NativeMountError::Driver("ProjFS callback worker stopped".to_owned()))?;
    let mut pending = pending;
    pending.sort_by(|(left, ..), (right, ..)| {
        left.components()
            .len()
            .cmp(&right.components().len())
            .then_with(|| left.components().cmp(right.components()))
    });
    // Host capture opens paths inside the virtualization root. Run one batch
    // outside the callback gate so nested notifications can run.
    let paths = pending
        .iter()
        .map(|(path, _, subtree)| (path.clone(), *subtree))
        .collect::<Vec<_>>();
    let result = capture(&paths);
    {
        let mut failure = lock_recover(failure.as_ref());
        for (path, serial, _) in pending {
            if failure
                .pending_captures
                .get(&path)
                .map(|capture| capture.serial)
                != Some(serial)
            {
                continue;
            }
            match &result {
                Ok(()) => {
                    failure.pending_captures.remove(&path);
                }
                Err(error) => {
                    if let Some(capture) = failure.pending_captures.get_mut(&path) {
                        capture.error = error.to_string();
                    }
                }
            }
        }
    }
    let failure = callbacks
        .call(move || {
            let failure = lock_recover(failure.as_ref());
            failure.unreplayable.clone().or_else(|| {
                failure
                    .pending_captures
                    .iter()
                    .next()
                    .map(|(path, capture)| format!("{path:?}: {}", capture.error))
            })
        })
        .ok_or_else(|| NativeMountError::Driver("ProjFS callback worker stopped".to_owned()))?;
    if let Some(failure) = failure {
        Err(NativeMountError::Driver(format!(
            "ProjFS post-operation capture failed; mount cannot publish: {failure}"
        )))
    } else {
        Ok(())
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

/// What writing one entry into the projection placed there.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Placed {
    /// Nothing: something is there already, or its directory is gone.
    Nothing,
    /// A placeholder, or an ordinary directory.
    Placeholder,
    /// A symbolic link to the target with this digest: a link placeholder,
    /// or an ordinary link where the volume refuses those (`ReFS`).
    Link(u64),
}

impl Placed {
    const fn link(self) -> Option<u64> {
        match self {
            Self::Link(index) => Some(index),
            Self::Nothing | Self::Placeholder => None,
        }
    }
}

/// What one placeholder was written from: the node, its metadata, and the
/// content its hydration returns.
#[derive(Clone, Copy, PartialEq, Eq)]
struct PlaceholderFacts {
    lookup: MountLookup,
    pin: Option<MountContentPin>,
}

impl PlaceholderFacts {
    /// Whether a placeholder written from `self` is also the one `current`
    /// would write. Access times move with every read and are not written.
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

/// One placeholder `ProjFS` keeps on disk, as a source read wrote it.
#[derive(Clone, Copy, PartialEq, Eq)]
struct WrittenPlaceholder {
    file_id: FileId,
    /// The digest of the target of the link written here: a link
    /// placeholder turns full once read, which `ProjFS` deletes only when
    /// told its data may be dirty, so the provider deletes it only while it
    /// still points where it was written to.
    link: Option<u64>,
    /// What it was written from; `None` for one known only to be gone.
    facts: Option<PlaceholderFacts>,
    /// The view it was read in; `None` when the source cannot version it
    /// or was moving, which no later change is needed to supersede.
    basis: Option<ReadBasis>,
}

impl WrittenPlaceholder {
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
    files: Vec<(MountPath, PlaceholderFacts)>,
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
                    .then_some((moved, PlaceholderFacts { lookup, pin }))
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

/// Most threads that write one tree of placeholders at once.
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
}

/// The projection this provider writes, and the worker that keeps it
/// current.
///
/// `ProjFS` never lets a directory it projected be renamed, so nothing is
/// projected on demand: every directory is created as an ordinary directory,
/// and a placeholder is written for every file in it before anything can
/// list it (`ProjFS` asks the provider nothing about an ordinary directory).
/// A placeholder's content is still read only when it is first opened.
///
/// `ProjFS` answers a placeholder from disk, attributes and all, until its
/// provider updates or deletes it. So every placeholder is kept with the
/// basis it was written in, and once a change supersedes it the worker
/// writes it again from the source, or deletes it where the source names
/// nothing. Every directory is kept with the names it holds, and once its
/// source listing changes the worker reconciles three ways: a name the
/// source gained is written unless something is there already, and a
/// directory the source lost is emptied of the placeholders written into it
/// and removed if nothing else is left. What the user creates, changes or
/// deletes through the mount is authored state and stays: `ProjFS` refuses
/// to update or delete a placeholder once it is modified.
///
/// A superseded placeholder is pending from the moment the worker finds it
/// until it is replaced, released, or fails, including while the worker is
/// replacing it, so revalidation never returns while one still describes a
/// superseded source. One another process holds open cannot be replaced
/// until the handle closes: it stays pending, retried as handles close and
/// every [`PLACEHOLDER_RETRY`], and revalidation waits for it at most
/// [`PLACEHOLDER_SETTLE_LIMIT`] once every change has been served, then
/// fails naming it.
struct Placeholders {
    state: Mutex<PlaceholderState>,
    /// Held by whatever writes directories into the projection, so a
    /// listing is written and recorded as one step.
    tree: Mutex<()>,
    /// Held exclusively while a rename moves a file in the source and in
    /// [`PlaceholderState::moves`], and shared by each content read, which
    /// so finds a file at its old path or where `moves` says it went.
    renames: RwLock<()>,
    changed: Condvar,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
}

/// How often a placeholder held open is tried again.
const PLACEHOLDER_RETRY: std::time::Duration = std::time::Duration::from_millis(20);
/// How long revalidation waits for placeholders held open, once every
/// change has been served.
const PLACEHOLDER_SETTLE_LIMIT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Default)]
struct PlaceholderState {
    written: HashMap<MountPath, WrittenPlaceholder>,
    /// Superseded placeholders not yet replaced, released, or failed.
    pending: HashMap<MountPath, WrittenPlaceholder>,
    /// Every directory created in the projection.
    directories: HashMap<MountPath, Materialized>,
    /// Directories to list again whatever their basis says.
    stale: HashSet<MountPath>,
    /// Pending paths a listing waits on: when one is settled, its parent is
    /// listed again.
    relist: HashSet<MountPath>,
    /// Where each path renames through the mount moved is now, by the path
    /// it had: `ProjFS` asks for a placeholder's content by the path it was
    /// written at, also after a rename of a directory above it moved it.
    /// One entry per path renamed away, composed with later renames, so
    /// moving a directory back and forth leaves one.
    moves: HashMap<MountPath, MountPath>,
    /// Whether a handle closed since pending placeholders were last tried.
    retry: bool,
    /// Latest change around the mount the source reported.
    notified: Option<ViewStamp>,
    /// Latest change every placeholder was checked against.
    processed: Option<ViewStamp>,
    /// First placeholder the worker could not replace since the last
    /// revalidation.
    failure: Option<String>,
    stopping: bool,
}

impl PlaceholderState {
    /// Records what one round of replacing pending placeholders came to.
    fn record_attempts(
        &mut self,
        settled: Vec<(MountPath, WrittenPlaceholder)>,
        kept: Vec<(MountPath, WrittenPlaceholder)>,
    ) {
        for (path, written) in settled {
            // Superseded again meanwhile, it stays pending as that.
            if self.pending.get(&path) == Some(&written) {
                self.pending.remove(&path);
                if self.relist.remove(&path)
                    && let Some(parent) = path.parent()
                {
                    self.stale.insert(parent);
                }
            }
        }
        for (path, placeholder) in kept {
            // Written again meanwhile, from a later basis.
            let entry = self.written.entry(path).or_insert(placeholder);
            if entry.basis.map(|basis| basis.stamp) < placeholder.basis.map(|basis| basis.stamp) {
                *entry = placeholder;
            }
        }
    }
}

impl ViewObserver for Placeholders {
    fn view_changed(&self, position: ViewStamp, _origin: ViewOrigin) {
        let mut state = lock_recover(&self.state);
        state.notified = state.notified.max(Some(position));
        self.changed.notify_all();
    }
}

/// What replacing one superseded placeholder came to.
enum Replaced {
    /// Written again from the source.
    Rewritten(Box<WrittenPlaceholder>),
    /// Deleted, or no longer a placeholder the provider owns.
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

impl Placeholders {
    fn new() -> Self {
        Self {
            state: Mutex::new(PlaceholderState::default()),
            tree: Mutex::new(()),
            renames: RwLock::new(()),
            changed: Condvar::new(),
            worker: Mutex::new(None),
        }
    }

    /// Keeps the placeholder written at `path` in view `basis`.
    fn written(
        &self,
        path: MountPath,
        facts: PlaceholderFacts,
        link: Option<u64>,
        basis: Option<ReadBasis>,
    ) {
        lock_recover(&self.state).written.insert(
            path,
            WrittenPlaceholder {
                file_id: facts.lookup.node.file_id,
                link,
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
        // Each placeholder is one kernel call, independent of the others:
        // several workers write a tree in a fraction of the time one does.
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
                return Ok(Vec::new());
            }
            Err(error) => return Err(format!("listing {directory:?}: {error}")),
        };
        let mut names = HashMap::with_capacity(entries.len());
        let mut found = Vec::new();
        let mut written = Vec::with_capacity(entries.len());
        for entry in &entries {
            let child = directory.child(entry.component.clone());
            let is_directory = entry.info.IsDirectory;
            let placed = write_entry(projection, &child, entry)?;
            if is_directory {
                found.push(child);
            } else if placed != Placed::Nothing {
                written.push((child, entry.facts, placed.link()));
            }
            names.insert(entry.component.clone(), is_directory);
        }
        let mut state = lock_recover(&self.state);
        for (path, facts, link) in written {
            state.written.insert(
                path,
                WrittenPlaceholder {
                    file_id: facts.lookup.node.file_id,
                    link,
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
    /// it was last written with (see [`Placeholders`]). A file the source
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
            match known.get(&entry.component) {
                Some(&was_directory) if was_directory == is_directory => continue,
                // Now another kind: what was written for the old one goes.
                Some(&true) => self.remove_tree(projection, &child),
                Some(&false) => {
                    // Written, or superseded and pending since.
                    let link = {
                        let state = lock_recover(&self.state);
                        state
                            .written
                            .get(&child)
                            .or_else(|| state.pending.get(&child))
                            .and_then(|written| written.link)
                    };
                    if release(projection, &child, link)? == Release::Busy {
                        // Tried again as pending, at the retry interval; this
                        // listing stays unrecorded, and is listed again once
                        // the old entry is gone.
                        let mut state = lock_recover(&self.state);
                        let written = state.written.remove(&child).unwrap_or(WrittenPlaceholder {
                            file_id: FileId::from_bytes([0; 16]),
                            link,
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
            if is_directory {
                created.push(child);
            } else if placed != Placed::Nothing {
                self.written(child, entry.facts, placed.link(), basis);
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
                // Replaced like any superseded placeholder: deleted unless
                // modified, and tried again while held open.
                let mut state = lock_recover(&self.state);
                // Already pending, it is replaced as what it was written as.
                if !state.pending.contains_key(&child) {
                    let placeholder = state.written.remove(&child).unwrap_or(WrittenPlaceholder {
                        file_id: FileId::from_bytes([0; 16]),
                        link: None,
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

    /// Deletes what was written beneath `directory` that `ProjFS` still
    /// holds untouched, and each directory left empty; forgets all of it.
    fn remove_tree(&self, projection: Projection<'_>, directory: &MountPath) {
        let (mut removed, links) = {
            let mut state = lock_recover(&self.state);
            let links = state
                .written
                .iter()
                .chain(&state.pending)
                .filter(|(path, _)| projfs_path_suffix(path, directory).is_some())
                .filter_map(|(path, written)| written.link.map(|link| (path.clone(), link)))
                .collect::<HashMap<_, _>>();
            let beneath = state
                .directories
                .keys()
                .filter(|path| projfs_path_suffix(path, directory).is_some())
                .cloned()
                .collect::<Vec<_>>();
            let removed = beneath
                .into_iter()
                .filter_map(|path| state.directories.remove(&path).map(|entry| (path, entry)))
                .collect::<Vec<_>>();
            (removed, links)
        };
        // Deepest first, so each directory holds nothing written by the time
        // it is removed.
        removed.sort_by_key(|(path, _)| std::cmp::Reverse(path.components().len()));
        for (path, materialized) in removed {
            for (name, is_directory) in materialized.names {
                if !is_directory {
                    let child = path.child(name);
                    let _ = release(projection, &child, links.get(&child).copied());
                }
            }
            if let Ok(host) = host_relative_path(&path)
                && !host.as_os_str().is_empty()
            {
                // What the user left in it keeps it.
                let _ = std::fs::remove_dir(projection.root.join(host));
            }
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
        for placeholders in [&mut state.written, &mut state.pending] {
            let moved = placeholders
                .keys()
                .filter_map(|path| rebased(path, from, to).map(|moved| (path.clone(), moved)))
                .collect::<Vec<_>>();
            for (old, new) in moved {
                if let Some(written) = placeholders.remove(&old) {
                    placeholders.insert(new, written);
                }
            }
        }
        // Paths an earlier rename moved to within `from` had other names
        // before: those follow this rename too.
        let earlier = state
            .moves
            .iter()
            .filter_map(|(origin, current)| rebased(from, current, origin))
            .filter(|origin| origin != from)
            .collect::<Vec<_>>();
        // What earlier renames moved beneath `from` moves on; a path back
        // where it started needs no entry.
        for current in state.moves.values_mut() {
            if let Some(moved) = rebased(current, from, to) {
                *current = moved;
            }
        }
        for origin in earlier.into_iter().chain(std::iter::once(from.clone())) {
            state.moves.entry(origin).or_insert_with(|| to.clone());
        }
        state.moves.retain(|origin, current| origin != current);
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
                WrittenPlaceholder {
                    file_id: facts.lookup.node.file_id,
                    link: written.or(pending).and_then(|written| written.link),
                    facts: Some(facts),
                    basis: carried.basis,
                },
            );
        }
    }

    /// Where the placeholder written at `path` is now, following every
    /// rename through the mount since.
    fn moved_path(&self, path: &MountPath) -> MountPath {
        // The deepest path renamed away that holds `path` says where it is.
        let state = lock_recover(&self.state);
        state
            .moves
            .iter()
            .filter(|(origin, _)| projfs_path_suffix(path, origin).is_some())
            .max_by_key(|(origin, _)| origin.components().len())
            .and_then(|(origin, current)| rebased(path, origin, current))
            .unwrap_or_else(|| path.clone())
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
    ) -> Result<(), NativeMountError> {
        let placeholders = Arc::clone(self);
        let observed = Arc::clone(&source);
        let context = SendContext(context);
        let worker = std::thread::Builder::new()
            .name("acyclic-projfs-placeholders".to_owned())
            .spawn(move || {
                let context = context;
                placeholders.run(Projection {
                    source: source.as_ref(),
                    context: context.0,
                    root: &root,
                });
            })
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        *lock_recover(&self.worker) = Some(worker);
        let placeholders = Arc::downgrade(self);
        let observer: std::sync::Weak<dyn ViewObserver> = placeholders;
        observed.observe_view(observer);
        Ok(())
    }

    fn run(&self, projection: Projection<'_>) {
        let source = projection.source;
        let mut state = lock_recover(&self.state);
        loop {
            // Waits for a change, or, while placeholders are pending, for a
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
                        .wait_timeout(state, PLACEHOLDER_RETRY)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    state = next;
                    if waited.timed_out() {
                        break;
                    }
                }
            }
            state.retry = false;
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
            // Attempted as a snapshot: each stays pending until its outcome
            // is recorded.
            let mut attempted = lock_recover(&self.state)
                .pending
                .iter()
                .map(|(path, written)| (path.clone(), *written))
                .collect::<Vec<_>>();
            attempted.sort_by_key(|(path, _)| std::cmp::Reverse(path.components().len()));
            let (mut settled, mut kept) = (Vec::new(), Vec::new());
            for (path, written) in attempted {
                match replace_placeholder(projection, &path, &written) {
                    Ok(Replaced::Rewritten(current)) => {
                        let current = *current;
                        kept.push((path.clone(), current));
                        settled.push((path, written));
                    }
                    Ok(Replaced::Released) => {
                        settled.push((path, written));
                    }
                    Ok(Replaced::Busy) => {}
                    Err(error) => {
                        failure.get_or_insert(error);
                        settled.push((path, written));
                    }
                }
            }
            state = lock_recover(&self.state);
            state.record_attempts(settled, kept);
            state.processed = state.processed.max(through);
            if let Some(failure) = failure {
                state.failure.get_or_insert(failure);
            }
            self.changed.notify_all();
        }
    }

    /// Reads each placeholder only unconfirmed changes superseded again:
    /// one the source would still write as it is holds from now on, and
    /// any other is replaced.
    fn confirm(
        &self,
        source: &dyn MountFilesystem,
        unconfirmed: Vec<(MountPath, WrittenPlaceholder)>,
    ) {
        for (path, written) in unconfirmed {
            let basis = ReadBasis::sample(source);
            let current = source.lookup_pinned(&path);
            let mut state = lock_recover(&self.state);
            match current {
                Ok(Some((lookup, pin)))
                    if written
                        .facts
                        .is_some_and(|facts| facts.describe(&PlaceholderFacts { lookup, pin })) =>
                {
                    // Written again meanwhile, from a later read.
                    state
                        .written
                        .entry(path)
                        .or_insert(WrittenPlaceholder { basis, ..written });
                }
                _ => {
                    state.pending.insert(path, written);
                }
            }
        }
    }

    /// Tries the placeholders held open again, as one of their handles
    /// closed.
    fn retry(&self) {
        let mut state = lock_recover(&self.state);
        if !state.pending.is_empty() {
            state.retry = true;
            self.changed.notify_all();
        }
    }

    /// Waits until every change reported so far has been served and no
    /// superseded placeholder or invalidated directory remains; fails,
    /// naming one, when one is still held open [`PLACEHOLDER_SETTLE_LIMIT`]
    /// after every change was served.
    fn settle(&self) -> Result<(), NativeMountError> {
        let mut state = lock_recover(&self.state);
        let target = state.notified;
        let mut deadline = None;
        while !state.stopping
            && (state.processed < target || !state.stale.is_empty() || !state.pending.is_empty())
        {
            if state.processed < target || !state.stale.is_empty() {
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                continue;
            }
            let deadline = *deadline
                .get_or_insert_with(|| std::time::Instant::now() + PLACEHOLDER_SETTLE_LIMIT);
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

    /// Stops the worker; nothing is kept current from here on.
    fn finish(&self) {
        lock_recover(&self.state).stopping = true;
        self.changed.notify_all();
        if let Some(worker) = lock_recover(&self.worker).take() {
            let _ = worker.join();
        }
    }
}

/// Writes `entry` at `path`: an ordinary directory for a directory, a
/// placeholder for anything else. Whatever is already there (the user's, or
/// written before) stays, and is reported as not written.
fn write_entry(
    projection: Projection<'_>,
    path: &MountPath,
    entry: &ProjectedEntry,
) -> Result<Placed, String> {
    let host = host_relative_path(path).map_err(|error| error.to_string())?;
    if entry.info.IsDirectory {
        return match std::fs::create_dir(projection.root.join(&host)) {
            Ok(()) => Ok(Placed::Placeholder),
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
    let mut placeholder = pinned_placeholder(entry.info, entry.facts.pin)
        .ok_or_else(|| format!("placeholder for {path:?} is unrepresentable"))?;
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
        Ok(()) => Ok(symlink.map_or(Placed::Placeholder, |(target, _)| {
            Placed::Link(link_digest(target.iter().copied()))
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
                "writing link {path:?}: {error} (this volume has no link placeholders, so \
                 projecting a link needs the right to create one: Developer Mode)"
            )),
        };
    }
    Ok(Placed::Link(link_digest(target.iter().copied())))
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
/// their own: a placeholder, or the link to the target with digest `link`.
fn release(
    projection: Projection<'_>,
    path: &MountPath,
    link: Option<u64>,
) -> Result<Release, String> {
    let Some(digest) = link else {
        return delete_placeholder(projection, path);
    };
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
    // Deleted directly, it leaves a tombstone that would hide what is written
    // there next. Only a tombstone goes here: whatever the user made since
    // stays.
    match prj_delete(projection, &relative, PRJ_UPDATE_ALLOW_TOMBSTONE) {
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

/// Deletes the symbolic link at `path` if it points to the target with
/// `digest`. The link itself is opened, not followed, and the object checked
/// is the one deleted, whatever takes its name meanwhile. `false` when
/// something else is there.
fn delete_link(path: &std::path::Path, digest: u64) -> std::io::Result<bool> {
    use std::os::windows::fs::OpenOptionsExt as _;
    use std::os::windows::io::AsRawHandle as _;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::{
        DELETE, FILE_DISPOSITION_FLAG_DELETE, FILE_DISPOSITION_FLAG_POSIX_SEMANTICS,
        FILE_DISPOSITION_INFO, FILE_DISPOSITION_INFO_EX, FILE_DISPOSITION_INFO_EX_FLAGS,
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_READ_ATTRIBUTES,
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, FileDispositionInfo,
        FileDispositionInfoEx, SetFileInformationByHandle,
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
    let handle = HANDLE(link.as_raw_handle());
    // A POSIX delete takes the name at once, also while another process
    // holds the link; delete-on-close serves where that is refused.
    let posix = FILE_DISPOSITION_INFO_EX {
        Flags: FILE_DISPOSITION_INFO_EX_FLAGS(
            FILE_DISPOSITION_FLAG_DELETE.0 | FILE_DISPOSITION_FLAG_POSIX_SEMANTICS.0,
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
    Ok(true)
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

/// Deletes the placeholder at `path` unless the user modified it.
fn delete_placeholder(projection: Projection<'_>, path: &MountPath) -> Result<Release, String> {
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

/// Writes the placeholder at `path` again from the source, or deletes it
/// where the source names nothing there any more (or a directory, which the
/// directory's reconcile writes).
fn replace_placeholder(
    projection: Projection<'_>,
    path: &MountPath,
    written: &WrittenPlaceholder,
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
    // A link's target is part of the link itself, which no update carries:
    // what was written goes and the link is written anew, as is whatever
    // takes the place of an ordinary link written for one.
    if written.link.is_some()
        || current.is_some_and(|(lookup, _)| lookup.node.kind == MountNodeKind::SymbolicLink)
    {
        return rewrite(projection, path, &relative, written.link, current, basis);
    }
    let result = match current {
        // A directory is written by its parent's reconcile.
        Some((lookup, pin))
            if !matches!(
                lookup.node.kind,
                MountNodeKind::SymbolicLink | MountNodeKind::Directory
            ) =>
        {
            let Some(placeholder) = placeholder_info(&lookup, pin) else {
                return Err("placeholder attributes are unrepresentable".to_owned());
            };
            // SAFETY: the name and placeholder outlive this synchronous
            // call on a live context.
            unsafe {
                PrjUpdateFileIfNeeded(
                    projection.context,
                    &relative,
                    &raw const placeholder,
                    u32::try_from(size_of::<PRJ_PLACEHOLDER_INFO>()).unwrap_or(u32::MAX),
                    Some(PRJ_UPDATE_ALLOW_READ_ONLY),
                    None,
                )
            }
            .map(|()| {
                Replaced::Rewritten(Box::new(WrittenPlaceholder {
                    file_id: lookup.node.file_id,
                    link: None,
                    facts: Some(PlaceholderFacts { lookup, pin }),
                    basis,
                }))
            })
        }
        // SAFETY: as above.
        _ => unsafe {
            PrjDeleteFile(
                projection.context,
                &relative,
                Some(PRJ_UPDATE_ALLOW_READ_ONLY),
                None,
            )
        }
        .map(|()| Replaced::Released),
    };
    match result {
        Ok(replaced) => Ok(replaced),
        // Gone already, authored since, or holding authored state: a
        // placeholder's read-only attribute, which follows its source, never
        // refuses an update, so nothing refused here is still projected.
        Err(error)
            if [
                HR_FILE_NOT_FOUND,
                HR_PATH_NOT_FOUND,
                HR_VIRTUALIZATION_INVALID_OPERATION,
                HR_DIRECTORY_NOT_EMPTY,
            ]
            .contains(&error.code()) =>
        {
            Ok(Replaced::Released)
        }
        Err(error) if error.code() == HR_SHARING_VIOLATION => Ok(Replaced::Busy),
        Err(error) => Err(driver_error(&error).to_string()),
    }
}

/// Deletes what was written at `path` and writes what the source holds
/// there now, for what no update can change in place: links.
fn rewrite(
    projection: Projection<'_>,
    path: &MountPath,
    relative: &HSTRING,
    link: Option<u64>,
    current: Option<(MountLookup, Option<MountContentPin>)>,
    basis: Option<ReadBasis>,
) -> Result<Replaced, String> {
    if link.is_some() {
        if release(projection, path, link)? == Release::Busy {
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
        placed => Replaced::Rewritten(Box::new(WrittenPlaceholder {
            file_id: lookup.node.file_id,
            link: placed.link(),
            facts: Some(entry.facts),
            basis,
        })),
    })
}

/// A virtualization context moved to the placeholder worker. The handle is
/// an opaque token `ProjFS` accepts from any thread.
struct SendContext(PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT);

// SAFETY: see `SendContext`; its lifetime is bounded by `Placeholders::finish`.
unsafe impl Send for SendContext {}

fn has_pending_capture(failure: &Mutex<PostOperationFailures>, path: &MountPath) -> bool {
    let failure = lock_recover(failure);
    failure.unreplayable.is_some()
        || failure.pending_captures.iter().any(|(pending, capture)| {
            pending == path || capture.subtree && projfs_path_suffix(path, pending).is_some()
        })
}

/// One process-owned `ProjFS` virtualization context.
pub(super) struct ProjFsSession {
    context: Option<PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT>,
    runtime: Option<Box<Runtime>>,
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
            placeholders: Arc::new(Placeholders::new()),
            metadata_probes: Arc::new(Mutex::new(HashMap::new())),
            metadata_edits: Arc::new(Mutex::new(())),
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
        // Nothing is ever projected on demand (see `Placeholders`), so every
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
                .placeholders
                .materialize(
                    Projection {
                        source: runtime.source.as_ref(),
                        context,
                        root: &runtime.root,
                    },
                    MountPath::root(),
                )
                .map_err(|error| {
                    NativeMountError::Driver(format!("projecting the source failed: {error}"))
                })
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
        })
    }

    pub(super) fn stop(&mut self) -> Result<(), NativeMountError> {
        // A post-operation notification cannot veto an already completed host
        // write. Preserve the live mount and its authored files if capturing
        // one of those writes failed; removing the projection here would lose
        // the only remaining copy.
        if self.runtime.is_some() {
            self.flush_callbacks()?;
        }
        if let Some(context) = self.context.take() {
            if let Some(runtime) = self.runtime.as_ref() {
                runtime.finish_workers();
            }
            // SAFETY: this is the sole owner and sole stop call for the context.
            unsafe { PrjStopVirtualizing(context) };
        }
        // Stop may synchronously drain callbacks admitted after the first
        // barrier. They must be captured (or reported) before the physical
        // projection can be removed.
        if self.runtime.is_some() {
            self.flush_callbacks()?;
        }
        finish_cleanup(&mut self.runtime, |runtime| {
            let root = runtime.root.clone();
            remove_authenticated_destination(&root, runtime.metadata_root.identity())?;
            std::fs::create_dir(&root).map_err(|error| NativeMountError::Driver(error.to_string()))
        })
    }

    pub(super) fn flush_callbacks(&self) -> Result<(), NativeMountError> {
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            NativeMountError::Driver("ProjFS callback runtime is stopped".to_owned())
        })?;
        let source = Arc::clone(&runtime.source);
        let root = runtime.root.clone();
        let host_root = Arc::clone(&runtime.metadata_root);
        let probes = Arc::clone(&runtime.metadata_probes);
        runtime
            .callbacks
            .drain()
            .ok_or_else(|| NativeMountError::Driver("ProjFS callback worker stopped".to_owned()))?;
        flush_callback_gate(
            &runtime.callbacks,
            Arc::clone(&runtime.post_operation_failure),
            move |paths| {
                if paths.is_empty() {
                    return Ok(());
                }
                // Host capture reads through this projection. Suppress only
                // those provider-owned metadata probes; external opens retain
                // their per-handle baselines.
                let _guards = paths
                    .iter()
                    .map(|(path, _)| MetadataProbeGuard::enter(probes.as_ref(), path))
                    .collect::<Vec<_>>();
                // Authenticate exact paths together so hard links share one
                // identity and one checkout transaction.
                let mut exact = Vec::new();
                let mut subtrees = Vec::new();
                for (path, subtree) in paths {
                    let host_path = host_relative_path(path)?;
                    // Only whether the name exists decides the capture's
                    // scope; the capture itself observes it through the
                    // held walk.
                    match host_root.stat(&host_path) {
                        Ok(_) => {
                            capture_missing_host_ancestors(source.as_ref(), &root, path)?;
                            if *subtree {
                                subtrees.push(path.clone());
                            } else {
                                exact.push(path.clone());
                            }
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            match source.lookup(path) {
                                Err(MountSourceError::NotFound) | Ok(None) => {
                                    // A host-created temporary that vanished
                                    // before the boundary has no state to join.
                                }
                                Ok(Some(lookup))
                                    if lookup.node.kind == MountNodeKind::Directory =>
                                {
                                    subtrees.push(path.clone());
                                }
                                Ok(Some(_)) => exact.push(path.clone()),
                                Err(error) => return Err(error),
                            }
                        }
                        Err(error) => {
                            return Err(MountSourceError::Engine(error.to_string()));
                        }
                    }
                }
                for subtree in subtrees {
                    source.capture_host_subtree(&root, &subtree)?;
                }
                // A captured file stays as the host wrote it: its local bytes
                // already are the source's, so nothing is re-projected.
                source.capture_host_paths(&root, &exact)
            },
        )
    }

    /// Makes a source-side change at one mount-relative path (leading `/`
    /// optional) visible: the provider forgets memoized source facts, and an
    /// unmodified placeholder at `path` is deleted and written again from
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
                runtime.placeholders.invalidate(&mount_path);
                Ok(())
            }
        }
    }

    /// Waits until every change made to the source so far is written into
    /// the projection: each superseded placeholder written again, each
    /// changed directory reconciled. A source that reports none of its
    /// changes has everything checked.
    pub(super) fn revalidate(&self) -> Result<(), NativeMountError> {
        let (runtime, _context) = self.live()?;
        if runtime.source.view_stamp().is_none() {
            runtime.placeholders.invalidate_all();
        }
        // Every change made to the source before this call is recorded, and
        // so notified, before the placeholders settle.
        runtime
            .source
            .fence_changes()
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        runtime.placeholders.settle()
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
        self.placeholders
            .attach(Arc::clone(&self.source), context, self.root.clone())
    }

    /// Stops the worker; nothing is kept current from here on.
    fn finish_workers(&self) {
        self.placeholders.finish();
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
        MountSourceError::Engine(_) => HR_IO_DEVICE,
        MountSourceError::Stale => HR_FILE_INVALID,
    }
}

/// Builds the placeholder `ProjFS` persists for one source lookup. A pinned
/// placeholder records exactly which content its hydration must return.
fn placeholder_info(
    lookup: &MountLookup,
    pin: Option<MountContentPin>,
) -> Option<PRJ_PLACEHOLDER_INFO> {
    pinned_placeholder(basic(lookup.node, Some(lookup.metadata))?, pin)
}

/// The placeholder for `info` that carries exactly `pin`.
///
/// `ProjFS` updates a placeholder only when its version differs, so each
/// placeholder written also carries a version of its own after the pin:
/// a rewrite with new attributes or unpinned content is never skipped.
fn pinned_placeholder(
    info: PRJ_FILE_BASIC_INFO,
    pin: Option<MountContentPin>,
) -> Option<PRJ_PLACEHOLDER_INFO> {
    static WRITTEN: AtomicU64 = AtomicU64::new(0);
    let mut version = PRJ_PLACEHOLDER_VERSION_INFO::default();
    let (pinned, written) = version.ContentID.split_at_mut(size_of::<MountContentPin>());
    if let Some(pin) = pin {
        *version.ProviderID.first_chunk_mut()? = *CONTENT_PIN_PROVIDER;
        *pinned.first_chunk_mut()? = pin.0;
    }
    *written.first_chunk_mut()? = WRITTEN.fetch_add(1, Ordering::Relaxed).to_le_bytes();
    Some(PRJ_PLACEHOLDER_INFO {
        FileBasicInfo: info,
        VersionInfo: version,
        ..PRJ_PLACEHOLDER_INFO::default()
    })
}

/// Recovers the content pin a placeholder was written with.
fn placeholder_pin(data: &PRJ_CALLBACK_DATA) -> Option<MountContentPin> {
    // SAFETY: ProjFS supplies either null or the placeholder's version
    // information for the duration of this callback.
    let version = unsafe { data.VersionInfo.as_ref() }?;
    let (tag, rest) = version.ProviderID.split_first_chunk()?;
    if tag != CONTENT_PIN_PROVIDER || rest.iter().any(|byte| *byte != 0) {
        return None;
    }
    version
        .ContentID
        .first_chunk()
        .copied()
        .map(MountContentPin)
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

fn finish_cleanup<T, E>(
    state: &mut Option<T>,
    cleanup: impl FnOnce(&T) -> Result<(), E>,
) -> Result<(), E> {
    let Some(value) = state.as_ref() else {
        return Ok(());
    };
    cleanup(value)?;
    let _ = state.take();
    Ok(())
}

/// Preserves a stale `ProjFS` root unless recovery can prove it is disposable.
///
/// A reparse tag or directory enumeration cannot prove that all authored files,
/// metadata, and tombstones reached the SDK before a provider crash. Only a
/// live session can flush its callbacks before removing the projection. A
/// stale root is retained for explicit recovery rather than silently deleting
/// changes that may exist only in the mount cache.
pub(super) fn recover_cache_only_destination(
    destination: &std::path::Path,
) -> Result<(), NativeMountError> {
    match std::fs::symlink_metadata(destination) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => return Err(NativeMountError::InvalidDestination),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(NativeMountError::Driver(error.to_string())),
    }
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

fn remove_authenticated_destination(
    destination: &std::path::Path,
    expected_identity: crate::NativeRootIdentity,
) -> Result<(), NativeMountError> {
    let metadata = match std::fs::symlink_metadata(destination) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(NativeMountError::Driver(error.to_string())),
    };
    if !metadata.is_dir() {
        return Err(NativeMountError::InvalidDestination);
    }
    let (tag, identity) = reparse_tag(destination).map_err(|error| {
        NativeMountError::Driver(format!(
            "ProjFS root authentication failed for {}: {error}",
            destination.display()
        ))
    })?;
    if identity != expected_identity {
        return Err(NativeMountError::Driver(format!(
            "ProjFS root identity changed at {}; refusing to remove a different directory",
            destination.display()
        )));
    }
    match tag {
        Some(windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS) => {}
        Some(tag) => {
            return Err(NativeMountError::Driver(format!(
                "destination has non-ProjFS reparse tag 0x{tag:08x}"
            )));
        }
        None => return Ok(()),
    }
    // The path can be replaced while the filter is draining, so every
    // removal of the root itself reauthenticates it first.
    let authenticate = || {
        let (current_tag, current_identity) = reparse_tag(destination)?;
        if current_tag != Some(windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS)
            || current_identity != expected_identity
        {
            return Err(NativeMountError::Driver(format!(
                "ProjFS root changed while stopping at {}; preserving it",
                destination.display()
            )));
        }
        Ok(())
    };
    remove_projection_tree(destination, &authenticate).map_err(|error| match error {
        TreeRemovalError::Io(error) => NativeMountError::Driver(format!(
            "authenticated ProjFS root removal failed for {}: {error}",
            destination.display()
        )),
        TreeRemovalError::Root(error) => error,
    })
}

enum TreeRemovalError {
    Io(std::io::Error),
    Root(NativeMountError),
}

impl From<std::io::Error> for TreeRemovalError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// Removes `directory` and everything below it, visiting each entry once.
///
/// After `PrjStopVirtualizing`, `ProjFS` refuses the first delete of each
/// emptied placeholder directory as temporarily unavailable and admits the
/// next. A refused removal has already removed everything it visited before
/// the refused directory, so only what remains is visited again: each child
/// directory is removed the same way, and then the directory itself, retried
/// in place while the filter keeps refusing it within a bounded patience.
/// `before_removal` runs before every removal of `directory` itself.
fn remove_projection_tree(
    directory: &std::path::Path,
    before_removal: &dyn Fn() -> Result<(), NativeMountError>,
) -> Result<(), TreeRemovalError> {
    const PATIENCE: std::time::Duration = std::time::Duration::from_millis(250);
    let refused = |error: &std::io::Error| matches!(error.raw_os_error(), Some(145 | 369));
    before_removal().map_err(TreeRemovalError::Root)?;
    match std::fs::remove_dir_all(directory) {
        Ok(()) => return Ok(()),
        Err(error) if refused(&error) => {}
        Err(error) => return Err(error.into()),
    }
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            remove_projection_tree(&entry.path(), &|| Ok(()))?;
        }
    }
    let deadline = std::time::Instant::now() + PATIENCE;
    loop {
        before_removal().map_err(TreeRemovalError::Root)?;
        match std::fs::remove_dir_all(directory) {
            Ok(()) => return Ok(()),
            Err(error) if refused(&error) && std::time::Instant::now() < deadline => {
                // The filter admits a refused directory's next delete; only
                // a filter still draining needs a moment.
                std::thread::yield_now();
            }
            Err(error) => return Err(error.into()),
        }
    }
}

#[allow(unsafe_code)]
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

fn capture_missing_host_ancestors(
    source: &dyn MountFilesystem,
    root: &std::path::Path,
    path: &MountPath,
) -> Result<(), MountSourceError> {
    let mut parent = MountPath::root();
    for component in path
        .components()
        .iter()
        .take(path.components().len().saturating_sub(1))
    {
        parent = parent.child(component.clone());
        if source.lookup(&parent)?.is_none() {
            source.capture_host_path(root, &parent)?;
        }
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
    const WINDOWS_EPOCH_OFFSET_SECONDS: i128 = 11_644_473_600;
    const HUNDRED_NANOSECONDS_PER_SECOND: i128 = 10_000_000;
    i128::from(windows_ticks)
        .checked_sub(
            WINDOWS_EPOCH_OFFSET_SECONDS
                .checked_mul(HUNDRED_NANOSECONDS_PER_SECOND)
                .ok_or_else(|| {
                    MountSourceError::Invalid("Windows epoch conversion overflow".to_owned())
                })?,
        )
        .and_then(|ticks| ticks.checked_mul(100))
        .and_then(|nanoseconds| i64::try_from(nanoseconds).ok())
        .ok_or_else(|| {
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

fn windows_time(field: MetadataField<i64>) -> Option<i64> {
    const WINDOWS_EPOCH_OFFSET_SECONDS: i128 = 11_644_473_600;
    const HUNDRED_NANOSECONDS_PER_SECOND: i128 = 10_000_000;
    let MetadataField::Value(unix_nanoseconds) = field else {
        return None;
    };
    let ticks = WINDOWS_EPOCH_OFFSET_SECONDS
        .checked_mul(HUNDRED_NANOSECONDS_PER_SECOND)?
        .checked_add(i128::from(unix_nanoseconds).div_euclid(100))?;
    i64::try_from(ticks).ok()
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

/// The source's listing of `path`, as this provider writes it. An entry no
/// placeholder can represent (a socket, say) is left out, as is a name that
/// case-folds to one listed before it: Windows can hold only one of them.
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
        facts: PlaceholderFacts { lookup, pin },
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

unsafe fn file_data(
    callback_data: *const PRJ_CALLBACK_DATA,
    byte_offset: u64,
    length: u32,
) -> HRESULT {
    let Some((data, runtime)) = runtime(callback_data) else {
        return HR_UNEXPECTED;
    };
    let Some(path) = path_from(data.FilePathName) else {
        return HR_INVALID_DATA;
    };
    let pin = placeholder_pin(data);
    let chunk = length.min(HYDRATION_CHUNK_BYTES);
    let buffer = PrjAllocateAlignedBuffer(data.NamespaceVirtualizationContext, chunk as usize);
    if buffer.is_null() {
        return HR_OUT_OF_MEMORY;
    }
    // Each piece is written at its offset; the range is complete only when
    // the pieces cover it exactly.
    let written = std::cell::Cell::new(0_u64);
    let write_failure = std::cell::Cell::new(None);
    let mut write = |offset: u64, bytes: Bytes| -> Result<(), MountSourceError> {
        let fits = u32::try_from(bytes.len()).ok().filter(|count| {
            *count <= chunk && byte_offset.checked_add(written.get()) == Some(offset)
        });
        let Some(count) = fits else {
            write_failure.set(Some(HR_INVALID_DATA));
            return Err(MountSourceError::Invalid(
                "hydration piece out of order".to_owned(),
            ));
        };
        // SAFETY: ProjFS allocated `chunk >= count` aligned bytes and both
        // buffers are live and non-overlapping for the synchronous write.
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), buffer.cast::<u8>(), bytes.len());
            PrjWriteFileData(
                data.NamespaceVirtualizationContext,
                &raw const data.DataStreamId,
                buffer,
                offset,
                count,
            )
        }
        .map_err(|error| {
            write_failure.set(Some(error.code()));
            MountSourceError::Engine("PrjWriteFileData failed".to_owned())
        })?;
        written.set(written.get() + u64::from(count));
        Ok(())
    };
    // Immutable hydration reads take the source's own read gate. Keeping them
    // off the mutation callback queue lets concurrent compiler reads proceed.
    // A file is hydrated before its own rename or link, but `ProjFS` names a
    // placeholder by the path it was written at also after a directory
    // above it was renamed: that one is read where the renames took it.
    let renames = runtime
        .placeholders
        .renames
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let range = ContentRange {
        offset: byte_offset,
        length,
        chunk,
    };
    let mut read = read_content(runtime, &path, pin, range, &written, &mut write);
    let moved = runtime.placeholders.moved_path(&path);
    if written.get() == 0
        && moved != path
        && matches!(
            read,
            Err(MountSourceError::NotFound | MountSourceError::Stale)
        )
    {
        read = read_content(runtime, &moved, pin, range, &written, &mut write);
    }
    drop(renames);
    PrjFreeAlignedBuffer(buffer);
    match (read, write_failure.get()) {
        (_, Some(failure)) => failure,
        (Err(error), None) => source_hresult(&error),
        (Ok(()), None) if written.get() == u64::from(length) => HR_OK,
        (Ok(()), None) => HR_INVALID_DATA,
    }
}

/// The bytes one `GetFileData` asks for, in pieces of at most `chunk`.
#[derive(Clone, Copy)]
struct ContentRange {
    offset: u64,
    length: u32,
    chunk: u32,
}

/// Streams `range` of `path` into `write`, which counts what it took in
/// `written`: at the pinned version when the placeholder carries one, else
/// as the source holds it now.
fn read_content(
    runtime: &Runtime,
    path: &MountPath,
    pin: Option<MountContentPin>,
    ContentRange {
        offset: byte_offset,
        length,
        chunk,
    }: ContentRange,
    written: &std::cell::Cell<u64>,
    write: &mut ContentSink<'_>,
) -> Result<(), MountSourceError> {
    if let Some(pin) = pin {
        return runtime
            .source
            .read_pinned(path, pin, byte_offset, u64::from(length), chunk, write);
    }
    let mut result = Ok(());
    // A short read ends the content; the length check below rejects it.
    while let Some(remaining) = u64::from(length)
        .checked_sub(written.get())
        .filter(|remaining| *remaining > 0 && result.is_ok())
    {
        let before = written.get();
        let count = u32::try_from(remaining).unwrap_or(u32::MAX).min(chunk);
        let offset = byte_offset + before;
        result = runtime
            .source
            .read_range(path, offset, count)
            .and_then(|bytes| write(offset, bytes));
        if written.get() - before < u64::from(count) {
            break;
        }
    }
    result
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
    // A closed handle may have held a superseded placeholder open.
    if matches!(
        notification,
        PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION
            | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED
            | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED
    ) {
        runtime.placeholders.retry();
    }
    if unsafe { (*callback_data).TriggeringProcessId } == std::process::id()
        && matches!(
            notification,
            PRJ_NOTIFICATION_FILE_OPENED | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION
        )
        && MetadataProbeGuard::is_active(runtime.metadata_probes.as_ref(), &path)
    {
        return HR_OK;
    }
    let file_id = file_id(data);
    let source_is_external = unsafe { empty_destination(data.FilePathName) };
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
        if is_directory || source_is_external {
            return HR_OK;
        }
        return match hydrate_before_rebinding(&runtime.metadata_root, &path) {
            Ok(()) => HR_OK,
            Err(error) => source_hresult(&error),
        };
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
    let placeholders = Arc::clone(&runtime.placeholders);
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
                    let _edit = lock_recover(metadata_edits.as_ref());
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
            let _renaming = placeholders
                .renames
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let current = placeholders.current_beneath(source.as_ref(), &path);
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
                placeholders.renamed(&path, destination, carried);
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

/// Hydrates a still-virtual placeholder before it is renamed or linked.
///
/// `ProjFS` hydrates a placeholder by the path it was projected at, whatever
/// name later reads it through. After a rename, or after a hard link whose
/// original name is then deleted, that path no longer resolves to this
/// content. Reading the file now, while its projected path still does,
/// stores the content in the file itself for every later name.
fn hydrate_before_rebinding(root: &HostRoot, path: &MountPath) -> Result<(), MountSourceError> {
    let host_path = host_relative_path(path)?;
    let engine = |error: std::io::Error| MountSourceError::Engine(error.to_string());
    let attributes = {
        use cap_std::fs::MetadataExt as _;
        root.symlink_metadata_held(&host_path)
            .map_err(engine)?
            .file_attributes()
    };
    if attributes & FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS.0 == 0 {
        return Ok(());
    }
    // The provider's own reads raise no notifications; they are served by
    // GetFileData on another callback thread.
    let mut file = root.open_file(&host_path).map_err(engine)?;
    std::io::copy(&mut file, &mut std::io::sink()).map_err(engine)?;
    Ok(())
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
    ($callback:ident($($argument:ident: $type:ty),*)) => {{
        unsafe extern "system" fn entry($($argument: $type),*) -> HRESULT {
            // SAFETY: `ProjFS` passes the arguments its callback contract
            // promises, and they stay valid until this entry returns.
            provider_stack::run(|| unsafe { $callback($($argument),*) })
                .unwrap_or_else(|error| error.code())
        }
        entry
    }};
}

fn callbacks() -> PRJ_CALLBACKS {
    PRJ_CALLBACKS {
        StartDirectoryEnumerationCallback: Some(on_provider_stack!(start_directory(
            callback_data: *const PRJ_CALLBACK_DATA,
            enumeration_id: *const GUID
        ))),
        EndDirectoryEnumerationCallback: Some(on_provider_stack!(end_directory(
            callback_data: *const PRJ_CALLBACK_DATA,
            enumeration_id: *const GUID
        ))),
        GetDirectoryEnumerationCallback: Some(on_provider_stack!(get_directory(
            callback_data: *const PRJ_CALLBACK_DATA,
            enumeration_id: *const GUID,
            search_expression: PCWSTR,
            buffer: PRJ_DIR_ENTRY_BUFFER_HANDLE
        ))),
        GetPlaceholderInfoCallback: Some(on_provider_stack!(placeholder(
            callback_data: *const PRJ_CALLBACK_DATA
        ))),
        GetFileDataCallback: Some(on_provider_stack!(file_data(
            callback_data: *const PRJ_CALLBACK_DATA,
            byte_offset: u64,
            length: u32
        ))),
        QueryFileNameCallback: Some(on_provider_stack!(query_name(
            callback_data: *const PRJ_CALLBACK_DATA
        ))),
        NotificationCallback: Some(on_provider_stack!(notification(
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
#[allow(clippy::expect_used)]
mod tests {
    use super::{
        CONTENT_PIN_PROVIDER, CallbackGate, PostOperationFailures, ReadBasis, finish_cleanup,
        flush_callback_gate, placeholder_info, placeholder_pin, record_post_operation_failure,
        recover_cache_only_destination, remove_authenticated_destination, source_hresult,
    };
    use crate::kernel::FileMetadata;
    use crate::model::{
        AccessMode, CheckoutMode, ConsistencyMode, GenerationSelector, Lifecycle, MutationMode,
        VolumeConfig,
    };
    use crate::native_mount::adapter::{CheckoutMountSource, SharedCheckout};
    use crate::native_mount::{
        MountContentPin, MountFilesystem, MountLookup, MountNode, MountNodeKind, MountPath,
        MountSourceError,
    };
    use crate::{
        CancellationToken, Fs, IdempotencyKey, LocalOptions, MountOptions, MountPublication,
        WorkBudget,
    };
    use bytes::Bytes;
    use std::collections::HashSet;
    use std::sync::{Arc, Barrier, Mutex};
    use windows::Win32::Storage::ProjectedFileSystem::{
        PRJ_CALLBACK_DATA, PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED,
        PRJ_NOTIFICATION_PRE_DELETE, PRJ_VIRTUALIZATION_INSTANCE_INFO,
        PrjGetVirtualizationInstanceInfo, PrjMarkDirectoryAsPlaceholder, PrjStartVirtualizing,
        PrjStopVirtualizing,
    };
    use windows::core::{GUID, HSTRING, PCWSTR};

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
            stop.to_string().contains("removal failed"),
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
        let copied = std::process::Command::new("cmd.exe")
            .args(["/D", "/C", "copy /Y seed projection\\late.txt > nul"])
            .current_dir(root.path())
            .status()?;
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
        let mut editor = std::process::Command::new("powershell.exe")
            .args(["-NoProfile", "-NonInteractive", "-Command", script])
            .env("ACYCLIC_FS_TEST_FILE", &file)
            .env("ACYCLIC_FS_TEST_READY", &ready)
            .env("ACYCLIC_FS_TEST_GO", &go)
            .spawn()?;
        while !ready.exists() {
            assert!(editor.try_wait()?.is_none(), "editor exited early");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        session.flush_callbacks()?;
        std::fs::write(&go, b"")?;
        assert!(editor.wait()?.success(), "metadata edit failed");
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
        let output = std::process::Command::new("powershell.exe")
            .args(["-NoProfile", "-NonInteractive", "-Command", script])
            .env("ACYCLIC_FS_TEST_ROOT", &destination)
            .output()?;
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
            let output = std::process::Command::new("cmd.exe")
                .args(["/D", "/C", command])
                .current_dir(&destination)
                .output()?;
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
        let external = std::process::Command::new("cmd.exe")
            .args(["/D", "/C", "echo c> c.txt && del b.txt"])
            .current_dir(&destination)
            .output()?;
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
        assert!(std::fs::symlink_metadata(&projected)?.is_dir());
        assert_eq!(std::fs::read(projected.join("inner.txt"))?, b"inner");
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

    /// Revalidation waits while a superseded placeholder is pending, which
    /// it is until its outcome is recorded, even once every change has been
    /// served, and returns as soon as it is settled.
    #[test]
    fn renames_compose_into_where_each_written_path_is_now() {
        use super::{Carried, Placeholders, lock_recover};

        let path = |parts: &[&str]| {
            parts.iter().fold(MountPath::root(), |path, part| {
                path.child(windows_name(part))
            })
        };
        let placeholders = Placeholders::new();
        placeholders.renamed(&path(&["a"]), &path(&["b"]), Carried::default());
        placeholders.renamed(&path(&["b", "child"]), &path(&["c"]), Carried::default());
        // Written before either rename, or between them.
        assert_eq!(
            placeholders.moved_path(&path(&["a", "child", "file"])),
            path(&["c", "file"])
        );
        assert_eq!(
            placeholders.moved_path(&path(&["b", "child", "file"])),
            path(&["c", "file"])
        );
        assert_eq!(
            placeholders.moved_path(&path(&["a", "other"])),
            path(&["b", "other"])
        );
        // Moving a directory back and forth keeps one entry, for what was
        // written while it was away.
        let placeholders = Placeholders::new();
        for _ in 0..3 {
            placeholders.renamed(&path(&["x"]), &path(&["y"]), Carried::default());
            placeholders.renamed(&path(&["y"]), &path(&["x"]), Carried::default());
        }
        assert_eq!(lock_recover(&placeholders.state).moves.len(), 1);
        assert_eq!(
            placeholders.moved_path(&path(&["y", "file"])),
            path(&["x", "file"])
        );
    }

    #[test]
    fn settling_waits_for_every_pending_placeholder() -> Result<(), Box<dyn std::error::Error>> {
        use super::{Placeholders, WrittenPlaceholder};
        use crate::FileId;
        use crate::native_mount::ViewStamp;

        let placeholders = Arc::new(Placeholders::new());
        let path = MountPath::root().child(b"held".to_vec());
        {
            let mut state = super::lock_recover(&placeholders.state);
            state.pending.insert(
                path.clone(),
                WrittenPlaceholder {
                    file_id: FileId::new(),
                    link: None,
                    facts: None,
                    basis: Some(ReadBasis {
                        stamp: ViewStamp::current(),
                        binding: 0,
                    }),
                },
            );
        }
        let settled = std::thread::spawn({
            let placeholders = Arc::clone(&placeholders);
            move || placeholders.settle()
        });
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert!(
            !settled.is_finished(),
            "settled while a placeholder is pending"
        );
        super::lock_recover(&placeholders.state)
            .pending
            .remove(&path);
        placeholders.changed.notify_all();
        // Settling ends once nothing is pending.
        settled
            .join()
            .map_err(|_| "settle panicked")?
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    /// A superseded placeholder another handle holds open is replaced once
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

    /// A read-only source file's placeholder is read-only too; a change to
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
    async fn placeholder_hydrates_only_the_source_content_it_promised()
    -> Result<(), Box<dyn std::error::Error>> {
        // A placeholder is written from a lookup, or from the listing that
        // projected its name; either promises exactly the listed content.
        for listed_first in [false, true] {
            hydrates_only_promised_content(listed_first).await?;
        }
        Ok(())
    }

    async fn hydrates_only_promised_content(
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
        // Stat writes both placeholders without hydrating either.
        assert_eq!(std::fs::metadata(destination.join("changed.txt"))?.len(), 6);
        assert_eq!(std::fs::metadata(destination.join("stable.txt"))?.len(), 6);

        // Same length, new version: only the pin can tell the bytes apart.
        let mut changed = std::fs::OpenOptions::new()
            .write(true)
            .open(source_root.join("changed.txt"))?;
        changed.write_all(b"after!")?;
        changed.set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1 << 30))?;
        drop(changed);

        // The placeholder either still promises the old version, whose
        // hydration fails, or was already written again from the new one.
        match std::fs::read(destination.join("changed.txt")) {
            Err(_) => {}
            Ok(read) => assert_eq!(
                read, b"after!",
                "a placeholder hydrated content it never promised"
            ),
        }
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
        assert_eq!(
            sorted_names(&destination.join("pkg"))?,
            ["shared.rs", "sub"]
        );
        // ProjFS reports only other processes' I/O to its provider.
        let moved = std::process::Command::new("cmd.exe")
            .args(["/D", "/C", "move pkg lib"])
            .current_dir(&destination)
            .output()?;
        assert!(moved.status.success(), "move failed: {moved:?}");
        assert_eq!(sorted_names(&destination)?, ["lib"]);
        assert_eq!(
            std::fs::read(destination.join("lib").join("shared.rs"))?,
            b"shared"
        );
        assert_eq!(
            std::fs::read(destination.join("lib").join("sub").join("deep.rs"))?,
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
    fn placeholder_version_carries_exactly_the_issued_pin() {
        let lookup = MountLookup {
            node: MountNode {
                file_id: crate::FileId::new(),
                kind: MountNodeKind::Regular,
                logical_bytes: 6,
                link_count: 1,
                device: None,
            },
            metadata: FileMetadata::default(),
        };
        let pin = MountContentPin([7; 32]);
        let mut callback = PRJ_CALLBACK_DATA::default();
        assert_eq!(placeholder_pin(&callback), None);

        let mut pinned = placeholder_info(&lookup, Some(pin))
            .expect("regular file placeholder")
            .VersionInfo;
        callback.VersionInfo = &raw mut pinned;
        assert_eq!(placeholder_pin(&callback), Some(pin));

        let mut unpinned = placeholder_info(&lookup, None)
            .expect("regular file placeholder")
            .VersionInfo;
        callback.VersionInfo = &raw mut unpinned;
        assert_eq!(placeholder_pin(&callback), None);
        // Every write is a new version, so `ProjFS` never skips a rewrite.
        let again = placeholder_info(&lookup, None)
            .expect("regular file placeholder")
            .VersionInfo;
        assert_ne!(again.ContentID, unpinned.ContentID);

        // Another provider's version information is never read as a pin.
        let mut foreign = pinned;
        foreign.ProviderID[CONTENT_PIN_PROVIDER.len()] = 1;
        callback.VersionInfo = &raw mut foreign;
        assert_eq!(placeholder_pin(&callback), None);
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
                let restarted = unsafe {
                    PrjStartVirtualizing(
                        PCWSTR::from_raw(wide.as_ptr()),
                        &raw const callbacks,
                        None,
                        None,
                    )
                }?;
                let mut instance = PRJ_VIRTUALIZATION_INSTANCE_INFO::default();
                unsafe { PrjGetVirtualizationInstanceInfo(restarted, &raw mut instance) }?;
                assert_eq!(instance.InstanceID, guid);
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
            std::process::Command::new("rustc")
                .current_dir(compiler_path)
                .args([
                    "--edition",
                    "2021",
                    "acyclic-workflow.rs",
                    "-o",
                    "acyclic-workflow-bin.exe",
                ])
                .output()
        })
        .await??;
        assert!(
            output.status.success(),
            "rustc failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        mount.sync().await?;
        let executable = path.join("acyclic-workflow-bin.exe");
        let execution =
            tokio::task::spawn_blocking(move || std::process::Command::new(executable).output())
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
            std::process::Command::new("attrib")
                .arg("+R")
                .arg(projected)
                .status()
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
        let external = std::process::Command::new("cmd.exe")
            .args(["/D", "/C", "ren before.txt after.txt"])
            .current_dir(&destination)
            .output()?;
        assert!(external.status.success(), "rename failed: {external:?}");
        mount.sync().await?;
        // The rename hydrated the file and moved it; nothing writes it again,
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
        let external = std::process::Command::new("cmd.exe")
            .args([
                "/D",
                "/C",
                "ren original.txt renamed.txt && echo replacement> original.txt",
            ])
            .current_dir(&first)
            .output()?;
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

        let external = std::process::Command::new("cmd.exe")
            .arg("/D")
            .arg("/C")
            .arg("ren original.txt renamed.txt && echo replacement> original.txt")
            .current_dir(&destination)
            .output()?;
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
        let error = recover_cache_only_destination(root.path()).expect_err("preserve stale root");
        assert!(error.to_string().contains("preserved for recovery"));
        assert!(root.path().exists());
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
        assert!(rejected.to_string().contains("identity changed"));
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
                .extend(paths.iter().map(|(path, _)| path.clone()));
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
                .extend(paths.iter().cloned());
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

    #[test]
    fn stopped_runtime_is_retained_until_cleanup_succeeds() {
        let mut state = Some("callback-runtime");
        let first = finish_cleanup(&mut state, |_| Err::<(), _>("transient cleanup failure"));
        assert_eq!(first, Err("transient cleanup failure"));
        assert_eq!(state, Some("callback-runtime"));

        assert_eq!(finish_cleanup(&mut state, |_| Ok::<(), &str>(())), Ok(()));
        assert_eq!(state, None);
    }
}
