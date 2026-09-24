//! Audited Windows `ProjFS` FFI with exact close-boundary authored capture.
//!
//! `ProjFS` does not expose write byte ranges, so modified and newly created
//! files are streamed from final host state only after the corresponding file
//! handle closes. Rename and hard-link notifications preserve stable identity,
//! and every acknowledged authored notification seals the checkout generation.

#![allow(unsafe_code, unsafe_op_in_unsafe_fn)]

use super::{
    DriverStartFailure, MountContentPin, MountFilesystem, MountLookup, MountNode, MountNodeKind,
    MountPath, MountSourceError, NativeMountError, NativeMountRequest,
};
use crate::kernel::{FileMetadata, MetadataField};
use crate::native_host::HostRoot;
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
    PRJ_CALLBACK_DATA, PRJ_CALLBACKS, PRJ_CB_DATA_FLAG_ENUM_RESTART_SCAN,
    PRJ_DIR_ENTRY_BUFFER_HANDLE, PRJ_EXT_INFO_TYPE_SYMLINK, PRJ_EXTENDED_INFO, PRJ_EXTENDED_INFO_0,
    PRJ_EXTENDED_INFO_0_0, PRJ_FILE_BASIC_INFO, PRJ_FLAG_USE_NEGATIVE_PATH_CACHE,
    PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT, PRJ_NOTIFICATION,
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
    PRJ_PLACEHOLDER_INFO, PRJ_PLACEHOLDER_VERSION_INFO, PRJ_STARTVIRTUALIZING_OPTIONS,
    PRJ_UPDATE_ALLOW_DIRTY_DATA, PRJ_UPDATE_ALLOW_DIRTY_METADATA, PRJ_UPDATE_ALLOW_READ_ONLY,
    PRJ_UPDATE_ALLOW_TOMBSTONE, PrjAllocateAlignedBuffer, PrjClearNegativePathCache, PrjDeleteFile,
    PrjFileNameCompare, PrjFileNameMatch, PrjFillDirEntryBuffer, PrjFillDirEntryBuffer2,
    PrjFreeAlignedBuffer, PrjMarkDirectoryAsPlaceholder, PrjStartVirtualizing, PrjStopVirtualizing,
    PrjUpdateFileIfNeeded, PrjWriteFileData, PrjWritePlaceholderInfo, PrjWritePlaceholderInfo2,
};
use windows::core::{GUID, HRESULT, HSTRING, PCWSTR};

const HR_OK: HRESULT = HRESULT(0);
const HR_FILE_NOT_FOUND: HRESULT = HRESULT(0x8007_0002_u32.cast_signed());
const HR_PATH_NOT_FOUND: HRESULT = HRESULT(0x8007_0003_u32.cast_signed());
const HR_ALREADY_EXISTS: HRESULT = HRESULT(0x8007_00b7_u32.cast_signed());
const HR_NOT_SAME_DEVICE: HRESULT = HRESULT(0x8007_0011_u32.cast_signed());
const HR_INVALID_DATA: HRESULT = HRESULT(0x8007_000d_u32.cast_signed());
const HR_NOT_SUPPORTED: HRESULT = HRESULT(0x8007_0032_u32.cast_signed());
const HR_OUT_OF_MEMORY: HRESULT = HRESULT(0x8007_000e_u32.cast_signed());
const HR_UNEXPECTED: HRESULT = HRESULT(0x8000_ffff_u32.cast_signed());
const HR_INSUFFICIENT_BUFFER: HRESULT = HRESULT(0x8007_007a_u32.cast_signed());
const HR_FILE_INVALID: HRESULT = HRESULT(0x8007_03ee_u32.cast_signed());
const HR_IO_DEVICE: HRESULT = HRESULT(0x8007_045d_u32.cast_signed());
const HR_VIRTUALIZATION_INVALID_OPERATION: HRESULT = HRESULT(0x8007_0181_u32.cast_signed());

const DIRECTORY_PAGE_SIZE: u32 = 256;
const NOTIFICATION_ROOT: [u16; 1] = [0];
/// Largest hydration unit. A multiple of every sector size, so each chunk
/// but the last keeps `PrjWriteFileData` aligned; bounds peak memory.
const HYDRATION_CHUNK_BYTES: u32 = 1 << 20;
/// Bounds the provider's memo of read-only close bindings.
const MAXIMUM_CACHED_BINDINGS: usize = 16_384;
/// Bounds the directory entries memoized across all cached directories.
const MAXIMUM_CACHED_DIRECTORY_ENTRIES: usize = 65_536;
/// Marks a placeholder whose `ContentID` carries a [`MountContentPin`].
const CONTENT_PIN_PROVIDER: &[u8; 16] = b"acyclic-fs-pin-1";

struct EnumState {
    path: MountPath,
    ended: bool,
    snapshot: Option<Arc<[ProjectedEntry]>>,
    next: usize,
    search_expression: Option<HSTRING>,
    search_expression_captured: bool,
}

struct ProjectedEntry {
    // ProjFS compares NUL-terminated UTF-16 names, not SDK byte cursors.
    name: Vec<u16>,
    info: PRJ_FILE_BASIC_INFO,
    symlink_target: Option<bytes::Bytes>,
}

#[derive(Clone)]
struct RenamedHydrationFile {
    file: Arc<dyn super::MountOpenFile>,
    destination: MountPath,
}

struct Runtime {
    source: Arc<dyn MountFilesystem>,
    root: PathBuf,
    metadata_root: Arc<HostRoot>,
    writable: bool,
    enumerations: Mutex<HashMap<u128, Arc<Mutex<EnumState>>>>,
    renamed_hydration_files: Arc<Mutex<HashMap<u128, RenamedHydrationFile>>>,
    renamed_paths: Arc<Mutex<HashMap<u128, Option<MountPath>>>>,
    metadata_baselines: Arc<Mutex<HashMap<u128, OpenMetadataState>>>,
    projection: Arc<Mutex<ProjectionCache>>,
    /// Source epochs under which every absence `ProjFS` holds in its negative
    /// path cache was reported; `None` once that cache must be cleared.
    negative_paths: Option<Mutex<Option<SourceEpochs>>>,
    metadata_probes: Arc<Mutex<HashMap<MountPath, usize>>>,
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

/// One coherent source view: stable, with both epochs known.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SourceEpochs {
    view: u64,
    binding: u64,
}

fn source_epochs(source: &dyn MountFilesystem) -> Option<SourceEpochs> {
    if !source.view_is_stable() {
        return None;
    }
    Some(SourceEpochs {
        view: source.view_epoch()?,
        binding: source.binding_epoch()?,
    })
}

/// Source facts the provider memoizes between callbacks.
///
/// Each fact is valid only in the source view it was read in, and only
/// until the provider itself next mutates the source: every such mutation
/// completes before [`Self::invalidate`] advances `generation`. A reader
/// samples `generation` before consulting the source and may remember its
/// result only while the generation is unchanged, so nothing read before a
/// concurrent provider mutation finished is ever retained.
struct ProjectionCache {
    /// `None` once the counter is exhausted; nothing is cached after that.
    generation: Option<u64>,
    bindings: HashMap<MountPath, ReadOnlyBinding>,
    /// The single view every entry of `directories` was enumerated in.
    directories_epochs: Option<SourceEpochs>,
    directories: HashMap<MountPath, Arc<[ProjectedEntry]>>,
    directory_entries: usize,
}

impl Default for ProjectionCache {
    fn default() -> Self {
        Self {
            generation: Some(0),
            bindings: HashMap::new(),
            directories_epochs: None,
            directories: HashMap::new(),
            directory_entries: 0,
        }
    }
}

impl ProjectionCache {
    fn invalidate(&mut self) {
        self.generation = self.generation.and_then(|value| value.checked_add(1));
        self.bindings.clear();
        self.clear_directories();
    }

    fn clear_directories(&mut self) {
        self.directories_epochs = None;
        self.directories.clear();
        self.directory_entries = 0;
    }

    fn directory(&self, path: &MountPath, epochs: SourceEpochs) -> Option<Arc<[ProjectedEntry]>> {
        (self.directories_epochs == Some(epochs))
            .then(|| self.directories.get(path).cloned())
            .flatten()
    }

    fn remember_directory(
        &mut self,
        generation: Option<u64>,
        epochs: SourceEpochs,
        path: MountPath,
        entries: Arc<[ProjectedEntry]>,
    ) {
        if generation.is_none()
            || self.generation != generation
            || entries.len() > MAXIMUM_CACHED_DIRECTORY_ENTRIES
        {
            return;
        }
        if self.directories_epochs != Some(epochs)
            || self.directory_entries + entries.len() > MAXIMUM_CACHED_DIRECTORY_ENTRIES
        {
            self.clear_directories();
            self.directories_epochs = Some(epochs);
        }
        self.directory_entries += entries.len();
        if let Some(replaced) = self.directories.insert(path, entries) {
            self.directory_entries -= replaced.len();
        }
    }

    fn remember_binding(
        &mut self,
        generation: Option<u64>,
        path: MountPath,
        binding: ReadOnlyBinding,
    ) {
        if generation.is_none() || self.generation != generation {
            return;
        }
        if self.bindings.len() >= MAXIMUM_CACHED_BINDINGS {
            self.bindings.clear();
        }
        self.bindings.insert(path, binding);
    }
}

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

        // An absence can be cached only by a source that versions its view;
        // any later change of that view clears the cache before new lookups.
        let negative_paths = source.view_epoch().map(|_| Mutex::new(None));
        let mut runtime = Box::new(Runtime {
            source,
            root: request.destination.clone(),
            metadata_root,
            writable: request.writable,
            enumerations: Mutex::new(HashMap::new()),
            renamed_hydration_files: Arc::new(Mutex::new(HashMap::new())),
            renamed_paths: Arc::new(Mutex::new(HashMap::new())),
            metadata_baselines: Arc::new(Mutex::new(HashMap::new())),
            projection: Arc::new(Mutex::new(ProjectionCache::default())),
            negative_paths,
            metadata_probes: Arc::new(Mutex::new(HashMap::new())),
            post_operation_failure: Arc::new(Mutex::new(PostOperationFailures::default())),
            callbacks: callback_gate,
        });
        let context_ptr = (&raw mut *runtime).cast::<c_void>();
        let callbacks = callbacks();
        // Only pre-operation callbacks that can veto are subscribed: a delete
        // is never refused, and it is captured when its handle closes.
        let mut notification_mapping = PRJ_NOTIFICATION_MAPPING {
            NotificationBitMask: PRJ_NOTIFY_NEW_FILE_CREATED
                | PRJ_NOTIFY_FILE_OVERWRITTEN
                | PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_MODIFIED
                | PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_DELETED
                | PRJ_NOTIFY_FILE_HANDLE_CLOSED_NO_MODIFICATION
                | PRJ_NOTIFY_FILE_OPENED
                | PRJ_NOTIFY_PRE_RENAME
                | PRJ_NOTIFY_FILE_RENAMED
                | PRJ_NOTIFY_PRE_SET_HARDLINK
                | PRJ_NOTIFY_HARDLINK_CREATED,
            // ProjFS requires a pointer to an empty UTF-16 string for the
            // virtualization root. An empty HSTRING may be represented by a
            // null handle, which is not the same contract as `L""`.
            NotificationRoot: PCWSTR::from_raw(NOTIFICATION_ROOT.as_ptr()),
        };
        let mut options = PRJ_STARTVIRTUALIZING_OPTIONS::default();
        if runtime.negative_paths.is_some() {
            options.Flags = PRJ_FLAG_USE_NEGATIVE_PATH_CACHE;
        }
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
                let cleanup = remove_authenticated_destination(&request.destination, root_identity)
                    .and_then(|()| {
                        std::fs::create_dir(&request.destination)
                            .map_err(|error| NativeMountError::Driver(error.to_string()))
                    });
                if let Err(cleanup) = cleanup {
                    return Err(DriverStartFailure::preserving_destination_fence(
                        NativeMountError::Driver(format!(
                            "{start_error}; ProjFS startup rollback failed: {cleanup}"
                        )),
                    ));
                }
                return Err(start_error.into());
            }
        };
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
        let projection = Arc::clone(&runtime.projection);
        let context = self.context;
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
                // Capture mutates the source whether or not it completes.
                let _invalidate = InvalidateOnDrop(projection.as_ref());
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
                    match host_root.symlink_metadata_held(&host_path) {
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
                source.capture_host_paths(&root, &exact)?;
                if let Some(context) = context {
                    for (path, _) in paths {
                        normalize_cache_path(context, source.as_ref(), path)?;
                    }
                }
                Ok(())
            },
        )
    }

    /// Makes a source-side change at one mount-relative path (leading `/`
    /// optional) visible now: the provider forgets memoized source facts,
    /// `ProjFS` forgets cached absences, and an unmodified projected
    /// placeholder at `path` is dropped so its next access re-reads the
    /// source. Locally modified state at `path` is authored and stays.
    pub(super) fn invalidate(&self, path: &[u8]) -> Result<(), NativeMountError> {
        let (runtime, context) = self.live()?;
        let text = std::str::from_utf8(path)
            .map_err(|_| NativeMountError::Driver("invalidation path is not UTF-8".to_owned()))?;
        let relative = text
            .split('/')
            .filter(|component| !component.is_empty())
            .collect::<PathBuf>();
        lock_recover(runtime.projection.as_ref()).invalidate();
        forget_negative_paths(runtime, context)?;
        if relative.as_os_str().is_empty() {
            return Ok(());
        }
        let relative = HSTRING::from(relative.as_os_str());
        // SAFETY: the relative UTF-16 name remains live for this synchronous
        // call and the context belongs to this mounted runtime.
        match unsafe { PrjDeleteFile(context, &relative, None, None) } {
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
            _ => Ok(()),
        }
    }

    /// Forgets `ProjFS`'s cached absences after the source view advanced,
    /// so paths the new view adds become visible.
    pub(super) fn source_view_changed(&self) -> Result<(), NativeMountError> {
        let (runtime, context) = self.live()?;
        forget_negative_paths(runtime, context)
    }

    fn live(&self) -> Result<(&Runtime, PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT), NativeMountError> {
        self.runtime
            .as_deref()
            .zip(self.context)
            .ok_or_else(|| NativeMountError::Driver("ProjFS session is stopped".to_owned()))
    }
}

/// Clears `ProjFS`'s negative path cache and records that no absence is
/// cached, so the next placeholder callback re-establishes the view.
fn forget_negative_paths(
    runtime: &Runtime,
    context: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT,
) -> Result<(), NativeMountError> {
    let Some(negative_paths) = runtime.negative_paths.as_ref() else {
        return Ok(());
    };
    let mut view = lock_recover(negative_paths);
    // SAFETY: the context belongs to this live mounted runtime.
    unsafe { PrjClearNegativePathCache(context, None) }.map_err(|error| driver_error(&error))?;
    *view = None;
    Ok(())
}

/// How one placeholder lookup may report an absence.
enum AbsenceReport {
    /// `ProjFS` caches no absences for this source.
    Uncached,
    /// Cacheable only if the source is still in this view after the lookup.
    CachedIn(Option<SourceEpochs>),
}

impl AbsenceReport {
    /// `ProjFS` caches `FILE_NOT_FOUND` as a negative path. An absence seen
    /// while the view was moving belongs to no current view, so it is
    /// reported as the equally absent but uncached `PATH_NOT_FOUND`.
    fn report(&self, epochs_after: Option<SourceEpochs>) -> HRESULT {
        match self {
            Self::CachedIn(epochs) if epochs.is_none() || *epochs != epochs_after => {
                HR_PATH_NOT_FOUND
            }
            Self::Uncached | Self::CachedIn(_) => HR_FILE_NOT_FOUND,
        }
    }
}

/// Admits the current source view for new cached absences, first clearing
/// every absence `ProjFS` cached under an earlier view.
fn admit_negative_paths(
    runtime: &Runtime,
    context: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT,
) -> Result<AbsenceReport, HRESULT> {
    let Some(negative_paths) = runtime.negative_paths.as_ref() else {
        return Ok(AbsenceReport::Uncached);
    };
    let current = source_epochs(runtime.source.as_ref());
    let mut cached = lock_recover(negative_paths);
    if *cached != current || current.is_none() {
        // SAFETY: the context belongs to this live mounted runtime.
        unsafe { PrjClearNegativePathCache(context, None) }.map_err(|error| error.code())?;
        *cached = current;
    }
    Ok(AbsenceReport::CachedIn(current))
}

/// Invalidates the provider's memo when a source mutation ends, however it
/// ends.
struct InvalidateOnDrop<'a>(&'a Mutex<ProjectionCache>);

impl Drop for InvalidateOnDrop<'_> {
    fn drop(&mut self) {
        lock_recover(self.0).invalidate();
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
    let mut version = PRJ_PLACEHOLDER_VERSION_INFO::default();
    if let Some(pin) = pin {
        *version.ProviderID.first_chunk_mut()? = *CONTENT_PIN_PROVIDER;
        *version.ContentID.first_chunk_mut()? = pin.0;
    }
    Some(PRJ_PLACEHOLDER_INFO {
        FileBasicInfo: basic(lookup.node, Some(lookup.metadata))?,
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

fn normalize_cache_path(
    context: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT,
    source: &dyn MountFilesystem,
    path: &MountPath,
) -> Result<(), MountSourceError> {
    let relative = host_relative_path(path)?;
    let relative = HSTRING::from(relative.as_os_str());
    let update_flags = PRJ_UPDATE_ALLOW_DIRTY_METADATA
        | PRJ_UPDATE_ALLOW_DIRTY_DATA
        | PRJ_UPDATE_ALLOW_TOMBSTONE
        | PRJ_UPDATE_ALLOW_READ_ONLY;
    let Some((node, pin)) = source.lookup_pinned(path)? else {
        // SAFETY: the relative UTF-16 name remains live for this synchronous
        // call and the context belongs to this mounted runtime.
        let result = unsafe {
            PrjDeleteFile(
                context,
                PCWSTR::from_raw(relative.as_ptr()),
                Some(update_flags),
                None,
            )
        };
        return match result {
            Ok(()) => Ok(()),
            Err(error)
                if error.code() == HR_FILE_NOT_FOUND || error.code() == HR_PATH_NOT_FOUND =>
            {
                Ok(())
            }
            Err(error) => Err(MountSourceError::Engine(driver_error(&error).to_string())),
        };
    };
    if matches!(
        node.node.kind,
        MountNodeKind::Directory | MountNodeKind::SymbolicLink
    ) {
        // ProjFS cannot normalize a non-empty directory, and its update API
        // cannot carry extended symlink information. Their exact state is
        // already captured; retaining the cache entry is harmless.
        return Ok(());
    }
    let placeholder = placeholder_info(&node, pin).ok_or_else(|| {
        MountSourceError::Unsupported("node cannot be represented by ProjFS".to_owned())
    })?;
    // SAFETY: every pointer refers to stack/owned data that remains live for
    // this synchronous call and the context belongs to this mounted runtime.
    let result = unsafe {
        PrjUpdateFileIfNeeded(
            context,
            PCWSTR::from_raw(relative.as_ptr()),
            &raw const placeholder,
            u32::try_from(size_of::<PRJ_PLACEHOLDER_INFO>()).unwrap_or(u32::MAX),
            Some(update_flags),
            None,
        )
    };
    match result {
        Ok(()) => Ok(()),
        Err(error) if error.code() == HR_FILE_NOT_FOUND || error.code() == HR_PATH_NOT_FOUND => {
            Ok(())
        }
        Err(error) => Err(MountSourceError::Engine(driver_error(&error).to_string())),
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
    // After PrjStopVirtualizing, ProjFS rejects the first delete of each
    // emptied placeholder directory as temporarily unavailable and admits
    // the next, so one removal pass clears at most one such directory.
    // Retry for as long as passes make progress; give up only after the
    // filter stops admitting deletes altogether.
    let patience = std::time::Duration::from_millis(250);
    let mut deadline = std::time::Instant::now() + patience;
    let mut remaining = usize::MAX;
    loop {
        // Every attempt must reauthenticate the directory, because the path
        // can be replaced while the filter is draining.
        let (current_tag, current_identity) = reparse_tag(destination)?;
        if current_tag != Some(windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS)
            || current_identity != expected_identity
        {
            return Err(NativeMountError::Driver(format!(
                "ProjFS root changed while stopping at {}; preserving it",
                destination.display()
            )));
        }
        match std::fs::remove_dir_all(destination) {
            Ok(()) => return Ok(()),
            Err(error) if matches!(error.raw_os_error(), Some(145 | 369)) => {
                let now = std::time::Instant::now();
                let left = descendant_count(destination);
                if left < remaining {
                    remaining = left;
                    deadline = now + patience;
                } else if now >= deadline {
                    return Err(NativeMountError::Driver(format!(
                        "authenticated ProjFS root removal failed for {}: {error}",
                        destination.display()
                    )));
                }
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            Err(error) => {
                return Err(NativeMountError::Driver(format!(
                    "authenticated ProjFS root removal failed for {}: {error}",
                    destination.display()
                )));
            }
        }
    }
}

/// Counts every entry below `directory` without following links; an
/// unreadable subtree counts as unbounded, so it never looks like progress.
fn descendant_count(directory: &std::path::Path) -> usize {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return usize::MAX;
    };
    entries.fold(0_usize, |count, entry| {
        let nested = match entry.and_then(|entry| Ok((entry.file_type()?, entry.path()))) {
            Ok((kind, path)) if kind.is_dir() => descendant_count(&path),
            Ok(_) => 0,
            Err(_) => usize::MAX,
        };
        count.saturating_add(1).saturating_add(nested)
    })
}

#[allow(unsafe_code)]
fn reparse_tag(
    path: &std::path::Path,
) -> Result<(Option<u32>, crate::NativeRootIdentity), NativeMountError> {
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use windows::Win32::Foundation::{
        ERROR_FILE_SYSTEM_VIRTUALIZATION_UNAVAILABLE, ERROR_NOT_A_REPARSE_POINT, HANDLE,
    };
    use windows::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_DELETE,
        FILE_SHARE_READ, FILE_SHARE_WRITE, MAXIMUM_REPARSE_DATA_BUFFER_SIZE,
    };
    use windows::Win32::System::IO::DeviceIoControl;
    use windows::Win32::System::Ioctl::FSCTL_GET_REPARSE_POINT;

    let file = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS.0 | FILE_FLAG_OPEN_REPARSE_POINT.0)
        .open(path)
        .map_err(|error| NativeMountError::Driver(error.to_string()))?;
    let identity = crate::NativeRootIdentity::from_file(&file)
        .map_err(|error| NativeMountError::Driver(error.to_string()))?;
    let output_len = usize::try_from(MAXIMUM_REPARSE_DATA_BUFFER_SIZE)
        .map_err(|_| NativeMountError::Driver("reparse buffer size overflow".to_owned()))?;
    let mut output = vec![0_u8; output_len];
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
    if let Err(error) = result {
        let code = error.code().0.cast_unsigned();
        if code == 0x8007_0000_u32 | ERROR_NOT_A_REPARSE_POINT.0 {
            return Ok((None, identity));
        }
        // A stopped ProjFS provider can leave an authenticated placeholder
        // root even when the filter declines to return its reparse payload.
        if code == 0x8007_0000_u32 | ERROR_FILE_SYSTEM_VIRTUALIZATION_UNAVAILABLE.0 {
            return Ok((
                Some(windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS),
                identity,
            ));
        }
        return Err(NativeMountError::Driver(error.to_string()));
    }
    if returned < 4 {
        return Err(NativeMountError::Driver(
            "reparse response omitted its tag".to_owned(),
        ));
    }
    let tag = output
        .get(..4)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| NativeMountError::Driver("reparse response omitted its tag".to_owned()))?;
    Ok((Some(u32::from_le_bytes(tag)), identity))
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
    if !bytes.len().is_multiple_of(2) {
        return None;
    }
    bytes
        .chunks_exact(2)
        .map(|unit| unit.try_into().ok().map(u16::from_le_bytes))
        .collect()
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
    epochs: SourceEpochs,
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

    let host_path = host_relative_path(path)?;
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
    let hydration_mask = FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS.0 | FILE_ATTRIBUTE_ARCHIVE.0;
    let attributes_changed = if baseline.attributes & FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS.0 != 0 {
        baseline.attributes & !hydration_mask != current.attributes & !hydration_mask
    } else {
        baseline.attributes != current.attributes
    };
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
    let hydration_mask = FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS.0 | FILE_ATTRIBUTE_ARCHIVE.0;
    let attributes_changed = if baseline.attributes & FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS.0 != 0 {
        baseline.attributes & !hydration_mask != current.attributes & !hydration_mask
    } else {
        baseline.attributes != current.attributes
    };
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

fn enum_id(pointer: *const GUID) -> Option<u128> {
    // SAFETY: callback ABI supplies a GUID pointer for the callback duration.
    unsafe { pointer.as_ref() }.map(guid_key)
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

unsafe extern "system" fn start_directory(
    callback_data: *const PRJ_CALLBACK_DATA,
    enumeration_id: *const GUID,
) -> HRESULT {
    let Some((data, runtime)) = runtime(callback_data) else {
        return HR_UNEXPECTED;
    };
    let Some(path) = path_from(data.FilePathName) else {
        return HR_INVALID_DATA;
    };
    let Some(id) = enum_id(enumeration_id) else {
        return HR_INVALID_DATA;
    };
    // A listing of a newer view must not coexist with absences cached under
    // an older one, or a listed name could fail to open.
    if let Err(error) = admit_negative_paths(runtime, data.NamespaceVirtualizationContext) {
        return error;
    }
    lock_recover(&runtime.enumerations).insert(
        id,
        Arc::new(Mutex::new(EnumState {
            path,
            ended: false,
            snapshot: None,
            next: 0,
            search_expression: None,
            search_expression_captured: false,
        })),
    );
    HR_OK
}

/// Returns one directory's projected entries, sharing a snapshot across
/// enumerations for as long as the source view it was read in is current.
fn directory_entries(
    runtime: &Runtime,
    path: &MountPath,
) -> Result<Arc<[ProjectedEntry]>, HRESULT> {
    let source = runtime.source.as_ref();
    let epochs = source_epochs(source);
    let generation = {
        let projection = lock_recover(runtime.projection.as_ref());
        if let Some(entries) = epochs.and_then(|epochs| projection.directory(path, epochs)) {
            return Ok(entries);
        }
        projection.generation
    };
    let entries = Arc::<[ProjectedEntry]>::from(directory_snapshot(source, path)?);
    if let Some(epochs) = epochs {
        lock_recover(runtime.projection.as_ref()).remember_directory(
            generation,
            epochs,
            path.clone(),
            Arc::clone(&entries),
        );
    }
    Ok(entries)
}

fn directory_snapshot(
    source: &dyn MountFilesystem,
    path: &MountPath,
) -> Result<Vec<ProjectedEntry>, HRESULT> {
    // Pin only while building the snapshot. Holding this lease for the native
    // directory handle's lifetime would block unrelated authored writes.
    let epoch = source.view_epoch();
    let lease = source
        .acquire_view_lease(epoch)
        .map_err(|error| source_hresult(&error))?;
    let mut cursor = None;
    let mut seen_cursors = HashSet::new();
    let mut entries = Vec::new();
    loop {
        let page = source
            .read_directory(path, cursor.as_deref(), DIRECTORY_PAGE_SIZE)
            .map_err(|error| source_hresult(&error))?;
        entries
            .try_reserve(page.entries.len())
            .map_err(|_| HR_OUT_OF_MEMORY)?;
        for entry in page.entries {
            let info = basic(entry.node, Some(entry.metadata)).ok_or(HR_NOT_SUPPORTED)?;
            let mut name = decode_utf16_name(&entry.name).ok_or(HR_INVALID_DATA)?;
            if name.contains(&0) {
                return Err(HR_INVALID_DATA);
            }
            name.push(0);
            let symlink_target = if entry.node.kind == MountNodeKind::SymbolicLink {
                let target = source
                    .read_link(&path.child(entry.name.clone()))
                    .map_err(|error| source_hresult(&error))?;
                symlink_extended(&target).ok_or(HR_INVALID_DATA)?;
                Some(target)
            } else {
                None
            };
            entries.push(ProjectedEntry {
                name,
                info,
                symlink_target,
            });
        }
        match page.next_cursor {
            Some(next) if !seen_cursors.insert(next.clone()) => return Err(HR_INVALID_DATA),
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    drop(lease);
    entries.sort_unstable_by(|left, right| {
        // SAFETY: both NUL-terminated name buffers remain live for this call.
        unsafe {
            PrjFileNameCompare(
                PCWSTR::from_raw(left.name.as_ptr()),
                PCWSTR::from_raw(right.name.as_ptr()),
            )
        }
        .cmp(&0)
    });
    if entries.windows(2).any(|pair| {
        let [first, second] = pair else {
            return false;
        };
        // SAFETY: both NUL-terminated name buffers remain live for this call.
        unsafe {
            PrjFileNameCompare(
                PCWSTR::from_raw(first.name.as_ptr()),
                PCWSTR::from_raw(second.name.as_ptr()),
            ) == 0
        }
    }) {
        // A case-fold collision cannot be projected as two distinct names.
        return Err(HR_INVALID_DATA);
    }
    Ok(entries)
}

unsafe extern "system" fn end_directory(
    callback_data: *const PRJ_CALLBACK_DATA,
    enumeration_id: *const GUID,
) -> HRESULT {
    let Some((_data, runtime)) = runtime(callback_data) else {
        return HR_UNEXPECTED;
    };
    let Some(id) = enum_id(enumeration_id) else {
        return HR_INVALID_DATA;
    };
    let state = lock_recover(&runtime.enumerations).get(&id).cloned();
    if let Some(state) = state {
        // Wait for any in-flight page fill before ending this enumeration.
        lock_recover(&state).ended = true;
        lock_recover(&runtime.enumerations).remove(&id);
    }
    HR_OK
}

#[allow(
    clippy::too_many_lines,
    reason = "paged ProjFS enumeration and buffer filling share one callback boundary"
)]
unsafe extern "system" fn get_directory(
    callback_data: *const PRJ_CALLBACK_DATA,
    enumeration_id: *const GUID,
    search_expression: PCWSTR,
    buffer: PRJ_DIR_ENTRY_BUFFER_HANDLE,
) -> HRESULT {
    let Some((data, runtime)) = runtime(callback_data) else {
        return HR_UNEXPECTED;
    };
    let Some(id) = enum_id(enumeration_id) else {
        return HR_INVALID_DATA;
    };
    let Some(state) = lock_recover(&runtime.enumerations).get(&id).cloned() else {
        return HR_INVALID_DATA;
    };
    // Serialize the complete cursor/read/fill transition for this enumeration.
    // The map lock is not held while waiting for this state lock.
    let mut state = lock_recover(&state);
    if state.ended {
        return HR_INVALID_DATA;
    }
    let restart = data.Flags.0 & PRJ_CB_DATA_FLAG_ENUM_RESTART_SCAN.0 != 0;
    if !state.search_expression_captured || restart {
        let Some(expression) = copy_optional_wide(search_expression) else {
            return HR_INVALID_DATA;
        };
        state.search_expression = expression;
        state.search_expression_captured = true;
    }
    let entries = match state.snapshot.as_ref() {
        Some(entries) if !restart => Arc::clone(entries),
        _ => {
            let entries = match directory_entries(runtime, &state.path) {
                Ok(entries) => entries,
                Err(error) => return error,
            };
            state.snapshot = Some(Arc::clone(&entries));
            state.next = 0;
            entries
        }
    };
    let mut emitted = false;
    while let Some(entry) = entries.get(state.next) {
        let Some((&0, name)) = entry.name.split_last() else {
            return HR_INVALID_DATA;
        };
        let entry_name = HSTRING::from_wide(name);
        let matches = state.search_expression.as_ref().is_none_or(|expression| {
            // SAFETY: both owned UTF-16 strings remain live for this synchronous comparison.
            unsafe { PrjFileNameMatch(&entry_name, expression) }
        });
        if !matches {
            state.next += 1;
            continue;
        }
        let symlink = entry.symlink_target.as_deref().and_then(symlink_extended);
        let result = if let Some((_target, extended)) = symlink.as_ref() {
            PrjFillDirEntryBuffer2(
                buffer,
                PCWSTR::from_raw(entry_name.as_ptr()),
                Some(&raw const entry.info),
                Some(extended),
            )
        } else {
            PrjFillDirEntryBuffer(
                PCWSTR::from_raw(entry_name.as_ptr()),
                Some(&raw const entry.info),
                buffer,
            )
        };
        match result {
            Ok(()) => {
                state.next += 1;
                emitted = true;
            }
            Err(error) if error.code() == HR_INSUFFICIENT_BUFFER => {
                return if emitted {
                    HR_OK
                } else {
                    HR_INSUFFICIENT_BUFFER
                };
            }
            Err(error) => return error.code(),
        }
    }
    HR_OK
}

unsafe extern "system" fn placeholder(callback_data: *const PRJ_CALLBACK_DATA) -> HRESULT {
    let Some((data, runtime)) = runtime(callback_data) else {
        return HR_UNEXPECTED;
    };
    let Some(path) = path_from(data.FilePathName) else {
        return HR_INVALID_DATA;
    };
    let absence = match admit_negative_paths(runtime, data.NamespaceVirtualizationContext) {
        Ok(absence) => absence,
        Err(error) => return error,
    };
    let (node, pin) = match runtime.source.lookup_pinned(&path) {
        Ok(Some(found)) => found,
        Ok(None) | Err(MountSourceError::NotFound) => {
            return absence.report(source_epochs(runtime.source.as_ref()));
        }
        Err(error) => return source_hresult(&error),
    };
    let Some(placeholder) = placeholder_info(&node, pin) else {
        return HR_NOT_SUPPORTED;
    };
    let symlink = if node.node.kind == MountNodeKind::SymbolicLink {
        let target = match runtime.source.read_link(&path) {
            Ok(target) => target,
            Err(error) => return source_hresult(&error),
        };
        let Some(target) = symlink_extended(&target) else {
            return HR_INVALID_DATA;
        };
        Some(target)
    } else {
        None
    };
    let result = if let Some((_target, extended)) = symlink.as_ref() {
        PrjWritePlaceholderInfo2(
            data.NamespaceVirtualizationContext,
            data.FilePathName,
            &raw const placeholder,
            u32::try_from(size_of::<PRJ_PLACEHOLDER_INFO>()).unwrap_or(u32::MAX),
            Some(extended),
        )
    } else {
        PrjWritePlaceholderInfo(
            data.NamespaceVirtualizationContext,
            data.FilePathName,
            &raw const placeholder,
            u32::try_from(size_of::<PRJ_PLACEHOLDER_INFO>()).unwrap_or(u32::MAX),
        )
    };
    match result {
        Ok(()) => HR_OK,
        Err(error) => error.code(),
    }
}

unsafe extern "system" fn file_data(
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
    let renamed_file = lock_recover(&runtime.renamed_hydration_files)
        .get(&file_id(data))
        .map(|renamed| Arc::clone(&renamed.file));
    let pin = placeholder_pin(data);
    // Immutable hydration reads take the source's own read gate. Keeping them
    // off the mutation callback queue lets concurrent compiler reads proceed.
    let read = |offset, length| match (&renamed_file, pin) {
        (Some(file), _) => file.read_range(offset, length),
        (None, Some(pin)) => runtime.source.read_pinned(&path, pin, offset, length),
        (None, None) => runtime.source.read_range(&path, offset, length),
    };
    let chunk = length.min(HYDRATION_CHUNK_BYTES);
    let buffer = PrjAllocateAlignedBuffer(data.NamespaceVirtualizationContext, chunk as usize);
    if buffer.is_null() {
        return HR_OUT_OF_MEMORY;
    }
    let mut written = 0_u32;
    let result = loop {
        let remaining = length - written;
        if remaining == 0 {
            break HR_OK;
        }
        let count = remaining.min(chunk);
        let Some(offset) = byte_offset.checked_add(u64::from(written)) else {
            break HR_INVALID_DATA;
        };
        let bytes = match read(offset, count) {
            Ok(bytes) if bytes.len() == count as usize => bytes,
            Ok(_) => break HR_INVALID_DATA,
            Err(error) => break source_hresult(&error),
        };
        // SAFETY: ProjFS allocated `chunk >= count` aligned bytes and both
        // buffers are live and non-overlapping for the synchronous write.
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), buffer.cast::<u8>(), bytes.len());
        if let Err(error) = PrjWriteFileData(
            data.NamespaceVirtualizationContext,
            &raw const data.DataStreamId,
            buffer,
            offset,
            count,
        ) {
            break error.code();
        }
        written += count;
    };
    PrjFreeAlignedBuffer(buffer);
    result
}

unsafe extern "system" fn query_name(callback_data: *const PRJ_CALLBACK_DATA) -> HRESULT {
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
unsafe extern "system" fn notification(
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
        return HR_OK;
    }
    if notification == PRJ_NOTIFICATION_NEW_FILE_CREATED {
        // The physical file is already present. One final-state capture at
        // the operation boundary covers every later write and metadata edit;
        // only topology changes need further per-file notifications.
        if is_directory {
            lock_recover(runtime.post_operation_failure.as_ref())
                .queue_subtree(&path, "new directory awaiting capture".to_owned());
        } else {
            defer_host_capture(runtime.post_operation_failure.as_ref(), path);
        }
        set_post_create_notification_mask(operation_parameters, true);
        return HR_OK;
    }
    if !is_directory && notification == PRJ_NOTIFICATION_FILE_OVERWRITTEN {
        set_post_create_notification_mask(operation_parameters, false);
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
    let renamed_hydration_files = Arc::clone(&runtime.renamed_hydration_files);
    let renamed_paths = Arc::clone(&runtime.renamed_paths);
    let metadata_root = Arc::clone(&runtime.metadata_root);
    let post_operation_failure = Arc::clone(&runtime.post_operation_failure);
    let operation_failures = Arc::clone(&post_operation_failure);
    let failure_path = path.clone();
    let retry_path = Arc::new(Mutex::new(None));
    let operation_retry_path = Arc::clone(&retry_path);
    let operation = move || {
        let path = lock_recover(renamed_hydration_files.as_ref())
            .get(&file_id)
            .map_or_else(|| path.clone(), |renamed| renamed.destination.clone());
        if path.components().is_empty()
            && (notification == PRJ_NOTIFICATION_FILE_OPENED
                || notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION)
        {
            return Ok(());
        }
        if notification == PRJ_NOTIFICATION_FILE_OPENED {
            {
                let mut files = lock_recover(renamed_hydration_files.as_ref());
                if !files.contains_key(&file_id) {
                    let pending = files
                        .iter()
                        .find_map(|(pending_id, renamed)| {
                            (renamed.destination == path).then_some(*pending_id)
                        })
                        .and_then(|pending_id| files.remove(&pending_id));
                    if let Some(renamed) = pending {
                        files.insert(file_id, renamed);
                    }
                }
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
            let renamed_path = lock_recover(renamed_paths.as_ref()).get(&file_id).cloned();
            let capture_path = renamed_path.clone().unwrap_or_else(|| Some(path.clone()));
            if final_close {
                lock_recover(renamed_hydration_files.as_ref()).remove(&file_id);
                lock_recover(renamed_paths.as_ref()).remove(&file_id);
            }
            let result = if let Some(path) = capture_path.as_ref() {
                if notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED
                    && renamed_path.is_none()
                {
                    lock_recover(operation_failures.as_ref())
                        .pending_captures
                        .remove(path);
                    *lock_recover(operation_retry_path.as_ref()) = Some(path.clone());
                    match source.lookup(path)? {
                        Some(_) => {
                            let _invalidate = InvalidateOnDrop(projection.as_ref());
                            source.remove(path, None)
                        }
                        None => Ok(()),
                    }
                } else {
                    defer_host_capture(operation_failures.as_ref(), path.clone());
                    Ok(())
                }
            } else {
                Ok(())
            };
            if result.is_ok()
                && !final_close
                && notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED
                && let Some(path) = capture_path.as_ref()
            {
                let baseline = probe_host_windows_metadata(
                    metadata_root.as_ref(),
                    path,
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
            let capture_path = lock_recover(renamed_paths.as_ref())
                .remove(&file_id)
                .unwrap_or_else(|| Some(path.clone()));
            let Some(capture_path) = capture_path else {
                return Ok(());
            };
            *lock_recover(operation_retry_path.as_ref()) = Some(capture_path.clone());
            let host = probe_host_windows_metadata(
                metadata_root.as_ref(),
                &capture_path,
                metadata_probes.as_ref(),
            )?;
            if let Some(baseline) = baseline {
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
                let (cached_binding, cache_generation) = {
                    let projection = lock_recover(projection.as_ref());
                    (
                        projection.bindings.get(&capture_path).copied(),
                        projection.generation,
                    )
                };
                if !metadata_changed_since_open(baseline, host)
                    && !has_pending_capture(operation_failures.as_ref(), &capture_path)
                    && let Some(binding) = cached_binding
                    && cache_generation.is_some()
                    && binding.host == host
                    && host.links == Some(1)
                    && let Ok(_view_lease) = source.acquire_view_lease(Some(binding.epochs.view))
                    && source.binding_epoch() == Some(binding.epochs.binding)
                    && lock_recover(projection.as_ref()).generation == cache_generation
                {
                    return Ok(());
                }
            }
            let epochs_before = source_epochs(source.as_ref());
            let cache_generation = lock_recover(projection.as_ref()).generation;
            let lookup = source.lookup(&capture_path);
            match lookup {
                Ok(Some(lookup)) => match baseline {
                    Some(baseline) if metadata_changed_since_open(baseline, host) => {
                        let _invalidate = InvalidateOnDrop(projection.as_ref());
                        capture_changed_windows_metadata(
                            source.as_ref(),
                            &capture_path,
                            lookup.metadata,
                            baseline,
                            host,
                        )
                    }
                    Some(_) => {
                        if let Some(epochs) = epochs_before
                            && source_epochs(source.as_ref()) == Some(epochs)
                            && !has_pending_capture(operation_failures.as_ref(), &capture_path)
                            && host.links == Some(1)
                        {
                            lock_recover(projection.as_ref()).remember_binding(
                                cache_generation,
                                capture_path,
                                ReadOnlyBinding { host, epochs },
                            );
                        }
                        Ok(())
                    }
                    None => Ok(()),
                },
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
            let renamed_path = if destination_is_external {
                None
            } else {
                destination.clone()
            };
            let _invalidate = InvalidateOnDrop(projection.as_ref());
            let result = handle_rename_source(
                source.as_ref(),
                &path,
                source_is_external,
                destination_is_external,
                is_directory,
                destination,
                file_id,
                renamed_hydration_files.as_ref(),
                operation_failures.as_ref(),
            );
            if result.is_ok() {
                if !source_is_external && let Some(destination) = renamed_path.as_ref() {
                    lock_recover(operation_failures.as_ref())
                        .rename_pending_captures(&path, destination);
                }
                lock_recover(renamed_paths.as_ref()).insert(file_id, renamed_path);
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
    if notification == PRJ_NOTIFICATION_NEW_FILE_CREATED
        || notification == PRJ_NOTIFICATION_FILE_OVERWRITTEN
    {
        set_post_create_notification_mask(
            operation_parameters,
            notification == PRJ_NOTIFICATION_NEW_FILE_CREATED,
        );
    } else if notification == PRJ_NOTIFICATION_FILE_RENAMED && matches!(&result, Some(Ok(()))) {
        set_post_rename_notification_mask(operation_parameters);
    }
    match result {
        Some(Ok(())) => HR_OK,
        Some(Err(error)) => source_hresult(&error),
        None => HR_UNEXPECTED,
    }
}

fn set_post_create_notification_mask(
    operation_parameters: *mut PRJ_NOTIFICATION_PARAMETERS,
    new_regular_file: bool,
) {
    if operation_parameters.is_null() {
        return;
    }
    // SAFETY: ProjFS supplies a writable notification-parameter union for
    // NEW_FILE_CREATED and FILE_OVERWRITTEN callbacks.
    unsafe {
        let topology_mask = PRJ_NOTIFY_PRE_RENAME
            | PRJ_NOTIFY_FILE_RENAMED
            | PRJ_NOTIFY_PRE_SET_HARDLINK
            | PRJ_NOTIFY_HARDLINK_CREATED;
        (*operation_parameters).PostCreate.NotificationMask = if new_regular_file {
            topology_mask
        } else {
            PRJ_NOTIFY_FILE_HANDLE_CLOSED_NO_MODIFICATION
                | PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_MODIFIED
                | PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_DELETED
                | PRJ_NOTIFY_FILE_OPENED
                | topology_mask
        };
    }
}

fn set_post_rename_notification_mask(operation_parameters: *mut PRJ_NOTIFICATION_PARAMETERS) {
    if operation_parameters.is_null() {
        return;
    }
    // SAFETY: ProjFS supplies the writable renamed-file union member for the
    // FILE_RENAMED callback. Only renamed placeholders need open callbacks to
    // bridge a possible FileId change during deferred hydration.
    unsafe {
        (*operation_parameters).FileRenamed.NotificationMask = PRJ_NOTIFY_FILE_OPENED
            | PRJ_NOTIFY_PRE_RENAME
            | PRJ_NOTIFY_FILE_RENAMED
            | PRJ_NOTIFY_PRE_SET_HARDLINK
            | PRJ_NOTIFY_HARDLINK_CREATED;
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "rename admission facts are supplied independently by the ProjFS callback"
)]
fn handle_rename_source(
    source_fs: &dyn MountFilesystem,
    source: &MountPath,
    source_is_external: bool,
    destination_is_external: bool,
    is_directory: bool,
    destination: Option<MountPath>,
    file_id: u128,
    renamed_hydration_files: &Mutex<HashMap<u128, RenamedHydrationFile>>,
    post_operation_failures: &Mutex<PostOperationFailures>,
) -> Result<(), MountSourceError> {
    if source_is_external {
        let destination = destination
            .ok_or_else(|| MountSourceError::Invalid("rename destination is invalid".to_owned()))?;
        lock_recover(renamed_hydration_files)
            .retain(|_, renamed| renamed.destination != destination);
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
    let open_file = (!is_directory)
        .then(|| source_fs.open_file(source))
        .transpose()?;
    // ProjFS rejects renaming projected placeholder directories before the
    // provider callback. Only files require an identity bridge here.
    source_fs.rename(source, &destination, true)?;
    source_fs.flush()?;
    if let Some(open_file) = open_file {
        let mut files = lock_recover(renamed_hydration_files);
        files.retain(|_, renamed| renamed.destination != destination);
        files.insert(
            file_id,
            RenamedHydrationFile {
                file: open_file,
                destination,
            },
        );
    }
    Ok(())
}

unsafe fn copy_optional_wide(pointer: PCWSTR) -> Option<Option<HSTRING>> {
    if pointer.is_null() {
        return Some(None);
    }
    let mut length = 0_usize;
    while *pointer.0.add(length) != 0 {
        length = length.checked_add(1)?;
    }
    Some(Some(HSTRING::from_wide(std::slice::from_raw_parts(
        pointer.0, length,
    ))))
}

unsafe extern "system" fn cancel(_callback_data: *const PRJ_CALLBACK_DATA) {}

fn callbacks() -> PRJ_CALLBACKS {
    PRJ_CALLBACKS {
        StartDirectoryEnumerationCallback: Some(start_directory),
        EndDirectoryEnumerationCallback: Some(end_directory),
        GetDirectoryEnumerationCallback: Some(get_directory),
        GetPlaceholderInfoCallback: Some(placeholder),
        GetFileDataCallback: Some(file_data),
        QueryFileNameCallback: Some(query_name),
        NotificationCallback: Some(notification),
        CancelCommandCallback: Some(cancel),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::{
        CONTENT_PIN_PROVIDER, CallbackGate, PostOperationFailures, ProjectedEntry, ProjectionCache,
        ReadOnlyBinding, SourceEpochs, finish_cleanup, flush_callback_gate, placeholder_info,
        placeholder_pin, record_post_operation_failure, recover_cache_only_destination,
        remove_authenticated_destination, source_hresult,
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
        PRJ_CALLBACK_DATA, PRJ_FILE_BASIC_INFO, PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED,
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

    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn cached_enumeration_follows_source_and_mount_changes()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = windows_checkout_source().await?;
        source.create_file(&windows_path("a.txt"), FileMetadata::default())?;
        let (_root, destination, mut session) = mount_source(&source)?;
        assert_eq!(sorted_names(&destination)?, ["a.txt"]);
        assert_eq!(sorted_names(&destination)?, ["a.txt"]);

        // A source-side create advances the source view; the listing cached
        // for the earlier view must not answer for the new one.
        source.create_file(&windows_path("b.txt"), FileMetadata::default())?;
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

    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn cached_absence_clears_when_the_source_view_changes()
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
        // Listing the newer view clears absences cached under the older one,
        // so a listed name also opens.
        assert_eq!(sorted_names(&destination)?, ["listed.txt"]);
        assert_eq!(std::fs::read(destination.join("listed.txt"))?, b"listed");

        assert_eq!(
            absent("invalidated.txt"),
            Some(std::io::ErrorKind::NotFound)
        );
        source.create_file(&windows_path("invalidated.txt"), FileMetadata::default())?;
        session.invalidate(b"/invalidated.txt")?;
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

    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn placeholder_hydrates_only_the_source_content_it_promised()
    -> Result<(), Box<dyn std::error::Error>> {
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

        let stale = std::fs::read(destination.join("changed.txt"));
        assert!(
            stale.is_err(),
            "a placeholder hydrated content it never promised: {stale:?}"
        );
        assert_eq!(std::fs::read(destination.join("stable.txt"))?, b"stable");
        mount.unmount().await?;
        Ok(())
    }

    #[test]
    fn projection_cache_retains_only_results_of_the_current_view_and_generation() {
        let path = windows_path("directory");
        let entries = Arc::<[ProjectedEntry]>::from(vec![ProjectedEntry {
            name: vec![u16::from(b'a'), 0],
            info: PRJ_FILE_BASIC_INFO::default(),
            symlink_target: None,
        }]);
        let first = SourceEpochs {
            view: 1,
            binding: 1,
        };
        let next = SourceEpochs {
            view: 2,
            binding: 1,
        };
        let mut cache = ProjectionCache::default();

        let generation = cache.generation;
        cache.remember_directory(generation, first, path.clone(), Arc::clone(&entries));
        assert!(cache.directory(&path, first).is_some());
        assert!(cache.directory(&path, next).is_none());

        // A snapshot read before a provider mutation finished is never kept.
        let stale_generation = cache.generation;
        cache.invalidate();
        assert!(cache.directory(&path, first).is_none());
        cache.remember_directory(stale_generation, first, path.clone(), Arc::clone(&entries));
        assert!(cache.directory(&path, first).is_none());
        let binding = ReadOnlyBinding {
            host: super::HostWindowsMetadata {
                identity: crate::NativeRootIdentity::from_bytes([1; 16]),
                links: Some(1),
                size: 0,
                attributes: 0,
                created: 0,
                modified: 0,
            },
            epochs: first,
        };
        cache.remember_binding(stale_generation, path.clone(), binding);
        assert!(cache.bindings.is_empty());

        // Remembering in a newer view drops every snapshot of the older one.
        let generation = cache.generation;
        cache.remember_directory(generation, first, path.clone(), Arc::clone(&entries));
        cache.remember_directory(
            generation,
            next,
            windows_path("other"),
            Arc::clone(&entries),
        );
        assert!(cache.directory(&path, first).is_none());
        assert!(cache.directory(&windows_path("other"), next).is_some());
        assert_eq!(cache.directory_entries, 1);

        // An exhausted generation disables caching for good.
        cache.generation = Some(u64::MAX);
        cache.invalidate();
        cache.remember_directory(cache.generation, next, path.clone(), entries);
        assert!(cache.directory(&path, next).is_none());
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

        finish_cleanup(&mut state, |_| Ok::<(), &str>(())).expect("retry cleanup succeeds");
        assert_eq!(state, None);
    }
}
