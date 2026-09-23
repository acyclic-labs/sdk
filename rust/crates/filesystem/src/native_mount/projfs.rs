//! Audited Windows `ProjFS` FFI with exact close-boundary authored capture.
//!
//! `ProjFS` does not expose write byte ranges, so modified and newly created
//! files are streamed from final host state only after the corresponding file
//! handle closes. Rename and hard-link notifications preserve stable identity,
//! and every acknowledged authored notification seals the checkout generation.

#![allow(unsafe_code, unsafe_op_in_unsafe_fn)]

use super::{
    DriverStartFailure, MountFilesystem, MountNode, MountNodeKind, MountPath, NativeMountError,
    NativeMountRequest,
};
use crate::kernel::{FileMetadata, MetadataField};
use crate::native_host::HostRoot;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::c_void;
use std::mem::size_of;
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, mpsc};
use windows::Win32::Storage::FileSystem::{
    FILE_ATTRIBUTE_ARCHIVE, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_NORMAL,
    FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS, FILE_ATTRIBUTE_REPARSE_POINT,
};
use windows::Win32::Storage::ProjectedFileSystem::{
    PRJ_CALLBACK_DATA, PRJ_CALLBACKS, PRJ_CB_DATA_FLAG_ENUM_RESTART_SCAN,
    PRJ_DIR_ENTRY_BUFFER_HANDLE, PRJ_EXT_INFO_TYPE_SYMLINK, PRJ_EXTENDED_INFO, PRJ_EXTENDED_INFO_0,
    PRJ_EXTENDED_INFO_0_0, PRJ_FILE_BASIC_INFO, PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT,
    PRJ_NOTIFICATION, PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED,
    PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED,
    PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION, PRJ_NOTIFICATION_FILE_OPENED,
    PRJ_NOTIFICATION_FILE_OVERWRITTEN, PRJ_NOTIFICATION_FILE_RENAMED,
    PRJ_NOTIFICATION_HARDLINK_CREATED, PRJ_NOTIFICATION_MAPPING, PRJ_NOTIFICATION_NEW_FILE_CREATED,
    PRJ_NOTIFICATION_PARAMETERS, PRJ_NOTIFICATION_PRE_DELETE, PRJ_NOTIFICATION_PRE_RENAME,
    PRJ_NOTIFICATION_PRE_SET_HARDLINK, PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_DELETED,
    PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_MODIFIED, PRJ_NOTIFY_FILE_HANDLE_CLOSED_NO_MODIFICATION,
    PRJ_NOTIFY_FILE_OPENED, PRJ_NOTIFY_FILE_OVERWRITTEN, PRJ_NOTIFY_FILE_RENAMED,
    PRJ_NOTIFY_HARDLINK_CREATED, PRJ_NOTIFY_NEW_FILE_CREATED, PRJ_NOTIFY_PRE_DELETE,
    PRJ_NOTIFY_PRE_RENAME, PRJ_NOTIFY_PRE_SET_HARDLINK, PRJ_PLACEHOLDER_INFO,
    PRJ_STARTVIRTUALIZING_OPTIONS, PrjAllocateAlignedBuffer, PrjFileNameCompare, PrjFileNameMatch,
    PrjFillDirEntryBuffer, PrjFillDirEntryBuffer2, PrjFreeAlignedBuffer,
    PrjMarkDirectoryAsPlaceholder, PrjStartVirtualizing, PrjStopVirtualizing, PrjWriteFileData,
    PrjWritePlaceholderInfo, PrjWritePlaceholderInfo2,
};
use windows::core::{GUID, HRESULT, HSTRING, PCWSTR};

const HR_OK: HRESULT = HRESULT(0);
const HR_FILE_NOT_FOUND: HRESULT = HRESULT(0x8007_0002_u32.cast_signed());
const HR_ALREADY_EXISTS: HRESULT = HRESULT(0x8007_00b7_u32.cast_signed());
const HR_NOT_SAME_DEVICE: HRESULT = HRESULT(0x8007_0011_u32.cast_signed());
const HR_INVALID_DATA: HRESULT = HRESULT(0x8007_000d_u32.cast_signed());
const HR_NOT_SUPPORTED: HRESULT = HRESULT(0x8007_0032_u32.cast_signed());
const HR_OUT_OF_MEMORY: HRESULT = HRESULT(0x8007_000e_u32.cast_signed());
const HR_UNEXPECTED: HRESULT = HRESULT(0x8000_ffff_u32.cast_signed());
const HR_INSUFFICIENT_BUFFER: HRESULT = HRESULT(0x8007_007a_u32.cast_signed());

const DIRECTORY_PAGE_SIZE: u32 = 256;
const NOTIFICATION_ROOT: [u16; 1] = [0];

struct EnumState {
    path: MountPath,
    ended: bool,
    entries: VecDeque<ProjectedEntry>,
    snapshot_ready: bool,
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
    metadata_probes: Arc<Mutex<HashMap<MountPath, usize>>>,
    post_operation_failure: Arc<Mutex<PostOperationFailures>>,
    executor: CallbackExecutor,
}

struct CallbackExecutor {
    senders: Vec<mpsc::SyncSender<Job>>,
}

type Job = Box<dyn FnOnce() + Send>;

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

impl CallbackExecutor {
    fn start() -> Result<Self, NativeMountError> {
        let workers = std::thread::available_parallelism().map_or(1, |count| count.get().min(16));
        let mut senders = Vec::with_capacity(workers);
        for index in 0..workers {
            let (sender, receiver) = mpsc::sync_channel::<Job>(256);
            std::thread::Builder::new()
                .name(format!("acyclic-projfs-{index}"))
                .stack_size(32 * 1024 * 1024)
                .spawn(move || {
                    while let Ok(job) = receiver.recv() {
                        job();
                    }
                })
                .map_err(|error| NativeMountError::Driver(error.to_string()))?;
            senders.push(sender);
        }
        Ok(Self { senders })
    }

    fn call<T: Send + 'static>(&self, operation: impl FnOnce() -> T + Send + 'static) -> Option<T> {
        self.drain()?;
        self.call_observed(0, operation, |_| {})
    }

    fn drain(&self) -> Option<()> {
        let mut completions = Vec::with_capacity(self.senders.len());
        for sender in &self.senders {
            let (complete, received) = mpsc::sync_channel(1);
            sender
                .send(Box::new(move || {
                    let _ = complete.send(());
                }))
                .ok()?;
            completions.push(received);
        }
        for received in completions {
            received.recv().ok()?;
        }
        Some(())
    }

    fn call_observed<T: Send + 'static>(
        &self,
        key: u128,
        operation: impl FnOnce() -> T + Send + 'static,
        observe: impl FnOnce(&T) + Send + 'static,
    ) -> Option<T> {
        let (sender, receiver) = mpsc::sync_channel(1);
        let count = u128::try_from(self.senders.len()).ok()?;
        let shard = usize::try_from(key % count).ok()?;
        self.senders
            .get(shard)?
            .send(Box::new(move || {
                let result = operation();
                observe(&result);
                let _ = sender.send(result);
            }))
            .ok()?;
        receiver.recv().ok()
    }
}

fn flush_callback_executor(
    executor: &CallbackExecutor,
    failure: Arc<Mutex<PostOperationFailures>>,
    capture: impl Fn(&[(MountPath, bool)]) -> Result<(), super::MountSourceError> + Send + 'static,
) -> Result<(), NativeMountError> {
    let pending_failure = Arc::clone(&failure);
    let pending = executor
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
    // outside the serialized callback executor so nested notifications can run.
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
    let failure = executor
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
        let executor = CallbackExecutor::start()?;
        let metadata_root = Arc::new(
            HostRoot::open(&request.destination)
                .map_err(|error| NativeMountError::Driver(error.to_string()))?,
        );
        let root = HSTRING::from(request.destination.as_os_str());
        let root_ptr = PCWSTR::from_raw(root.as_ptr());
        let bytes = request.mount_id.into_bytes();
        let guid = GUID::from_values(
            u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            u16::from_le_bytes([bytes[4], bytes[5]]),
            u16::from_le_bytes([bytes[6], bytes[7]]),
            [
                bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14],
                bytes[15],
            ],
        );
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
            enumerations: Mutex::new(HashMap::new()),
            renamed_hydration_files: Arc::new(Mutex::new(HashMap::new())),
            renamed_paths: Arc::new(Mutex::new(HashMap::new())),
            metadata_baselines: Arc::new(Mutex::new(HashMap::new())),
            metadata_probes: Arc::new(Mutex::new(HashMap::new())),
            post_operation_failure: Arc::new(Mutex::new(PostOperationFailures::default())),
            executor,
        });
        let context_ptr = (&raw mut *runtime).cast::<c_void>();
        let callbacks = callbacks();
        let mut notification_mapping = PRJ_NOTIFICATION_MAPPING {
            NotificationBitMask: PRJ_NOTIFY_NEW_FILE_CREATED
                | PRJ_NOTIFY_FILE_OVERWRITTEN
                | PRJ_NOTIFY_FILE_HANDLE_CLOSED_NO_MODIFICATION
                | PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_MODIFIED
                | PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_DELETED
                | PRJ_NOTIFY_FILE_OPENED
                | PRJ_NOTIFY_PRE_DELETE
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
        flush_callback_executor(
            &runtime.executor,
            Arc::clone(&runtime.post_operation_failure),
            move |paths| {
                // Captures read through this projection. Suppress their own
                // read-only notifications while the source mutation lease is
                // held, and authenticate exact paths together so hard links
                // share one identity and one checkout transaction.
                let _guards = paths
                    .iter()
                    .map(|(path, _)| MetadataProbeGuard::enter(probes.as_ref(), path))
                    .collect::<Vec<_>>();
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
                                Err(super::MountSourceError::NotFound) | Ok(None) => {
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
                            return Err(super::MountSourceError::Engine(error.to_string()));
                        }
                    }
                }
                for subtree in subtrees {
                    source.capture_host_subtree(&root, &subtree)?;
                }
                source.capture_host_paths(&root, &exact)
            },
        )
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
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(250);
    loop {
        // ProjFS may briefly reject removal after PrjStopVirtualizing. Every
        // attempt must reauthenticate the directory, because the path can be
        // replaced while the filter is draining.
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
            Err(error)
                if matches!(error.raw_os_error(), Some(145 | 369))
                    && std::time::Instant::now() < deadline =>
            {
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

#[allow(unsafe_code)]
fn reparse_tag(
    path: &std::path::Path,
) -> Result<(Option<u32>, crate::NativeRootIdentity), NativeMountError> {
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use windows::Win32::Foundation::{ERROR_NOT_A_REPARSE_POINT, HANDLE};
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
    attributes: u32,
    created: i64,
    modified: i64,
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
) -> Result<HostWindowsMetadata, super::MountSourceError> {
    use cap_std::fs::MetadataExt as _;

    let host_path = host_relative_path(path)?;
    let metadata = root
        .symlink_metadata_held(&host_path)
        .map_err(|error| super::MountSourceError::Engine(error.to_string()))?;
    Ok(HostWindowsMetadata {
        attributes: metadata.file_attributes(),
        created: i64::try_from(metadata.creation_time()).map_err(|_| {
            super::MountSourceError::Invalid("Windows creation time exceeds i64".to_owned())
        })?,
        modified: i64::try_from(metadata.last_write_time()).map_err(|_| {
            super::MountSourceError::Invalid("Windows write time exceeds i64".to_owned())
        })?,
    })
}

fn host_relative_path(path: &MountPath) -> Result<PathBuf, super::MountSourceError> {
    let mut host_path = PathBuf::new();
    for component in path.components() {
        let name = decode_utf16_name(component).ok_or_else(|| {
            super::MountSourceError::Invalid("ProjFS path component is malformed".to_owned())
        })?;
        host_path.push(std::ffi::OsString::from_wide(&name));
    }
    Ok(host_path)
}

fn capture_missing_host_ancestors(
    source: &dyn MountFilesystem,
    root: &std::path::Path,
    path: &MountPath,
) -> Result<(), super::MountSourceError> {
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
) -> Result<HostWindowsMetadata, super::MountSourceError> {
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
) -> Result<(), super::MountSourceError> {
    let mut baselines = lock_recover(baselines);
    if let Some(state) = baselines.get_mut(&file_id) {
        if state.open_handles == 0 {
            state.open_handles = 1;
        } else {
            state.open_handles = state.open_handles.checked_add(1).ok_or_else(|| {
                super::MountSourceError::Invalid("ProjFS open-handle count overflow".to_owned())
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
    // Hydration replaces RECALL_ON_DATA_ACCESS with ARCHIVE, while a read may
    // also advance last-access time. Neither transition is an authored edit.
    let hydration_mask = FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS.0 | FILE_ATTRIBUTE_ARCHIVE.0;
    let attributes_changed = if baseline.attributes & FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS.0 != 0 {
        baseline.attributes & !hydration_mask != current.attributes & !hydration_mask
    } else {
        baseline.attributes != current.attributes
    };
    attributes_changed
        || baseline.created != current.created
        || baseline.modified != current.modified
}

fn capture_changed_windows_metadata(
    source: &dyn MountFilesystem,
    path: &MountPath,
    mut metadata: FileMetadata,
    baseline: HostWindowsMetadata,
    current: HostWindowsMetadata,
) -> Result<(), super::MountSourceError> {
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

fn enum_id(pointer: *const GUID) -> Option<u128> {
    // SAFETY: callback ABI supplies a GUID pointer for the callback duration.
    let guid = unsafe { pointer.as_ref()? };
    let mut bytes = [0_u8; 16];
    bytes[0..4].copy_from_slice(&guid.data1.to_le_bytes());
    bytes[4..6].copy_from_slice(&guid.data2.to_le_bytes());
    bytes[6..8].copy_from_slice(&guid.data3.to_le_bytes());
    bytes[8..16].copy_from_slice(&guid.data4);
    Some(u128::from_le_bytes(bytes))
}

fn file_id(data: &PRJ_CALLBACK_DATA) -> u128 {
    let guid = &data.FileId;
    let mut bytes = [0_u8; 16];
    bytes[0..4].copy_from_slice(&guid.data1.to_le_bytes());
    bytes[4..6].copy_from_slice(&guid.data2.to_le_bytes());
    bytes[6..8].copy_from_slice(&guid.data3.to_le_bytes());
    bytes[8..16].copy_from_slice(&guid.data4);
    u128::from_le_bytes(bytes)
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

fn unix_nanoseconds(windows_ticks: i64) -> Result<i64, super::MountSourceError> {
    const WINDOWS_EPOCH_OFFSET_SECONDS: i128 = 11_644_473_600;
    const HUNDRED_NANOSECONDS_PER_SECOND: i128 = 10_000_000;
    i128::from(windows_ticks)
        .checked_sub(
            WINDOWS_EPOCH_OFFSET_SECONDS
                .checked_mul(HUNDRED_NANOSECONDS_PER_SECOND)
                .ok_or_else(|| {
                    super::MountSourceError::Invalid("Windows epoch conversion overflow".to_owned())
                })?,
        )
        .and_then(|ticks| ticks.checked_mul(100))
        .and_then(|nanoseconds| i64::try_from(nanoseconds).ok())
        .ok_or_else(|| {
            super::MountSourceError::Invalid("Windows timestamp exceeds i64 nanoseconds".to_owned())
        })
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
    lock_recover(&runtime.enumerations).insert(
        id,
        Arc::new(Mutex::new(EnumState {
            path,
            ended: false,
            entries: VecDeque::new(),
            snapshot_ready: false,
            search_expression: None,
            search_expression_captured: false,
        })),
    );
    HR_OK
}

fn directory_snapshot(
    source: &dyn MountFilesystem,
    path: &MountPath,
) -> Result<VecDeque<ProjectedEntry>, HRESULT> {
    // Pin only while building the snapshot. Holding this lease for the native
    // directory handle's lifetime would block unrelated authored writes.
    let epoch = source.view_epoch();
    let lease = source
        .acquire_view_lease(epoch)
        .map_err(|_| HR_UNEXPECTED)?;
    let mut cursor = None;
    let mut seen_cursors = HashSet::new();
    let mut entries = Vec::new();
    loop {
        let page = source
            .read_directory(path, cursor.as_deref(), DIRECTORY_PAGE_SIZE)
            .map_err(|_| HR_UNEXPECTED)?;
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
                    .map_err(|_| HR_UNEXPECTED)?;
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
    Ok(entries.into())
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
    if !state.snapshot_ready || restart {
        state.snapshot_ready = false;
        state.entries.clear();
        state.entries = match directory_snapshot(runtime.source.as_ref(), &state.path) {
            Ok(entries) => entries,
            Err(error) => return error,
        };
        state.snapshot_ready = true;
    }
    let mut emitted = false;
    loop {
        let Some(entry) = state.entries.front() else {
            return HR_OK;
        };
        let Some((&0, name)) = entry.name.split_last() else {
            return HR_INVALID_DATA;
        };
        let entry_name = HSTRING::from_wide(name);
        let matches = state.search_expression.as_ref().is_none_or(|expression| {
            // SAFETY: both owned UTF-16 strings remain live for this synchronous comparison.
            unsafe { PrjFileNameMatch(&entry_name, expression) }
        });
        if !matches {
            state.entries.pop_front();
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
                state.entries.pop_front();
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
}

unsafe extern "system" fn placeholder(callback_data: *const PRJ_CALLBACK_DATA) -> HRESULT {
    let Some((data, runtime)) = runtime(callback_data) else {
        return HR_UNEXPECTED;
    };
    let Some(path) = path_from(data.FilePathName) else {
        return HR_INVALID_DATA;
    };
    let node = match runtime.source.lookup(&path) {
        Ok(Some(node)) => node,
        Ok(None) | Err(super::MountSourceError::NotFound) => return HR_FILE_NOT_FOUND,
        Err(_) => return HR_UNEXPECTED,
    };
    let Some(info) = basic(node.node, Some(node.metadata)) else {
        return HR_NOT_SUPPORTED;
    };
    let placeholder = PRJ_PLACEHOLDER_INFO {
        FileBasicInfo: info,
        ..PRJ_PLACEHOLDER_INFO::default()
    };
    let symlink = if node.node.kind == MountNodeKind::SymbolicLink {
        let target = match runtime.source.read_link(&path) {
            Ok(target) => target,
            Err(super::MountSourceError::NotFound) => return HR_FILE_NOT_FOUND,
            Err(_) => return HR_UNEXPECTED,
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
    let source_path = path;
    let renamed_file = lock_recover(&runtime.renamed_hydration_files)
        .get(&file_id(data))
        .cloned();
    let metadata_path = renamed_file.as_ref().map_or_else(
        || source_path.clone(),
        |renamed| renamed.destination.clone(),
    );
    // Immutable hydration reads take the source's own read gate. Keeping them
    // off the mutation callback queue lets concurrent compiler reads proceed.
    let bytes = match renamed_file.map_or_else(
        || runtime.source.read_range(&source_path, byte_offset, length),
        |renamed| renamed.file.read_range(byte_offset, length),
    ) {
        Ok(bytes) if bytes.len() == length as usize => bytes,
        Ok(_) => return HR_INVALID_DATA,
        Err(super::MountSourceError::NotFound) => return HR_FILE_NOT_FOUND,
        Err(_) => return HR_UNEXPECTED,
    };
    let buffer = PrjAllocateAlignedBuffer(data.NamespaceVirtualizationContext, bytes.len());
    if buffer.is_null() {
        return HR_OUT_OF_MEMORY;
    }
    // SAFETY: ProjFS allocated `bytes.len()` aligned bytes and both buffers are
    // live and non-overlapping for the synchronous write.
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), buffer.cast::<u8>(), bytes.len());
    let result = PrjWriteFileData(
        data.NamespaceVirtualizationContext,
        &raw const data.DataStreamId,
        buffer,
        byte_offset,
        length,
    );
    PrjFreeAlignedBuffer(buffer);
    match result {
        Ok(()) => match probe_host_windows_metadata(
            runtime.metadata_root.as_ref(),
            &metadata_path,
            runtime.metadata_probes.as_ref(),
        ) {
            Ok(metadata) => {
                refresh_metadata_baseline(
                    runtime.metadata_baselines.as_ref(),
                    file_id(data),
                    metadata,
                );
                HR_OK
            }
            Err(_) => HR_UNEXPECTED,
        },
        Err(error) => error.code(),
    }
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
        Ok(None) | Err(super::MountSourceError::NotFound) => HR_FILE_NOT_FOUND,
        Err(_) => HR_UNEXPECTED,
    }
}

fn record_post_operation_failure(
    failure: &Mutex<PostOperationFailures>,
    notification: PRJ_NOTIFICATION,
    path: &MountPath,
    retry_path: Option<&MountPath>,
    result: &Result<(), super::MountSourceError>,
) {
    if !matches!(
        notification,
        PRJ_NOTIFICATION_NEW_FILE_CREATED
            | PRJ_NOTIFICATION_FILE_OVERWRITTEN
            | PRJ_NOTIFICATION_FILE_RENAMED
            | PRJ_NOTIFICATION_HARDLINK_CREATED
            | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION
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
    if notification == PRJ_NOTIFICATION_PRE_DELETE
        || notification == PRJ_NOTIFICATION_PRE_RENAME
        || notification == PRJ_NOTIFICATION_PRE_SET_HARDLINK
    {
        return HR_OK;
    }
    if !is_directory && notification == PRJ_NOTIFICATION_NEW_FILE_CREATED {
        // The physical file is already present. One final-state capture at
        // the operation boundary covers every later write and metadata edit;
        // only topology changes need further per-file notifications.
        defer_host_capture(runtime.post_operation_failure.as_ref(), path);
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
    let source = Arc::clone(&runtime.source);
    let metadata_baselines = Arc::clone(&runtime.metadata_baselines);
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
                        Some(_) => source.remove(path, None),
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
            // This is the final close. Retire every per-handle record before
            // doing fallible host I/O so an error cannot poison a later
            // ProjFS FileId reuse with stale rename or hydration state.
            let capture_path = lock_recover(renamed_paths.as_ref())
                .remove(&file_id)
                .unwrap_or_else(|| Some(path.clone()));
            // ProjFS may close a renamed placeholder before requesting its
            // contents. Its later data callback still names the original path,
            // so retain the immutable source until the placeholder is replaced
            // or this virtualization instance ends.
            let Some(capture_path) = capture_path else {
                return Ok(());
            };
            *lock_recover(operation_retry_path.as_ref()) = Some(capture_path.clone());
            let host = probe_host_windows_metadata(
                metadata_root.as_ref(),
                &capture_path,
                metadata_probes.as_ref(),
            )?;
            match source.lookup(&capture_path) {
                Ok(Some(lookup)) => match baseline {
                    Some(baseline) if metadata_changed_since_open(baseline, host) => {
                        // ProjFS classifies FileBasicInfo-only changes as a
                        // close without data modification. Apply only the
                        // fields that changed since open so metadata capture
                        // never re-reads a projected file while this callback
                        // owns the checkout worker.
                        capture_changed_windows_metadata(
                            source.as_ref(),
                            &capture_path,
                            lookup.metadata,
                            baseline,
                            host,
                        )
                    }
                    _ => Ok(()),
                },
                Ok(None) => {
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
        } else if notification == PRJ_NOTIFICATION_HARDLINK_CREATED {
            destination
                .ok_or_else(|| {
                    super::MountSourceError::Invalid("hard-link destination is invalid".to_owned())
                })
                .map(|destination| {
                    // ProjFS has completed the physical link. Capture both
                    // names together at the operation boundary: per-link SDK
                    // transactions serialize compiler output and can flatten
                    // a new source's identity before its close notification.
                    defer_host_capture(operation_failures.as_ref(), path);
                    defer_host_capture(operation_failures.as_ref(), destination);
                })
        } else if notification == PRJ_NOTIFICATION_PRE_DELETE
            || notification == PRJ_NOTIFICATION_PRE_RENAME
            || notification == PRJ_NOTIFICATION_PRE_SET_HARDLINK
        {
            Ok(())
        } else {
            Err(super::MountSourceError::Unsupported(
                "ProjFS emitted an unadmitted write notification".to_owned(),
            ))
        }
    };
    let result = runtime
        .executor
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
        set_post_create_notification_mask(operation_parameters, false);
    }
    match result {
        Some(Ok(())) => HR_OK,
        Some(Err(super::MountSourceError::NotFound)) => HR_FILE_NOT_FOUND,
        Some(Err(super::MountSourceError::AlreadyExists)) => HR_ALREADY_EXISTS,
        Some(Err(super::MountSourceError::Invalid(_))) => HR_INVALID_DATA,
        Some(Err(super::MountSourceError::Unsupported(_))) => HR_NOT_SUPPORTED,
        Some(Err(super::MountSourceError::Engine(_) | super::MountSourceError::Stale)) | None => {
            HR_UNEXPECTED
        }
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
                | PRJ_NOTIFY_PRE_DELETE
                | topology_mask
        };
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
) -> Result<(), super::MountSourceError> {
    if source_is_external {
        let destination = destination.ok_or_else(|| {
            super::MountSourceError::Invalid("rename destination is invalid".to_owned())
        })?;
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
    let destination = destination.ok_or_else(|| {
        super::MountSourceError::Invalid("rename destination is invalid".to_owned())
    })?;
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
        CallbackExecutor, HostWindowsMetadata, MetadataClose, MetadataProbeGuard,
        PostOperationFailures, close_metadata_handle, finish_cleanup, flush_callback_executor,
        record_metadata_open, record_post_operation_failure, recover_cache_only_destination,
        refresh_metadata_baseline, remove_authenticated_destination,
    };
    use crate::native_mount::{MountPath, MountSourceError};
    use crate::{Fs, IdempotencyKey, LocalOptions, MountOptions, MountPublication};
    use bytes::Bytes;
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Barrier, Mutex};
    use windows::Win32::Storage::ProjectedFileSystem::{
        PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED,
        PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION, PRJ_NOTIFICATION_PRE_DELETE,
        PRJ_VIRTUALIZATION_INSTANCE_INFO, PrjGetVirtualizationInstanceInfo,
        PrjMarkDirectoryAsPlaceholder, PrjStartVirtualizing, PrjStopVirtualizing,
    };
    use windows::core::{GUID, HSTRING, PCWSTR};

    #[test]
    fn projfs_name_order_differs_from_sdk_cursor_order() {
        let name = |value: &str| {
            value
                .encode_utf16()
                .flat_map(u16::to_le_bytes)
                .collect::<Vec<_>>()
        };
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
        let executor = CallbackExecutor::start().expect("callback executor");
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
        let error = flush_callback_executor(&executor, Arc::clone(&failure), |_| {
            Err(MountSourceError::Invalid("transient failure".to_owned()))
        })
        .expect_err("failed retry must fence publication");
        assert!(error.to_string().contains("transient failure"));
        flush_callback_executor(&executor, Arc::clone(&failure), |_| Ok(()))
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
        let executor = CallbackExecutor::start().expect("callback executor");
        let failure = Arc::new(Mutex::new(PostOperationFailures::default()));
        let path = MountPath::root().child(b"changed".to_vec());
        failure
            .lock()
            .expect("failure state")
            .queue_capture(path.clone(), "first write".to_owned());
        let during_capture = Arc::clone(&failure);
        let error = flush_callback_executor(&executor, Arc::clone(&failure), move |paths| {
            during_capture
                .lock()
                .expect("failure state")
                .queue_capture(paths[0].0.clone(), "second write".to_owned());
            Ok(())
        })
        .expect_err("the newer write remains pending");
        assert!(error.to_string().contains("second write"));
        flush_callback_executor(&executor, Arc::clone(&failure), |_| Ok(()))
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
            PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION,
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
        let executor = CallbackExecutor::start().expect("callback executor");
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
        flush_callback_executor(&executor, Arc::clone(&failure), move |paths| {
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
        let executor = CallbackExecutor::start().expect("callback executor");
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
        flush_callback_executor(&executor, Arc::clone(&failure), move |paths| {
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
        let component = |name: &str| {
            name.encode_utf16()
                .flat_map(u16::to_le_bytes)
                .collect::<Vec<_>>()
        };
        let source = MountPath::root().child(component("Old"));
        let callback_source = MountPath::root().child(component("old"));
        let destination = MountPath::root().child(component("New"));
        let mut failures = PostOperationFailures::default();
        failures.queue_capture(source.clone(), "capture failed".to_owned());
        failures.rename_pending_captures(&callback_source, &destination);
        assert!(!failures.pending_captures.contains_key(&source));
        assert!(failures.pending_captures.contains_key(&destination));
    }

    #[test]
    fn callback_admitted_after_first_stop_barrier_prevents_cleanup() {
        let executor = CallbackExecutor::start().expect("callback executor");
        let failure = Arc::new(Mutex::new(PostOperationFailures::default()));
        flush_callback_executor(&executor, Arc::clone(&failure), |_| Ok(()))
            .expect("first barrier");
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
            flush_callback_executor(&executor, Arc::clone(&failure), |_| {
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

    #[test]
    fn metadata_probe_guard_is_path_scoped_counted_and_raii() {
        let probes = Mutex::new(HashMap::new());
        let first = MountPath::root().child(vec![b'a', 0]);
        let same_casefolded = MountPath::root().child(vec![b'A', 0]);
        let second = MountPath::root().child(vec![b'b', 0]);

        let outer = MetadataProbeGuard::enter(&probes, &first);
        assert!(MetadataProbeGuard::is_active(&probes, &first));
        assert!(MetadataProbeGuard::is_active(&probes, &same_casefolded));
        assert!(!MetadataProbeGuard::is_active(&probes, &second));
        {
            let _overlap = MetadataProbeGuard::enter(&probes, &first);
            assert!(MetadataProbeGuard::is_active(&probes, &first));
        }
        assert!(MetadataProbeGuard::is_active(&probes, &first));
        drop(outer);
        assert!(!MetadataProbeGuard::is_active(&probes, &first));
    }

    #[test]
    fn metadata_baseline_closes_only_after_the_final_handle() {
        let baselines = Mutex::new(HashMap::new());
        let baseline = HostWindowsMetadata {
            attributes: 1,
            created: 2,
            modified: 3,
        };
        record_metadata_open(&baselines, 7, baseline).expect("first open is recorded");
        record_metadata_open(&baselines, 7, baseline).expect("overlapping open is recorded");
        assert!(matches!(
            close_metadata_handle(&baselines, 7),
            MetadataClose::Pending
        ));
        assert!(matches!(
            close_metadata_handle(&baselines, 7),
            MetadataClose::Final(Some(value)) if value == baseline
        ));
        assert!(matches!(
            close_metadata_handle(&baselines, 7),
            MetadataClose::Final(None)
        ));

        refresh_metadata_baseline(&baselines, 8, baseline);
        record_metadata_open(&baselines, 8, baseline)
            .expect("open after hydration baseline is idempotent");
        assert!(matches!(
            close_metadata_handle(&baselines, 8),
            MetadataClose::Final(Some(value)) if value == baseline
        ));
    }

    #[test]
    fn hydration_attributes_are_not_authored_metadata_changes() {
        let recalled = HostWindowsMetadata {
            attributes: windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS.0,
            created: 2,
            modified: 3,
        };
        let hydrated = HostWindowsMetadata {
            attributes: windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_ARCHIVE.0,
            ..recalled
        };
        assert!(!super::metadata_changed_since_open(recalled, recalled));
        assert!(!super::metadata_changed_since_open(recalled, hydrated));
    }
}
