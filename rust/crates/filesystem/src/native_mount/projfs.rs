//! Windows ProjFS projection with close-boundary authored capture.

#![allow(
    unsafe_code,
    unsafe_op_in_unsafe_fn,
    reason = "audited Windows ProjFS FFI"
)]

use super::provider_stack;
use super::{
    DIRECTORY_PAGE_SIZE, DriverStartFailure, MountContentPin, MountFilesystem, MountLookup,
    MountNode, MountNodeKind, MountPath, MountSourceError, NativeMountError, NativeMountRequest,
    NativeMountCallbackFailure, ViewObserver, ViewOrigin, ViewStamp,
};
use crate::FileId;
use crate::kernel::{FileMetadata, MetadataField};
use crate::native_host::HostRoot;
use crate::demand::DemandWork;
use crate::native_host::NativeHostWork as _;
use std::ffi::c_void;
use std::mem::size_of;
use std::os::windows::ffi::OsStrExt;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::sync::atomic::Ordering;
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
    PRJ_NOTIFY_TYPES, PRJ_PLACEHOLDER_INFO, PRJ_STARTVIRTUALIZING_OPTIONS, PRJ_VIRTUALIZATION_INSTANCE_INFO,
    PRJ_UPDATE_ALLOW_READ_ONLY, PRJ_UPDATE_ALLOW_TOMBSTONE, PRJ_UPDATE_TYPES, PRJ_UPDATE_FAILURE_CAUSES, PrjDeleteFile,
    PrjFileNameCompare, PrjMarkDirectoryAsPlaceholder, PrjStartVirtualizing, PrjStopVirtualizing,
    PrjWritePlaceholderInfo2, PrjGetVirtualizationInstanceInfo,
};
use windows::core::{GUID, HRESULT, PCWSTR};

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
const HR_OUT_OF_MEMORY: HRESULT = HRESULT(0x8007_000e_u32.cast_signed());
const HR_VIRTUALIZATION_INVALID_OPERATION: HRESULT = HRESULT(0x8007_0181_u32.cast_signed());
const HR_SHARING_VIOLATION: HRESULT = HRESULT(0x8007_0020_u32.cast_signed());
const HR_DIRECTORY_NOT_EMPTY: HRESULT = HRESULT(0x8007_0091_u32.cast_signed());

/// Every notification the provider observes, for every file, for its whole life.
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
    name: RetainedBuffer<Vec<u16>>,
    /// The exact component retains its source's canonical paid guard.
    component: RetainedBuffer<Vec<u8>>,
    /// What the entry is written from.
    facts: EntryFacts,
    /// The basic information written for this entry.
    info: PRJ_FILE_BASIC_INFO,
    symlink_target: Option<RetainedBuffer<crate::kernel::OwnedSourceChunk>>,
    /// Whether a link names a directory: a Windows link says so when made.
    directory_link: bool,
}

impl ProjectedEntry {
    fn take_component(&mut self) -> RetainedBuffer<Vec<u8>> {
        self.component.take()
    }
}


/// Physical data drops first.
struct RetainedBuffer<T> {
    value: Option<T>,
    custody: crate::demand::NativeMountAllocationCustody,
    bytes: u64,
    /// Canonical native claim; `bytes` counts only ordinary custody above.
    native_allocation: Option<crate::NativeAllocation>,
    /// Intrinsic source payload custody, including ordinary original ownership.
    source_ownership: Option<crate::demand::SourcePayloadOwnership>,
}

impl<T> RetainedBuffer<T> {
    fn take(&mut self) -> Self {
        Self { value: self.value.take(), custody: self.custody.clone(), bytes: std::mem::take(&mut self.bytes),
            native_allocation: self.native_allocation.take(), source_ownership: self.source_ownership.take() }
    }

    fn transferred(
        value: T, bytes: u64, custody: crate::demand::NativeMountAllocationCustody, work: &mut DemandWork,
    ) -> Result<Self, NativeMountError> {
        if let Err(error) = custody.retain_admitted(bytes, work) {
            drop(value);
            work.release(bytes).map_err(detach_work_error)?;
            return Err(error.into());
        }
        Ok(Self { value: Some(value), custody, bytes, native_allocation: None, source_ownership: None })
    }


    fn source_owned(value: T, ownership: crate::demand::SourcePayloadOwnership, custody: crate::demand::NativeMountAllocationCustody) -> Self {
        Self { value: Some(value), custody, bytes: 0, native_allocation: None, source_ownership: Some(ownership) }
    }

}

impl RetainedBuffer<crate::kernel::OwnedSourceChunk> {
    fn source_chunk(packet: crate::kernel::OwnedSourceChunk, projection: Projection<'_>) -> Result<Self, NativeMountError> {
        if packet.is_intrinsic() {
            // OwnedBytes keeps the original guard through native completion.
            return Ok(Self { value: Some(packet), custody: projection.custody.clone(), bytes: 0, native_allocation: None, source_ownership: None });
        }
        let bytes = packet.logical_owned_bytes;
        Self::transferred(packet, bytes, projection.custody.clone(), &mut projection.work.meter().lock())
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
        if self.bytes != 0 {
            self.custody.retire(self.bytes).expect("only actually freed buffer bytes retire");
        }
    }
}

struct RetainedArcValue<T> {
    value: T,
    _allocation: crate::NativeAllocation,
}

impl<T> std::ops::Deref for RetainedArcValue<T> {
    type Target = T;
    fn deref(&self) -> &T { &self.value }
}

impl<T: ViewObserver> ViewObserver for RetainedArcValue<T> {
    fn view_changed(&self, position: ViewStamp, origin: ViewOrigin) {
        self.value.view_changed(position, origin);
    }
    fn allocation_custody(&self) -> Option<crate::demand::NativeMountAllocationCustody> {
        self.value.allocation_custody()
    }
}


fn retire_ordinary_storage(
    bytes: u64, custody: &crate::demand::NativeMountAllocationCustody,
) -> Result<(), NativeMountError> {
    if bytes != 0 { custody.retire(bytes)?; }
    Ok(())
}


fn clone_host_path_with_work(
    path: &std::path::Path, custody: &crate::demand::NativeMountAllocationCustody, work: &super::NativeMountMeterWork,
) -> Result<RetainedBuffer<PathBuf>, NativeMountError> {
    let bytes = path.as_os_str().as_encoded_bytes().len() as u64;
    let copied = crate::WorkCounters { bytes_copied: bytes, ..crate::WorkCounters::UNCHARGED };
    let materialization = work.native_materialization_work();
    materialization.admit(copied)?;
    let mut native_allocation = work.native_materialization_work().claim(bytes)?;
    let mut value = PathBuf::new();
    if let Err(error) = value.try_reserve_exact(bytes as usize) {
        drop(value);
        return Err(NativeMountError::Allocation(error));
    }
    native_allocation.reconcile_capacity(value.capacity() as u64)?;
    value.push(path);
    let retained = RetainedBuffer { value: Some(value), custody: custody.clone(), bytes: 0, native_allocation: Some(native_allocation), source_ownership: None };
    materialization.record_admitted(copied)?;
    Ok(retained)
}

fn retained_arc_on_meter<T>(
    value: T, _custody: &crate::demand::NativeMountAllocationCustody,
    meter: &super::NativeMountMeterWork,
) -> Result<Arc<RetainedArcValue<T>>, NativeMountError> {
    let allocation = meter.native_materialization_work().claim(size_of::<RetainedArcValue<T>>() as u64)?;
    Ok(Arc::new(RetainedArcValue { value, _allocation: allocation }))
}


struct Runtime {
    source: Arc<dyn MountFilesystem>,
    original_work: Option<super::NativeMountMeterWork>,
    callback_guard: Option<Arc<dyn super::NativeMountCallbackGuard>>,
    root: RetainedBuffer<PathBuf>,
    metadata_root: Arc<RetainedArcValue<HostRoot>>,
    writable: bool,
    metadata_baselines: Arc<RetainedArcValue<Mutex<MemoMap<u128, OpenMetadataState>>>>,
    projection: Arc<RetainedArcValue<Mutex<ProjectionCache>>>,
    /// Everything written into the projection, kept current as the source changes.
    projector: Arc<RetainedArcValue<Projector>>,
    metadata_probes: Arc<RetainedArcValue<Mutex<MemoMap<MountPath, Arc<RetainedArcValue<std::sync::atomic::AtomicUsize>>>>>>,
    /// The admitted retry snapshot doubles as borrowed metadata suppression; no second path clone or guessed hash-table allocation is needed.
    capture_probes: std::sync::RwLock<Vec<(MountPath, u64, bool)>>,
    flush_gate: Mutex<()>,
    /// Present only while a real lifecycle operation owns SDK callbacks.
    stop_meter: Mutex<super::NativeMountMeterActivation>,
    /// The actual sibling created by this driver, including an unprobed held capability.
    staging_custody: Mutex<Option<StagingDirectoryCustody>>,
    /// Held for a file while one handle's metadata edits are read from the source and written back.
    metadata_edits: Arc<RetainedArcValue<FileLocks>>,
    post_operation_failure: Arc<RetainedArcValue<Mutex<PostOperationFailures>>>,
    callbacks: RetainedBuffer<CallbackGate>,
    handler_budget: crate::WorkBudget,
    handler_cancellation: crate::CancellationToken,
}

fn same_callback_work(
    left: &super::NativeMountMeterWork, right: &super::NativeMountMeterWork,
) -> bool {
    Arc::ptr_eq(left.meter(), right.meter())
        && match (left.resource_allowance(), right.resource_allowance()) {
            (None, None) => true,
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
        && match (left.capture_origin(), right.capture_origin()) {
            (None, None) => true,
            (Some(left), Some(right)) => left.ptr_eq(right),
            _ => false,
        }
        && match (left.invocation_view_state(), right.invocation_view_state()) {
            (None, None) => true,
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
}

fn installed_callback_work(
    runtime: &Runtime,
) -> Result<Option<super::NativeMountMeterWork>, NativeMountError> {
    let current_guard = runtime.source.native_callback_guard();
    let same_guard = match (&runtime.callback_guard, &current_guard) {
        (None, None) => true,
        (Some(installed), Some(current)) => Arc::ptr_eq(installed, current),
        _ => false,
    };
    if !same_guard {
        return Err(NativeMountError::Lifecycle("source callback guard differs from its installed owner"));
    }
    let current = runtime.source.native_original_work().map_err(detach_source_error)?;
    match (&runtime.original_work, current) {
        (Some(installed), Some(current)) if same_callback_work(installed, &current) =>
            Ok(Some(installed.clone())),
        (None, None) if runtime.callback_guard.is_none() => Ok(None),
        _ => Err(NativeMountError::Lifecycle("source callback work differs from its installed invocation")),
    }
}

/// Installed native sources retain their original full invocation even outside a lifecycle window.
fn with_callback_meter<T>(
    runtime: &Runtime,
    operation: impl FnOnce(&super::NativeMountMeterWork) -> Result<T, NativeMountError>,
) -> Result<T, NativeMountCallbackFailure> {
    with_callback_meter_for_role(runtime, false, operation)
}

fn with_callback_meter_for_role<T>(
    runtime: &Runtime, authored_write: bool,
    operation: impl FnOnce(&super::NativeMountMeterWork) -> Result<T, NativeMountError>,
) -> Result<T, NativeMountCallbackFailure> {
    let bound = installed_callback_work(runtime).map_err(|cause| NativeMountCallbackFailure {
        cause, work: crate::WorkCounters::UNCHARGED,
    })?;
    let active = lock_recover(&runtime.stop_meter).upgrade();
    if let Some(meter) = active.or(bound) {
        verify_callback_work_for_role(runtime, &meter, authored_write)
            .map_err(|cause| NativeMountCallbackFailure {
                cause, work: meter.meter().lock().work,
            })?;
        return super::run_native_mount_handler_on_meter(&meter, operation)
            .map_err(|cause| NativeMountCallbackFailure { cause, work: meter.meter().lock().work });
    }
    let result = super::run_native_mount_handler(runtime.handler_budget, &runtime.handler_cancellation, operation);
    let completed = match &result {
        Ok(receipt) => receipt.work,
        Err(failure) => failure.work,
    };
    // Only this genuinely ordinary request has physically completed.
    let folded = {
        let mut failures = lock_recover(runtime.post_operation_failure.as_ref());
        let folded = failures.completed_callback_work.try_add_assign(&completed);
        if let Err(cause) = folded {
            failures.unreplayable_barrier = true;
            if result.is_ok() && failures.unreplayable.is_none() {
                failures.unreplayable = Some(NativeMountCallbackFailure {
                    cause: NativeMountError::Work(cause), work: completed,
                });
            }
        }
        folded
    };
    match (result, folded) {
        (result, Ok(())) => result.map(|receipt| receipt.value),
        (Err(failure), Err(_)) => Err(failure),
        (Ok(_), Err(cause)) => Err(NativeMountCallbackFailure {
            cause: NativeMountError::Work(cause), work: completed,
        }),
    }
}

/// Await the source's authentic installed guard on its existing callback runtime.
fn verify_callback_work(
    runtime: &Runtime, work: &super::NativeMountMeterWork,
) -> Result<(), NativeMountError> {
    verify_callback_work_for_role(runtime, work, false)
}

fn verify_callback_work_for_role(
    runtime: &Runtime, work: &super::NativeMountMeterWork, authored_write: bool,
) -> Result<(), NativeMountError> {
    if runtime.original_work.as_ref().is_some_and(|original| !same_callback_work(original, work)) {
        return Err(NativeMountError::Lifecycle("callback work differs from its installed invocation"));
    }
    work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)
        .map_err(detach_work_error)?;
    if let Some(guard) = runtime.callback_guard.as_ref() {
        let callbacks = super::adapter::CallbackRuntime::existing()?;
        if authored_write {
            callbacks.verify_native_callback_write_guard(guard.as_ref(), work)?;
        } else {
            callbacks.verify_native_callback_guard(guard.as_ref(), work)?;
        }
    }
    Ok(())
}

fn verify_source_write_work(
    source: &dyn MountFilesystem, work: &super::NativeMountMeterWork,
) -> Result<(), NativeMountError> {
    work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)
        .map_err(detach_work_error)?;
    if let Some(guard) = source.native_callback_guard() {
        super::adapter::CallbackRuntime::existing()?
            .verify_native_callback_write_guard(guard.as_ref(), work)?;
    }
    Ok(())
}

/// Moves the first actual fatal callback cause, without erasing its persistent publication fence or stealing any replayable path/cause from the old queue.
fn take_callback_failure(runtime: &Runtime) -> Option<NativeMountCallbackFailure> {
    lock_recover(runtime.post_operation_failure.as_ref()).unreplayable.take()
}

fn callback_projection<'a>(
    runtime: &'a Runtime, data: &PRJ_CALLBACK_DATA,
    work: &'a super::NativeMountMeterWork,
    custody: &'a crate::demand::NativeMountAllocationCustody,
) -> Projection<'a> {
    Projection {
        source: runtime.source.as_ref(), work, custody,
        context: data.NamespaceVirtualizationContext,
        root: &runtime.root, root_identity: runtime.metadata_root.identity(),
        staging_custody: &runtime.staging_custody,
        captures: &runtime.post_operation_failure, starting: false,
    }
}

fn native_hresult(error: &NativeMountError) -> HRESULT {
    match error {
        NativeMountError::Source(error) => source_hresult(error),
        NativeMountError::Materialization(error) => materialization_hresult(error),
        NativeMountError::CallbackDenied { .. } => HRESULT::from_win32(5),
        NativeMountError::Demand(crate::DemandError::Cancelled) => HR_FILE_INVALID,
        NativeMountError::Io(error) if error.kind() == std::io::ErrorKind::NotFound => HR_FILE_NOT_FOUND,
        _ => HR_IO_DEVICE,
    }
}

struct CallbackGate {
    shards: Box<[Mutex<()>]>,
    active: Mutex<usize>,
    idle: Condvar,
    closing: std::sync::atomic::AtomicBool,
    _allocation: Option<crate::NativeAllocation>,
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
    pending_captures: MemoMap<MountPath, PendingCapture>,
    next_capture_serial: u64,
    /// Terminal distinct SDK requests only; active lifecycle scopes remain in their original invocation and never contribute to this aggregate.
    completed_callback_work: crate::WorkCounters,
    unreplayable: Option<NativeMountCallbackFailure>,
    /// Moving the first original cause to a caller never clears its fence.
    unreplayable_barrier: bool,
    /// A callback whose provider controls could not be admitted never ran its authored handler.
    provider_control_failure: Option<provider_stack::ProviderFailure>,
}

struct PendingCapture {
    serial: u64,
    reason: &'static str,
    error: Option<NativeMountError>,
    subtree: bool,
}

impl PostOperationFailures {
    fn queue_capture(
        &mut self, path: &MountPath, reason: &'static str, error: &mut Option<NativeMountError>,
        projection: Projection<'_>,
    ) -> Result<(), NativeMountError> {
        self.queue(path, reason, error, false, projection)
    }

    fn queue_subtree(
        &mut self, path: &MountPath, reason: &'static str, projection: Projection<'_>,
    ) -> Result<(), NativeMountError> {
        self.queue(path, reason, &mut None, true, projection)
    }

    fn queue(
        &mut self, path: &MountPath, reason: &'static str, error: &mut Option<NativeMountError>,
        subtree: bool, projection: Projection<'_>,
    ) -> Result<(), NativeMountError> {
        let serial = self.next_capture_serial.wrapping_add(1);
        match self.pending_captures.index(path, projection.work)? {
            Ok(index) => {
                let pending = &mut self.pending_captures.entries.items[index].1;
                pending.serial = serial;
                pending.reason = reason;
                pending.subtree |= subtree;
                if pending.error.is_none() { pending.error = error.take(); }
            }
            Err(index) => {
                let key = path.copy_owned(projection)?;
                self.pending_captures.entries.reserve_one(projection)?;
                let shifted = self.pending_captures.entries.items.len() - index;
                projection.work.native_materialization_work().charge(crate::WorkCounters {
                    bytes_copied: shifted.checked_mul(size_of::<(RetainedBuffer<MountPath>, PendingCapture)>())
                        .ok_or(crate::WorkError::Overflow)? as u64,
                    ..crate::WorkCounters::UNCHARGED
                }).map_err(detach_work_error)?;
                // Moving the original cause is now infallible.
                self.pending_captures.entries.items.insert(index,
                    (key, PendingCapture { serial, reason, error: error.take(), subtree }));
            }
        }
        self.next_capture_serial = serial;
        Ok(())
    }

    /// All new keys/backing and the complete shift are admitted before the first old entry moves.
    fn rename_pending_captures(
        &mut self, source: &MountPath, destination: &MountPath, projection: Projection<'_>,
    ) -> Result<(), NativeMountError> {
        if source == destination { return Ok(()); }
        let mut moved = MemoVec::default();
        for (path, _) in self.pending_captures.iter() {
            projection.work.native_materialization_work().charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            }).map_err(detach_work_error)?;
            if let Some(destination) = owned_rebased(path, source, destination, projection)? {
                moved.push((path.copy_owned(projection)?, destination), projection)?;
            }
        }
        for (source, destination) in moved.items.drain(..) {
            self.pending_captures.rekey(&source, destination, projection, |existing, mut capture| {
                existing.subtree |= capture.subtree;
                if capture.serial > existing.serial { existing.reason = capture.reason; }
                existing.serial = existing.serial.max(capture.serial);
                if existing.error.is_none() { existing.error = capture.error.take(); }
            })?;
        }
        Ok(())
    }
}


fn comparison_name_with_work(
    bytes: &[u8], work: &super::NativeMountMeterWork,
) -> Result<ProjectionBuffer<u16>, NativeMountError> {
    if !bytes.len().is_multiple_of(2) {
        return Err(NativeMountError::Demand(crate::DemandError::InvalidRequest));
    }
    let count = (bytes.len() / 2).checked_add(1).ok_or(crate::WorkError::Overflow)?;
    let mut name = ProjectionBuffer::new(count, work)?;
    for pair in bytes.chunks_exact(2) {
        let examined = crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED };
        work.native_materialization_work().charge(examined).map_err(detach_work_error)?;
        let unit = u16::from_le_bytes([pair[0], pair[1]]);
        if unit == 0 { return Err(NativeMountError::Demand(crate::DemandError::InvalidRequest)); }
        let encoded = crate::WorkCounters { bytes_encoded: 2, ..crate::WorkCounters::UNCHARGED };
        work.native_materialization_work().admit(encoded).map_err(detach_work_error)?;
        name.as_mut().push(unit);
        work.native_materialization_work().record_admitted(encoded).map_err(detach_work_error)?;
    }
    let encoded = crate::WorkCounters { bytes_encoded: 2, ..crate::WorkCounters::UNCHARGED };
    work.native_materialization_work().admit(encoded).map_err(detach_work_error)?;
    name.as_mut().push(0);
    work.native_materialization_work().record_admitted(encoded).map_err(detach_work_error)?;
    Ok(name)
}

fn same_projfs_name_with_work(
    left: &[u8], right: &[u8], work: &super::NativeMountMeterWork,
) -> Result<bool, NativeMountError> {
    work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED).map_err(detach_work_error)?;
    if left == right { return Ok(true); }
    let left = comparison_name_with_work(left, work)?;
    let right = comparison_name_with_work(right, work)?;
    projection_native_call(work, false, || {
        // SAFETY: both exact native owners include NUL and stay live through the actual SDK comparison; no original work lock spans the call.
        Ok(unsafe { PrjFileNameCompare(
            PCWSTR::from_raw(left.as_slice().as_ptr()),
            PCWSTR::from_raw(right.as_slice().as_ptr()),
        ) } == 0)
    })
}

fn projfs_path_suffix_with_work<'a>(
    path: &'a MountPath, prefix: &MountPath, work: &super::NativeMountMeterWork,
) -> Result<Option<&'a [Vec<u8>]>, NativeMountError> {
    let Some(suffix) = path.components().get(prefix.components().len()..) else { return Ok(None); };
    for (left, right) in path.components().iter().zip(prefix.components()) {
        work.native_materialization_work().charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED }).map_err(detach_work_error)?;
        if !same_projfs_name_with_work(left, right, work)? { return Ok(None); }
    }
    Ok(Some(suffix))
}

impl CallbackGate {
    #[cfg(test)]
    fn start() -> Result<Self, NativeMountError> {
        let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .map_err(|(cause, _)| detach_work_error(cause))?;
        const SHARDS: usize = 64;
        let mut allocation = work.claim((SHARDS * size_of::<Mutex<()>>()) as u64)?;
        let mut shards = Vec::new();
        shards.try_reserve_exact(SHARDS).map_err(NativeMountError::Allocation)?;
        allocation.reconcile_capacity((shards.capacity() * size_of::<Mutex<()>>()) as u64)?;
        for _ in 0..SHARDS { shards.push(Mutex::new(())); }
        Ok(Self {
            shards: shards.into_boxed_slice(), active: Mutex::new(0), idle: Condvar::new(),
            closing: std::sync::atomic::AtomicBool::new(false), _allocation: Some(allocation),
        })
    }

    fn start_with_meter(
        work: &super::NativeMountMeterWork, custody: &crate::demand::NativeMountAllocationCustody,
    ) -> Result<RetainedBuffer<Self>, NativeMountError> {
        const SHARDS: usize = 64;
        let bytes = (SHARDS * size_of::<Mutex<()>>()) as u64;
        let mut native_allocation = work.native_materialization_work().claim(bytes)?;
        let mut shards = Vec::new();
        if let Err(error) = shards.try_reserve_exact(SHARDS) {
            drop(shards);
            return Err(NativeMountError::Allocation(error));
        }
        native_allocation.reconcile_capacity((shards.capacity() * size_of::<Mutex<()>>()) as u64)?;
        for _ in 0..SHARDS { shards.push(Mutex::new(())); }
        Ok(RetainedBuffer { value: Some(Self { shards: shards.into_boxed_slice(), active: Mutex::new(0), idle: Condvar::new(), closing: std::sync::atomic::AtomicBool::new(false), _allocation: Some(native_allocation) }), bytes: 0, custody: custody.clone(), native_allocation: None, source_ownership: None })
    }

    fn close_admission(&self) {
        let _active = lock_recover(&self.active);
        self.closing.store(true, std::sync::atomic::Ordering::Release);
    }

    fn drain_cleanup(&self, wait: Option<&crate::CancellationToken>) -> Result<(), NativeMountError> {
        let mut active = lock_recover(&self.active);
        loop {
            if let Some(wait) = wait {
                wait.check().map_err(|_| detach_work_error(crate::NativeTreeMaterializationError::Cancelled))?;
            }
            if *active == 0 { return Ok(()); }
            active = self.idle.wait_timeout(active, std::time::Duration::from_millis(10))
                .unwrap_or_else(std::sync::PoisonError::into_inner).0;
        }
    }

    /// Waits on the existing callback controls without assigning a guessed cost to the foreign callbacks that the SDK happens to drain.
    fn drain_measured(&self, work: &super::NativeMountMeterWork) -> Result<(), NativeMountError> {
        let mut active = lock_recover(&self.active);
        while *active != 0 {
            work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED).map_err(detach_work_error)?;
            active = self.idle.wait_timeout(active, std::time::Duration::from_millis(10))
                .unwrap_or_else(std::sync::PoisonError::into_inner).0;
        }
        work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED).map_err(detach_work_error)
    }

    fn call_observed<T, U>(
        &self, key: u128, work: &super::NativeMountMeterWork,
        operation: impl FnOnce() -> T, observe: impl FnOnce(T) -> U,
    ) -> Result<U, NativeMountError> {
        work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED).map_err(detach_work_error)?;
        let count = self.shards.len() as u128;
        if count == 0 { return Err(NativeMountError::Lifecycle("callback gate has no shards")); }
        let shard = (key % count) as usize;
        {
            let mut active = loop {
                work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED).map_err(detach_work_error)?;
                match self.active.try_lock() {
                    Ok(guard) => break guard,
                    Err(std::sync::TryLockError::Poisoned(error)) => break error.into_inner(),
                    Err(std::sync::TryLockError::WouldBlock) =>
                        std::thread::sleep(std::time::Duration::from_millis(1)),
                }
            };
            if self.closing.load(std::sync::atomic::Ordering::Acquire) {
                return Err(NativeMountError::Lifecycle("original SDK instance callback admission is closed"));
            }
            *active = active.checked_add(1).ok_or(crate::WorkError::Overflow)?;
        }
        let _active = ActiveCallback(self);
        let _serial = loop {
            work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED).map_err(detach_work_error)?;
            match self.shards[shard].try_lock() {
                Ok(guard) => break guard,
                Err(std::sync::TryLockError::Poisoned(error)) => break error.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) =>
                    std::thread::sleep(std::time::Duration::from_millis(1)),
            }
        };
        work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED).map_err(detach_work_error)?;
        // The observer consumes the original result while its request is still active: no error clone or receipt-erasing string conversion.
        Ok(observe(operation()))
    }
}

#[cfg(test)]
fn flush_callback_gate(
    callbacks: &CallbackGate,
    failure: Arc<Mutex<PostOperationFailures>>,
    capture: impl Fn(&[(MountPath, u64, bool)]) -> Result<(), MountSourceError>,
) -> Result<(), NativeMountError> {
    let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .map_err(|(cause, _)| detach_work_error(cause))?;
    flush_callback_gate_measured(callbacks, failure.as_ref(), None, &work.native_mount_work(),
        |paths, _meter| capture(paths).map_err(detach_source_error))
}

/// Snapshots only replayable failures, retaining their serials until a successful capture proves exactly those writes were joined.
fn flush_callback_gate_measured(
    callbacks: &CallbackGate,
    failure: &Mutex<PostOperationFailures>,
    probes: Option<&std::sync::RwLock<Vec<(MountPath, u64, bool)>>>,
    work: &super::NativeMountMeterWork,
    mut capture: impl FnMut(&[(MountPath, u64, bool)], &super::NativeMountMeterWork) -> Result<(), NativeMountError>,
) -> Result<(), NativeMountError> {
    callbacks.drain_measured(work)?;
    if let Some(error) = lock_recover(failure).provider_control_failure {
        return Err(error.into_mount_error());
    }
    let materialization = work.native_materialization_work();
    let mut allocation = materialization.claim(0)?;
    let mut pending: Vec<(MountPath, u64, bool)> = Vec::new();
    struct RestoreProbeSnapshot<'a> {
        probes: &'a std::sync::RwLock<Vec<(MountPath, u64, bool)>>,
        pending: &'a mut Vec<(MountPath, u64, bool)>,
    }
    impl Drop for RestoreProbeSnapshot<'_> {
        fn drop(&mut self) {
            *self.pending = std::mem::take(&mut *self.probes.write()
                .unwrap_or_else(std::sync::PoisonError::into_inner));
        }
    }
    let result = (|| {
        {
            let failure = lock_recover(failure);
            // Only bounded Rust construction/copy runs under this guard.
            let bytes = (failure.pending_captures.entries.items.len() as u64)
                .checked_mul(size_of::<(MountPath, u64, bool)>() as u64)
                .ok_or(crate::WorkError::Overflow)?;
            allocation.absorb(&mut materialization.claim(bytes)?)?;
            pending.try_reserve_exact(failure.pending_captures.entries.items.len())
                .map_err(NativeMountError::Allocation)?;
            debug_assert_eq!(pending.capacity() as u64 * size_of::<(MountPath, u64, bool)>() as u64, bytes,
                "Global reserve-exact reports the admitted layout");
            for (path, capture) in failure.pending_captures.iter() {
                materialization.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                let (path, mut owned) = clone_mount_components_native(path.components(), work)?;
                allocation.absorb(&mut owned)?;
                let (mut low, mut high) = (0, pending.len());
                while low < high {
                    materialization.charge(crate::WorkCounters { items_examined: 2, ..crate::WorkCounters::UNCHARGED })?;
                    let middle = low + (high - low) / 2;
                    let order = pending[middle].0.components().len().cmp(&path.components().len())
                        .then_with(|| pending[middle].0.components().cmp(path.components()));
                    if order.is_lt() { low = middle + 1; } else { high = middle; }
                }
                let shifted = (pending.len() - low).checked_mul(size_of::<(MountPath, u64, bool)>())
                    .ok_or(crate::WorkError::Overflow)? as u64;
                materialization.charge(crate::WorkCounters { bytes_copied: shifted, ..crate::WorkCounters::UNCHARGED })?;
                pending.insert(low, (path, capture.serial, capture.subtree));
            }
        }
        // Host capture reads inside the virtualization root; release the failure lock so nested native notifications can finish.
        if let Some(probes) = probes {
            *probes.write().unwrap_or_else(std::sync::PoisonError::into_inner) = std::mem::take(&mut pending);
            let restore = RestoreProbeSnapshot { probes, pending: &mut pending };
            let held = probes.read().unwrap_or_else(std::sync::PoisonError::into_inner);
            let captured = capture(&held, work);
            drop(held);
            drop(restore);
            captured?;
        } else {
            capture(&pending, work)?;
        }
        {
            let mut failure = lock_recover(failure);
            for (path, serial, _) in &pending {
                materialization.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                if let Ok(index) = failure.pending_captures.index(path, work)?
                    && failure.pending_captures.entries.items[index].1.serial == *serial
                {
                    let shifted = failure.pending_captures.entries.items.len() - index - 1;
                    materialization.charge(crate::WorkCounters {
                        bytes_copied: shifted.checked_mul(size_of::<(RetainedBuffer<MountPath>, PendingCapture)>())
                            .ok_or(crate::WorkError::Overflow)? as u64,
                        ..crate::WorkCounters::UNCHARGED
                    })?;
                    drop(failure.pending_captures.entries.items.remove(index));
                }
            }
        }
        callbacks.drain_measured(work)?;
        let failure = lock_recover(failure);
        if failure.unreplayable_barrier || !failure.pending_captures.is_empty() {
            return Err(NativeMountError::ControlPending);
        }
        Ok(())
    })();
    drop(pending);
    drop(allocation);
    result
}

fn clone_mount_components_native(
    source: &[Vec<u8>], work: &super::NativeMountMeterWork,
) -> Result<(MountPath, crate::NativeAllocation), NativeMountError> {
    let materialization = work.native_materialization_work();
    let bytes = source.len().checked_mul(size_of::<Vec<u8>>()).ok_or(crate::WorkError::Overflow)? as u64;
    let mut allocation = materialization.claim(bytes)?;
    let mut components = Vec::new();
    components.try_reserve_exact(source.len()).map_err(NativeMountError::Allocation)?;
    for component in source {
        let copied = crate::WorkCounters {
            bytes_copied: component.len() as u64, items_examined: 1, ..crate::WorkCounters::UNCHARGED
        };
        materialization.admit(copied)?;
        allocation.absorb(&mut materialization.claim(component.len() as u64)?)?;
        let mut owned = Vec::new();
        owned.try_reserve_exact(component.len()).map_err(NativeMountError::Allocation)?;
        owned.extend_from_slice(component);
        components.push(owned);
        materialization.record_admitted(copied)?;
    }
    Ok((MountPath { components }, allocation))
}



fn defer_host_capture(
    failure: &Mutex<PostOperationFailures>, path: &MountPath, projection: Projection<'_>,
) -> Result<(), NativeMountError> {
    lock_recover(failure).queue_capture(path, "host capture pending", &mut None, projection)
}

/// The source view one read began in.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReadBasis {
    stamp: ViewStamp,
    binding: u64,
}

impl ReadBasis {
    /// `None` while the actual view is moving or cannot version its binding.
    fn sample(projection: Projection<'_>) -> Result<Option<Self>, NativeMountError> {
        let work = projection.work.clone();
        if !projection.source.view_is_stable_with_meter(&work).map_err(detach_source_error)? {
            return Ok(None);
        }
        let Some(stamp) = projection.source.view_stamp_with_meter(&work).map_err(detach_source_error)?
            else { return Ok(None); };
        let Some(binding) = projection.source.binding_epoch_with_meter(&work).map_err(detach_source_error)?
            else { return Ok(None); };
        Ok(Some(Self { stamp, binding }))
    }

    /// Checks the same original source history, not a broad invalidation or an unmeasured lookup substitute.
    fn still_describes(
        self, projection: Projection<'_>, path: &MountPath, file_id: Option<FileId>,
    ) -> Result<bool, NativeMountError> {
        let work = projection.work.clone();
        if projection.source.binding_epoch_with_meter(&work).map_err(detach_source_error)? != Some(self.binding) {
            return Ok(false);
        }
        projection.source.unchanged_since_with_meter(path, file_id, self.stamp, &work)
            .map_err(detach_source_error)
    }

    fn reported_still_describes(
        self, projection: Projection<'_>, path: &MountPath, file_id: Option<FileId>,
    ) -> Result<bool, NativeMountError> {
        let work = projection.work.clone();
        if projection.source.binding_epoch_with_meter(&work).map_err(detach_source_error)? != Some(self.binding) {
            return Ok(false);
        }
        projection.source.reported_unchanged_since_with_meter(path, file_id, self.stamp, &work)
            .map_err(detach_source_error)
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

/// What the projection wrote at a path that `ProjFS` does not manage for it: the projection replaces or removes it only while it is still exactly what was written.
struct PhysicalEntry {
    file: Mutex<Option<std::fs::File>>,
    directory: bool,
    identity: Mutex<Option<crate::NativeRootIdentity>>,
    disposed: std::sync::atomic::AtomicBool,
    expected_reparse: std::sync::atomic::AtomicU32,
    delete_on_close: std::sync::atomic::AtomicBool,
    controls: Mutex<crate::materializer::NativeWorkReservation>,
    temporary_close: Mutex<Option<crate::materializer::NativeWorkReservation>>,
    creator_transfer_close: Mutex<Option<crate::materializer::NativeWorkReservation>>,
    close: Mutex<crate::materializer::NativeWorkReservation>,
}

impl PhysicalEntry {
    fn prepare(
        work: &super::NativeMountMeterWork,
        custody: &crate::demand::NativeMountAllocationCustody,
        directory: bool,
    ) -> Result<Arc<RetainedArcValue<Self>>, NativeMountError> {
        let original = work.native_materialization_work().allocation_work();
        let controls = original.reserve_work(crate::WorkCounters {
            backend_read_operations: if directory { 3 } else { 2 }, backend_write_operations: 2,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let close = original.reserve_work(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        })?;
        let temporary_close = if directory {
            Some(original.reserve_work(crate::WorkCounters {
                backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
            })?)
        } else { None };
        retained_arc_on_meter(Self {
            file: Mutex::new(None), directory, identity: Mutex::new(None),
            disposed: std::sync::atomic::AtomicBool::new(false),
            expected_reparse: std::sync::atomic::AtomicU32::new(0),
            delete_on_close: std::sync::atomic::AtomicBool::new(false),
            controls: Mutex::new(controls), temporary_close: Mutex::new(temporary_close),
            creator_transfer_close: Mutex::new(None), close: Mutex::new(close),
        }, custody, work)
    }

    fn install(&self, file: std::fs::File) {
        *lock_recover(&self.file) = Some(file);
    }

    fn identify(&self, work: &super::NativeMountMeterWork) -> Result<(), NativeMountError> {
        let file = lock_recover(&self.file);
        let file = file.as_ref().expect("installed original creation capability");
        let (identity, _) = crate::native_host::file_identity_and_length_with_work(
            file, &mut &work.native_materialization_work(),
        ).map_err(detach_work_error)?;
        *lock_recover(&self.identity) = Some(identity);
        Ok(())
    }

    /// Derive a metadata-only control from the same original Open.Link after the positional writer closes.
    fn retain_metadata_control(&self, work: &super::NativeMountMeterWork) -> Result<(), NativeMountError> {
        use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _};
        use windows::Win32::Storage::FileSystem::{
            DELETE, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
            FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, ReOpenFile,
        };
        let mut transfer = lock_recover(&self.creator_transfer_close);
        let Some(credit) = transfer.as_mut() else { return Ok(()); };
        let mut held = lock_recover(&self.file);
        let original = held.as_ref().expect("original creator remains held through transfer");
        let control = projection_native_call(work, false, || {
            // SAFETY: derive the same original Open.Link, never a pathname.
            let handle = unsafe { ReOpenFile(
                windows::Win32::Foundation::HANDLE(original.as_raw_handle()),
                DELETE.0 | FILE_READ_ATTRIBUTES.0,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
            ) }.map_err(projection_sdk_io_error)?;
            Ok(unsafe { std::fs::File::from_raw_handle(handle.0) })
        })?;
        let original = held.replace(control).expect("original creator is transferred exactly once");
        credit.started_drop_io(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        });
        drop(original);
        drop(transfer.take());
        Ok(())
    }

    fn final_state(&self, work: &super::NativeMountMeterWork) -> Result<HostWindowsMetadata, NativeMountError> {
        let held = lock_recover(&self.file);
        handle_state_measured(held.as_ref().expect("original metadata control remains held"), work)
    }
}

impl Drop for PhysicalEntry {
    fn drop(&mut self) {
        let file = self.file.get_mut().unwrap_or_else(std::sync::PoisonError::into_inner).take();
        if let Some(file) = file {
            self.close.get_mut().unwrap_or_else(std::sync::PoisonError::into_inner).started_drop_io(crate::WorkCounters {
                backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
            });
            drop(file);
        }
    }
}

/// Close the original capability without disposing namespace data that could not be captured.
fn close_preserved_physical_entry(
    owner: &PhysicalEntry, work: &super::NativeMountMeterWork,
    wait: Option<&crate::CancellationToken>,
) -> Result<(), NativeMountError> {
    if let Some(wait) = wait { wait.check().map_err(|_| detach_work_error(crate::NativeTreeMaterializationError::Cancelled))?; }
    let mut file = lock_recover(&owner.file);
    if owner.delete_on_close.load(std::sync::atomic::Ordering::Acquire) {
        if let Some(original) = file.as_ref() {
            let mut controls = lock_recover(&owner.controls);
            let loan = match wait { Some(wait) => controls.cleanup_loan(wait), None => controls.accepted_cleanup_loan() };
            let mut host = &work.native_materialization_work();
            // ON_CLOSE without DELETE clears the creator's provisional flag.
            set_disposition_ex_through_measured(original, 8, &mut host, Some(&loan))?;
            owner.delete_on_close.store(false, std::sync::atomic::Ordering::Release);
        }
    }
    let file = file.take();
    if let Some(file) = file {
        lock_recover(&owner.close).started_drop_io(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        });
        drop(file);
    }
    Ok(())
}

/// The original SDK instance's own accepted placeholder, not an OS file ID or a version-hash CAS.
#[derive(Clone, Copy)]
struct SdkNamespaceResidue {
    hresult: HRESULT,
    update_causes: PRJ_UPDATE_FAILURE_CAUSES,
}

struct SdkPhysicalEntry {
    context: Mutex<Option<PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT>>,
    name: RetainedBuffer<Vec<u16>>,
    born: std::sync::atomic::AtomicBool,
    effect_uncertain: std::sync::atomic::AtomicBool,
    retired: std::sync::atomic::AtomicBool,
    namespace_residue: Mutex<Option<SdkNamespaceResidue>>,
    cleanup: Mutex<crate::materializer::NativeWorkReservation>,
}
// SAFETY: only the existing drained ProjFS controller calls this original opaque instance; no runtime Arc/backedge or OS path authority is invented.
unsafe impl Send for SdkPhysicalEntry {}
unsafe impl Sync for SdkPhysicalEntry {}

impl SdkPhysicalEntry {
    fn prepare(projection: Projection<'_>, relative: PCWSTR) -> Result<Arc<RetainedArcValue<Self>>, NativeMountError> {
        // SAFETY: the existing scoped SDK spelling is terminated and borrowed.
        let name = unsafe { relative.as_wide() };
        let bytes = (name.len() as u64).checked_add(1).and_then(|units| units.checked_mul(2))
            .ok_or(crate::WorkError::Overflow)?;
        let original = projection.work.native_materialization_work();
        let cleanup = original.allocation_work().reserve_work(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        })?;
        let copied = crate::WorkCounters { bytes_copied: bytes, ..crate::WorkCounters::UNCHARGED };
        original.admit(copied)?;
        let mut allocation = original.claim(bytes)?;
        let mut owned = Vec::new();
        owned.try_reserve_exact(name.len() + 1).map_err(NativeMountError::Allocation)?;
        allocation.reconcile_capacity(owned.capacity() as u64 * 2)?;
        owned.extend_from_slice(name);
        owned.push(0);
        original.record_admitted(copied)?;
        let name = RetainedBuffer {
            value: Some(owned), custody: projection.custody.clone(), bytes: 0,
            native_allocation: Some(allocation), source_ownership: None,
        };
        retained_arc_on_meter(Self {
            context: Mutex::new(Some(projection.context)), name, born: std::sync::atomic::AtomicBool::new(false),
            effect_uncertain: std::sync::atomic::AtomicBool::new(false),
            retired: std::sync::atomic::AtomicBool::new(false), namespace_residue: Mutex::new(None), cleanup: Mutex::new(cleanup),
        }, projection.custody, projection.work)
    }

    fn retire(&self, context: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT, wait: Option<&crate::CancellationToken>)
        -> Result<(), NativeMountError>
    {
        if *lock_recover(&self.context) != Some(context) { return Err(crate::DemandError::StaleSource.into()); }
        if self.retired.load(std::sync::atomic::Ordering::Acquire) { return Ok(()); }
        if let Some(residue) = *lock_recover(&self.namespace_residue) {
            return Err(projection_sdk_io_error(windows::core::Error::from_hresult(residue.hresult)).into());
        }
        if !self.born.load(std::sync::atomic::Ordering::Acquire) {
            if !self.effect_uncertain.load(std::sync::atomic::Ordering::Acquire) { return Ok(()); }
            return Err(NativeMountError::Lifecycle("SDK placeholder effect is uncertain; original instance retained"));
        }
        let mut cleanup = lock_recover(&self.cleanup);
        if cleanup.remaining_work().backend_write_operations == 0 {
            return Err(NativeMountError::Lifecycle("original SDK placeholder delete credit exhausted; instance retained"));
        }
        let loan = match wait {
            Some(wait) => cleanup.cleanup_loan(wait),
            None => cleanup.accepted_cleanup_loan(),
        };
        loan.started_io(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(detach_work_error)?;
        // SAFETY: the sole protected original live SDK instance and owned spelling survive this synchronous call.
        let mut update_causes = PRJ_UPDATE_FAILURE_CAUSES(0);
        let result = unsafe { PrjDeleteFile(context, PCWSTR::from_raw(self.name.as_ptr()), None, Some(&raw mut update_causes)) };
        match result {
            Ok(()) => {
                self.retired.store(true, std::sync::atomic::Ordering::Release);
                Ok(())
            }
            Err(error) if matches!(error.code(), HR_FILE_NOT_FOUND | HR_PATH_NOT_FOUND) => {
                self.retired.store(true, std::sync::atomic::Ordering::Release);
                Ok(())
            }
            Err(error) => {
                *lock_recover(&self.namespace_residue) = Some(SdkNamespaceResidue {
                    hresult: error.code(), update_causes,
                });
                Err(projection_sdk_io_error(error).into())
            }
        }
    }
    /// The synchronous failure is namespace data, not outstanding SDK IO.
    fn retire_for_shutdown(&self, context: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT)
        -> Result<(), NativeMountError>
    {
        if lock_recover(&self.namespace_residue).is_some() { return Ok(()); }
        match self.retire(context, None) {
            Ok(()) => Ok(()),
            Err(NativeMountError::Io(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }
}

#[derive(Clone)]
enum Owned {
    /// A symbolic link to the target with this digest: a link placeholder turns full once read, and `ReFS` takes only ordinary links.
    Link(Arc<RetainedArcValue<SdkPhysicalEntry>>),
    /// An ordinary file the projection wrote, as it left it.
    File(HostWindowsMetadata, Arc<RetainedArcValue<PhysicalEntry>>),
    Directory(Arc<RetainedArcValue<PhysicalEntry>>),
    /// The genuine creation capability before final file metadata/publication.
    ProvisionalFile(Arc<RetainedArcValue<PhysicalEntry>>),
    PhysicalLink(u64, Arc<RetainedArcValue<PhysicalEntry>>),
}

impl PartialEq for Owned {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Link(left), Self::Link(right)) => Arc::ptr_eq(left, right),
            (Self::File(left, a), Self::File(right, b)) =>
                left == right && Arc::ptr_eq(a, b),
            (Self::Directory(a), Self::Directory(b)) => Arc::ptr_eq(a, b),
            (Self::ProvisionalFile(a), Self::ProvisionalFile(b)) => Arc::ptr_eq(a, b),
            (Self::PhysicalLink(left, a), Self::PhysicalLink(right, b)) =>
                left == right && Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}
impl Eq for Owned {}

/// What writing one entry into the projection placed there.
#[derive(Clone, PartialEq, Eq)]
enum Placed {
    /// Nothing: something is there already, or its directory is gone.
    Nothing,
    /// Written with nothing the projection owns in it: a link placeholder, which `ProjFS` manages, or an ordinary directory.
    Unowned,
    /// What `ProjFS` does not manage for the projection.
    Owned(Owned),
    /// Nothing yet: the source changed while its content was read.
    Retry,
}

impl Placed {
    fn owned(&self) -> Option<Owned> {
        match self {
            Self::Owned(owned) => Some(owned.clone()),
            Self::Nothing | Self::Unowned | Self::Retry => None,
        }
    }
}

/// What one path was written from: the node, its metadata, and the content its file holds.
#[derive(Clone, Copy, PartialEq, Eq)]
struct EntryFacts {
    lookup: MountLookup,
    pin: Option<MountContentPin>,
}

impl EntryFacts {
    /// Whether what was written from `self` is also what `current` would write.
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
#[derive(Default)]
struct ProjectionCache {
    bindings: MemoMap<MountPath, ReadOnlyBinding>,
}

impl ProjectionCache {
    fn clear(&mut self) {
        drop(std::mem::take(&mut self.bindings));
    }

    fn remember_binding(
        &mut self, path: &MountPath, binding: ReadOnlyBinding, projection: Projection<'_>,
    ) -> Result<(), NativeMountError> {
        if self.bindings.entries.items.len() >= MAXIMUM_CACHED_BINDINGS { self.clear(); }
        self.bindings.insert(path, binding, projection)?;
        Ok(())
    }
}

/// What the projection wrote at one path, as a source read wrote it.
#[derive(Clone, PartialEq, Eq)]
struct WrittenEntry {
    file_id: FileId,
    /// What is held here that `ProjFS` does not manage; `None` for what it does.
    owned: Option<Owned>,
    /// What it was written from; `None` for one known only to be gone.
    facts: Option<EntryFacts>,
    /// The view it was read in; `None` when the source cannot version it or was moving, which no later change is needed to supersede.
    basis: Option<ReadBasis>,
}

impl WrittenEntry {
    fn superseded(&self, projection: Projection<'_>, path: &MountPath) -> Result<bool, NativeMountError> {
        match self.basis {
            Some(basis) => basis.still_describes(projection, path, Some(self.file_id)).map(|current| !current),
            None => Ok(true),
        }
    }

    fn unconfirmed(&self, projection: Projection<'_>, path: &MountPath) -> Result<bool, NativeMountError> {
        if !self.facts.is_some() { return Ok(false); }
        match self.basis {
            Some(basis) => basis.reported_still_describes(projection, path, Some(self.file_id)),
            None => Ok(false),
        }
    }
}

/// The files a rename through the mount carried unchanged, as the source holds them at their new paths, read in `basis`.
#[derive(Default)]
struct Carried {
    basis: Option<ReadBasis>,
    files: MemoVec<(RetainedBuffer<MountPath>, EntryFacts)>,
}

impl Carried {
    fn read(
        projection: Projection<'_>, from: &MountPath, to: &MountPath,
        current: &[RetainedBuffer<MountPath>],
    ) -> Result<Self, NativeMountError> {
        let basis = ReadBasis::sample(projection)?;
        let source_work = projection.work.clone();
        let mut files = MemoVec::default();
        for path in current {
            examine_memo(projection)?;
            let Some(moved) = owned_rebased(path, from, to, projection)? else { continue; };
            let Some((lookup, pin)) = projection.source.lookup_pinned_with_meter(&moved, &source_work)
                .map_err(detach_source_error)? else { continue; };
            if lookup.node.kind != MountNodeKind::Directory {
                files.push((moved, EntryFacts { lookup, pin }), projection)?;
            }
        }
        Ok(Self { basis, files })
    }
}

/// A directory this provider created as an ordinary directory, and what it wrote into it.
struct Materialized {
    /// The view its listing was read in; `None` when the source cannot version it or was moving.
    basis: Option<ReadBasis>,
    /// Every name written or found here, and whether it is a directory: the base a changed listing is reconciled against.
    names: MemoMap<Vec<u8>, bool>,
}

/// Most threads that write one tree at once.
const MATERIALIZE_WORKERS: usize = 8;

/// A [`Projection`] several workers write through at once.
struct SharedProjection<'a>(Projection<'a>);

// SAFETY: the source is `Sync`, the root a shared path, and the context an opaque token `ProjFS` accepts from any thread for the projection's life, which the scoped workers do not outlive.
unsafe impl Sync for SharedProjection<'_> {}

/// Directories waiting to be written, and how many are being written.
struct MaterializeQueue {
    state: Mutex<MaterializeProgress>,
    changed: Condvar,
}

struct MaterializeProgress {
    pending: MemoVec<RetainedBuffer<MountPath>>,
    writing: usize,
    failure: Option<NativeMountError>,
}

impl MaterializeQueue {
    fn new(pending: MemoVec<RetainedBuffer<MountPath>>) -> Self {
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
    fn next(&self, projection: Projection<'_>) -> Option<RetainedBuffer<MountPath>> {
        let mut state = lock_recover(&self.state);
        loop {
            if state.failure.is_some() { return None; }
            if let Err(error) = projection.work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED) {
                state.failure = Some(error.into());
                self.changed.notify_all();
                return None;
            }
            if let Some(directory) = state.pending.items.pop() {
                state.writing += 1;
                return Some(directory);
            }
            if state.writing == 0 { return None; }
            state = self.changed.wait_timeout(state, HELD_RETRY)
                .unwrap_or_else(std::sync::PoisonError::into_inner).0;
        }
    }

    /// Records how writing one directory went, and what it found.
    fn done(
        &self, written: Result<MemoVec<RetainedBuffer<MountPath>>, NativeMountError>,
        projection: Projection<'_>,
    ) {
        let mut state = lock_recover(&self.state);
        state.writing -= 1;
        match written {
            Ok(mut found) => {
                for directory in found.items.drain(..) {
                    if let Err(error) = state.pending.push(directory, projection) {
                        state.failure.get_or_insert(error);
                        break;
                    }
                }
            }
            Err(error) => { state.failure.get_or_insert(error); }
        }
        self.changed.notify_all();
    }

    fn finish(self) -> Result<(), NativeMountError> {
        self.state
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .failure
            .map_or(Ok(()), Err)
    }
}

/// Where this provider writes: the source it projects, its live context, and the projection's root on the host.
#[derive(Clone, Copy)]
struct Projection<'a> {
    source: &'a dyn MountFilesystem,
    /// This round's original full invocation, never a second handler ledger.
    work: &'a super::NativeMountMeterWork,
    /// Physical owners may retain this proof, but never an original meter alias, after the scoped queue has returned the original invocation.
    custody: &'a crate::demand::NativeMountAllocationCustody,
    context: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT,
    root: &'a std::path::Path,
    root_identity: crate::NativeRootIdentity,
    staging_custody: &'a Mutex<Option<StagingDirectoryCustody>>,
    /// Host edits not yet captured: a file holding one is the user's until the source has it.
    captures: &'a Mutex<PostOperationFailures>,
    /// Whether the mount is still starting, which nothing can read yet: a file is written at its name directly, with no staged name first.
    starting: bool,
}

// SAFETY: the SDK context is an opaque thread-independent token.
unsafe impl Send for Projection<'_> {}
unsafe impl Sync for Projection<'_> {}

/// The existing memo's backing, with portable exact growth and physical-drop evidence.
struct MemoVec<T> {
    items: Vec<T>,
    native_allocation: Option<crate::NativeAllocation>,
}

impl<T> Default for MemoVec<T> {
    fn default() -> Self { Self { items: Vec::new(), native_allocation: None } }
}

impl<T> MemoVec<T> {
    fn reserve_one(&mut self, projection: Projection<'_>) -> Result<(), NativeMountError> {
        let needed = self.items.len().checked_add(1).ok_or(crate::WorkError::Overflow)?;
        let count = if needed > self.items.capacity() {
            self.items.capacity().checked_mul(2).ok_or(crate::WorkError::Overflow)?.max(needed)
        } else { needed };
        self.reserve_for(count, projection)
    }

    fn reserve_for(&mut self, count: usize, projection: Projection<'_>) -> Result<(), NativeMountError> {
        if count <= self.items.capacity() { return Ok(()); }
        let bytes = count.checked_mul(size_of::<T>()).ok_or(crate::WorkError::Overflow)? as u64;
        let copied = crate::WorkCounters {
            bytes_copied: (self.items.len() * size_of::<T>()) as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        let work = projection.work.native_materialization_work();
        work.admit(copied)?;
        let mut native_allocation = projection.work.native_materialization_work().claim(bytes)?;
        let mut replacement = Vec::new();
        if let Err(error) = replacement.try_reserve_exact(count) {
            drop(replacement);
            return Err(NativeMountError::Allocation(error));
        }
        native_allocation.reconcile_capacity((replacement.capacity() * size_of::<T>()) as u64)?;
        if let Err(error) = work.admit(copied) {
            drop(replacement);
            return Err(error.into());
        }
        replacement.append(&mut self.items);
        let old = std::mem::replace(&mut self.items, replacement);
        drop(old);
        drop(self.native_allocation.take());
        self.native_allocation = Some(native_allocation);
        work.record_admitted(copied)?;
        Ok(())
    }

    fn push(&mut self, item: T, projection: Projection<'_>) -> Result<(), NativeMountError> {
        self.reserve_one(projection)?;
        self.items.push(item);
        Ok(())
    }

    fn insert(&mut self, index: usize, item: T, projection: Projection<'_>) -> Result<(), NativeMountError> {
        self.reserve_one(projection)?;
        let copied = crate::WorkCounters {
            bytes_copied: ((self.items.len() - index) * size_of::<T>()) as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        projection.work.native_materialization_work().charge(copied).map_err(detach_work_error)?;
        self.items.insert(index, item);
        Ok(())
    }

    fn remove(&mut self, index: usize, projection: Projection<'_>) -> Result<T, NativeMountError> {
        projection.work.native_materialization_work().charge(crate::WorkCounters {
            bytes_copied: ((self.items.len() - index - 1) * size_of::<T>()) as u64,
            ..crate::WorkCounters::UNCHARGED
        })?;
        Ok(self.items.remove(index))
    }

}

impl<T> Drop for MemoVec<T> {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.items));
    }
}

trait MemoKey {
    fn compare(&self, other: &Self) -> std::cmp::Ordering;
    fn copy_owned(&self, projection: Projection<'_>) -> Result<RetainedBuffer<Self>, NativeMountError>
    where Self: Sized;
}

impl MemoKey for MountPath {
    fn compare(&self, other: &Self) -> std::cmp::Ordering { self.cmp(other) }
    fn copy_owned(&self, projection: Projection<'_>) -> Result<RetainedBuffer<Self>, NativeMountError> {
        owned_components(self.components().len(), self.components().iter(), projection)
    }
}

impl MemoKey for Vec<u8> {
    fn compare(&self, other: &Self) -> std::cmp::Ordering { self.cmp(other) }
    fn copy_owned(&self, projection: Projection<'_>) -> Result<RetainedBuffer<Self>, NativeMountError> {
        let bytes = self.len() as u64;
        let copied = crate::WorkCounters { bytes_copied: bytes, ..crate::WorkCounters::UNCHARGED };
        let work = projection.work.native_materialization_work();
        work.admit(copied)?;
        let mut native_allocation = projection.work.native_materialization_work().claim(bytes)?;
        let mut value = Vec::new();
        if let Err(error) = value.try_reserve_exact(self.len()) {
            drop(value);
            return Err(NativeMountError::Allocation(error));
        }
        native_allocation.reconcile_capacity(value.capacity() as u64)?;
        value.extend_from_slice(self);
        let value = RetainedBuffer { value: Some(value), custody: projection.custody.clone(), bytes: 0, native_allocation: Some(native_allocation), source_ownership: None };
        work.record_admitted(copied)?;
        Ok(value)
    }
}

impl MemoKey for u128 {
    fn compare(&self, other: &Self) -> std::cmp::Ordering { self.cmp(other) }
    fn copy_owned(&self, projection: Projection<'_>) -> Result<RetainedBuffer<Self>, NativeMountError> {
        Ok(RetainedBuffer { value: Some(*self), custody: projection.custody.clone(), bytes: 0, native_allocation: None, source_ownership: None })
    }
}

impl MemoKey for crate::NativeRootIdentity {
    fn compare(&self, other: &Self) -> std::cmp::Ordering {
        (self.device, self.object).cmp(&(other.device, other.object))
    }
    fn copy_owned(&self, projection: Projection<'_>) -> Result<RetainedBuffer<Self>, NativeMountError> {
        Ok(RetainedBuffer { value: Some(*self), custody: projection.custody.clone(), bytes: 0, native_allocation: None, source_ownership: None })
    }
}

struct MemoMap<K, V> { entries: MemoVec<(RetainedBuffer<K>, V)> }

impl<K, V> Default for MemoMap<K, V> {
    fn default() -> Self { Self { entries: MemoVec::default() } }
}

impl<K: MemoKey, V> MemoMap<K, V> {
    fn index(&self, key: &K, work: &super::NativeMountMeterWork)
        -> Result<Result<usize, usize>, NativeMountError>
    {
        let (mut left, mut right) = (0, self.entries.items.len());
        while left < right {
            work.native_materialization_work().charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            })?;
            let middle = left + (right - left) / 2;
            match self.entries.items[middle].0.compare(key) {
                std::cmp::Ordering::Less => left = middle + 1,
                std::cmp::Ordering::Greater => right = middle,
                std::cmp::Ordering::Equal => return Ok(Ok(middle)),
            }
        }
        Ok(Err(left))
    }


    fn get(&self, key: &K, projection: Projection<'_>) -> Result<Option<&V>, NativeMountError> {
        Ok(self.index(key, projection.work)?.ok().map(|index| &self.entries.items[index].1))
    }

    fn get_mut(&mut self, key: &K, projection: Projection<'_>) -> Result<Option<&mut V>, NativeMountError> {
        Ok(self.index(key, projection.work)?.ok().map(|index| &mut self.entries.items[index].1))
    }



    fn insert(&mut self, key: &K, value: V, projection: Projection<'_>) -> Result<Option<V>, NativeMountError> {
        match self.index(key, projection.work)? {
            Ok(index) => Ok(Some(std::mem::replace(&mut self.entries.items[index].1, value))),
            Err(index) => {
                let key = key.copy_owned(projection)?;
                self.entries.insert(index, (key, value), projection)?;
                Ok(None)
            }
        }
    }

    fn insert_owned(
        &mut self, key: RetainedBuffer<K>, value: V, projection: Projection<'_>,
    ) -> Result<Option<V>, NativeMountError> {
        self.insert_owned_get(key, value, projection).map(|(_, old)| old)
    }

    fn insert_owned_get(
        &mut self, key: RetainedBuffer<K>, value: V, projection: Projection<'_>,
    ) -> Result<(&K, Option<V>), NativeMountError> {
        match self.index(&key, projection.work)? {
            Ok(index) => {
                let old = std::mem::replace(&mut self.entries.items[index].1, value);
                Ok((&self.entries.items[index].0, Some(old)))
            }
            Err(index) => {
                self.entries.insert(index, (key, value), projection)?;
                Ok((&self.entries.items[index].0, None))
            }
        }
    }

    /// Changes one existing key without losing its value on a budget denial.
    fn rekey(
        &mut self, source: &K, destination: RetainedBuffer<K>, projection: Projection<'_>,
        merge: impl FnOnce(&mut V, V),
    ) -> Result<(), NativeMountError> {
        let Ok(source_index) = self.index(source, projection.work)? else { return Ok(()); };
        let destination_index = self.index(&destination, projection.work)?;
        if destination_index == Ok(source_index) { return Ok(()); }
        let len = self.entries.items.len();
        let removal = len - source_index - 1;
        let insertion = match destination_index {
            Ok(_) => 0,
            Err(index) => len - 1 - (index - usize::from(source_index < index)),
        };
        let bytes = removal.checked_add(insertion)
            .and_then(|count| count.checked_mul(size_of::<(RetainedBuffer<K>, V)>()))
            .ok_or(crate::WorkError::Overflow)? as u64;
        projection.work.native_materialization_work().charge(crate::WorkCounters {
            bytes_copied: bytes, ..crate::WorkCounters::UNCHARGED
        }).map_err(detach_work_error)?;
        let (old, value) = self.entries.items.remove(source_index);
        drop(old);
        match destination_index {
            Ok(index) => {
                let index = index - usize::from(source_index < index);
                merge(&mut self.entries.items[index].1, value);
            }
            Err(index) => {
                let index = index - usize::from(source_index < index);
                self.entries.items.insert(index, (destination, value));
            }
        }
        Ok(())
    }

    fn remove(&mut self, key: &K, projection: Projection<'_>) -> Result<Option<V>, NativeMountError> {
        match self.index(key, projection.work)? {
            Ok(index) => {
                let (key, value) = self.entries.remove(index, projection)?;
                drop(key);
                Ok(Some(value))
            }
            Err(_) => Ok(None),
        }
    }


    fn remove_entry(
        &mut self, key: &K, projection: Projection<'_>,
    ) -> Result<Option<(RetainedBuffer<K>, V)>, NativeMountError> {
        match self.index(key, projection.work)? {
            Ok(index) => self.entries.remove(index, projection).map(Some),
            Err(_) => Ok(None),
        }
    }

    fn contains_key(&self, key: &K, projection: Projection<'_>) -> Result<bool, NativeMountError> {
        Ok(self.index(key, projection.work)?.is_ok())
    }

    fn iter(&self) -> impl DoubleEndedIterator<Item = (&K, &V)> + ExactSizeIterator {
        self.entries.items.iter().map(|(key, value)| (&**key, value))
    }

    fn keys(&self) -> impl DoubleEndedIterator<Item = &K> + ExactSizeIterator { self.iter().map(|(key, _)| key) }
    fn is_empty(&self) -> bool { self.entries.items.is_empty() }
}

impl<K: MemoKey, V: Clone> MemoMap<K, V> {
    /// A snapshot is needed while native calls may reenter and edit the memo.
    fn copy_owned(&self, projection: Projection<'_>) -> Result<Self, NativeMountError> {
        let mut copied = Self::default();
        copied.entries.reserve_for(self.entries.items.len(), projection)?;
        for (key, value) in self.iter() {
            examine_memo(projection)?;
            copied.entries.items.push((key.copy_owned(projection)?, value.clone()));
        }
        Ok(copied)
    }
}

fn owned_components<'a>(
    count: usize, components: impl IntoIterator<Item = &'a Vec<u8>>, projection: Projection<'_>,
) -> Result<RetainedBuffer<MountPath>, NativeMountError> {
    let outer = count.checked_mul(size_of::<Vec<u8>>()).ok_or(crate::WorkError::Overflow)? as u64;
    let work = projection.work.native_materialization_work();
    let mut native_allocation = projection.work.native_materialization_work().claim(outer)?;
    let mut copied = Vec::new();
    let answer = (|| {
        copied.try_reserve_exact(count).map_err(NativeMountError::Allocation)?;
        native_allocation.reconcile_capacity((copied.capacity() * size_of::<Vec<u8>>()) as u64)?;
        for component in components {
            let delta = crate::WorkCounters {
                bytes_copied: component.len() as u64, items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            };
            work.admit(delta)?;
            let mut component_allocation = projection.work.native_materialization_work().claim(component.len() as u64)?;
            let mut value = Vec::new();
            value.try_reserve_exact(component.len()).map_err(NativeMountError::Allocation)?;
            component_allocation.reconcile_capacity(value.capacity() as u64)?;
            native_allocation.absorb(&mut component_allocation)?;
            value.extend_from_slice(component);
            copied.push(value);
            work.record_admitted(delta)?;
        }
        Ok(())
    })();
    if let Err(error) = answer {
        drop(copied);
        return Err(error);
    }
    Ok(RetainedBuffer { value: Some(MountPath { components: copied }), bytes: 0, custody: projection.custody.clone(), native_allocation: Some(native_allocation), source_ownership: None })
}

fn owned_parent(
    path: &MountPath, projection: Projection<'_>,
) -> Result<Option<RetainedBuffer<MountPath>>, NativeMountError> {
    let Some(last) = path.components().len().checked_sub(1) else { return Ok(None); };
    owned_components(last, path.components()[..last].iter(), projection).map(Some)
}

fn owned_child(
    parent: &MountPath, component: &Vec<u8>, projection: Projection<'_>,
) -> Result<RetainedBuffer<MountPath>, NativeMountError> {
    let count = parent.components().len().checked_add(1).ok_or(crate::WorkError::Overflow)?;
    owned_components(count, parent.components().iter().chain(std::iter::once(component)), projection)
}

fn owned_rebased(
    path: &MountPath, from: &MountPath, to: &MountPath, projection: Projection<'_>,
) -> Result<Option<RetainedBuffer<MountPath>>, NativeMountError> {
    let Some(suffix) = projfs_path_suffix_with_work(path, from, projection.work)? else { return Ok(None); };
    let count = to.components().len().checked_add(suffix.len()).ok_or(crate::WorkError::Overflow)?;
    owned_components(count, to.components().iter().chain(suffix), projection).map(Some)
}

fn examine_memo(projection: Projection<'_>) -> Result<(), NativeMountError> {
    projection.work.native_materialization_work().charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED }).map_err(detach_work_error)
}

fn rebase_memo<V>(
    memo: &mut MemoMap<MountPath, V>, from: &MountPath, to: &MountPath, projection: Projection<'_>,
) -> Result<(), NativeMountError> {
    let mut moved = MemoVec::default();
    for path in memo.keys() {
        examine_memo(projection)?;
        if let Some(destination) = owned_rebased(path, from, to, projection)? {
            moved.push((path.copy_owned(projection)?, destination), projection)?;
        }
    }
    for (source, destination) in moved.items.drain(..) {
        memo.rekey(&source, destination, projection, |existing, incoming| *existing = incoming)?;
    }
    Ok(())
}

/// The projection this provider writes, and the worker that keeps it current.
struct Projector {
    state: Mutex<ProjectorState>,
    /// Observer callbacks never take the memo lock while a source holds the original meter.
    notification: Mutex<Option<ViewStamp>>,
    /// Held by whatever writes directories into the projection, so a listing is written and recorded as one step.
    tree: Mutex<()>,
    /// Held for a path while what is written there is replaced or removed.
    paths: KeyedLocks<MountPath>,
    changed: Condvar,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
    budget: crate::WorkBudget,
    cancellation: crate::CancellationToken,
    custody: crate::demand::NativeMountAllocationCustody,
    scoped_work: Mutex<super::NativeMountMeterActivation>,
    scope_gate: Mutex<()>,
    active_rounds: std::sync::atomic::AtomicUsize,
}

/// Owns the preexisting physical runtime across a mounted host operation.
pub(super) struct ProjectionWorkScope {
    runtime: Arc<RetainedArcValue<Runtime>>,
    meter: super::NativeMountMeterWork,
    projector_generation: u64,
    runtime_generation: u64,
}

impl ProjectionWorkScope {
    /// Moves an independent same-engine activation into the actual queued native-I/O closure, keeping SDK callbacks scoped until it terminates.
    pub(super) fn try_clone_for_io(&self) -> Result<Self, NativeMountError> {
        let (projector_generation, runtime_generation) =
            enter_projection_work(&self.runtime.projector, &self.runtime, &self.meter)?;
        Ok(Self {
            runtime: self.runtime.clone(), meter: self.meter.clone(),
            projector_generation, runtime_generation,
        })
    }
}

/// Internal synchronous projection of the same activation metadata.
struct ProjectionWorkLease<'a> {
    projector: &'a Projector,
    runtime: &'a Runtime,
    meter: super::NativeMountMeterWork,
    projector_generation: u64,
    runtime_generation: u64,
}

fn enter_projection_work(
    projector: &Projector, runtime: &Runtime,
    meter: &super::NativeMountMeterWork,
) -> Result<(u64, u64), NativeMountError> {
    meter.native_materialization_work().admit(crate::WorkCounters::UNCHARGED).map_err(detach_work_error)?;
    let mut projected = lock_recover(&projector.scoped_work);
    let mut callbacks = lock_recover(&runtime.stop_meter);
    let projector_generation = projected.enter(meter)?;
    match callbacks.enter(meter) {
        Ok(runtime_generation) => Ok((projector_generation, runtime_generation)),
        Err(error) => {
            projected.leave(meter, projector_generation);
            Err(error)
        }
    }
}

fn leave_projection_work(
    projector: &Projector, runtime: &Runtime,
    meter: &super::NativeMountMeterWork,
    projector_generation: u64, runtime_generation: u64,
) {
    let final_projection = {
        let mut projected = lock_recover(&projector.scoped_work);
        projected.leave(meter, projector_generation);
        projected.upgrade().is_none()
    };
    if final_projection {
        while projector.active_rounds.load(Ordering::Acquire) != 0 {
            std::thread::sleep(HELD_RETRY);
        }
    }
    lock_recover(&runtime.stop_meter).leave(meter, runtime_generation);
}

impl<'a> ProjectionWorkLease<'a> {
    fn enter(
        projector: &'a Projector, runtime: &'a Runtime,
        meter: &super::NativeMountMeterWork,
    ) -> Result<Self, NativeMountError> {
        let (projector_generation, runtime_generation) = enter_projection_work(projector, runtime, meter)?;
        Ok(Self { projector, runtime, meter: meter.clone(), projector_generation, runtime_generation })
    }
}

impl Drop for ProjectionWorkLease<'_> {
    fn drop(&mut self) {
        leave_projection_work(self.projector, self.runtime, &self.meter,
            self.projector_generation, self.runtime_generation);
    }
}

impl Drop for ProjectionWorkScope {
    fn drop(&mut self) {
        leave_projection_work(&self.runtime.projector, &self.runtime, &self.meter,
            self.projector_generation, self.runtime_generation);
    }
}

struct ProjectorRound<'a>(&'a Projector);
impl Drop for ProjectorRound<'_> {
    fn drop(&mut self) {
        self.0.active_rounds.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_all();
    }
}

/// How often a path held open is tried again.
const HELD_RETRY: std::time::Duration = std::time::Duration::from_millis(20);
/// How long revalidation waits for paths held open, once every change has been served.
const HELD_SETTLE_LIMIT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Default)]
struct ProjectorState {
    written: MemoMap<MountPath, WrittenEntry>,
    /// Superseded paths not yet replaced, released, or failed.
    pending: MemoMap<MountPath, WrittenEntry>,
    /// Every directory created in the projection.
    directories: MemoMap<MountPath, Materialized>,
    /// Directories to list again whatever their basis says.
    stale: MemoMap<MountPath, ()>,
    /// Pending paths a listing waits on: when one is settled, its parent is listed again.
    relist: MemoMap<MountPath, ()>,
    /// Directories the source removed that still held a file another process had open: removed once nothing beneath them is pending.
    vacated: MemoMap<MountPath, ()>,
    /// Whether the worker's pass in progress took stale directories it has not listed yet.
    relisting: bool,
    /// Whether a handle closed since pending paths were last tried.
    retry: bool,
    /// Latest change around the mount the source reported.
    notified: Option<ViewStamp>,
    /// Latest change everything written was checked against.
    processed: Option<ViewStamp>,
    /// First path the worker could not replace since the last revalidation.
    failure: Option<NativeMountError>,
    stopping: bool,
}

/// Only the already-drained sole producer borrows this inline state.
struct RetiredProjectorState<'a> {
    slot: &'a Mutex<ProjectorState>,
    state: Option<ProjectorState>,
}
impl<'a> RetiredProjectorState<'a> {
    fn take(slot: &'a Mutex<ProjectorState>) -> Self {
        let state = std::mem::take(&mut *lock_recover(slot));
        Self { slot, state: Some(state) }
    }
}
impl Drop for RetiredProjectorState<'_> {
    fn drop(&mut self) {
        *lock_recover(self.slot) = self.state.take().expect("original retired projector state");
    }
}

impl ProjectorState {
    /// Updates the original memo only after actual placement/release.
    fn record_attempt(
        &mut self, path: &MountPath, attempted: &WrittenEntry,
        current: Option<WrittenEntry>, projection: Projection<'_>,
    ) -> Result<(), NativeMountError> {
        if let Some(current) = current {
            match self.written.get_mut(path, projection)? {
                Some(entry) => {
                    if entry.basis.map(|basis| basis.stamp) < current.basis.map(|basis| basis.stamp) {
                        *entry = current;
                    }
                }
                None => { self.written.insert(path, current, projection)?; }
            }
        }
        if self.pending.get(path, projection)? == Some(attempted) {
            if self.relist.contains_key(path, projection)?
                && let Some(parent) = owned_parent(path, projection)?
            {
                self.stale.insert(&parent, (), projection)?;
            }
            self.relist.remove(path, projection)?;
            self.pending.remove(path, projection)?;
        }
        Ok(())
    }
}

impl ViewObserver for Projector {
    fn view_changed(&self, position: ViewStamp, _origin: ViewOrigin) {
        let mut notification = lock_recover(&self.notification);
        *notification = (*notification).max(Some(position));
        self.changed.notify_all();
    }

    fn allocation_custody(&self) -> Option<crate::demand::NativeMountAllocationCustody> {
        Some(self.custody.clone())
    }
}

/// What replacing one superseded path came to.
enum Replaced {
    /// Written again from the source.
    Rewritten(WrittenEntry),
    /// Deleted, or no longer the projection's.
    Released,
    /// Open elsewhere; pending until it can be replaced.
    Busy,
}


impl Projector {
    #[cfg(test)]
    fn new() -> Self {
        let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .expect("one original fixture invocation");
        let custody = crate::demand::NativeMountAllocationCustody::new_with_meter(&work.native_mount_work())
            .expect("unbounded fixture custody");
        Self::new_with_work(&custody, &work.native_read_meter().lock())
    }

    fn new_with_work(custody: &crate::demand::NativeMountAllocationCustody, work: &DemandWork) -> Self {
        Self {
            state: Mutex::new(ProjectorState::default()),
            notification: Mutex::new(None),
            tree: Mutex::new(()),
            paths: KeyedLocks::default(),
            changed: Condvar::new(),
            worker: Mutex::new(None),
            budget: work.budget,
            cancellation: work.cancellation().clone(),
            custody: custody.clone(),
            scoped_work: Mutex::new(super::NativeMountMeterActivation::default()),
            scope_gate: Mutex::new(()),
            active_rounds: std::sync::atomic::AtomicUsize::new(0),
        }
    }


    fn with_control_meter<T>(
        &self, runtime: &Runtime, meter: &super::NativeMountMeterWork,
        operation: impl FnOnce(&super::NativeMountMeterWork) -> Result<T, NativeMountError>,
    ) -> Result<T, NativeMountError> {
        let _gate = loop {
            meter.native_materialization_work().admit(crate::WorkCounters::UNCHARGED).map_err(detach_work_error)?;
            match self.scope_gate.try_lock() {
                Ok(gate) => break gate,
                Err(std::sync::TryLockError::Poisoned(error)) => break error.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => std::thread::sleep(HELD_RETRY),
            }
        };
        self.with_control_meter_locked(runtime, meter, operation)
    }

    fn with_control_work_locked<T>(
        &self, runtime: &Runtime, work: &crate::NativeMaterializationWork,
        operation: impl FnOnce(&super::NativeMountMeterWork) -> Result<T, NativeMountError>,
    ) -> Result<T, NativeMountError> {
        self.with_control_meter_locked(runtime, &work.native_mount_work(), operation)
    }

    fn with_control_meter_locked<T>(
        &self, runtime: &Runtime, meter: &super::NativeMountMeterWork,
        operation: impl FnOnce(&super::NativeMountMeterWork) -> Result<T, NativeMountError>,
    ) -> Result<T, NativeMountError> {
        let _scope = ProjectionWorkLease::enter(self, runtime, meter)?;
        operation(meter)
    }

    fn with_projection_work<T>(
        &self, runtime: &Runtime, context: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT,
        starting: bool, work: &crate::NativeMaterializationWork,
        operation: impl FnOnce(Projection<'_>) -> Result<T, NativeMountError>,
    ) -> Result<T, NativeMountError> {
        self.with_projection_meter(runtime, context, starting, &work.native_mount_work(), operation)
    }

    fn with_projection_meter<T>(
        &self, runtime: &Runtime, context: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT,
        starting: bool, meter: &super::NativeMountMeterWork,
        operation: impl FnOnce(Projection<'_>) -> Result<T, NativeMountError>,
    ) -> Result<T, NativeMountError> {
        self.with_control_meter(runtime, meter, |meter| {
            let custody = crate::demand::NativeMountAllocationCustody::new_with_meter(meter)?;
            operation(Projection {
                source: runtime.source.as_ref(), work: meter, custody: &custody, context,
                root: &runtime.root, root_identity: runtime.metadata_root.identity(),
                staging_custody: &runtime.staging_custody,
                captures: &runtime.post_operation_failure, starting,
            })
        })
    }

    /// Keeps only an actually placed entry in the original projection memo.
    fn written(
        &self, path: &MountPath, facts: EntryFacts, owned: Option<Owned>,
        basis: Option<ReadBasis>, projection: Projection<'_>,
    ) -> Result<(), NativeMountError> {
        lock_recover(&self.state).written.insert(path, WrittenEntry {
            file_id: facts.lookup.node.file_id, owned, facts: Some(facts), basis,
        }, projection)?;
        Ok(())
    }

    fn lock_tree(&self, projection: Projection<'_>) -> Result<MutexGuard<'_, ()>, NativeMountError> {
        loop {
            projection.work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED).map_err(detach_work_error)?;
            match self.tree.try_lock() {
                Ok(held) => return Ok(held),
                Err(std::sync::TryLockError::Poisoned(error)) => return Ok(error.into_inner()),
                Err(std::sync::TryLockError::WouldBlock) => std::thread::sleep(HELD_RETRY),
            }
        }
    }

    /// Runs the existing parallel materializer, borrowing one original meter.
    fn materialize(&self, projection: Projection<'_>, path: &MountPath) -> Result<(), NativeMountError> {
        let _tree = self.lock_tree(projection)?;
        let mut directories = MemoVec::default();
        directories.push(path.copy_owned(projection)?, projection)?;
        self.materialize_locked(projection, directories)
    }

    fn materialize_locked(
        &self, projection: Projection<'_>, directories: MemoVec<RetainedBuffer<MountPath>>,
    ) -> Result<(), NativeMountError> {
        if directories.items.is_empty() { return Ok(()); }
        let workers = projection_native_call(projection.work, false, || Ok(
            std::thread::available_parallelism().map_or(1, usize::from).min(MATERIALIZE_WORKERS)
        ))?;
        let queue = MaterializeQueue::new(directories);
        let shared = SharedProjection(projection);
        std::thread::scope(|scope| {
            for _ in 1..workers {
                let spawned = projection_native_call(projection.work, true, || {
                    std::thread::Builder::new().spawn_scoped(scope, || self.materialize_from(&shared, &queue))
                });
                if let Err(error) = spawned {
                    lock_recover(&queue.state).failure.get_or_insert(error);
                    queue.changed.notify_all();
                    break;
                }
            }
            self.materialize_from(&shared, &queue);
        });
        queue.finish()
    }

    fn materialize_from(&self, projection: &SharedProjection<'_>, queue: &MaterializeQueue) {
        while let Some(directory) = queue.next(projection.0) {
            let written = self.materialize_directory(projection.0, directory);
            queue.done(written, projection.0);
        }
    }

    fn materialize_directory(
        &self, projection: Projection<'_>, directory: RetainedBuffer<MountPath>,
    ) -> Result<MemoVec<RetainedBuffer<MountPath>>, NativeMountError> {
        let basis = ReadBasis::sample(projection)?;
        let mut entries = match source_listing(projection, &directory) {
            Ok(entries) => entries,
            Err(NativeMountError::Source(MountSourceError::NotFound)) => return Ok(MemoVec::default()),
            Err(NativeMountError::Source(MountSourceError::Stale)) => {
                let mut state = lock_recover(&self.state);
                state.stale.insert(&directory, (), projection)?;
                state.directories.insert_owned(directory, Materialized {
                    basis: None, names: MemoMap::default(),
                }, projection)?;
                self.changed.notify_all();
                return Ok(MemoVec::default());
            }
            Err(error) => return Err(error),
        };
        let mut names = MemoMap::default();
        let mut found = MemoVec::default();
        for entry in entries.iter_mut() {
            examine_memo(projection)?;
            let child = owned_child(&directory, &entry.component, projection)?;
            let is_directory = entry.info.IsDirectory;
            let mut committed = None;
            let placed = write_entry(projection, &child, entry, &mut committed, &mut |owner|
                self.written(&child, entry.facts, Some(owner.clone()), basis, projection));
            if let Some(owned) = committed.as_ref() {
                self.written(&child, entry.facts, Some(owned.clone()), basis, projection)?;
            }
            let placed = placed?;
            if placed == Placed::Retry {
                lock_recover(&self.state).stale.insert(&directory, (), projection)?;
                self.changed.notify_all();
                continue;
            }
            if is_directory {
                found.push(child, projection)?;
            } else if placed != Placed::Nothing && committed.is_none() {
                self.written(&child, entry.facts, placed.owned(), basis, projection)?;
            }
            names.insert_owned(entry.take_component(), is_directory, projection)?;
        }
        lock_recover(&self.state).directories.insert_owned(
            directory, Materialized { basis, names }, projection,
        )?;
        Ok(found)
    }

    /// Lists `directory` again and reconciles what it holds with the names it was last written with (see [`Projector`]).
    fn reconcile(&self, projection: Projection<'_>, directory: &MountPath) -> Result<(), NativeMountError> {
        let _tree = self.lock_tree(projection)?;
        let known = {
            let state = lock_recover(&self.state);
            let Some(materialized) = state.directories.get(directory, projection)? else { return Ok(()); };
            materialized.names.copy_owned(projection)?
        };
        let basis = ReadBasis::sample(projection)?;
        let mut entries = match source_listing(projection, directory) {
            Ok(entries) => entries,
            Err(NativeMountError::Source(MountSourceError::NotFound)) => {
                return self.remove_tree(projection, directory);
            }
            Err(NativeMountError::Source(MountSourceError::Stale)) => {
                lock_recover(&self.state).stale.insert(directory, (), projection)?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let mut names = MemoMap::default();
        names.entries.reserve_for(entries.len(), projection)?;
        let mut created = MemoVec::default();
        for entry in entries.iter_mut() {
            examine_memo(projection)?;
            let is_directory = entry.info.IsDirectory;
            let child = owned_child(directory, &entry.component, projection)?;
            let written_before = match known.get(&entry.component, projection)?.copied() {
                Some(kind) => Some(kind),
                None => lock_recover(&self.state).pending.contains_key(&child, projection)?.then_some(false),
            };
            match written_before {
                Some(kind) if kind == is_directory => {
                    names.insert_owned(entry.take_component(), is_directory, projection)?;
                    continue;
                }
                Some(true) => self.remove_tree(projection, &child)?,
                Some(false) => {
                    let (owned, released) = self.release_recorded(projection, &child)?;
                    if released == Release::Busy {
                        let mut state = lock_recover(&self.state);
                        state.relist.insert(&child, (), projection)?;
                        self.make_pending(&mut state, &child, owned, projection)?;
                        return Ok(());
                    }
                }
                None => {}
            }
            let mut committed = None;
            let placed = write_entry(projection, &child, entry, &mut committed, &mut |owner|
                self.written(&child, entry.facts, Some(owner.clone()), basis, projection));
            if let Some(owned) = committed.as_ref() {
                self.written(&child, entry.facts, Some(owned.clone()), basis, projection)?;
            }
            let placed = placed?;
            if placed == Placed::Retry {
                lock_recover(&self.state).stale.insert(directory, (), projection)?;
                continue;
            }
            if is_directory { created.push(child, projection)?; }
            else if placed != Placed::Nothing && committed.is_none() {
                self.written(&child, entry.facts, placed.owned(), basis, projection)?;
            }
            names.insert_owned(entry.take_component(), is_directory, projection)?;
        }
        for (name, was_directory) in known.iter() {
            examine_memo(projection)?;
            if names.contains_key(name, projection)? { continue; }
            let child = owned_child(directory, name, projection)?;
            if *was_directory { self.remove_tree(projection, &child)?; }
            else {
                self.make_pending(&mut lock_recover(&self.state), &child, None, projection)?;
            }
        }
        lock_recover(&self.state).directories.insert(directory, Materialized { basis, names }, projection)?;
        self.materialize_locked(projection, created)
    }

    fn make_pending(
        &self, state: &mut ProjectorState, path: &MountPath,
        owned: Option<Owned>, projection: Projection<'_>,
    ) -> Result<(), NativeMountError> {
        if state.pending.contains_key(path, projection)? { return Ok(()); }
        let written = state.written.get(path, projection)?.cloned().unwrap_or(WrittenEntry {
            file_id: FileId::from_bytes([0; 16]), owned, facts: None, basis: None,
        });
        state.pending.insert(path, written, projection)?;
        state.written.remove(path, projection)?;
        Ok(())
    }

    /// Releases only the recorded object under the same per-path exclusion.
    fn release_recorded(
        &self, projection: Projection<'_>, path: &MountPath,
    ) -> Result<(Option<Owned>, Release), NativeMountError> {
        self.paths.with(path, projection, || {
            let owned = {
                let state = lock_recover(&self.state);
                match state.written.get(path, projection)? {
                    Some(written) => written.owned.clone(),
                    None => state.pending.get(path, projection)?.and_then(|written| written.owned.clone()),
                }
            };
            let released = release(projection, path, owned.clone())?;
            if released == Release::Done {
                let mut state = lock_recover(&self.state);
                state.written.remove(path, projection)?;
                state.pending.remove(path, projection)?;
                state.relist.remove(path, projection)?;
            }
            Ok((owned, released))
        })
    }

    /// Descendants precede ancestors in reverse sorted-key order.
    fn remove_tree(&self, projection: Projection<'_>, directory: &MountPath) -> Result<(), NativeMountError> {
        let paths = {
            let state = lock_recover(&self.state);
            let mut paths = MemoVec::default();
            for path in state.directories.keys() {
                examine_memo(projection)?;
                if projfs_path_suffix_with_work(path, directory, projection.work)?.is_some() {
                    paths.push(path.copy_owned(projection)?, projection)?;
                }
            }
            paths
        };
        let mut held = false;
        for path in paths.items.iter().rev() {
            let names = {
                let state = lock_recover(&self.state);
                let Some(materialized) = state.directories.get(path, projection)? else { continue; };
                materialized.names.copy_owned(projection)?
            };
            for (name, is_directory) in names.iter() {
                examine_memo(projection)?;
                if *is_directory { continue; }
                let child = owned_child(path, name, projection)?;
                let (owned, released) = self.release_recorded(projection, &child)?;
                if released == Release::Busy {
                    held = true;
                    self.make_pending(&mut lock_recover(&self.state), &child, owned, projection)?;
                }
            }
            if held { lock_recover(&self.state).vacated.insert(path, (), projection)?; }
            else if !self.retire_vacated(projection, path)? {
                held = true;
                lock_recover(&self.state).vacated.insert(path, (), projection)?;
            }
            lock_recover(&self.state).directories.remove(path, projection)?;
        }
        Ok(())
    }

    fn retire_vacated(&self, projection: Projection<'_>, path: &MountPath) -> Result<bool, NativeMountError> {
        let owned = lock_recover(&self.state).written.get(path, projection)?
            .and_then(|written| written.owned.as_ref()).cloned();
        if matches!(owned, Some(Owned::Directory(_))) {
            let (_, released) = self.release_recorded(projection, path)?;
            return Ok(released == Release::Done);
        }
        remove_vacated(projection, path)?;
        Ok(true)
    }

    fn clear_vacated(&self, projection: Projection<'_>) -> Result<(), NativeMountError> {
        let paths = {
            let state = lock_recover(&self.state);
            let mut paths = MemoVec::default();
            for directory in state.vacated.keys() {
                examine_memo(projection)?;
                let mut pending = false;
                for path in state.pending.keys() {
                    examine_memo(projection)?;
                    if projfs_path_suffix_with_work(path, directory, projection.work)?.is_some() {
                        pending = true;
                        break;
                    }
                }
                if !pending { paths.push(directory.copy_owned(projection)?, projection)?; }
            }
            paths
        };
        for directory in paths.items.iter().rev() {
            let projected = {
                let state = lock_recover(&self.state);
                let mut projected = false;
                for path in state.directories.keys() {
                    examine_memo(projection)?;
                    if projfs_path_suffix_with_work(path, directory, projection.work)?.is_some() {
                        projected = true;
                        break;
                    }
                }
                projected
            };
            if !projected && !self.retire_vacated(projection, directory)? { continue; }
            lock_recover(&self.state).vacated.remove(directory, projection)?;
        }
        Ok(())
    }

    fn current_beneath(
        &self, projection: Projection<'_>, from: &MountPath,
    ) -> Result<MemoVec<RetainedBuffer<MountPath>>, NativeMountError> {
        let mut snapshot = lock_recover(&self.state).written.copy_owned(projection)?;
        let mut current = MemoVec::default();
        for (path, written) in snapshot.entries.items.drain(..) {
            examine_memo(projection)?;
            if written.facts.is_some()
                && projfs_path_suffix_with_work(&path, from, projection.work)?.is_some()
                && !written.superseded(projection, &path)?
            {
                current.push(path, projection)?;
            }
        }
        Ok(current)
    }

    fn renamed(
        &self, projection: Projection<'_>, from: &MountPath, to: &MountPath, mut carried: Carried,
    ) -> Result<(), NativeMountError> {
        let mut state = lock_recover(&self.state);
        let state = &mut *state;
        rebase_memo(&mut state.directories, from, to, projection)?;
        rebase_memo(&mut state.written, from, to, projection)?;
        rebase_memo(&mut state.pending, from, to, projection)?;
        let is_directory = state.directories.contains_key(to, projection)?;
        if let (Some(parent), Some(name)) = (owned_parent(from, projection)?, from.components().last())
            && let Some(materialized) = state.directories.get_mut(&parent, projection)?
        {
            materialized.names.remove(name, projection)?;
        }
        if let (Some(parent), Some(name)) = (owned_parent(to, projection)?, to.components().last())
            && let Some(materialized) = state.directories.get_mut(&parent, projection)?
        {
            materialized.names.insert(name, is_directory, projection)?;
        }
        for (path, facts) in carried.files.items.drain(..) {
            examine_memo(projection)?;
            let owned = match state.written.get(&path, projection)? {
                Some(written) => written.owned.clone(),
                None => state.pending.get(&path, projection)?.and_then(|written| written.owned.clone()),
            };
            let (key, _) = state.written.insert_owned_get(path, WrittenEntry {
                file_id: facts.lookup.node.file_id, owned, facts: Some(facts), basis: carried.basis,
            }, projection)?;
            state.pending.remove(key, projection)?;
        }
        self.changed.notify_all();
        Ok(())
    }

    fn invalidate(&self, path: &MountPath, projection: Projection<'_>) -> Result<(), NativeMountError> {
        let mut state = lock_recover(&self.state);
        if let (Some(parent), Some(name)) = (owned_parent(path, projection)?, path.components().last()) {
            state.stale.insert(&parent, (), projection)?;
            if let Some(materialized) = state.directories.get_mut(&parent, projection)? {
                materialized.names.remove(name, projection)?;
            }
        }
        state.written.remove(path, projection)?;
        self.changed.notify_all();
        Ok(())
    }

    fn invalidate_all(&self, projection: Projection<'_>) -> Result<(), NativeMountError> {
        let mut state = lock_recover(&self.state);
        let state = &mut *state;
        for directory in state.directories.keys() {
            examine_memo(projection)?;
            state.stale.insert(directory, (), projection)?;
        }
        for (path, written) in state.written.iter() {
            examine_memo(projection)?;
            state.pending.insert(path, written.clone(), projection)?;
        }
        drop(std::mem::take(&mut state.written));
        self.changed.notify_all();
        Ok(())
    }

    /// The worker borrows the existing Arc runtime.
    fn run(&self, runtime: &Runtime, context: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT) {
        loop {
            let through = {
                let mut state = lock_recover(&self.state);
                loop {
                    if state.stopping { return; }
                    state.notified = state.notified.max(*lock_recover(&self.notification));
                    if state.processed < state.notified || state.retry || !state.stale.is_empty() { break; }
                    let retry = !state.pending.is_empty() || !state.vacated.is_empty();
                    let (next, waited) = self.changed.wait_timeout(state, HELD_RETRY)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    state = next;
                    if retry && waited.timed_out() { break; }
                }
                state.retry = false;
                state.relisting = true;
                state.notified
            };
            let (inherited, _default_gate) = loop {
                if lock_recover(&self.state).stopping { return; }
                let inherited = {
                    let scope = lock_recover(&self.scoped_work);
                    scope.upgrade().map(|meter| {
                        self.active_rounds.fetch_add(1, Ordering::AcqRel);
                        meter
                    })
                };
                if let Some(meter) = inherited { break (Some(meter), None); }
                match self.scope_gate.try_lock() {
                    Ok(gate) => break (None, Some(gate)),
                    Err(std::sync::TryLockError::Poisoned(error)) =>
                        break (None, Some(error.into_inner())),
                    Err(std::sync::TryLockError::WouldBlock) => std::thread::sleep(HELD_RETRY),
                }
                // A lifecycle loan can appear while this round waits for its gate.
            };
            let outcome = if let Some(meter) = inherited {
                let _round = ProjectorRound(self);
                (|| {
                    installed_callback_work(runtime)?;
                    verify_callback_work(runtime, &meter)?;
                    let custody = crate::demand::NativeMountAllocationCustody::new_with_meter(&meter)?;
                    self.run_round(Projection {
                        source: runtime.source.as_ref(), work: &meter, custody: &custody,
                        context, root: &runtime.root, root_identity: runtime.metadata_root.identity(),
                        staging_custody: &runtime.staging_custody,
                        captures: &runtime.post_operation_failure, starting: false,
                    })
                })()
            } else {
                installed_callback_work(runtime)
                    .and_then(|bound| match bound {
                        Some(work) => Ok(work.native_materialization_work()),
                        None => crate::NativeMaterializationWork::new(self.budget, &self.cancellation)
                            .map_err(|(cause, _)| detach_work_error(cause)),
                    })
                    .and_then(|work| {
                        verify_callback_work(runtime, &work.native_mount_work())?;
                        self.with_control_work_locked(runtime, &work, |meter| {
                            self.active_rounds.fetch_add(1, Ordering::AcqRel);
                            let _round = ProjectorRound(self);
                            let custody = crate::demand::NativeMountAllocationCustody::new_with_meter(meter)?;
                            self.run_round(Projection {
                                source: runtime.source.as_ref(), work: meter, custody: &custody,
                                context, root: &runtime.root, root_identity: runtime.metadata_root.identity(),
                                staging_custody: &runtime.staging_custody,
                                captures: &runtime.post_operation_failure, starting: false,
                            })
                        })
                    })
            };
            let mut state = lock_recover(&self.state);
            state.relisting = false;
            state.processed = state.processed.max(through);
            if let Err(error) = outcome { state.failure.get_or_insert(error); }
            self.changed.notify_all();
        }
    }

    fn run_round(&self, projection: Projection<'_>) -> Result<(), NativeMountError> {
        let (mut directories, mut stale, mut written) = {
            let state = lock_recover(&self.state);
            let mut directories = MemoVec::default();
            directories.reserve_for(state.directories.entries.items.len(), projection)?;
            for (path, directory) in state.directories.iter() {
                examine_memo(projection)?;
                directories.items.push((path.copy_owned(projection)?, directory.basis));
            }
            (directories, state.stale.copy_owned(projection)?, state.written.copy_owned(projection)?)
        };
        let mut listings = MemoMap::default();
        for (path, basis) in directories.items.drain(..) {
            examine_memo(projection)?;
            let current = match basis {
                Some(basis) => basis.still_describes(projection, &path, None)?,
                None => false,
            };
            if !current { listings.insert_owned(path, (), projection)?; }
        }
        for (path, ()) in stale.entries.items.drain(..) {
            listings.insert_owned(path, (), projection)?;
        }
        for (path, attempted) in written.entries.items.drain(..) {
            examine_memo(projection)?;
            if !attempted.superseded(projection, &path)? { continue; }
            let confirm = attempted.unconfirmed(projection, &path)?;
            let current = if confirm {
                let work = projection.work.clone();
                projection.source.lookup_pinned_with_meter(&path, &work).map_err(detach_source_error)?
            } else { None };
            let basis = if confirm { ReadBasis::sample(projection)? } else { None };
            let mut state = lock_recover(&self.state);
            let state = &mut *state;
            if state.written.get(&path, projection)? != Some(&attempted) { continue; }
            if current.is_some_and(|(lookup, pin)| attempted.facts
                .is_some_and(|facts| facts.describe(&EntryFacts { lookup, pin })))
            {
                if let Some(written) = state.written.get_mut(&path, projection)? { written.basis = basis; }
            } else {
                let (key, _) = state.pending.insert_owned_get(path, attempted, projection)?;
                state.written.remove(key, projection)?;
            }
        }
        // Sorted keys visit every parent before its descendants, without an opaque sort or guessed sorting-buffer/copy costs.
        for directory in listings.keys() {
            examine_memo(projection)?;
            self.reconcile(projection, directory)?;
            lock_recover(&self.state).stale.remove(directory, projection)?;
        }
        self.replace_pending(projection)?;
        self.clear_vacated(projection)
    }

    /// Pending paths survive every denial/uncertain native error.
    fn replace_pending(&self, projection: Projection<'_>) -> Result<(), NativeMountError> {
        let attempted = lock_recover(&self.state).pending.copy_owned(projection)?;
        for path in attempted.keys().rev() {
            examine_memo(projection)?;
            self.paths.with(path, projection, || {
                let Some(written) = lock_recover(&self.state).pending.get(path, projection)?.cloned()
                    else { return Ok(()); };
                let mut committed = None;
                let replaced = replace_written(projection, path, &written, &mut committed,
                    &mut |current| lock_recover(&self.state).written.insert(path, current.clone(), projection).map(|_| ()));
                let mut state = lock_recover(&self.state);
                if let Some(current) = committed {
                    state.written.insert(path, current.clone(), projection)?;
                    if replaced.is_err()
                        && let Some(pending) = state.pending.get_mut(path, projection)?
                    { *pending = current; }
                }
                match replaced {
                    Ok(Replaced::Rewritten(current)) => state.record_attempt(path, &written, Some(current), projection),
                    Ok(Replaced::Released) => state.record_attempt(path, &written, None, projection),
                    Ok(Replaced::Busy) => Ok(()),
                    Err(error) => Err(error),
                }
            })?;
        }
        Ok(())
    }

    /// Tries the paths held open again, as one of their handles closed.
    fn retry(&self) {
        let mut state = lock_recover(&self.state);
        if !state.pending.is_empty() {
            state.retry = true;
            self.changed.notify_all();
        }
    }

    fn drop_retired_memos(&self, preserve_namespace: bool) {
        let mut loan = RetiredProjectorState::take(&self.state);
        let state = loan.state.as_mut().expect("original physically drained memo payload");
        if !preserve_namespace {
            state.written.entries.items.retain(|(_, written)| matches!(written.owned.as_ref(),
                Some(Owned::Link(owner)) if lock_recover(&owner.namespace_residue).is_some()));
            state.pending.entries.items.retain(|(_, written)| matches!(written.owned.as_ref(),
                Some(Owned::Link(owner)) if lock_recover(&owner.namespace_residue).is_some()));
        }
        drop(std::mem::take(&mut state.directories));
        drop(std::mem::take(&mut state.stale));
        drop(std::mem::take(&mut state.vacated));
        drop(std::mem::take(&mut state.relist));
    }

    /// Retire the existing intrinsic memo capabilities without a fresh path snapshot, namespace adoption, or variable recursive directory walk.
    fn retire_owned_entries(
        &self, context: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT,
        work: &super::NativeMountMeterWork, wait: Option<&crate::CancellationToken>,
        preserve_namespace: bool,
    ) -> Result<(), NativeMountError> {
        let loan = RetiredProjectorState::take(&self.state);
        let state = loan.state.as_ref().expect("original physically drained memo payload");
        for (_, written) in state.written.iter().rev().chain(state.pending.iter().rev()) {
            match written.owned.as_ref() {
                Some(Owned::Link(owner)) => owner.retire_for_shutdown(context)?,
                Some(Owned::File(_, owner) | Owned::ProvisionalFile(owner) | Owned::Directory(owner) | Owned::PhysicalLink(_, owner)) => {
                    if preserve_namespace { close_preserved_physical_entry(owner, work, wait)?; }
                    else { retire_physical_entry(owner, work, wait)?; }
                }
                None => {}
            }
        }
        Ok(())
    }

    /// Waits on the existing control, checking the original invocation rather than installing a fresh timeout/cancellation or erasing pending work.
    fn settle_measured(&self, work: &super::NativeMountMeterWork) -> Result<(), NativeMountError> {
        let target = *lock_recover(&self.notification);
        let mut state = lock_recover(&self.state);
        let mut deadline = None;
        loop {
            work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED).map_err(detach_work_error)?;
            if let Some(error) = state.failure.take() { return Err(error); }
            if state.stopping { return Err(NativeMountError::Lifecycle("ProjFS projector is stopped")); }
            if state.processed >= target && !state.relisting && state.stale.is_empty()
                && state.pending.is_empty() && state.vacated.is_empty()
            { return Ok(()); }
            if state.processed >= target && !state.relisting && state.stale.is_empty() {
                let limit = *deadline.get_or_insert_with(|| std::time::Instant::now() + HELD_SETTLE_LIMIT);
                if std::time::Instant::now() >= limit {
                    return Err(NativeMountError::Lifecycle("a superseded projection entry remains held open"));
                }
            } else { deadline = None; }
            state = self.changed.wait_timeout(state, HELD_RETRY)
                .unwrap_or_else(std::sync::PoisonError::into_inner).0;
        }
    }

    /// Signals the existing worker and keeps its join handle in custody if cancellation arrives before completion.
    fn finish_measured<W: crate::native_host::NativeHostWork>(
        &self, work: &mut W, cleanup: Option<&crate::materializer::NativeCleanupLoan<'_>>,
    ) -> Result<(), NativeMountError>
    where NativeMountError: From<W::Error> {
        if let Some(loan) = cleanup {
            loan.check_wait().map_err(detach_work_error)?;
        } else {
            work.check().map_err(detach_work_error)?;
        }
        lock_recover(&self.state).stopping = true;
        self.changed.notify_all();
        loop {
            if let Some(loan) = cleanup {
                loan.check_wait().map_err(detach_work_error)?;
            } else {
                work.check().map_err(detach_work_error)?;
            }
            {
                let mut worker = lock_recover(&self.worker);
                if worker.as_ref().is_none_or(std::thread::JoinHandle::is_finished) {
                    if worker.is_some() {
                        let attempt = crate::WorkCounters {
                            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
                        };
                        if let Some(loan) = cleanup {
                            loan.started_io(attempt).map_err(detach_work_error)?;
                        } else {
                            work.started_io(attempt).map_err(detach_work_error)?;
                        }
                        let joined = worker.take().expect("owned finished worker").join();
                        if joined.is_err() {
                            return Err(NativeMountError::Lifecycle("ProjFS projector worker panicked"));
                        }
                    }
                    return Ok(());
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}


fn projection_io_error(error: std::io::Error) -> NativeMountError {
    detach_work_error(crate::demand::native_demand_error(error))
}

/// Preserve Win32 classification and the complete non-Win32 SDK status without allocating a replacement diagnostic or truncating its original cause.
fn projection_sdk_io_error(error: windows::core::Error) -> std::io::Error {
    let code = error.code().0;
    let raw = if ((code as u32 >> 16) & 0x1fff) == 7 { code & 0xffff } else { code };
    std::io::Error::from_raw_os_error(raw)
}

fn native_mount_error_code(error: &NativeMountError) -> Option<HRESULT> {
    match error {
        NativeMountError::Io(error) => error.raw_os_error().map(|code| {
            if code < 0 { HRESULT(code) } else { HRESULT::from_win32(code as u32) }
        }),
        _ => None,
    }
}

fn projection_native_call<T>(
    work: &super::NativeMountMeterWork,
    write: bool,
    operation: impl FnOnce() -> std::io::Result<T>,
) -> Result<T, NativeMountError> {
    let delta = if write {
        crate::WorkCounters { backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED }
    } else {
        crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED }
    };
    let host_work = work.native_materialization_work();
    host_work.admit(delta)?;
    host_work.record_admitted(delta)?;
    operation().map_err(projection_io_error)
}

/// Poll accepted native work to its real terminal fence, including cancellation.
fn wait_native_unit_measured(
    completion: acyclic_native_runtime::UnitCompletion,
    work: &super::NativeMountMeterWork,
) -> Result<(), NativeMountError> {
    let mut completion = std::pin::pin!(completion);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    loop {
        match std::future::Future::poll(completion.as_mut(), &mut context) {
            std::task::Poll::Ready(result) => return result.map_err(|error|
                detach_work_error(work.native_materialization_work().runtime_error(error))),
            std::task::Poll::Pending => std::thread::sleep(std::time::Duration::from_millis(1)),
        }
    }
}

/// Owns only a fresh leaf buffer.
struct ProjectionBuffer<T> {
    bytes: Vec<T>,
    _allocation: crate::NativeAllocation,
}

impl<T> ProjectionBuffer<T> {
    fn new(
        count: usize, work: &super::NativeMountMeterWork,
    ) -> Result<Self, NativeMountError> {
        let size = count.checked_mul(size_of::<T>()).ok_or(crate::WorkError::Overflow)? as u64;
        let allocation = work.native_materialization_work().claim(size)?;
        let mut bytes = Vec::new();
        if let Err(error) = bytes.try_reserve_exact(count) {
            drop(bytes);
            return Err(NativeMountError::Allocation(error));
        }
        // The declared std Global reserve-exact ABI reports this chosen logical layout; no allocation may be financed by post-hoc capacity adoption.
        debug_assert_eq!(bytes.capacity().checked_mul(size_of::<T>()), Some(size as usize));
        Ok(Self { bytes, _allocation: allocation })
    }

    fn as_mut(&mut self) -> &mut Vec<T> { &mut self.bytes }
    fn as_slice(&self) -> &[T] { &self.bytes }
}


fn projection_component(
    component: &[u8], work: &super::NativeMountMeterWork,
) -> Result<ProjectionBuffer<u16>, NativeMountError> {
    if component.is_empty() || !component.len().is_multiple_of(2) {
        return Err(NativeMountError::Demand(crate::DemandError::InvalidRequest));
    }
    let mut name = ProjectionBuffer::new(component.len() / 2, work)?;
    let mut host_work = &work.native_materialization_work();
    for pair in component.chunks_exact(2) {
        let examined = crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED };
        host_work.admit(examined).map_err(detach_work_error)?;
        let unit = u16::from_le_bytes([pair[0], pair[1]]);
        host_work.record(examined).map_err(detach_work_error)?;
        if matches!(unit, 0 | 47 | 58 | 92) {
            return Err(NativeMountError::Demand(crate::DemandError::InvalidRequest));
        }
        let encoded = crate::WorkCounters { bytes_encoded: 2, ..crate::WorkCounters::UNCHARGED };
        host_work.admit(encoded).map_err(detach_work_error)?;
        name.as_mut().push(unit);
        host_work.record(encoded).map_err(detach_work_error)?;
    }
    if name.as_slice() == [46] || name.as_slice() == [46, 46] {
        return Err(NativeMountError::Demand(crate::DemandError::InvalidRequest));
    }
    Ok(name)
}

fn with_projection_relative_name<T>(
    path: &MountPath, work: &super::NativeMountMeterWork,
    operation: impl FnOnce(PCWSTR) -> Result<T, NativeMountError>,
) -> Result<T, NativeMountError> {
    let mut units = path.components().len().saturating_sub(1)
        .checked_add(1).ok_or(crate::WorkError::Overflow)?;
    for component in path.components() {
        units = units.checked_add(component.len() / 2).ok_or(crate::WorkError::Overflow)?;
    }
    let mut name = ProjectionBuffer::new(units, work)?;
    let mut host_work = &work.native_materialization_work();
    for component in path.components() {
        if component.is_empty() || !component.len().is_multiple_of(2) {
            return Err(NativeMountError::Demand(crate::DemandError::InvalidRequest));
        }
        if !name.as_slice().is_empty() {
            let delta = crate::WorkCounters { bytes_encoded: 2, ..crate::WorkCounters::UNCHARGED };
            host_work.admit(delta).map_err(detach_work_error)?;
            name.as_mut().push(92);
            host_work.record(delta).map_err(detach_work_error)?;
        }
        let start = name.as_slice().len();
        for pair in component.chunks_exact(2) {
            let examined = crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED };
            host_work.admit(examined).map_err(detach_work_error)?;
            let unit = u16::from_le_bytes([pair[0], pair[1]]);
            host_work.record(examined).map_err(detach_work_error)?;
            if matches!(unit, 0 | 47 | 58 | 92) {
                return Err(NativeMountError::Demand(crate::DemandError::InvalidRequest));
            }
            let encoded = crate::WorkCounters { bytes_encoded: 2, ..crate::WorkCounters::UNCHARGED };
            host_work.admit(encoded).map_err(detach_work_error)?;
            name.as_mut().push(unit);
            host_work.record(encoded).map_err(detach_work_error)?;
        }
        if name.as_slice()[start..] == [46] || name.as_slice()[start..] == [46, 46] {
            return Err(NativeMountError::Demand(crate::DemandError::InvalidRequest));
        }
    }
    let encoded = crate::WorkCounters { bytes_encoded: 2, ..crate::WorkCounters::UNCHARGED };
    host_work.admit(encoded).map_err(detach_work_error)?;
    name.as_mut().push(0);
    host_work.record(encoded).map_err(detach_work_error)?;
    operation(PCWSTR::from_raw(name.as_slice().as_ptr()))
}

fn with_projection_parent<T>(
    root: &std::path::Path, root_identity: crate::NativeRootIdentity, path: &MountPath,
    work: &super::NativeMountMeterWork,
    operation: impl FnOnce(&cap_std::fs::Dir, &[u16]) -> Result<T, NativeMountError>,
) -> Result<T, NativeMountError> {
    let (leaf, parents) = path.components().split_last()
        .ok_or(NativeMountError::Demand(crate::DemandError::InvalidRequest))?;
    let mut host_work = &work.native_materialization_work();
    let (mut parent, observed) = crate::native_host::open_root_directory_identity_with_work(
        root, &mut host_work,
    ).map_err(detach_work_error)?;
    if observed != root_identity {
        return Err(NativeMountError::Demand(crate::DemandError::StaleSource));
    }
    for component in parents {
        let component = projection_component(component, work)?;
        parent = crate::native_host::open_owned_child_directory_with_work(
            &parent, component.as_slice(), &mut host_work,
        ).map_err(detach_work_error)?;
    }
    let leaf = projection_component(leaf, work)?;
    operation(&parent, leaf.as_slice())
}

/// Writes `entry` at `path`: an ordinary directory for a directory, an ordinary file for a file, and a placeholder for a link.
fn write_entry(
    projection: Projection<'_>, path: &MountPath, entry: &ProjectedEntry,
    placed: &mut Option<Owned>,
    record: &mut impl FnMut(&Owned) -> Result<(), NativeMountError>,
) -> Result<Placed, NativeMountError> {
    if entry.info.IsDirectory {
        let result = with_projection_parent(projection.root, projection.root_identity, path, projection.work, |parent, leaf| {
            let owner = PhysicalEntry::prepare(projection.work, projection.custody, true)?;
            let owned = Owned::Directory(owner.clone());
            record(&owned)?;
            let mut work = &projection.work.native_materialization_work();
            let file = crate::native_host::create_windows_new_directory_with_work(parent, leaf, &mut work)
                .map_err(detach_work_error)?;
            owner.install(file);
            *placed = Some(owned.clone());
            owner.identify(projection.work)?;
            Ok(Placed::Owned(owned))
        });
        return match result {
            Err(NativeMountError::Io(error)) if matches!(error.kind(),
                std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::NotFound
                    | std::io::ErrorKind::NotADirectory) => Ok(Placed::Nothing),
            result => result,
        };
    }
    let Some(target) = entry.symlink_target.as_ref()
        .map(|target| target.value.as_ref().expect("live link packet").bytes.as_ref()) else {
        return write_ordinary(projection, path, entry, None, placed, record);
    };
    if !target.len().is_multiple_of(2) {
        return Err(NativeMountError::Demand(crate::DemandError::InvalidRequest));
    }
    let mut target_name = ProjectionBuffer::new(target.len() / 2 + 1, projection.work)?;
    let delta = crate::WorkCounters { bytes_encoded: target.len() as u64, ..crate::WorkCounters::UNCHARGED };
    let mut work = &projection.work.native_materialization_work();
    work.admit(delta).map_err(detach_work_error)?;
    for pair in target.chunks_exact(2) {
        let unit = u16::from_le_bytes([pair[0], pair[1]]);
        if unit == 0 { return Err(NativeMountError::Demand(crate::DemandError::InvalidRequest)); }
        target_name.as_mut().push(unit);
    }
    work.record(delta).map_err(detach_work_error)?;
    let digest = link_digest_measured(target_name.as_slice(), projection.work)?;
    target_name.as_mut().push(0);
    let mut placeholder = link_placeholder(entry.info);
    if entry.directory_link {
        placeholder.FileBasicInfo.IsDirectory = true;
        placeholder.FileBasicInfo.FileAttributes |= FILE_ATTRIBUTE_DIRECTORY.0;
    }
    let extended = PRJ_EXTENDED_INFO {
        InfoType: PRJ_EXT_INFO_TYPE_SYMLINK, NextInfoOffset: 0,
        Anonymous: PRJ_EXTENDED_INFO_0 { Symlink: PRJ_EXTENDED_INFO_0_0 {
            TargetName: PCWSTR::from_raw(target_name.as_slice().as_ptr()),
        } },
    };
    let result = with_projection_relative_name(path, projection.work, |relative| {
        let owner = SdkPhysicalEntry::prepare(projection, relative)?;
        let owned = Owned::Link(owner.clone());
        record(&owned)?;
        projection_native_call(projection.work, true, || {
            *placed = Some(owned.clone());
            owner.effect_uncertain.store(true, std::sync::atomic::Ordering::Release);
            // SAFETY: admitted native names and fixed SDK information remain live throughout this synchronous, potentially reentrant call.
            let result = unsafe { PrjWritePlaceholderInfo2(projection.context, relative, &raw const placeholder,
                size_of::<PRJ_PLACEHOLDER_INFO>() as u32, Some(&extended)) }
                .map_err(|error| {
                    *lock_recover(&owner.namespace_residue) = Some(SdkNamespaceResidue {
                        hresult: error.code(), update_causes: PRJ_UPDATE_FAILURE_CAUSES(0),
                    });
                    projection_sdk_io_error(error)
                });
            match &result {
                Ok(()) => {
                    owner.born.store(true, std::sync::atomic::Ordering::Release);
                    owner.effect_uncertain.store(false, std::sync::atomic::Ordering::Release);
                }
                Err(error) if matches!(error.raw_os_error(), Some(2 | 3 | 50 | 80 | 183 | 267)) => {
                    *placed = None;
                    *lock_recover(&owner.namespace_residue) = None;
                    owner.effect_uncertain.store(false, std::sync::atomic::Ordering::Release);
                }
                Err(_) => {}
            }
            result
        })?;
        Ok(Placed::Owned(owned))
    });
    match result {
        Ok(placed) => Ok(placed),
        Err(error) if native_mount_error_code(&error).is_some_and(|code| [
            HR_ALREADY_EXISTS, HR_FILE_EXISTS, HR_FILE_NOT_FOUND, HR_PATH_NOT_FOUND, HR_DIRECTORY,
        ].contains(&code)) => Ok(Placed::Nothing),
        Err(error) if native_mount_error_code(&error) == Some(HR_NOT_SUPPORTED) =>
            write_link(projection, path, target_name.as_slice(), entry.directory_link, digest, placed, record),
        Err(error) => Err(error),
    }
}

/// Writes an ordinary link through the existing held-parent create engine.
fn write_link(
    projection: Projection<'_>, path: &MountPath, target: &[u16], directory: bool,
    digest: u64, placed: &mut Option<Owned>,
    record: &mut impl FnMut(&Owned) -> Result<(), NativeMountError>,
) -> Result<Placed, NativeMountError> {
    let target = target.strip_suffix(&[0]).unwrap_or(target);
    with_projection_parent(projection.root, projection.root_identity, path, projection.work, |parent, leaf| {
        let owner = PhysicalEntry::prepare(projection.work, projection.custody, false)?;
        let owned = Owned::PhysicalLink(digest, owner.clone());
        record(&owned)?;
        let mut capability = lock_recover(&owner.file);
        let result = crate::native_host::create_windows_symbolic_link_with_work(
            parent, leaf, target, directory, &mut capability,
            || {
                owner.delete_on_close.store(true, std::sync::atomic::Ordering::Release);
                *placed = Some(owned.clone());
            },
            || { owner.expected_reparse.store(
                windows::Win32::System::SystemServices::IO_REPARSE_TAG_SYMLINK,
                std::sync::atomic::Ordering::Release,
            ); },
            &mut &projection.work.native_materialization_work(),
        ).map_err(detach_work_error);
        drop(capability);
        if result.is_ok() { owner.delete_on_close.store(false, std::sync::atomic::Ordering::Release); }
        if placed.is_some() && result.is_ok() { owner.identify(projection.work)?; }
        match result {
            Ok(()) => Ok(Placed::Owned(owned)),
            Err(NativeMountError::Io(error)) if placed.is_none() && matches!(error.kind(),
                std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::NotFound) => Ok(Placed::Nothing),
            Err(error) => Err(error),
        }
    })
}

fn link_digest_measured(
    target: &[u16], work: &super::NativeMountMeterWork,
) -> Result<u64, NativeMountError> {
    let delta = crate::WorkCounters { bytes_hashed: target.len() as u64 * 2, ..crate::WorkCounters::UNCHARGED };
    let mut host_work = &work.native_materialization_work();
    host_work.admit(delta).map_err(detach_work_error)?;
    let digest = link_digest(target.iter().copied());
    host_work.record(delta).map_err(detach_work_error)?;
    Ok(digest)
}

/// A digest of a link's target, which says whether a link is still the one written.
fn link_digest(target: impl IntoIterator<Item = u16>) -> u64 {
    use std::hash::{Hash as _, Hasher as _};

    let mut hasher = std::hash::DefaultHasher::new();
    target.into_iter().for_each(|unit| unit.hash(&mut hasher));
    hasher.finish()
}

/// Deletes what the projection wrote at `path` unless the user made it their own: what `ProjFS` manages, or what it does not.
fn release(
    projection: Projection<'_>,
    path: &MountPath,
    owned: Option<Owned>,
) -> Result<Release, NativeMountError> {
    match owned {
        None => delete_unowned(projection, path),
        Some(Owned::Link(owner)) => release_link(projection, path, &owner),
        Some(Owned::File(state, owner)) => release_file(projection, path, state, &owner),
        Some(Owned::Directory(owner)) => release_directory(projection, path, &owner),
        Some(Owned::PhysicalLink(_, owner)) => release_directory(projection, path, &owner),
        Some(Owned::ProvisionalFile(owner)) => release_directory(projection, path, &owner),
    }
}

/// Deletes the ordinary file the projection made at `path` while it is still `state`.
fn release_file(
    projection: Projection<'_>, path: &MountPath, state: HostWindowsMetadata,
    owner: &PhysicalEntry,
) -> Result<Release, NativeMountError> {
    if has_pending_capture(projection.captures, path, projection)? { return Ok(Release::Busy); }
    let deleted = with_projection_parent(projection.root, projection.root_identity, path, projection.work, |parent, leaf| {
        delete_file_while_owned(parent, leaf, state, projection.work, owner)
    });
    match deleted {
        Ok(true) => {}
        Ok(false) => return Ok(Release::Done),
        Err(NativeMountError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound =>
            return Ok(Release::Done),
        Err(NativeMountError::Io(error)) if error.raw_os_error() == Some(SHARING_VIOLATION) =>
            return Ok(Release::Busy),
        Err(error) => return Err(error),
    }
    with_projection_relative_name(path, projection.work, |relative| clear_tombstone(projection, relative))
}

fn release_directory(
    projection: Projection<'_>, path: &MountPath, owner: &PhysicalEntry,
) -> Result<Release, NativeMountError> {
    if has_pending_capture(projection.captures, path, projection)? { return Ok(Release::Busy); }
    // Original namespace/capture admission remains separate from physical birth credit; cancelled source work cannot authorize publication.
    projection.work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)?;
    match retire_physical_entry(owner, projection.work, Some(projection.work.native_materialization_work().cancellation())) {
        Ok(()) => Ok(Release::Done),
        Err(NativeMountError::Io(error)) if refused_for_now(&error) => Ok(Release::Busy),
        Err(error) => Err(error),
    }
}

fn retire_physical_entry(
    owner: &PhysicalEntry, work: &super::NativeMountMeterWork, wait: Option<&crate::CancellationToken>,
) -> Result<(), NativeMountError> {
    if owner.disposed.load(std::sync::atomic::Ordering::Acquire) { return Ok(()); }
    let retained = lock_recover(&owner.file);
    let Some(file) = retained.as_ref() else { return Ok(()); };
    let expected_identity = *lock_recover(&owner.identity);
    let mut credit = lock_recover(&owner.controls);
    let required_reads = if owner.directory { 3 } else { 2 };
    if credit.remaining_work().backend_read_operations < required_reads
        || credit.remaining_work().backend_write_operations < 2
    {
        return Err(NativeMountError::Lifecycle(
            "original projected child disposition credit exhausted; capability retained",
        ));
    }
    let loan = match wait {
        Some(wait) => credit.cleanup_loan(wait),
        None => credit.accepted_cleanup_loan(),
    };
    let mut host = &work.native_materialization_work();
    let mut close = lock_recover(&owner.temporary_close);
    if owner.directory && close.as_ref().is_none_or(|close| close.remaining_work().backend_write_operations == 0) {
        return Err(NativeMountError::Lifecycle(
            "original projected directory reopen-close credit exhausted; capability retained",
        ));
    }
    let reopened = if owner.directory { Some(reopen_for_delete_measured(file, &mut host, Some(&loan))?) } else { None };
    let actual = reopened.as_ref().unwrap_or(file);
    let result = (|| {
        let information = held_information_measured(actual, &mut host, Some(&loan))?;
        let actual_identity = crate::NativeRootIdentity {
            device: u64::from(information.dwVolumeSerialNumber),
            object: (u64::from(information.nFileIndexHigh) << 32) | u64::from(information.nFileIndexLow),
        };
        if expected_identity.is_some_and(|identity| actual_identity != identity) {
            return Err(crate::DemandError::StaleSource.into());
        }
        let expected_tag = match owner.expected_reparse.load(std::sync::atomic::Ordering::Acquire) {
            0 => None, tag => Some(tag),
        };
        if reparse_tag_held_measured(actual, &mut host, Some(&loan))? != expected_tag {
            return Err(crate::DemandError::StaleSource.into());
        }
        *lock_recover(&owner.identity) = Some(actual_identity);
        delete_through_measured(actual, &mut host, Some(&loan))?;
        owner.disposed.store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    })();
    if let Some(reopened) = reopened {
        close.as_mut().expect("directory born close credit").started_drop_io(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        });
        drop(reopened);
    }
    result
}

/// Only the original protected live SDK instance can retire its placeholder.
fn release_link(
    projection: Projection<'_>, path: &MountPath, owner: &SdkPhysicalEntry,
) -> Result<Release, NativeMountError> {
    if has_pending_capture(projection.captures, path, projection)? { return Ok(Release::Busy); }
    projection.work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)?;
    match owner.retire(projection.context, Some(projection.work.native_materialization_work().cancellation())) {
        Ok(()) => Ok(Release::Done),
        Err(NativeMountError::Io(error)) if error.raw_os_error() == Some(SHARING_VIOLATION) => Ok(Release::Busy),
        Err(error) => Err(error),
    }
}

/// Clears the tombstone deleting what the projection wrote at `path` directly leaves, which would hide what is written there next.
fn clear_tombstone(
    projection: Projection<'_>, relative: PCWSTR,
) -> Result<Release, NativeMountError> {
    match prj_delete(projection, relative, PRJ_UPDATE_ALLOW_TOMBSTONE) {
        Ok(()) => Ok(Release::Done),
        Err(error) if native_mount_error_code(&error) == Some(HR_SHARING_VIOLATION) => Ok(Release::Busy),
        Err(error) if native_mount_error_code(&error).is_some_and(|code| [
            HR_FILE_NOT_FOUND, HR_PATH_NOT_FOUND, HR_VIRTUALIZATION_INVALID_OPERATION,
        ].contains(&code)) => Ok(Release::Done),
        Err(error) => Err(error),
    }
}

/// `ERROR_SHARING_VIOLATION`, as an I/O error reports it.
const SHARING_VIOLATION: i32 = 32;

/// `PrjDeleteFile` at `relative`, allowed what `update` allows.
fn prj_delete(
    projection: Projection<'_>, relative: PCWSTR, update: PRJ_UPDATE_TYPES,
) -> Result<(), NativeMountError> {
    projection_native_call(projection.work, true, || {
        // SAFETY: this scoped native name survives synchronous SDK reentry.
        unsafe { PrjDeleteFile(projection.context, relative, Some(update), None) }
            .map_err(projection_sdk_io_error)
    })
}


/// Removes the directory the projection made at `path` if nothing is left in it: what the user left in it keeps it.
fn remove_vacated(
    projection: Projection<'_>, path: &MountPath,
) -> Result<(), NativeMountError> {
    if path.components().is_empty() { return Ok(()); }
    let result = with_projection_parent(projection.root, projection.root_identity, path, projection.work, |parent, leaf| {
        crate::native_host::remove_windows_leaf_with_work(
            parent, leaf, true, &mut &projection.work.native_materialization_work(),
        ).map_err(detach_work_error)
    });
    match result {
        Err(NativeMountError::Io(error)) if matches!(error.kind(),
            std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty) => Ok(()),
        result => result,
    }
}

/// Writes `entry`, a regular file, at `path` as an ordinary file: the content the listing named, its times and attributes, all under a staged name, then given its own name at once, so no reader sees it partly written.
fn write_ordinary(
    projection: Projection<'_>, path: &MountPath, entry: &ProjectedEntry,
    replacing: Option<(HostWindowsMetadata, &PhysicalEntry)>, placed: &mut Option<Owned>,
    record: &mut impl FnMut(&Owned) -> Result<(), NativeMountError>,
) -> Result<Placed, NativeMountError> {
    verify_projection_namespace_work(projection)?;
    let result = with_projection_parent(projection.root, projection.root_identity, path, projection.work, |parent, leaf| {
        if projection.starting && replacing.is_none() {
            return write_directly(projection, path, parent, leaf, entry, placed, record);
        }
        // No source allocation, staging allocation or native writer is made for an existing user name.
        if replacing.is_none() {
            match crate::native_host::stat_windows_leaf_with_work(
                parent, leaf, &mut &projection.work.native_materialization_work(),
            ) {
                Ok(_) => return Ok(Placed::Nothing),
                Err(crate::NativeTreeMaterializationError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(detach_work_error(error)),
            }
        }
        static NEXT_STAGING_NAME: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let sequence = NEXT_STAGING_NAME.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // This process-local native value does not allocate or perform filesystem I/O; FILE_CREATE still refuses an actual colliding private name.
        let process = unsafe { windows::Win32::System::Threading::GetCurrentProcessId() };
        let mut name = [0_u16; 37];
        let encoded = crate::WorkCounters { bytes_encoded: 74, ..crate::WorkCounters::UNCHARGED };
        let mut host_work = &projection.work.native_materialization_work();
        host_work.admit(encoded).map_err(detach_work_error)?;
        for (slot, byte) in name.iter_mut().zip(b".acyclic-copy-") { *slot = u16::from(*byte); }
        for index in 0..8 {
            name[13 + index] = u16::from(b"0123456789abcdef"[((process >> ((7 - index) * 4)) & 15) as usize]);
        }
        for index in 0..16 {
            name[21 + index] = u16::from(b"0123456789abcdef"[((sequence >> ((15 - index) * 4)) & 15) as usize]);
        }
        host_work.record(encoded).map_err(detach_work_error)?;
        let mut writer = ProjectedNativeWriter::create_by(projection.work, projection.custody,
            |owner| record(&Owned::ProvisionalFile(owner.clone())),
            || with_staging_directory(projection.root, projection.work, projection.root_identity, projection.staging_custody,
                |staging| crate::native_host::create_windows_provisional_overlapped_file_with_work(
                    staging, &name, &mut &projection.work.native_materialization_work(),
                ).map_err(detach_work_error)))?;
        let written = (|| {
            let file = writer.file();
            if !write_listed_content(projection, path, entry, file)? { return Ok(Placed::Retry); }
            finish_projected_file(file, entry, projection.work)?;
            let held = match replacing {
                Some((expected, original)) => {
                    if has_pending_capture(projection.captures, path, projection)? { return Ok(Placed::Retry); }
                    original.retain_metadata_control(projection.work)?;
                    match held_original_while(original, expected, projection.work)? {
                        Some(held) => Some((held, original)),
                        None => return Ok(Placed::Nothing),
                    }
                }
                None => None,
            };
            verify_projection_namespace_work(projection)?;
            if let Some((_, original)) = held.as_ref() {
                // A genuine SDK placeholder is deleted through its live namespace with flags NONE.
                let sdk = with_projection_relative_name(path, projection.work, |relative|
                    prj_delete(projection, relative, PRJ_UPDATE_TYPES(0)));
                match sdk {
                    Ok(()) => {}
                    Err(error) if native_mount_error_code(&error).is_some_and(|code|
                        [HR_FILE_NOT_FOUND, HR_PATH_NOT_FOUND, HR_VIRTUALIZATION_INVALID_OPERATION].contains(&code)) => {}
                    Err(error) if native_mount_error_code(&error) == Some(HR_SHARING_VIOLATION) =>
                        return Ok(Placed::Retry),
                    Err(error) => return Err(error),
                }
                if has_pending_capture(projection.captures, path, projection)? { return Ok(Placed::Retry); }
                retire_physical_entry(original, projection.work,
                    Some(projection.work.native_materialization_work().cancellation()))?;
            }
            crate::native_host::keep_windows_created_file_with_work(
                file.as_file(), &mut &projection.work.native_materialization_work(),
            ).map_err(detach_work_error)?;
            writer.owner.delete_on_close.store(false, std::sync::atomic::Ordering::Release);
            let published = publish_projected_file(file, parent, leaf, projection.work);
            match published {
                Ok(()) => {
                    writer.provisional = false;
                    writer.retire()?;
                    writer.owner.retain_metadata_control(projection.work)?;
                    let owned = Owned::File(writer.owner.final_state(projection.work)?, writer.owner.clone());
                    record(&owned)?;
                    *placed = Some(owned.clone());
                    crate::native_host::sync_directory_held_with_work(
                        parent, &mut &projection.work.native_materialization_work(),
                    ).map_err(detach_work_error)?;
                    Ok(Placed::Owned(owned))
                }
                Err(NativeMountError::Io(error)) if matches!(error.kind(),
                    std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::NotFound) => Ok(Placed::Nothing),
                Err(NativeMountError::Io(error)) if error.raw_os_error() == Some(SHARING_VIOLATION) => Ok(Placed::Retry),
                Err(error) => Err(error),
            }
        })();
        let retired = writer.retire();
        match written {
            Err(error) => Err(error),
            Ok(placed) => retired.map(|()| placed),
        }
    });
    match result {
        Err(NativeMountError::Io(error)) if placed.is_none() && matches!(error.kind(),
            std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory) => Ok(Placed::Nothing),
        result => result,
    }
}

/// The times and attributes `entry` names, for its ordinary file.
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

/// The actual staged file and its original native-runtime registration.
struct ProjectedNativeWriter<'a> {
    file: Option<acyclic_native_runtime::NativeFile>,
    work: &'a super::NativeMountMeterWork,
    allocation: u64,
    cleanup: Option<crate::materializer::NativeCleanupWrite>,
    provisional: bool,
    owner: Arc<RetainedArcValue<PhysicalEntry>>,
}

impl<'a> ProjectedNativeWriter<'a> {
    fn create(
        parent: &cap_std::fs::Dir, leaf: &[u16],
        work: &'a super::NativeMountMeterWork,
        custody: &crate::demand::NativeMountAllocationCustody,
        record: impl FnOnce(&Arc<RetainedArcValue<PhysicalEntry>>) -> Result<(), NativeMountError>,
    ) -> Result<Self, NativeMountError> {
        Self::create_by(work, custody, record, ||
            crate::native_host::create_windows_provisional_overlapped_file_with_work(
                parent, leaf, &mut &work.native_materialization_work(),
            ).map_err(detach_work_error))
    }

    fn create_by(
        work: &'a super::NativeMountMeterWork,
        custody: &crate::demand::NativeMountAllocationCustody,
        record: impl FnOnce(&Arc<RetainedArcValue<PhysicalEntry>>) -> Result<(), NativeMountError>,
        create: impl FnOnce() -> Result<std::fs::File, NativeMountError>,
    ) -> Result<Self, NativeMountError> {
        use std::os::windows::io::AsRawHandle as _;
        use windows::Win32::Storage::FileSystem::{
            FILE_DISPOSITION_FLAG_DELETE, FILE_DISPOSITION_FLAG_ON_CLOSE,
            FILE_DISPOSITION_FLAG_POSIX_SEMANTICS, FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE,
            FILE_DISPOSITION_INFO_EX, FILE_DISPOSITION_INFO_EX_FLAGS, FileDispositionInfoEx,
            SetFileInformationByHandle,
        };
        let materialization = work.native_materialization_work();
        let mut host_work = &materialization;
        let transfer_close = materialization.allocation_work().reserve_work(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        })?;
        let owner = PhysicalEntry::prepare(work, custody, false)?;
        *lock_recover(&owner.creator_transfer_close) = Some(transfer_close);
        record(&owner)?;
        let duplicate = materialization.reserve_work(crate::WorkCounters {
            backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED
        })?;
        let cleanup = materialization.reserve_cleanup_write()?;
        let file = match create() {
            Ok(file) => file,
            Err(error) => {
                drop(cleanup);
                return Err(error);
            }
        };
        owner.install(file);
        owner.delete_on_close.store(true, std::sync::atomic::Ordering::Release);
        let capability = lock_recover(&owner.file);
        let file = capability.as_ref().expect("original returned create capability");
        // Configure the same kernel-owned cleanup before a read-only final attribute can be set.
        let disposition = FILE_DISPOSITION_INFO_EX { Flags: FILE_DISPOSITION_INFO_EX_FLAGS(
            FILE_DISPOSITION_FLAG_DELETE.0 | FILE_DISPOSITION_FLAG_ON_CLOSE.0
                | FILE_DISPOSITION_FLAG_POSIX_SEMANTICS.0
                | FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE.0,
        ) };
        let configured = projection_native_call(work, true, || {
            // SAFETY: newly owned DELETE handle and fixed native disposition.
            unsafe { SetFileInformationByHandle(
                windows::Win32::Foundation::HANDLE(file.as_raw_handle()), FileDispositionInfoEx,
                (&raw const disposition).cast(), size_of::<FILE_DISPOSITION_INFO_EX>() as u32,
            ) }.map_err(projection_sdk_io_error)
        });
        if let Err(error) = configured {
            return Err(error);
        }
        let mut duplicate = duplicate;
        let loan = duplicate.cleanup_loan(materialization.cancellation());
        let writer_file = detach_attempt(&mut host_work, Some(&loan), false, ||
            file.try_clone())?;
        drop(loan);
        drop(capability);
        if let Err(error) = owner.identify(work) {
            let _ = cleanup.attempt_recorded(|| drop(writer_file));
            return Err(error);
        }
        // SAFETY: the existing raw native creator set asynchronous I/O and this transfer is the handle's sole positional I/O owner.
        let file = match unsafe {
            acyclic_native_runtime::NativeFile::from_overlapped_file_measured_unchecked(
                writer_file, work.native_materialization_work().runtime_meter(),
            )
        } {
            Ok(file) => file,
            Err((error, returned_file)) => {
                // A denied import returns its actual original descriptor.
                let _ = cleanup.attempt_recorded(|| drop(returned_file));
                return Err(detach_work_error(materialization.runtime_error(error)));
            }
        };
        let allocation = file.logical_owned_bytes();
        Ok(Self { file: Some(file), work, allocation, cleanup: Some(cleanup), provisional: true, owner })
    }

    fn file(&self) -> &acyclic_native_runtime::NativeFile {
        self.file.as_ref().expect("writer is retained until terminal completion")
    }

    fn retire(&mut self) -> Result<(), NativeMountError> {
        let Some(file) = self.file.take() else { return Ok(()); };
        let cleanup = self.cleanup.take().expect("created writer has cleanup custody");
        // Kept SDK files also own a real positional descriptor.
        let recorded = cleanup.attempt_recorded(|| drop(file)).1;
        self.work.native_materialization_work().runtime_meter().release(self.allocation);
        self.allocation = 0;
        recorded.map_err(detach_work_error)
    }
}

impl Drop for ProjectedNativeWriter<'_> {
    fn drop(&mut self) { let _ = self.retire(); }
}

fn handle_state_measured(
    file: &std::fs::File, work: &super::NativeMountMeterWork,
) -> Result<HostWindowsMetadata, NativeMountError> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Win32::Storage::FileSystem::{BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle};
    let information = projection_native_call(work, false, || {
        let mut information = BY_HANDLE_FILE_INFORMATION::default();
        // SAFETY: one live held handle and fixed-size output, no std metadata retries.
        unsafe { GetFileInformationByHandle(
            windows::Win32::Foundation::HANDLE(file.as_raw_handle()), &raw mut information,
        ) }.map_err(projection_sdk_io_error)?;
        Ok(information)
    })?;
    let ticks = |value: windows::Win32::Foundation::FILETIME| {
        i64::try_from(u64::from(value.dwHighDateTime) << 32 | u64::from(value.dwLowDateTime))
            .map_err(|error| NativeMountError::Io(std::io::Error::other(error)))
    };
    Ok(HostWindowsMetadata {
        identity: crate::NativeRootIdentity {
            device: u64::from(information.dwVolumeSerialNumber),
            object: u64::from(information.nFileIndexHigh) << 32 | u64::from(information.nFileIndexLow),
        },
        links: None,
        size: u64::from(information.nFileSizeHigh) << 32 | u64::from(information.nFileSizeLow),
        attributes: information.dwFileAttributes & !(FILE_ATTRIBUTE_ARCHIVE.0 | FILE_ATTRIBUTE_NORMAL.0),
        created: ticks(information.ftCreationTime)?,
        modified: ticks(information.ftLastWriteTime)?,
    })
}

fn finish_projected_file(
    file: &acyclic_native_runtime::NativeFile, entry: &ProjectedEntry,
    work: &super::NativeMountMeterWork,
) -> Result<(), NativeMountError> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Win32::Storage::FileSystem::{FileBasicInfo, FILE_BASIC_INFO, SetFileInformationByHandle};
    let meter = work.native_materialization_work().runtime_meter_owner();
    let size = u64::try_from(entry.info.FileSize).map_err(|_| NativeMountError::Demand(crate::DemandError::InvalidRequest))?;
    wait_native_unit_measured(file.set_len_bounded_async(size, Arc::clone(&meter))
        .map_err(projection_io_error)?, work)?;
    let information = ordinary_basic_information(entry);
    wait_native_unit_measured(file.control_bounded_async(move |file, meter| {
        meter.admit_write(0)?;
        // SAFETY: admitted, ordered held handle and fixed-size input.
        unsafe { SetFileInformationByHandle(
            windows::Win32::Foundation::HANDLE(file.as_raw_handle()), FileBasicInfo,
            (&raw const information).cast(), size_of::<FILE_BASIC_INFO>() as u32,
        ) }.map_err(projection_sdk_io_error)
    }, Arc::clone(&meter)).map_err(projection_io_error)?, work)?;
    wait_native_unit_measured(file.sync_bounded_async(acyclic_native_runtime::Durability::Full, meter)
        .map_err(projection_io_error)?, work)
}

/// The real parent duplicate retained by the existing accepted rename control.
struct ProjectionParentDescriptor {
    file: Option<std::fs::File>,
    close: Option<crate::materializer::NativeCleanupWrite>,
}

impl Drop for ProjectionParentDescriptor {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            let close = self.close.take().expect("parent duplicate close reserved before duplication");
            let _ = close.attempt_recorded(|| drop(file));
        }
    }
}

fn publish_projected_file(
    file: &acyclic_native_runtime::NativeFile, parent: &cap_std::fs::Dir,
    leaf: &[u16], work: &super::NativeMountMeterWork,
) -> Result<(), NativeMountError> {
    use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _};
    use windows::Wdk::Storage::FileSystem::{
        FILE_INFORMATION_CLASS, FILE_RENAME_INFORMATION, NtSetInformationFile,
    };
    use windows::Win32::Foundation::{HANDLE, DuplicateHandle, DUPLICATE_SAME_ACCESS};
    use windows::Win32::System::Threading::GetCurrentProcess;
    let mut held_parent = ProjectionParentDescriptor {
        file: None,
        close: Some(work.native_materialization_work().allocation_work().reserve_cleanup_write()?),
    };
    held_parent.file = Some(projection_native_call(work, false, || {
        let mut duplicated = HANDLE::default();
        // SAFETY: the actual held parent is duplicated for the accepted control, so unwind cannot retire it before native completion.
        unsafe {
            let process = GetCurrentProcess();
            DuplicateHandle(process, HANDLE(parent.as_raw_handle()), process,
                &raw mut duplicated, 0, false, DUPLICATE_SAME_ACCESS)
                .map_err(projection_sdk_io_error)?;
            Ok(std::fs::File::from_raw_handle(duplicated.0))
        }
    })?);
    let name_bytes = leaf.len().checked_mul(2).ok_or(crate::WorkError::Overflow)?;
    let offset = std::mem::offset_of!(FILE_RENAME_INFORMATION, FileName);
    let total = offset.checked_add(name_bytes).ok_or(crate::WorkError::Overflow)?
        .max(size_of::<FILE_RENAME_INFORMATION>());
    let mut buffer = ProjectionBuffer::new(total.div_ceil(8), work)?;
    buffer.as_mut().resize(total.div_ceil(8), 0_u64);
    let delta = crate::WorkCounters { bytes_copied: name_bytes as u64, ..crate::WorkCounters::UNCHARGED };
    let mut host_work = &work.native_materialization_work();
    host_work.admit(delta).map_err(detach_work_error)?;
    let information = buffer.as_mut().as_mut_ptr().cast::<FILE_RENAME_INFORMATION>();
    // SAFETY: aligned backing covers the full native header and borrowed name.
    unsafe {
        (*information).Anonymous.ReplaceIfExists = false.into();
        (*information).RootDirectory = HANDLE(held_parent.file.as_ref().expect("actual admitted parent duplicate").as_raw_handle());
        (*information).FileNameLength = u32::try_from(name_bytes).map_err(|_| crate::WorkError::Overflow)?;
        std::ptr::copy_nonoverlapping(leaf.as_ptr(),
            information.cast::<u8>().add(offset).cast::<u16>(), leaf.len());
    }
    host_work.record(delta).map_err(detach_work_error)?;
    let length = u32::try_from(total).map_err(|_| crate::WorkError::Overflow)?;
    let meter = work.native_materialization_work().runtime_meter_owner();
    wait_native_unit_measured(file.control_bounded_async(move |file, meter| {
        meter.admit_write(0)?;
        let mut status_block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
        // SAFETY: the control owns aligned information and the duplicated parent until this exact same-driver request has completed.
        let status = unsafe { NtSetInformationFile(
            HANDLE(file.as_raw_handle()), &raw mut status_block,
            buffer.as_slice().as_ptr().cast(), length,
            FILE_INFORMATION_CLASS(65), // FileRenameInformationEx, flags NONE
        ) };
        drop(held_parent);
        if status.is_ok() { Ok(()) } else {
            // SAFETY: pure translation of this actual native result.
            Err(std::io::Error::from_raw_os_error(
                unsafe { windows::Win32::Foundation::RtlNtStatusToDosError(status) } as i32,
            ))
        }
    }, meter).map_err(projection_io_error)?, work)
}

/// Startup writes use the same positional writer and controls, without a second staged name.
fn write_directly(
    projection: Projection<'_>, path: &MountPath, parent: &cap_std::fs::Dir,
    leaf: &[u16], entry: &ProjectedEntry, placed: &mut Option<Owned>,
    record: &mut impl FnMut(&Owned) -> Result<(), NativeMountError>,
) -> Result<Placed, NativeMountError> {
    let mut writer = match ProjectedNativeWriter::create(parent, leaf, projection.work, projection.custody,
        |owner| record(&Owned::ProvisionalFile(owner.clone()))) {
        Ok(writer) => writer,
        Err(NativeMountError::Io(error)) if matches!(error.kind(),
            std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::NotFound
                | std::io::ErrorKind::NotADirectory) => return Ok(Placed::Nothing),
        Err(error) => return Err(error),
    };
    let written = (|| {
        if !write_listed_content(projection, path, entry, writer.file())? { return Ok(Placed::Retry); }
        finish_projected_file(writer.file(), entry, projection.work)?;
        crate::native_host::keep_windows_created_file_with_work(
            writer.file().as_file(), &mut &projection.work.native_materialization_work(),
        ).map_err(detach_work_error)?;
        writer.owner.delete_on_close.store(false, std::sync::atomic::Ordering::Release);
        writer.provisional = false;
        writer.retire()?;
        writer.owner.retain_metadata_control(projection.work)?;
        let owned = Owned::File(writer.owner.final_state(projection.work)?, writer.owner.clone());
        record(&owned)?;
        *placed = Some(owned.clone());
        crate::native_host::sync_directory_held_with_work(
            parent, &mut &projection.work.native_materialization_work(),
        ).map_err(detach_work_error)?;
        Ok(Placed::Owned(owned))
    })();
    let retired = writer.retire();
    match written {
        Err(error) => Err(error),
        Ok(placed) => retired.map(|()| placed),
    }
}

/// Only FILE_CREATE establishes ownership.
struct StagingDirectoryCustody {
    // Field order keeps original credit live through this handle's real close.
    directory: Option<cap_std::fs::Dir>,
    name: ProjectionBuffer<u16>,
    identity: Option<crate::NativeRootIdentity>,
    cleanup: crate::materializer::NativeWorkReservation,
    temporary_close: crate::materializer::NativeWorkReservation,
    original_close: crate::materializer::NativeWorkReservation,
}

impl StagingDirectoryCustody {
    fn close_directory(&mut self) {
        if self.directory.is_some() {
            self.original_close.started_drop_io(crate::WorkCounters {
                backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
            });
            drop(self.directory.take());
        }
    }
}

impl Drop for StagingDirectoryCustody {
    fn drop(&mut self) { self.close_directory(); }
}

/// The directory beside the projection at `root` where its files are written before they take their names in it; made on first use.
fn with_staging_directory<T>(
    root: &std::path::Path, work: &super::NativeMountMeterWork,
    expected_root: crate::NativeRootIdentity,
    custody: &Mutex<Option<StagingDirectoryCustody>>,
    operation: impl FnOnce(&cap_std::fs::Dir) -> Result<T, NativeMountError>,
) -> Result<T, NativeMountError> {
    let mut host_work = &work.native_materialization_work();
    {
        let mut owned = lock_recover(custody);
        if let Some(stage) = owned.as_mut() {
            let directory = stage.directory.as_ref().ok_or(NativeMountError::Lifecycle("staging namespace data is physically closed"))?;
            let identity = crate::native_host::directory_identity_with_work(directory, &mut host_work)
                .map_err(detach_work_error)?;
            if identity.device != expected_root.device || stage.identity.is_some_and(|expected| expected != identity) {
                return Err(NativeMountError::Demand(crate::DemandError::StaleSource));
            }
            stage.identity = Some(identity);
            // Borrow only for this synchronous creator leaf.
            return operation(directory);
        }
    }
    let (_root_directory, root_identity) = crate::native_host::open_root_directory_identity_with_work(
        root, &mut host_work,
    ).map_err(detach_work_error)?;
    if root_identity != expected_root {
        return Err(NativeMountError::Demand(crate::DemandError::StaleSource));
    }
    let parent = crate::native_host::open_root_directory_with_work(
        destination_parent(root).map_err(detach_work_error)?, &mut host_work,
    ).map_err(detach_work_error)?;
    let leaf = root.file_name().unwrap_or_default();
    let mut units = 1_usize + ".acyclic-staging".len();
    for scalar in char::decode_utf16(leaf.encode_wide()) {
        let delta = crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED };
        host_work.admit(delta).map_err(detach_work_error)?;
        units = units.checked_add(scalar.unwrap_or(char::REPLACEMENT_CHARACTER).len_utf16())
            .ok_or(crate::WorkError::Overflow)?;
        host_work.record(delta).map_err(detach_work_error)?;
    }
    let mut name = ProjectionBuffer::new(units, work)?;
    let delta = crate::WorkCounters { bytes_encoded: units as u64 * 2, ..crate::WorkCounters::UNCHARGED };
    host_work.admit(delta).map_err(detach_work_error)?;
    name.as_mut().push(46);
    for scalar in char::decode_utf16(leaf.encode_wide()) {
        let mut encoded = [0; 2];
        name.as_mut().extend_from_slice(scalar.unwrap_or(char::REPLACEMENT_CHARACTER).encode_utf16(&mut encoded));
    }
    name.as_mut().extend(".acyclic-staging".encode_utf16());
    host_work.record(delta).map_err(detach_work_error)?;
    let mut owned = lock_recover(custody);
    if owned.is_none() {
        // These are the actual bounded held-capability controls, not guessed tree costs: reopen, identity and reparse probes; two possible dispositions; then close the reopened and original handles.
        let cleanup = work.native_materialization_work().allocation_work().reserve_work(crate::WorkCounters {
            backend_read_operations: 3,
            backend_write_operations: 2,
            ..crate::WorkCounters::UNCHARGED
        }).map_err(detach_work_error)?;
        let temporary_close = work.native_materialization_work().allocation_work().reserve_work(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(detach_work_error)?;
        let original_close = work.native_materialization_work().allocation_work().reserve_work(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(detach_work_error)?;
        let directory = cap_std::fs::Dir::from_std_file(
            crate::native_host::create_windows_new_directory_with_work(&parent, name.as_slice(), &mut host_work)
                .map_err(detach_work_error)?,
        );
        // Publish the real capability before any fallible identity admission.
        *owned = Some(StagingDirectoryCustody { directory: Some(directory), name, identity: None, cleanup, temporary_close, original_close });
    }
    let stage = owned.as_mut().expect("created or retained staging custody");
    let directory = stage.directory.as_ref().ok_or(NativeMountError::Lifecycle("staging namespace data is physically closed"))?;
    let identity = crate::native_host::directory_identity_with_work(directory, &mut host_work)
        .map_err(detach_work_error)?;
    if identity.device != root_identity.device || stage.identity.is_some_and(|expected| expected != identity) {
        return Err(NativeMountError::Demand(crate::DemandError::StaleSource));
    }
    stage.identity = Some(identity);
    operation(directory)
}

/// Where the projection at `root` stages its files: a hidden sibling on the same volume, which the original Open.Link rename can cross.
#[cfg(test)]
fn staging_path(root: &std::path::Path) -> PathBuf {
    let name = root
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    root.with_file_name(format!(".{name}.acyclic-staging"))
}

/// Move the canonical packet into the same ordered native operation.
fn write_source_chunk(
    projection: Projection<'_>, file: &acyclic_native_runtime::NativeFile,
    offset: u64, packet: crate::kernel::OwnedSourceChunk,
) -> Result<(), NativeMountError> {
    let packet = RetainedBuffer::source_chunk(packet, projection)?;
    let meter = projection.work.native_materialization_work().runtime_meter_owner();
    let completion = file.control_bounded_async(move |file, meter| {
        acyclic_native_runtime::write_all_at_measured_held(
            file, offset, &std::ops::Deref::deref(&packet).bytes, meter,
        )
    }, meter).map_err(projection_io_error)?;
    let written = wait_native_unit_measured(completion, projection.work);
    let retired = projection.work.native_materialization_work().record_admitted(crate::WorkCounters::UNCHARGED).map_err(detach_work_error);
    written.and(retired)
}

/// Writes the content the listing named for `entry` at `path` to `file`; `false` when the source changed or lost it since.
fn write_listed_content(
    projection: Projection<'_>, path: &MountPath, entry: &ProjectedEntry,
    file: &acyclic_native_runtime::NativeFile,
) -> Result<bool, NativeMountError> {
    let size = u64::try_from(entry.info.FileSize).map_err(|_| crate::WorkError::Overflow)?;
    let source_work = projection.work.clone();
    let mut failure = None;
    let mut delivered = 0_u64;
    let mut sink = |offset: u64, packet: crate::kernel::OwnedSourceChunk| {
        let length = packet.bytes.len() as u64;
        let written = write_source_chunk(projection, file, offset, packet).and_then(|()| {
            delivered = delivered.checked_add(length).ok_or(crate::WorkError::Overflow)?;
            Ok(())
        });
        if let Err(error) = written {
            // The source sink interface does not own a native I/O error.
            failure = Some(error);
            return Err(MountSourceError::Stale);
        }
        Ok(())
    };
    let read = match entry.facts.pin {
        Some(pin) => projection.source.read_pinned_with_meter(
            path, pin, 0, size, WRITE_CHUNK_BYTES, &mut sink, &source_work,
        ),
        None => (|| {
            let mut offset = 0_u64;
            while offset < size {
                let count = u32::try_from(size - offset).unwrap_or(u32::MAX).min(WRITE_CHUNK_BYTES);
                let packet = projection.source.read_range_with_meter(path, offset, count, &source_work)?;
                let length = packet.bytes.len() as u64;
                if length == 0 {
                    let bytes = packet.logical_owned_bytes;
                    let claimed = !packet.is_intrinsic();
                    drop(packet);
                    if claimed {
                        projection.work.meter().lock().release(bytes).map_err(super::lazy::mount_demand_error)?;
                    }
                    return Err(MountSourceError::Stale);
                }
                sink(offset, packet)?;
                offset = offset.checked_add(length).ok_or(crate::WorkError::Overflow)?;
            }
            Ok(())
        })(),
    };
    match (read, failure) {
        (_, Some(error)) => Err(error),
        (Ok(()), None) => Ok(delivered == size),
        (Err(MountSourceError::Stale | MountSourceError::NotFound), None) => Ok(false),
        (Err(error), None) => Err(detach_source_error(error)),
    }
}

fn projection_status_error(status: windows::Win32::Foundation::NTSTATUS) -> std::io::Error {
    if status.0 == 0xC000_0056_u32 as i32 {
        return std::io::Error::from_raw_os_error(SHARING_VIOLATION);
    }
    // SAFETY: this translates only the actual NT result, without another I/O.
    std::io::Error::from_raw_os_error(
        unsafe { windows::Win32::Foundation::RtlNtStatusToDosError(status) } as i32,
    )
}

fn open_projection_leaf(
    parent: &cap_std::fs::Dir, leaf: &[u16], deny_writers: bool,
    work: &super::NativeMountMeterWork,
) -> Result<std::fs::File, NativeMountError> {
    use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _};
    use windows::Wdk::Storage::FileSystem::{
        FILE_OPEN, FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT, NtCreateFile,
    };
    use windows::Win32::Foundation::{HANDLE, OBJ_CASE_INSENSITIVE, UNICODE_STRING};
    use windows::Win32::Storage::FileSystem::{
        DELETE, FILE_READ_ATTRIBUTES, SYNCHRONIZE, FILE_ATTRIBUTE_NORMAL,
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };
    let length = u16::try_from(leaf.len().checked_mul(2).ok_or(crate::WorkError::Overflow)?)
        .map_err(|_| NativeMountError::Demand(crate::DemandError::InvalidRequest))?;
    let name = UNICODE_STRING { Length: length, MaximumLength: length,
        Buffer: windows::core::PWSTR(leaf.as_ptr().cast_mut()) };
    let attributes = windows::Wdk::Foundation::OBJECT_ATTRIBUTES {
        Length: size_of::<windows::Wdk::Foundation::OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: HANDLE(parent.as_raw_handle()), ObjectName: &raw const name,
        Attributes: OBJ_CASE_INSENSITIVE, ..Default::default()
    };
    projection_native_call(work, false, || {
        let mut handle = HANDLE::default();
        let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
        // SAFETY: authenticated held parent, borrowed native leaf, and exact outputs remain live.
        let status = unsafe { NtCreateFile(
            &raw mut handle, DELETE | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            &raw const attributes, &raw mut block, None, FILE_ATTRIBUTE_NORMAL,
            FILE_SHARE_READ | FILE_SHARE_DELETE
                | if deny_writers { windows::Win32::Storage::FileSystem::FILE_SHARE_MODE(0) }
                    else { FILE_SHARE_WRITE },
            FILE_OPEN, FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT, None, 0,
        ) };
        if status.is_err() { return Err(projection_status_error(status)); }
        // SAFETY: successful native open transferred the new handle once.
        Ok(unsafe { std::fs::File::from_raw_handle(handle.0) })
    })
}

/// The ordinary file at `path`, held so no writer can share it, if it is still `state`; `None` when something else is there.
fn held_while(
    parent: &cap_std::fs::Dir, leaf: &[u16], state: HostWindowsMetadata,
    work: &super::NativeMountMeterWork,
) -> Result<Option<std::fs::File>, NativeMountError> {
    let file = match open_projection_leaf(parent, leaf, true, work) {
        Ok(file) => file,
        Err(NativeMountError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    Ok((handle_state_measured(&file, work)? == state).then_some(file))
}

/// The unchanged original creator under a real no-share-write window.
fn held_original_while(
    original: &PhysicalEntry, expected: HostWindowsMetadata,
    work: &super::NativeMountMeterWork,
) -> Result<Option<std::fs::File>, NativeMountError> {
    use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _};
    use windows::Win32::Storage::FileSystem::{
        DELETE, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE, FILE_SHARE_READ, ReOpenFile,
    };
    let capability = lock_recover(&original.file);
    let Some(capability) = capability.as_ref() else { return Ok(None); };
    let file = projection_native_call(work, false, || {
        // SAFETY: reopen the exact original creator, excluding WRITE sharing.
        let handle = unsafe { ReOpenFile(
            windows::Win32::Foundation::HANDLE(capability.as_raw_handle()),
            DELETE.0 | FILE_READ_ATTRIBUTES.0, FILE_SHARE_READ | FILE_SHARE_DELETE,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
        ) }.map_err(projection_sdk_io_error)?;
        Ok(unsafe { std::fs::File::from_raw_handle(handle.0) })
    })?;
    Ok((handle_state_measured(&file, work)? == expected).then_some(file))
}

/// Current READ authorization plus the original live SDK namespace permits private cache hydration.
fn verify_projection_namespace_work(projection: Projection<'_>) -> Result<(), NativeMountError> {
    if projection.context.0.is_null() {
        return Err(NativeMountError::Lifecycle("cache publication has no original live SDK namespace"));
    }
    projection.work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)?;
    if let Some(guard) = projection.source.native_callback_guard() {
        super::adapter::CallbackRuntime::existing()?
            .verify_native_callback_guard(guard.as_ref(), projection.work)?;
    }
    Ok(())
}



fn delete_file_while_owned(
    parent: &cap_std::fs::Dir, leaf: &[u16], state: HostWindowsMetadata,
    work: &super::NativeMountMeterWork, owner: &PhysicalEntry,
) -> Result<bool, NativeMountError> {
    let Some(_file) = held_while(parent, leaf, state, work)? else { return Ok(false); };
    retire_physical_entry(owner, work, Some(work.native_materialization_work().cancellation()))?;
    crate::native_host::sync_directory_held_with_work(parent, &mut &work.native_materialization_work())
        .map_err(detach_work_error)?;
    Ok(true)
}




/// Deletes what `ProjFS` manages at `path` unless the user modified it.
fn delete_unowned(
    projection: Projection<'_>, path: &MountPath,
) -> Result<Release, NativeMountError> {
    with_projection_relative_name(path, projection.work, |relative| {
        match prj_delete(projection, relative, PRJ_UPDATE_ALLOW_READ_ONLY) {
            Ok(()) => Ok(Release::Done),
            Err(error) if native_mount_error_code(&error) == Some(HR_SHARING_VIOLATION) => Ok(Release::Busy),
            Err(error) if native_mount_error_code(&error).is_some_and(|code| [
                HR_FILE_NOT_FOUND, HR_PATH_NOT_FOUND, HR_VIRTUALIZATION_INVALID_OPERATION,
                HR_DIRECTORY_NOT_EMPTY,
            ].contains(&code)) => Ok(Release::Done),
            Err(error) => Err(error),
        }
    })
}


fn replace_written(
    projection: Projection<'_>, path: &MountPath, written: &WrittenEntry,
    committed: &mut Option<WrittenEntry>,
    record: &mut impl FnMut(&WrittenEntry) -> Result<(), NativeMountError>,
) -> Result<Replaced, NativeMountError> {
    let basis = ReadBasis::sample(projection)?;
    let work = projection.work.clone();
    let current = match projection.source.lookup_pinned_with_meter(path, &work) {
        Ok(found) => found,
        Err(MountSourceError::NotFound) => None,
        Err(error) => return Err(detach_source_error(error)),
    };
    rewrite(projection, path, written.owned.clone(), current, basis, committed, record)
}

fn rewrite(
    projection: Projection<'_>, path: &MountPath, owned: Option<Owned>,
    current: Option<(MountLookup, Option<MountContentPin>)>, basis: Option<ReadBasis>,
    committed: &mut Option<WrittenEntry>,
    record: &mut impl FnMut(&WrittenEntry) -> Result<(), NativeMountError>,
) -> Result<Replaced, NativeMountError> {
    if let Some(Owned::File(state, original)) = owned.as_ref()
        && let Some((lookup, pin)) = current
        && lookup.node.kind == MountNodeKind::Regular
    {
        return replace_ordinary(projection, path, *state, original, lookup, pin, basis, committed, record);
    }
    if owned.is_some() {
        if release(projection, path, owned)? == Release::Busy { return Ok(Replaced::Busy); }
    } else {
        let released = with_projection_relative_name(path, projection.work, |relative| {
            prj_delete(projection, relative, PRJ_UPDATE_ALLOW_READ_ONLY)
        });
        match released {
            Ok(()) => {}
            Err(error) if native_mount_error_code(&error).is_some_and(|code|
                [HR_FILE_NOT_FOUND, HR_PATH_NOT_FOUND].contains(&code)) => {}
            Err(error) if native_mount_error_code(&error) == Some(HR_SHARING_VIOLATION) => return Ok(Replaced::Busy),
            Err(error) if native_mount_error_code(&error).is_some_and(|code|
                [HR_VIRTUALIZATION_INVALID_OPERATION, HR_DIRECTORY_NOT_EMPTY].contains(&code)) =>
                return Ok(Replaced::Released),
            Err(error) => return Err(error),
        }
    }
    let Some((lookup, pin)) = current.filter(|(lookup, _)| lookup.node.kind != MountNodeKind::Directory)
        else { return Ok(Replaced::Released); };
    let (Some(parent), Some(component)) = (owned_parent(path, projection)?, path.components().last())
        else { return Ok(Replaced::Released); };
    let component = source_copy_bytes(projection, component)?;
    let Some(entry) = projected_entry(projection, &parent, component, lookup, pin)?
        else { return Ok(Replaced::Released); };
    let mut placed = None;
    let result = write_entry(projection, path, &entry, &mut placed, &mut |owner|
        record(&WrittenEntry { file_id: lookup.node.file_id, owned: Some(owner.clone()), facts: Some(entry.facts), basis }));
    if let Some(owned) = placed {
        *committed = Some(WrittenEntry {
            file_id: lookup.node.file_id, owned: Some(owned), facts: Some(entry.facts), basis,
        });
    }
    Ok(match result? {
        Placed::Nothing => Replaced::Released,
        Placed::Retry => Replaced::Busy,
        placed => Replaced::Rewritten(WrittenEntry {
            file_id: lookup.node.file_id, owned: placed.owned(), facts: Some(entry.facts), basis,
        }),
    })
}

fn replace_ordinary(
    projection: Projection<'_>, path: &MountPath, state: HostWindowsMetadata, original: &PhysicalEntry,
    lookup: MountLookup, pin: Option<MountContentPin>, basis: Option<ReadBasis>,
    committed: &mut Option<WrittenEntry>,
    record: &mut impl FnMut(&WrittenEntry) -> Result<(), NativeMountError>,
) -> Result<Replaced, NativeMountError> {
    let (Some(parent), Some(component)) = (owned_parent(path, projection)?, path.components().last())
        else { return Ok(Replaced::Released); };
    let component = source_copy_bytes(projection, component)?;
    let Some(entry) = projected_entry(projection, &parent, component, lookup, pin)?
        else { return Ok(Replaced::Released); };
    let mut placed = None;
    let result = write_ordinary(projection, path, &entry, Some((state, original)), &mut placed, &mut |owner|
        record(&WrittenEntry { file_id: lookup.node.file_id, owned: Some(owner.clone()), facts: Some(entry.facts), basis }));
    if let Some(owned) = placed {
        *committed = Some(WrittenEntry {
            file_id: lookup.node.file_id, owned: Some(owned), facts: Some(entry.facts), basis,
        });
    }
    Ok(match result? {
        Placed::Owned(owned) => Replaced::Rewritten(WrittenEntry {
            file_id: lookup.node.file_id, owned: Some(owned), facts: Some(entry.facts), basis,
        }),
        Placed::Retry => Replaced::Busy,
        Placed::Nothing | Placed::Unowned => Replaced::Released,
    })
}

/// A virtualization context moved to the projection worker.
struct SendContext(PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT);

// SAFETY: see `SendContext`; its lifetime is bounded by `Projector::finish`.
unsafe impl Send for SendContext {}

struct SendThreadHandle(std::os::windows::io::RawHandle);
// SAFETY: this is the actual transferred std thread handle, not an SDK token.
unsafe impl Send for SendThreadHandle {}

/// Borrowed Arc runtime capability, valid until the actual worker join.
struct SendRuntime(*const Runtime);
unsafe impl Send for SendRuntime {}

impl SendRuntime {
    fn run(self, projector: Arc<RetainedArcValue<Projector>>, context: SendContext) {
        // SAFETY: the original physical slot retains Runtime until this run body returns; only this same producer then enters retirement.
        let runtime = unsafe { &*self.0 };
        projector.run(runtime, context.0);
    }
}

fn has_pending_capture(
    failure: &Mutex<PostOperationFailures>, path: &MountPath, projection: Projection<'_>,
) -> Result<bool, NativeMountError> {
    let failure = lock_recover(failure);
    if failure.unreplayable_barrier { return Ok(true); }
    for (pending, capture) in failure.pending_captures.iter() {
        projection.work.native_materialization_work().charge(crate::WorkCounters {
            items_examined: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(detach_work_error)?;
        if pending == path || capture.subtree
            && projfs_path_suffix_with_work(path, pending, projection.work)?.is_some()
        { return Ok(true); }
    }
    Ok(false)
}



/// The root capability opened before SDK birth, not a later pathname adoption.
struct ProjectionRootCustody {
    directory: Option<cap_std::fs::Dir>,
    identity: crate::NativeRootIdentity,
    close: crate::materializer::NativeWorkReservation,
}




impl ProjectionRootCustody {
    fn close_directory(&mut self) {
        if self.directory.is_some() {
            self.close.started_drop_io(crate::WorkCounters {
                backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
            });
            drop(self.directory.take());
        }
    }
}

impl Drop for ProjectionRootCustody {
    fn drop(&mut self) { self.close_directory(); }
}


/// Preborn inline move slot of the existing producer, not Runtime/Projector state: neither can own an Arc backedge into its own physical Session.
struct WindowsRetirementIngress {
    state: Mutex<WindowsRetirementState>,
    wake: Condvar,
}

struct WindowsRetirementState {
    context: Option<SendContext>,
    abort: bool,
    drained: bool,
    requested: bool,
    retiring: bool,
    physical_ready: bool,
    allow_return: bool,
    last_public_drop: bool,
    terminal: bool,
    owner: Option<ProjFsSession>,
    /// Actual packet custody transferred to the in-flight physical stack loan.
    loan_custody: Option<(bool, bool)>,
    failure: Option<NativeMountError>,
    root_identity: Option<crate::NativeRootIdentity>,
    namespace_detached: bool,
    completed_callback_work: crate::WorkCounters,
    callback_failure: Option<NativeMountCallbackFailure>,
    original_cancellation: crate::CancellationToken,
}

impl WindowsRetirementIngress {
    fn new(identity: crate::NativeRootIdentity, cancellation: crate::CancellationToken) -> Self {
        Self { state: Mutex::new(WindowsRetirementState {
            context: None, abort: false, drained: false, requested: false, terminal: false,
            retiring: false, physical_ready: false, allow_return: false, last_public_drop: false,
            owner: None, loan_custody: None, failure: None, root_identity: Some(identity), namespace_detached: false,
            completed_callback_work: crate::WorkCounters::UNCHARGED, callback_failure: None, original_cancellation: cancellation,
        }), wake: Condvar::new() }
    }

    fn run(&self, runtime: SendRuntime, projector: Arc<RetainedArcValue<Projector>>) {
        let context = {
            let mut state = lock_recover(&self.state);
            while state.context.is_none() && !state.abort {
                state = self.wake.wait(state).unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            state.context.take()
        };
        if let Some(context) = context { runtime.run(projector.clone(), context); }
        {
            let mut state = lock_recover(&self.state);
            state.drained = true;
            self.wake.notify_all();
        }
        loop {
            let mut owner = {
                let mut state = lock_recover(&self.state);
                while !state.requested || state.owner.is_none() {
                    state = self.wake.wait(state).unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                state.requested = false;
                state.loan_custody = Some(state.owner.as_ref().expect("original held driver").retirement_ownership());
                state.owner.take().expect("actual original driver restored to its prepaid slot")
            };
            let outcome = if owner.physically_retired() { Ok(()) } else { owner.stop_accepted_cleanup(self) };
            let mut state = lock_recover(&self.state);
            state.namespace_detached = owner.sdk_stop_confirmed;
            state.completed_callback_work = owner.completed_callback_work();
            if let Some(failure) = owner.take_callback_failure() { state.callback_failure = Some(failure); }
            state.failure = outcome.err();
            state.physical_ready = owner.physically_retired();
            state.owner = Some(owner);
            state.loan_custody = None;
            self.wake.notify_all();
            if state.physical_ready {
                while !state.allow_return && !state.last_public_drop {
                    state = self.wake.wait(state).unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                if state.allow_return { return; }
                let mut owner = state.owner.take().expect("actual terminal-tail owner");
                drop(state);
                let closed = owner.close_own_worker_handle();
                let mut state = lock_recover(&self.state);
                match closed {
                    Ok(()) => {
                        state.terminal = true;
                        state.owner = Some(owner);
                        self.wake.notify_all();
                        return;
                    }
                    Err(error) => {
                        state.physical_ready = false;
                        state.failure = Some(error);
                        state.owner = Some(owner);
                        self.wake.notify_all();
                    }
                }
            }
        }
    }

    fn request(&self, projector: &Projector) {
        lock_recover(&projector.state).stopping = true;
        projector.changed.notify_all();
        let mut state = lock_recover(&self.state);
        if let Some(runtime) = state.owner.as_ref().and_then(|owner| owner.runtime.as_ref()) {
            runtime.callbacks.close_admission();
        }
        state.requested = true;
        state.retiring = true;
        state.abort = true;
        self.wake.notify_all();
    }
}

/// Borrowed stack factory guard.
struct WindowsStartupAbort<'a> {
    ingress: &'a WindowsRetirementIngress,
    projector: &'a Projector,
    armed: bool,
}

impl Drop for WindowsStartupAbort<'_> {
    fn drop(&mut self) {
        if self.armed { self.ingress.request(self.projector); }
    }
}

/// The actual issuer's successful SDK marking operation and supplied instance GUID.
#[derive(Clone, Copy)]
pub(super) struct ProjFsNamespaceQualification {
    root_identity: crate::NativeRootIdentity,
    instance_id: GUID,
}

/// Moves the original funded namespace memo payloads out of the stopped physical driver.
pub(super) struct ProjFsNamespaceOutcome {
    capture_failure: Option<NativeMountError>,
    cleanup_failure: Option<NativeMountError>,
    written: MemoMap<MountPath, WrittenEntry>,
    pending: MemoMap<MountPath, WrittenEntry>,
    qualification: Option<ProjFsNamespaceQualification>,
    stage: Option<StagingDirectoryCustody>,
    captures: Option<Arc<RetainedArcValue<Mutex<PostOperationFailures>>>>,
    continuation_ready: bool,
}

impl ProjFsNamespaceOutcome {
    pub(super) fn qualification(&self) -> Option<ProjFsNamespaceQualification> { self.qualification }
    pub(super) fn continuation_ready(&self) -> bool { self.continuation_ready }
    pub(super) fn capture_failure(&self) -> Option<&NativeMountError> { self.capture_failure.as_ref() }
    pub(super) fn take_capture_failures(&mut self) -> Option<(NativeMountError, Option<NativeMountError>)> {
        match self.capture_failure.take() {
            Some(primary) => Some((primary, self.cleanup_failure.take())),
            None => self.cleanup_failure.take().map(|primary| (primary, None)),
        }
    }
    pub(super) fn pending_capture_count(&self) -> usize {
        self.captures.as_ref().map_or(0, |queue|
            lock_recover(queue).pending_captures.entries.items.len())
    }
    pub(super) fn first_cleanup_residue(&self) -> Option<(i32, u32)> {
        self.written.iter().chain(self.pending.iter()).find_map(|(_, written)| {
            let Some(Owned::Link(owner)) = written.owned.as_ref() else { return None; };
            lock_recover(&owner.namespace_residue).as_ref().map(|residue|
                (residue.hresult.0, residue.update_causes.0 as u32))
        })
    }
}

/// One process-owned `ProjFS` virtualization context.
pub(super) struct ProjFsSession {
    context: Option<PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT>,
    runtime: Option<Arc<RetainedArcValue<Runtime>>>,
    root_custody: Option<ProjectionRootCustody>,
    /// Original authored execution until drain, then the same identity's metadata-only physical frame through every remaining cleanup owner.
    original_stop_work: Option<super::NativeMountMeterWork>,
    /// One known SDK detach, withheld before the SDK can own a context.
    sdk_stop_credit: Option<crate::materializer::NativeWorkReservation>,
    sdk_info_credit: Option<crate::materializer::NativeWorkReservation>,
    namespace_qualification: Option<ProjFsNamespaceQualification>,
    /// One join of the sole worker that START may create.
    worker_join_credit: Option<crate::materializer::NativeWorkReservation>,
    ingress: Option<Arc<RetainedArcValue<WindowsRetirementIngress>>>,
    projector_control: Option<Arc<RetainedArcValue<Projector>>>,
    worker_handle_close_credit: Option<crate::materializer::NativeWorkReservation>,
    worker_raw_handle: Option<SendThreadHandle>,
    authored_capture_attempted: bool,
    capture_residue: Option<Arc<RetainedArcValue<Mutex<PostOperationFailures>>>>,
    namespace_stage: Option<StagingDirectoryCustody>,
    cleanup_failure: Option<NativeMountError>,
    capture_failure: Option<NativeMountError>,
    retirement: Option<super::NativeMountRetirement>,
    root_mark_attempted: bool,
    sdk_stop_confirmed: bool,
    completed_callback_work: crate::WorkCounters,
    first_callback_failure: Mutex<Option<NativeMountCallbackFailure>>,
}

// SAFETY: opaque ProjFS tokens and native owners move only into the same preborn producer.
unsafe impl Send for ProjFsSession {}
// SAFETY: shared methods borrow synchronized controls; SDK stop and handle retirement require exclusive session access.
unsafe impl Sync for ProjFsSession {}

impl ProjFsSession {
    pub(super) fn start(
        request: &NativeMountRequest,
        source: Arc<dyn MountFilesystem>,
    ) -> Result<Self, DriverStartFailure> {
        let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .map_err(|(cause, _)| DriverStartFailure::from(detach_work_error(cause)))?;
        Self::start_with_work(request, source, &work)
    }

    /// Starts the same eager projector using the original lifecycle budget, ledger, and cancellation.
    pub(super) fn start_with_work(
        request: &NativeMountRequest,
        source: Arc<dyn MountFilesystem>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Self, DriverStartFailure> {
        Self::start_with_meter(request, source, &work.native_mount_work())
    }

    /// Enters the actual SDK with the caller's original shared engine.
    pub(super) fn start_with_meter(
        request: &NativeMountRequest, source: Arc<dyn MountFilesystem>,
        work: &super::NativeMountMeterWork,
    ) -> Result<Self, DriverStartFailure> {
        let mut out = None;
        let mut retirement = Some(super::NativeMountRetirement {
            retained_source: Some(source.clone()), destination_guard: None, startup_source_watch: None,
            warm_namespace: None,
        });
        match Self::start_into(request, source, work, &mut out, &mut retirement, |_| Ok(())) {
            Ok(()) => Ok(out.take().expect("successful startup owns its real driver")),
            Err(error) => match out.take() {
                Some(session) => Err(DriverStartFailure::retaining_driver(error.error, super::DriverSession::ProjFs(session))),
                None => Err(error),
            },
        }
    }


    pub(super) fn start_into<F>(
        request: &NativeMountRequest, source: Arc<dyn MountFilesystem>,
        work: &super::NativeMountMeterWork, out: &mut Option<Self>,
        retirement: &mut Option<super::NativeMountRetirement>, prepare: F,
    ) -> Result<(), DriverStartFailure>
    where F: FnOnce(&mut super::NativeMountRetirement) -> Result<(), NativeMountError> {
        if out.is_some() {
            return Err(NativeMountError::Lifecycle("startup already retains a ProjFS driver").into());
        }
        let warm_namespace = retirement.as_ref().and_then(|packet| packet.warm_namespace);
        let callback_guard = source.native_callback_guard();
        let original_work = source.native_original_work().map_err(detach_source_error)?;
        if callback_guard.is_some() && original_work.is_none() {
            return Err(NativeMountError::Lifecycle("task source has no original callback work").into());
        }
        if original_work.as_ref().is_some_and(|original| !same_callback_work(original, work)) {
            return Err(NativeMountError::Lifecycle("source callback work differs from its installed invocation").into());
        }
        let meter = work;
        let materialization = work.native_materialization_work();
        materialization.admit(crate::WorkCounters::UNCHARGED).map_err(detach_work_error)?;
        let mut host_work = &materialization;
        let physical_materialization = materialization.allocation_work();
        let sdk_stop_credit = physical_materialization.reserve_work(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(detach_work_error)?;
        let worker_join_credit = physical_materialization.reserve_work(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(detach_work_error)?;
        let root_close = physical_materialization.reserve_work(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(detach_work_error)?;
        let sdk_info_credit = physical_materialization.reserve_work(crate::WorkCounters {
            backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(detach_work_error)?;
        let custody = crate::demand::NativeMountAllocationCustody::new_with_meter(work).map_err(detach_work_error)?;
        let callback_gate = CallbackGate::start_with_meter(work, &custody)?;
        let metadata_root = retained_arc_on_meter(
            HostRoot::open_with_work(&request.destination, &mut host_work).map_err(detach_work_error)?,
            &custody, meter,
        )?;
        let root_directory = reopen_for_delete_measured(
            &metadata_root.directory_handle(), &mut host_work, None,
        )?;
        let root_custody = ProjectionRootCustody {
            directory: Some(cap_std::fs::Dir::from_std_file(root_directory)),
            identity: metadata_root.identity(), close: root_close,
        };
        let root = clone_host_path_with_work(&request.destination, &custody, meter)?;
        let projector = {
            let policy = meter.meter().lock();
            Projector::new_with_work(&custody, &policy)
        };
        let (handler_budget, handler_cancellation) = {
            let policy = meter.meter().lock();
            (policy.budget, policy.cancellation().clone())
        };
        let runtime = Runtime {
            source,
            original_work,
            callback_guard,
            root,
            metadata_root,
            writable: request.writable,
            metadata_baselines: retained_arc_on_meter(Mutex::new(MemoMap::default()), &custody, meter)?,
            projection: retained_arc_on_meter(Mutex::new(ProjectionCache::default()), &custody, meter)?,
            projector: retained_arc_on_meter(projector, &custody, meter)?,
            metadata_probes: retained_arc_on_meter(Mutex::new(MemoMap::default()), &custody, meter)?,
            capture_probes: std::sync::RwLock::new(Vec::new()),
            flush_gate: Mutex::new(()),
            stop_meter: Mutex::new(super::NativeMountMeterActivation::default()),
            staging_custody: Mutex::new(None),
            metadata_edits: retained_arc_on_meter(FileLocks::default(), &custody, meter)?,
            post_operation_failure: retained_arc_on_meter(Mutex::new(PostOperationFailures::default()), &custody, meter)?,
            callbacks: callback_gate,
            handler_budget,
            handler_cancellation,
        };
        let runtime = retained_arc_on_meter(runtime, &custody, meter)?;
        let ingress = retained_arc_on_meter(
            WindowsRetirementIngress::new(runtime.metadata_root.identity(), materialization.cancellation().clone()), &custody, meter,
        )?;
        let worker_handle_close_credit = physical_materialization.reserve_work(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(detach_work_error)?;
        let projector_control = runtime.projector.clone();
        let session = Self {
            context: None, runtime: Some(runtime),
            original_stop_work: Some(work.clone()), sdk_stop_credit: Some(sdk_stop_credit),
            worker_join_credit: Some(worker_join_credit),
            sdk_info_credit: Some(sdk_info_credit), namespace_qualification: warm_namespace,
            root_custody: Some(root_custody),
            ingress: Some(ingress.clone()), projector_control: Some(projector_control.clone()),
            worker_handle_close_credit: Some(worker_handle_close_credit), worker_raw_handle: None,
            authored_capture_attempted: false, capture_residue: None, capture_failure: None,
            namespace_stage: None, cleanup_failure: None,
            retirement: None,
            root_mark_attempted: false, sdk_stop_confirmed: false,
            completed_callback_work: crate::WorkCounters::UNCHARGED,
            first_callback_failure: Mutex::new(None),
        };
        *out = Some(session);
        let session = out.as_mut().expect("logical driver installed before platform effect");
        let runtime = session.runtime.as_ref().expect("original preborn runtime");
        runtime.birth_worker_with_work(ingress.clone(), meter)?;
        session.retirement = retirement.take();
        let mut startup = WindowsStartupAbort { ingress: &ingress, projector: &projector_control, armed: true };
        if let Some(packet) = session.retirement.as_mut() { prepare(packet)?; }
        let entered = (|| {
            let runtime = session.runtime.as_ref().expect("stable admitted callback runtime");
            let (name, name_allocation) = crate::native_host::normalize_root_wide_with_work(
                &request.destination, &mut host_work,
            ).map_err(detach_work_error)?;
            let name = name;
            let result = runtime.projector.with_control_meter(runtime, meter, |meter| {
                installed_callback_work(runtime)?;
                verify_callback_work(runtime, meter)?;
                let root_ptr = PCWSTR::from_raw(name.as_ptr());
                let guid = guid_from_key(u128::from_le_bytes(request.mount_id.into_bytes()));
                let (marked_root, identity) = crate::native_host::open_root_directory_identity_with_work(
                    &runtime.root, &mut &meter.native_materialization_work(),
                ).map_err(detach_work_error)?;
                if identity != runtime.metadata_root.identity() {
                    return Err(crate::DemandError::StaleSource.into());
                }
                let tag = reparse_tag_held_measured(&marked_root, &mut &meter.native_materialization_work(), None)
                    .map_err(detach_work_error)?;
                match warm_namespace {
                    Some(issued) => {
                        // This original SDK-issued permission is retained by the old quiesced owner.
                        if issued.root_identity != identity || issued.instance_id != guid
                            || tag != Some(windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS)
                        {
                            return Err(NativeMountError::UnsafeRecoveryDestination { reparse_tag: tag });
                        }
                    }
                    None => {
                        if tag.is_some() {
                            return Err(NativeMountError::UnsafeRecoveryDestination { reparse_tag: tag });
                        }
                        projection_native_call(meter, true, || {
                            session.root_mark_attempted = true;
                            // SAFETY: the admitted native name remains live.
                            unsafe { PrjMarkDirectoryAsPlaceholder(root_ptr, PCWSTR::null(), None, &raw const guid) }
                                .map_err(projection_sdk_io_error)
                        })?;
                        // Actual Mark ACK issued permission even if a later START/info/initialization is denied.
                        session.namespace_qualification = Some(ProjFsNamespaceQualification {
                            root_identity: runtime.metadata_root.identity(), instance_id: guid,
                        });
                    }
                }
                let callbacks = callbacks();
                let mut notification = PRJ_NOTIFICATION_MAPPING {
                    NotificationBitMask: FILE_NOTIFICATIONS,
                    NotificationRoot: PCWSTR::from_raw(NOTIFICATION_ROOT.as_ptr()),
                };
                let mut options = PRJ_STARTVIRTUALIZING_OPTIONS {
                    Flags: PRJ_FLAG_USE_NEGATIVE_PATH_CACHE,
                    ..PRJ_STARTVIRTUALIZING_OPTIONS::default()
                };
                if request.writable {
                    options.NotificationMappings = &raw mut notification;
                    options.NotificationMappingsCount = 1;
                }
                let context_ptr = std::ptr::from_ref(&***runtime).cast::<c_void>();
                let (_started_root, identity) = crate::native_host::open_root_directory_identity_with_work(
                    &runtime.root, &mut &meter.native_materialization_work(),
                ).map_err(detach_work_error)?;
                if identity != runtime.metadata_root.identity() {
                    return Err(crate::DemandError::StaleSource.into());
                }
                installed_callback_work(runtime)?;
                verify_callback_work(runtime, meter)?;
                let started = projection_native_call(meter, true, || unsafe {
                    PrjStartVirtualizing(root_ptr, &raw const callbacks,
                        Some(context_ptr), Some(&raw const options))
                        .map_err(projection_sdk_io_error)
                });
                let context = match started {
                    Ok(context) => {
                        // Save the actual SDK return before preferring its callback's original typed cause.
                        session.context = Some(context);
                        lock_recover(&ingress.state).context = Some(SendContext(context));
                        ingress.wake.notify_all();
                        context
                    }
                    Err(error) => return Err(take_callback_failure(runtime).map_or(error, |failure| failure.cause)),
                };
                let info_credit = session.sdk_info_credit.as_mut().expect("SDK info credit withheld before START");
                let loan = info_credit.cleanup_loan(materialization.cancellation());
                loan.started_io(crate::WorkCounters {
                    backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED
                }).map_err(detach_work_error)?;
                let mut instance = PRJ_VIRTUALIZATION_INSTANCE_INFO::default();
                unsafe { PrjGetVirtualizationInstanceInfo(context, &raw mut instance) }
                    .map_err(projection_sdk_io_error)?;
                if instance.InstanceID != guid {
                    return Err(NativeMountError::Lifecycle("SDK instance differs from the original issued namespace"));
                }
                if let Some(failure) = take_callback_failure(runtime) { return Err(failure.cause); }
                let custody = crate::demand::NativeMountAllocationCustody::new_with_meter(work)?;
                let projection = Projection {
                    source: runtime.source.as_ref(), work: meter, custody: &custody, context,
                    root: &runtime.root, root_identity: runtime.metadata_root.identity(),
                    staging_custody: &runtime.staging_custody,
                    captures: &runtime.post_operation_failure, starting: true,
                };
                runtime.attach_workers_with_work(context, projection)?;
                runtime.projector.materialize(projection, &MountPath::root())?;
                runtime.projector.settle_measured(meter)
            });
            drop(name);
            drop(name_allocation);
            result
        })();
        startup.armed = entered.is_err();
        let physical = session.take_physical_owner();
        lock_recover(&ingress.state).owner = Some(physical);
        ingress.wake.notify_all();
        entered.map_err(DriverStartFailure::from)
    }

    pub(super) fn stop(&mut self) -> Result<(), NativeMountError> {
        let cancellation = match self.ingress.as_ref() {
            Some(ingress) => lock_recover(&ingress.state).original_cancellation.clone(),
            None => self.original_stop_work.as_ref().ok_or(NativeMountError::Lifecycle(
                "physical retirement requires its original producer",
            ))?.native_materialization_work().cancellation().clone(),
        };
        self.stop_with_cleanup_wait(&cancellation)
    }

    /// Uses the supplied token only to bound observation of the existing producer.
    pub(super) fn stop_measured(&mut self, work: &crate::NativeMaterializationWork) -> Result<(), NativeMountError> {
        self.stop_with_meter(&work.native_mount_work())
    }

    /// Uses only this driver's original work and accepted SDK/worker credits.
    pub(super) fn stop_with_cleanup_wait(
        &mut self, wait: &crate::CancellationToken,
    ) -> Result<(), NativeMountError> {
        wait.check().map_err(|_| detach_work_error(crate::NativeTreeMaterializationError::Cancelled))?;
        let ingress = self.ingress.as_ref().ok_or(NativeMountError::Lifecycle(
            "physical driver is retained by its original cleanup producer",
        ))?.clone();
        let projector = self.projector_control.as_ref().ok_or(NativeMountError::Lifecycle(
            "original cleanup producer lost its controller",
        ))?.clone();
        if self.runtime.is_some() {
            if lock_recover(&projector.worker).is_none() && !lock_recover(&ingress.state).drained {
                return Err(NativeMountError::Lifecycle("original worker birth refused; original startup owners retained"));
            }
            let physical = self.take_physical_owner();
            let mut state = lock_recover(&ingress.state);
            if state.owner.is_some() { unreachable!("unique original physical controller"); }
            state.owner = Some(physical);
        }
        ingress.request(&projector);
        let mut state = lock_recover(&ingress.state);
        loop {
            if state.terminal { return Ok(()); }
            if let Some(error) = state.failure.take() { return Err(error); }
            wait.check().map_err(|_| detach_work_error(crate::NativeTreeMaterializationError::Cancelled))?;
            if state.physical_ready {
                let owner = state.owner.as_mut().expect("original completed physical controller");
                let mut credit = owner.worker_join_credit.take().ok_or(NativeMountError::Lifecycle(
                    "original worker join credit unavailable",
                ))?;
                let mut worker = lock_recover(&projector.worker);
                let Some(handle) = worker.take() else {
                    owner.worker_join_credit = Some(credit);
                    return Err(NativeMountError::Lifecycle("original worker handle unavailable"));
                };
                let admitted = credit.cleanup_loan(wait).started_io(crate::WorkCounters {
                    backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
                });
                if let Err(error) = admitted {
                    *worker = Some(handle);
                    owner.worker_join_credit = Some(credit);
                    return Err(detach_work_error(error));
                }
                drop(worker);
                state.allow_return = true;
                ingress.wake.notify_all();
                drop(state);
                let joined = handle.join();
                drop(credit);
                let mut state = lock_recover(&ingress.state);
                state.terminal = true;
                if let Some(owner) = state.owner.as_mut() { owner.worker_handle_close_credit = None; }
                ingress.wake.notify_all();
                return joined.map_err(|_| NativeMountError::Lifecycle("original ProjFS worker panicked after physical drain"));
            }
            let waited = ingress.wake.wait_timeout(state, HELD_RETRY)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state = waited.0;
        }
    }

    /// Detaches through the original engine without locking it across SDK drains.
    pub(super) fn stop_with_meter(
        &mut self, work: &super::NativeMountMeterWork,
    ) -> Result<(), NativeMountError> {
        self.stop_with_cleanup_wait(work.native_materialization_work().cancellation())
    }

    fn stop_pipeline_with_meter(
        &mut self, work: &super::NativeMountMeterWork, cleanup_wait: Option<&crate::CancellationToken>,
        ingress: &WindowsRetirementIngress,
    ) -> Result<(), NativeMountError> {
        let check_wait = || cleanup_wait.map_or(Ok(()), |wait| {
            wait.check().map_err(|_| detach_work_error(crate::NativeTreeMaterializationError::Cancelled))
        });
        check_wait()?;
        if let Some(runtime) = self.runtime.as_ref() {
            runtime.callbacks.close_admission();
            runtime.callbacks.drain_cleanup(cleanup_wait)?;
            // The existing producer has returned from its actual run body.
            if runtime.writable && self.context.is_some() && !self.authored_capture_attempted {
                let original = self.original_stop_work.as_ref().ok_or(NativeMountError::Lifecycle(
                    "authored capture lost its original execution frame",
                ))?.clone();
                if let Err(cause) = self.flush_callbacks_with_meter(&original) {
                    // The returned authored failure is namespace DATA, not an active physical job.
                    self.capture_failure = Some(cause);
                    self.capture_residue = Some(runtime.post_operation_failure.clone());
                }
                self.authored_capture_attempted = true;
            }
        }
        // Drop only the completed/unused FULL authored execution frame after the real producer/callback fence.
        let metadata_work = work.native_materialization_work().allocation_work().native_mount_work();
        self.original_stop_work = Some(metadata_work.clone());
        if let Some(retained) = self.runtime.as_mut() {
            let runtime = &mut Arc::get_mut(retained)
                .ok_or(NativeMountError::ControlPending)?.value;
            runtime.original_work = runtime.original_work.as_ref().map(|original|
                original.native_materialization_work().allocation_work().native_mount_work());
        }
        if let Some(retirement) = self.retirement.as_mut() {
            let retired = retirement.retire_source_and_watch_with_cleanup(cleanup_wait);
            lock_recover(&ingress.state).loan_custody = Some((
                retirement.destination_guard.is_some(),
                retirement.retained_source.is_some() || retirement.startup_source_watch.is_some(),
            ));
            if !retired? {
                return Err(NativeMountError::ControlPending);
            }
        }
        if let Some(context) = self.context {
            self.runtime.as_ref().expect("original live SDK runtime").projector
                .retire_owned_entries(context, &metadata_work, cleanup_wait, self.capture_failure.is_some())?;
            let credit = self.sdk_stop_credit.as_mut().ok_or(NativeMountError::Lifecycle(
                "live original SDK instance lost its birth stop credit",
            ))?;
            let loan = match cleanup_wait {
                Some(wait) => credit.cleanup_loan(wait),
                None => credit.accepted_cleanup_loan(),
            };
            loan.started_io(crate::WorkCounters {
                backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
            }).map_err(detach_work_error)?;
            // SAFETY: this original live opaque instance has closed callback admission and drained both accepted callbacks and producer IO.
            unsafe { PrjStopVirtualizing(context) };
            self.context = None;
            self.sdk_stop_confirmed = true;
            if let Some(projector) = self.projector_control.as_ref() {
                let state = lock_recover(&projector.state);
                for (_, written) in state.written.iter().chain(state.pending.iter()) {
                    if let Some(Owned::Link(owner)) = written.owned.as_ref() {
                        *lock_recover(&owner.context) = None;
                    }
                }
            }
        }
        if let Some(retained) = self.runtime.as_ref() {
            let mut host = &metadata_work.native_materialization_work();
            if let Err(cause) = remove_staging_directory_with_cleanup(&retained.staging_custody, &mut host, cleanup_wait, true) {
                if native_mount_error_code(&cause) != Some(HR_DIRECTORY_NOT_EMPTY) { return Err(cause); }
                // The real empty-only refusal has finished; its contents are namespace DATA, not an active physical owner.
                self.cleanup_failure = Some(cause);
                let mut stage = lock_recover(&retained.staging_custody).take()
                    .expect("real refused staging capability");
                stage.close_directory();
                self.namespace_stage = Some(stage);
            }
            let mut failures = lock_recover(retained.post_operation_failure.as_ref());
            self.completed_callback_work = failures.completed_callback_work;
            *lock_recover(&self.first_callback_failure) = failures.unreplayable.take();
        }
        // The marker is persistent SDK instance configuration.
        if let Some(projector) = self.projector_control.as_ref() {
            projector.drop_retired_memos(self.capture_failure.is_some());
        }
        drop(self.runtime.take());
        drop(self.root_custody.take());
        if let Some(retirement) = self.retirement.as_mut() {
            let retired = match cleanup_wait {
                Some(wait) => retirement.retire_with_cleanup_wait(wait),
                None => retirement.retire_accepted_cleanup(),
            };
            lock_recover(&ingress.state).loan_custody = Some((
                retirement.destination_guard.is_some(),
                retirement.retained_source.is_some() || retirement.startup_source_watch.is_some(),
            ));
            let retired = retired?;
            if !retired { return Err(NativeMountError::ControlPending); }
            if retirement.is_retired() { drop(self.retirement.take()); }
        }
        drop(self.sdk_stop_credit.take());
        drop(self.sdk_info_credit.take());
        drop(self.original_stop_work.take());
        Ok(())
    }

    /// Borrow the real packet owner, including its current physical stack loan.
    pub(super) fn retirement_ownership(&self) -> (bool, bool) {
        if let Some(packet) = self.retirement.as_ref() {
            return (packet.destination_guard.is_some(),
                packet.retained_source.is_some() || packet.startup_source_watch.is_some());
        }
        let Some(ingress) = self.ingress.as_ref() else { return (false, false); };
        let state = lock_recover(&ingress.state);
        state.owner.as_ref().map_or_else(
            || state.loan_custody.unwrap_or((false, false)),
            |owner| owner.retirement_ownership(),
        )
    }

    fn stop_accepted_cleanup(&mut self, ingress: &WindowsRetirementIngress) -> Result<(), NativeMountError> {
        let work = self.original_stop_work.as_ref().ok_or(NativeMountError::Lifecycle(
            "original physical driver lost its retirement frame",
        ))?.native_materialization_work().allocation_work().native_mount_work();
        self.stop_pipeline_with_meter(&work, None, ingress)
    }

    fn take_physical_owner(&mut self) -> Self {
        Self {
            context: self.context.take(), runtime: self.runtime.take(),
            root_custody: self.root_custody.take(), original_stop_work: self.original_stop_work.take(),
            sdk_stop_credit: self.sdk_stop_credit.take(), worker_join_credit: self.worker_join_credit.take(),
            sdk_info_credit: self.sdk_info_credit.take(), namespace_qualification: self.namespace_qualification.take(),
            ingress: None, projector_control: self.projector_control.clone(),
            worker_handle_close_credit: self.worker_handle_close_credit.take(),
            worker_raw_handle: self.worker_raw_handle.take(),
            authored_capture_attempted: self.authored_capture_attempted,
            capture_residue: self.capture_residue.take(), capture_failure: self.capture_failure.take(),
            namespace_stage: self.namespace_stage.take(), cleanup_failure: self.cleanup_failure.take(),
            retirement: self.retirement.take(),
            root_mark_attempted: self.root_mark_attempted, sdk_stop_confirmed: self.sdk_stop_confirmed,
            completed_callback_work: self.completed_callback_work,
            first_callback_failure: Mutex::new(lock_recover(&self.first_callback_failure).take()),
        }
    }
    fn physically_retired(&self) -> bool {
        self.context.is_none() && self.runtime.is_none()
            && self.root_custody.is_none() && self.retirement.is_none()
    }

    fn close_own_worker_handle(&mut self) -> Result<(), NativeMountError> {
        use std::os::windows::io::IntoRawHandle;
        if self.worker_raw_handle.is_none() {
            if let Some(worker) = self.projector_control.as_ref().and_then(|projector| lock_recover(&projector.worker).take()) {
                self.worker_raw_handle = Some(SendThreadHandle(worker.into_raw_handle()));
            }
        }
        let Some(handle) = self.worker_raw_handle.as_ref() else { return Ok(()); };
        let credit = self.worker_handle_close_credit.as_mut().ok_or(NativeMountError::Lifecycle(
            "original worker handle has no accepted close credit",
        ))?;
        let loan = credit.accepted_cleanup_loan();
        loan.started_io(crate::WorkCounters { backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED })
            .map_err(detach_work_error)?;
        unsafe { windows::Win32::Foundation::CloseHandle(windows::Win32::Foundation::HANDLE(handle.0)) }
            .map_err(|error| NativeMountError::Io(std::io::Error::from_raw_os_error(error.code().0 & 0xffff)))?;
        self.worker_raw_handle = None;
        self.worker_handle_close_credit = None;
        self.worker_join_credit = None;
        Ok(())
    }
    pub(super) fn physical_retirement_complete(&self) -> bool {
        self.ingress.as_ref().is_some_and(|ingress| lock_recover(&ingress.state).terminal)
    }

    pub(super) fn retired_namespace_qualification(&self) -> Option<ProjFsNamespaceQualification> {
        let ingress = self.ingress.as_ref()?;
        let state = lock_recover(&ingress.state);
        if !state.terminal { return None; }
        state.owner.as_ref()?.namespace_qualification
    }

    pub(super) fn take_namespace_outcome(&mut self) -> Option<ProjFsNamespaceOutcome> {
        if !self.physical_retirement_complete() { return None; }
        let qualification = self.retired_namespace_qualification();
        let ingress = self.ingress.as_ref()?;
        let mut retirement = lock_recover(&ingress.state);
        let owner = retirement.owner.as_mut()?;
        let captures = owner.capture_residue.take();
        let capture_failure = owner.capture_failure.take();
        let stage = owner.namespace_stage.take();
        let cleanup_failure = owner.cleanup_failure.take();
        let projector = self.projector_control.as_ref()?;
        let mut state = lock_recover(&projector.state);
        let continuation_ready = capture_failure.is_none() && cleanup_failure.is_none() && stage.is_none()
            && captures.as_ref().is_none_or(|queue| {
                let queue = lock_recover(queue);
                queue.pending_captures.entries.items.is_empty()
                    && queue.provider_control_failure.is_none() && !queue.unreplayable_barrier
            })
            && !state.written.iter().chain(state.pending.iter()).any(|(_, written)|
                matches!(written.owned.as_ref(), Some(Owned::Link(owner))
                    if lock_recover(&owner.namespace_residue).is_some()));
        Some(ProjFsNamespaceOutcome {
            written: std::mem::take(&mut state.written),
            pending: std::mem::take(&mut state.pending),
            qualification,
            stage, captures, capture_failure, cleanup_failure,
            continuation_ready,
        })
    }



    /// Evidence that the sole `PrjStopVirtualizing` call completed, not that the destination currently exists, is empty, or is no longer a reparse point.
    pub(super) fn namespace_detach_confirmed(&self) -> bool {
        self.ingress.as_ref().map_or(self.sdk_stop_confirmed, |ingress| lock_recover(&ingress.state).namespace_detached)
    }


    /// Identity of the actual held projected root, never a fresh pathname probe.
    pub(super) fn native_root_identity(&self) -> Option<crate::NativeRootIdentity> {
        self.live().ok().map(|(runtime, _)| runtime.metadata_root.identity())
    }

    /// Charges mounted host-I/O callbacks to the caller until this guard drops.
    pub(super) fn begin_original_work(
        &self, work: &super::NativeMountMeterWork,
    ) -> Result<ProjectionWorkScope, NativeMountError> {
        let (runtime, _) = self.live()?;
        let (projector_generation, runtime_generation) =
            enter_projection_work(&runtime.projector, &runtime, work)?;
        Ok(ProjectionWorkScope {
            runtime: runtime.clone(), meter: work.clone(),
            projector_generation, runtime_generation,
        })
    }

    pub(super) fn flush_callbacks(&self) -> Result<(), NativeMountError> {
        let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .map_err(|(cause, _)| detach_work_error(cause))?;
        self.flush_callbacks_measured(&work)
    }

    /// Installs the caller's SDK callback scope before any probe or capture.
    pub(super) fn flush_callbacks_measured(&self, work: &crate::NativeMaterializationWork) -> Result<(), NativeMountError> {
        self.flush_callbacks_with_meter(&work.native_mount_work())
    }

    pub(super) fn flush_callbacks_with_meter(
        &self, work: &super::NativeMountMeterWork,
    ) -> Result<(), NativeMountError> {
        let (runtime, _) = self.live()?;
        runtime.projector.with_control_meter(&runtime, work, |meter| {
            self.flush_callbacks_on_meter(&runtime, meter)
        })
    }

    fn flush_callbacks_on_meter(
        &self, runtime: &Runtime, meter: &super::NativeMountMeterWork,
    ) -> Result<(), NativeMountError> {
        let _flush = loop {
            meter.native_materialization_work().admit(crate::WorkCounters::UNCHARGED).map_err(detach_work_error)?;
            match runtime.flush_gate.try_lock() {
                Ok(held) => break held,
                Err(std::sync::TryLockError::Poisoned(error)) => break error.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => std::thread::sleep(HELD_RETRY),
            }
        };
        flush_callback_gate_measured(
            &runtime.callbacks, runtime.post_operation_failure.as_ref(),
            Some(&runtime.capture_probes), meter,
            |paths, meter| {
                if paths.is_empty() { return Ok(()); }
                self.verify_authored_capture_work(runtime, meter)?;
                let envelope = (paths.len() as u64).checked_mul(size_of::<MountPath>() as u64)
                    .ok_or(crate::WorkError::Overflow)?;
                let materialization = meter.native_materialization_work();
                let mut allocation = materialization.claim(envelope)?;
                let mut exact = Vec::new();
                let work = meter.clone();
                let result = (|| {
                    exact.try_reserve_exact(paths.len()).map_err(NativeMountError::Allocation)?;
                    for (path, _, subtree) in paths {
                        meter.native_materialization_work().charge(crate::WorkCounters {
                            items_examined: 1, ..crate::WorkCounters::UNCHARGED
                        }).map_err(detach_work_error)?;
                        let kind = match with_mount_wide_path_on_meter(path, meter, |name, meter| {
                            runtime.metadata_root.stat_wide_with_work(name, &mut &meter.native_materialization_work())
                                .map_err(detach_work_error)
                        }) {
                            Ok(_) => {
                                self.verify_authored_capture_work(runtime, meter)?;
                                capture_missing_host_ancestors_measured(
                                    runtime.source.as_ref(), &runtime.root, path,
                                    runtime.metadata_root.identity(), meter,
                                )?;
                                self.verify_authored_capture_work(runtime, meter)?;
                                Some(*subtree)
                            }
                            Err(NativeMountError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                                match runtime.source.lookup_with_meter(path, &work) {
                                    Err(MountSourceError::NotFound) | Ok(None) => None,
                                    Ok(Some(lookup)) => Some(lookup.node.kind == MountNodeKind::Directory),
                                    Err(error) => return Err(detach_source_error(error)),
                                }
                            }
                            Err(error) => return Err(error),
                        };
                        match kind {
                            Some(true) => {
                                self.verify_authored_capture_work(runtime, meter)?;
                                runtime.source.capture_host_subtree_with_meter(
                                    &runtime.root, path, runtime.metadata_root.identity(), &work,
                                ).map_err(detach_source_error)?;
                                self.verify_authored_capture_work(runtime, meter)?;
                            }
                            Some(false) => {
                                let (path, mut owned) = clone_mount_components_native(path.components(), meter)?;
                                allocation.absorb(&mut owned)?;
                                exact.push(path);
                            }
                            None => {}
                        }
                    }
                    if !exact.is_empty() {
                        self.verify_authored_capture_work(runtime, meter)?;
                        runtime.source.capture_host_paths_with_meter(
                            &runtime.root, &exact, runtime.metadata_root.identity(), &work,
                        ).map_err(detach_source_error)?;
                        self.verify_authored_capture_work(runtime, meter)?;
                    }
                    Ok(())
                })();
                drop(exact);
                drop(allocation);
                result
            },
        )
    }

    /// Authored Source publication is distinct from private SDK hydration.
    fn verify_authored_capture_work(
        &self, runtime: &Runtime, meter: &super::NativeMountMeterWork,
    ) -> Result<(), NativeMountError> {
        if !runtime.writable {
            return Err(NativeMountError::Lifecycle("read-only SDK namespace cannot publish authored Source data"));
        }
        installed_callback_work(runtime)?;
        verify_callback_work(runtime, meter)?;
        if let Some(guard) = runtime.callback_guard.as_ref() {
            super::adapter::CallbackRuntime::existing()?
                .verify_native_callback_write_guard(guard.as_ref(), meter)?;
        }
        Ok(())
    }

    /// Makes a source-side change at one mount-relative path (leading `/` optional) visible: the provider forgets memoized source facts, and an unmodified file at `path` is replaced by what is written again from the source (settled by [`Self::revalidate`]).
    pub(super) fn invalidate(&self, path: &[u8]) -> Result<(), NativeMountError> {
        let (runtime, context) = self.live()?;
        let text = std::str::from_utf8(path).map_err(|_| crate::DemandError::InvalidRequest)?;
        let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .map_err(|(cause, _)| detach_work_error(cause))?;
        runtime.projector.with_projection_work(&runtime, context, false, &work, |projection| {
            lock_recover(runtime.projection.as_ref()).clear();
            let components = text.split('/').filter(|part| !part.is_empty());
            let mut count = 0_usize;
            let mut units = 1_usize;
            for component in components.clone() {
                projection.work.native_materialization_work().charge(crate::WorkCounters {
                    items_examined: 1, ..crate::WorkCounters::UNCHARGED
                }).map_err(detach_work_error)?;
                units = units.checked_add(component.encode_utf16().count()).ok_or(crate::WorkError::Overflow)?;
                if count != 0 { units = units.checked_add(1).ok_or(crate::WorkError::Overflow)?; }
                count += 1;
            }
            if count == 0 { return Ok(()); }
            let bytes = (units as u64).checked_mul(2).ok_or(crate::WorkError::Overflow)?;
            let encoded = crate::WorkCounters { bytes_encoded: bytes, ..crate::WorkCounters::UNCHARGED };
            let original = projection.work.native_materialization_work();
            original.admit(encoded)?;
            let mut native_allocation = projection.work.native_materialization_work().claim(bytes)?;
            let mut name = Vec::new();
            if let Err(error) = name.try_reserve_exact(units) {
                drop(name);
                    return Err(NativeMountError::Allocation(error));
            }
            native_allocation.reconcile_capacity((name.capacity() * size_of::<u16>()) as u64)?;
            for (index, component) in components.enumerate() {
                if index != 0 { name.push(u16::from(b'\\')); }
                name.extend(component.encode_utf16());
            }
            name.push(0);
            let name = RetainedBuffer { value: Some(name), custody: projection.custody.clone(), bytes: 0, native_allocation: Some(native_allocation), source_ownership: None };
            original.record_admitted(encoded)?;
            let mount_path = path_from(PCWSTR::from_raw(name.as_ptr()), projection)?
                .ok_or(crate::DemandError::InvalidRequest)?;
            match prj_delete(projection, PCWSTR::from_raw(name.as_ptr()), PRJ_UPDATE_ALLOW_READ_ONLY) {
                Err(error) if !native_mount_error_code(&error).is_some_and(|code| [
                    HR_FILE_NOT_FOUND, HR_PATH_NOT_FOUND, HR_VIRTUALIZATION_INVALID_OPERATION,
                ].contains(&code)) => Err(error),
                _ => runtime.projector.invalidate(&mount_path, projection),
            }
        })
    }

    /// Waits until every change made to the source so far is written into the projection: each superseded path written again, each changed directory reconciled.
    pub(super) fn revalidate(&self) -> Result<(), NativeMountError> {
        let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .map_err(|(cause, _)| detach_work_error(cause))?;
        self.revalidate_measured(&work)
    }

    /// Reuses the original worker, source fence and callback capture barrier.
    pub(super) fn revalidate_measured(&self, work: &crate::NativeMaterializationWork) -> Result<(), NativeMountError> {
        self.revalidate_with_meter(&work.native_mount_work())
    }

    pub(super) fn revalidate_with_meter(
        &self, work: &super::NativeMountMeterWork,
    ) -> Result<(), NativeMountError> {
        let (runtime, context) = self.live()?;
        runtime.projector.with_projection_meter(&runtime, context, false, work, |projection| {
            let source_work = projection.work.clone();
            if runtime.source.view_stamp_with_meter(&source_work).map_err(detach_source_error)?.is_none() {
                runtime.projector.invalidate_all(projection)?;
            }
            runtime.source.fence_changes_with_meter(&source_work).map_err(detach_source_error)?;
            runtime.projector.retry();
            runtime.projector.settle_measured(projection.work)?;
            self.flush_callbacks_on_meter(&runtime, projection.work)
        })
    }

    /// Transfers only the actual first fatal cause; publication barriers and pending paths remain owned by the live runtime.
    pub(super) fn take_callback_failure(&self) -> Option<super::NativeMountCallbackFailure> {
        if let Some(ingress) = self.ingress.as_ref() {
            let mut state = lock_recover(&ingress.state);
            if let Some(owner) = state.owner.as_ref() {
                if let Some(runtime) = owner.runtime.as_ref() {
                    if let Some(failure) = take_callback_failure(runtime) { return Some(failure); }
                }
            }
            return state.callback_failure.take();
        }
        self.runtime.as_ref().and_then(|runtime| take_callback_failure(runtime))
            .or_else(|| lock_recover(&self.first_callback_failure).take())
    }

    /// Completed future SDK requests only.
    pub(super) fn completed_callback_work(&self) -> crate::WorkCounters {
        if let Some(ingress) = self.ingress.as_ref() {
            let state = lock_recover(&ingress.state);
            return state.owner.as_ref().map_or(state.completed_callback_work, Self::completed_callback_work);
        }
        self.runtime.as_ref().map_or(self.completed_callback_work, |runtime| {
            lock_recover(runtime.post_operation_failure.as_ref()).completed_callback_work
        })
    }

    #[cfg(test)]
    fn test_runtime(&self) -> Option<Arc<RetainedArcValue<Runtime>>> {
        self.runtime.clone().or_else(|| self.ingress.as_ref().and_then(|ingress|
            lock_recover(&ingress.state).owner.as_ref().and_then(|owner| owner.runtime.clone())))
    }

    #[cfg(test)]
    fn test_context(&self) -> Option<PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT> {
        self.context.or_else(|| self.ingress.as_ref().and_then(|ingress|
            lock_recover(&ingress.state).owner.as_ref().and_then(|owner| owner.context)))
    }

    #[cfg(test)]
    fn test_original_stop_work(&self) -> Option<super::NativeMountMeterWork> {
        self.original_stop_work.clone().or_else(|| self.ingress.as_ref().and_then(|ingress|
            lock_recover(&ingress.state).owner.as_ref().and_then(|owner| owner.original_stop_work.clone())))
    }

    #[cfg(test)]
    fn test_sdk_stop_credit(&self) -> Option<crate::WorkCounters> {
        self.sdk_stop_credit.as_ref().map(|credit| credit.remaining_work()).or_else(|| self.ingress.as_ref().and_then(|ingress|
            lock_recover(&ingress.state).owner.as_ref().and_then(|owner| owner.sdk_stop_credit.as_ref().map(|credit| credit.remaining_work()))))
    }

    #[cfg(test)]
    fn test_worker_join_credit(&self) -> Option<crate::WorkCounters> {
        self.worker_join_credit.as_ref().map(|credit| credit.remaining_work()).or_else(|| self.ingress.as_ref().and_then(|ingress|
            lock_recover(&ingress.state).owner.as_ref().and_then(|owner| owner.worker_join_credit.as_ref().map(|credit| credit.remaining_work()))))
    }

    fn live(&self) -> Result<(Arc<RetainedArcValue<Runtime>>, PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT), NativeMountError> {
        if let Some(ingress) = self.ingress.as_ref() {
            let state = lock_recover(&ingress.state);
            if state.retiring { return Err(NativeMountError::Lifecycle("ProjFS producer admission is closed")); }
            let owner = state.owner.as_ref().ok_or(NativeMountError::ControlPending)?;
            return owner.runtime.as_ref().cloned().zip(owner.context)
                .ok_or(NativeMountError::Lifecycle("ProjFS session is stopped"));
        }
        self.runtime.as_ref().cloned().zip(self.context)
            .ok_or(NativeMountError::Lifecycle("ProjFS session is stopped"))
    }
}

impl Drop for ProjFsSession {
    fn drop(&mut self) {
        let Some(ingress) = self.ingress.take() else { return; };
        let Some(projector) = self.projector_control.as_ref().cloned() else { return; };
        if self.runtime.is_some() && lock_recover(&projector.worker).is_none()
            && !lock_recover(&ingress.state).drained
        {
            // No SDK/factory effect exists and the original packet was never taken from its caller.
            return;
        }
        if self.runtime.is_some() || self.retirement.is_some() || self.context.is_some() {
            let physical = self.take_physical_owner();
            let mut state = lock_recover(&ingress.state);
            if state.owner.is_none() { state.owner = Some(physical); }
            else { unreachable!("one authentic physical controller per original producer"); }
        }
        {
            let mut state = lock_recover(&ingress.state);
            state.last_public_drop = true;
        }
        ingress.request(&projector);
    }
}

impl Runtime {
    fn birth_worker_with_work(
        &self, ingress: Arc<RetainedArcValue<WindowsRetirementIngress>>, work: &super::NativeMountMeterWork,
    ) -> Result<(), NativeMountError> {
        let projector = self.projector.clone();
        let runtime = SendRuntime(std::ptr::from_ref(self));
        let worker = projection_native_call(work, true, || {
            std::thread::Builder::new().spawn(move || ingress.run(runtime, projector))
        })?;
        *lock_recover(&self.projector.worker) = Some(worker);
        Ok(())
    }

    /// Uses the original runtime and worker engine; no duplicate root/path envelope or cached original-meter alias escapes the lifecycle scope.
    fn attach_workers_with_work(
        &self, context: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT, projection: Projection<'_>,
    ) -> Result<(), NativeMountError> {
        let _ = context;
        let observer = Arc::downgrade(&self.projector);
        let observer: std::sync::Weak<dyn ViewObserver> = observer;
        let work = projection.work.clone();
        self.source.observe_view_with_meter(observer, &work).map_err(detach_source_error)
    }

}

/// Reports one typed source failure as its distinct Win32 status.
fn source_hresult(error: &MountSourceError) -> HRESULT {
    match error.semantic_error() {
        MountSourceError::NotFound => HR_FILE_NOT_FOUND,
        MountSourceError::AlreadyExists => HR_ALREADY_EXISTS,
        MountSourceError::Invalid(_) | MountSourceError::NamespacePath(_) => HR_INVALID_DATA,
        MountSourceError::Unsupported(_) | MountSourceError::NativeUnsupported => HR_NOT_SUPPORTED,
        MountSourceError::NativeText { kind, .. } | MountSourceError::NativeStaticText { kind, .. } =>
            match kind {
                super::NativeSourceTextKind::Engine => HR_IO_DEVICE,
                super::NativeSourceTextKind::Invalid => HR_INVALID_DATA,
                super::NativeSourceTextKind::Unsupported => HR_NOT_SUPPORTED,
            },
        MountSourceError::Stale | MountSourceError::Cancelled => HR_FILE_INVALID,
        MountSourceError::NativeWork(error) => materialization_hresult(error),
        MountSourceError::WorkspaceCause(error) => workspace_hresult(error),
        MountSourceError::WorkspacePrefixFailure { source, .. } => workspace_hresult(source),
        MountSourceError::NativeCallbackDenied { .. } => HRESULT::from_win32(5),
        MountSourceError::NativeWorkspaceCause(error) => workspace_hresult(&error.source),
        MountSourceError::Filesystem(error) => filesystem_hresult(error),
        MountSourceError::Io(error) | MountSourceError::NativeRecordIo(error, _) => error.raw_os_error().map_or(HR_IO_DEVICE, |code| {
            if code < 0 { HRESULT(code) } else { HRESULT::from_win32(code as u32) }
        }),
        MountSourceError::NativeRecordJson(_, _) | MountSourceError::NativeRecordLockTimeout(_, _)
        | MountSourceError::NativeRecordOwnershipConflict(_, _)
        | MountSourceError::NativeRecordMaterializationInProgress
        | MountSourceError::NativeRecordIntegrity | MountSourceError::NativeRecordContextTransaction(_, _)
        | MountSourceError::NativeRecordCorrupt(_, _) | MountSourceError::PublicationRetryLimit => HR_IO_DEVICE,
        MountSourceError::MutationFinalization { .. } => HR_IO_DEVICE,
        _ => HR_IO_DEVICE,
    }
}

/// Only the Win32 failure class crosses SDK ABI; the original owned cause remains in the source error or the runtime's inline first-fatal slot.
fn materialization_hresult(error: &crate::NativeTreeMaterializationError) -> HRESULT {
    use crate::NativeTreeMaterializationError as Error;
    match error {
        Error::Io(error) => error.raw_os_error().map_or_else(|| match error.kind() {
            std::io::ErrorKind::NotFound => HR_FILE_NOT_FOUND,
            std::io::ErrorKind::AlreadyExists => HR_ALREADY_EXISTS,
            std::io::ErrorKind::PermissionDenied => HRESULT::from_win32(5),
            std::io::ErrorKind::InvalidInput | std::io::ErrorKind::InvalidData => HR_INVALID_DATA,
            std::io::ErrorKind::Unsupported => HR_NOT_SUPPORTED,
            std::io::ErrorKind::OutOfMemory => HR_OUT_OF_MEMORY,
            _ => HR_IO_DEVICE,
        }, |code| {
            if code < 0 { HRESULT(code) } else { HRESULT::from_win32(code as u32) }
        }),
        Error::Cancelled => HR_FILE_INVALID,
        Error::AllocationFailed(_) => HR_OUT_OF_MEMORY,
        Error::Work(_) => HR_IO_DEVICE,
        _ => HR_IO_DEVICE,
    }
}

fn filesystem_hresult(error: &crate::FsError) -> HRESULT {
    match error {
        crate::FsError::Cancelled(_) | crate::FsError::SourceSnapshotStale => HR_FILE_INVALID,
        crate::FsError::Work(_) => HR_IO_DEVICE,
        crate::FsError::NativeKernel(error) => materialization_hresult(error),
        crate::FsError::NotFound => HR_FILE_NOT_FOUND,
        crate::FsError::HandleAllocationFailed => HR_OUT_OF_MEMORY,
        _ => HR_IO_DEVICE,
    }
}

fn filesystem_failure_is_fatal(error: &crate::FsError) -> bool {
    match error {
        crate::FsError::Work(_) | crate::FsError::Cancelled(_)
            | crate::FsError::HandleAllocationFailed => true,
        crate::FsError::NativeKernel(error) => super::native_materialization_failure_is_fatal(error),
        _ => false,
    }
}

/// Projects only an ABI status; the whole original workspace cause stays owned.
fn workspace_hresult(error: &crate::WorkspaceError) -> HRESULT {
    match error {
        crate::WorkspaceError::Cancelled(_) => HR_FILE_INVALID,
        crate::WorkspaceError::Work(_) => HR_IO_DEVICE,
        crate::WorkspaceError::NotFound => HR_FILE_NOT_FOUND,
        crate::WorkspaceError::Filesystem(error) => filesystem_hresult(error),
        crate::WorkspaceError::StaleGeneration | crate::WorkspaceError::StaleSource
            | crate::WorkspaceError::StaleIdentity | crate::WorkspaceError::StaleTarget => HR_FILE_INVALID,
        _ => HR_IO_DEVICE,
    }
}

fn workspace_failure_is_fatal(error: &crate::WorkspaceError) -> bool {
    match error {
        crate::WorkspaceError::Work(_) | crate::WorkspaceError::Cancelled(_) => true,
        crate::WorkspaceError::Filesystem(error) => filesystem_failure_is_fatal(error),
        _ => false,
    }
}

/// The placeholder `ProjFS` persists for a link described by `info`.
fn link_placeholder(info: PRJ_FILE_BASIC_INFO) -> PRJ_PLACEHOLDER_INFO {
    PRJ_PLACEHOLDER_INFO {
        FileBasicInfo: info,
        ..PRJ_PLACEHOLDER_INFO::default()
    }
}


/// Recovery owns only a capability it actually opened, never an invented virtualization context from another process.
#[derive(Default)]
pub(super) struct ProjFsRecoveryControl {
    directory: Option<cap_std::fs::Dir>,
    identity: Option<crate::NativeRootIdentity>,
    retirement_close: Option<crate::NativeWorkReservation>,
}

impl ProjFsRecoveryControl {
    /// One physical admin-directory close is accepted before actor/fence birth.
    pub(super) fn prepare_retirement(
        &mut self, original: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        self.retirement_close = Some(original.reserve_work(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        })?);
        Ok(())
    }

    /// Physical capability close only; no SDK stop or namespace evidence.
    pub(super) fn retire_controls_with_work(
        &mut self, _original: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        if self.directory.is_some() {
            let credit = self.retirement_close.as_mut().ok_or(NativeMountError::ControlPending)?;
            // This credit already carries the original administrative issuer.
            credit.started_drop_io(crate::WorkCounters {
                backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
            });
            drop(self.directory.take());
        }
        if self.controls_pending() { return Err(NativeMountError::ControlPending); }
        Ok(())
    }

    /// Binds the constructor's actual root observation, independently of the sibling fence file identity.
    pub(super) fn bind_root_identity(&mut self, actual: crate::NativeRootIdentity) {
        debug_assert!(self.directory.is_none(), "bind before acquiring recovery controls");
        self.identity = Some(actual);
    }

    /// Corroborates this owner's exact root using the original caller ledger.
    pub(super) fn recover_measured<W: crate::native_host::NativeHostWork>(
        &mut self,
        destination: &std::path::Path,
        disposable: bool,
        work: &mut W,
    ) -> Result<(), NativeMountError>
    where NativeMountError: From<W::Error> {
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
                let _ = self.directory.take();
                return Err(NativeMountError::Demand(crate::DemandError::StaleSource));
            }
            self.identity = Some(observed);
            observed
        } else {
            self.identity.expect("retained capability has its original identity")
        };
        if retained && crate::native_host::open_root_directory_identity_with_work(destination, work)
            .map_err(detach_work_error)?.1 != identity
        {
            return Err(NativeMountError::Demand(crate::DemandError::StaleSource));
        }
        if disposable {
            if let Some(tag) = reparse_tag_held_measured(directory, work, None).map_err(detach_work_error)? {
                return Err(NativeMountError::UnsafeRecoveryDestination { reparse_tag: Some(tag) });
            }
        }
        let _ = self.directory.take();
        Ok(())
    }

    /// Recovery did not call or observe an SDK stop.
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


fn detach_work_error(error: impl Into<NativeMountError>) -> NativeMountError {
    match error.into() {
        NativeMountError::Materialization(crate::NativeTreeMaterializationError::Io(error)) => NativeMountError::Io(error),
        NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(error)) => NativeMountError::Work(error),
        NativeMountError::Materialization(crate::NativeTreeMaterializationError::Cancelled) =>
            NativeMountError::Demand(crate::DemandError::Cancelled),
        NativeMountError::Materialization(crate::NativeTreeMaterializationError::AllocationFailed(error)) =>
            NativeMountError::Allocation(error),
        error => error,
    }
}

fn detach_source_error(error: MountSourceError) -> NativeMountError {
    error.into()
}

/// Admits exactly one actual native call and records even its failed attempt.
fn detach_attempt<T, W: crate::native_host::NativeHostWork>(
    work: &mut W,
    cleanup: Option<&crate::materializer::NativeCleanupLoan<'_>>,
    write: bool,
    operation: impl FnOnce() -> std::io::Result<T>,
) -> Result<T, NativeMountError>
where NativeMountError: From<W::Error> {
    let delta = if write {
        crate::WorkCounters { backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED }
    } else {
        crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED }
    };
    if let Some(loan) = cleanup {
        loan.started_io(delta).map_err(detach_work_error)?;
        operation().map_err(NativeMountError::Io)
    } else {
        work.attempt_io(delta, operation).map_err(detach_work_error)
    }
}

fn destination_parent(destination: &std::path::Path) -> Result<&std::path::Path, crate::DemandError> {
    let parent = destination.parent().ok_or(crate::DemandError::InvalidRequest)?;
    Ok(if parent.as_os_str().is_empty() { std::path::Path::new(".") } else { parent })
}



fn reopen_for_delete_measured<W: crate::native_host::NativeHostWork>(
    file: &impl std::os::windows::io::AsRawHandle, work: &mut W,
    cleanup: Option<&crate::materializer::NativeCleanupLoan<'_>>,
) -> Result<std::fs::File, NativeMountError>
where NativeMountError: From<W::Error> {
    use std::os::windows::io::FromRawHandle as _;
    use windows::Win32::Storage::FileSystem::{
        DELETE, FILE_READ_ATTRIBUTES, SYNCHRONIZE, FILE_SHARE_READ, FILE_SHARE_WRITE,
        FILE_SHARE_DELETE, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, ReOpenFile,
    };
    detach_attempt(work, cleanup, false, || {
        let handle = unsafe { ReOpenFile(
            windows::Win32::Foundation::HANDLE(file.as_raw_handle()),
            (DELETE | FILE_READ_ATTRIBUTES | SYNCHRONIZE).0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
        ) }.map_err(std::io::Error::from)?;
        Ok(unsafe { std::fs::File::from_raw_handle(handle.0) })
    })
}

fn remove_staging_directory_measured<W: crate::native_host::NativeHostWork>(
    custody: &Mutex<Option<StagingDirectoryCustody>>, work: &mut W,
    cleanup_wait: Option<&crate::CancellationToken>,
) -> Result<(), NativeMountError>
where NativeMountError: From<W::Error> {
    remove_staging_directory_with_cleanup(custody, work, cleanup_wait, cleanup_wait.is_some())
}

fn remove_staging_directory_with_cleanup<W: crate::native_host::NativeHostWork>(
    custody: &Mutex<Option<StagingDirectoryCustody>>, work: &mut W,
    cleanup_wait: Option<&crate::CancellationToken>, accepted: bool,
) -> Result<(), NativeMountError>
where NativeMountError: From<W::Error> {
    let mut owned = loop {
        if let Some(wait) = cleanup_wait {
            wait.check().map_err(|_| detach_work_error(crate::NativeTreeMaterializationError::Cancelled))?;
        } else if !accepted {
            work.check().map_err(detach_work_error)?;
        }
        match custody.try_lock() {
            Ok(owned) => break owned,
            Err(std::sync::TryLockError::Poisoned(error)) => break error.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => std::thread::sleep(HELD_RETRY),
        }
    };
    let Some(stage) = owned.as_mut() else { return Ok(()); };
    let directory = stage.directory.as_ref().ok_or(NativeMountError::Lifecycle("staging namespace data is physically closed"))?;
    let expected = stage.identity;
    let cleanup = &mut stage.cleanup;
    let temporary_close = &mut stage.temporary_close;
    if temporary_close.remaining_work().backend_write_operations == 0 {
        return Err(NativeMountError::Lifecycle("original staging temporary-close credit exhausted; capability retained"));
    }
    let loan = if accepted {
        Some(match cleanup_wait {
            Some(wait) => cleanup.cleanup_loan(wait),
            None => cleanup.accepted_cleanup_loan(),
        })
    } else { None };
    if loan.as_ref().is_some_and(|loan| {
        let remaining = loan.remaining_work();
        remaining.backend_read_operations < 3 || remaining.backend_write_operations < 2
    }) {
        return Err(NativeMountError::Lifecycle("original staging probe/disposition credit exhausted; capability retained"));
    }
    // Reopen only the original creation capability.
    let file = reopen_for_delete_measured(directory, work, loan.as_ref())?;
    let reopened = cap_std::fs::Dir::from_std_file(file);
    let result = (|| {
        let information = held_information_measured(&reopened, work, loan.as_ref())?;
        let actual = crate::NativeRootIdentity {
            device: u64::from(information.dwVolumeSerialNumber),
            object: (u64::from(information.nFileIndexHigh) << 32) | u64::from(information.nFileIndexLow),
        };
        if expected.is_some_and(|expected| expected != actual) {
            return Err(crate::DemandError::StaleSource.into());
        }
        if let Some(tag) = reparse_tag_held_measured(&reopened, work, loan.as_ref()).map_err(detach_work_error)? {
            return Err(NativeMountError::UnsafeRecoveryDestination { reparse_tag: Some(tag) });
        }
        // Atomic empty-only disposition cannot erase foreign staging residue.
        delete_through_measured(&reopened, work, loan.as_ref())
    })();
    // Closing this temporary physical control is mandatory, even if admission or the caller was refused after its successful reopen.
    temporary_close.started_drop_io(crate::WorkCounters {
        backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
    });
    drop(reopened);
    drop(loan);
    result?;
    // The creation handle's existing owner records and performs its own close.
    drop(owned.take());
    Ok(())
}

#[cfg(test)]
fn remove_authenticated_destination(
    destination: &std::path::Path,
    expected_identity: crate::NativeRootIdentity,
) -> Result<(), NativeMountError> {
    let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
        .map_err(|(cause, _)| detach_work_error(cause))?;
    remove_authenticated_destination_measured(destination, expected_identity, &mut &work).map(|_| ())
}

/// Observes the physical name without opening it through the stopped provider.
fn physical_attributes_measured<W: crate::native_host::NativeHostWork>(
    path: &std::path::Path, work: &mut W,
) -> Result<Option<u32>, NativeMountError>
where NativeMountError: From<W::Error> {
    use windows::Win32::Storage::FileSystem::{GetFileAttributesW, INVALID_FILE_ATTRIBUTES};
    let (name, allocation) = crate::native_host::normalize_root_wide_with_work(path, work)
        .map_err(detach_work_error)?;
    let name = name;
    let result = detach_attempt(work, None, false, || {
        // SAFETY: this exact native spelling includes its final NUL.
        let attributes = unsafe { GetFileAttributesW(PCWSTR::from_raw(name.as_ptr())) };
        if attributes == INVALID_FILE_ATTRIBUTES {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(attributes)
        }
    });
    drop(name);
    work.release(allocation).map_err(detach_work_error)?;
    match result {
        Ok(attributes) => Ok(Some(attributes)),
        Err(NativeMountError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// Holds the authenticated root for the entire walk.
fn remove_authenticated_destination_measured<W: crate::native_host::NativeHostWork>(
    destination: &std::path::Path,
    expected_identity: crate::NativeRootIdentity,
    work: &mut W,
) -> Result<bool, NativeMountError>
where NativeMountError: From<W::Error> {
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
    match reparse_tag_held_measured(&directory, work, None).map_err(detach_work_error)? {
        Some(windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS) => {}
        Some(tag) => return Err(NativeMountError::UnsafeRecoveryDestination { reparse_tag: Some(tag) }),
        None => return Ok(false),
    }
    remove_projection_tree(&directory, Some(expected_identity), work, None).map_err(detach_work_error)?;
    drop(directory);
    // A successful disposition can still be delete-on-close.
    if physical_attributes_measured(destination, work).map_err(detach_work_error)?.is_some() {
        return Err(NativeMountError::Lifecycle("authenticated ProjFS root deletion is still pending; namespace preserved"));
    }
    Ok(true)
}

/// Removes only this owner's originally observed, currently empty physical root.
pub(super) fn remove_empty_recovery_destination_measured<W: crate::native_host::NativeHostWork>(
    destination: &std::path::Path,
    expected: Option<crate::NativeRootIdentity>,
    work: &mut W,
) -> Result<(), NativeMountError>
where NativeMountError: From<W::Error> {
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
    if let Some(tag) = reparse_tag_held_measured(&directory, work, None).map_err(detach_work_error)? {
        return Err(NativeMountError::UnsafeRecoveryDestination { reparse_tag: Some(tag) });
    }
    // Both actual disposition attempts refuse a nonempty directory.
    delete_through_measured(&directory, work, None).map_err(detach_work_error)?;
    drop(directory);
    if physical_attributes_measured(destination, work).map_err(detach_work_error)?.is_some() {
        return Err(NativeMountError::Lifecycle("empty recovery root deletion is still pending; namespace preserved"));
    }
    Ok(())
}

/// Visits physical entries explicitly instead of hiding recursive traversal in remove_dir_all.
fn remove_projection_tree<W: crate::native_host::NativeHostWork>(
    directory: &cap_std::fs::Dir,
    expected_root: Option<crate::NativeRootIdentity>,
    work: &mut W,
    cleanup: Option<&crate::materializer::NativeCleanupLoan<'_>>,
) -> Result<(), NativeMountError>
where NativeMountError: From<W::Error> {
    const PATIENCE: std::time::Duration = std::time::Duration::from_secs(5);
    let deadline = std::time::Instant::now() + PATIENCE;
    loop {
        work.check()?;
        match remove_directories_in(directory, work) {
            Err(NativeMountError::Io(error)) if refused_for_now(&error) => {}
            result => result?,
        }
        if let Some(cleanup) = cleanup {
            let remaining = cleanup.remaining_work();
            let reads = if expected_root.is_some() { 2 } else { 0 };
            if remaining.backend_read_operations < reads || remaining.backend_write_operations < 2 {
                return Err(NativeMountError::Lifecycle(
                    "original root disposition credit exhausted; capability retained",
                ));
            }
        }
        if let Some(expected) = expected_root {
            let information = held_information_measured(directory, work, cleanup)?;
            let actual = crate::NativeRootIdentity {
                device: u64::from(information.dwVolumeSerialNumber),
                object: (u64::from(information.nFileIndexHigh) << 32) | u64::from(information.nFileIndexLow),
            };
            if actual != expected
                || reparse_tag_held_measured(directory, work, cleanup)?
                    != Some(windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS)
            {
                return Err(crate::DemandError::StaleSource.into());
            }
        }
        match delete_through_measured(directory, work, cleanup) {
            Ok(()) => return Ok(()),
            Err(NativeMountError::Io(error)) if refused_for_now(&error) && std::time::Instant::now() < deadline => {
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

fn remove_directories_in<W: crate::native_host::NativeHostWork>(
    directory: &cap_std::fs::Dir, work: &mut W,
) -> Result<(), NativeMountError>
where NativeMountError: From<W::Error> {
    use windows::Win32::Storage::FileSystem::{DELETE, FILE_READ_ATTRIBUTES, SYNCHRONIZE};
    let (entries, buffer) = crate::native_host::read_dir_held_with_work(directory, work)
        .map_err(detach_work_error)?;
    let mut entries = entries;
    let result = (|| {
        while let Some((entry, allocation)) = entries.next_wide_with_work(work)? {
            let entry = entry;
            let result = (|| {
                let file = match crate::native_host::open_windows_leaf_handle_with_work(
                    directory, &entry.name, DELETE | FILE_READ_ATTRIBUTES | SYNCHRONIZE, None, work,
                ).map_err(detach_work_error) {
                    Ok(file) => file,
                    Err(NativeMountError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                    Err(error) => return Err(error),
                };
                let attributes = held_information_measured(&file, work, None)?.dwFileAttributes;
                // A directory link is a leaf, never a recursively followed tree.
                if attributes & FILE_ATTRIBUTE_DIRECTORY.0 != 0
                    && attributes & FILE_ATTRIBUTE_REPARSE_POINT.0 == 0
                {
                    remove_projection_tree(&cap_std::fs::Dir::from_std_file(file), None, work, None)
                } else if attributes & FILE_ATTRIBUTE_DIRECTORY.0 != 0
                    && reparse_tag_held_measured(&file, work, None)?
                        == Some(windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS)
                {
                    remove_projection_tree(&cap_std::fs::Dir::from_std_file(file), None, work, None)
                } else {
                    delete_through_measured(&file, work, None)
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

fn held_information_measured<W: crate::native_host::NativeHostWork>(
    file: &impl std::os::windows::io::AsRawHandle, work: &mut W,
    cleanup: Option<&crate::materializer::NativeCleanupLoan<'_>>,
) -> Result<windows::Win32::Storage::FileSystem::BY_HANDLE_FILE_INFORMATION, NativeMountError>
where NativeMountError: From<W::Error> {
    use windows::Win32::Storage::FileSystem::{BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle};
    detach_attempt(work, cleanup, false, || {
        let mut information = BY_HANDLE_FILE_INFORMATION::default();
        // SAFETY: this identity-bearing handle and output storage remain live.
        unsafe { GetFileInformationByHandle(windows::Win32::Foundation::HANDLE(file.as_raw_handle()), &raw mut information) }?;
        Ok(information)
    })
}

fn set_disposition_ex_through_measured<W: crate::native_host::NativeHostWork>(
    file: &impl std::os::windows::io::AsRawHandle, flags: u32, work: &mut W,
    cleanup: Option<&crate::materializer::NativeCleanupLoan<'_>>,
) -> Result<(), NativeMountError>
where NativeMountError: From<W::Error> {
    use windows::Win32::Storage::FileSystem::{
        FILE_DISPOSITION_INFO_EX, FILE_DISPOSITION_INFO_EX_FLAGS, FileDispositionInfoEx, SetFileInformationByHandle,
    };
    let disposition = FILE_DISPOSITION_INFO_EX { Flags: FILE_DISPOSITION_INFO_EX_FLAGS(flags) };
    detach_attempt(work, cleanup, true, || {
        // SAFETY: original held DELETE capability and the fixed native ABI.
        unsafe { SetFileInformationByHandle(
            windows::Win32::Foundation::HANDLE(file.as_raw_handle()), FileDispositionInfoEx,
            (&raw const disposition).cast(), size_of::<FILE_DISPOSITION_INFO_EX>() as u32,
        ) }.map_err(Into::into)
    })
}

fn delete_through_measured<W: crate::native_host::NativeHostWork>(
    file: &impl std::os::windows::io::AsRawHandle, work: &mut W,
    cleanup: Option<&crate::materializer::NativeCleanupLoan<'_>>,
) -> Result<(), NativeMountError>
where NativeMountError: From<W::Error> {
    use windows::Win32::Storage::FileSystem::{
        FILE_DISPOSITION_FLAG_DELETE, FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE,
        FILE_DISPOSITION_FLAG_POSIX_SEMANTICS, FILE_DISPOSITION_INFO,
        FileDispositionInfo, SetFileInformationByHandle,
    };
    let result = set_disposition_ex_through_measured(file,
        FILE_DISPOSITION_FLAG_DELETE.0 | FILE_DISPOSITION_FLAG_POSIX_SEMANTICS.0
            | FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE.0, work, cleanup);
    let handle = windows::Win32::Foundation::HANDLE(file.as_raw_handle());
    match result {
        Err(NativeMountError::Io(_)) => {
            let on_close = FILE_DISPOSITION_INFO { DeleteFile: true };
            detach_attempt(work, cleanup, true, || {
                // SAFETY: same held handle; this fallback is separately admitted.
                unsafe { SetFileInformationByHandle(handle, FileDispositionInfo, (&raw const on_close).cast(), size_of::<FILE_DISPOSITION_INFO>() as u32) }
                    .map_err(Into::into)
            })
        }
        result => result,
    }
}

fn reparse_tag_held_measured<W: crate::native_host::NativeHostWork>(
    file: &impl std::os::windows::io::AsRawHandle, work: &mut W,
    cleanup: Option<&crate::materializer::NativeCleanupLoan<'_>>,
) -> Result<Option<u32>, NativeMountError>
where NativeMountError: From<W::Error> {
    use windows::Win32::Foundation::{ERROR_NOT_A_REPARSE_POINT, ERROR_FILE_SYSTEM_VIRTUALIZATION_UNAVAILABLE, HANDLE};
    use windows::Win32::Storage::FileSystem::MAXIMUM_REPARSE_DATA_BUFFER_SIZE;
    use windows::Win32::System::IO::DeviceIoControl;
    use windows::Win32::System::Ioctl::FSCTL_GET_REPARSE_POINT;
    // The ABI has one fixed output maximum.
    let mut output = std::mem::MaybeUninit::<[u8; MAXIMUM_REPARSE_DATA_BUFFER_SIZE as usize]>::uninit();
    let mut returned = 0_u32;
    match detach_attempt(work, cleanup, false, || {
        // SAFETY: the held handle and exact-sized output storage stay live.
        unsafe { DeviceIoControl(HANDLE(file.as_raw_handle()), FSCTL_GET_REPARSE_POINT, None, 0,
            Some(output.as_mut_ptr().cast()), MAXIMUM_REPARSE_DATA_BUFFER_SIZE, Some(&raw mut returned), None) }
            .map_err(Into::into)
    }) {
        Ok(()) if returned >= 4 => {
            // SAFETY: successful DeviceIoControl initialized at least these four bytes in the caller-provided ABI-sized output storage.
            let tag = unsafe { output.as_ptr().cast::<[u8; 4]>().read() };
            Ok(Some(u32::from_le_bytes(tag)))
        }
        Ok(()) => Err(NativeMountError::Io(std::io::ErrorKind::InvalidData.into())),
        Err(NativeMountError::Io(error)) if error.raw_os_error() == Some(ERROR_NOT_A_REPARSE_POINT.0 as i32) => Ok(None),
        Err(NativeMountError::Io(error)) if error.raw_os_error() == Some(ERROR_FILE_SYSTEM_VIRTUALIZATION_UNAVAILABLE.0 as i32) =>
            Ok(Some(windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS)),
        Err(error) => Err(error),
    }
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
        // A stopped ProjFS provider can leave an authenticated placeholder root even when the filter declines to return its reparse payload.
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
    // SAFETY: the handle and output buffer remain valid for the synchronous control call; the output length exactly matches the allocated buffer.
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


/// One lock per key, apart from every other key's.
struct KeyedLocks<K>(Mutex<MemoMap<K, Arc<RetainedArcValue<Mutex<()>>>>>);

impl<K> Default for KeyedLocks<K> {
    fn default() -> Self {
        Self(Mutex::new(MemoMap::default()))
    }
}

/// One lock per file, keyed by its host identity.
type FileLocks = KeyedLocks<crate::NativeRootIdentity>;

impl<K: MemoKey> KeyedLocks<K> {
    /// Existing per-key exclusion, with only newly owned key/control storage charged here.
    fn with<T>(
        &self, key: &K, projection: Projection<'_>,
        edit: impl FnOnce() -> Result<T, NativeMountError>,
    ) -> Result<T, NativeMountError> {
        let lock = {
            let mut locks = lock_recover(&self.0);
            if let Some(lock) = locks.get(key, projection)? {
                lock.clone()
            } else {
                let lock = retained_arc_on_meter(Mutex::new(()), projection.custody, projection.work)?;
                locks.insert(key, lock.clone(), projection)?;
                lock
            }
        };
        let outcome = (|| {
            let _held = loop {
                projection.work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED).map_err(detach_work_error)?;
                match lock.try_lock() {
                    Ok(held) => break held,
                    Err(std::sync::TryLockError::Poisoned(error)) => break error.into_inner(),
                    Err(std::sync::TryLockError::WouldBlock) => std::thread::sleep(HELD_RETRY),
                }
            };
            edit()
        })();
        let retirement = {
            let mut locks = lock_recover(&self.0);
            if Arc::strong_count(&lock) == 2 { locks.remove(key, projection).map(drop) }
            else { Ok(()) }
        };
        match outcome { Err(error) => Err(error), Ok(value) => retirement.map(|()| value) }
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

/// Borrows the SDK's exact raw UTF-16 spelling.
fn path_from(
    pointer: PCWSTR, projection: Projection<'_>,
) -> Result<Option<RetainedBuffer<MountPath>>, NativeMountError> {
    let mut path = RetainedBuffer { value: Some(MountPath::root()), custody: projection.custody.clone(), bytes: 0, native_allocation: None, source_ownership: None };
    if pointer.is_null() { return Ok(Some(path)); }
    let mut length = 0_usize;
    loop {
        projection.work.native_materialization_work().charge(crate::WorkCounters {
            items_examined: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(detach_work_error)?;
        // SAFETY: ProjFS owns this NUL-terminated callback spelling through handler completion; denial precedes even the next borrowed unit.
        let unit = unsafe { *pointer.0.add(length) };
        if unit == 0 { break; }
        length = length.checked_add(1).ok_or(crate::WorkError::Overflow)?;
    }
    // SAFETY: the preceding admitted scan found this exact terminal NUL.
    let wide = unsafe { std::slice::from_raw_parts(pointer.0, length) };
    if wide.first().is_some_and(|unit| matches!(*unit, 47 | 92))
        || wide.get(1) == Some(&58) && wide.first().is_some_and(|unit|
            (65..=90).contains(unit) || (97..=122).contains(unit))
    { return Ok(None); }
    let mut count = 0_usize;
    for (index, component) in wide.split(|unit| matches!(*unit, 47 | 92)).enumerate() {
        projection.work.native_materialization_work().charge(crate::WorkCounters {
            items_examined: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(detach_work_error)?;
        match component {
            [] => {}
            [46] if index != 0 => {}
            [46] | [46, 46] => return Ok(None),
            _ => count = count.checked_add(1).ok_or(crate::WorkError::Overflow)?,
        }
    }
    source_reserve(projection, &mut path.value.as_mut().expect("live SDK path").components, &mut path.bytes, &mut path.native_allocation, count)?;
    for component in wide.split(|unit| matches!(*unit, 47 | 92)) {
        if component.is_empty() || component == [46] { continue; }
        let mut name = source_empty_vec(projection);
        let bytes = component.len().checked_mul(2).ok_or(crate::WorkError::Overflow)?;
        source_reserve_vec(projection, &mut name, bytes)?;
        let encoded = crate::WorkCounters {
            bytes_encoded: bytes as u64, items_examined: component.len() as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        projection.work.native_materialization_work().admit(encoded).map_err(detach_work_error)?;
        for unit in component { name.extend_from_slice(&unit.to_le_bytes()); }
        projection.work.native_materialization_work().record_admitted(encoded).map_err(detach_work_error)?;
        source_path_push(projection, &mut path, name)?;
    }
    Ok(Some(path))
}

unsafe fn empty_destination(
    pointer: PCWSTR, projection: Projection<'_>,
) -> Result<bool, NativeMountError> {
    if pointer.is_null() { return Ok(true); }
    projection.work.native_materialization_work().charge(crate::WorkCounters {
        items_examined: 1, ..crate::WorkCounters::UNCHARGED
    }).map_err(detach_work_error)?;
    Ok(unsafe { *pointer.0 == 0 })
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
    path: &MountPath, projection: Projection<'_>,
) -> Result<HostWindowsMetadata, NativeMountError> {
    with_projection_parent(projection.root, projection.root_identity, path, projection.work, |parent, leaf| {
        let metadata = crate::native_host::stat_windows_leaf_with_work(
            parent, leaf, &mut &projection.work.native_materialization_work(),
        ).map_err(detach_work_error)?;
        Ok(HostWindowsMetadata {
            identity: metadata.identity().map_err(NativeMountError::Demand)?,
            links: metadata.number_of_links(),
            size: metadata.len(),
            attributes: metadata.file_attributes(),
            created: i64::try_from(metadata.creation_time()).map_err(|_| crate::WorkError::Overflow)?,
            modified: i64::try_from(metadata.last_write_time()).map_err(|_| crate::WorkError::Overflow)?,
        })
    })
}


/// Drops the owned native name before releasing its original live claim.
fn with_mount_wide_path_on_meter<T>(
    path: &MountPath, meter: &super::NativeMountMeterWork,
    operation: impl FnOnce(&[u16], &super::NativeMountMeterWork) -> Result<T, NativeMountError>,
) -> Result<T, NativeMountError> {
    let work = meter.native_materialization_work();
    let mut units = path.components().len().saturating_sub(1);
    for component in path.components() {
        work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        crate::kernel::types::utf16le_units(component).ok_or(crate::DemandError::InvalidRequest)?;
        units = units.checked_add(component.len() / 2).ok_or(crate::WorkError::Overflow)?;
    }
    let bytes = (units as u64).checked_mul(2).ok_or(crate::WorkError::Overflow)?;
    let encoded = crate::WorkCounters { bytes_encoded: bytes, ..crate::WorkCounters::UNCHARGED };
    work.admit(encoded)?;
    let mut name = ProjectionBuffer::new(units, meter)?;
    for (index, component) in path.components().iter().enumerate() {
        if index != 0 { name.as_mut().push(u16::from(b'\\')); }
        name.as_mut().extend(crate::kernel::types::utf16le_units(component).expect("validated component"));
    }
    work.record_admitted(encoded)?;
    let result = operation(name.as_slice(), meter);
    drop(name);
    result
}

fn capture_missing_host_ancestors_measured(
    source: &dyn MountFilesystem, root: &std::path::Path, path: &MountPath,
    identity: crate::NativeRootIdentity, meter: &super::NativeMountMeterWork,
) -> Result<(), NativeMountError> {
    let work = meter.clone();
    for count in 1..path.components().len() {
        let (parent, allocation) = clone_mount_components_native(&path.components()[..count], meter)?;
        let result: Result<(), NativeMountError> = (|| {
            if source.lookup_with_meter(&parent, &work).map_err(detach_source_error)?.is_none() {
                verify_source_write_work(source, meter)?;
                source.capture_host_paths_with_meter(
                    root, std::slice::from_ref(&parent), identity, &work,
                ).map_err(detach_source_error)?;
                verify_source_write_work(source, meter)?;
            }
            Ok(())
        })();
        drop(parent);
        drop(allocation);
        result?;
    }
    Ok(())
}

struct MetadataProbeGuard<'a> {
    probes: &'a Mutex<MemoMap<MountPath, Arc<RetainedArcValue<std::sync::atomic::AtomicUsize>>>>,
    path: &'a MountPath,
    counter: Option<Arc<RetainedArcValue<std::sync::atomic::AtomicUsize>>>,
    projection: Projection<'a>,
}

impl<'a> MetadataProbeGuard<'a> {
    fn enter(
        probes: &'a Mutex<MemoMap<MountPath, Arc<RetainedArcValue<std::sync::atomic::AtomicUsize>>>>,
        path: &'a MountPath, projection: Projection<'a>,
    ) -> Result<Self, NativeMountError> {
        let mut active = lock_recover(probes);
        let index = match active.index(path, projection.work)? {
            Ok(index) => index,
            Err(index) => {
                let key = path.copy_owned(projection)?;
                let counter = retained_arc_on_meter(std::sync::atomic::AtomicUsize::new(0),
                    projection.custody, projection.work)?;
                active.entries.insert(index, (key, counter), projection)?;
                index
            }
        };
        let counter = active.entries.items[index].1.clone();
        counter.fetch_update(std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire, |count| count.checked_add(1))
            .map_err(|_| crate::WorkError::Overflow)?;
        Ok(Self { probes, path, counter: Some(counter), projection })
    }

    fn is_active(
        probes: &Mutex<MemoMap<MountPath, Arc<RetainedArcValue<std::sync::atomic::AtomicUsize>>>>,
        path: &MountPath, projection: Projection<'_>,
    ) -> Result<bool, NativeMountError> {
        for (active, counter) in lock_recover(probes).iter() {
            projection.work.native_materialization_work().charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            }).map_err(detach_work_error)?;
            if counter.load(std::sync::atomic::Ordering::Acquire) != 0
                && matches!(projfs_path_suffix_with_work(active, path, projection.work)?, Some([]))
            { return Ok(true); }
        }
        Ok(false)
    }

    /// Decrementing the actual request lifetime cannot be cancelled.
    fn finish(&mut self) -> Result<(), NativeMountError> {
        let Some(counter) = self.counter.take() else { return Ok(()); };
        let last = counter.fetch_sub(1, std::sync::atomic::Ordering::AcqRel) == 1;
        let removed = if last {
            let mut active = lock_recover(self.probes);
            match active.index(self.path, self.projection.work) {
                Ok(Ok(index)) if active.entries.items[index].1.load(std::sync::atomic::Ordering::Acquire) == 0 =>
                    active.entries.remove(index, self.projection).map(Some),
                Ok(_) => Ok(None),
                Err(error) => Err(error),
            }
        } else { Ok(None) };
        // The outside map owner must be the last strong physical token.
        drop(counter);
        drop(removed?);
        self.projection.work.native_materialization_work().record_admitted(crate::WorkCounters::UNCHARGED).map_err(detach_work_error)
    }
}

impl Drop for MetadataProbeGuard<'_> {
    fn drop(&mut self) { let _ = self.finish(); }
}

fn probe_host_windows_metadata(
    path: &MountPath,
    probes: &Mutex<MemoMap<MountPath, Arc<RetainedArcValue<std::sync::atomic::AtomicUsize>>>>,
    projection: Projection<'_>,
) -> Result<HostWindowsMetadata, NativeMountError> {
    let mut guard = MetadataProbeGuard::enter(probes, path, projection)?;
    let metadata = host_windows_metadata(path, projection);
    let cleanup = guard.finish();
    let metadata = metadata?;
    cleanup?;
    Ok(metadata)
}

fn close_metadata_handle(
    baselines: &Mutex<MemoMap<u128, OpenMetadataState>>, file_id: u128, projection: Projection<'_>,
) -> Result<MetadataClose, NativeMountError> {
    let mut baselines = lock_recover(baselines);
    let Ok(index) = baselines.index(&file_id, projection.work)? else {
        return Ok(MetadataClose::Final(None));
    };
    if baselines.entries.items[index].1.open_handles > 1 {
        baselines.entries.items[index].1.open_handles -= 1;
        Ok(MetadataClose::Pending)
    } else {
        let (key, state) = baselines.entries.remove(index, projection)?;
        drop(key);
        Ok(MetadataClose::Final(Some(state.baseline)))
    }
}

fn record_metadata_open(
    baselines: &Mutex<MemoMap<u128, OpenMetadataState>>, file_id: u128,
    baseline: HostWindowsMetadata, projection: Projection<'_>,
) -> Result<(), NativeMountError> {
    let mut baselines = lock_recover(baselines);
    match baselines.index(&file_id, projection.work)? {
        Ok(index) => {
            let state = &mut baselines.entries.items[index].1;
            state.open_handles = state.open_handles.checked_add(1).ok_or(crate::WorkError::Overflow)?;
        }
        Err(index) => {
            let key = file_id.copy_owned(projection)?;
            baselines.entries.insert(index, (key, OpenMetadataState { baseline, open_handles: 1 }), projection)?;
        }
    }
    Ok(())
}

fn refresh_metadata_baseline(
    baselines: &Mutex<MemoMap<u128, OpenMetadataState>>, file_id: u128,
    baseline: HostWindowsMetadata, projection: Projection<'_>,
) -> Result<(), NativeMountError> {
    let mut baselines = lock_recover(baselines);
    match baselines.index(&file_id, projection.work)? {
        Ok(index) => baselines.entries.items[index].1.baseline = baseline,
        Err(index) => {
            let key = file_id.copy_owned(projection)?;
            baselines.entries.insert(index, (key, OpenMetadataState { baseline, open_handles: 0 }), projection)?;
        }
    }
    Ok(())
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
    path: &MountPath,
    mut metadata: FileMetadata,
    baseline: HostWindowsMetadata,
    current: HostWindowsMetadata,
    projection: Projection<'_>,
) -> Result<(), NativeMountError> {
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
    let source_work = projection.work.clone();
    verify_source_write_work(projection.source, projection.work)?;
    projection.source.set_attributes_with_meter(path, metadata, None, &source_work)
        .map_err(detach_source_error)?;
    verify_source_write_work(projection.source, projection.work)?;
    projection.source.flush_with_meter(&source_work).map_err(detach_source_error)?;
    verify_source_write_work(projection.source, projection.work)
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

fn unix_nanoseconds(windows_ticks: i64) -> Result<i64, NativeMountError> {
    crate::native_host::filetime_to_unix_nanoseconds(windows_ticks)
        .ok_or_else(|| crate::WorkError::Overflow.into())
}


/// Presented placeholder times round toward the past: a projection must not fail because its source keeps finer timestamps than Windows can name.
fn windows_time(field: MetadataField<i64>) -> Option<i64> {
    let MetadataField::Value(unix_nanoseconds) = field else {
        return None;
    };
    Some(crate::native_host::unix_nanoseconds_to_filetime(
        unix_nanoseconds,
    ))
}

// Every directory beneath the root is an ordinary one, and the root holds on disk everything the source lists: `ProjFS` merges an enumeration with what is on disk, so the provider enumerates nothing and names nothing.
unsafe fn start_directory(
    _callback_data: *const PRJ_CALLBACK_DATA,
    _enumeration_id: *const GUID,
    _projection: Projection<'_>,
) -> HRESULT {
    HR_OK
}

unsafe fn end_directory(
    _callback_data: *const PRJ_CALLBACK_DATA,
    _enumeration_id: *const GUID,
    _projection: Projection<'_>,
) -> HRESULT {
    HR_OK
}

unsafe fn get_directory(
    _callback_data: *const PRJ_CALLBACK_DATA,
    _enumeration_id: *const GUID,
    _search_expression: PCWSTR,
    _buffer: PRJ_DIR_ENTRY_BUFFER_HANDLE,
    _projection: Projection<'_>,
) -> HRESULT {
    HR_OK
}

unsafe fn placeholder(_callback_data: *const PRJ_CALLBACK_DATA, _projection: Projection<'_>) -> HRESULT {
    HR_FILE_NOT_FOUND
}

/// Empty owners allocate nothing; their proof is this invocation's original custody, never a meter alias retained beyond the scoped native operation.
fn source_empty_vec<T>(projection: Projection<'_>) -> RetainedBuffer<Vec<T>> {
    RetainedBuffer { value: Some(Vec::new()), custody: projection.custody.clone(), bytes: 0, native_allocation: None, source_ownership: None }
}

/// Reallocation keeps both real backings live through the copy.
fn source_reserve<T>(
    projection: Projection<'_>, value: &mut Vec<T>, live: &mut u64,
    native_allocation: &mut Option<crate::NativeAllocation>, additional: usize,
) -> Result<(), NativeMountError> {
    let required = value.len().checked_add(additional).ok_or(crate::WorkError::Overflow)?;
    if required <= value.capacity() { return Ok(()); }
    let required = required.max(value.capacity().checked_mul(2).unwrap_or(required));
    let width = size_of::<T>();
    let bytes = required.checked_mul(width).ok_or(crate::WorkError::Overflow)? as u64;
    let copied = crate::WorkCounters {
        bytes_copied: value.len().checked_mul(width).ok_or(crate::WorkError::Overflow)? as u64,
        items_examined: value.len() as u64,
        ..crate::WorkCounters::UNCHARGED
    };
    let work = projection.work.native_materialization_work();
    work.admit(copied)?;
    let old_bytes = value.capacity().checked_mul(width).ok_or(crate::WorkError::Overflow)? as u64;
    let aggregate = native_allocation.as_ref().is_some_and(|allocation| allocation.bytes() > old_bytes);
    if aggregate && native_allocation.as_ref().is_some_and(|allocation|
        !allocation.work().shares_ledger(&work) || !allocation.work().shares_resource_allowance(&work))
    {
        return Err(NativeMountError::ControlPending);
    }
    let old_ordinary_bytes = if native_allocation.is_none() { old_bytes } else { 0 };
    let next_live = live.checked_sub(old_ordinary_bytes).ok_or(crate::WorkError::Overflow)?;
    let mut replacement_allocation = work.claim(bytes)?;
    let mut replacement = Vec::new();
    if let Err(error) = replacement.try_reserve_exact(required) {
        drop(replacement);
        return Err(NativeMountError::Allocation(error));
    }
    let actual = replacement.capacity().checked_mul(width).ok_or(crate::WorkError::Overflow)? as u64;
    replacement_allocation.reconcile_capacity(actual)?;
    if aggregate {
        native_allocation.as_mut().expect("the current path retains its native component claims")
            .absorb(&mut replacement_allocation)?;
    }
    replacement.append(value);
    drop(std::mem::replace(value, replacement));
    *live = next_live;
    if aggregate {
        native_allocation.as_mut().expect("aggregate physical path ownership").release(old_bytes)?;
    } else {
        let old_allocation = std::mem::replace(native_allocation, Some(replacement_allocation));
        drop(old_allocation);
    }
    retire_ordinary_storage(old_ordinary_bytes, projection.custody)?;
    work.record_admitted(copied).map_err(detach_work_error)
}

fn source_reserve_vec<T>(
    projection: Projection<'_>, value: &mut RetainedBuffer<Vec<T>>, additional: usize,
) -> Result<(), NativeMountError> {
    source_reserve(projection, value.value.as_mut().expect("live source vector"), &mut value.bytes,
        &mut value.native_allocation, additional)
}


fn source_copy_bytes(
    projection: Projection<'_>, bytes: &[u8],
) -> Result<RetainedBuffer<Vec<u8>>, NativeMountError> {
    let mut owned = source_empty_vec(projection);
    source_reserve_vec(projection, &mut owned, bytes.len())?;
    let copied = crate::WorkCounters {
        bytes_copied: bytes.len() as u64, items_examined: 1, ..crate::WorkCounters::UNCHARGED
    };
    projection.work.native_materialization_work().admit(copied).map_err(detach_work_error)?;
    owned.extend_from_slice(bytes);
    projection.work.native_materialization_work().record_admitted(copied).map_err(detach_work_error)?;
    Ok(owned)
}

/// Raw UTF-16LE is preserved, including unpaired surrogates.
fn source_utf16(
    projection: Projection<'_>, bytes: &[u8], terminate: bool,
) -> Result<Option<RetainedBuffer<Vec<u16>>>, NativeMountError> {
    if bytes.len() % 2 != 0 { return Ok(None); }
    let count = (bytes.len() / 2).checked_add(usize::from(terminate)).ok_or(crate::WorkError::Overflow)?;
    let mut owned = source_empty_vec(projection);
    source_reserve_vec(projection, &mut owned, count)?;
    for pair in bytes.chunks_exact(2) {
        let copied = crate::WorkCounters {
            bytes_copied: 2, items_examined: 1, ..crate::WorkCounters::UNCHARGED
        };
        projection.work.native_materialization_work().admit(copied).map_err(detach_work_error)?;
        let unit = u16::from_le_bytes([pair[0], pair[1]]);
        if unit == 0 {
            projection.work.native_materialization_work().record_admitted(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            }).map_err(detach_work_error)?;
            return Ok(None);
        }
        owned.push(unit);
        projection.work.native_materialization_work().record_admitted(copied).map_err(detach_work_error)?;
    }
    if terminate { owned.push(0); }
    Ok(Some(owned))
}

fn source_valid_utf16(projection: Projection<'_>, bytes: &[u8]) -> Result<bool, NativeMountError> {
    if bytes.len() % 2 != 0 { return Ok(false); }
    for pair in bytes.chunks_exact(2) {
        projection.work.native_materialization_work().charge(crate::WorkCounters {
            items_examined: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(detach_work_error)?;
        if pair == [0, 0] { return Ok(false); }
    }
    Ok(true)
}

fn source_clone_path(
    projection: Projection<'_>, path: &MountPath,
) -> Result<RetainedBuffer<MountPath>, NativeMountError> {
    let mut owned = RetainedBuffer { value: Some(MountPath::root()), custody: projection.custody.clone(), bytes: 0, native_allocation: None, source_ownership: None };
    source_reserve(projection, &mut owned.value.as_mut().expect("live path").components, &mut owned.bytes, &mut owned.native_allocation, path.components().len())?;
    for component in path.components() {
        let component = source_copy_bytes(projection, component)?;
        source_path_push(projection, &mut owned, component)?;
    }
    Ok(owned)
}

fn source_path_push(
    projection: Projection<'_>, path: &mut RetainedBuffer<MountPath>, mut component: RetainedBuffer<Vec<u8>>,
) -> Result<(), NativeMountError> {
    source_reserve(projection, &mut path.value.as_mut().expect("live path").components, &mut path.bytes,
        &mut path.native_allocation, 1)?;
    let copied = crate::WorkCounters {
        bytes_copied: size_of::<Vec<u8>>() as u64, items_examined: 1, ..crate::WorkCounters::UNCHARGED
    };
    projection.work.native_materialization_work().admit(copied).map_err(detach_work_error)?;
    let bytes = path.bytes.checked_add(component.bytes).ok_or(crate::WorkError::Overflow)?;
    if let Some(allocation) = component.native_allocation.as_mut() {
        path.native_allocation.as_mut().expect("new components use this path's full native resource view").absorb(allocation)?;
    }
    path.bytes = bytes;
    component.bytes = 0;
    let component = component.value.take().expect("moving an actual path component");
    path.components.push(component);
    projection.work.native_materialization_work().record_admitted(copied).map_err(detach_work_error)
}

fn source_path_pop(projection: Projection<'_>, path: &mut RetainedBuffer<MountPath>)
    -> Result<bool, NativeMountError>
{
    projection.work.native_materialization_work().charge(crate::WorkCounters {
        items_examined: 1, ..crate::WorkCounters::UNCHARGED
    }).map_err(detach_work_error)?;
    let Some(component) = path.components.pop() else { return Ok(false); };
    let bytes = component.capacity() as u64;
    drop(component);
    if let Some(allocation) = path.native_allocation.as_mut() {
        allocation.release(bytes)?;
    } else {
        path.custody.retire(bytes)?;
        path.bytes = path.bytes.checked_sub(bytes).ok_or(crate::WorkError::Overflow)?;
    }
    Ok(true)
}

/// What the projection writes for an already-live source component.
fn projected_entry(
    projection: Projection<'_>, directory: &MountPath, component: RetainedBuffer<Vec<u8>>,
    lookup: MountLookup, pin: Option<MountContentPin>,
) -> Result<Option<ProjectedEntry>, NativeMountError> {
    let Some(info) = basic(lookup.node, Some(lookup.metadata)) else { return Ok(None); };
    let Some(name) = source_utf16(projection, &component, true)? else { return Ok(None); };
    let symlink = if lookup.node.kind == MountNodeKind::SymbolicLink {
        let source_work = projection.work.clone();
        let mut path = source_clone_path(projection, directory)?;
        source_path_push(projection, &mut path, source_copy_bytes(projection, &component)?)?;
        let target = projection.source.read_link_with_meter(&path, &source_work).map_err(detach_source_error)?;
        let target = RetainedBuffer::source_chunk(target, projection)?;
        if !source_valid_utf16(projection, &target.value.as_ref().expect("live link packet").bytes)? { return Ok(None); }
        Some(target)
    } else { None };
    let directory_link = match symlink.as_ref() {
        Some(target) => links_to_directory(projection, directory, lookup.metadata.windows_attributes,
            &target.value.as_ref().expect("live link packet").bytes)?,
        None => false,
    };
    Ok(Some(ProjectedEntry {
        name, component, facts: EntryFacts { lookup, pin }, info, symlink_target: symlink, directory_link,
    }))
}

/// Windows links retain their recorded directory bit; otherwise resolve the exact raw native spelling through measured source and host query leaves.
fn links_to_directory(
    projection: Projection<'_>, directory: &MountPath, attributes: MetadataField<u32>, target: &[u8],
) -> Result<bool, NativeMountError> {
    if let MetadataField::Value(attributes) = attributes {
        return Ok(attributes & FILE_ATTRIBUTE_DIRECTORY.0 != 0);
    }
    let Some(target) = source_utf16(projection, target, false)? else { return Ok(false); };
    resolves_to_directory(projection, directory, &target, 0)
}

fn resolves_to_directory(
    projection: Projection<'_>, directory: &MountPath, target: &[u16], hops: u8,
) -> Result<bool, NativeMountError> {
    const MOST_HOPS: u8 = 8;
    if target.first().is_some_and(|unit| matches!(*unit, 47 | 92)) || target.get(1) == Some(&58) {
        use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _};
        use windows::Win32::Foundation::HANDLE;
        use windows::Win32::Storage::FileSystem::{
            CreateFileW, FILE_BASIC_INFO, FILE_FLAG_BACKUP_SEMANTICS, FILE_READ_ATTRIBUTES,
            FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, FileBasicInfo,
            GetFileInformationByHandleEx, OPEN_EXISTING,
        };
        let mut name = source_empty_vec(projection);
        source_reserve_vec(projection, &mut name, target.len().checked_add(1).ok_or(crate::WorkError::Overflow)?)?;
        let copied = crate::WorkCounters {
            bytes_copied: (target.len() as u64).checked_mul(2).ok_or(crate::WorkError::Overflow)?,
            items_examined: target.len() as u64, ..crate::WorkCounters::UNCHARGED
        };
        projection.work.native_materialization_work().admit(copied).map_err(detach_work_error)?;
        name.extend_from_slice(target);
        name.push(0);
        projection.work.native_materialization_work().record_admitted(copied).map_err(detach_work_error)?;
        // Follow the actual host target, rather than returning the reparse link's directory bit (which would misclassify a broken host link).
        let opened = projection_native_call(projection.work, false, || {
            // SAFETY: exact raw units plus NUL remain owned through this call.
            unsafe { CreateFileW(PCWSTR::from_raw(name.as_ptr()), FILE_READ_ATTRIBUTES.0,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, None, OPEN_EXISTING,
                FILE_FLAG_BACKUP_SEMANTICS, None) }
                .map_err(|error| std::io::Error::from_raw_os_error(error.code().0 & 0xffff))
        });
        let handle = match opened {
            Ok(handle) => handle,
            Err(NativeMountError::Io(error)) if matches!(error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory) => return Ok(false),
            Err(error) => return Err(error),
        };
        // SAFETY: this uniquely owned CreateFileW handle is transferred once.
        let file = unsafe { std::fs::File::from_raw_handle(handle.0) };
        let information = projection_native_call(projection.work, false, || {
            let mut information = FILE_BASIC_INFO::default();
            // SAFETY: the held handle and stack-sized output remain resident.
            unsafe { GetFileInformationByHandleEx(HANDLE(file.as_raw_handle()), FileBasicInfo,
                (&raw mut information).cast(), size_of::<FILE_BASIC_INFO>() as u32) }
                .map_err(|error| std::io::Error::from_raw_os_error(error.code().0 & 0xffff))?;
            Ok(information)
        })?;
        projection.work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED).map_err(detach_work_error)?;
        return Ok(information.FileAttributes & FILE_ATTRIBUTE_DIRECTORY.0 != 0);
    }
    let mut path = source_clone_path(projection, directory)?;
    for part in target.split(|unit| matches!(*unit, 47 | 92)) {
        projection.work.native_materialization_work().charge(crate::WorkCounters {
            items_examined: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(detach_work_error)?;
        match part {
            [] | [46] => {}
            [46, 46] => if !source_path_pop(projection, &mut path)? { return Ok(false); },
            part => {
                let mut component = source_empty_vec(projection);
                source_reserve_vec(projection, &mut component,
                    part.len().checked_mul(2).ok_or(crate::WorkError::Overflow)?)?;
                let copied = crate::WorkCounters {
                    bytes_copied: (part.len() as u64).checked_mul(2).ok_or(crate::WorkError::Overflow)?,
                    items_examined: part.len() as u64, ..crate::WorkCounters::UNCHARGED
                };
                projection.work.native_materialization_work().admit(copied).map_err(detach_work_error)?;
                for unit in part { component.extend_from_slice(&unit.to_le_bytes()); }
                projection.work.native_materialization_work().record_admitted(copied).map_err(detach_work_error)?;
                source_path_push(projection, &mut path, component)?;
            }
        }
    }
    let source_work = projection.work.clone();
    let lookup = match projection.source.lookup_with_meter(&path, &source_work) {
        Ok(lookup) => lookup,
        Err(MountSourceError::NotFound) => return Ok(false),
        Err(error) => return Err(detach_source_error(error)),
    };
    match lookup {
        Some(lookup) if lookup.node.kind == MountNodeKind::Directory => Ok(true),
        Some(lookup) if lookup.node.kind == MountNodeKind::SymbolicLink && hops < MOST_HOPS => {
            let next = match projection.source.read_link_with_meter(&path, &source_work) {
                Ok(next) => next,
                Err(MountSourceError::NotFound) => return Ok(false),
                Err(error) => return Err(detach_source_error(error)),
            };
            let next = RetainedBuffer::source_chunk(next, projection)?;
            if !source_path_pop(projection, &mut path)? { return Ok(false); }
            let Some(target) = source_utf16(projection, &next.value.as_ref().expect("live link packet").bytes, false)? else { return Ok(false); };
            resolves_to_directory(projection, &path, &target, hops + 1)
        }
        _ => Ok(false),
    }
}

struct SourceListingLease<'a> {
    lease: Option<Box<dyn super::MountViewLease>>,
    work: &'a super::NativeMountMeterWork,
}

impl SourceListingLease<'_> {
    fn finish(&mut self) -> Result<(), NativeMountError> {
        drop(self.lease.take());
        self.work.native_materialization_work().record_admitted(crate::WorkCounters::UNCHARGED).map_err(detach_work_error)
    }
}

impl Drop for SourceListingLease<'_> {
    fn drop(&mut self) {
        self.finish().expect("original source lease reserves its physical retirement");
    }
}

/// A source page is already LIVE in the original ledger.
struct SourceListingPage {
    entries: Vec<super::MountDirectoryEntry>,
    next_cursor: Option<Vec<u8>>,
    ownership: crate::demand::SourcePayloadOwnership,
}


fn source_insert<T>(
    projection: Projection<'_>, entries: &mut RetainedBuffer<Vec<T>>, index: usize, entry: T,
) -> Result<(), NativeMountError> {
    source_reserve_vec(projection, entries, 1)?;
    let copied = crate::WorkCounters {
        bytes_copied: (entries.len() - index + 1).checked_mul(size_of::<T>())
            .ok_or(crate::WorkError::Overflow)? as u64,
        items_examined: 1, ..crate::WorkCounters::UNCHARGED
    };
    projection.work.native_materialization_work().admit(copied).map_err(detach_work_error)?;
    entries.insert(index, entry);
    projection.work.native_materialization_work().record_admitted(copied).map_err(detach_work_error)
}

/// The same sorted producer preserves first-listed case-fold collisions without an allocating stable sort.
fn source_listing(
    projection: Projection<'_>, path: &MountPath,
) -> Result<RetainedBuffer<Vec<ProjectedEntry>>, NativeMountError> {
    let source_work = projection.work.clone();
    let lease = projection.source.acquire_binding_lease_with_meter(
        projection.source.binding_epoch_with_meter(&source_work).map_err(detach_source_error)?, &source_work,
    ).map_err(detach_source_error)?;
    let mut lease = SourceListingLease { lease: Some(lease), work: projection.work };
    let mut cursor: Option<usize> = None;
    let mut seen_cursors: RetainedBuffer<Vec<RetainedBuffer<Vec<u8>>>> = source_empty_vec(projection);
    let mut entries: RetainedBuffer<Vec<ProjectedEntry>> = source_empty_vec(projection);
    loop {
        let page = projection.source.read_directory_with_meter(
            path, cursor.map(|index| seen_cursors[index].as_slice()), DIRECTORY_PAGE_SIZE, &source_work,
        ).map_err(detach_source_error)?;
        let (entries_page, next_cursor, ownership) = page.into_parts();
        let mut page = SourceListingPage { entries: entries_page, next_cursor, ownership };
        {
            let mut drain = page.entries.drain(..);
            while drain.len() != 0 {
                projection.work.native_materialization_work().charge(crate::WorkCounters {
                    bytes_copied: size_of::<super::MountDirectoryEntry>() as u64,
                    items_examined: 1, ..crate::WorkCounters::UNCHARGED
                }).map_err(detach_work_error)?;
                let entry = drain.next().expect("nonempty source page");
                let lookup = MountLookup { node: entry.node, metadata: entry.metadata };
                let ownership = page.ownership.split_owned_payload(entry.name.capacity() as u64).map_err(detach_work_error)?;
                let component = RetainedBuffer::source_owned(entry.name, ownership, projection.custody.clone());
                let Some(projected) = projected_entry(projection, path, component, lookup, entry.pin)? else { continue; };
                let mut low = 0;
                let mut high = entries.len();
                let mut collision = false;
                while low < high {
                    projection.work.native_materialization_work().charge(crate::WorkCounters {
                        items_examined: 1, ..crate::WorkCounters::UNCHARGED
                    }).map_err(detach_work_error)?;
                    let middle = low + (high - low) / 2;
                    // SAFETY: both actual native name owners include NUL and remain resident throughout the comparison.
                    let compared = projection_native_call(projection.work, false, || Ok(unsafe {
                        PrjFileNameCompare(
                            PCWSTR::from_raw(entries[middle].name.as_ptr()),
                            PCWSTR::from_raw(projected.name.as_ptr()),
                        )
                    }))?;
                    if compared == 0 { collision = true; break; }
                    if compared < 0 { low = middle + 1; } else { high = middle; }
                }
                if !collision { source_insert(projection, &mut entries, low, projected)?; }
            }
        }
        let Some(next) = page.next_cursor.take() else { break; };
        let ownership = page.ownership.split_owned_payload(next.capacity() as u64).map_err(detach_work_error)?;
        let next = RetainedBuffer::source_owned(next, ownership, projection.custody.clone());
        let mut low = 0;
        let mut high = seen_cursors.len();
        while low < high {
            projection.work.native_materialization_work().charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            }).map_err(detach_work_error)?;
            let middle = low + (high - low) / 2;
            match seen_cursors[middle].as_slice().cmp(next.as_slice()) {
                std::cmp::Ordering::Equal => return Err(NativeMountError::Lifecycle("source directory listing repeated a cursor")),
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
            }
        }
        source_insert(projection, &mut seen_cursors, low, next)?;
        cursor = Some(low);
    }
    lease.finish()?;
    Ok(entries)
}

/// `ProjFS` asks only a placeholder for its content, and no file of the projection is one.
unsafe fn file_data(
    _callback_data: *const PRJ_CALLBACK_DATA,
    _byte_offset: u64,
    _length: u32,
    _projection: Projection<'_>,
) -> HRESULT {
    HR_UNEXPECTED
}

unsafe fn query_name(callback_data: *const PRJ_CALLBACK_DATA, projection: Projection<'_>) -> HRESULT {
    let Some((data, _runtime)) = runtime(callback_data) else { return HR_UNEXPECTED; };
    let result = (|| {
        let Some(path) = path_from(data.FilePathName, projection)? else { return Ok(HR_INVALID_DATA); };
        let source_work = projection.work.clone();
        match projection.source.lookup_with_meter(&path, &source_work).map_err(detach_source_error)? {
            Some(_) => Ok(HR_OK),
            None => Ok(HR_FILE_NOT_FOUND),
        }
    })();
    match result {
        Ok(result) => result,
        Err(error) => retain_callback_failure(projection, error),
    }
}

fn retain_callback_failure(projection: Projection<'_>, error: NativeMountError) -> HRESULT {
    let result = native_hresult(&error);
    if let NativeMountError::Source(source) = &error {
        let ordinary = match source.semantic_error() {
            MountSourceError::NotFound => true,
            MountSourceError::WorkspaceCause(cause) => !workspace_failure_is_fatal(cause),
            MountSourceError::WorkspacePrefixFailure { source, accounting } =>
                !workspace_failure_is_fatal(source) && !workspace_failure_is_fatal(accounting),
            MountSourceError::NativeCallbackDenied { .. } => true,
            MountSourceError::NativeWorkspaceCause(cause) => !workspace_failure_is_fatal(&cause.source),
            MountSourceError::Filesystem(cause) => !filesystem_failure_is_fatal(cause),
            MountSourceError::NativeText { .. } | MountSourceError::NativeStaticText { .. } => true,
            #[cfg(feature = "native-watch")]
            MountSourceError::NativeWatchIo(_) | MountSourceError::NativeWatchBackend(_)
                | MountSourceError::NativeWatchOriginalIo(_) | MountSourceError::NativeWatchOriginalBackend(_) => true,
            _ => false,
        };
        if ordinary { return result; }
        if matches!(source.semantic_error(),
            MountSourceError::NativeRecordIo(_, _)
                | MountSourceError::NativeRecordJson(_, _) | MountSourceError::NativeRecordLockTimeout(_, _)
                | MountSourceError::NativeRecordOwnershipConflict(_, _)
                | MountSourceError::NativeRecordIntegrity | MountSourceError::NativeRecordContextTransaction(_, _)
                | MountSourceError::NativeRecordCorrupt(_, _) | MountSourceError::PublicationRetryLimit)
        {
            return result;
        }
    }
    let native_work = match &error {
        NativeMountError::Materialization(cause) => Some(cause),
        NativeMountError::Source(source) => match source.semantic_error() {
            MountSourceError::NativeWork(cause) => Some(cause),
            _ => None,
        },
        _ => None,
    };
    if native_work.is_some_and(|cause| !super::native_materialization_failure_is_fatal(cause)) {
        return result;
    }
    let mut failure = lock_recover(projection.captures);
    failure.unreplayable_barrier = true;
    if failure.unreplayable.is_none() {
        failure.unreplayable = Some(NativeMountCallbackFailure { cause: error, work: projection.work.meter().lock().work });
    }
    result
}

/// A panic or original-publication uncertainty cannot be turned into a new host import retry.
fn callback_requires_permanent_fence(error: &NativeMountError) -> bool {
    match error {
        NativeMountError::DriverPanicked { .. } | NativeMountError::CallbackDenied { .. } => true,
        NativeMountError::Materialization(error) => super::native_materialization_failure_is_fatal(error),
        NativeMountError::Source(error) => match error.semantic_error() {
            MountSourceError::NativeWork(error) => super::native_materialization_failure_is_fatal(error),
            MountSourceError::WorkspaceCause(error) => workspace_failure_is_fatal(error),
            MountSourceError::WorkspacePrefixFailure { source, accounting } =>
                workspace_failure_is_fatal(source) || workspace_failure_is_fatal(accounting),
            MountSourceError::NativeCallbackDenied { .. } => false,
            MountSourceError::NativeWorkspaceCause(error) => workspace_failure_is_fatal(&error.source),
            MountSourceError::Filesystem(error) => filesystem_failure_is_fatal(error),
            MountSourceError::OriginalSnapshotUnavailable | MountSourceError::Indeterminate
                | MountSourceError::SelectedPublication(_)
                | MountSourceError::NativeRecordMaterializationInProgress => true,
            #[cfg(feature = "native-watch")]
            MountSourceError::SourceWatchStartup(_) | MountSourceError::NativeWatchStartupFailed { .. }
                | MountSourceError::NativeWatchSourceStartupFailed { .. } => true,
            _ => false,
        },
        _ => false,
    }
}

fn record_post_operation_failure(
    notification: PRJ_NOTIFICATION, retry_path: Option<&MountPath>,
    result: Result<(), NativeMountError>, projection: Projection<'_>,
) -> HRESULT {
    // A successful callback never proves an earlier deferred final-state capture joined.
    let Err(error) = result else { return HR_OK; };
    if callback_requires_permanent_fence(&error) {
        return retain_callback_failure(projection, error);
    }
    let result = native_hresult(&error);
    if !matches!(notification, PRJ_NOTIFICATION_NEW_FILE_CREATED
        | PRJ_NOTIFICATION_FILE_OVERWRITTEN | PRJ_NOTIFICATION_FILE_RENAMED
        | PRJ_NOTIFICATION_HARDLINK_CREATED
        | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED
        | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED)
    { return result; }
    let mut error = Some(error);
    let mut failure = lock_recover(projection.captures);
    if let Some(path) = retry_path {
        match failure.queue_capture(path, "post-operation capture failed", &mut error, projection) {
            Ok(()) => return result,
            Err(queue_error) => {
                let result = native_hresult(&queue_error);
                failure.unreplayable_barrier = true;
                if failure.unreplayable.is_none() {
                    failure.unreplayable = error.or(Some(queue_error)).map(|cause| NativeMountCallbackFailure {
                        cause, work: projection.work.meter().lock().work,
                    });
                }
                return result;
            }
        }
    }
    failure.unreplayable_barrier = true;
    if failure.unreplayable.is_none() {
        failure.unreplayable = error.map(|cause| NativeMountCallbackFailure { cause, work: projection.work.meter().lock().work });
    }
    result
}

fn notification_requires_authored_write(notification: PRJ_NOTIFICATION) -> bool {
    matches!(notification, PRJ_NOTIFICATION_PRE_RENAME | PRJ_NOTIFICATION_PRE_SET_HARDLINK
        | PRJ_NOTIFICATION_NEW_FILE_CREATED | PRJ_NOTIFICATION_FILE_OVERWRITTEN
        | PRJ_NOTIFICATION_FILE_RENAMED | PRJ_NOTIFICATION_HARDLINK_CREATED
        | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED
        | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED)
}

unsafe fn notification(
    callback_data: *const PRJ_CALLBACK_DATA, is_directory: bool, notification: PRJ_NOTIFICATION,
    destination_filename: PCWSTR, operation_parameters: *mut PRJ_NOTIFICATION_PARAMETERS,
    projection: Projection<'_>,
) -> HRESULT {
    let Some((data, runtime)) = runtime(callback_data) else { return HR_UNEXPECTED; };
    match unsafe { notification_with_work(data, runtime, is_directory, notification,
        destination_filename, operation_parameters, projection) } {
        Ok(result) => result,
        Err(error) => retain_callback_failure(projection, error),
    }
}

#[allow(clippy::too_many_lines, reason = "one SDK notification owns its actual admitted paths and source transition")]
unsafe fn notification_with_work(
    data: &PRJ_CALLBACK_DATA, runtime: &Runtime, is_directory: bool, notification: PRJ_NOTIFICATION,
    destination_filename: PCWSTR, operation_parameters: *mut PRJ_NOTIFICATION_PARAMETERS,
    projection: Projection<'_>,
) -> Result<HRESULT, NativeMountError> {
    if !runtime.writable { return Ok(HR_NOT_SUPPORTED); }
    let Some(path) = path_from(data.FilePathName, projection)? else { return Ok(HR_INVALID_DATA); };
    if matches!(notification, PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION
        | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED
        | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED)
    { runtime.projector.retry(); }
    if data.TriggeringProcessId == std::process::id()
        && matches!(notification, PRJ_NOTIFICATION_FILE_OPENED
            | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION)
    {
        if MetadataProbeGuard::is_active(runtime.metadata_probes.as_ref(), &path, projection)? {
            return Ok(HR_OK);
        }
        for (active, ..) in runtime.capture_probes.read()
            .unwrap_or_else(std::sync::PoisonError::into_inner).iter()
        {
            projection.work.native_materialization_work().charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            }).map_err(detach_work_error)?;
            if matches!(projfs_path_suffix_with_work(active, &path, projection.work)?, Some([])) {
                return Ok(HR_OK);
            }
        }
    }
    let file_id = file_id(data);
    let source_is_external = unsafe { empty_destination(data.FilePathName, projection)? };
    let destination_is_external = unsafe { empty_destination(destination_filename, projection)? };
    let destination = if destination_is_external { None }
        else { path_from(destination_filename, projection)? };
    if notification == PRJ_NOTIFICATION_PRE_SET_HARDLINK
        && (source_is_external || destination_is_external || destination.is_none())
    {
        // A cross-root link would admit writes that cannot produce this original projection's notification and source-admission proof.
        return Ok(HR_NOT_SAME_DEVICE);
    }
    if notification == PRJ_NOTIFICATION_PRE_RENAME
        && (source_is_external && destination_is_external
            || is_directory && (source_is_external || destination_is_external)
            || !destination_is_external && destination.is_none())
    { return Ok(HR_NOT_SAME_DEVICE); }
    if matches!(notification, PRJ_NOTIFICATION_PRE_RENAME | PRJ_NOTIFICATION_PRE_SET_HARDLINK) {
        return Ok(HR_OK);
    }
    if notification == PRJ_NOTIFICATION_NEW_FILE_CREATED {
        if is_directory {
            lock_recover(projection.captures).queue_subtree(
                &path, "new directory awaiting capture", projection)?;
        } else { defer_host_capture(projection.captures, &path, projection)?; }
        keep_file_notifications(notification, operation_parameters);
        return Ok(HR_OK);
    }
    if !is_directory && notification == PRJ_NOTIFICATION_FILE_OVERWRITTEN {
        keep_file_notifications(notification, operation_parameters);
        return Ok(HR_OK);
    }
    if path.components().is_empty() && notification == PRJ_NOTIFICATION_FILE_OPENED {
        return Ok(HR_OK);
    }
    if notification == PRJ_NOTIFICATION_HARDLINK_CREATED {
        let destination = destination.as_deref().ok_or(crate::DemandError::InvalidRequest)
            .map_err(NativeMountError::Demand)?;
        // Both final names are real queue producers; either denied prefix remains physically retained rather than claiming atomic enqueue.
        defer_host_capture(projection.captures, &path, projection)?;
        defer_host_capture(projection.captures, destination, projection)?;
        return Ok(HR_OK);
    }
    let retry_path = std::cell::Cell::new(None::<&MountPath>);
    let source_work = projection.work.clone();
    let operation = || -> Result<(), NativeMountError> {
        if path.components().is_empty()
            && matches!(notification, PRJ_NOTIFICATION_FILE_OPENED
                | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION)
        { return Ok(()); }
        if notification == PRJ_NOTIFICATION_FILE_OPENED {
            if has_pending_capture(projection.captures, &path, projection)? { return Ok(()); }
            let baseline = probe_host_windows_metadata(&path, runtime.metadata_probes.as_ref(), projection)?;
            return record_metadata_open(runtime.metadata_baselines.as_ref(), file_id, baseline, projection);
        }
        if matches!(notification, PRJ_NOTIFICATION_NEW_FILE_CREATED | PRJ_NOTIFICATION_FILE_OVERWRITTEN) {
            if is_directory { defer_host_capture(projection.captures, &path, projection)?; }
            return Ok(());
        }
        if matches!(notification, PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED
            | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED)
        {
            let close = close_metadata_handle(runtime.metadata_baselines.as_ref(), file_id, projection)?;
            let final_close = matches!(close, MetadataClose::Final(_));
            if notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED {
                lock_recover(projection.captures).pending_captures.remove(&path, projection)?;
                retry_path.set(Some(&path));
                if projection.source.lookup_with_meter(&path, &source_work).map_err(detach_source_error)?.is_some() {
                    verify_source_write_work(projection.source, projection.work)?;
                    projection.source.remove_with_meter(&path, None, &source_work).map_err(detach_source_error)?;
                    verify_source_write_work(projection.source, projection.work)?;
                }
            } else { defer_host_capture(projection.captures, &path, projection)?; }
            if !final_close && notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED {
                let baseline = probe_host_windows_metadata(&path, runtime.metadata_probes.as_ref(), projection)?;
                refresh_metadata_baseline(runtime.metadata_baselines.as_ref(), file_id, baseline, projection)?;
            }
            return Ok(());
        }
        if notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION {
            let baseline = match close_metadata_handle(runtime.metadata_baselines.as_ref(), file_id, projection)? {
                MetadataClose::Pending => return Ok(()),
                MetadataClose::Final(baseline) => baseline,
            };
            if has_pending_capture(projection.captures, &path, projection)? { return Ok(()); }
            let Some(baseline) = baseline else {
                return defer_host_capture(projection.captures, &path, projection);
            };
            retry_path.set(Some(&path));
            let host = probe_host_windows_metadata(&path, runtime.metadata_probes.as_ref(), projection)?;
            if baseline.identity != host.identity || baseline.links != host.links || baseline.size != host.size {
                lock_recover(runtime.projection.as_ref()).bindings.remove(&path, projection)?;
                return defer_host_capture(projection.captures, &path, projection);
            }
            let cached = lock_recover(runtime.projection.as_ref()).bindings.get(&path, projection)?.copied();
            if !metadata_changed_since_open(baseline, host)
                && !has_pending_capture(projection.captures, &path, projection)?
                && let Some(binding) = cached
                && binding.host == host && host.links == Some(1)
            {
                match projection.source.acquire_binding_lease_with_meter(Some(binding.basis.binding), &source_work) {
                    Ok(lease) => {
                        let valid = binding.basis.still_describes(projection, &path, Some(binding.file_id));
                        drop(lease);
                        let cleanup = projection.work.native_materialization_work().record_admitted(crate::WorkCounters::UNCHARGED).map_err(detach_work_error);
                        let valid = valid?;
                        cleanup?;
                        if valid { return Ok(()); }
                    }
                    Err(MountSourceError::Stale) => {}
                    Err(error) => return Err(detach_source_error(error)),
                }
            }
            let basis = ReadBasis::sample(projection)?;
            let lookup = projection.source.lookup_with_meter(&path, &source_work).map_err(detach_source_error)?;
            match lookup {
                Some(_) if metadata_changed_since_open(baseline, host) => {
                    verify_source_write_work(projection.source, projection.work)?;
                    runtime.metadata_edits.with(&host.identity, projection, || {
                        let Some(lookup) = projection.source.lookup_with_meter(&path, &source_work)
                            .map_err(detach_source_error)? else {
                            return defer_host_capture(projection.captures, &path, projection);
                        };
                        capture_changed_windows_metadata(&path, lookup.metadata, baseline, host, projection)
                    })
                },
                Some(lookup) => {
                    if let Some(basis) = basis
                        && !has_pending_capture(projection.captures, &path, projection)?
                        && host.links == Some(1)
                    {
                        lock_recover(runtime.projection.as_ref()).remember_binding(&path,
                            ReadOnlyBinding { host, file_id: lookup.node.file_id, basis }, projection)?;
                    }
                    Ok(())
                }
                None => {
                    lock_recover(runtime.projection.as_ref()).bindings.remove(&path, projection)?;
                    defer_host_capture(projection.captures, &path, projection)
                }
            }
        } else if notification == PRJ_NOTIFICATION_FILE_RENAMED {
            let _tree = loop {
                projection.work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED).map_err(detach_work_error)?;
                match runtime.projector.tree.try_lock() {
                    Ok(guard) => break guard,
                    Err(std::sync::TryLockError::Poisoned(error)) => break error.into_inner(),
                    Err(std::sync::TryLockError::WouldBlock) =>
                        std::thread::sleep(std::time::Duration::from_millis(1)),
                }
            };
            let current = runtime.projector.current_beneath(projection, &path)?;
            handle_rename_source(&path, source_is_external, destination_is_external,
                is_directory, destination.as_deref(), projection)?;
            if !source_is_external && !destination_is_external
                && let Some(destination) = destination.as_deref()
            {
                lock_recover(projection.captures).rename_pending_captures(&path, destination, projection)?;
                let carried = Carried::read(projection, &path, destination, &current.items)?;
                runtime.projector.renamed(projection, &path, destination, carried)?;
            }
            Ok(())
        } else {
            Err(NativeMountError::Lifecycle("ProjFS emitted an unadmitted write notification"))
        }
    };
    let result = runtime.callbacks.call_observed(file_id, projection.work, operation, |result| {
        record_post_operation_failure(notification, retry_path.get(), result, projection)
    });
    keep_file_notifications(notification, operation_parameters);
    result
}

/// Keeps [`FILE_NOTIFICATIONS`] for a file whose notification set `ProjFS` lets the provider replace on this callback.
fn keep_file_notifications(
    notification: PRJ_NOTIFICATION,
    operation_parameters: *mut PRJ_NOTIFICATION_PARAMETERS,
) {
    if operation_parameters.is_null() {
        return;
    }
    // SAFETY: ProjFS supplies the writable union member matching each of these notifications for the callback's duration.
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
    source: &MountPath, source_is_external: bool, destination_is_external: bool,
    is_directory: bool, destination: Option<&MountPath>, projection: Projection<'_>,
) -> Result<(), NativeMountError> {
    if source_is_external {
        let destination = destination.ok_or(crate::DemandError::InvalidRequest)
            .map_err(NativeMountError::Demand)?;
        return if is_directory {
            lock_recover(projection.captures).queue_subtree(
                destination, "imported directory awaiting capture", projection)
        } else { defer_host_capture(projection.captures, destination, projection) };
    }
    if destination_is_external {
        // Only this name vanished. No path outside the original root is read.
        return defer_host_capture(projection.captures, source, projection);
    }
    let destination = destination.ok_or(crate::DemandError::InvalidRequest)
        .map_err(NativeMountError::Demand)?;
    let source_work = projection.work.clone();
    if projection.source.lookup_with_meter(source, &source_work).map_err(detach_source_error)?.is_none() {
        return if is_directory {
            lock_recover(projection.captures).queue_subtree(
                destination, "renamed directory awaiting capture", projection)
        } else { defer_host_capture(projection.captures, destination, projection) };
    }
    verify_source_write_work(projection.source, projection.work)?;
    projection.source.rename_with_meter(source, destination, true, &source_work).map_err(detach_source_error)?;
    verify_source_write_work(projection.source, projection.work)?;
    projection.source.flush_with_meter(&source_work).map_err(detach_source_error)?;
    verify_source_write_work(projection.source, projection.work)
}

/// No handler accepts an asynchronous SDK command or returns E_PENDING.
unsafe extern "system" fn cancel(callback_data: *const PRJ_CALLBACK_DATA) {
    let Some((_data, runtime)) = (unsafe { runtime(callback_data) }) else { return; };
    let result = with_callback_meter(runtime, |meter| {
        meter.native_materialization_work().admit(crate::WorkCounters::UNCHARGED).map_err(detach_work_error)?;
        provider_stack::run_with_meter(Some(meter), || HR_OK).map(|_| ()).map_err(|error| {
            lock_recover(runtime.post_operation_failure.as_ref())
                .provider_control_failure.get_or_insert(error);
            error.into_mount_error()
        })
    });
    if let Err(error) = result {
        let mut failure = lock_recover(runtime.post_operation_failure.as_ref());
        failure.unreplayable_barrier = true;
        if failure.unreplayable.is_none() { failure.unreplayable = Some(error); }
    }
}

/// The `ProjFS` entry point for `callback`: it runs the callback on the provider's stack, not on the stack of the thread `ProjFS` calls from.
macro_rules! on_provider_stack {
    ($level:ident, $callback:ident($data:ident: $data_type:ty $(, $argument:ident: $type:ty)*) $(; authored_write: $write:expr)?) => {{
        unsafe extern "system" fn entry($data: $data_type $(, $argument: $type)*) -> HRESULT {
            let span = callback_span!($level, $callback);
            // SAFETY: the callback data and its heap-stable runtime remain owned until the SDK drain has returned every callback.
            let Some((data, runtime)) = (unsafe { runtime($data) }) else { return HR_UNEXPECTED; };
            let authored_write = false $(|| $write)?;
            let invoked = with_callback_meter_for_role(runtime, authored_write, |meter| {
                let custody = crate::demand::NativeMountAllocationCustody::new_with_meter(meter)?;
                let projection = callback_projection(runtime, data, meter, &custody);
                provider_stack::run_with_meter(Some(meter), || unsafe {
                    $callback($data $(, $argument)*, projection)
                }).map_err(|error| {
                    lock_recover(runtime.post_operation_failure.as_ref())
                        .provider_control_failure.get_or_insert(error);
                    error.into_mount_error()
                })
            });
            let result = match invoked {
                Ok(result) => result,
                Err(error) => {
                    let result = native_hresult(&error.cause);
                    let mut failures = lock_recover(runtime.post_operation_failure.as_ref());
                    failures.unreplayable_barrier = true;
                    if failures.unreplayable.is_none() { failures.unreplayable = Some(error); }
                    result
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
        ); authored_write: notification_requires_authored_write(kind))),
        CancelCommandCallback: Some(cancel),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CallbackGate, PostOperationFailures, Projector, ReadBasis, flush_callback_gate,
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

    fn native_write_test_meter(
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> crate::native_mount::NativeMountMeterWork {
        crate::NativeMaterializationWork::new(budget, cancellation)
            .expect("the actual invocation admits its one original state").native_mount_work()
    }


    #[test]
    fn native_sdk_arc_clones_keep_one_guard_through_the_actual_payload_drop()
        -> Result<(), Box<dyn std::error::Error>>
    {
        struct PayloadDrop {
            original: Arc<crate::demand::NativeDemandReadMeter>,
            observed_live: Arc<std::sync::atomic::AtomicU64>,
        }
        impl Drop for PayloadDrop {
            fn drop(&mut self) {
                self.observed_live.store(self.original.lock().live_allocation_bytes(), std::sync::atomic::Ordering::Release);
            }
        }
        let original = native_write_test_meter(WorkBudget::UNBOUNDED, &CancellationToken::new());
        let custody = crate::native_mount::NativeMountAllocationCustody::new_with_meter(&original)?;
        let resource = original.native_materialization_work().native_volume_work(WorkBudget {
            allocation_operations: 1, ..WorkBudget::UNBOUNDED
        })?;
        let observed_live = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let before_live = original.meter().lock().live_allocation_bytes();
        let bytes = std::mem::size_of::<super::RetainedArcValue<PayloadDrop>>() as u64;
        let first = super::retained_arc_on_meter(PayloadDrop {
            original: Arc::clone(original.meter()), observed_live: Arc::clone(&observed_live),
        }, &custody, &resource.native_mount_work())?;
        let before = resource.receipt();
        let second = first.clone();
        let weak = Arc::downgrade(&first);
        drop(first);
        assert_eq!(resource.receipt(), before, "Arc clones allocate no extra body or guard");
        assert_eq!(original.meter().lock().live_allocation_bytes(), before_live + bytes);
        drop(second);
        assert_eq!(observed_live.load(std::sync::atomic::Ordering::Acquire), before_live + bytes,
            "the actual SDK payload drops before its one original native guard");
        assert_eq!(original.meter().lock().live_allocation_bytes(), before_live);
        assert!(weak.upgrade().is_none(), "the remaining opaque Weak does not own SDK payload");
        drop(weak);
        Ok(())
    }

    #[tokio::test]
    async fn native_memo_backing_denial_preserves_its_owner_and_replacement_keeps_the_new_view()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let source = windows_checkout_source().await?;
        let failure = Mutex::new(PostOperationFailures::default());
        let (result, _, live) = measured_capture_projection(
            source.as_ref(), &failure, WorkBudget::UNBOUNDED, &CancellationToken::new(),
            |projection| {
                let original = projection.work.native_materialization_work();
                let first = original.native_volume_work(WorkBudget {
                    peak_allocation_bytes: 8, ..WorkBudget::UNBOUNDED
                })?;
                let denied = original.native_volume_work(WorkBudget {
                    allocation_operations: 0, ..WorkBudget::UNBOUNDED
                })?;
                let replacement = original.native_volume_work(WorkBudget {
                    peak_allocation_bytes: 16, ..WorkBudget::UNBOUNDED
                })?;
                let first_work = first.native_mount_work();
                let denied_work = denied.native_mount_work();
                let replacement_work = replacement.native_mount_work();
                let mut memo = super::MemoVec::<u64>::default();
                memo.push(7, super::Projection { work: &first_work, ..projection })?;
                let before = original.receipt();
                assert!(matches!(memo.reserve_for(2, super::Projection { work: &denied_work, ..projection }),
                    Err(super::NativeMountError::Materialization(crate::NativeTreeMaterializationError::Work(
                        crate::WorkError::BudgetExceeded { counter: "allocation_operations", .. })))));
                assert_eq!(original.receipt(), before, "denial does not allocate a replacement");
                assert_eq!(memo.items, [7]);
                assert!(memo.native_allocation.as_ref().expect("actual native backing").work().shares_resource_allowance(&first));
                memo.reserve_for(2, super::Projection { work: &replacement_work, ..projection })?;
                assert_eq!(memo.items, [7]);
                assert!(memo.native_allocation.as_ref().expect("actual replacement").work().shares_resource_allowance(&replacement));
                let reused_first = first.claim(8)?;
                drop(reused_first);
                drop(memo);
                Ok(())
            },
        );
        result?;
        assert_eq!(live, 0);
        Ok(())
    }

    fn native_write_test_parent(
        root: &std::path::Path,
    ) -> Result<cap_std::fs::Dir, Box<dyn std::error::Error>> {
        // Runtime services and borrowed parent fixtures precede the invocation; every file import/request/fence/control inside it uses the real meter.
        acyclic_native_runtime::initialize_native_io_services()?;
        Ok(cap_std::fs::Dir::open_ambient_dir(root, cap_std::ambient_authority())?)
    }








    #[test]
    fn measured_writer_cancellation_keeps_the_real_prefix_and_uses_reserved_cleanup()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        let parent = native_write_test_parent(root.path())?;
        let cancellation = CancellationToken::new();
        let meter = native_write_test_meter(WorkBudget::UNBOUNDED, &cancellation);
        let custody = crate::demand::NativeMountAllocationCustody::new_with_meter(&meter)?;
        let mut writer = super::ProjectedNativeWriter::create(&parent, &[102], &meter, &custody, |_| Ok(()))?;
        let native_meter = meter.native_materialization_work().runtime_meter_owner();
        super::wait_native_unit_measured(writer.file().write_bounded_async(
            0, Bytes::from_static(b"physical"), Arc::clone(&native_meter),
        ).map_err(super::projection_io_error)?, &meter)?;
        let writes = meter.meter().lock().work.backend_write_operations;
        cancellation.cancel();
        let error = match writer.file().write_bounded_async(
            8, Bytes::from_static(b"not written"), native_meter,
        ) {
            Err(error) => super::projection_io_error(error),
            Ok(completion) => super::wait_native_unit_measured(completion, &meter)
                .expect_err("cancelled native operation cannot write"),
        };
        assert!(matches!(error, super::NativeMountError::Demand(crate::DemandError::Cancelled)));
        assert_eq!(std::fs::read(root.path().join("f"))?, b"physical");
        writer.retire()?;
        assert_eq!(std::fs::read(root.path().join("f"))?, b"physical",
            "positional-I/O close does not erase the intrinsic owner before its retirement");
        drop(writer);
        assert!(!root.path().join("f").exists());
        Ok(())
    }








    /// The originally created staging capability survives a rename.
    #[test]
    fn staging_keeps_original_capability_when_a_link_replaces_its_spelling() -> Result<(), Box<dyn std::error::Error>>
    {
        let parent = tempfile::tempdir()?;
        let root = parent.path().join("mount");
        std::fs::create_dir(&root)?;
        let cancellation = CancellationToken::new();
        let meter = native_write_test_meter(WorkBudget::UNBOUNDED, &cancellation);
        let identity = crate::NativeRootIdentity::of_root_path(&root)?;
        let custody = Mutex::new(None);
        super::with_staging_directory(&root, &meter, identity, &custody, |_| Ok(()))?;
        let stage = super::staging_path(&root);
        let created = crate::NativeRootIdentity::of_root_path(&stage)?;
        let moved = parent.path().join("original-stage");
        std::fs::rename(&stage, &moved)?;
        let elsewhere = tempfile::tempdir()?;
        std::os::windows::fs::symlink_dir(elsewhere.path(), &stage)?;
        let original = super::with_staging_directory(&root, &meter, identity, &custody,
            |original| crate::native_host::directory_identity_with_work(
                original, &mut &meter.native_materialization_work(),
            ).map_err(super::detach_work_error))?;
        assert_eq!(original, created);
        super::remove_staging_directory_measured(&custody, &mut &meter.native_materialization_work(), None)?;
        assert!(!moved.exists(), "the actual owned directory was removed");
        assert!(std::fs::symlink_metadata(&stage)?.file_type().is_symlink(),
            "the substituted link remains untouched");
        assert!(elsewhere.path().is_dir());
        Ok(())
    }


    #[test]
    fn staging_never_adopts_an_existing_empty_sibling()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let parent = tempfile::tempdir()?;
        let root = parent.path().join("mount");
        std::fs::create_dir(&root)?;
        let staging = super::staging_path(&root);
        std::fs::create_dir(&staging)?;
        let foreign = crate::NativeRootIdentity::of_root_path(&staging)?;
        let root_identity = crate::NativeRootIdentity::of_root_path(&root)?;
        let meter = native_write_test_meter(WorkBudget::UNBOUNDED, &CancellationToken::new());
        let custody = Mutex::new(None);
        let error = super::with_staging_directory(&root, &meter, root_identity, &custody, |_| Ok(()))
            .err().expect("FILE_CREATE must reject the existing directory");
        assert!(matches!(error, crate::NativeMountError::Io(error)
            if error.kind() == std::io::ErrorKind::AlreadyExists));
        assert!(super::lock_recover(&custody).is_none());
        assert_eq!(crate::NativeRootIdentity::of_root_path(&staging)?, foreign);
        assert_eq!(std::fs::read_dir(&staging)?.count(), 0);
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
        let mut observation = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &token).expect("the actual phase admits its one original native state");
        control.recover_measured(&root, false, &mut &observation)?;
        let mut bounded = crate::NativeMaterializationWork::new(crate::WorkBudget {
            allocation_operations: observation.receipt().allocation_operations,
            ..crate::WorkBudget::UNBOUNDED
        }, &token).expect("the actual phase admits its one original native state");
        assert!(matches!(
            control.recover_measured(&root, true, &mut &bounded),
            Err(super::NativeMountError::Work(_)),
        ));
        assert!(control.controls_pending());
        std::fs::rename(&root, &residue)?;
        std::fs::create_dir(&root)?;
        std::fs::write(root.join("replacement"), b"replacement")?;
        let mut retry = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &token).expect("the actual phase admits its one original native state");
        assert!(matches!(
            control.recover_measured(&root, true, &mut &retry),
            Err(super::NativeMountError::Demand(crate::DemandError::StaleSource)),
        ));
        assert!(control.controls_pending());
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
        let mut observed = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &token).expect("the actual phase admits its one original native state");
        super::remove_empty_recovery_destination_measured(&baseline, Some(baseline_identity), &mut &observed)?;
        assert!(!baseline.exists());
        let actual_dispositions = observed.receipt().backend_write_operations;
        assert!((1..=2).contains(&actual_dispositions), "one actual POSIX disposition and only its supported fallback");

        let root = parent.path().join("root");
        std::fs::create_dir(&root)?;
        let identity = crate::capture_root_identity(&root)?;
        let mut denied = crate::NativeMaterializationWork::new(crate::WorkBudget { backend_write_operations: 0, ..crate::WorkBudget::UNBOUNDED }, &token).expect("the actual phase admits its one original native state");
        assert!(matches!(
            super::remove_empty_recovery_destination_measured(&root, Some(identity), &mut &denied),
            Err(super::NativeMountError::Work(_)),
        ));
        assert!(root.is_dir());
        let mut exact = crate::NativeMaterializationWork::new(crate::WorkBudget {
            backend_write_operations: actual_dispositions,
            ..crate::WorkBudget::UNBOUNDED
        }, &token).expect("the actual phase admits its one original native state");
        super::remove_empty_recovery_destination_measured(&root, Some(identity), &mut &exact)?;
        assert!(!root.exists());

        std::fs::create_dir(&root)?;
        std::fs::write(root.join("authored"), b"must not be recursively removed")?;
        let identity = crate::capture_root_identity(&root)?;
        let mut nonempty = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &token).expect("the actual phase admits its one original native state");
        assert!(matches!(
            super::remove_empty_recovery_destination_measured(&root, Some(identity), &mut &nonempty),
            Err(super::NativeMountError::Io(_)),
        ));
        assert_eq!(std::fs::read(root.join("authored"))?, b"must not be recursively removed");
        Ok(())
    }

    #[test]
    fn empty_recovery_cleanup_refuses_root_created_after_absent_constructor_observation()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        let token = crate::CancellationToken::new();
        let mut work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &token).expect("the actual phase admits its one original native state");
        assert!(matches!(
            super::remove_empty_recovery_destination_measured(root.path(), None, &mut &work),
            Err(super::NativeMountError::Demand(crate::DemandError::StaleSource)),
        ));
        assert!(root.path().is_dir());
        Ok(())
    }

    #[tokio::test]
    async fn projfs_name_order_differs_from_sdk_cursor_order()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let source = windows_checkout_source().await?;
        for name in ["éclair", "Zebra", "entry000", "Alpha"] {
            source.create_file(&windows_path(name), FileMetadata::default())?;
        }
        let alpha = windows_name("Alpha");
        let lower_alpha = windows_name("alpha");
        assert!(alpha < lower_alpha, "the source cursor uses raw bytes, not native case folding");
        let expected = ["Alpha", "entry000", "Zebra", "éclair"]
            .map(|name| name.encode_utf16().chain(std::iter::once(0)).collect::<Vec<_>>());
        let read = |projection: super::Projection<'_>| {
            assert!(super::same_projfs_name_with_work(&alpha, &lower_alpha, projection.work)?);
            let projected = super::source_listing(projection, &MountPath::root())?;
            assert_eq!(projected.len(), expected.len());
            for (entry, name) in projected.iter().zip(&expected) {
                assert_eq!(entry.name.as_slice(), name.as_slice());
            }
            drop(projected);
            Ok(())
        };
        let cancellation = CancellationToken::new();
        let (result, exact, live) = measured_source_leaf(source.as_ref(), WorkBudget::UNBOUNDED, &cancellation, read);
        result?;
        assert_eq!(live, 0);
        let (result, counters, live) = measured_source_leaf(source.as_ref(), exact, &cancellation, read);
        result?;
        counters.verify(exact)?;
        assert_eq!(live, 0);
        Ok(())
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
        windows_checkout_source_with_work(None).await
    }

    async fn windows_checkout_source_with_work(
        original: Option<&crate::native_mount::NativeMountMeterWork>,
    ) -> Result<Arc<MemorySource>, Box<dyn std::error::Error>> {
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
        let checkout = Arc::new(SharedCheckout::new(checkout));
        let source = if let Some(original) = original {
            crate::native_mount::initialize_native_mount_services()?;
            let root = crate::kernel::NamespacePath::new(Vec::new(), config.limits)?;
            let allocation = original.native_materialization_work().claim(root.retained_bytes())?;
            CheckoutMountSource::new_at_meter(
                checkout, config, root, allocation, original,
                Some(&original.native_materialization_work()),
            )?
        } else {
            CheckoutMountSource::new(checkout, config)?
        };
        Ok(Arc::new(source))
    }

    #[tokio::test]
    #[ignore = "requires a host that permits mounting a ProjFS provider"]
    async fn prepared_restart_requires_original_issuer_and_fresh_stock_capture_gate()
        -> Result<(), Box<dyn std::error::Error>>
    {
        crate::native_mount::initialize_native_mount_services()?;
        acyclic_native_runtime::initialize_native_io_services()?;
        let original = crate::NativeMaterializationWork::new(
            WorkBudget::UNBOUNDED, &CancellationToken::new(),
        ).map_err(|(cause, _)| cause)?;
        let meter = original.native_mount_work();
        let source = windows_checkout_source_with_work(Some(&meter)).await?;
        let root = tempfile::tempdir()?;
        let destination = root.path().join("view");
        std::fs::create_dir(&destination)?;
        let request = crate::NativeMountRequest {
            mount_id: crate::MountId::new(), volume_id: source.volume_id()?,
            destination: destination.clone(), writable: false,
        };
        let checkout = source.native_namespace_lineage().expect("stock borrowed lineage")
            .owner.retain().downcast::<SharedCheckout<
                crate::facade::MemoryAuthorityBackend, crate::facade::MemoryObjectBackend,
            >>().map_err(|_| "original stock checkout alias differs")?;
        let mut old = crate::native_mount::prepare_native_mount_start_with_meter(&meter)?
            .start_with_meter(request.clone(), source.clone(), &meter)?;
        old.stop_with_cleanup_wait(&CancellationToken::new())?;
        assert!(old.cleanup_state().cleanup_complete);
        let refused = crate::native_mount::prepare_native_mount_start_with_meter(&meter)?
            .restart_with_meter(old, source.clone(), &meter).err().expect("old permanently retired capture gate is denied");
        let crate::NativeMountError::StartupFailed(refused) = refused else { panic!("original preborn failure custody"); };
        assert!(refused.retired_owner().expect("complete original outcome").cleanup_state().cleanup_complete);
        let (_, next, old) = refused.into_parts();
        assert!(next.destination().as_os_str().is_empty(), "no adopted warm path before admission");
        assert!(next.cleanup_state().source_retained);
        drop(next);
        let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
        config.profile = crate::model::FilesystemProfile::Windows;
        let root_path = crate::kernel::NamespacePath::new(Vec::new(), config.limits)?;
        let fresh = CheckoutMountSource::new_at_meter(
            checkout, config, root_path, original.claim_allocation(0, 0)?,
            &meter, Some(&original),
        )?.into_arc_meter(&meter)?;
        let cold = crate::native_mount::prepare_native_mount_start_with_meter(&meter)?
            .start_with_meter(request, fresh.clone(), &meter).err().expect("cold raw ProjFS marker never grants authority");
        let crate::NativeMountError::StartupFailed(mut cold) = cold else { panic!("cold failure retains actual controls"); };
        assert!(matches!(cold.error(), crate::NativeMountError::UnsafeRecoveryDestination { .. }));
        cold.owner_mut().stop_with_cleanup_wait(&CancellationToken::new())?;
        // Cold refusal retired its source gate; use another genuinely new source.
        let checkout = fresh.native_namespace_lineage().expect("original stock alias")
            .owner.retain().downcast::<SharedCheckout<
                crate::facade::MemoryAuthorityBackend, crate::facade::MemoryObjectBackend,
            >>().map_err(|_| "original stock checkout alias differs")?;
        let fresh = CheckoutMountSource::new_at_meter(
            checkout, config, crate::kernel::NamespacePath::new(Vec::new(), config.limits)?,
            original.claim_allocation(0, 0)?, &meter, Some(&original),
        )?.into_arc_meter(&meter)?;
        let mut restarted = crate::native_mount::prepare_native_mount_start_with_meter(&meter)?
            .restart_with_meter(old.expect("actual original issuer retained once"), fresh, &meter)?;
        assert_eq!(restarted.destination(), destination);
        restarted.stop_with_cleanup_wait(&CancellationToken::new())?;
        assert!(restarted.cleanup_state().cleanup_complete);
        assert!(!original.cancellation().is_cancelled(), "retirement never resets or cancels original work");
        Ok(())
    }

    /// Exercises the actual source/native leaves with one original invocation; fixture construction and borrowed input paths are outside that boundary.
    fn measured_source_leaf(
        source: &dyn MountFilesystem, budget: WorkBudget, cancellation: &CancellationToken,
        operation: impl FnOnce(super::Projection<'_>) -> Result<(), super::NativeMountError>,
    ) -> (Result<(), super::NativeMountError>, crate::WorkCounters, u64) {
        let root_identity = crate::capture_root_identity(std::path::Path::new("."))
            .expect("source fixture root has an actual native identity");
        let work = match crate::NativeMaterializationWork::new(budget, cancellation) {
            Ok(work) => work,
            Err((cause, spent)) => return (Err(super::detach_work_error(cause)), spent, 0),
        };
        let meter = work.native_mount_work();
        let result = (|| {
            let custody = crate::native_mount::NativeMountAllocationCustody::new_with_meter(&meter)?;
            let captures = Mutex::new(PostOperationFailures::default());
            let staging_custody = Mutex::new(None);
            operation(super::Projection {
                source, work: &meter, custody: &custody,
                context: Default::default(), root: std::path::Path::new("."),
                root_identity, staging_custody: &staging_custody,
                captures: &captures, starting: true,
            })
        })();
        work.record_admitted(crate::WorkCounters::UNCHARGED).expect("actual source leaf cleanup reconciles");
        let payload_live = work.native_read_meter().lock().live_allocation_bytes()
            .checked_sub(crate::NativeMaterializationWork::accounting_state_bytes()).expect("the original accounting body remains owned");
        (result, work.receipt(), payload_live)
    }

    fn measured_capture_projection(
        source: &dyn MountFilesystem, failure: &Mutex<PostOperationFailures>,
        budget: WorkBudget, cancellation: &CancellationToken,
        operation: impl FnOnce(super::Projection<'_>) -> Result<(), super::NativeMountError>,
    ) -> (Result<(), super::NativeMountError>, crate::WorkCounters, u64) {
        measured_source_leaf(source, budget, cancellation, |projection| {
            operation(super::Projection { captures: failure, ..projection })
        })
    }

    fn queue_capture_fixture(
        source: &dyn MountFilesystem, failure: &Mutex<PostOperationFailures>,
        path: &MountPath, reason: &'static str,
    ) {
        measured_capture_projection(source, failure, WorkBudget::UNBOUNDED, &CancellationToken::new(),
            |projection| failure.lock().expect("capture fixture")
                .queue_capture(path, reason, &mut None, projection)).0.expect("actual capture producer");
    }

    fn capture_serial_fixture(failure: &Mutex<PostOperationFailures>, path: &MountPath) -> u64 {
        failure.lock().expect("capture fixture").pending_captures.iter()
            .find(|(key, _)| *key == path).expect("actual pending path").1.serial
    }


    #[tokio::test]
    async fn source_listing_pages_preserve_native_order_and_name_ownership()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let source = windows_checkout_source().await?;
        let count = super::DIRECTORY_PAGE_SIZE + 2;
        for index in 0..count {
            source.create_file(&windows_path(&format!("entry{index:04}")), FileMetadata::default())?;
        }
        let cancellation = CancellationToken::new();
        let read = |projection: super::Projection<'_>| {
            let baseline = projection.work.meter().lock().live_allocation_bytes();
            let mut entries = super::source_listing(projection, &MountPath::root())?;
            assert_eq!(entries.len(), count as usize);
            for adjacent in entries.windows(2) {
                // SAFETY: these are the real retained NUL-terminated names.
                assert!(unsafe { super::PrjFileNameCompare(
                    PCWSTR::from_raw(adjacent[0].name.as_ptr()),
                    PCWSTR::from_raw(adjacent[1].name.as_ptr()),
                ) } < 0);
            }
            let escaped = entries[0].component.take();
            let escaped_bytes = escaped.capacity() as u64;
            assert!(escaped.source_ownership.as_ref().is_some_and(|owner| owner.has_native_custody()),
                "the projected component retains its intrinsic page partition");
            let mut memo = super::MemoMap::default();
            memo.insert_owned(escaped, true, projection)?;
            drop(entries);
            assert!(projection.work.meter().lock().live_allocation_bytes() >= baseline + escaped_bytes,
                "the actual moved name stays funded after its page and listing drop");
            drop(memo);
            assert_eq!(projection.work.meter().lock().live_allocation_bytes(), baseline);
            Ok(())
        };
        let (observed, _, live) = measured_source_leaf(source.as_ref(), WorkBudget::UNBOUNDED, &cancellation, read);
        observed?;
        assert_eq!(live, 0);
        Ok(())
    }


    #[tokio::test]
    async fn callback_raw_path_preserves_opaque_spelling()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let source = windows_checkout_source().await?;
        let raw = [0xd800, 65, 92, 98, 46, 84, 88, 84, 0];
        let cancellation = CancellationToken::new();
        let parse = |projection: super::Projection<'_>| {
            let path = super::path_from(PCWSTR::from_raw(raw.as_ptr()), projection)?
                .ok_or(super::NativeMountError::Lifecycle("valid raw callback spelling rejected"))?;
            assert_eq!(path.components.len(), 2);
            assert_eq!(path.components[0], [0, 0xd8, 65, 0]);
            assert_eq!(path.components[1], [98, 0, 46, 0, 84, 0, 88, 0, 84, 0]);
            drop(path);
            Ok(())
        };
        let (observed, _, live) = measured_source_leaf(source.as_ref(), WorkBudget::UNBOUNDED, &cancellation, parse);
        observed?;
        assert_eq!(live, 0);
        Ok(())
    }







    #[tokio::test]
    async fn source_packets_survive_actual_native_writes_and_late_write_denial()
        -> Result<(), Box<dyn std::error::Error>>
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        acyclic_native_runtime::initialize_native_io_services()?;
        let source = windows_checkout_source().await?;
        let path = windows_path("packet.bin");
        let component = windows_name("packet.bin");
        source.create_file(&path, FileMetadata::default())?;
        let content = vec![0x6d; super::WRITE_CHUNK_BYTES as usize + 17];
        source.write_range(&path, 0, Bytes::copy_from_slice(&content))?;
        let temporary = tempfile::tempdir()?;
        let cancellation = CancellationToken::new();
        let run = |budget, name: &str| -> Result<_, Box<dyn std::error::Error>> {
            let destination = temporary.path().join(name);
            let file = std::fs::OpenOptions::new().read(true).write(true).create_new(true)
                .custom_flags(windows::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED.0)
                .open(&destination)?;
            // SAFETY: this fresh FILE_FLAG_OVERLAPPED handle has no independent I/O or completion-port association; the fixture transfers it.
            let file = unsafe { acyclic_native_runtime::NativeFile::from_overlapped_file_unchecked(file)? };
            let (result, counters, live) = measured_source_leaf(source.as_ref(), budget, &cancellation, |projection| {
                let entries = super::source_listing(projection, &MountPath::root())?;
                let entry = entries.iter().find(|entry| entry.component.as_slice() == component.as_slice())
                    .ok_or(super::NativeMountError::Lifecycle("actual source packet file absent"))?;
                assert!(super::write_listed_content(projection, &path, entry, &file)?);
                drop(entries);
                Ok(())
            });
            drop(file);
            Ok((result, counters, live, std::fs::read(destination)?))
        };
        let (observed, exact, live, written) = run(WorkBudget::UNBOUNDED, "observed")?;
        observed?;
        assert_eq!(written, content);
        assert_eq!(live, 0);
        let denied = WorkBudget { backend_write_operations: 1, ..exact };
        let (failure, prefix, live, written) = run(denied, "denied")?;
        assert!(matches!(failure, Err(super::NativeMountError::Work(_))));
        prefix.verify(denied)?;
        assert_eq!(written, content[..super::WRITE_CHUNK_BYTES as usize]);
        assert_eq!(live, 0, "accepted native packet completion retired before the source loan returned");
        Ok(())
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
    #[ignore = "requires a host that supports ProjFS SDK symbolic-link placeholders"]
    async fn read_only_read_full_sdk_link_retires_physical_owners_and_preserves_actual_refusal_data()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let source = windows_checkout_source().await?;
        let target = windows_path("target.txt");
        source.create_file(&target, FileMetadata::default())?;
        source.write_range(&target, 0, Bytes::from_static(b"linked-data"))?;
        let link = windows_path("link");
        source.create_symbolic_link(&link, Bytes::from(windows_name("target.txt")), FileMetadata::default())?;
        let root = tempfile::tempdir()?;
        let destination = root.path().join("projection");
        std::fs::create_dir(&destination)?;
        let mut session = super::ProjFsSession::start(&crate::NativeMountRequest {
            mount_id: crate::MountId::new(), volume_id: source.volume_id()?,
            destination: destination.clone(), writable: false,
        }, Arc::clone(&source) as Arc<dyn MountFilesystem>).map_err(|failure| failure.error)?;
        let runtime = session.test_runtime().expect("actual original SDK runtime");
        let owner = {
            let state = super::lock_recover(&runtime.projector.state);
            let owner = state.written.iter().find_map(|(path, written)| {
                if path != &link { return None; }
                match written.owned.as_ref() { Some(super::Owned::Link(owner)) => Some(owner.clone()), _ => None }
            }).expect("this fixture uses the genuine SDK-born entry, not raw-link fallback");
            owner
        };
        assert!(owner.born.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(std::fs::read(destination.join("link"))?, b"linked-data");
        drop(runtime);
        let original = session.test_original_stop_work().expect("actual original authorization");
        let cancellation = original.native_materialization_work().cancellation().clone();
        drop(original);
        cancellation.cancel();
        session.stop_with_cleanup_wait(&CancellationToken::new())?;
        assert!(session.physical_retirement_complete());
        assert!(session.namespace_detach_confirmed());
        assert!(super::lock_recover(&owner.context).is_none(), "no stale SDK instance remains callable");
        let outcome = session.take_namespace_outcome().expect("actual original namespace carrier");
        assert!(outcome.capture_failure().is_none(), "read-only retirement never publishes authored data");
        let refused = *super::lock_recover(&owner.namespace_residue);
        if let Some(refused) = refused {
            assert_eq!(outcome.first_cleanup_residue(), Some((refused.hresult.0, refused.update_causes.0 as u32)));
            assert!(std::fs::symlink_metadata(destination.join("link")).is_ok(),
                "flags-NONE refusal preserved actual namespace DATA rather than broadening delete authority");
        } else {
            assert!(owner.retired.load(std::sync::atomic::Ordering::Acquire),
                "absence of residue requires actual SDK delete success");
        }
        assert!(cancellation.is_cancelled());
        Ok(())
    }





    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn cleanup_wait_refusal_preserves_a_renamed_original_stage_and_foreign_replacement()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = windows_checkout_source().await?;
        let (_root, destination, mut session) = mount_driver_source(&source)?;
        let original_meter = session.test_original_stop_work().expect("constructor work").clone();
        let original = original_meter.native_materialization_work().allocation_work();
        let runtime = session.test_runtime().expect("real SDK runtime");
        super::with_staging_directory(
            &destination, &original_meter, runtime.metadata_root.identity(), &runtime.staging_custody, |_| Ok(()),
        )?;
        let stage = super::staging_path(&destination);
        let moved = stage.with_extension("owned-before-wait-cleanup");
        std::fs::rename(&stage, &moved)?;
        std::fs::create_dir(&stage)?;
        let foreign = crate::NativeRootIdentity::of_root_path(&stage)?;
        let before = original.receipt();
        let denied_wait = CancellationToken::new();
        denied_wait.cancel();
        assert!(session.stop_with_cleanup_wait(&denied_wait).is_err());
        assert_eq!(original.receipt(), before);
        assert_eq!(session.test_sdk_stop_credit().expect("unspent SDK birth credit").backend_write_operations, 1);
        assert_eq!(session.test_worker_join_credit().expect("unspent worker join credit").backend_write_operations, 1);
        assert!(session.test_context().is_some());
        assert!(session.test_runtime().is_some());
        assert!(!session.namespace_detach_confirmed());
        assert_eq!(crate::NativeRootIdentity::of_root_path(&stage)?, foreign);
        assert!(moved.is_dir());
        drop(runtime);
        drop(original_meter);
        session.stop_with_cleanup_wait(&CancellationToken::new())?;
        assert!(session.namespace_detach_confirmed());
        assert!(session.physical_retirement_complete());
        assert!(!moved.exists());
        assert_eq!(crate::NativeRootIdentity::of_root_path(&stage)?, foreign,
            "retirement never adopts or removes the substituted sibling");
        Ok(())
    }


    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn nonempty_original_stage_preserves_namespace_data_and_retires_its_real_handle()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let source = windows_checkout_source().await?;
        let (_root, destination, mut session) = mount_driver_source(&source)?;
        let runtime = session.test_runtime().expect("actual original callback payload");
        let original = session.test_original_stop_work().expect("actual original producer work");
        super::with_staging_directory(&destination, &original,
            runtime.metadata_root.identity(), &runtime.staging_custody, |_| Ok(()))?;
        let stage = super::staging_path(&destination);
        let identity = super::lock_recover(&runtime.staging_custody).as_ref().expect("actual created stage").identity;
        std::fs::write(stage.join("preserved.txt"), b"namespace-data")?;
        drop(original);
        drop(runtime);
        session.stop_with_cleanup_wait(&CancellationToken::new())?;
        assert!(session.physical_retirement_complete());
        assert!(session.namespace_detach_confirmed());
        assert!(session.test_runtime().is_none());
        assert_eq!(std::fs::read(stage.join("preserved.txt"))?, b"namespace-data");
        let mut outcome = session.take_namespace_outcome().expect("the same funded original metadata carrier");
        let original = outcome.stage.as_ref().expect("original held-stage metadata, not an ambient replacement");
        assert!(original.directory.is_none(), "physical success requires the actual original directory handle to close");
        assert_eq!(original.identity, identity);
        assert!(!original.name.as_slice().is_empty());
        let (primary, secondary) = outcome.take_capture_failures().expect("actual namespace-only refusal");
        assert_eq!(super::native_mount_error_code(&primary), Some(super::HR_DIRECTORY_NOT_EMPTY));
        assert!(secondary.is_none(), "no authored capture failure was invented");
        assert!(outcome.take_capture_failures().is_none(), "original causes move once without copying");
        assert!(outcome.stage.as_ref().expect("DATA metadata remains").directory.is_none());
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn physical_sdk_retirement_preserves_original_user_root_and_foreign_contents()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = windows_checkout_source().await?;
        let (_root, destination, mut session) = mount_driver_source(&source)?;
        let identity = session.native_root_identity().expect("actual held user root");
        let original = session.test_original_stop_work().expect("actual original work");
        let runtime = session.test_runtime().expect("actual original SDK payload");
        super::with_staging_directory(&destination, &original, identity, &runtime.staging_custody, |_| Ok(()))?;
        drop(runtime);
        drop(original);
        std::fs::create_dir(destination.join("foreign-directory"))?;
        let foreign = crate::NativeRootIdentity::of_root_path(&destination.join("foreign-directory"))?;
        session.stop_with_cleanup_wait(&CancellationToken::new())?;
        assert!(session.physical_retirement_complete());
        assert!(session.namespace_detach_confirmed());
        assert!(session.test_runtime().is_none());
        assert_eq!(crate::NativeRootIdentity::of_root_path(&destination)?, identity);
        assert_eq!(crate::NativeRootIdentity::of_root_path(&destination.join("foreign-directory"))?, foreign);
        assert!(!super::staging_path(&destination).exists());
        let outcome = session.take_namespace_outcome().expect("original funded SDK metadata moves without another owner");
        assert!(outcome.qualification().is_some(), "qualification is actual original SDK information, not ambient marker adoption");
        Ok(())
    }



    #[tokio::test]
    #[ignore = "requires a host that permits mounting a writable ProjFS provider"]
    async fn original_capture_cancellation_preserves_dirty_data_and_retires_physical_owners()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = windows_checkout_source().await?;
        let (_root, destination, mut session) = mount_driver_source(&source)?;
        let path = windows_path("authored.txt");
        std::fs::write(destination.join("authored.txt"), b"only-host-copy")?;
        let runtime = session.test_runtime().expect("live callback runtime");
        queue_capture_fixture(source.as_ref(), runtime.post_operation_failure.as_ref(),
            &path, "authored close awaiting capture");
        let serial = capture_serial_fixture(runtime.post_operation_failure.as_ref(), &path);
        drop(runtime);
        let original = session.test_original_stop_work().expect("actual original execution frame");
        let original_cancellation = original.native_materialization_work().cancellation().clone();
        drop(original);
        original_cancellation.cancel();
        let wait = CancellationToken::new();
        session.stop_with_cleanup_wait(&wait)?;
        assert!(original_cancellation.is_cancelled(), "physical cleanup never resets authored authorization");
        assert!(session.namespace_detach_confirmed());
        assert!(session.physical_retirement_complete());
        assert!(session.test_runtime().is_none());
        assert!(session.test_context().is_none());
        assert_eq!(std::fs::read(destination.join("authored.txt"))?, b"only-host-copy");
        assert!(source.lookup(&path)?.is_none(), "denied original capture did not publish host data");
        let outcome = session.take_namespace_outcome().expect("original namespace data moves without a new map");
        assert!(outcome.capture_failure().is_some());
        assert_eq!(outcome.pending_capture_count(), 1);
        let queue = outcome.captures.as_ref().expect("the same original funded queue survives");
        assert_eq!(capture_serial_fixture(queue, &path), serial);
        Ok(())
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

        // Excluding FILE_SHARE_DELETE models a background compiler or language server that outlives the host tool boundary.
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

        // The provider is already stopped, so no callback can authenticate or publish this write.
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
        // ProjFS notifies the provider only of other processes' I/O.
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

        // A source-side create advances the source view, which the projection follows once revalidated.
        source.create_file(&windows_path("b.txt"), FileMetadata::default())?;
        session.revalidate()?;
        assert_eq!(sorted_names(&destination)?, ["a.txt", "b.txt"]);

        // Creates and removals through the mount stay exact before and after the provider captures them into the source.
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

    /// A link is projected with its target, as a placeholder or, where the volume refuses link placeholders, as an ordinary link, and follows a source that points it elsewhere.
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

    /// Runs `script` in another process: `ProjFS` notifies the provider only of other processes' I/O.
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

    /// A time set through a writable handle stays, on disk and in the source, for empty files and others, also while another process reads the files: every file of the projection is an ordinary file.
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

    /// What another process writes to a projected file stays, and reaches the source.
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

    /// A link another process holds open while the source puts a directory in its place gives way to that directory once it is closed.
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

    /// A link another process holds open goes once it is closed: its removal is tried again, not given up.
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

    /// What the user put in place of a projected link stays when the source removes the link.
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

    /// Revalidation waits while a superseded path is pending, which it is until its outcome is recorded, even once every change has been served, and returns as soon as it is settled.
    #[tokio::test]
    async fn edits_of_one_file_run_one_at_a_time() -> Result<(), Box<dyn std::error::Error>> {
        use super::{FileLocks, lock_recover};
        use std::sync::atomic::{AtomicUsize, Ordering};
        let source = windows_checkout_source().await?;
        let cancellation = CancellationToken::new();
        let (result, _, _) = measured_source_leaf(source.as_ref(), WorkBudget::UNBOUNDED,
            &cancellation, |projection| {
                let locks = FileLocks::default();
                let file = projection.root_identity;
                let running = AtomicUsize::new(0);
                let most = AtomicUsize::new(0);
                std::thread::scope(|scope| {
                    for _ in 0..8 {
                        scope.spawn(|| {
                            for _ in 0..50 {
                                locks.with(&file, projection, || {
                                    let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                                    most.fetch_max(now, Ordering::SeqCst);
                                    std::thread::yield_now();
                                    running.fetch_sub(1, Ordering::SeqCst);
                                    Ok(())
                                }).expect("unbounded synchronized edit");
                            }
                        });
                    }
                });
                assert_eq!(most.load(Ordering::SeqCst), 1);
                assert!(lock_recover(&locks.0).is_empty());
                Ok(())
            });
        result?;
        Ok(())
    }

    #[tokio::test]
    async fn settling_waits_for_every_pending_placeholder() -> Result<(), Box<dyn std::error::Error>> {
        use super::{Projector, WrittenEntry};
        use crate::FileId;
        use crate::native_mount::ViewStamp;
        let source = windows_checkout_source().await?;
        let cancellation = CancellationToken::new();
        let (result, _, _) = measured_source_leaf(source.as_ref(), WorkBudget::UNBOUNDED,
            &cancellation, |projection| {
                let projector = Arc::new(Projector::new());
                let path = MountPath::root().child(b"held".to_vec());
                super::lock_recover(&projector.state).pending.insert(&path, WrittenEntry {
                    file_id: FileId::new(), owned: None, facts: None,
                    basis: Some(ReadBasis { stamp: ViewStamp::current(), binding: 0 }),
                }, projection)?;
                let settled = std::thread::spawn({
                    let projector = Arc::clone(&projector);
                    let meter = projection.work.clone();
                    move || projector.settle_measured(&meter)
                });
                std::thread::sleep(std::time::Duration::from_millis(100));
                assert!(!settled.is_finished(), "settled while a placeholder is pending");
                super::lock_recover(&projector.state).pending.remove(&path, projection)?;
                projector.changed.notify_all();
                settled.join().map_err(|_| super::NativeMountError::Lifecycle("settle panicked"))??;
                Ok(())
            });
        result?;
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
        // A file is written from a lookup, or from the listing that projected its name; either names exactly the listed content.
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

        // The file holds the version the listing named, or was already written again from the new one: never a mix.
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

    /// A directory projected from the source renames like any other (a directory `ProjFS` projected on demand never could), and a file moved with it, never opened before, still reads its content: `ProjFS` asks for it by the path it was written at.
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
        // The rename moved the written file; nothing writes it again, which would take it away from under a reader for a moment.
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
        let mut work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &token).expect("the actual phase admits its one original native state");
        assert!(matches!(
            control.recover_measured(root.path(), true, &mut &work),
            Err(super::NativeMountError::UnsafeRecoveryDestination { reparse_tag: Some(_) }),
        ));
        assert!(control.controls_pending(), "actual probe capability remains owned after refusal");
        assert!(!control.namespace_detach_confirmed(), "no SDK stop was observed");
        control.recover_measured(root.path(), false, &mut &crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &token).expect("the actual phase admits its one original native state")).expect("preserving recovery corroborates root without deleting authored residue");
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









}
