//! One native callback adapter for every embedded checkout consumer.

use super::view_gate::{CallbackOwner, ViewGate, ViewReadLease, ViewWriteLease};
use super::view_ledger::{ViewChange, ViewEffect, ViewLedger, ViewStamp};
use super::{
    CaptureOptions, MountAttributePage, MountAttributeWriteMode, MountDirectoryEntry,
    MountDirectoryPage, MountFilesystem, MountLookup, MountNode, MountNodeKind, MountOpenFile,
    MountPath, MountPublication, MountRangeAllocation, MountSparseRange, MountSeekTarget, MountSourceError,
    MountViewLease, NativeMountError, ViewObserver, ViewOrigin, capture_root_identity,
    seal_checkout,
};
use crate::heap_future::in_heap;
use crate::native_host::NativeHostWork as _;
use crate::kernel::{
    AttributeClass, AttributeName, ExtentSeekTarget, FileKind, FileMetadata, FilePayload,
    FileRecord, GenerationMutationError, LogicalName, MetadataField, NameEncoding, NamespacePath, RebaseDecision,
};
use crate::model::{CaseSensitivity, FilesystemProfile, VolumeConfig, VolumeLimits};
use crate::native_capture::{
    MAX_NATIVE_EXACT_CAPTURE_PATHS, NativeViewBaseline, capture_paths_batched_with_native_work,
    capture_subtrees_with_native_work,
};
#[cfg(test)]
use crate::native_capture::capture_paths_batched_with_baseline;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, AuthoredMutation, ByteRange, CancellationToken,
    Checkout, ContentChange, ContentTimes, DetachedFile, FileId, FsError, GroupedChange,
    GroupedOutcome, NamedAttributeWriteMode, NativeRootIdentity, ObjectId, OperationFailure,
    OperationId, VolumeId, WorkBudget, WorkError,
};
use bytes::Bytes;
use acyclic_stream::OwnedBytes;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::OnceLock;
use std::sync::PoisonError;
use std::sync::Weak;
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

const CALLBACK_TIMEOUT: Duration = Duration::from_secs(30);
/// Diff bound for recording an installed candidate exactly; a larger effect
/// is recorded as unenumerated.
const MAXIMUM_EXACT_INSTALL_CHANGES: u32 = 65_536;
const MAX_NATIVE_SUBTREE_CAPTURE_PATHS: u32 = 4_000_000;
const CAPTURE_PENDING: u8 = 0;
const CAPTURE_COMMITTED: u8 = 1;
const CAPTURE_CANCELLED: u8 = 2;

#[derive(Clone, Copy)]
pub(super) enum HostCaptureScope {
    ExactPaths,
    Subtree,
}

// A lifecycle capture runs host traversal off the executor. Dropping its
// caller cancels the scan unless the atomic commit decision already won.
struct CaptureCommitGate {
    cancellation: CancellationToken,
    state: AtomicU8,
    slot: std::sync::atomic::AtomicUsize,
    _allocation: CaptureOwnedAllocation,
}

struct CancelCaptureOnDrop(Arc<CaptureCommitGate>);

struct CaptureSourceSlot {
    request: Weak<CaptureCommitGate>,
    cancel_visit: Option<crate::materializer::NativeItemReservation>,
}

#[derive(Default)]
pub(super) struct CaptureSourceGate {
    cancelled: bool,
    producers: u64,
    producers_pending: bool,
    active: Vec<CaptureSourceSlot>,
    allocation: Option<CaptureOwnedAllocation>,
    native_allocation: Option<crate::NativeAllocation>,
    cancel_next: usize,
    retiring: bool,
    retired: bool,
    namespace_bound: bool,
    ever_used: bool,
}

impl CaptureSourceGate {
    fn fresh_for_namespace(&self) -> bool {
        !self.namespace_bound && !self.ever_used && !self.cancelled && !self.retired && !self.retiring
            && self.producers == 0 && self.active.is_empty()
    }
}

#[cfg(windows)]
pub(super) fn namespace_gate_is_fresh(lineage: &super::NativeMountSourceLineage<'_>) -> bool {
    lineage.capture_gate.lock().unwrap_or_else(PoisonError::into_inner).fresh_for_namespace()
        && !lineage.capture_cancellation.is_cancelled()
}

/// Physical custody of an accepted native Source job, not IO authorization.
/// Clones share one already-admitted producer until the last physical owner.
#[derive(Clone)]
pub struct NativeSourceIoLease {
    inner: Arc<NativeSourceIoLeaseInner>,
}

#[derive(Clone, Copy)]
pub(super) enum NativeSourceIoBinding { Ordinary, Native }

pub(super) fn validate_native_source_io_binding(
    payer: &crate::NativeMaterializationWork,
    binding: Option<&crate::NativeMaterializationWork>,
) -> Result<NativeSourceIoBinding, MountSourceError> {
    match binding {
        Some(binding) if payer.shares_ledger(binding) && payer.shares_resource_allowance(binding) =>
            Ok(NativeSourceIoBinding::Native),
        Some(_) => Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::InvalidLayout)),
        None => Ok(NativeSourceIoBinding::Ordinary),
    }
}

impl NativeSourceIoLease {
    pub(crate) fn original_work(&self) -> Option<&crate::NativeMaterializationWork> {
        match self.inner.binding {
            NativeSourceIoBinding::Ordinary => None,
            NativeSourceIoBinding::Native => Some(self.inner._allocation.work()),
        }
    }
}

impl std::fmt::Debug for NativeSourceIoLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("NativeSourceIoLease").finish_non_exhaustive()
    }
}

pub(super) trait NativeSourceIoRetirement: Send + Sync {
    fn release_native_source_io(&self);
}

struct NativeSourceIoLeaseInner {
    owner: Arc<dyn NativeSourceIoRetirement>,
    binding: NativeSourceIoBinding,
    _allocation: crate::NativeAllocation,
}

impl Drop for NativeSourceIoLeaseInner {
    fn drop(&mut self) { self.owner.release_native_source_io(); }
}

impl<A, O> NativeSourceIoRetirement for CheckoutMountSource<A, O>
where Self: Send + Sync {
    fn release_native_source_io(&self) {
        drop(CaptureProducerLease { gate: &self.capture_gate, quiesced: &self.capture_quiesced });
    }
}

/// Stack custody of a real direct producer, including request-less driver calls.
/// This constant gate bookkeeping owns no allocation or namespace item.
struct CaptureProducerLease<'a> {
    gate: &'a StdMutex<CaptureSourceGate>,
    quiesced: &'a std::sync::Condvar,
}

impl Drop for CaptureProducerLease<'_> {
    fn drop(&mut self) {
        let mut gate = self.gate.lock().unwrap_or_else(PoisonError::into_inner);
        gate.producers = gate.producers.checked_sub(1).expect("original capture producer custody");
        if gate.producers == 0 { gate.producers_pending = false; }
        if gate.producers == 0 && (gate.cancelled || gate.active.is_empty()) {
            self.quiesced.notify_all();
        }
    }
}

struct HostCapturePolicy<'a> {
    baseline: Option<&'a NativeViewBaseline>,
    request: Option<&'a CaptureCommitGate>,
    meter: &'a Arc<crate::demand::NativeDemandReadMeter>,
    native: &'a super::NativeMountMeterWork,
}

impl Drop for CancelCaptureOnDrop {
    fn drop(&mut self) {
        if self
            .0
            .state
            .compare_exchange(
                CAPTURE_PENDING,
                CAPTURE_CANCELLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.0.cancellation.cancel();
        }
    }
}

struct CaptureOwnedAllocation {
    meter: Arc<crate::demand::NativeDemandReadMeter>,
    bytes: u64,
}

impl Drop for CaptureOwnedAllocation {
    fn drop(&mut self) {
        if self.bytes != 0 {
            self.meter.lock().release(self.bytes).expect("capture ownership balances");
        }
    }
}

/// Releases only this scope's inputs after their physical owners unwind.
/// Other roots may retire old backing concurrently, so total-ledger deltas
/// cannot identify the ownership of partially constructed native inputs.
pub(super) struct CaptureScratchAllocation<'a> {
    work: &'a mut crate::demand::DemandWork,
    bytes: u64,
}

impl<'a> CaptureScratchAllocation<'a> {
    pub(super) fn new(work: &'a mut crate::demand::DemandWork) -> Self { Self { work, bytes: 0 } }

    fn track(&mut self, bytes: u64) {
        self.bytes = self.bytes.checked_add(bytes)
            .expect("scratch ownership fits the original allocation ledger");
    }

    fn vec<T>(&mut self, count: usize) -> Result<Vec<T>, MountSourceError> {
        use super::lazy::mount_demand_error;
        let requested = count.checked_mul(std::mem::size_of::<T>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let vector = super::lazy::mount_vec::<T>(count, self.work)?;
        self.track(requested);
        let actual = vector.capacity().checked_mul(std::mem::size_of::<T>())
            .and_then(|bytes| u64::try_from(bytes).ok());
        let Some(extra) = actual.and_then(|bytes| bytes.checked_sub(requested)) else {
            drop(vector);
            return Err(MountSourceError::Work(crate::WorkError::Overflow));
        };
        if extra != 0 {
            let retained = self.work.record_capture_adoption(extra);
            self.track(extra);
            if let Err(error) = retained {
                drop(vector);
                return Err(mount_demand_error(error));
            }
        }
        Ok(vector)
    }

    fn release(&mut self, bytes: u64) -> Result<(), crate::DemandError> {
        self.work.release(bytes)?;
        self.bytes = self.bytes.checked_sub(bytes).expect("scratch releases its own input");
        Ok(())
    }

    pub(super) fn bytes(&self) -> u64 { self.bytes }

    pub(super) fn retain(mut self) -> u64 { std::mem::take(&mut self.bytes) }
}

impl Drop for CaptureScratchAllocation<'_> {
    fn drop(&mut self) {
        self.work.release(self.bytes).expect("capture input scratch ownership balances");
    }
}

impl crate::native_host::NativeHostWork for CaptureScratchAllocation<'_> {
    type Error = crate::DemandError;
    type Allocation = u64;
    type CleanupWrite = <crate::demand::DemandWork as crate::native_host::NativeHostWork>::CleanupWrite;

    fn io_error_kind(error: &Self::Error) -> Option<std::io::ErrorKind> {
        <crate::demand::DemandWork as crate::native_host::NativeHostWork>::io_error_kind(error)
    }
    fn allocation_failed(error: std::collections::TryReserveError) -> Self::Error {
        crate::DemandError::Allocation(error)
    }
    fn check(&self) -> Result<(), Self::Error> { self.work.check() }
    fn admit(&mut self, delta: crate::WorkCounters) -> Result<(), Self::Error> {
        crate::native_host::NativeHostWork::admit(self.work, delta)
    }
    fn record(&mut self, delta: crate::WorkCounters) -> Result<(), Self::Error> {
        self.work.record(delta)
    }
    fn started_io(&mut self, delta: crate::WorkCounters) -> Result<(), Self::Error> {
        crate::native_host::NativeHostWork::started_io(self.work, delta)
    }
    fn claim(&mut self, bytes: u64) -> Result<u64, Self::Error> {
        self.work.allocate(bytes)?;
        self.track(bytes);
        Ok(bytes)
    }
    fn reserve_opaque_decode(&mut self, bytes: u64) -> Result<u64, Self::Error> {
        self.work.adopt(bytes)?;
        self.track(bytes);
        Ok(bytes)
    }
    fn reconcile_allocation(&mut self, allocation: &mut u64, actual: u64) -> Result<(), Self::Error> {
        let before = *allocation;
        let result = self.work.reconcile_native_allocation(allocation, actual);
        self.bytes = self.bytes.checked_sub(before).and_then(|bytes| bytes.checked_add(*allocation))
            .expect("reconciled capture scratch ownership balances");
        result
    }
    fn release(&mut self, bytes: u64) -> Result<(), Self::Error> { Self::release(self, bytes) }
    fn reserve_cleanup_write(&mut self) -> Result<Self::CleanupWrite, Self::Error> {
        crate::native_host::NativeHostWork::reserve_cleanup_write(self.work)
    }
    fn cancel_cleanup_write(&mut self, reservation: Self::CleanupWrite) {
        crate::native_host::NativeHostWork::cancel_cleanup_write(self.work, reservation);
    }
    fn attempt_cleanup_write<T>(
        &mut self, reservation: Self::CleanupWrite, operation: impl FnOnce() -> T,
    ) -> (T, Result<(), Self::Error>) {
        crate::native_host::NativeHostWork::attempt_cleanup_write(self.work, reservation, operation)
    }
}

/// One static accounting seam for ordinary and full-resource input producers.
pub(super) trait CaptureInputScratch {
    fn vec<T>(&mut self, count: usize) -> Result<Vec<T>, MountSourceError>;
    fn admit(&mut self, delta: crate::WorkCounters) -> Result<(), MountSourceError>;
    fn record(&mut self, delta: crate::WorkCounters) -> Result<(), MountSourceError>;
    fn charge(&mut self, delta: crate::WorkCounters) -> Result<(), MountSourceError>;
    fn release(&mut self, bytes: u64) -> Result<(), MountSourceError>;
    fn clone_path(&mut self, path: &Path) -> Result<std::path::PathBuf, MountSourceError>;

    fn unsupported(&self, message: &'static str) -> MountSourceError {
        MountSourceError::Unsupported(message.to_owned())
    }

    fn invalid(&self, message: &'static str) -> MountSourceError {
        MountSourceError::Invalid(message.to_owned())
    }

    fn engine_error<E: std::fmt::Display + Into<crate::FsError>>(&self, error: E) -> MountSourceError {
        engine_error(error)
    }

    fn items(&mut self, count: usize) -> Result<(), MountSourceError> {
        self.charge(crate::WorkCounters { items_examined: count as u64, ..crate::WorkCounters::UNCHARGED })
    }
}

impl CaptureInputScratch for CaptureScratchAllocation<'_> {
    fn vec<T>(&mut self, count: usize) -> Result<Vec<T>, MountSourceError> { Self::vec(self, count) }
    fn admit(&mut self, delta: crate::WorkCounters) -> Result<(), MountSourceError> {
        crate::native_host::NativeHostWork::admit(self.work, delta).map_err(super::lazy::mount_demand_error)
    }
    fn record(&mut self, delta: crate::WorkCounters) -> Result<(), MountSourceError> {
        self.work.record(delta).map_err(super::lazy::mount_demand_error)
    }
    fn charge(&mut self, delta: crate::WorkCounters) -> Result<(), MountSourceError> {
        self.work.charge(delta).map_err(super::lazy::mount_demand_error)
    }
    fn release(&mut self, bytes: u64) -> Result<(), MountSourceError> {
        Self::release(self, bytes).map_err(super::lazy::mount_demand_error)
    }
    fn clone_path(&mut self, path: &Path) -> Result<std::path::PathBuf, MountSourceError> {
        crate::native_host::clone_path_with_work(path, self).map(|(path, _)| path)
            .map_err(super::lazy::mount_demand_error)
    }
}

pub(super) struct NativeCaptureScratchAllocation<'a> {
    work: &'a crate::NativeMaterializationWork,
    allocation: crate::NativeAllocation,
    copy: Option<crate::materializer::NativeCopyReservation>,
    items: Option<crate::materializer::NativeItemReservation>,
}

impl<'a> NativeCaptureScratchAllocation<'a> {
    pub(super) fn new(work: &'a crate::NativeMaterializationWork) -> Result<Self, MountSourceError> {
        Ok(Self {
            work, allocation: work.claim_allocation(0, 0).map_err(MountSourceError::NativeWork)?,
            copy: None, items: None,
        })
    }

    pub(super) fn absorb(&mut self, mut allocation: crate::NativeAllocation) {
        self.allocation.absorb(&mut allocation).expect("native input guards share exact original and resource custody");
    }

    pub(super) fn retain(self) -> crate::NativeAllocation { self.allocation }
}

impl CaptureInputScratch for NativeCaptureScratchAllocation<'_> {
    fn vec<T>(&mut self, count: usize) -> Result<Vec<T>, MountSourceError> {
        let requested = count.checked_mul(std::mem::size_of::<T>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        self.absorb(self.work.claim_allocation(requested, u64::from(requested != 0))
            .map_err(MountSourceError::NativeWork)?);
        let mut vector = Vec::new();
        vector.try_reserve_exact(count).map_err(|error|
            MountSourceError::NativeWork(crate::NativeTreeMaterializationError::AllocationFailed(error)))?;
        let actual = vector.capacity().checked_mul(std::mem::size_of::<T>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let extra = actual.checked_sub(requested).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        if extra != 0 {
            let (allocation, recorded) = self.work.record_completed_allocation(extra, None);
            self.absorb(allocation);
            recorded.map_err(MountSourceError::NativeWork)?;
        }
        Ok(vector)
    }
    fn admit(&mut self, mut delta: crate::WorkCounters) -> Result<(), MountSourceError> {
        let copied = std::mem::take(&mut delta.bytes_copied);
        let examined = std::mem::take(&mut delta.items_examined);
        self.work.admit(delta).map_err(MountSourceError::NativeWork)?;
        let copy = if copied == 0 { None } else {
            Some(self.work.reserve_copy(copied).map_err(MountSourceError::NativeWork)?)
        };
        let items = if examined == 0 { None } else {
            Some(self.work.reserve_items(examined).map_err(MountSourceError::NativeWork)?)
        };
        self.copy = copy;
        self.items = items;
        Ok(())
    }
    fn record(&mut self, mut delta: crate::WorkCounters) -> Result<(), MountSourceError> {
        let copied = if let Some(copy) = self.copy.take() {
            copy.record(std::mem::take(&mut delta.bytes_copied))
        } else { Ok(()) };
        let examined = if let Some(mut items) = self.items.take() {
            items.consume(std::mem::take(&mut delta.items_examined))
        } else { Ok(()) };
        // Every completed dimension remains truthful even if another one has
        // already reported cancellation or a late refusal.
        let remaining = self.work.record_admitted(delta);
        copied.and(examined).and(remaining).map_err(MountSourceError::NativeWork)
    }
    fn charge(&mut self, delta: crate::WorkCounters) -> Result<(), MountSourceError> {
        self.work.charge(delta).map_err(MountSourceError::NativeWork)
    }
    fn unsupported(&self, message: &'static str) -> MountSourceError {
        MountSourceError::NativeStaticText { kind: super::NativeSourceTextKind::Unsupported, text: message }
    }
    fn invalid(&self, message: &'static str) -> MountSourceError {
        MountSourceError::NativeStaticText { kind: super::NativeSourceTextKind::Invalid, text: message }
    }
    fn engine_error<E: std::fmt::Display + Into<crate::FsError>>(&self, error: E) -> MountSourceError {
        MountSourceError::Filesystem(error.into())
    }
    fn release(&mut self, bytes: u64) -> Result<(), MountSourceError> {
        drop(self.allocation.split_off(bytes).map_err(MountSourceError::NativeWork)?);
        Ok(())
    }
    fn clone_path(&mut self, path: &Path) -> Result<std::path::PathBuf, MountSourceError> {
        let allocation;
        let cloned;
        (cloned, allocation) = crate::native_host::clone_path_with_work(path, &mut self.work)
            .map_err(MountSourceError::NativeWork)?;
        self.absorb(allocation);
        Ok(cloned)
    }
}

struct CaptureMeterOwned<T> {
    value: Option<T>,
    _allocation: CaptureOwnedAllocation,
}

impl<T> Deref for CaptureMeterOwned<T> {
    type Target = T;
    fn deref(&self) -> &T { self.value.as_ref().expect("measured callback value is live") }
}

impl<T> DerefMut for CaptureMeterOwned<T> {
    fn deref_mut(&mut self) -> &mut T { self.value.as_mut().expect("measured callback value is live") }
}

impl<T> Drop for CaptureMeterOwned<T> {
    fn drop(&mut self) {
        drop(self.value.take());
    }
}

/// Owns one canonical observer and its original invocation's proof attribution.
/// Shared and directly borrowed callers use the same custody/receipt lifecycle;
/// neither a shared-map snapshot nor a strong map hold can attribute other work.
pub(super) struct LookupObserverOwned<'a, 'w, A, O> {
    value: Option<crate::facade::CheckoutObserver<'a, A, O>>,
    work: NativeDemandCarrier<'w>,
    bytes: u64,
    finished: bool,
}

/// One access seam to the same operative native invocation. Shared access
/// locks only the synchronous accounting/proof step, never the awaited SDK IO.
pub(super) enum NativeDemandCarrier<'w> {
    Borrowed(&'w mut crate::demand::DemandWork),
    Shared(&'w Arc<crate::demand::NativeDemandReadMeter>),
    Native(&'w super::NativeMountMeterWork),
}

impl NativeDemandCarrier<'_> {
    pub(super) fn with_work<T>(
        &mut self,
        operation: impl FnOnce(&mut crate::demand::DemandWork) -> T,
    ) -> T {
        match self {
            Self::Borrowed(work) => operation(work),
            Self::Shared(meter) => operation(&mut meter.lock()),
            Self::Native(native) => native.native_materialization_work().with_original_leaf(operation),
        }
    }

    pub(super) fn cancellation(&mut self) -> crate::CancellationToken {
        match self {
            Self::Native(native) => native.native_materialization_work().cancellation().clone(),
            Self::Borrowed(work) => work.cancellation().clone(),
            Self::Shared(meter) => meter.with_original_leaf(|work| work.cancellation().clone()),
        }
    }

    pub(super) fn new_custody(&mut self) -> Result<super::NativeMountAllocationCustody, MountSourceError> {
        if let Some(original) = self.original_native_work() {
            super::NativeMountAllocationCustody::new_on_original(&original.original_demand_work())
                .map_err(super::lazy::mount_demand_error)
        } else {
            self.with_work(super::NativeMountAllocationCustody::new).map_err(super::lazy::mount_demand_error)
        }
    }

    /// Projects only an actual complete native capability, including its
    /// original resource allowance; a raw meter cannot recreate that authority.
    pub(super) fn original_native_work(&self) -> Option<crate::NativeMaterializationWork> {
        match self {
            Self::Native(native) => Some(native.native_materialization_work()),
            Self::Borrowed(_) | Self::Shared(_) => None,
        }
    }

    pub(super) fn empty_source_chunk(&self) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        let Some(original) = self.original_native_work() else {
            return Ok(crate::kernel::OwnedSourceChunk::claimed(OwnedBytes::new(), 0));
        };
        let custody = original.claim(0).map_err(MountSourceError::NativeWork)?;
        let bytes = OwnedBytes::from_vec_with_guard(
            Vec::new(), custody, original.runtime_meter_owner(),
        ).map_err(|failure| match failure.into_error() {
            acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                MountSourceError::NativeWork(original.runtime_error(error))
            }
            acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized
            | acyclic_stream::PreparedOwnedBytesError::NotUnique => {
                MountSourceError::NativeWork(crate::NativeTreeMaterializationError::InvalidLayout)
            }
        })?;
        Ok(crate::kernel::OwnedSourceChunk::intrinsic(bytes, 0))
    }
}

impl NativeDemandCarrier<'_> {
    pub(super) fn reborrow(&mut self) -> NativeDemandCarrier<'_> {
        match self {
            Self::Borrowed(work) => NativeDemandCarrier::Borrowed(work),
            Self::Shared(meter) => NativeDemandCarrier::Shared(meter),
            Self::Native(native) => NativeDemandCarrier::Native(native),
        }
    }
}

impl NativeDemandCarrier<'_> {
    pub(super) fn accept_with_prior_live<T, E>(
        &mut self,
        result: crate::MeasuredResult<crate::OperationReceipt<T>, E>,
        prior_live: u64,
        map: impl FnOnce(E) -> MountSourceError,
    ) -> Result<T, MountSourceError> {
        self.with_work(|work| match result {
            Ok(receipt) => {
                work.merge_with_prior_live(receipt.work, prior_live)
                    .map_err(super::lazy::mount_demand_error)?;
                Ok(receipt.value)
            }
            Err(failure) => {
                work.merge_with_prior_live(*failure.work, prior_live)
                    .map_err(super::lazy::mount_demand_error)?;
                Err(map(failure.error))
            }
        })
    }
}

fn record_view_change_with_carrier(
    ledger: &Arc<ViewLedger>,
    change: &ViewChange<'_>,
    carrier: &mut NativeDemandCarrier<'_>,
) -> Result<(), MountSourceError> {
    if let Some(original) = carrier.original_native_work() {
        // Native map backing carries its real intrinsic allocation guard.
        // Borrowed service maps are not imported into this operation.
        return ledger.record(change, &mut NativeDemandCarrier::Native(&super::NativeMountMeterWork::from_native_materialization_work(&original)), &mut super::view_ledger::ViewLedgerAdmission::default());
    }
    let (slot, mut admission) = carrier.with_work(|work| ledger.take_for_work(work))?;
    let recorded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ledger.record(change, carrier, &mut admission)
    }));
    carrier.with_work(|work| work.restore_view_ledger_admission(slot, admission));
    match recorded {
        Err(panic) => std::panic::resume_unwind(panic),
        Ok(result) => result,
    }
}

async fn native_node_if_bound_with_carrier<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &Checkout<A, O>,
    path: &NamespacePath,
    carrier: &mut NativeDemandCarrier<'_>,
    cancellation: &CancellationToken,
) -> Result<Option<FileId>, MountSourceError> {
    use super::lazy::{mount_demand_error, mount_engine_error};
    let original_native = carrier.original_native_work();
    let inspector = if let Some(original) = original_native.as_ref() {
        checkout.inspector_with_native_work(original, cancellation).map_err(MountSourceError::Filesystem)?
    } else {
        let (budget, prior_live) = carrier.with_work(|work| {
            Ok::<_, MountSourceError>((
                work.remaining().map_err(mount_demand_error)?, work.live_allocation_bytes(),
            ))
        })?;
        let inspected = checkout.inspector_measured(budget, cancellation);
        carrier.accept_with_prior_live(inspected, prior_live, mount_engine_error)?
    };
    let mut inspector = LookupObserverOwned::new_in(inspector, carrier.reborrow())?;
    let accepted = if let Some(original) = original_native.as_ref() {
        inspector.lookup_no_follow_with_native_work(
            path, original, cancellation,
        ).await.map_err(MountSourceError::Filesystem)
    } else {
        let (budget, prior_live) = inspector.work.with_work(|work| {
            Ok::<_, MountSourceError>((
                work.remaining().map_err(mount_demand_error)?, work.live_allocation_bytes(),
            ))
        })?;
        let lookup = inspector.lookup_no_follow(path, budget, cancellation).await;
        let accepted = inspector.work.accept_with_prior_live(lookup, prior_live, mount_engine_error);
        inspector.receipt_delivered();
        accepted
    };
    let finished = inspector.finish();
    let value = accepted?;
    finished?;
    inspector.work.with_work(|work| {
        work.check().map_err(mount_demand_error)?;
        if value.record.is_some() {
            work.charge(crate::WorkCounters {
                bytes_copied: std::mem::size_of::<FileId>() as u64,
                ..crate::WorkCounters::UNCHARGED
            }).map_err(mount_demand_error)?;
        }
        Ok(value.record.map(|record| record.file_id))
    })
}
type SourcePublicationFuture = futures::future::Either<
    std::future::Ready<Result<(), MountSourceError>>,
    futures::future::Map<
        super::NativeMountCallbackFuture<'static>,
        fn(Result<(), super::NativeMountError>) -> Result<(), MountSourceError>,
    >,
>;

fn source_publication_result(
    result: Result<(), super::NativeMountError>,
) -> Result<(), MountSourceError> {
    result.map_err(|error| match error {
        super::NativeMountError::CallbackDenied { source, custody } =>
            MountSourceError::NativeCallbackDenied { source, custody },
        super::NativeMountError::Materialization(error) => MountSourceError::NativeWork(error),
        // The constructor admits only through NativeMaterializationWork, and
        // new_native_guarded funds every other asynchronous cause before IO.
        _ => unreachable!("native guarded callback constructor or funded denial"),
    })
}

/// The original source's actual guard, not a substitute filesystem authority.
/// An unguarded SDK source checks its own lifetime and invocation only.
pub(super) fn source_publication_guard(
    guard: Option<Arc<dyn super::NativeMountCallbackGuard>>,
    lifetime: Option<CancellationToken>,
) -> impl for<'m> FnMut(&'m crate::NativeMaterializationWork) -> SourcePublicationFuture + Clone + Send + 'static {
    move |original: &crate::NativeMaterializationWork| {
        use futures::FutureExt;
        let policy = || {
            if lifetime.as_ref().is_some_and(CancellationToken::is_cancelled) {
                return Err(MountSourceError::Stale);
            }
            original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)
        };
        if let Err(error) = policy() {
            return futures::future::Either::Left(std::future::ready(Err(error)));
        }
        let Some(guard) = guard.as_ref().map(Arc::clone) else {
            return futures::future::Either::Left(std::future::ready(Ok(())));
        };
        let mounted = super::NativeMountMeterWork::from_native_materialization_work(original);
        let owned_work = mounted.clone();
        let lifetime = lifetime.clone();
        match super::NativeMountCallbackFuture::new_native_guarded(&mounted, move || async move {
            if lifetime.as_ref().is_some_and(CancellationToken::is_cancelled) {
                return Err(super::NativeMountError::Source(MountSourceError::Stale));
            }
            guard.check_write(&owned_work)?.await?;
            if lifetime.as_ref().is_some_and(CancellationToken::is_cancelled) {
                return Err(super::NativeMountError::Source(MountSourceError::Stale));
            }
            owned_work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)?;
            Ok(())
        }) {
            Ok(future) => futures::future::Either::Right(future.map(
                source_publication_result as fn(_) -> _,
            )),
            Err(error) => futures::future::Either::Left(std::future::ready(
                source_publication_result(Err(error)),
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NativeSourcePublicationAck {
    Unchanged { generation: crate::GenerationId },
    Published { operation: OperationId, generation: crate::GenerationId },
}

#[derive(Clone)]
struct NativeSourcePublicationEvidence {
    operation_evidence: acyclic_stream::OwnedBytes,
    operation_context: crate::Digest,
    context: crate::lazy_workspace::NativeSnapshotContext,
    original_permit: crate::PublicationPermit,
}

enum NativeSealError<E> {
    Filesystem(crate::FsError),
    Guard(E),
}

impl<E> From<crate::FsError> for NativeSealError<E> {
    fn from(error: crate::FsError) -> Self { Self::Filesystem(error) }
}

impl<E: From<MountSourceError>> From<MountSourceError> for NativeSealError<E> {
    fn from(error: MountSourceError) -> Self { Self::Guard(E::from(error)) }
}

/// A producer's actual owned input, released only after its physical drop.
struct NativeCarrierOwned<'a, 'w, T> {
    value: Option<T>,
    carrier: &'a mut NativeDemandCarrier<'w>,
    bytes: u64,
}

impl<'w, T> NativeCarrierOwned<'_, 'w, T> {
    fn parts(&mut self) -> (&T, &mut NativeDemandCarrier<'w>) {
        (self.value.as_ref().expect("native input is live"), self.carrier)
    }
}

impl<T> Drop for NativeCarrierOwned<'_, '_, T> {
    fn drop(&mut self) {
        drop(self.value.take());
        self.carrier.with_work(|work| {
            work.release(self.bytes).expect("native input ownership balances");
        });
    }
}

impl<'a, 'w, A: AsyncAuthorityStore, O: AsyncObjectStore> LookupObserverOwned<'a, 'w, A, O> {
    pub(super) fn new(
        observer: crate::facade::CheckoutObserver<'a, A, O>,
        meter: &'w Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<Self, MountSourceError> {
        Self::new_in(observer, NativeDemandCarrier::Shared(meter))
    }
}

impl<'a, 'w, A: AsyncAuthorityStore, O: AsyncObjectStore> LookupObserverOwned<'a, 'w, A, O> {
    fn new_in(
        observer: crate::facade::CheckoutObserver<'a, A, O>,
        work: NativeDemandCarrier<'w>,
    ) -> Result<Self, MountSourceError> {
        use super::lazy::mount_demand_error;
        if matches!(&work, NativeDemandCarrier::Native(_)) {
            // The native constructor's intrinsic guards already own every
            // paid root and control claim in this same original ledger.
            return Ok(Self { value: Some(observer), work, bytes: 0, finished: false });
        }
        let bytes = observer.logical_owned_bytes();
        let custody = observer.proof_data_custody();
        let custody_bytes = custody.logical_owned_bytes();
        let mut owned = Self { value: Some(observer), work, bytes: 0, finished: false };
        let retained_bytes = &mut owned.bytes;
        owned.work.with_work(|work| {
            let adopted = work.record_adoption(bytes).map_err(mount_demand_error);
            *retained_bytes = bytes;
            adopted?;
            work.retain_observer_custody(custody).map_err(mount_demand_error)?;
            // The actual custody payload remains in the original carrier.
            *retained_bytes = bytes.checked_sub(custody_bytes)
                .expect("observer ownership includes its custody payload");
            Ok::<(), MountSourceError>(())
        })?;
        Ok(owned)
    }
}

impl<'a, 'w, A, O> LookupObserverOwned<'a, 'w, A, O> {
    /// Called after merging each SDK receipt, even when that merge returns a
    /// typed failure. Only the delivery cache drains; original counters never
    /// reset, and actually adopted DATA becomes standing data for the next query.
    pub(super) fn receipt_delivered(&mut self) {
        self.proof_data_take_completed_work();
    }

    /// Merges only undelivered proof work before reconciling new DATA. Shared
    /// callers must invoke this outside their work mutex.
    /// Already-applied native work retires only its SDK delivery cache.
    pub(super) fn finish(&mut self) -> Result<(), MountSourceError> {
        if self.finished { return Ok(()); }
        if matches!(&self.work, NativeDemandCarrier::Native(_)) {
            self.finished = true;
            self.proof_data_take_completed_work();
            return Ok(());
        }
        let pending = self.proof_data_pending_work();
        self.finished = true;
        let result = self.work.with_work(|work| {
            work.merge(pending).map_err(super::lazy::mount_demand_error)
        });
        self.proof_data_take_completed_work();
        result
    }

}

impl<'a, A, O> Deref for LookupObserverOwned<'a, '_, A, O> {
    type Target = crate::facade::CheckoutObserver<'a, A, O>;
    fn deref(&self) -> &Self::Target { self.value.as_ref().expect("measured observer is live") }
}

impl<A, O> DerefMut for LookupObserverOwned<'_, '_, A, O> {
    fn deref_mut(&mut self) -> &mut Self::Target { self.value.as_mut().expect("measured observer is live") }
}

impl<A, O> Drop for LookupObserverOwned<'_, '_, A, O> {
    fn drop(&mut self) {
        let _ = self.finish();
        drop(self.value.take());
        self.work.with_work(|work| {
            work.release(self.bytes).expect("measured observer roots ownership balances");
        });
    }
}

fn retire_capture_vector(
    vector: Vec<CaptureSourceSlot>,
    allocation: Option<CaptureOwnedAllocation>,
    native_allocation: Option<crate::NativeAllocation>,
) {
    drop(vector);
    drop(allocation);
    drop(native_allocation);
}

/// Moves the complete caller invocation into a native callback and restores it
/// only after the callback's owned inputs, futures, and accepted I/O retire.
/// The borrowed shell is never used as an operative meter.
pub(super) struct BorrowedNativeDemandMeter<'a> {
    original: &'a mut crate::demand::DemandWork,
    meter: Option<Arc<crate::demand::NativeDemandReadMeter>>,
}

impl<'a> BorrowedNativeDemandMeter<'a> {
    pub(super) fn new(original: &'a mut crate::demand::DemandWork) -> Result<Self, MountSourceError> {
        let mut admission = CaptureScratchAllocation::new(original);
        let bytes = std::mem::size_of::<crate::demand::NativeDemandReadMeter>() as u64;
        admission.work.allocate(bytes).map_err(super::lazy::mount_demand_error)?;
        admission.track(bytes);
        // Allocate while the complete original remains in its caller slot.
        // An allocator/constructor unwind can therefore release this admission
        // without ever losing the original invocation to a dropped argument.
        let mut meter = Arc::new(crate::demand::NativeDemandReadMeter(StdMutex::new(
            admission.work.native_queue_shell(),
        )));
        let target = Arc::get_mut(&mut meter).expect("new native meter has exclusive custody")
            .0.get_mut().unwrap_or_else(PoisonError::into_inner);
        // Disarm and drop the scratch guard BEFORE leaving a shell: even a
        // zero-byte cleanup must never execute against that placeholder.
        admission.retain();
        *target = original.take_for_native_queue();
        Ok(Self { original, meter: Some(meter) })
    }

    /// Runs a scoped callback whose result retains no meter alias.
    ///
    /// Accepted I/O must use the existing measured completion engine. Scoped
    /// TLS/fiber owners and prepared cancellation wakers are retired before the
    /// callback returns; wakers must be cleared outside the work mutex. A panic
    /// is resumed only after all remaining SDK aliases release and the complete
    /// original invocation has been restored.
    pub(super) fn run<T>(
        mut self,
        operation: impl FnOnce(&Arc<crate::demand::NativeDemandReadMeter>) -> T,
    ) -> T {
        // Preserve the original panic until every accepted native operation and
        // SDK meter alias has retired; restoration must precede resume_unwind.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            operation(self.meter.as_ref().expect("borrowed native meter is live"))
        }));
        self.restore();
        match result {
            Ok(value) => value,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }

    /// Runs an admitted future and restores its original meter after physical retirement.
    pub(super) fn run_async<T, F, E>(
        self,
        create: impl FnOnce(Arc<crate::demand::NativeDemandReadMeter>) -> F,
    ) -> impl Future<Output = Result<T, E>>
    where
        F: Future<Output = Result<T, E>>,
        E: From<MountSourceError>,
    {
        let meter = self.meter.as_ref().expect("borrowed native meter is live");
        let mut allocation = CaptureOwnedAllocation { meter: Arc::clone(meter), bytes: 0 };
        let created = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            crate::heap_future::in_heap_admitted(
                (create, Arc::clone(meter), &mut allocation),
                |(_, _, allocation), bytes| {
                    meter.lock().allocate(bytes).map_err(super::lazy::mount_demand_error)?;
                    allocation.bytes = bytes;
                    Ok::<(), MountSourceError>(())
                },
                |(create, meter, _)| create(meter),
            )
        }));
        // An unpolled bridge drops the future and its allocation before restoring the meter.
        let state = (created, allocation, self);
        async move {
            let (created, allocation, mut owner) = state;
            let (result, drop_panic) = {
                let _allocation = allocation;
                match created {
                    Err(panic) => (Err(panic), None),
                    Ok(Err(error)) => (Ok(Err(E::from(error))), None),
                    Ok(Ok((mut future, _))) => {
                        let result = std::future::poll_fn(|context| {
                            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                future.as_mut().poll(context)
                            })) {
                                Ok(Poll::Pending) => Poll::Pending,
                                Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
                                Err(panic) => Poll::Ready(Err(panic)),
                            }
                        }).await;
                        let destroyed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            drop(future);
                        })).err();
                        (result, destroyed)
                    }
                }
            };
            owner.restore();
            match result {
                Err(panic) => {
                    let _drop_panic = drop_panic;
                    std::panic::resume_unwind(panic)
                }
                Ok(value) => match drop_panic {
                    Some(panic) => std::panic::resume_unwind(panic),
                    None => value,
                },
            }
        }
    }

    fn restore(&mut self) {
        let Some(mut meter) = self.meter.take() else { return; };
        let owned = loop {
            match Arc::try_unwrap(meter) {
                Ok(owned) => break owned,
                Err(remaining) => {
                    // InlineBlocking makes callback host I/O synchronous. An
                    // already accepted SDK completion may nevertheless still
                    // be retiring its last measured ownership on another thread.
                    meter = remaining;
                    std::thread::yield_now();
                }
            }
        };
        let mut recovered = owned.0.into_inner().unwrap_or_else(PoisonError::into_inner);
        let released = recovered.release(std::mem::size_of::<crate::demand::NativeDemandReadMeter>() as u64);
        *self.original = recovered;
        released.expect("borrowed native meter payload ownership balances");
    }
}

impl Drop for BorrowedNativeDemandMeter<'_> {
    fn drop(&mut self) {
        self.restore();
    }
}

struct CaptureInputs {
    paths: Vec<NamespacePath>,
    options: CaptureOptions,
    _allocation: CaptureInputsAllocation,
}

enum CaptureInputsAllocation {
    Ordinary { _allocation: CaptureOwnedAllocation },
    Native { _allocation: crate::NativeAllocation },
}

struct CaptureCallerWait {
    prepared: Option<crate::cancellation::PreparedCancelled>,
    error: Option<MountSourceError>,
    _allocation: crate::NativeAllocation,
}

impl CaptureCallerWait {
    fn prepare_capture(
        work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(Arc<StdMutex<Self>>, CaptureRegistrationRetirement), MountSourceError> {
        let prepared = cancellation.prepare_cancelled_with_native_work(work)
            .map_err(super::lazy::mount_wait_error)?;
        let retirement = CaptureRegistrationRetirement::new(work)?;
        let allocation = work.claim_allocation(std::mem::size_of::<StdMutex<Self>>() as u64, 1)
            .map_err(MountSourceError::NativeWork)?;
        let wait = Arc::new(StdMutex::new(Self {
            prepared: Some(prepared), error: None, _allocation: allocation,
        }));
        Ok((wait, retirement))
    }

    fn finish(&mut self) {
        let Some(mut prepared) = self.prepared.take() else { return; };
        prepared.retire_waker();
        drop(prepared);
    }
}

impl Drop for CaptureCallerWait {
    fn drop(&mut self) {
        self.finish();
    }
}

struct CaptureRegistrationRetirement {
    items: crate::materializer::NativeItemReservation,
    copied: Option<crate::materializer::NativeCopyReservation>,
}

impl CaptureRegistrationRetirement {
    fn new(work: &crate::NativeMaterializationWork) -> Result<Self, MountSourceError> {
        let items = work.reserve_items(2).map_err(MountSourceError::NativeWork)?;
        let copied = work.reserve_copy(std::mem::size_of::<CaptureSourceSlot>() as u64)
            .map_err(MountSourceError::NativeWork)?;
        Ok(Self { items, copied: Some(copied) })
    }
}

struct CaptureRegistration<A, O> {
    source: Arc<CheckoutMountSource<A, O>>,
    request: Arc<CaptureCommitGate>,
    wait: Arc<StdMutex<CaptureCallerWait>>,
    retirement: Option<CaptureRegistrationRetirement>,
}

impl<A, O> Drop for CaptureRegistration<A, O> {
    fn drop(&mut self) {
        let mut retirement = self.retirement.take().expect("registered capture owns its native retirement");
        let (removed, vector, allocation, native_allocation, copied) = {
            let mut gate = self.source.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
            let slot = self.request.slot.load(Ordering::Acquire);
            if gate.cancelled {
                // Cancellation has frozen registration. Preserve stable indices
                // and the slot's original scan credit until the real drain.
                let removed = std::mem::replace(&mut gate.active[slot].request, Weak::new());
                (Some(CaptureSourceSlot { request: removed, cancel_visit: None }), None, None, None, 0)
            } else {
                let copied = usize::from(slot + 1 != gate.active.len())
                    * std::mem::size_of::<CaptureSourceSlot>();
                let removed = gate.active.swap_remove(slot);
                if let Some(moved) = gate.active.get(slot).and_then(|slot| slot.request.upgrade()) {
                    moved.slot.store(slot, Ordering::Release);
                }
                if gate.active.is_empty() {
                    (Some(removed), Some(std::mem::take(&mut gate.active)),
                        gate.allocation.take(), gate.native_allocation.take(), copied)
                } else {
                    (Some(removed), None, None, None, copied)
                }
            }
        };
        let _retirement = CaptureProducerLease {
            gate: &self.source.capture_gate, quiesced: &self.source.capture_quiesced,
        };
        drop(removed);
        if let Some(vector) = vector { retire_capture_vector(vector, allocation, native_allocation); }
        let items_error = retirement.items.consume(1 + u64::from(copied != 0)).err();
        let copied_error = retirement.copied.take().expect("capture retirement owns its copy credit")
            .record(copied as u64).err();
        // Refund unused visits before the producer lease announces quiescence.
        drop(retirement);
        if let Some(error) = items_error.or(copied_error) {
            let mut wait = self.wait.lock().unwrap_or_else(PoisonError::into_inner);
            if wait.error.is_none() { wait.error = Some(MountSourceError::NativeWork(error)); }
        }
    }
}

struct CaptureWorkerResources<A, O> {
    inputs: Option<CaptureInputs>,
    registration: Option<CaptureRegistration<A, O>>,
    retirement: Option<CaptureRegistrationRetirement>,
    wait: Arc<StdMutex<CaptureCallerWait>>,
}

impl<A, O> Drop for CaptureWorkerResources<A, O> {
    fn drop(&mut self) {
        drop(self.inputs.take());
        self.wait.lock().unwrap_or_else(PoisonError::into_inner).finish();
        drop(self.registration.take());
        drop(self.retirement.take());
    }
}

fn capture_io_error(error: std::io::Error) -> MountSourceError {
    match crate::demand::native_demand_error(error) {
        crate::DemandError::Io(error) => MountSourceError::Engine(error.to_string()),
        error => super::lazy::mount_demand_error(error),
    }
}

/// Blocking native callback adapter over one async canonical checkout.
pub struct CheckoutMountSource<A, O> {
    checkout: Arc<SharedCheckout<A, O>>,
    root: NamespacePath,
    limits: VolumeLimits,
    profile: FilesystemProfile,
    /// Whether the volume resolves every case of a name to one entry.
    folds_names: bool,
    cancellation: CancellationToken,
    capture_gate: StdMutex<CaptureSourceGate>,
    capture_quiesced: std::sync::Condvar,
    runtime: CallbackRuntime,
    native_original: Option<crate::NativeMaterializationWork>,
    native_io_binding: NativeSourceIoBinding,
    native_callback_guard: Option<Arc<dyn super::NativeMountCallbackGuard>>,
    cleanup_credit: StdMutex<Option<crate::materializer::NativeWorkReservation>>,
    cleanup_final_credit: StdMutex<Option<crate::materializer::NativeItemReservation>>,
    // Actual constructor-owned buffers drop before their allocation custody.
    _namespace_allocation: Option<crate::NativeAllocation>,
    _source_allocation: Option<crate::NativeAllocation>,
}

#[cfg(windows)]
pub(super) fn retain_namespace_lineage(
    lineage: &super::NativeMountSourceLineage<'_>, work: &super::NativeMountMeterWork,
    require_fresh: bool,
) -> Result<super::NativeMountNamespaceBinding, MountSourceError> {
    let original = work.native_materialization_work();
    let mut scratch = NativeCaptureScratchAllocation::new(&original)?;
    let mut names = scratch.vec(lineage.root.components().len())?;
    for name in lineage.root.components() {
        scratch.items(1)?;
        names.push(capture_copy_name(
            name.encoding(), name.as_bytes(), lineage.limits, &mut scratch,
        )?);
    }
    let root = NamespacePath::new(names, lineage.limits).map_err(|error|
        MountSourceError::Filesystem(crate::FsError::Path(
            crate::kernel::PathLookupError::NamespacePath(error),
        )))?;
    let lazy_root = match lineage.lazy_root {
        Some(text) => {
            let copied = crate::WorkCounters { bytes_copied: text.len() as u64, ..crate::WorkCounters::UNCHARGED };
            scratch.admit(copied)?;
            let mut bytes = scratch.vec(text.len())?;
            bytes.extend_from_slice(text.as_bytes());
            scratch.record(copied)?;
            Some(String::from_utf8(bytes).expect("canonical lazy root is UTF-8"))
        }
        None => None,
    };
    {
        let mut gate = lineage.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
        if require_fresh && (!gate.fresh_for_namespace() || lineage.capture_cancellation.is_cancelled()) {
            return Err(MountSourceError::Stale);
        }
        // Actual one-way constructor bookkeeping, not a namespace grant.
        gate.namespace_bound = true;
    }
    Ok(super::NativeMountNamespaceBinding {
        owner: lineage.owner.retain(), root,
        native_original: lineage.native_original.cloned(),
        lazy_owner: lineage.lazy_owner.map(super::NativeMountLineageAlias::retain),
        lazy_root, _custody: scratch.retain(),
    })
}

/// Blocking bridge from native driver threads into the shared async runtime.
///
/// A callback polls its future on the calling thread inside the runtime's
/// context, so timers, I/O, and blocking pools resolve against the shared
/// runtime while no helper thread is ever created. The future lives in one
/// heap allocation, so while a callback waits, a driver thread's stack holds
/// only poll frames, whatever the future's size.
#[derive(Clone)]
pub(super) struct CallbackRuntime {
    handle: tokio::runtime::Handle,
}

// Existing-only access must not implicitly initialize a bounded invocation.
static CALLBACK_RUNTIME: OnceLock<Result<tokio::runtime::Runtime, Arc<std::io::Error>>> = OnceLock::new();

/// Wakes one callback without initializing opaque foreign-thread metadata.
/// The guarded flag preserves wakes delivered before the callback waits.
struct ParkedCallback {
    woken: StdMutex<bool>,
    changed: std::sync::Condvar,
    _native_allocation: Option<crate::NativeAllocation>,
}

impl Wake for ParkedCallback {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        *self.woken.lock().unwrap_or_else(PoisonError::into_inner) = true;
        self.changed.notify_one();
    }
}

impl ParkedCallback {
    fn poll_to_completion<F: Future + Unpin>(future: F) -> F::Output {
        Self::poll_to_completion_measured(future, None)
            .expect("unmetered callback has no denied allocations")
    }

    fn poll_to_completion_measured<F: Future + Unpin>(
        future: F, meter: Option<&dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<F::Output> {
        let _allocation = CaptureRuntimeAllocation::new(meter, std::mem::size_of::<Self>())?;
        // A polled capture future may retain this meter's lock. Drop that
        // future before releasing the allocation guard on unwind as well.
        let future = future;
        let parked = Arc::new(Self {
            woken: StdMutex::new(false),
            changed: std::sync::Condvar::new(),
            _native_allocation: None,
        });
        Ok(Self::poll_with_parked(future, parked))
    }

    fn poll_with_parked<F: Future + Unpin>(mut future: F, parked: Arc<Self>) -> F::Output {
        let waker = Waker::from(Arc::clone(&parked));
        let mut context = Context::from_waker(&waker);
        loop {
            if let Poll::Ready(output) = Pin::new(&mut future).poll(&mut context) {
                drop(future);
                return output;
            }
            let mut woken = parked.woken.lock().unwrap_or_else(PoisonError::into_inner);
            while !*woken {
                woken = parked.changed.wait(woken).unwrap_or_else(PoisonError::into_inner);
            }
            *woken = false;
        }
    }
}

/// Actual wake owner admitted before a future borrows the invocation meter.
pub(super) struct PreparedCallbackWait {
    parked: Arc<ParkedCallback>,
    bytes: u64,
}

impl PreparedCallbackWait {
    pub(super) fn logical_owned_bytes(&self) -> u64 { self.bytes }
}

struct CaptureRuntimeAllocation<'a> {
    meter: Option<&'a dyn acyclic_native_runtime::NativeReadMeter>,
    bytes: u64,
}

impl<'a> CaptureRuntimeAllocation<'a> {
    fn new(
        meter: Option<&'a dyn acyclic_native_runtime::NativeReadMeter>, bytes: usize,
    ) -> std::io::Result<Self> {
        let bytes = u64::try_from(bytes).map_err(std::io::Error::other)?;
        if let Some(meter) = meter { meter.allocate(bytes)?; }
        Ok(Self { meter, bytes })
    }
}

impl Drop for CaptureRuntimeAllocation<'_> {
    fn drop(&mut self) {
        if let Some(meter) = self.meter { meter.release(self.bytes); }
    }
}

/// Initializes the process-shared mount callback runtime outside a bounded
/// preparation invocation. Native I/O services have their own explicit startup.
///
/// # Errors
///
/// Preserves the original runtime-startup error and its OS cause.
pub fn initialize_native_mount_services() -> Result<(), NativeMountError> {
    CallbackRuntime::create().map(|_| ())
}

impl CallbackRuntime {
    pub(super) fn create() -> Result<Self, NativeMountError> {
        let runtime = CALLBACK_RUNTIME.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .thread_name("acyclic-fs-mount")
                .build()
                .map_err(Arc::new)
        });
        Self::from_service(runtime)
    }

    pub(super) fn existing() -> Result<Self, NativeMountError> {
        Self::existing_startup().map_err(Into::into)
    }

    pub(super) fn existing_startup() -> Result<Self, super::NativeMountStartupAdmissionError> {
        let runtime = CALLBACK_RUNTIME.get()
            .ok_or(super::NativeMountStartupAdmissionError::ServicesCold)?;
        match runtime {
            Ok(runtime) => Ok(Self { handle: runtime.handle().clone() }),
            Err(error) => Err(super::NativeMountStartupAdmissionError::ServiceUnavailable(Arc::clone(error))),
        }
    }

    fn from_service(
        runtime: &Result<tokio::runtime::Runtime, Arc<std::io::Error>>,
    ) -> Result<Self, NativeMountError> {
        match runtime {
            Ok(runtime) => Ok(Self { handle: runtime.handle().clone() }),
            Err(error) => Err(NativeMountError::ServiceUnavailable(Arc::clone(error))),
        }
    }

    pub(super) fn prepare_wait(
        &self, work: &mut crate::demand::DemandWork,
    ) -> Result<PreparedCallbackWait, MountSourceError> {
        work.check().map_err(super::lazy::mount_demand_error)?;
        let bytes = std::mem::size_of::<ParkedCallback>() as u64;
        work.allocate(bytes).map_err(super::lazy::mount_demand_error)?;
        Ok(PreparedCallbackWait {
            parked: Arc::new(ParkedCallback {
                woken: StdMutex::new(false), changed: std::sync::Condvar::new(),
                _native_allocation: None,
            }),
            bytes,
        })
    }

    /// Polls the original producer in place, retaining its cancellation and
    /// quiescence protocol. No independent timeout drops already-started work.
    /// Release the prepared owner's bytes only after this call returns.
    pub(super) fn wait_prepared<F: Future>(
        &self, prepared: PreparedCallbackWait, create: impl FnOnce() -> F,
    ) -> F::Output {
        let poll = || {
            let _runtime = self.handle.enter();
            let _inline = acyclic_native_runtime::InlineBlocking::enter();
            let mut future = std::pin::pin!(tokio::task::unconstrained(create()));
            ParkedCallback::poll_with_parked(future.as_mut(), prepared.parked)
        };
        match tokio::runtime::Handle::try_current() {
            Ok(current) if current.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread =>
                tokio::task::block_in_place(poll),
            _ => poll(),
        }
    }

    /// Checks authentic callback authority on this existing runtime before the
    /// provider allocates its exchange frame or starts a source effect.
    pub(super) fn verify_native_callback_guard(
        &self, guard: &dyn super::NativeMountCallbackGuard,
        work: &super::NativeMountMeterWork,
    ) -> Result<(), NativeMountError> {
        self.verify_native_callback_role(guard, work, false)
    }

    /// Checks authored WRITE on the same runtime and original callback binding.
    /// SDK cache hydration and physical retirement use their distinct original
    /// namespace permission, never this authored-source grant.
    pub(super) fn verify_native_callback_write_guard(
        &self, guard: &dyn super::NativeMountCallbackGuard,
        work: &super::NativeMountMeterWork,
    ) -> Result<(), NativeMountError> {
        self.verify_native_callback_role(guard, work, true)
    }

    fn verify_native_callback_role(
        &self, guard: &dyn super::NativeMountCallbackGuard,
        work: &super::NativeMountMeterWork, authored_write: bool,
    ) -> Result<(), NativeMountError> {
        let allocation = work.native_materialization_work().claim_allocation(
            std::mem::size_of::<ParkedCallback>() as u64, 1,
        )?;
        let prepared = PreparedCallbackWait {
            parked: Arc::new(ParkedCallback {
                woken: StdMutex::new(false),
                changed: std::sync::Condvar::new(),
                _native_allocation: Some(allocation),
            }),
            // Custody is intrinsic to the actual wake owner and survives every
            // cloned waker, rather than being released by the waiting observer.
            bytes: 0,
        };
        self.wait_prepared(prepared, || async {
            let mut verification = if authored_write { guard.check_write(work)? }
                else { guard.check(work)? };
            (&mut verification).await
        })
    }

    fn block_on_measured<F: Future>(
        &self, create: impl FnOnce() -> F, meter: &dyn acyclic_native_runtime::NativeReadMeter,
    ) -> std::io::Result<F::Output> {
        let _runtime = self.handle.enter();
        let _inline = acyclic_native_runtime::InlineBlocking::enter();
        let mut allocation = CaptureRuntimeAllocation { meter: Some(meter), bytes: 0 };
        let (future, _) = crate::heap_future::in_heap_admitted(
            (create, &mut allocation),
            |(_, allocation), bytes| {
                meter.allocate(bytes)?;
                allocation.bytes = bytes;
                Ok::<_, std::io::Error>(())
            },
            |(create, _)| tokio::task::unconstrained(create()),
        )?;
        ParkedCallback::poll_to_completion_measured(future, Some(meter))
    }

    fn block_on<F: Future>(&self, create: impl FnOnce() -> F) -> F::Output {
        // Polled in place, the callback would otherwise draw on the calling
        // task's cooperative budget. On a current-thread runtime that budget
        // refills only when the calling task yields, which it cannot do
        // while blocked here: once spent, every Tokio resource would report
        // Pending and wake at once, forever.
        let poll = || {
            // Built inside the runtime's context, which timers need.
            let _runtime = self.handle.enter();
            // This thread serves exactly this callback until it completes.
            let _inline = acyclic_native_runtime::InlineBlocking::enter();
            ParkedCallback::poll_to_completion(tokio::task::unconstrained(in_heap(create)))
        };
        match tokio::runtime::Handle::try_current() {
            // A multi-thread worker first hands its scheduler core to another
            // thread, so tasks queued behind this callback keep running.
            Ok(current)
                if current.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread =>
            {
                tokio::task::block_in_place(poll)
            }
            _ => poll(),
        }
    }

    /// Runs one callback's complete future under the finite callback deadline.
    pub(super) fn wait<T, F: Future<Output = Result<T, MountSourceError>>>(
        &self,
        create: impl FnOnce() -> F,
    ) -> Result<T, MountSourceError> {
        self.block_on(|| tokio::time::timeout(CALLBACK_TIMEOUT, in_heap(create)))
            .map_err(|_| MountSourceError::Stale)?
    }
}

struct DetachedMountState<A, O> {
    file: DetachedFile<A, O>,
    metadata: FileMetadata,
    mutation_epoch: u64,
}

impl<A, O> DetachedMountState<A, O> {
    /// Applies one content change with its time stamp, as every native
    /// write does, as one detached record transition, then records it: the
    /// change and its record happen together or not at all.
    async fn change_content(
        &mut self,
        change: ContentChange<std::convert::Infallible>,
        ledger: &ViewLedger,
        cancellation: &CancellationToken,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let mut metadata = self.metadata;
        let stamped = metadata
            .stamp_content_change(content_change_time()?)
            .then_some(metadata);
        self.file
            .change_content(change, stamped, boundary_budget(), cancellation)
            .await
            .map_err(engine_error)?;
        self.metadata = metadata;
        self.record_change(ledger)
    }

    /// Advances the identity's own publication epoch and invalidates every
    /// cached lookup of the identity, under the lock its lookups take.
    fn record_change(&mut self, ledger: &ViewLedger) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.mutation_epoch = self
            .mutation_epoch
            .checked_add(1)
            .ok_or(MountSourceError::Stale)?;
        ledger.record(&ViewChange::Node(self.file.file_id()), &mut NativeDemandCarrier::Borrowed(&mut super::view_ledger::unbounded_view_work()), &mut super::view_ledger::ViewLedgerAdmission::default()).expect("unbounded view recording");
        Ok(())
    }
}

pub(super) struct CheckoutDetachedFile<A, O> {
    state: tokio::sync::Mutex<DetachedMountState<A, O>>,
    ledger: Arc<ViewLedger>,
    runtime: CallbackRuntime,
    cancellation: CancellationToken,
    profile: FilesystemProfile,
    limits: VolumeLimits,
    // Physical fields and the collection hold drop before either original claim.
    _custody: Option<super::NativeMountAllocationCustody>,
    native_allocation: Option<crate::NativeAllocation>,
}

struct CheckoutAttachedFile<A, O> {
    checkout: Arc<SharedCheckout<A, O>>,
    file_id: FileId,
    runtime: CallbackRuntime,
    cancellation: CancellationToken,
    profile: FilesystemProfile,
    limits: VolumeLimits,
    native_callback_guard: Option<Arc<dyn super::NativeMountCallbackGuard>>,
    _native_allocation: Option<crate::NativeAllocation>,
}

/// One process-local serialization and publication-fencing boundary for every
/// adapter, watcher, and transport view of the same checkout.
///
/// Mutations are exclusive and ordered; callback reads share the current view.
pub struct SharedCheckout<A, O> {
    state: tokio::sync::RwLock<SharedCheckoutState<A, O>>,
    revision: Arc<AtomicU64>,
    ledger: Arc<ViewLedger>,
    view_gate: Arc<ViewGate>,
    grouped: StdMutex<GroupedQueue>,
    /// Creations applied together with their first bytes; see
    /// [`PendingCreate`].
    pending: StdMutex<Vec<Arc<PendingCreate>>>,
    pending_allocation: StdMutex<Option<super::NativeMountAllocationCustody>>,
    publication_return: StdMutex<Option<SharedCheckoutState<A, O>>>,
    publication_changed: tokio::sync::Notify,
    // All revision aliases are private state/loan fields borrowing this Self.
    // Ledger and gate aliases instead carry their own intrinsic guards.
    _native_revision_allocation: Option<crate::NativeAllocation>,
    _native_allocation: Option<crate::NativeAllocation>,
}

/// Most bytes a created file holds before its creation is applied.
const PENDING_CREATE_BYTES: u64 = 1 << 20;

/// Waits on the original lock future under the same borrowed invocation.
pub(super) async fn await_measured_lock<F: Future>(
    lock: F,
    work: &mut crate::demand::DemandWork,
    cancellation: &CancellationToken,
    lifetime: Option<&CancellationToken>,
) -> Result<F::Output, MountSourceError> {
    await_measured_lock_with_carrier(
        lock, &mut NativeDemandCarrier::Borrowed(work), cancellation, lifetime,
    ).await
}

/// The same lock-wait engine, with short leaf locks on an existing carrier.
pub(super) async fn await_measured_lock_with_meter<F: Future>(
    lock: F,
    meter: &Arc<crate::demand::NativeDemandReadMeter>,
    cancellation: &CancellationToken,
    lifetime: Option<&CancellationToken>,
) -> Result<F::Output, MountSourceError> {
    await_measured_lock_with_carrier(
        lock, &mut NativeDemandCarrier::Shared(meter), cancellation, lifetime,
    ).await
}

/// Ready locks need no waiter allocation. Blocked locks retain their actual
/// cancellation proof and cleanup quota through completion, failure and drop.
/// The original future and prepared wakers are polled outside shared work locks.
pub(super) async fn await_measured_lock_with_carrier<F: Future>(
    lock: F,
    carrier: &mut NativeDemandCarrier<'_>,
    cancellation: &CancellationToken,
    lifetime: Option<&CancellationToken>,
) -> Result<F::Output, MountSourceError> {
    use super::lazy::{mount_demand_error, mount_wait_error};
    fn reserve_cleanup(
        carrier: &mut NativeDemandCarrier<'_>, count: u64, reserved: &mut u64,
    ) -> Result<(), MountSourceError> {
        let next_reserved = reserved.checked_add(count)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        carrier.with_work(|work| {
            let mut budget = work.budget;
            budget.items_examined = budget.items_examined.checked_sub(count)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            work.work.verify(budget).map_err(MountSourceError::Work)?;
            work.budget = budget;
            *reserved = next_reserved;
            Ok(())
        })
    }
    struct LockWait<'a, 'w> {
        carrier: &'a mut NativeDemandCarrier<'w>,
        native: Option<crate::NativeMaterializationWork>,
        reserved_items: u64,
        caller_wait: Option<crate::cancellation::PreparedCancelled>,
        lifetime_wait: Option<crate::cancellation::PreparedCancelled>,
    }
    impl LockWait<'_, '_> {
        fn finish(&mut self) -> Result<(), MountSourceError> {
            let mut caller_wait = self.caller_wait.take();
            let mut lifetime_wait = self.lifetime_wait.take();
            // Wakers may own a task/future containing this exact meter. Retire
            // them before taking its mutex, including cancellation and unwind.
            if let Some(wait) = caller_wait.as_mut() { wait.retire_waker(); }
            if let Some(wait) = lifetime_wait.as_mut() { wait.retire_waker(); }
            if self.native.is_some() {
                // Native registrations own their exact original/resource
                // cleanup escrow. Physical unregister and guard retirement
                // must run outside the original work mutex.
                drop(caller_wait);
                drop(lifetime_wait);
                return Ok(());
            }
            let reserved = std::mem::take(&mut self.reserved_items);
            self.carrier.with_work(|work| {
                // Restore only our escrow into the current budget; another
                // active waiter may still own a separate reserved delta.
                let quota = work.budget.items_examined.checked_add(reserved)
                    .ok_or(MountSourceError::Work(crate::WorkError::Overflow))
                    .map(|items| work.budget.items_examined = items);
                let caller = caller_wait.map(|wait| {
                    work.finish_cancelled(wait).map_err(mount_wait_error)
                }).unwrap_or(Ok(()));
                let lifetime = lifetime_wait.map(|wait| {
                    work.finish_cancelled(wait).map_err(mount_wait_error)
                }).unwrap_or(Ok(()));
                quota.and(caller).and(lifetime)
            })
        }
    }
    impl Drop for LockWait<'_, '_> {
        fn drop(&mut self) { let _ = self.finish(); }
    }
    cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
    if lifetime.is_some_and(CancellationToken::is_cancelled) {
        return Err(MountSourceError::Stale);
    }
    let native = carrier.original_native_work();
    if let Some(native) = &native {
        native.check().map_err(MountSourceError::NativeWork)?;
    } else {
        carrier.with_work(|work| work.check().map_err(mount_demand_error))?;
    }
    let mut waits = LockWait {
        carrier, native, reserved_items: 0, caller_wait: None, lifetime_wait: None,
    };
    let mut lock = std::pin::pin!(lock);
    let result = std::future::poll_fn(|context| {
        if let Poll::Ready(guard) = lock.as_mut().poll(context) {
            return Poll::Ready(Ok(guard));
        }
        if cancellation.is_cancelled() {
            return Poll::Ready(Err(MountSourceError::Cancelled));
        }
        if lifetime.is_some_and(CancellationToken::is_cancelled) {
            return Poll::Ready(Err(MountSourceError::Stale));
        }
        if waits.caller_wait.is_none() {
            let preparation = match &waits.native {
                Some(native) => cancellation.prepare_cancelled_with_native_work(native),
                None => waits.carrier.with_work(|work| work.prepare_cancelled(cancellation)),
            };
            let prepared = match preparation {
                Ok(prepared) => prepared,
                Err(error) => return Poll::Ready(Err(mount_wait_error(error))),
            };
            let cleanup = prepared.cleanup_items();
            waits.caller_wait = Some(prepared);
            if waits.native.is_none()
                && let Err(error) = reserve_cleanup(waits.carrier, cleanup, &mut waits.reserved_items)
            {
                return Poll::Ready(Err(error));
            }
        }
        if let Some(wait) = &mut waits.caller_wait
            && Pin::new(wait).poll(context).is_ready()
        {
            return Poll::Ready(Err(MountSourceError::Cancelled));
        }
        if let Some(token) = lifetime
            && !waits.caller_wait.as_ref().is_some_and(|wait| wait.same_token(token))
        {
            if waits.lifetime_wait.is_none() {
                let preparation = match &waits.native {
                    Some(native) => token.prepare_cancelled_with_native_work(native),
                    None => waits.carrier.with_work(|work| work.prepare_cancelled(token)),
                };
                let prepared = match preparation {
                    Ok(prepared) => prepared,
                    Err(error) => return Poll::Ready(Err(mount_wait_error(error))),
                };
                let cleanup = prepared.cleanup_items();
                waits.lifetime_wait = Some(prepared);
                if waits.native.is_none()
                    && let Err(error) = reserve_cleanup(waits.carrier, cleanup, &mut waits.reserved_items)
                {
                    return Poll::Ready(Err(error));
                }
            }
            if let Some(wait) = &mut waits.lifetime_wait
                && Pin::new(wait).poll(context).is_ready()
            {
                return Poll::Ready(Err(MountSourceError::Stale));
            }
        }
        Poll::Pending
    }).await;
    let finished = waits.finish();
    let guard = result?;
    finished?;
    cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
    if lifetime.is_some_and(CancellationToken::is_cancelled) {
        return Err(MountSourceError::Stale);
    }
    if let Some(native) = &waits.native {
        native.check().map_err(MountSourceError::NativeWork)?;
    } else {
        waits.carrier.with_work(|work| work.check().map_err(mount_demand_error))?;
    }
    Ok(guard)
}

/// A regular file created through the mount whose creation is applied with
/// its first bytes, as one change, and with the creations pending beside it,
/// as one group: when a durability request settles it, when its bytes
/// outgrow [`PENDING_CREATE_BYTES`], when [`MAXIMUM_GROUPED_CHANGES`]
/// creations wait, or before anything reads or changes what it creates,
/// whichever is first. Its name is recorded as changed when it is created,
/// so nothing remembers it absent, and a lookup of that name reports it
/// from here; no other name's answer depends on it.
pub(super) struct PendingCreate {
    path: NamespacePath,
    file_id: FileId,
    origin: ViewOrigin,
    native_callback_guard: Option<Arc<dyn super::NativeMountCallbackGuard>>,
    source_lifetime: CancellationToken,
    maximum_bytes: u64,
    state: StdMutex<PendingState>,
    failure: std::sync::OnceLock<MountSourceError>,
    custody: Option<super::NativeMountAllocationCustody>,
    _native_allocation: Option<crate::NativeAllocation>,
}

/// A creation's same original body, mutable until its first application.
/// Restored shared bytes stay immutable if a backend retains a clone.
enum PendingBytes {
    Mutable { bytes: Vec<u8>, allocation: Option<crate::NativeAllocation> },
    Shared { bytes: OwnedBytes, capacity: usize, original: Option<crate::NativeMaterializationWork> },
}

impl PendingBytes {
    fn capacity(&self) -> usize {
        match self {
            Self::Mutable { bytes, .. } => bytes.capacity(),
            Self::Shared { capacity, .. } => *capacity,
        }
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Mutable { bytes, .. } => bytes,
            Self::Shared { bytes, .. } => bytes,
        }
    }

    fn sharing_allocation(&self) -> u64 {
        match self {
            Self::Mutable { bytes, allocation } =>
                u64::from(allocation.is_none() && bytes.capacity() != 0),
            Self::Shared { .. } => 0,
        }
    }

    fn original_work(&self) -> Option<crate::NativeMaterializationWork> {
        match self {
            Self::Mutable { allocation, .. } => allocation.as_ref().map(|guard| guard.work().clone()),
            Self::Shared { original, .. } => original.clone(),
        }
    }

    fn share(
        &mut self, original: Option<&crate::NativeMaterializationWork>,
    ) -> Result<OwnedBytes, MountSourceError> {
        if let Self::Mutable { bytes, allocation } = self {
            let capacity = bytes.capacity();
            let body_original = allocation.as_ref().map(|guard| guard.work().clone());
            let shared = if let Some(guard) = allocation.as_ref() {
                let payer = original.cloned().unwrap_or_else(|| guard.work().clone());
                OwnedBytes::from_vec_with_guard_in_place(
                    bytes, allocation, payer.runtime_meter_owner(),
                ).map_err(|error| MountSourceError::NativeWork(payer.runtime_error(error)))?
            } else {
                OwnedBytes::from(std::mem::take(bytes))
            };
            *self = Self::Shared { bytes: shared, capacity, original: body_original };
        }
        let Self::Shared { bytes, .. } = self else { unreachable!("pending body was promoted"); };
        Ok(bytes.clone())
    }

    fn mutable(&mut self) -> &mut Vec<u8> {
        match self {
            Self::Mutable { bytes, .. } => bytes,
            Self::Shared { .. } => {
                let previous = std::mem::replace(self, Self::Mutable { bytes: Vec::new(), allocation: None });
                let mutable = match previous {
                    Self::Mutable { bytes, .. } => bytes,
                    Self::Shared { bytes, capacity, .. } => match bytes.try_into_ordinary_vec() {
                        Ok(bytes) => bytes,
                        Err(bytes) => {
                            // A published alias or intrinsic guard forbids a
                            // mutable take. Only this ordinary write owns a COW copy.
                            let mut mutable = Vec::with_capacity(capacity);
                            mutable.extend_from_slice(&bytes);
                            mutable
                        }
                    },
                };
                *self = Self::Mutable { bytes: mutable, allocation: None };
                self.mutable()
            }
        }
    }

    fn resize(&mut self, length: usize, value: u8) {
        self.mutable().resize(length, value);
    }

    /// Prepares a distinct funded backing only when mutation cannot reuse the
    /// same mutable allocation. The original body is untouched on refusal.
    fn prepare_native_capacity(
        &self, length: usize, maximum: usize, original: &crate::NativeMaterializationWork,
    ) -> Result<Option<Self>, MountSourceError> {
        if matches!(self, Self::Mutable { bytes, .. } if bytes.capacity() >= length) {
            return Ok(None);
        }
        let requested = match self {
            Self::Mutable { bytes, .. } => length.max(bytes.capacity().saturating_mul(2).max(8).min(maximum)),
            Self::Shared { capacity, .. } => length.max(*capacity),
        };
        let copied = self.len().min(length);
        let mut scratch = NativeCaptureScratchAllocation::new(original)?;
        scratch.admit(crate::WorkCounters { bytes_copied: copied as u64, ..crate::WorkCounters::UNCHARGED })?;
        let mut grown = scratch.vec::<u8>(requested)?;
        grown.extend_from_slice(&self.as_slice()[..copied]);
        scratch.record(crate::WorkCounters { bytes_copied: copied as u64, ..crate::WorkCounters::UNCHARGED })?;
        Ok(Some(Self::Mutable { bytes: grown, allocation: Some(scratch.retain()) }))
    }
}

impl Deref for PendingBytes {
    type Target = [u8];
    fn deref(&self) -> &Self::Target { self.as_slice() }
}

impl DerefMut for PendingBytes {
    fn deref_mut(&mut self) -> &mut Self::Target { self.mutable().as_mut_slice() }
}

enum PendingState {
    Pending {
        metadata: Box<FileMetadata>,
        bytes: PendingBytes,
    },
    Applying {
        metadata: Box<FileMetadata>,
        bytes: PendingBytes,
    },
    Applied,
    Failed,
}

impl PendingCreate {
    fn state(&self) -> std::sync::MutexGuard<'_, PendingState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Prepares the creation without moving its original bytes until every
    /// allocation needed to begin has been admitted.
    fn begin_applying_measured(
        &self,
        limits: VolumeLimits,
        work: &mut crate::demand::DemandWork,
        claimed_bytes: &mut u64,
    ) -> Result<Option<GroupedChange>, MountSourceError> {
        use super::lazy::{mount_clone_namespace, mount_demand_error};
        let mut state = self.state();
        let PendingState::Pending { metadata, bytes } = &*state else {
            return Ok(None);
        };
        let path = mount_clone_namespace(self.path.components(), limits, work)?;
        *claimed_bytes = claimed_bytes.checked_add(path.retained_bytes())
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let metadata_bytes = std::mem::size_of::<FileMetadata>() as u64;
        let sharing_allocation = bytes.sharing_allocation();
        work.work.admit(&crate::WorkCounters {
            allocation_operations: 1 + sharing_allocation,
            bytes_copied: metadata_bytes,
            ..crate::WorkCounters::default()
        }, &work.budget).map_err(MountSourceError::Work)?;
        work.allocate(std::mem::size_of::<FileMetadata>() as u64)
            .map_err(mount_demand_error)?;
        *claimed_bytes = claimed_bytes.checked_add(metadata_bytes)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let prepared_metadata = Box::new(**metadata);
        work.record(crate::WorkCounters { bytes_copied: metadata_bytes, ..crate::WorkCounters::default() })
            .map_err(mount_demand_error)?;
        // A spare-capacity Vec eagerly acquires Bytes' sharing owner; a
        // full nonempty Vec acquires it on the first required clone below.
        // Its private refcount/allocator bookkeeping is not logical payload,
        // but the actual allocation operation is part of this preparation.
        work.charge(crate::WorkCounters {
            allocation_operations: sharing_allocation,
            ..crate::WorkCounters::default()
        }).map_err(mount_demand_error)?;
        self.begin_applying_prepared(&mut state, path, prepared_metadata, None)
    }

    fn begin_applying_with_native_work(
        &self, original: &crate::NativeMaterializationWork,
        claimed: &mut crate::NativeAllocation, cancellation: &CancellationToken,
    ) -> Result<Option<GroupedChange>, MountSourceError> {
        let mut state = self.state();
        let PendingState::Pending { metadata, bytes } = &*state else { return Ok(None); };
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, cancellation);
        let (path, path_allocation) = self.path.clone_with_accounting(&mut accounting, cancellation)
            .map_err(MountSourceError::Filesystem)?;
        let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(mut path_allocation) =
            path_allocation else { unreachable!("native pending path retains its original allocation"); };
        claimed.absorb(&mut path_allocation).map_err(MountSourceError::NativeWork)?;
        let metadata_bytes = std::mem::size_of::<FileMetadata>() as u64;
        let sharing = bytes.sharing_allocation();
        original.admit(crate::WorkCounters {
            allocation_operations: 1 + sharing, bytes_copied: metadata_bytes,
            ..crate::WorkCounters::UNCHARGED
        }).map_err(MountSourceError::NativeWork)?;
        let mut metadata_allocation = original.claim(metadata_bytes).map_err(MountSourceError::NativeWork)?;
        original.charge(crate::WorkCounters {
            bytes_copied: metadata_bytes, ..crate::WorkCounters::UNCHARGED
        }).map_err(MountSourceError::NativeWork)?;
        let prepared_metadata = Box::new(**metadata);
        claimed.absorb(&mut metadata_allocation).map_err(MountSourceError::NativeWork)?;
        original.charge(crate::WorkCounters {
            allocation_operations: sharing, ..crate::WorkCounters::UNCHARGED
        }).map_err(MountSourceError::NativeWork)?;
        self.begin_applying_prepared(&mut state, path, prepared_metadata, Some(original))
    }

    fn begin_applying_prepared(
        &self, state: &mut PendingState, path: NamespacePath, prepared_metadata: Box<FileMetadata>,
        original: Option<&crate::NativeMaterializationWork>,
    ) -> Result<Option<GroupedChange>, MountSourceError> {
        let PendingState::Pending { bytes, .. } = state else { return Err(MountSourceError::Stale); };
        // Rejected control admission leaves the same mutable vector and guard
        // in Pending. No body or metadata ownership is split before promotion.
        let prepared_bytes = bytes.share(original)?;
        let PendingState::Pending { metadata, bytes } =
            std::mem::replace(state, PendingState::Applied)
        else { return Err(MountSourceError::Stale); };
        let change = GroupedChange::CreateFile {
            path, metadata: prepared_metadata, file_id: self.file_id, bytes: prepared_bytes,
        };
        *state = PendingState::Applying { metadata, bytes };
        Ok(Some(change))
    }

    /// Restores an uninstalled creation by moving its same owned body. No
    /// uniqueness or allocation is required when a backend retains clones.
    fn restore_applying_retained(&self) {
        let mut state = self.state();
        match std::mem::replace(&mut *state, PendingState::Applied) {
            PendingState::Applying { metadata, bytes, .. } => {
                *state = PendingState::Pending { metadata, bytes };
            }
            other => *state = other,
        }
    }

    fn finish_applying(&self, applied: Result<GroupedOutcome, MountSourceError>) {
        let next = match applied {
            Ok(_) => PendingState::Applied,
            Err(error) => {
                let own_alias = matches!(&error, MountSourceError::Retained(cause)
                    if Arc::as_ptr(cause) as *const () == self as *const Self as *const ());
                if own_alias {
                    assert!(self.failure.get().is_some(), "original pending error cause exists");
                } else {
                    assert!(self.failure.set(error).is_ok(), "original pending outcome delivered once");
                }
                PendingState::Failed
            }
        };
        let old = std::mem::replace(&mut *self.state(), next);
        let metadata_dropped = matches!(&old, PendingState::Pending { .. } | PendingState::Applying { .. });
        drop(old);
        if metadata_dropped && let Some(custody) = &self.custody {
            custody.retire(std::mem::size_of::<FileMetadata>() as u64)
                .expect("original pending metadata allocation custody");
        }
    }

    /// The file as it is created, while its creation waits.
    fn lookup(&self) -> Option<MountLookup> {
        match &*self.state() {
            PendingState::Pending { metadata, bytes } => Some(MountLookup {
                node: MountNode {
                    file_id: self.file_id,
                    kind: MountNodeKind::Regular,
                    logical_bytes: bytes.len() as u64,
                    link_count: 1,
                    device: None,
                },
                metadata: **metadata,
            }),
            _ => None,
        }
    }

    /// What the creation left for a handle to report once applied.
    fn applied(self: &Arc<Self>) -> Result<(), MountSourceError> {
        match &*self.state() {
            PendingState::Failed => {
                let cause: Arc<dyn super::MountSourceFailureCause> = Arc::clone(self) as Arc<dyn super::MountSourceFailureCause>;
                Err(MountSourceError::Retained(cause))
            }
            _ => Ok(()),
        }
    }
}

impl super::MountSourceFailureCause for PendingCreate {
    fn semantic_error(&self) -> &MountSourceError {
        self.failure.get().expect("original pending failure was delivered").semantic_error()
    }
}

/// Most queued changes one hold of the checkout applies as one mutation.
const MAXIMUM_GROUPED_CHANGES: usize = 64;

/// Changes waiting for group commit, and whether a drain is running.
#[derive(Default)]
struct GroupedQueue {
    requests: std::collections::VecDeque<GroupedRequest>,
    draining: bool,
    publishing: bool,
}

/// Delivery stays attached to the original requester, including pending files.
enum GroupedReply {
    Callback(tokio::sync::oneshot::Sender<Result<GroupedOutcome, MountSourceError>>),
    Pending(Arc<PendingCreate>),
}

impl GroupedReply {
    fn send(self, result: Result<GroupedOutcome, MountSourceError>) {
        match self {
            Self::Callback(reply) => { let _ = reply.send(result); }
            Self::Pending(create) => create.finish_applying(result),
        }
    }

    fn reject_uninstalled(self, error: MountSourceError) {
        let retryable = match error.semantic_error() {
            MountSourceError::Work(_) | MountSourceError::Cancelled | MountSourceError::Stale => true,
            MountSourceError::Filesystem(FsError::Work(_) | FsError::Cancelled(_)) => true,
            MountSourceError::NativeWork(error)
            | MountSourceError::Filesystem(FsError::NativeKernel(error)) =>
                super::native_materialization_failure_is_fatal(error),
            MountSourceError::NativeWorkspaceCause(cause) =>
                super::native_workspace_failure_is_fatal(&cause.source),
            MountSourceError::WorkspaceCause(error) => super::native_workspace_failure_is_fatal(error),
            _ => false,
        };
        match self {
            Self::Pending(create) if retryable => {
                create.restore_applying_retained();
            }
            reply => reply.send(Err(error)),
        }
    }
}

/// Keeps each already admitted cause owner attached to its terminal reply.
struct GroupedCompletion {
    admission: Arc<GroupedAdmission>,
    reply: GroupedReply,
}

/// Owns the same reply and outcome buffers across cancellable awaits.
#[derive(Default)]
struct GroupedDelivery {
    replies: Vec<GroupedCompletion>,
    views: Vec<GroupedView>,
    results: Option<Vec<Result<GroupedOutcome, FsError>>>,
    installed: bool,
    native_errors: bool,
}

/// Only newly allocated phase slots and view paths belong to this guard.
/// Requests, their changes, and any shared payload remain borrowed in preflight.
struct GroupedPhaseBuffers {
    pub origins: Vec<(ViewOrigin, bool)>,
    pub changes: Vec<GroupedChange>,
    pub delivery: GroupedDelivery,
    // The changes backing physically retires before its transferable ticket.
    pub changes_allocation: Option<crate::NativeAllocation>,
    // All other native phase backing retires before this original ticket.
    pub native_phase_allocation: Option<crate::NativeAllocation>,
}

struct NativeGroupedChangeSlots {
    changes: Vec<GroupedChange>,
    allocation: Option<crate::NativeAllocation>,
}

fn prepare_native_grouped_change_slots(
    count: usize,
    native: &crate::NativeMaterializationWork,
) -> Result<NativeGroupedChangeSlots, MountSourceError> {
    let mut scratch = NativeCaptureScratchAllocation::new(native)?;
    let changes = scratch.vec::<GroupedChange>(count)?;
    Ok(NativeGroupedChangeSlots { changes, allocation: Some(scratch.retain()) })
}

struct NativeAuthoredPhaseInput {
    authored: Vec<AuthoredMutation>,
    slots_allocation: crate::NativeAllocation,
}

fn prepare_native_authored_phase_input(
    path: NamespacePath,
    original: &crate::NativeMaterializationWork,
    build: impl FnOnce(NamespacePath) -> AuthoredMutation,
) -> Result<NativeAuthoredPhaseInput, MountSourceError> {
    let mut slots = NativeCaptureScratchAllocation::new(original)?;
    let mut authored = slots.vec(1)?;
    original.charge(crate::WorkCounters {
        bytes_copied: std::mem::size_of::<AuthoredMutation>() as u64,
        items_examined: 1,
        ..crate::WorkCounters::UNCHARGED
    }).map_err(MountSourceError::NativeWork)?;
    authored.push(build(path));
    Ok(NativeAuthoredPhaseInput { authored, slots_allocation: slots.retain() })
}

fn prepare_grouped_buffers_with_carrier<'a, 'w>(
    requests: &[GroupedRequest],
    limits: VolumeLimits,
    carrier: &'a mut NativeDemandCarrier<'w>,
) -> Result<NativeCarrierOwned<'a, 'w, GroupedPhaseBuffers>, MountSourceError> {
    if let Some(native) = carrier.original_native_work() {
        // Full native admissions operate outside the DemandWork leaf lock;
        // each new backing carries the same original resource allowance.
        let mut phase = NativeCaptureScratchAllocation::new(&native)?;
        let mut change_slots = prepare_native_grouped_change_slots(requests.len(), &native)?;
        let mut buffers = prepare_grouped_phase_buffers_with_scratch(
            requests, limits, std::mem::take(&mut change_slots.changes), true, &mut phase,
        )?;
        buffers.changes_allocation = change_slots.allocation.take();
        buffers.native_phase_allocation = Some(phase.retain());
        return Ok(NativeCarrierOwned { value: Some(buffers), carrier, bytes: 0 });
    }
    let (buffers, bytes) = carrier.with_work(|work| {
        let mut scratch = CaptureScratchAllocation::new(work);
        let changes = scratch.vec::<GroupedChange>(requests.len())?;
        let buffers = prepare_grouped_phase_buffers_with_scratch(
            requests, limits, changes, false, &mut scratch,
        )?;
        Ok::<_, MountSourceError>((buffers, scratch.retain()))
    })?;
    Ok(NativeCarrierOwned { value: Some(buffers), carrier, bytes })
}

fn prepare_grouped_phase_buffers_with_scratch<S: CaptureInputScratch>(
    requests: &[GroupedRequest],
    limits: VolumeLimits,
    changes: Vec<GroupedChange>,
    native_errors: bool,
    scratch: &mut S,
) -> Result<GroupedPhaseBuffers, MountSourceError> {
    // Scratch belongs to the caller, so every partial physical owner here
    // unwinds before its exact original allocation tickets are released.
    let mut origins = scratch.vec::<(ViewOrigin, bool)>(requests.len())?;
    let replies = scratch.vec::<GroupedCompletion>(requests.len())?;
    let mut views = scratch.vec::<GroupedView>(requests.len())?;
    for request in requests {
        scratch.items(1)?;
        scratch.charge(crate::WorkCounters {
            bytes_copied: (std::mem::size_of::<ViewOrigin>()
                + std::mem::size_of::<bool>() + std::mem::size_of::<GroupedView>()) as u64,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let view = match &request.change {
            GroupedChange::CreateFile { path, .. } => {
                let mut names = scratch.vec::<LogicalName>(path.components().len())?;
                for name in path.components() {
                    scratch.items(1)?;
                    scratch.charge(crate::WorkCounters {
                        bytes_copied: std::mem::size_of::<LogicalName>() as u64,
                        ..crate::WorkCounters::UNCHARGED
                    })?;
                    let name = capture_copy_name(
                        name.encoding(), name.as_bytes(), limits, scratch,
                    )?;
                    names.push(name);
                }
                scratch.items(names.len())?;
                GroupedView::Bound(NamespacePath::new(names, limits)
                    .map_err(|error| scratch.engine_error(error))?)
            }
            GroupedChange::Content { file_id, .. } => {
                scratch.charge(crate::WorkCounters {
                    bytes_copied: std::mem::size_of::<FileId>() as u64,
                    ..crate::WorkCounters::UNCHARGED
                })?;
                GroupedView::Node(*file_id)
            }
        };
        origins.push((request.origin, request.recorded));
        views.push(view);
    }
    Ok(GroupedPhaseBuffers {
        origins,
        changes,
        delivery: GroupedDelivery { replies, views, results: None, installed: false, native_errors },
        changes_allocation: None,
        native_phase_allocation: None,
    })
}

impl GroupedDelivery {
    fn complete(&mut self, completed: Result<(), MountSourceError>) -> Result<(), MountSourceError> {
        let mut completed_error = completed.err();
        let mut completed_cause: Option<Arc<dyn super::MountSourceFailureCause>> = None;
        let mut batch = Ok(());
        if let Some(results) = self.results.take() {
            for ((result, view), completion) in results.into_iter()
                .zip(std::mem::take(&mut self.views)).zip(self.replies.drain(..))
            {
                let (answer, installed) = match result {
                    Ok(outcome) => {
                        // A successful request has no per-item error, so its
                        // original admission can own the common terminal cause.
                        if let Some(error) = completed_error.take() {
                            let cause = completion.admission.retain_failure(error);
                            batch = Err(MountSourceError::Retained(Arc::clone(&cause)));
                            completed_cause = Some(cause);
                        }
                        let answer = match &completed_cause {
                            Some(cause) => Err(MountSourceError::Retained(Arc::clone(cause))),
                            None => Ok(outcome),
                        };
                        (answer, self.installed)
                    }
                    Err(error) => {
                        let cause = completion.admission.retain_failure(grouped_error(error, &view, self.native_errors));
                        if batch.is_ok() && completed_error.is_none() {
                            batch = Err(MountSourceError::Retained(Arc::clone(&cause)));
                        }
                        (Err(MountSourceError::Retained(cause)), false)
                    }
                };
                match answer {
                    Err(error) if !installed => completion.reply.reject_uninstalled(error),
                    answer => completion.reply.send(answer),
                }
            }
        } else if let Some(completion) = self.replies.first() {
            let failed = completed_error.is_some();
            let cause = completion.admission.retain_failure(
                completed_error.take().unwrap_or(MountSourceError::Cancelled),
            );
            if failed {
                batch = Err(MountSourceError::Retained(Arc::clone(&cause)));
            }
            for completion in self.replies.drain(..) {
                completion.reply.reject_uninstalled(MountSourceError::Retained(Arc::clone(&cause)));
            }
        }
        // When every item failed, no reply needs the common completion error.
        // Keep that original error as the batch result instead of occupying an
        // admission whose cell already owns its distinct per-item cause.
        match completed_error {
            Some(error) => Err(error),
            None => batch,
        }
    }
}

impl Drop for GroupedDelivery {
    fn drop(&mut self) {
        let _ = self.complete(Err(MountSourceError::Cancelled));
    }
}

fn grouped_error(error: FsError, view: &GroupedView, native_errors: bool) -> MountSourceError {
    if native_errors {
        return MountSourceError::Filesystem(error);
    }
    match error {
        error if matches!(view, GroupedView::Bound(_)) && matches!(error,
            FsError::CreationRejected | FsError::NotFound
                | FsError::Mutation(GenerationMutationError::AlreadyExists
                    | GenerationMutationError::MissingParent | GenerationMutationError::MissingSource)
        ) => fs_error(error),
        error => super::lazy::mount_engine_error(error),
    }
}

/// One change waiting for group commit, and where its own result goes.
struct GroupedRequest {
    change: GroupedChange,
    admission: Arc<GroupedAdmission>,
    reply: GroupedReply,
    /// The requester's origin, which its change takes whichever caller or
    /// drain applies the group.
    origin: ViewOrigin,
    /// Whether the change was recorded when it was requested, as a pending
    /// creation is (see [`PendingCreate`]).
    recorded: bool,
    native_callback_guard: Option<Arc<dyn super::NativeMountCallbackGuard>>,
    source_lifetime: Option<CancellationToken>,
}

/// Current verification bindings of this original group, not cached decisions.
struct GroupedCallbackBindings {
    entries: Vec<(Option<Arc<dyn super::NativeMountCallbackGuard>>, Option<CancellationToken>)>,
    legacy_custody: Option<super::NativeMountAllocationCustody>,
    native_allocation: Option<crate::NativeAllocation>,
}
impl GroupedCallbackBindings {
    fn prepare(
        requests: &[GroupedRequest],
        admission: &GroupedAdmission,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<Self, MountSourceError> {
        use super::lazy::{mount_demand_error, mount_vec};
        let native = carrier.original_native_work();
        let mut bindings = if let Some(original) = native.as_ref() {
            let mut scratch = NativeCaptureScratchAllocation::new(original)?;
            let entries = scratch.vec(requests.len())?;
            Self { entries, legacy_custody: None, native_allocation: Some(scratch.retain()) }
        } else {
            let custody = match admission._allocation.as_ref() {
                Some(custody) => custody.clone(),
                None => carrier.new_custody()?,
            };
            carrier.with_work(|work| {
                let mut scratch = CaptureScratchAllocation::new(work);
                let entries = mount_vec(requests.len(), scratch.work)?;
                let bytes = entries.capacity().checked_mul(std::mem::size_of::<(
                    Option<Arc<dyn super::NativeMountCallbackGuard>>, Option<CancellationToken>,
                )>()).ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
                scratch.track(bytes);
                custody.retain_admitted(bytes, scratch.work).map_err(super::lazy::mount_demand_error)?;
                scratch.retain();
                Ok::<_, MountSourceError>(Self { entries, legacy_custody: Some(custody), native_allocation: None })
            })?
        };
        for request in requests {
            let delta = crate::WorkCounters {
                items_examined: 1,
                bytes_copied: std::mem::size_of::<(
                    Option<Arc<dyn super::NativeMountCallbackGuard>>, Option<CancellationToken>,
                )>() as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            match native.as_ref() {
                Some(original) => original.charge(delta).map_err(MountSourceError::NativeWork)?,
                None => carrier.with_work(|work| work.charge(delta).map_err(mount_demand_error))?,
            }
            bindings.entries.push((
                request.native_callback_guard.as_ref().map(Arc::clone), request.source_lifetime.clone(),
            ));
        }
        Ok(bindings)
    }
}

fn grouped_publication_guard(
    admission: Arc<GroupedAdmission>,
) -> impl for<'m> FnMut(&'m crate::NativeMaterializationWork) -> SourcePublicationFuture + Clone + Send + 'static {
    move |original: &crate::NativeMaterializationWork| {
        use futures::FutureExt;
        if let Err(error) = original.admit(crate::WorkCounters::UNCHARGED) {
            return futures::future::Either::Left(std::future::ready(Err(MountSourceError::NativeWork(error))));
        }
        let mounted = super::NativeMountMeterWork::from_native_materialization_work(original);
        let owned_work = mounted.clone();
        let admission = Arc::clone(&admission);
        match super::NativeMountCallbackFuture::new_native_guarded(&mounted, move || async move {
            let original = owned_work.native_materialization_work();
            let bindings = admission.callback_bindings.get().expect("original group bindings prepared before effects");
            for (guard, lifetime) in &bindings.entries {
                original.charge(crate::WorkCounters {
                    items_examined: 1, ..crate::WorkCounters::UNCHARGED
                })?;
                if lifetime.as_ref().is_some_and(CancellationToken::is_cancelled) {
                    return Err(super::NativeMountError::Source(MountSourceError::Stale));
                }
                if let Some(guard) = guard { guard.check_write(&owned_work)?.await?; }
                if lifetime.as_ref().is_some_and(CancellationToken::is_cancelled) {
                    return Err(super::NativeMountError::Source(MountSourceError::Stale));
                }
            }
            original.admit(crate::WorkCounters::UNCHARGED)?;
            Ok(())
        }) {
            Ok(future) => futures::future::Either::Right(future.map(
                source_publication_result as fn(_) -> _,
            )),
            Err(error) => futures::future::Either::Left(std::future::ready(
                source_publication_result(Err(error)),
            )),
        }
    }
}

/// Whether a queued change was claimed for application or abandoned by its
/// caller first. Exactly one of the two ever happens.
struct GroupedAdmission {
    phase: AtomicU8,
    failure: OnceLock<MountSourceError>,
    callback_bindings: OnceLock<GroupedCallbackBindings>,
    _allocation: Option<super::NativeMountAllocationCustody>,
    _native_allocation: Option<crate::NativeAllocation>,
}

const GROUPED_QUEUED: u8 = 0;
const GROUPED_CLAIMED: u8 = 1;
const GROUPED_ABANDONED: u8 = 2;

impl GroupedAdmission {
    fn new(phase: u8) -> Self {
        Self { phase: AtomicU8::new(phase), failure: OnceLock::new(),
            callback_bindings: OnceLock::new(), _allocation: None, _native_allocation: None }
    }

    fn new_measured(
        phase: u8,
        work: &mut crate::demand::DemandWork,
    ) -> Result<Arc<Self>, MountSourceError> {
        let allocation = super::NativeMountAllocationCustody::new(work).map_err(super::lazy::mount_demand_error)?;
        allocation.claim(std::mem::size_of::<Self>() as u64, work).map_err(super::lazy::mount_demand_error)?;
        Ok(Arc::new(Self {
            phase: AtomicU8::new(phase),
            failure: OnceLock::new(),
            callback_bindings: OnceLock::new(),
            _allocation: Some(allocation),
            _native_allocation: None,
        }))
    }

    fn new_with_native_work(
        phase: u8, original: &crate::NativeMaterializationWork,
    ) -> Result<Arc<Self>, MountSourceError> {
        let allocation = original.claim(std::mem::size_of::<Self>() as u64)
            .map_err(MountSourceError::NativeWork)?;
        Ok(Arc::new(Self {
            phase: AtomicU8::new(phase), failure: OnceLock::new(),
            callback_bindings: OnceLock::new(),
            _allocation: None, _native_allocation: Some(allocation),
        }))
    }

    fn claim(&self) -> bool {
        self.phase
            .compare_exchange(
                GROUPED_QUEUED,
                GROUPED_CLAIMED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn retain_failure(self: &Arc<Self>, error: MountSourceError) -> Arc<dyn super::MountSourceFailureCause> {
        let error = match error {
            // Never put an alias back inside its owner, even if this is the
            // same admission: retaining an existing cause needs no new cell.
            MountSourceError::Retained(cause) => return cause,
            error => error,
        };
        self.failure.get_or_init(|| error);
        let cause: Arc<Self> = Arc::clone(self);
        cause
    }
}

impl super::MountSourceFailureCause for GroupedAdmission {
    fn semantic_error(&self) -> &MountSourceError {
        self.failure.get().expect("grouped cause published before sharing its admission")
    }
}

/// A future polled in place by its caller that, if the caller drops it
/// unfinished, continues on its own task: no caller's deadline can stop it
/// midway.
struct RunToCompletion {
    future: Option<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>>,
    runtime: tokio::runtime::Handle,
}

impl RunToCompletion {
    fn new(future: impl std::future::Future<Output = ()> + Send + 'static) -> Self {
        Self {
            future: Some(Box::pin(future)),
            runtime: tokio::runtime::Handle::current(),
        }
    }
}

impl std::future::Future for RunToCompletion {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        let Some(future) = self.future.as_mut() else {
            return std::task::Poll::Ready(());
        };
        let polled = future.as_mut().poll(context);
        if polled.is_ready() {
            self.future = None;
        }
        polled
    }
}

impl Drop for RunToCompletion {
    fn drop(&mut self) {
        if let Some(future) = self.future.take() {
            drop(self.runtime.spawn(future));
        }
    }
}

/// Abandons a still-queued change when its caller stops waiting, so a
/// change whose caller saw it fail can never apply later. A change already
/// claimed is being applied and completes regardless.
struct AbandonUnlessClaimed(Arc<GroupedAdmission>);

impl Drop for AbandonUnlessClaimed {
    fn drop(&mut self) {
        let _ = self.0.phase.compare_exchange(
            GROUPED_QUEUED,
            GROUPED_ABANDONED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
}

/// Exclusive access to a shared checkout. The retained view lease prevents a
/// native callback from observing a partial external SDK operation.
///
/// A change to the checkout's view that no exact `ViewChange` accounts for
/// invalidates every cached lookup before the guard releases the view.
pub struct SharedCheckoutGuard<'a, A, O> {
    _view: ViewWriteLease,
    state: tokio::sync::RwLockWriteGuard<'a, SharedCheckoutState<A, O>>,
}

/// One callback's shared admission to the checkout's current view.
///
/// Observations overlap freely. Each excludes every mutation until it drops,
/// because a mutation first needs the view gate's exclusive lease; reads go
/// through [`Checkout::observer`], which cannot outlive this admission.
pub(super) struct SharedCheckoutObservation<'a, A, O> {
    _view: ViewReadLease,
    state: tokio::sync::RwLockReadGuard<'a, SharedCheckoutState<A, O>>,
}

/// The same pinned observer and view admission, without a checkout-state lock.
pub(super) struct SharedCheckoutOwnedObservation<A: 'static, O: 'static> {
    pub(super) observer: crate::facade::CheckoutObserver<'static, A, O>,
    pub(super) _view: ViewReadLease,
}

/// Locked checkout state. Dereferencing reaches the canonical checkout while
/// publication helpers retain an indeterminate operation in the same lock.
pub struct SharedCheckoutState<A, O> {
    checkout: Checkout<A, O>,
    publication_operation: Option<OperationId>,
    publication_evidence: Option<NativeSourcePublicationEvidence>,
    publication: MountPublication,
    revision: Arc<AtomicU64>,
    ledger: Arc<ViewLedger>,
    /// The file table every change up to now has been recorded against.
    recorded_view: ObjectId,
    // The canonical producer's physical control witness, not a cached-size
    // claim. Checkout drops before this last alias retires its real payload.
    checkout_custody: Option<Arc<crate::kernel::ProofDataCustody>>,
}

/// The original checkout, not a candidate later rejected after an accepted CAS.
struct NativePublicationLoan<'a, A, O> {
    shared: &'a SharedCheckout<A, O>,
    original: Option<SharedCheckoutState<A, O>>,
    notification: Option<super::view_ledger::PreparedEverythingNotification>,
}

impl<A, O> NativePublicationLoan<'_, A, O> {
    fn return_original(&mut self) {
        let Some(mut original) = self.original.take() else { return };
        if self.notification.is_some() {
            let current = original.checkout.root().file_table;
            if current != original.recorded_view {
                original.recorded_view = current;
            } else {
                drop(self.notification.take());
            }
        }
        {
            let mut returned = self.shared.publication_return.lock().unwrap_or_else(PoisonError::into_inner);
            assert!(returned.is_none(), "one original publication checkout");
            *returned = Some(original);
        }
        self.shared.grouped.lock().unwrap_or_else(PoisonError::into_inner).publishing = false;
        self.shared.publication_changed.notify_waiters();
    }

    fn record_notification(&mut self) {
        if let Some(notification) = self.notification.take() {
            self.shared.ledger.record_everything_prepared(notification);
        }
    }

    fn restore(mut self) -> impl Future<Output = ()> + Send
    where A: Send + Sync, O: Send + Sync {
        self.return_original();
        async move {
            self.shared.restore_returned_checkout().await;
            self.record_notification();
        }
    }
}

impl<A, O> Drop for NativePublicationLoan<'_, A, O> {
    fn drop(&mut self) {
        self.return_original();
        self.record_notification();
    }
}

impl<A, O> SharedCheckout<A, O> {
    /// Creates the sole shared adapter boundary for one canonical checkout.
    #[must_use]
    pub fn new(checkout: Checkout<A, O>) -> Self {
        Self::with_publication(checkout, MountPublication::CloseAndSync)
    }

    pub(crate) fn with_publication_meter(
        checkout: Checkout<A, O>, publication: MountPublication,
        original: &super::NativeMountMeterWork,
    ) -> Result<Arc<Self>, MountSourceError>
    where A: Send + Sync + 'static, O: Send + Sync + 'static,
    {
        let work = original.native_materialization_work();
        work.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        let mut value = Self::with_publication_engine(checkout, publication, Some(&work))?;
        let allocation = work.claim_allocation(std::mem::size_of::<Self>() as u64, 1)
            .map_err(MountSourceError::NativeWork)?;
        value._native_allocation = Some(allocation);
        Ok(Arc::new(value))
    }

    fn with_publication_engine(
        checkout: Checkout<A, O>,
        publication: MountPublication,
        native: Option<&crate::NativeMaterializationWork>,
    ) -> Result<Self, MountSourceError> {
        let native_revision_allocation = native.map(|work|
            work.claim_allocation(std::mem::size_of::<AtomicU64>() as u64, 1)
                .map_err(MountSourceError::NativeWork)
        ).transpose()?;
        let revision = Arc::new(AtomicU64::new(1));
        let ledger = match native {
            Some(work) => ViewLedger::new_with_native_work(work)?,
            None => Arc::new(ViewLedger::new()),
        };
        let view_gate = match native {
            Some(work) => ViewGate::new_with_native_work(work)?,
            None => Arc::new(ViewGate::new()),
        };
        Ok(Self {
            state: tokio::sync::RwLock::new(SharedCheckoutState {
                recorded_view: checkout.root().file_table,
                checkout,
                publication_operation: None,
                publication_evidence: None,
                publication,
                revision: Arc::clone(&revision),
                ledger: Arc::clone(&ledger),
                checkout_custody: None,
            }),
            revision,
            ledger,
            view_gate,
            pending: StdMutex::new(Vec::new()),
            grouped: StdMutex::new(GroupedQueue::default()),
            publication_return: StdMutex::new(None),
            publication_changed: tokio::sync::Notify::new(),
            pending_allocation: StdMutex::new(None),
            _native_revision_allocation: native_revision_allocation,
            _native_allocation: None,
        })
    }

    /// Creates a shared checkout with one explicit native publication policy.
    #[must_use]
    pub fn with_publication(checkout: Checkout<A, O>, publication: MountPublication) -> Self {
        Self::with_publication_engine(checkout, publication, None)
            .expect("unbounded shared checkout construction")
    }

    /// Applies one change by group commit. Changes queued while the checkout
    /// is busy are applied together, as one mutation per hold and in queue
    /// order, by a drain that runs detached from every caller, so no
    /// caller's deadline can interrupt a group; each caller receives exactly
    /// its own change's result.
    ///
    /// Queued changes are concurrent: none was issued after another's result
    /// returned, so applying them in queue order is linearizable. A caller
    /// that stops waiting before its change is claimed abandons it, and an
    /// abandoned change never applies.
    pub(super) async fn apply_grouped(
        self: &Arc<Self>,
        change: GroupedChange,
        cancellation: &CancellationToken,
        callback_guard: Option<Arc<dyn super::NativeMountCallbackGuard>>,
    ) -> Result<GroupedOutcome, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        // A change may depend on a pending creation: apply those first.
        self.settle_pending().await;
        let (reply, outcome) = tokio::sync::oneshot::channel();
        let admission = Arc::new(GroupedAdmission::new(GROUPED_QUEUED));
        let _abandon = AbandonUnlessClaimed(Arc::clone(&admission));
        let start_drain = {
            let mut queue = self
                .grouped
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            queue.requests.push_back(GroupedRequest {
                change,
                admission,
                reply: GroupedReply::Callback(reply),
                origin: ViewOrigin::current(),
                recorded: false,
                native_callback_guard: callback_guard,
                source_lifetime: Some(cancellation.clone()),
            });
            !std::mem::replace(&mut queue.draining, true)
        };
        if start_drain {
            let checkout = Arc::clone(self);
            let cancellation = cancellation.clone();
            // The caller that starts the drain runs it in place, saving a
            // hand-off to another thread; if the caller stops waiting, the
            // drain moves to its own task and still runs to completion.
            RunToCompletion::new(async move { checkout.drain_grouped(&cancellation).await }).await;
        }
        outcome.await.map_err(|_| MountSourceError::Stale)?
    }

    /// Claims queued changes under a short checkout hold, then applies each
    /// group without shared locks. Abandoned changes are discarded unapplied.
    fn drain_grouped<'a>(&'a self, cancellation: &CancellationToken)
        -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let cancellation = cancellation.clone();
        in_heap(move || async move {
            loop {
                let checkout = self.lock_settled().await;
                let requests = {
                    let mut queue = self
                        .grouped
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let mut claimed = Vec::new();
                    while claimed.len() < MAXIMUM_GROUPED_CHANGES
                        && let Some(request) = queue.requests.pop_front()
                    {
                        if request.admission.claim() {
                            claimed.push(request);
                        }
                    }
                    if claimed.is_empty() {
                        queue.draining = false;
                        return;
                    }
                    claimed
                };
                drop(checkout);
                self.apply_grouped_requests(requests, cancellation.clone()).await;
            }
        })
    }

    /// Serializes external checkout access and excludes native callbacks,
    /// once every pending creation is applied.
    pub async fn lock(&self) -> SharedCheckoutGuard<'_, A, O>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.settle_pending().await;
        self.lock_settled().await
    }

    /// Settles queued creations with the caller's remaining work before
    /// exposing the checkout. A denied creation keeps its original queue
    /// identity and bytes for the next settlement.
    pub(crate) async fn lock_measured(
        &self,
        work: &mut crate::demand::DemandWork,
        view: &mut super::view_ledger::ViewLedgerAdmission,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<SharedCheckoutGuard<'_, A, O>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        view.reconcile(&self.ledger, work)?;
        self.settle_pending_measured(work, view, cancellation, lifetime).await?;
        self.lock_settled_measured(work, cancellation, lifetime).await
    }

    pub(super) async fn prepare_native_phase_with_carrier(
        &self,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<(CheckoutCandidate<A, O>, u64, ViewStamp), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let checkout = self.lock_with_carrier(carrier, cancellation, lifetime).await?;
        self.prepare_native_phase_from_guard(checkout, None, carrier, cancellation)
    }

    pub(super) async fn prepare_native_settled_phase_with_carrier(
        &self,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<(CheckoutCandidate<A, O>, u64, ViewStamp), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        self.prepare_native_phase_for_operation_with_carrier(
            None, carrier, cancellation, lifetime,
        ).await
    }

    pub(super) async fn prepare_native_phase_for_operation_with_carrier(
        &self,
        operation: Option<OperationId>,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<(CheckoutCandidate<A, O>, u64, ViewStamp), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let checkout = match carrier {
            NativeDemandCarrier::Borrowed(work) => self.lock_settled_measured(
                work, cancellation, lifetime,
            ).await?,
            NativeDemandCarrier::Shared(meter) => self.lock_settled_with_meter(
                meter, cancellation, lifetime,
            ).await?,
            NativeDemandCarrier::Native(original) => self.lock_settled_with_native_work(
                original, cancellation, lifetime,
            ).await?,
        };
        self.prepare_native_phase_from_guard(checkout, operation, carrier, cancellation)
    }

    fn prepare_native_phase_from_guard(
        &self,
        checkout: SharedCheckoutGuard<'_, A, O>,
        operation: Option<OperationId>,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(CheckoutCandidate<A, O>, u64, ViewStamp), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let epoch = self.view_gate.generation();
        let stamp = self.ledger.stamp();
        let candidate = checkout.candidate_for_operation_with_carrier(
            operation, carrier, cancellation,
        )?;
        // Only immutable private producer controls escape. Neither shared
        // state nor the exclusive gate is held across the SDK operation.
        drop(checkout);
        Ok((candidate, epoch, stamp))
    }

    pub(super) async fn finish_native_phase_with_carrier(
        &self,
        candidate: CheckoutCandidate<A, O>,
        epoch: u64,
        stamp: ViewStamp,
        change: &ViewChange<'_>,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<MountPublication, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        self.finish_native_phase_with_recording(
            candidate, epoch, stamp, carrier, cancellation, lifetime,
            |carrier| record_view_change_with_carrier(&self.ledger, change, carrier),
        ).await
    }

    pub(super) async fn finish_native_phase_with_recording<F>(
        &self,
        candidate: CheckoutCandidate<A, O>,
        epoch: u64,
        stamp: ViewStamp,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
        record: F,
    ) -> Result<MountPublication, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
          F: FnOnce(&mut NativeDemandCarrier<'_>) -> Result<(), MountSourceError> {
        self.finish_native_phase_optional_with_recording(
            candidate, epoch, stamp, carrier, cancellation, lifetime, record,
        ).await?.ok_or(MountSourceError::Stale)
    }

    pub(super) async fn finish_native_phase_optional_with_recording<F>(
        &self,
        candidate: CheckoutCandidate<A, O>,
        epoch: u64,
        stamp: ViewStamp,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
        record: F,
    ) -> Result<Option<MountPublication>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
          F: FnOnce(&mut NativeDemandCarrier<'_>) -> Result<(), MountSourceError> {
        self.finish_native_phase_recording_with_completion(
            candidate, epoch, stamp, carrier, cancellation, lifetime, record, None,
            |state| state.publication,
        ).await
    }

    pub(super) async fn finish_native_phase_optional_with_operation_and_recording<F>(
        &self,
        candidate: CheckoutCandidate<A, O>,
        epoch: u64,
        stamp: ViewStamp,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
        record: F,
    ) -> Result<Option<(MountPublication, OperationId)>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
          F: FnOnce(&mut NativeDemandCarrier<'_>) -> Result<(), MountSourceError> {
        self.finish_native_phase_recording_with_completion(
            candidate, epoch, stamp, carrier, cancellation, lifetime, record, None,
            |state| (state.publication, state.retained_operation_id()),
        ).await
    }

    pub(super) async fn finish_native_phase_optional_with_selected_operation_and_recording<F>(
        &self,
        candidate: CheckoutCandidate<A, O>,
        epoch: u64,
        stamp: ViewStamp,
        operation: OperationId,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
        record: F,
    ) -> Result<Option<(MountPublication, OperationId)>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
          F: FnOnce(&mut NativeDemandCarrier<'_>) -> Result<(), MountSourceError> {
        self.finish_native_phase_recording_with_completion(
            candidate, epoch, stamp, carrier, cancellation, lifetime, record, Some(operation),
            |state| {
                state.publication_operation = Some(operation);
                (state.publication, operation)
            },
        ).await
    }

    async fn finish_native_phase_recording_with_completion<F, H, T>(
        &self,
        candidate: CheckoutCandidate<A, O>,
        epoch: u64,
        stamp: ViewStamp,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
        record: F,
        selected_operation: Option<OperationId>,
        complete: H,
    ) -> Result<Option<T>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
          F: FnOnce(&mut NativeDemandCarrier<'_>) -> Result<(), MountSourceError>,
          H: FnOnce(&mut SharedCheckoutState<A, O>) -> T {
        self.finish_native_phase_commit_with_completion(
            candidate, epoch, stamp, carrier, cancellation, lifetime, record,
            |checkout, candidate, prepared| {
                let commit = || checkout.adopt_prepared_candidate(
                    &candidate.base, candidate.checkout, prepared,
                );
                match lifetime {
                    Some(lifetime) => cancellation.commit_if_both_live(lifetime, commit),
                    None => cancellation.commit_if_live(commit),
                }.map_err(|_| MountSourceError::Cancelled)
            },
            selected_operation, complete,
        ).await
    }

    pub(super) async fn finish_native_phase_with_commit<F, G>(
        &self,
        candidate: CheckoutCandidate<A, O>,
        epoch: u64,
        stamp: ViewStamp,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
        record: F,
        commit: G,
    ) -> Result<Option<MountPublication>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
          F: FnOnce(&mut NativeDemandCarrier<'_>) -> Result<(), MountSourceError>,
          G: FnOnce(
              &mut Checkout<A, O>, CheckoutCandidate<A, O>,
              crate::facade::PreparedCandidateAdoption,
          ) -> Result<bool, MountSourceError> {
        self.finish_native_phase_commit_with_completion(
            candidate, epoch, stamp, carrier, cancellation, lifetime, record, commit,
            None, |state| state.publication,
        ).await
    }

    async fn finish_native_phase_commit_with_completion<F, G, H, T>(
        &self,
        candidate: CheckoutCandidate<A, O>,
        epoch: u64,
        stamp: ViewStamp,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
        record: F,
        commit: G,
        selected_operation: Option<OperationId>,
        complete: H,
    ) -> Result<Option<T>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
          F: FnOnce(&mut NativeDemandCarrier<'_>) -> Result<(), MountSourceError>,
          G: FnOnce(
              &mut Checkout<A, O>, CheckoutCandidate<A, O>,
              crate::facade::PreparedCandidateAdoption,
          ) -> Result<bool, MountSourceError>,
          H: FnOnce(&mut SharedCheckoutState<A, O>) -> T {
        use super::lazy::{mount_demand_error, mount_engine_error};
        let original_native = carrier.original_native_work();
        carrier.with_work(|work| {
            if original_native.is_none() {
                work.retain_checkout_control(&candidate.checkout.private_control_custody())
                    .map_err(mount_demand_error)?;
            }
            work.check().map_err(mount_demand_error)?;
            if lifetime.is_some_and(CancellationToken::is_cancelled) {
                return Err(MountSourceError::Stale);
            }
            Ok::<_, MountSourceError>(())
        })?;
        if self.view_gate.generation() != epoch || self.ledger.stamp() != stamp {
            return Ok(None);
        }
        // Notification is conservative until the guarded physical install.
        // Arbitrary observers may reenter the same source, so run them before
        // acquiring either shared-state or exclusive-view locks.
        record(carrier)?;
        let mut state = match carrier {
            NativeDemandCarrier::Borrowed(work) => self.lock_settled_measured(
                work, cancellation, lifetime,
            ).await?,
            NativeDemandCarrier::Shared(meter) => self.lock_settled_with_meter(
                meter, cancellation, lifetime,
            ).await?,
            NativeDemandCarrier::Native(original) => self.lock_settled_with_native_work(
                original, cancellation, lifetime,
            ).await?,
        };
        if self.view_gate.generation() != epoch {
            return Ok(None);
        }
        if selected_operation.is_some_and(|operation| {
            state.publication_operation.is_some_and(|retained| retained != operation)
        }) {
            return Err(MountSourceError::Stale);
        }
        let prepared = if let Some(original) = original_native.as_ref() {
            state.checkout.prepare_candidate_adoption_with_native_work(
                &candidate.base, &candidate.checkout, original, cancellation,
            ).map_err(MountSourceError::Filesystem)?
        } else {
            carrier.with_work(|work| {
            let prior_live = work.live_allocation_bytes();
            let result = state.checkout.prepare_candidate_adoption_measured(
                &candidate.base, &candidate.checkout,
                work.remaining().map_err(mount_demand_error)?, cancellation,
            );
            match result {
                Ok(receipt) => {
                    work.merge_with_prior_live(receipt.work, prior_live).map_err(mount_demand_error)?;
                    Ok(receipt.value)
                }
                Err(failure) => {
                    work.merge_with_prior_live(*failure.work, prior_live).map_err(mount_demand_error)?;
                    Err(mount_engine_error(failure.error))
                }
            }
            })?
        };
        let Some(prepared) = prepared else { return Ok(None); };
        let token = candidate.checkout.private_control_custody();
        if original_native.is_none() {
            carrier.with_work(|work| {
                work.retain_checkout_control(&token).map_err(mount_demand_error)
            })?;
        }
        let old_token = state.checkout_custody.replace(Arc::clone(&token));
        let adopted = commit(&mut state.checkout, candidate, prepared);
        if !matches!(adopted, Ok(true)) {
            state.checkout_custody = old_token;
            return adopted.map(|_| None);
        }
        // The old physical checkout has dropped before its custody alias.
        drop(old_token);
        state.recorded_view = state.checkout.root().file_table;
        state.revision.fetch_add(1, Ordering::AcqRel);
        Ok(Some(complete(&mut state)))
    }

    pub(super) async fn change_content_by_id_with_carrier(
        &self,
        file_id: FileId,
        change: ContentChange<FileId>,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
        callback_guard: Option<&Arc<dyn super::NativeMountCallbackGuard>>,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        use super::lazy::{mount_demand_error, mount_engine_error};
        let (mut candidate, epoch, stamp) = self.prepare_native_phase_with_carrier(
            carrier, cancellation, lifetime,
        ).await?;
        let times = ContentTimes::Stamp(content_change_time()?);
        let accepted = if let Some(original) = carrier.original_native_work() {
            candidate.checkout.change_content_by_id_with_native_work(
                file_id, change, times, &original, cancellation,
            ).await.map_err(MountSourceError::Filesystem)
        } else {
            let (budget, prior_live) = carrier.with_work(|work| {
                Ok::<_, MountSourceError>((
                    work.remaining().map_err(mount_demand_error)?, work.live_allocation_bytes(),
                ))
            })?;
            let result = candidate.checkout.change_content_by_id(
                file_id, change, times, budget, cancellation,
            ).await;
            carrier.accept_with_prior_live(result, prior_live, mount_engine_error)
        };
        let installed = self.finish_native_phase_with_carrier(
            candidate, epoch, stamp, &ViewChange::Node(file_id), carrier, cancellation, lifetime,
        ).await;
        accepted?;
        installed?;
        self.publish_mutation_with_carrier(carrier, cancellation, lifetime, callback_guard).await
    }

    pub(super) async fn settle_with_carrier(
        &self,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let checkout = self.lock_with_carrier(carrier, cancellation, lifetime).await?;
        checkout.ensure_publication_resolved()?;
        carrier.with_work(|work| work.check()).map_err(super::lazy::mount_demand_error)
    }

    pub(super) async fn write_sparse_range_by_id_with_carrier(
        &self,
        file_id: FileId,
        offset: u64,
        range: &super::MountSparseRange,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
        callback_guard: Option<&Arc<dyn super::NativeMountCallbackGuard>>,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
        use super::lazy::{mount_demand_error, mount_engine_error, mount_items};
        let original = carrier.original_native_work();
        let Some(end) = offset.checked_add(range.logical_bytes) else {
            if original.is_some() {
                return Err(MountSourceError::NativeStaticText {
                    kind: super::NativeSourceTextKind::Invalid,
                    text: "sparse range overflows the file address space",
                });
            }
            return carrier.with_work(|work| measured_native_name_error(
                "sparse range overflows the file address space", true, work,
            ));
        };
        let record = {
            let observation = self.observe_owned_with_carrier(
                ViewGate::callback_owner(), carrier, cancellation, lifetime,
            ).await?;
            let _view = observation._view;
            let mut observer = LookupObserverOwned::new_in(
                observation.observer, carrier.reborrow(),
            )?;
            let accepted = if let Some(original) = &original {
                observer.read_file_record_by_id_with_native_work(
                    file_id, original, cancellation,
                ).await.map_err(mount_engine_error)
            } else {
                let (budget, prior_live) = observer.work.with_work(|work| {
                    Ok::<_, MountSourceError>((
                        work.remaining().map_err(mount_demand_error)?, work.live_allocation_bytes(),
                    ))
                })?;
                let result = observer.read_file_record_by_id(file_id, budget, cancellation).await;
                let accepted = observer.work.accept_with_prior_live(result, prior_live, mount_engine_error);
                observer.receipt_delivered();
                accepted
            };
            let finished = observer.finish();
            let record = accepted?;
            finished?;
            record
        };
        if lifetime.is_some_and(CancellationToken::is_cancelled) {
            return Err(MountSourceError::Stale);
        }
        if end > mount_node(record).logical_bytes {
            self.change_content_by_id_with_carrier(
                file_id, ContentChange::Resize { logical_bytes: end }, carrier, cancellation, lifetime,
                callback_guard,
            ).await?;
        }
        if lifetime.is_some_and(CancellationToken::is_cancelled) {
            return Err(MountSourceError::Stale);
        }
        self.change_content_by_id_with_carrier(
            file_id,
            range_allocation(ByteRange { offset, length: range.logical_bytes }, MountRangeAllocation::PunchHole),
            carrier, cancellation, lifetime,
            callback_guard,
        ).await?;
        for span in &range.spans {
            if let Some(original) = &original {
                original.charge(crate::WorkCounters {
                    items_examined: 1, ..crate::WorkCounters::UNCHARGED
                }).map_err(MountSourceError::NativeWork)?;
            } else {
                carrier.with_work(|work| mount_items(work, 1))?;
            }
            if lifetime.is_some_and(CancellationToken::is_cancelled) {
                return Err(MountSourceError::Stale);
            }
            let Some(destination) = offset.checked_add(span.relative_offset) else {
                if original.is_some() {
                    return Err(MountSourceError::NativeStaticText {
                        kind: super::NativeSourceTextKind::Invalid,
                        text: "sparse span offset overflows",
                    });
                }
                return carrier.with_work(|work| measured_native_name_error(
                    "sparse span offset overflows", true, work,
                ));
            };
            let copied = crate::WorkCounters {
                bytes_copied: std::mem::size_of::<OwnedBytes>() as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            if let Some(original) = &original {
                original.charge(copied).map_err(MountSourceError::NativeWork)?;
            } else {
                carrier.with_work(|work| work.charge(copied).map_err(mount_demand_error))?;
            }
            self.change_content_by_id_with_carrier(
                file_id, ContentChange::Write { offset: destination, bytes: span.bytes.clone() },
                carrier, cancellation, lifetime,
                callback_guard,
            ).await?;
        }
        if lifetime.is_some_and(CancellationToken::is_cancelled) {
            return Err(MountSourceError::Stale);
        }
        if let Some(original) = &original {
            original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)
        } else {
            carrier.with_work(|work| work.check()).map_err(mount_demand_error)
        }
    }

    pub(super) async fn lock_with_carrier(
        &self,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<SharedCheckoutGuard<'_, A, O>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        match carrier {
            NativeDemandCarrier::Borrowed(work) => self.lock_measured(
                work, &mut super::view_ledger::ViewLedgerAdmission::default(),
                cancellation, lifetime,
            ).await,
            NativeDemandCarrier::Shared(meter) => {
                self.settle_pending_with_meter(meter, cancellation, lifetime).await?;
                self.lock_settled_with_meter(meter, cancellation, lifetime).await
            }
            NativeDemandCarrier::Native(original) => {
                self.settle_pending_with_native_work(original, cancellation, lifetime).await?;
                self.lock_settled_with_native_work(original, cancellation, lifetime).await
            }
        }
    }

    /// Changes metadata and optional size together on the bound file identity.
    pub(super) async fn set_attributes_by_id_with_carrier(
        &self,
        file_id: FileId,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
        callback_guard: Option<&Arc<dyn super::NativeMountCallbackGuard>>,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        use super::lazy::{mount_demand_error, mount_engine_error};
        let (mut candidate, epoch, stamp) = self.prepare_native_phase_with_carrier(
            carrier, cancellation, lifetime,
        ).await?;
        let accepted = if let Some(original) = carrier.original_native_work() {
            candidate.checkout.set_attributes_by_id_with_native_work(
                file_id, metadata, logical_bytes, &original, cancellation,
            ).await.map_err(MountSourceError::Filesystem)
        } else {
            let (budget, prior_live) = carrier.with_work(|work| {
                Ok::<_, MountSourceError>((
                    work.remaining().map_err(mount_demand_error)?, work.live_allocation_bytes(),
                ))
            })?;
            let result = candidate.checkout.set_attributes_by_id(
                file_id, metadata, logical_bytes, budget, cancellation,
            ).await;
            carrier.accept_with_prior_live(result, prior_live, mount_engine_error)
        };
        let installed = self.finish_native_phase_with_carrier(
            candidate, epoch, stamp, &ViewChange::Node(file_id), carrier, cancellation, lifetime,
        ).await;
        accepted?;
        installed?;
        self.publish_mutation_with_carrier(carrier, cancellation, lifetime, callback_guard).await
    }

    /// Consumes an already admitted name; the caller retains payload custody.
    pub(super) async fn mutate_named_attribute_by_id_with_carrier(
        &self,
        file_id: FileId,
        name: AttributeName,
        value: Option<(OwnedBytes, NamedAttributeWriteMode)>,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
        callback_guard: Option<&Arc<dyn super::NativeMountCallbackGuard>>,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        use super::lazy::{mount_demand_error, mount_engine_error};
        let bytes = match &*carrier {
            NativeDemandCarrier::Native(_) => 0,
            NativeDemandCarrier::Borrowed(_) | NativeDemandCarrier::Shared(_) => name.retained_bytes() as u64,
        };
        let mut input = NativeCarrierOwned { value: Some((name, value)), carrier, bytes };
        let (mut candidate, epoch, stamp) = self.prepare_native_phase_with_carrier(
            input.carrier, cancellation, lifetime,
        ).await?;
        let map_error = |error| match error {
            FsError::CreationRejected | FsError::NotFound | FsError::Mutation(
                GenerationMutationError::AlreadyExists |
                GenerationMutationError::MissingParent |
                GenerationMutationError::MissingSource,
            ) => fs_error(error),
            other => mount_engine_error(other),
        };
        let accepted = if let Some(original) = input.carrier.original_native_work() {
            let (name, value) = input.value.as_ref().expect("native attribute input is live");
            match value {
                Some((value, mode)) => candidate.checkout.write_named_attribute_by_id_with_native_work(
                    file_id, name, value, *mode, &original, cancellation,
                ).await,
                None => candidate.checkout.remove_named_attribute_by_id_with_native_work(
                    file_id, name, &original, cancellation,
                ).await,
            }.map_err(MountSourceError::Filesystem)
        } else {
            let (budget, prior_live) = input.carrier.with_work(|work| {
                Ok::<_, MountSourceError>((
                    work.remaining().map_err(mount_demand_error)?, work.live_allocation_bytes(),
                ))
            })?;
            let (name, value) = input.value.take().expect("native attribute input is live");
            let result = match value {
                Some((value, mode)) => candidate.checkout.write_named_attribute_by_id(
                    file_id, name, value, mode, budget, cancellation,
                ).await,
                None => candidate.checkout.remove_named_attribute_by_id(
                    file_id, name, budget, cancellation,
                ).await,
            };
            input.carrier.accept_with_prior_live(result, prior_live, map_error)
        };
        drop(input);
        let installed = self.finish_native_phase_with_carrier(
            candidate, epoch, stamp, &ViewChange::Node(file_id), carrier, cancellation, lifetime,
        ).await;
        accepted?;
        installed?;
        self.publish_mutation_with_carrier(carrier, cancellation, lifetime, callback_guard).await
    }

    async fn lock_settled(&self) -> SharedCheckoutGuard<'_, A, O> {
        loop {
            let notified = self.publication_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.publication_in_flight() {
                notified.await;
                continue;
            }
            let view = self.view_gate.write().await;
            let mut state = self.state.write().await;
            if self.publication_in_flight() {
                drop(state);
                drop(view);
                notified.await;
                continue;
            }
            self.restore_returned_into(&mut state);
            return SharedCheckoutGuard { _view: view, state };
        }
    }

    pub(super) fn publication_in_flight(&self) -> bool {
        self.grouped.lock().unwrap_or_else(PoisonError::into_inner).publishing
    }

    fn restore_returned_into(&self, state: &mut SharedCheckoutState<A, O>) {
        let returned = self.publication_return.lock().unwrap_or_else(PoisonError::into_inner).take();
        if let Some(original) = returned {
            let projection = std::mem::replace(state, original);
            drop(projection);
        }
    }

    async fn restore_returned_checkout(&self) {
        if self.publication_return.lock().unwrap_or_else(PoisonError::into_inner).is_some() {
            let mut state = self.state.write().await;
            self.restore_returned_into(&mut state);
        }
    }

    pub(super) async fn read_current_state(&self) -> tokio::sync::RwLockReadGuard<'_, SharedCheckoutState<A, O>> {
        loop {
            self.restore_returned_checkout().await;
            let state = self.state.read().await;
            if self.publication_return.lock().unwrap_or_else(PoisonError::into_inner).is_none() {
                return state;
            }
            drop(state);
        }
    }

    async fn publication_loan_with_carrier(
        &self, carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken, lifetime: Option<&CancellationToken>,
    ) -> Result<NativePublicationLoan<'_, A, O>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        use super::lazy::{mount_demand_error, mount_engine_error};
        let mut checkout = await_measured_lock_with_carrier(
            self.lock_settled(), carrier, cancellation, lifetime,
        ).await?;
        let native = carrier.original_native_work();
        let projection = if let Some(native) = &native {
            checkout.checkout.private_candidate_with_native_work(native, cancellation)
                .map_err(MountSourceError::Filesystem)?
        } else {
            let (budget, prior_live) = carrier.with_work(|work| {
                Ok::<_, MountSourceError>((
                    work.remaining().map_err(mount_demand_error)?, work.live_allocation_bytes(),
                ))
            })?;
            let projected = checkout.checkout.private_candidate_measured(budget, cancellation);
            carrier.accept_with_prior_live(projected, prior_live, mount_engine_error)?
        };
        let projection_custody = projection.private_control_custody();
        if native.is_none() {
            carrier.with_work(|work| work.retain_checkout_control(&projection_custody))
                .map_err(mount_demand_error)?;
        }
        if lifetime.is_some_and(CancellationToken::is_cancelled) { return Err(MountSourceError::Stale); }
        carrier.with_work(|work| work.check()).map_err(mount_demand_error)?;
        let projected_state = SharedCheckoutState {
            recorded_view: projection.root().file_table,
            checkout: projection,
            publication_operation: checkout.publication_operation,
            publication_evidence: checkout.publication_evidence.clone(),
            publication: checkout.publication,
            revision: Arc::clone(&checkout.revision),
            ledger: Arc::clone(&checkout.ledger),
            checkout_custody: Some(projection_custody),
        };
        self.grouped.lock().unwrap_or_else(PoisonError::into_inner).publishing = true;
        let original = std::mem::replace(&mut *checkout.state, projected_state);
        let loan = NativePublicationLoan {
            shared: self, original: Some(original), notification: None,
        };
        drop(checkout);
        Ok(loan)
    }

    pub(super) async fn resolve_acknowledged_original_with_native_work<F, Fut>(
        &self, operation: OperationId, generation: crate::GenerationId,
        mounted: &super::NativeMountMeterWork,
        cancellation: &CancellationToken, lifetime: Option<&CancellationToken>,
        mut before_publication: F,
    ) -> Result<bool, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), MountSourceError>> + Send + 'static,
    {
        let meter = mounted.meter();
        let mut carrier = NativeDemandCarrier::Native(mounted);
        let mut loan = self.publication_loan_with_carrier(&mut carrier, cancellation, lifetime).await?;
        if loan.original.as_ref().expect("original publication checkout").publication_operation != Some(operation) {
            loan.restore().await;
            return Ok(false);
        }
        match self.ledger.prepare_everything_notification(meter) {
            Ok(notification) => loan.notification = Some(notification),
            Err(error) => {
                loan.restore().await;
                return Err(error);
            }
        }
        let native = mounted.native_materialization_work();
        let original = loan.original.as_mut().expect("original publication checkout");
        let producer = original.checkout.private_control_custody();
        let (original_fingerprint_permit, operation_context, operation_evidence) = match &original.publication_evidence {
            Some(retained) => (Some(retained.original_permit), Some(retained.operation_context), Some(retained.operation_evidence.clone())),
            None => (None, None, None),
        };
        let mut guarded = move |work: &crate::NativeMaterializationWork| {
            let future = before_publication(work);
            async move { future.await.map_err(NativeSealError::Guard) }
        };
        let result = original.checkout.retry_stale_commit_guarded_with_native_work(
            operation, crate::PublicationPermit::Unrestricted, &native, cancellation,
            &mut guarded, Some(&producer), operation_context, operation_evidence, original_fingerprint_permit,
        ).await.map_err(|error| match error {
            NativeSealError::Guard(error) => error,
            NativeSealError::Filesystem(error) => MountSourceError::Filesystem(error),
        });
        let result = result.map(|outcome| {
            let resolved = matches!(outcome,
                Some(crate::CheckoutCommitOutcome::Committed { generation_id, .. }
                    | crate::CheckoutCommitOutcome::AlreadyCommitted { generation_id, .. })
                if generation_id == generation);
            if resolved { original.clear_retained_operation(operation); }
            resolved
        });
        loan.restore().await;
        result
    }

    pub(super) async fn seal_original_with_carrier_guarded<F, Fut, E>(
        &self, permit: crate::PublicationPermit, force: bool,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken, lifetime: Option<&CancellationToken>,
        before_publication: &mut F,
    ) -> Result<(), E>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        self.seal_original_with_carrier_guarded_evidence(
            permit, force, carrier, cancellation, lifetime, before_publication, None, None,
        ).await.map(|_| ())
    }

    pub(super) async fn seal_original_with_carrier_guarded_evidence<F, Fut, E>(
        &self, permit: crate::PublicationPermit, force: bool,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken, lifetime: Option<&CancellationToken>,
        before_publication: &mut F,
        operation_context: Option<crate::Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
    ) -> Result<NativeSourcePublicationAck, E>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        let mut acknowledged = None;
        let mut accounting_failure = None;
        self.seal_original_with_carrier_guarded_context(
            permit, force, carrier, cancellation, lifetime, before_publication,
            operation_context, operation_evidence, None, &mut acknowledged, &mut accounting_failure,
        ).await.map(|(ack, _)| ack)
    }

    async fn seal_original_with_carrier_guarded_context<F, Fut, E>(
        &self, permit: crate::PublicationPermit, force: bool,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken, lifetime: Option<&CancellationToken>,
        before_publication: &mut F,
        operation_context: Option<crate::Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
        snapshot_context: Option<crate::lazy_workspace::NativeSnapshotContext>,
        acknowledged: &mut Option<crate::lazy_workspace::NativePublicationSnapshot>,
        accounting_failure: &mut Option<MountSourceError>,
    ) -> Result<(NativeSourcePublicationAck, Option<crate::lazy_workspace::NativePublicationSnapshot>), E>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        match carrier {
            NativeDemandCarrier::Borrowed(work) => BorrowedNativeDemandMeter::new(work)?
                .run_async(|meter| async move {
                    self.seal_original_on_meter_guarded(
                        permit, force, &meter, cancellation, lifetime, before_publication,
                        operation_context, operation_evidence, snapshot_context, acknowledged, accounting_failure,
                    ).await
                }).await,
            NativeDemandCarrier::Shared(meter) => self.seal_original_on_meter_guarded(
                permit, force, meter, cancellation, lifetime, before_publication,
                operation_context, operation_evidence, snapshot_context, acknowledged, accounting_failure,
            ).await,
            NativeDemandCarrier::Native(original) => self.seal_original_on_native_work_guarded(
                permit, force, original, cancellation, lifetime, before_publication,
                operation_context, operation_evidence, snapshot_context, acknowledged, accounting_failure,
            ).await,
        }
    }

    async fn seal_original_on_meter_guarded<F, Fut, E>(
        &self, permit: crate::PublicationPermit, force: bool,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
        cancellation: &CancellationToken, lifetime: Option<&CancellationToken>,
        before_publication: &mut F,
        operation_context: Option<crate::Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
        snapshot_context: Option<crate::lazy_workspace::NativeSnapshotContext>,
        acknowledged: &mut Option<crate::lazy_workspace::NativePublicationSnapshot>,
        accounting_failure: &mut Option<MountSourceError>,
    ) -> Result<(NativeSourcePublicationAck, Option<crate::lazy_workspace::NativePublicationSnapshot>), E>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        let original = super::NativeMountMeterWork::from_meter(Arc::clone(meter));
        self.seal_original_on_native_work_guarded(
            permit, force, &original, cancellation, lifetime, before_publication,
            operation_context, operation_evidence, snapshot_context, acknowledged, accounting_failure,
        ).await
    }

    async fn seal_original_on_native_work_guarded<F, Fut, E>(
        &self, permit: crate::PublicationPermit, force: bool,
        mounted: &super::NativeMountMeterWork,
        cancellation: &CancellationToken, lifetime: Option<&CancellationToken>,
        before_publication: &mut F,
        operation_context: Option<crate::Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
        snapshot_context: Option<crate::lazy_workspace::NativeSnapshotContext>,
        acknowledged: &mut Option<crate::lazy_workspace::NativePublicationSnapshot>,
        accounting_failure: &mut Option<MountSourceError>,
    ) -> Result<(NativeSourcePublicationAck, Option<crate::lazy_workspace::NativePublicationSnapshot>), E>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        use super::lazy::mount_demand_error;
        let meter = mounted.meter();
        let mut carrier = NativeDemandCarrier::Native(mounted);
        let mut loan = self.publication_loan_with_carrier(&mut carrier, cancellation, lifetime).await?;
        match self.ledger.prepare_everything_notification(meter) {
            Ok(notification) => loan.notification = Some(notification),
            Err(error) => {
                loan.restore().await;
                return Err(E::from(error));
            }
        };
        let mut owned_guard = before_publication.clone();
        let result = {
            let loan = &mut loan;
            let carrier = &mut carrier;
            async move {
                let native = mounted.native_materialization_work();
                carrier.with_work(|work| work.check()).map_err(mount_demand_error)?;
                if lifetime.is_some_and(CancellationToken::is_cancelled) {
                    return Err(E::from(MountSourceError::Stale));
                }
                let original = loan.original.as_mut().expect("original publication checkout");
                let operation = original.retained_operation_id();
                if original.publication_evidence.is_none()
                    && (force || original.checkout.has_pending_mutations())
                    && let Some(context) = snapshot_context
                {
                    let (operation_context, operation_evidence) =
                        crate::lazy_workspace::native_publication_evidence_with_native_work(
                            context, operation, permit, &native, cancellation,
                        ).map_err(super::lazy::mount_engine_error).map_err(E::from)?;
                    original.publication_evidence = Some(NativeSourcePublicationEvidence {
                        operation_evidence, operation_context, context, original_permit: permit,
                    });
                }
                let (operation_context, operation_evidence) = match &original.publication_evidence {
                    Some(retained) => (Some(retained.operation_context), Some(retained.operation_evidence.clone())),
                    None => (operation_context, operation_evidence),
                };
                let retained_context = original.publication_evidence.as_ref().map(|retained| retained.context);
                let original_fingerprint_permit = original.publication_evidence.as_ref().map(|retained| retained.original_permit);
                let producer = original.checkout.private_control_custody();
                let mut guarded = move |work: &crate::NativeMaterializationWork| {
                    let future = owned_guard(work);
                    async move { future.await.map_err(NativeSealError::Guard) }
                };
                let future = super::publication::seal_checkout_with_permit_guarded_with_native_work(
                    &mut original.checkout, operation, permit, force, &native,
                    cancellation, &mut guarded, Some(&producer), operation_context, operation_evidence, original_fingerprint_permit,
                );
                let result = future.await;
                match result {
                    Ok(ack) => {
                        let snapshot = ack.and_then(|ack| match ack {
                            NativeSourcePublicationAck::Unchanged { generation } => retained_context.or(snapshot_context)
                                .map(|context| crate::lazy_workspace::NativePublicationSnapshot::unchanged(context, generation)),
                            NativeSourcePublicationAck::Published { operation, generation } => retained_context
                                .map(|context| crate::lazy_workspace::NativePublicationSnapshot::published(context, operation, generation)),
                        });
                        if matches!(ack, Some(NativeSourcePublicationAck::Published { .. })) {
                            *acknowledged = snapshot;
                        }
                        if ack.is_some() || original.checkout.mode().mutations == crate::model::MutationMode::PrivateOverlay {
                            original.clear_retained_operation(operation);
                        }
                        ack.map(|ack| (ack, snapshot)).ok_or_else(|| E::from(MountSourceError::Stale))
                    }
                    Err(NativeSealError::Guard(error)) => Err(error),
                    Err(NativeSealError::Filesystem(error)) => Err(E::from(match error {
                        crate::FsError::NativeKernel(error) => MountSourceError::NativeWork(error),
                        crate::FsError::Work(error) => MountSourceError::Work(error),
                        crate::FsError::Cancelled(_) => MountSourceError::Cancelled,
                        error => MountSourceError::Filesystem(error),
                    })),
                }
            }.await
        };
        loan.restore().await;
        let reconciled = carrier.with_work(|work| work.record(crate::WorkCounters::UNCHARGED))
            .map_err(mount_demand_error);
        match (result, reconciled) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(primary), Ok(())) => Err(primary),
            (Ok(_), Err(error)) => Err(E::from(error)),
            (Err(primary), Err(error)) => {
                *accounting_failure = Some(error);
                Err(primary)
            }
        }
    }

    pub(super) async fn publish_mutation_with_carrier(
        &self, carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken, lifetime: Option<&CancellationToken>,
        callback_guard: Option<&Arc<dyn super::NativeMountCallbackGuard>>,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        self.publish_mutation_with_carrier_guarded(
            carrier, cancellation, lifetime, source_publication_guard(
                callback_guard.map(Arc::clone), lifetime.cloned(),
            ),
        ).await
    }

    async fn publish_mutation_with_carrier_guarded<F, Fut>(
        &self, carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken, lifetime: Option<&CancellationToken>,
        mut before_publication: F,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), MountSourceError>> + Send + 'static,
    {
        let publication = await_measured_lock_with_carrier(
            self.read_current_state(), carrier, cancellation, lifetime,
        ).await?.publication;
        if publication == MountPublication::PerMutation {
            self.seal_original_with_carrier_guarded(
                crate::PublicationPermit::Unrestricted, false, carrier, cancellation, lifetime,
                &mut before_publication,
            ).await?;
        }
        Ok(())
    }

    async fn lock_settled_measured(
        &self,
        work: &mut crate::demand::DemandWork,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<SharedCheckoutGuard<'_, A, O>, MountSourceError> {
        await_measured_lock(self.lock_settled(), work, cancellation, lifetime).await
    }

    pub(super) async fn lock_settled_with_meter(
        &self, meter: &Arc<crate::demand::NativeDemandReadMeter>,
        cancellation: &CancellationToken, lifetime: Option<&CancellationToken>,
    ) -> Result<SharedCheckoutGuard<'_, A, O>, MountSourceError> {
        await_measured_lock_with_meter(self.lock_settled(), meter, cancellation, lifetime).await
    }

    pub(super) fn lock_settled_with_native_work<'a>(
        &'a self, original: &super::NativeMountMeterWork,
        cancellation: &CancellationToken, lifetime: Option<&CancellationToken>,
    ) -> impl Future<Output = Result<SharedCheckoutGuard<'a, A, O>, MountSourceError>> + Send + 'a + use<'a, A, O>
    where A: Send + Sync, O: Send + Sync {
        let original = original.clone();
        let cancellation = cancellation.clone();
        let lifetime = lifetime.cloned();
        async move {
            await_measured_lock_with_carrier(
                self.lock_settled(), &mut NativeDemandCarrier::Native(&original), &cancellation, lifetime.as_ref(),
            ).await
        }
    }

    /// Records `pending` as created: its name changed now, though its
    /// creation is applied later (see [`PendingCreate`]).
    /// Registers a pending creation, atomically with checking that no other
    /// pending creation holds its path and, given `absent_since`, that
    /// nothing rebound the path since it was found absent. Answers whether
    /// it registered.
    fn hold_pending_with_carrier(
        &self,
        pending: Arc<PendingCreate>,
        absent_since: Option<ViewStamp>,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<bool, MountSourceError> {
        use super::lazy::{mount_demand_error, mount_items, mount_vec};
        let mut held = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        for other in held.iter() {
            let same = carrier.with_work(|work| {
                mount_items(work, 1)?;
                if other.path.depth() != pending.path.depth() { return Ok(false); }
                for (left, right) in other.path.components().iter().zip(pending.path.components()) {
                    mount_items(work, 1)?;
                    if left != right { return Ok(false); }
                }
                Ok::<_, MountSourceError>(true)
            })?;
            if same { return Ok(false); }
        }
        if let Some(stamp) = absent_since {
            let unchanged = self.view_gate.is_stable() && carrier.with_work(|work| {
                self.ledger.binding_unchanged_since(&pending.path, stamp, work)
            })?;
            if !unchanged { return Ok(false); }
        }
        if held.len() == held.capacity() {
            let count = held.capacity().max(1).checked_mul(2)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            let (mut replacement, custody) = carrier.with_work(|work| {
                let custody = super::NativeMountAllocationCustody::new(work)
                    .map_err(super::lazy::mount_demand_error)?;
                let copied = crate::WorkCounters {
                    bytes_copied: (held.len() * std::mem::size_of::<Arc<PendingCreate>>()) as u64,
                    items_examined: held.len() as u64,
                    ..crate::WorkCounters::UNCHARGED
                };
                work.work.admit(&copied, &work.budget).map_err(MountSourceError::Work)?;
                let vector = mount_vec::<Arc<PendingCreate>>(count, work)?;
                let bytes = (vector.capacity() * std::mem::size_of::<Arc<PendingCreate>>()) as u64;
                if let Err(error) = custody.retain_admitted(bytes, work) {
                    drop(vector);
                    work.release(bytes).map_err(mount_demand_error)?;
                    return Err(super::lazy::mount_demand_error(error));
                }
                work.record(copied).map_err(mount_demand_error)?;
                Ok::<_, MountSourceError>((vector, custody))
            })?;
            replacement.append(&mut held);
            let old = std::mem::replace(&mut *held, replacement);
            drop(old);
            *self.pending_allocation.lock().unwrap_or_else(PoisonError::into_inner) = Some(custody);
        }
        carrier.with_work(|work| work.charge(crate::WorkCounters {
            bytes_copied: std::mem::size_of::<Arc<PendingCreate>>() as u64,
            ..crate::WorkCounters::UNCHARGED
        }).map_err(mount_demand_error))?;
        let (slot, mut admission) = carrier.with_work(|work| self.ledger.take_for_work(work))?;
        let recorded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _origin = pending.origin.enter();
            self.ledger.record(&ViewChange::Bound(&pending.path), carrier, &mut admission)
        }));
        carrier.with_work(|work| work.restore_view_ledger_admission(slot, admission));
        match recorded {
            Err(panic) => std::panic::resume_unwind(panic),
            Ok(result) => result?,
        }
        self.revision.fetch_add(1, Ordering::AcqRel);
        held.push(pending);
        Ok(true)
    }

    /// The pending creation of `path`, reported as it stands.
    fn pending_lookup(&self, path: &NamespacePath) -> Option<MountLookup> {
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|pending| pending.path == *path)
            .find_map(|pending| pending.lookup())
    }

    fn pending_lookup_with_work(
        &self,
        path: &NamespacePath,
        work: &mut crate::demand::DemandWork,
    ) -> Result<Option<MountLookup>, MountSourceError> {
        use super::lazy::{mount_demand_error, mount_items};
        let held = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        for pending in held.iter() {
            mount_items(work, 1)?;
            if pending.path.depth() != path.depth() { continue; }
            let mut same = true;
            for (left, right) in pending.path.components().iter().zip(path.components()) {
                mount_items(work, 1)?;
                if left != right { same = false; break; }
            }
            if !same { continue; }
            let copied = crate::WorkCounters {
                bytes_copied: std::mem::size_of::<MountLookup>() as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            work.work.admit(&copied, &work.budget).map_err(MountSourceError::Work)?;
            if let Some(lookup) = pending.lookup() {
                work.record(copied).map_err(mount_demand_error)?;
                work.check().map_err(mount_demand_error)?;
                return Ok(Some(lookup));
            }
        }
        Ok(None)
    }

    fn pending_create(&self, file_id: FileId) -> Option<Arc<PendingCreate>> {
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .find(|pending| pending.file_id == file_id)
            .cloned()
    }

    /// Applies pending creations through the same measured engine, with no
    /// finite work bound for ordinary native callbacks.
    async fn settle_pending(&self)
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let cancellation = CancellationToken::new();
        let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let mut view = super::view_ledger::ViewLedgerAdmission::default();
        let _ = self.settle_pending_measured(&mut work, &mut view, &cancellation, None).await;
    }

    /// Keeps the original queue and each creation's original owned buffer
    /// until the bounded engine has either installed or restored its result.
    async fn settle_pending_measured(
        &self, work: &mut crate::demand::DemandWork,
        view: &mut super::view_ledger::ViewLedgerAdmission,
        cancellation: &CancellationToken, lifetime: Option<&CancellationToken>,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
    {
        self.settle_pending_with_carrier(
            &mut NativeDemandCarrier::Borrowed(work), view, cancellation, lifetime,
        ).await
    }

    pub(super) async fn settle_pending_with_meter(
        &self, meter: &Arc<crate::demand::NativeDemandReadMeter>,
        cancellation: &CancellationToken, lifetime: Option<&CancellationToken>,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
    {
        self.settle_pending_with_carrier(
            &mut NativeDemandCarrier::Shared(meter), &mut Default::default(), cancellation, lifetime,
        ).await
    }

    pub(super) async fn settle_pending_with_native_work(
        &self, original: &super::NativeMountMeterWork,
        cancellation: &CancellationToken, lifetime: Option<&CancellationToken>,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        self.settle_pending_with_carrier(
            &mut NativeDemandCarrier::Native(original), &mut Default::default(), cancellation, lifetime,
        ).await
    }

    async fn settle_pending_with_carrier(
        &self, carrier: &mut NativeDemandCarrier<'_>,
        _view: &mut super::view_ledger::ViewLedgerAdmission,
        cancellation: &CancellationToken, lifetime: Option<&CancellationToken>,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
    {
        use super::lazy::{mount_demand_error, mount_items, mount_vec};
        cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        if lifetime.is_some_and(CancellationToken::is_cancelled) {
            return Err(MountSourceError::Stale);
        }
        if let Some(original) = carrier.original_native_work() {
            original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        } else {
            carrier.with_work(|work| work.check()).map_err(mount_demand_error)?;
        }
        if self.pending.lock().unwrap_or_else(PoisonError::into_inner).is_empty() { return Ok(()); }
        let checkout = await_measured_lock_with_carrier(
            self.lock_settled(), carrier, cancellation, lifetime,
        ).await?;
        struct PreparedBytes<'a, 'w> {
            carrier: &'a mut NativeDemandCarrier<'w>,
            bytes: u64,
            pending: &'a StdMutex<Vec<Arc<PendingCreate>>>,
            cleanup_reserved: u64,
            scan: bool,
            native_cleanup: Option<crate::materializer::NativeItemReservation>,
            native_allocation: Option<crate::NativeAllocation>,
        }
        impl PreparedBytes<'_, '_> {
            fn finish_cleanup(&mut self, examined: u64, deferred: bool) -> Result<(), MountSourceError> {
                self.scan = false;
                let reserved = std::mem::take(&mut self.cleanup_reserved);
                if let Some(mut cleanup) = self.native_cleanup.take() {
                    if deferred {
                        cleanup.consume_deferred(examined);
                        return Ok(());
                    }
                    return cleanup.consume(examined).map_err(MountSourceError::NativeWork);
                }
                if reserved == 0 && examined == 0 { return Ok(()); }
                self.carrier.with_work(|work| {
                    work.budget.items_examined = work.budget.items_examined.checked_add(reserved)
                        .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
                    work.record(crate::WorkCounters {
                        items_examined: examined, ..crate::WorkCounters::UNCHARGED
                    }).map_err(mount_demand_error)
                })
            }

            fn cleanup(&mut self, deferred: bool) -> Result<(), MountSourceError> {
                if !self.scan { return self.finish_cleanup(0, deferred); }
                let examined = {
                    let mut held = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
                    let count = held.len().min(self.cleanup_reserved as usize);
                    let mut terminal = true;
                    for create in &held[..count] {
                        terminal &= matches!(&*create.state(), PendingState::Applied | PendingState::Failed);
                    }
                    if terminal && count == held.len() { held.clear(); }
                    count as u64
                };
                self.finish_cleanup(examined, deferred)
            }

            fn release(&mut self, deferred: bool) -> Result<(), MountSourceError> {
                let cleanup = self.cleanup(deferred);
                let bytes = std::mem::take(&mut self.bytes);
                let released = if self.carrier.original_native_work().is_some() {
                    drop(self.native_allocation.take());
                    Ok(())
                } else {
                    self.carrier.with_work(|work| work.release(bytes)).map_err(mount_demand_error)
                };
                cleanup.and(released)
            }
        }
        impl Drop for PreparedBytes<'_, '_> {
            fn drop(&mut self) { let _ = self.release(true); }
        }
        let mut preparation = PreparedBytes {
            carrier, bytes: 0, pending: &self.pending, cleanup_reserved: 0, scan: false,
            native_cleanup: None, native_allocation: None,
        };
        let mut requests = Vec::new();
        let prepared = {
            let held = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(original) = preparation.carrier.original_native_work() {
                (|| {
                    let cleanup = held.len() as u64;
                    preparation.native_cleanup = Some(original.reserve_items(cleanup).map_err(MountSourceError::NativeWork)?);
                    preparation.cleanup_reserved = cleanup;
                    for create in held.iter() {
                        original.charge(crate::WorkCounters {
                            items_examined: 1, ..crate::WorkCounters::UNCHARGED
                        }).map_err(MountSourceError::NativeWork)?;
                        cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
                        if lifetime.is_some_and(CancellationToken::is_cancelled) {
                            return Err(MountSourceError::Stale);
                        }
                        if !matches!(&*create.state(), PendingState::Pending { .. }) { continue; }
                        if requests.capacity() == 0 {
                            let mut scratch = NativeCaptureScratchAllocation::new(&original)?;
                            requests = scratch.vec::<GroupedRequest>(held.len())?;
                            preparation.native_allocation = Some(scratch.retain());
                        }
                        let admission = GroupedAdmission::new_with_native_work(GROUPED_CLAIMED, &original)?;
                        let copied = original.reserve_copy(std::mem::size_of::<GroupedRequest>() as u64)
                            .map_err(MountSourceError::NativeWork)?;
                        if let Some(change) = create.begin_applying_with_native_work(
                            &original, preparation.native_allocation.as_mut().expect("prepared request vector"), cancellation,
                        )? {
                            requests.push(GroupedRequest {
                                change, admission, reply: GroupedReply::Pending(Arc::clone(create)),
                                origin: create.origin, recorded: true,
                                native_callback_guard: create.native_callback_guard.as_ref().map(Arc::clone),
                                source_lifetime: Some(create.source_lifetime.clone()),
                            });
                            copied.record(std::mem::size_of::<GroupedRequest>() as u64)
                                .map_err(MountSourceError::NativeWork)?;
                        }
                    }
                    Ok(())
                })()
            } else {
                let PreparedBytes { carrier, bytes: claimed, cleanup_reserved, .. } = &mut preparation;
                carrier.with_work(|work| {
                    let cleanup = held.len() as u64;
                    work.work.admit(&crate::WorkCounters {
                        items_examined: cleanup, ..crate::WorkCounters::UNCHARGED
                    }, &work.budget).map_err(MountSourceError::Work)?;
                    work.budget.items_examined = work.budget.items_examined.checked_sub(cleanup)
                        .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
                    *cleanup_reserved = cleanup;
                    for create in held.iter() {
                        mount_items(work, 1)?;
                        cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
                        if lifetime.is_some_and(CancellationToken::is_cancelled) {
                            return Err(MountSourceError::Stale);
                        }
                        if !matches!(&*create.state(), PendingState::Pending { .. }) { continue; }
                        if requests.capacity() == 0 {
                            requests = mount_vec::<GroupedRequest>(held.len(), work)?;
                            let request_bytes = requests.capacity().checked_mul(std::mem::size_of::<GroupedRequest>())
                                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
                            *claimed = claimed.checked_add(request_bytes)
                                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
                        }
                        let admission = GroupedAdmission::new_measured(GROUPED_CLAIMED, work)?;
                        if let Some(change) = create.begin_applying_measured(
                            checkout.checkout.volume_config().limits, work, claimed,
                        )? {
                            requests.push(GroupedRequest {
                                change, admission, reply: GroupedReply::Pending(Arc::clone(create)),
                                origin: create.origin, recorded: true,
                                native_callback_guard: create.native_callback_guard.as_ref().map(Arc::clone),
                                source_lifetime: Some(create.source_lifetime.clone()),
                            });
                        }
                    }
                    Ok(())
                })
            }
        };
        if let Err(error) = prepared {
            let restored = requests.len() as u64;
            for request in requests {
                let GroupedRequest { change, reply, .. } = request;
                drop(change);
                if let GroupedReply::Pending(create) = reply { create.restore_applying_retained(); }
            }
            let _ = preparation.finish_cleanup(restored, false);
            let _ = preparation.release(false);
            return Err(error);
        }
        drop(checkout);
        preparation.scan = true;
        let result = match &mut *preparation.carrier {
            NativeDemandCarrier::Borrowed(work) => self
                .apply_grouped_requests_measured(requests, work, cancellation, lifetime).await,
            NativeDemandCarrier::Shared(meter) => self
                .apply_grouped_requests_with_meter(requests, meter, cancellation, lifetime).await,
            NativeDemandCarrier::Native(original) => self
                .apply_grouped_requests_with_native_work(requests, original, cancellation, lifetime).await,
        };
        let released = preparation.release(false);
        if preparation.carrier.original_native_work().is_none()
            && result.is_ok() && lifetime.is_some_and(CancellationToken::is_cancelled) {
            return Err(MountSourceError::Stale);
        }
        result.and(released)
    }

    /// Admits one callback to the current view alongside every other reader,
    /// once every pending creation is applied. A callback that already reads
    /// the view reads the one it began with.
    pub(super) async fn observe(
        &self,
        owner: CallbackOwner,
    ) -> Result<SharedCheckoutObservation<'_, A, O>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        if !self.view_gate.reads_for_callback(owner) {
            self.settle_pending().await;
        }
        self.observe_as_is(owner).await
    }

    /// Admits one callback to the view as it stands, pending creations and
    /// all: for facts no creation changes.
    async fn observe_as_is(
        &self,
        owner: CallbackOwner,
    ) -> Result<SharedCheckoutObservation<'_, A, O>, MountSourceError> {
        let view = self.view_gate.read_for_callback(owner, None).await?;
        let state = self.read_current_state().await;
        Ok(SharedCheckoutObservation { _view: view, state })
    }

    /// Observes the original shared view without settling queued creations.
    /// Reentrant callbacks remain readers even when a writer is queued.
    pub(super) async fn observe_as_is_with_work(
        &self, owner: CallbackOwner, work: &mut crate::demand::DemandWork,
        cancellation: &CancellationToken, lifetime: Option<&CancellationToken>,
    ) -> Result<SharedCheckoutObservation<'_, A, O>, MountSourceError> {
        self.observe_as_is_with_carrier(
            owner, &mut NativeDemandCarrier::Borrowed(work), cancellation, lifetime,
        ).await
    }

    pub(super) async fn observe_as_is_with_carrier(
        &self, owner: CallbackOwner, carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken, lifetime: Option<&CancellationToken>,
    ) -> Result<SharedCheckoutObservation<'_, A, O>, MountSourceError> {
        let view = self.view_gate.read_for_callback_with_carrier(
            owner, None, carrier, cancellation, lifetime,
        ).await?;
        let state = await_measured_lock_with_carrier(
            self.read_current_state(), carrier, cancellation, lifetime,
        ).await?;
        Ok(SharedCheckoutObservation { _view: view, state })
    }

    /// Settles only when the callback is not already a reader of this view.
    pub(super) async fn observe_with_work(
        &self,
        owner: CallbackOwner,
        work: &mut crate::demand::DemandWork,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<SharedCheckoutObservation<'_, A, O>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.observe_with_carrier(
            owner, &mut NativeDemandCarrier::Borrowed(work), cancellation, lifetime,
        ).await
    }

    async fn settle_pending_for_callback_with_carrier(
        &self,
        owner: CallbackOwner,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        use super::lazy::mount_demand_error;
        cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        if lifetime.is_some_and(CancellationToken::is_cancelled) {
            return Err(MountSourceError::Stale);
        }
        let already_reading = carrier.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            let (proofs, allocations, counters, budget) =
                work.view_gate_admission_parts().map_err(mount_demand_error)?;
            self.view_gate.reads_for_callback_measured(owner, proofs, allocations, counters, budget)
        })?;
        if !already_reading && !self.publication_in_flight() {
            match carrier {
                NativeDemandCarrier::Borrowed(work) => self.settle_pending_measured(
                    work, &mut Default::default(), cancellation, lifetime,
                ).await?,
                NativeDemandCarrier::Shared(meter) => self.settle_pending_with_meter(
                    meter, cancellation, lifetime,
                ).await?,
                NativeDemandCarrier::Native(original) => self.settle_pending_with_native_work(
                    original, cancellation, lifetime,
                ).await?,
            }
        }
        Ok(())
    }

    pub(super) async fn observe_with_carrier(
        &self,
        owner: CallbackOwner,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<SharedCheckoutObservation<'_, A, O>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        self.settle_pending_for_callback_with_carrier(owner, carrier, cancellation, lifetime).await?;
        self.observe_as_is_with_carrier(owner, carrier, cancellation, lifetime).await
    }

    pub(super) async fn observe_owned_with_carrier(
        &self,
        owner: CallbackOwner,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<SharedCheckoutOwnedObservation<A, O>, MountSourceError>
    where A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
        use super::lazy::{mount_accept, mount_demand_error, mount_engine_error};
        let observation = self.observe_with_carrier(
            owner, carrier, cancellation, lifetime,
        ).await?;
        let observer = if let Some(original) = carrier.original_native_work() {
            observation.observer_with_native_work(&original, cancellation).map_err(MountSourceError::Filesystem)?
        } else {
            let budget = carrier.with_work(|work| work.remaining().map_err(mount_demand_error))?;
            let receipt = observation.observer_measured(budget, cancellation);
            carrier.with_work(|work| mount_accept(work, receipt, mount_engine_error))?
        };
        let observer = observer.into_owned();
        let SharedCheckoutObservation { _view, state } = observation;
        drop(state);
        Ok(SharedCheckoutOwnedObservation { observer, _view })
    }

    /// Whether a native durability request (`fsync`, or a close that
    /// publishes) publishes; [`MountPublication::Manual`] leaves every
    /// publication to an explicit sync or unmount.
    pub(super) async fn publishes_at_native_boundary(&self) -> bool {
        self.read_current_state().await.publishes_at_native_boundary()
    }

    pub(super) async fn publishes_at_native_boundary_with_carrier(
        &self, carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken, lifetime: Option<&CancellationToken>,
    ) -> Result<bool, MountSourceError> {
        let state = await_measured_lock_with_carrier(
            self.read_current_state(), carrier, cancellation, lifetime,
        ).await?;
        Ok(state.publishes_at_native_boundary())
    }

    /// Begins an optimistic transaction on a copy of the current candidate.
    /// No reader is excluded: the copy records nothing into this checkout.
    pub(super) async fn candidate(&self) -> Result<CheckoutCandidate<A, O>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.read_current_state().await.candidate()
    }

    /// Advances with every recorded change to the checkout's view.
    pub(super) fn revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }

    fn unchanged_since(
        &self,
        path: &NamespacePath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
    ) -> bool {
        self.view_gate.is_stable() && self.ledger.unchanged_since(path, file_id, stamp, &mut super::view_ledger::unbounded_view_work()).expect("unbounded view query")
    }

    fn reported_unchanged_since(
        &self,
        path: &NamespacePath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
    ) -> bool {
        self.view_gate.is_stable() && self.ledger.reported_unchanged_since(path, file_id, stamp, &mut super::view_ledger::unbounded_view_work()).expect("unbounded view query")
    }

    fn node_unchanged_since(&self, file_id: FileId, stamp: ViewStamp) -> bool {
        self.view_gate.is_stable() && self.ledger.node_unchanged_since(file_id, stamp, &mut super::view_ledger::unbounded_view_work()).expect("unbounded view query")
    }

    fn listing_unchanged_since(&self, path: &NamespacePath, stamp: ViewStamp) -> bool {
        self.view_gate.is_stable() && self.ledger.listing_unchanged_since(path, stamp, &mut super::view_ledger::unbounded_view_work()).expect("unbounded view query")
    }

    fn binding_unchanged_since(&self, path: &NamespacePath, stamp: ViewStamp) -> bool {
        self.view_gate.is_stable() && self.ledger.binding_unchanged_since(path, stamp, &mut super::view_ledger::unbounded_view_work()).expect("unbounded view query")
    }
}

impl<A, O> Deref for SharedCheckoutGuard<'_, A, O> {
    type Target = SharedCheckoutState<A, O>;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<A, O> DerefMut for SharedCheckoutGuard<'_, A, O> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

impl<A, O> Drop for SharedCheckoutGuard<'_, A, O> {
    fn drop(&mut self) {
        if self.state.checkout.root().file_table != self.state.recorded_view {
            self.state.record(&ViewChange::Everything);
        }
    }
}

impl<A, O> Deref for SharedCheckoutObservation<'_, A, O> {
    type Target = SharedCheckoutState<A, O>;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<A, O> SharedCheckoutState<A, O> {
    async fn seal(&mut self, cancellation: &CancellationToken) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let operation_id = self.retained_operation_id();
        let result = seal_checkout(
            &mut self.checkout,
            operation_id,
            boundary_budget(),
            cancellation,
        )
        .await;
        if result.is_ok()
            || (matches!(result, Err(MountSourceError::Stale))
                && self.checkout.mode().mutations == crate::model::MutationMode::PrivateOverlay)
        {
            self.clear_retained_operation(operation_id);
        }
        result
    }

    fn publishes_at_native_boundary(&self) -> bool {
        self.publication != MountPublication::Manual
    }

    /// Rejects later mutation while one publication has an unresolved result.
    ///
    /// # Errors
    ///
    /// Returns [`MountSourceError::Stale`] while publication is indeterminate.
    pub fn ensure_publication_resolved(&self) -> Result<(), MountSourceError> {
        if self.publication_operation.is_some() {
            return Err(MountSourceError::Stale);
        }
        Ok(())
    }

    /// Returns the exact operation retained until publication resolves.
    pub fn retained_operation_id(&mut self) -> OperationId {
        *self
            .publication_operation
            .get_or_insert_with(OperationId::new)
    }

    pub(super) fn has_retained_operation(&self) -> bool {
        self.publication_operation.is_some()
    }

    /// Retains a caller-selected operation identity, or rejects a conflicting
    /// unresolved publication from another surface sharing this checkout.
    ///
    /// # Errors
    ///
    /// Returns [`MountSourceError::Stale`] for a different retained operation.
    pub fn retain_operation_id(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), MountSourceError> {
        match self.publication_operation {
            None => {
                self.publication_operation = Some(operation_id);
                Ok(())
            }
            Some(retained) if retained == operation_id => Ok(()),
            Some(_) => Err(MountSourceError::Stale),
        }
    }

    /// Clears only an operation with a known terminal publication outcome.
    pub fn clear_retained_operation(&mut self, operation_id: OperationId) {
        if self.publication_operation == Some(operation_id) {
            self.publication_operation = None;
            self.publication_evidence = None;
        }
    }

    /// Begins an optimistic transaction on a copy of this candidate.
    pub(super) fn candidate(&self) -> Result<CheckoutCandidate<A, O>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.ensure_publication_resolved()?;
        Ok(CheckoutCandidate {
            base: self.private_candidate(),
            checkout: self.private_candidate(),
        })
    }

    pub(super) fn candidate_for_operation_with_carrier(
        &self,
        operation: Option<OperationId>,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
    ) -> Result<CheckoutCandidate<A, O>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        use super::lazy::{mount_demand_error, mount_engine_error};
        match operation {
            Some(operation) if self.publication_operation == Some(operation) => {}
            Some(_) => return Err(MountSourceError::Stale),
            None => self.ensure_publication_resolved()?,
        }
        let clone = |carrier: &mut NativeDemandCarrier<'_>| {
            if let Some(original) = carrier.original_native_work() {
                return self.checkout.private_candidate_with_native_work(
                    &original, cancellation,
                ).map_err(MountSourceError::Filesystem);
            }
            carrier.with_work(|work| {
                let prior_live = work.live_allocation_bytes();
                let result = self.private_candidate_measured(
                    work.remaining().map_err(mount_demand_error)?, cancellation,
                );
                let checkout = match result {
                    Ok(receipt) => {
                        work.merge_with_prior_live(receipt.work, prior_live).map_err(mount_demand_error)?;
                        receipt.value
                    }
                    Err(failure) => {
                        work.merge_with_prior_live(*failure.work, prior_live).map_err(mount_demand_error)?;
                        return Err(mount_engine_error(failure.error));
                    }
                };
                work.retain_checkout_control(&checkout.private_control_custody())
                    .map_err(mount_demand_error)?;
                Ok(checkout)
            })
        };
        let base = clone(carrier)?;
        let checkout = clone(carrier)?;
        Ok(CheckoutCandidate { base, checkout })
    }

    /// Whether a transaction's candidate may still be installed: nothing
    /// but observations has changed this checkout since the transaction
    /// began, so installing undoes no other change, publication included.
    pub(super) fn admits(&self, candidate: &CheckoutCandidate<A, O>) -> bool {
        self.checkout
            .admits_candidate(&candidate.base, &candidate.checkout)
    }

    /// Installs a transaction's candidate with its recorded effect, only
    /// when [`Self::admits`] it. Returns whether it installed; any other
    /// change since leaves the checkout untouched for the caller to retry.
    pub(super) fn install_candidate(
        &mut self,
        candidate: CheckoutCandidate<A, O>,
        installed: &ViewEffect,
    ) -> bool {
        if !self
            .checkout
            .adopt_candidate(&candidate.base, candidate.checkout)
        {
            return false;
        }
        self.record(&ViewChange::Effect(installed));
        true
    }

    /// Records one change to the checkout's view. The exclusive guard keeps
    /// every lookup from overlapping the change it records.
    pub(super) fn record(&mut self, change: &ViewChange<'_>) {
        self.ledger.record(change, &mut NativeDemandCarrier::Borrowed(&mut super::view_ledger::unbounded_view_work()), &mut super::view_ledger::ViewLedgerAdmission::default()).expect("unbounded view recording");
        self.revision.fetch_add(1, Ordering::AcqRel);
        self.recorded_view = self.checkout.root().file_table;
    }
}

impl<A, O> SharedCheckout<A, O> {
    /// Applies queued changes as one group, records exactly what each
    /// successful one changed, publishes once when the policy publishes
    /// every mutation, and answers every caller.
    fn apply_grouped_requests<'a>(
        &'a self,
        requests: Vec<GroupedRequest>,
        cancellation: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        Box::pin(async move {
            let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
            let _ = self.apply_grouped_requests_measured(
                requests, &mut work, &cancellation, None,
            ).await;
        })
    }

    async fn apply_grouped_requests_measured(
        &self,
        requests: Vec<GroupedRequest>,
        work: &mut crate::demand::DemandWork,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        self.apply_grouped_requests_with_carrier(
            requests, &mut NativeDemandCarrier::Borrowed(work), cancellation, lifetime,
        ).await
    }

    async fn apply_grouped_requests_with_meter(
        &self,
        requests: Vec<GroupedRequest>,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        self.apply_grouped_requests_with_carrier(
            requests, &mut NativeDemandCarrier::Shared(meter), cancellation, lifetime,
        ).await
    }

    async fn apply_grouped_requests_with_native_work(
        &self,
        requests: Vec<GroupedRequest>,
        original: &super::NativeMountMeterWork,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        self.apply_grouped_requests_with_carrier(
            requests, &mut NativeDemandCarrier::Native(original), cancellation, lifetime,
        ).await
    }

    async fn apply_grouped_requests_with_carrier(
        &self,
        requests: Vec<GroupedRequest>,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        use super::lazy::{mount_demand_error, mount_engine_error};

        struct UnpreparedRequests(Vec<GroupedRequest>);
        impl UnpreparedRequests {
            fn reject(&mut self, error: MountSourceError) -> Result<(), MountSourceError> {
                let Some(request) = self.0.first() else { return Err(error); };
                let cause = request.admission.retain_failure(error);
                for request in self.0.drain(..) {
                    drop(request.change);
                    request.reply.reject_uninstalled(MountSourceError::Retained(Arc::clone(&cause)));
                }
                Err(MountSourceError::Retained(cause))
            }
        }
        impl Drop for UnpreparedRequests {
            fn drop(&mut self) {
                if !self.0.is_empty() {
                    let _ = self.reject(MountSourceError::Cancelled);
                }
            }
        }

        let mut requests = UnpreparedRequests(requests);
        let original = carrier.original_native_work();
        let check_lifetime = || {
            if lifetime.is_some_and(CancellationToken::is_cancelled) {
                Err(MountSourceError::Stale)
            } else {
                Ok(())
            }
        };
        if requests.0.is_empty() {
            check_lifetime()?;
            return match &original {
                Some(original) => original.admit(crate::WorkCounters::UNCHARGED)
                    .map_err(MountSourceError::NativeWork),
                None => carrier.with_work(|work| work.check().map_err(mount_demand_error)),
            };
        }
        let group_admission = Arc::clone(&requests.0.first().expect("nonempty original group").admission);
        let bindings = match GroupedCallbackBindings::prepare(&requests.0, &group_admission, carrier) {
            Ok(bindings) => bindings,
            Err(error) => return requests.reject(error),
        };
        if let Err(bindings) = group_admission.callback_bindings.set(bindings) {
            // Retire returned physical entries before their original guards.
            drop(bindings);
            return requests.reject(MountSourceError::Stale);
        }
        let (mut candidate, epoch, stamp) = match self.prepare_native_settled_phase_with_carrier(
            carrier, cancellation, lifetime,
        ).await {
            Ok(prepared) => prepared,
            Err(error) => return requests.reject(error),
        };
        // This witness is declared before the physical buffer owner, so its
        // original result claim retires only after every result slot drops.
        let mut native_result_custody = None;
        let mut buffers = match prepare_grouped_buffers_with_carrier(
            &requests.0, candidate.checkout.volume_config().limits, carrier,
        ) {
            Ok(buffers) => buffers,
            Err(error) => {
                drop(candidate);
                return requests.reject(error);
            }
        };
        if let Err(error) = (|| {
            let copied = requests.0.len().checked_mul(
                std::mem::size_of::<GroupedChange>() + std::mem::size_of::<GroupedCompletion>(),
            ).ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
            let delta = crate::WorkCounters {
                bytes_copied: copied,
                items_examined: requests.0.len() as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            match &original {
                Some(original) => original.charge(delta).map_err(MountSourceError::NativeWork),
                None => buffers.carrier.with_work(|work| work.charge(delta).map_err(mount_demand_error)),
            }
        })() {
            drop(candidate);
            return requests.reject(error);
        }
        let value = buffers.value.as_mut().expect("grouped phase buffers are live");
        for request in requests.0.drain(..) {
            value.changes.push(request.change);
            value.delivery.replies.push(GroupedCompletion {
                admission: request.admission,
                reply: request.reply,
            });
        }
        // Drain, rather than consume, keeps the caller's original request
        // backing physically alive until this engine returns.
        let captured = match &original {
            Some(original) => original.remaining().map(|budget|
                (budget, original.live_allocation_bytes())).map_err(MountSourceError::NativeWork),
            None => buffers.carrier.with_work(|work| {
                Ok::<_, MountSourceError>((
                    work.remaining().map_err(mount_demand_error)?, work.live_allocation_bytes(),
                ))
            }),
        };
        let (budget, prior_live) = match captured {
            Ok(captured) => captured,
            Err(error) => {
                drop(candidate);
                return buffers.value.as_mut().expect("grouped phase buffers are live")
                    .delivery.complete(Err(error));
            }
        };
        let changes = std::mem::take(
            &mut buffers.value.as_mut().expect("grouped phase buffers are live").changes,
        );
        let change_bytes = (changes.capacity() * std::mem::size_of::<GroupedChange>()) as u64;
        let (accepted, released) = if let Some(original) = &original {
            let input_allocation = buffers.value.as_mut().expect("grouped phase buffers are live")
                .changes_allocation.take().expect("native grouped slots retain their original allocation");
            let applied = candidate.checkout.apply_group_with_native_work(
                changes, input_allocation, original, cancellation,
            ).await;
            let accepted = applied.map(|(results, allocation)| {
                native_result_custody = Some(allocation);
                results
            }).map_err(MountSourceError::Filesystem);
            (accepted, Ok(()))
        } else {
            let applied = candidate.checkout.apply_group(changes, budget, cancellation).await;
            let accepted = buffers.carrier.accept_with_prior_live(applied, prior_live, mount_engine_error);
            // Only the standalone projection imports its child receipt. Its
            // physically dropped input slots have no borrowed payload claim.
            let released = buffers.carrier.with_work(|work|
                work.release(change_bytes).map_err(mount_demand_error));
            if released.is_ok() {
                buffers.bytes = buffers.bytes.checked_sub(change_bytes)
                    .expect("grouped change slots have exact custody");
            }
            (accepted, released)
        };
        let results = match accepted {
            Ok(results) => results,
            Err(error) => {
                drop(candidate);
                return buffers.value.as_mut().expect("grouped phase buffers are live")
                    .delivery.complete(Err(error));
            }
        };
        let result_bytes = (results.capacity()
            * std::mem::size_of::<Result<GroupedOutcome, FsError>>()) as u64;
        let adopted = if native_result_custody.is_some() {
            Ok(())
        } else {
            let adopted = buffers.carrier.with_work(|work|
                work.record_adoption(result_bytes).map_err(mount_demand_error));
            buffers.bytes = buffers.bytes.checked_add(result_bytes)
                .expect("grouped outcome slots have exact custody");
            adopted
        };
        buffers.value.as_mut().expect("grouped phase buffers are live").delivery.results = Some(results);
        let mut completed = released.and(adopted).and_then(|()| check_lifetime());
        let changed = buffers.value.as_ref().expect("grouped phase buffers are live")
            .delivery.results.as_ref().into_iter().flatten().any(Result::is_ok);
        if completed.is_ok() && changed {
            let value = buffers.value.as_ref().expect("grouped phase buffers are live");
            let installed = self.finish_native_phase_with_recording(
                candidate, epoch, stamp, buffers.carrier, cancellation, lifetime,
                |carrier| {
                    for ((result, change), (origin, recorded)) in value.delivery.results.as_ref()
                        .into_iter().flatten().zip(&value.delivery.views).zip(&value.origins)
                    {
                        if result.is_ok() && !*recorded {
                            let _origin = origin.enter();
                            let change = match change {
                                GroupedView::Bound(path) => ViewChange::Bound(path),
                                GroupedView::Node(file_id) => ViewChange::Node(*file_id),
                            };
                            record_view_change_with_carrier(&self.ledger, &change, carrier)?;
                        }
                    }
                    Ok(())
                },
            ).await;
            if installed.is_ok() {
                buffers.value.as_mut().expect("grouped phase buffers are live").delivery.installed = true;
                completed = self.publish_mutation_with_carrier_guarded(
                    buffers.carrier, cancellation, lifetime, grouped_publication_guard(Arc::clone(&group_admission)),
                ).await;
            } else {
                completed = installed.map(|_| ());
            }
        } else {
            drop(candidate);
        }
        if completed.is_ok() {
            completed = match &original {
                Some(original) => original.admit(crate::WorkCounters::UNCHARGED)
                    .map_err(MountSourceError::NativeWork),
                None => buffers.carrier.with_work(|work| work.check().map_err(mount_demand_error)),
            }.and_then(|()| check_lifetime());
        }
        buffers.value.as_mut().expect("grouped phase buffers are live").delivery.complete(completed)
    }
}

/// One optimistic transaction: a copy of the checkout to change, and the
/// exact checkout it began from, against which its effect is computed
/// without holding any lock.
pub(super) struct CheckoutCandidate<A, O> {
    base: Checkout<A, O>,
    pub(super) checkout: Checkout<A, O>,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> CheckoutCandidate<A, O> {
    pub(super) fn logical_owned_bytes(&self) -> u64 {
        self.base.logical_owned_bytes() + self.checkout.logical_owned_bytes()
    }

    /// The exact effect installing this candidate has: the generation diff
    /// from the checkout it began from. `scope` names what the candidate was
    /// prepared to change and locates the directories whose names changed.
    pub(super) async fn installed_changes(
        &self,
        scope: InstallScope<'_>,
        cancellation: &CancellationToken,
    ) -> ViewEffect {
        installed_changes(&self.base, &self.checkout, scope, cancellation).await
    }

    pub(super) async fn installed_changes_measured(
        &self,
        scope: InstallScope<'_>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<ViewEffect>, MountSourceError> {
        installed_changes_measured(&self.base, &self.checkout, scope, budget, cancellation).await
    }

    pub(super) async fn installed_changes_with_native_work(
        &self,
        scope: InstallScope<'_>,
        work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(ViewEffect, Option<crate::NativeAllocation>), MountSourceError> {
        let installed = installed_changes_with_native_work(
            &self.base, &self.checkout, scope, work, cancellation,
        ).await?;
        Ok((installed.value, installed._allocation))
    }

    /// The checkout this transaction began from.
    pub(super) fn base(&self) -> &Checkout<A, O> {
        &self.base
    }
}

/// What a prepared candidate was built to change.
#[derive(Clone, Copy)]
pub(super) enum InstallScope<'a> {
    /// Exactly these paths.
    Paths(&'a [NamespacePath]),
    /// Two borrowed portions of the same original candidate's exact paths.
    CombinedPaths(&'a [NamespacePath], &'a [NamespacePath]),
    /// Anything beneath this path.
    Subtree(&'a NamespacePath),
}

impl InstallScope<'_> {
    fn paths(&self) -> impl Iterator<Item = &NamespacePath> {
        let (first, second) = match self {
            Self::Paths(paths) => (*paths, &[][..]),
            Self::CombinedPaths(first, second) => (*first, *second),
            Self::Subtree(root) => (std::slice::from_ref(*root), &[][..]),
        };
        first.iter().chain(second.iter())
    }
}

/// The exact effect of replacing `base` with `candidate`, from their
/// generation diff. A changed name in a directory the scope's paths do not
/// pass through is covered by its subtree scope, or by everything.
async fn installed_changes<A, O>(
    base: &Checkout<A, O>,
    candidate: &Checkout<A, O>,
    scope: InstallScope<'_>,
    cancellation: &CancellationToken,
) -> ViewEffect
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    installed_changes_measured(base, candidate, scope, boundary_budget(), cancellation)
        .await
        .map(|receipt| receipt.value)
        .unwrap_or_else(|_| ViewEffect { everything: true, ..ViewEffect::default() })
}

struct InstalledCaptureEffect {
    value: ViewEffect,
    _allocation: Option<crate::NativeAllocation>,
}

enum InstalledCaptureAccounting<'a> {
    Ordinary(&'a mut crate::demand::DemandWork),
    Native(NativeCaptureScratchAllocation<'a>),
}

impl InstalledCaptureAccounting<'_> {
    fn vec<T>(&mut self, count: usize) -> Result<Vec<T>, MountSourceError> {
        match self {
            Self::Ordinary(work) => super::lazy::mount_vec(count, work),
            Self::Native(scratch) => scratch.vec(count),
        }
    }

    fn items(&mut self, count: usize) -> Result<(), MountSourceError> {
        match self {
            Self::Ordinary(work) => super::lazy::mount_items(work, count),
            Self::Native(scratch) => scratch.items(count),
        }
    }

    fn charge(&mut self, delta: crate::WorkCounters) -> Result<(), MountSourceError> {
        match self {
            Self::Ordinary(work) => work.charge(delta).map_err(super::lazy::mount_demand_error),
            Self::Native(scratch) => scratch.charge(delta),
        }
    }

    fn release(&mut self, bytes: u64) -> Result<(), MountSourceError> {
        match self {
            Self::Ordinary(work) => work.release(bytes).map_err(super::lazy::mount_demand_error),
            Self::Native(scratch) => CaptureInputScratch::release(scratch, bytes),
        }
    }

    fn namespace_bytes(&self, path: &NamespacePath) -> u64 {
        match self {
            Self::Ordinary(_) => super::lazy::mount_namespace_bytes(path),
            Self::Native(_) => path.retained_bytes(),
        }
    }

    fn name_bytes(&self, name: &LogicalName) -> u64 {
        match self {
            Self::Ordinary(_) => name.as_bytes().len() as u64,
            Self::Native(_) => name.retained_bytes() as u64,
        }
    }

    fn binding_history_bytes(&self, binding: &crate::DirectoryBindingChange) -> u64 {
        match self {
            Self::Ordinary(_) => 0,
            Self::Native(_) => binding.before.as_ref().map_or(0, |entry| entry.name.retained_bytes() as u64)
                + binding.after.as_ref().map_or(0, |entry| entry.name.retained_bytes() as u64),
        }
    }

    fn clone_namespace(
        &mut self, names: &[LogicalName], limits: VolumeLimits,
    ) -> Result<NamespacePath, MountSourceError> {
        match self {
            Self::Ordinary(work) => super::lazy::mount_clone_namespace(names, limits, work),
            Self::Native(scratch) => {
                let mut owned = scratch.vec(names.len())?;
                for name in names {
                    scratch.items(1)?;
                    owned.push(capture_copy_name(
                        name.encoding(), name.as_bytes(), limits, scratch,
                    )?);
                }
                NamespacePath::new(owned, limits).map_err(|error| MountSourceError::Filesystem(
                    crate::FsError::Path(crate::kernel::PathLookupError::NamespacePath(error)),
                ))
            }
        }
    }

    async fn diff<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &mut self, base: &Checkout<A, O>, candidate: &Checkout<A, O>,
        cancellation: &CancellationToken,
    ) -> Result<crate::GenerationDiff, MountSourceError> {
        use super::lazy::{mount_accept, mount_demand_error, mount_engine_error};
        match self {
            Self::Ordinary(work) => {
                let result = base.candidate_diff(
                    candidate, MAXIMUM_EXACT_INSTALL_CHANGES,
                    work.remaining().map_err(mount_demand_error)?, cancellation,
                ).await;
                let diff = mount_accept(work, result, mount_engine_error)?;
                if !diff.truncated {
                    let bytes = (diff.files.capacity() * std::mem::size_of::<crate::FileRecordChange>()
                        + diff.bindings.capacity() * std::mem::size_of::<crate::DirectoryBindingChange>()) as u64
                        + diff.bindings.iter().map(|binding| binding.name.as_bytes().len() as u64).sum::<u64>();
                    work.adopt(bytes).map_err(mount_demand_error)?;
                }
                Ok(diff)
            }
            Self::Native(scratch) => {
                let allocation;
                let diff;
                (diff, allocation) = base.candidate_diff_with_native_work(
                    candidate, MAXIMUM_EXACT_INSTALL_CHANGES, scratch.work, cancellation,
                ).await.map_err(MountSourceError::Filesystem)?;
                scratch.absorb(allocation);
                Ok(diff)
            }
        }
    }

    async fn lookup<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &mut self, checkout: &Checkout<A, O>, path: &NamespacePath,
        cancellation: &CancellationToken,
    ) -> Result<Option<FileRecord>, MountSourceError> {
        use super::lazy::{mount_accept, mount_demand_error, mount_engine_error};
        match self {
            Self::Ordinary(work) => {
                let result = checkout.inspector().lookup_no_follow(
                    path, work.remaining().map_err(mount_demand_error)?, cancellation,
                ).await;
                Ok(mount_accept(work, result, mount_engine_error)?.record)
            }
            Self::Native(scratch) => {
                let mut inspector = checkout.inspector_with_native_work(scratch.work, cancellation)
                    .map_err(MountSourceError::Filesystem)?;
                Ok(inspector.lookup_no_follow_with_native_work(
                    path, scratch.work, cancellation,
                ).await.map_err(MountSourceError::Filesystem)?.record)
            }
        }
    }

    fn into_allocation(self) -> Option<crate::NativeAllocation> {
        match self {
            Self::Ordinary(_) => None,
            Self::Native(scratch) => Some(scratch.retain()),
        }
    }
}

fn installed_directory_position(
    entries: &[(FileId, Option<NamespacePath>)],
    file_id: FileId,
    accounting: &mut InstalledCaptureAccounting<'_>,
) -> Result<Result<usize, usize>, MountSourceError> {
    let (mut low, mut high) = (0, entries.len());
    while low < high {
        accounting.items(1)?;
        let middle = low + (high - low) / 2;
        let entry = entries.get(middle).ok_or(MountSourceError::Stale)?;
        match entry.0.cmp(&file_id) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => return Ok(Ok(middle)),
        }
    }
    Ok(Err(low))
}

async fn installed_changes_measured<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    base: &Checkout<A, O>,
    candidate: &Checkout<A, O>,
    scope: InstallScope<'_>,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> crate::MeasuredResult<crate::OperationReceipt<ViewEffect>, MountSourceError> {
    let mut work = crate::demand::DemandWork::new(budget, cancellation);
    let result = installed_changes_with_accounting(
        base, candidate, scope, &mut InstalledCaptureAccounting::Ordinary(&mut work), cancellation,
    ).await;
    result.map(|value| crate::OperationReceipt { value, work: work.work })
        .map_err(|error| crate::OperationFailure::new(error, work.work))
}

async fn installed_changes_with_native_work<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    base: &Checkout<A, O>,
    candidate: &Checkout<A, O>,
    scope: InstallScope<'_>,
    work: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<InstalledCaptureEffect, MountSourceError> {
    let mut accounting = InstalledCaptureAccounting::Native(NativeCaptureScratchAllocation::new(work)?);
    let value = installed_changes_with_accounting(
        base, candidate, scope, &mut accounting, cancellation,
    ).await?;
    Ok(InstalledCaptureEffect { value, _allocation: accounting.into_allocation() })
}

async fn installed_changes_with_accounting<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    base: &Checkout<A, O>,
    candidate: &Checkout<A, O>,
    scope: InstallScope<'_>,
    accounting: &mut InstalledCaptureAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<ViewEffect, MountSourceError> {
    let diff = accounting.diff(base, candidate, cancellation).await?;
    let file_buffer_bytes = (diff.files.capacity() * std::mem::size_of::<crate::FileRecordChange>()) as u64;
    let binding_buffer_bytes = (diff.bindings.capacity() * std::mem::size_of::<crate::DirectoryBindingChange>()) as u64;
    if diff.truncated {
        if matches!(accounting, InstalledCaptureAccounting::Native(_)) {
            let bytes = file_buffer_bytes + binding_buffer_bytes
                + diff.bindings.iter().map(|binding|
                    accounting.name_bytes(&binding.name) + accounting.binding_history_bytes(binding)
                ).sum::<u64>();
            drop(diff);
            accounting.release(bytes)?;
        } else {
            drop(diff);
        }
        return Ok(ViewEffect { everything: true, ..ViewEffect::default() });
    }
    let mut installed = ViewEffect {
        nodes: accounting.vec(diff.files.len())?,
        names: accounting.vec(diff.bindings.len())?,
        ..ViewEffect::default()
    };
    for change in diff.files {
        accounting.items(1)?;
        installed.nodes.push(change.file_id);
    }
    accounting.release(file_buffer_bytes)?;
    // The same sorted parent index resolves every scope prefix once.
    let mut directories = accounting.vec::<(FileId, Option<NamespacePath>)>(diff.bindings.len())?;
    for binding in &diff.bindings {
        if let Err(position) = installed_directory_position(&directories, binding.directory_id, accounting)? {
            accounting.charge(crate::WorkCounters {
                bytes_copied: ((directories.len() - position) * std::mem::size_of::<(FileId, Option<NamespacePath>)>()) as u64,
                ..crate::WorkCounters::UNCHARGED
            })?;
            directories.insert(position, (binding.directory_id, None));
        }
    }
    if !directories.is_empty() {
        for path in scope.paths() {
            for length in 0..=path.depth() {
                let names = path.components().get(..length).ok_or(MountSourceError::Stale)?;
                let prefix = accounting.clone_namespace(names, base.volume_config().limits)?;
                let bytes = accounting.namespace_bytes(&prefix);
                for checkout in [base, candidate] {
                    if let Some(record) = accounting.lookup(checkout, &prefix, cancellation).await? {
                        if let Ok(position) = installed_directory_position(&directories, record.file_id, accounting)? {
                            let entry = directories.get_mut(position).ok_or(MountSourceError::Stale)?;
                            if entry.1.is_none() {
                                entry.1 = Some(accounting.clone_namespace(
                                    prefix.components(), base.volume_config().limits,
                                )?);
                            }
                        }
                    }
                }
                drop(prefix);
                accounting.release(bytes)?;
            }
        }
    }
    for binding in diff.bindings {
        accounting.items(1)?;
        let position = installed_directory_position(&directories, binding.directory_id, accounting)?
            .map_err(|_| MountSourceError::Stale)?;
        let directory = directories.get(position).and_then(|entry| entry.1.as_ref());
        let name_bytes = accounting.name_bytes(&binding.name);
        let history_bytes = accounting.binding_history_bytes(&binding);
        drop(binding.before);
        drop(binding.after);
        if history_bytes != 0 { accounting.release(history_bytes)?; }
        match (directory, scope) {
            (Some(directory), _) => installed.names.push((accounting.clone_namespace(
                directory.components(), base.volume_config().limits,
            )?, binding.name)),
            (None, InstallScope::Subtree(root)) => {
                drop(binding.name);
                accounting.release(name_bytes)?;
                if installed.subtrees.is_empty() {
                    installed.subtrees = accounting.vec(1)?;
                    installed.subtrees.push(accounting.clone_namespace(
                        root.components(), base.volume_config().limits,
                    )?);
                }
            }
            (None, InstallScope::Paths(_) | InstallScope::CombinedPaths(_, _)) => {
                drop(binding.name);
                accounting.release(name_bytes)?;
                installed.everything = true;
            }
        }
    }
    accounting.release(binding_buffer_bytes)?;
    if matches!(accounting, InstalledCaptureAccounting::Native(_)) {
        let bytes = (directories.capacity() * std::mem::size_of::<(FileId, Option<NamespacePath>)>()) as u64
            + directories.iter().filter_map(|entry| entry.1.as_ref())
                .map(|path| accounting.namespace_bytes(path)).sum::<u64>();
        drop(directories);
        accounting.release(bytes)?;
    }
    Ok(installed)
}

impl<A, O> Deref for SharedCheckoutState<A, O> {
    type Target = Checkout<A, O>;

    fn deref(&self) -> &Self::Target {
        &self.checkout
    }
}

impl<A, O> DerefMut for SharedCheckoutState<A, O> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.checkout
    }
}

impl<A, O> CheckoutDetachedFile<A, O> {
    fn attribute_name(&self, bytes: &[u8]) -> Result<AttributeName, MountSourceError> {
        if self.profile != FilesystemProfile::Posix {
            return Err(MountSourceError::Unsupported(
                "native POSIX attributes require a POSIX volume profile".to_owned(),
            ));
        }
        AttributeName::new(
            AttributeClass::PosixXattr,
            bytes.to_vec(),
            self.limits.maximum_component_bytes,
        )
        .map_err(engine_error)
    }

    pub(super) async fn snapshot_async(&self) -> Result<(FileRecord, FileMetadata, u64), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let state = self.state.lock().await;
        Ok((state.file.record(), state.metadata, state.mutation_epoch))
    }

    /// Snapshots the same detached state under the caller's work and original
    /// cancellation tokens, without allocating a callback runtime or token.
    pub(super) async fn snapshot_with_work(
        &self,
        work: &mut crate::demand::DemandWork,
        cancellation: &CancellationToken,
    ) -> Result<(FileRecord, FileMetadata, u64), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.snapshot_with_carrier(
            &mut NativeDemandCarrier::Borrowed(work), cancellation,
        ).await
    }

    pub(super) async fn snapshot_with_carrier(
        &self,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(FileRecord, FileMetadata, u64), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let state = await_measured_lock_with_carrier(
            self.state.lock(), carrier, cancellation, Some(&self.cancellation),
        ).await?;
        let metadata = if let Some(original) = carrier.original_native_work() {
            state.file.read_metadata_with_native_work(&original, cancellation)
                .await.map_err(MountSourceError::Filesystem)?
        } else {
            state.metadata
        };
        Ok((state.file.record(), metadata, state.mutation_epoch))
    }

    /// Atomically changes metadata and optional size on this held identity.
    pub(super) async fn set_attributes_async_with_carrier(
        &self,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        use super::lazy::{mount_accept, mount_demand_error, mount_engine_error};
        let cancellation = carrier.cancellation();
        let mut state = await_measured_lock_with_carrier(
            self.state.lock(), carrier, &cancellation, Some(&self.cancellation),
        ).await?;
        let epoch = state.mutation_epoch.checked_add(1).ok_or(MountSourceError::Stale)?;
        record_view_change_with_carrier(
            &self.ledger, &ViewChange::Node(state.file.file_id()), carrier,
        )?;
        if let Some(original) = carrier.original_native_work() {
            state.file.set_attributes_with_native_work(
                metadata, logical_bytes, &original, &cancellation,
            ).await.map_err(MountSourceError::Filesystem)?;
            state.metadata = metadata;
            state.mutation_epoch = epoch;
            return Ok(());
        }
        let budget = carrier.with_work(|work| work.remaining().map_err(mount_demand_error))?;
        let receipt = state.file.set_attributes(
            metadata, logical_bytes, budget, &cancellation,
        ).await;
        if receipt.is_ok() {
            state.metadata = metadata;
            state.mutation_epoch = epoch;
        }
        carrier.with_work(|work| mount_accept(work, receipt, mount_engine_error))
    }

    /// Reads the held identity on the original caller's meter, with no runtime
    /// reentry. The returned producer capacity remains live until its caller
    /// physically retires or transfers the owned packet.
    pub(super) async fn read_range_owned_async_with_carrier(
        &self,
        offset: u64,
        length: u32,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.read_owned_async_with_carrier(offset, length, false, carrier).await
    }

    async fn read_owned_async_with_carrier(
        &self, offset: u64, length: u32, clip: bool, carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let cancellation = carrier.cancellation();
        let state = await_measured_lock_with_carrier(
            self.state.lock(), carrier, &cancellation, Some(&self.cancellation),
        ).await?;
        let length = if clip {
            state.file.logical_bytes().saturating_sub(offset).min(u64::from(length))
        } else {
            u64::from(length)
        };
        if clip && length == 0 {
            return carrier.empty_source_chunk();
        }
        self.read_owned_from_file_with_carrier(
            &state.file, ByteRange { offset, length }, carrier, &cancellation,
        ).await
    }

    async fn read_owned_from_file_with_carrier(
        &self, file: &DetachedFile<A, O>, range: ByteRange,
        carrier: &mut NativeDemandCarrier<'_>, cancellation: &CancellationToken,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        use super::lazy::{mount_accept, mount_demand_error, mount_engine_error};
        if let Some(original) = carrier.original_native_work() {
            let read = file.read_range_with_native_work(range, &original, cancellation)
                .await.map_err(MountSourceError::Filesystem)?;
            original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            let logical_owned_bytes = read.logical_owned_bytes();
            return Ok(crate::kernel::OwnedSourceChunk::intrinsic(read.bytes, logical_owned_bytes));
        }
        let budget = carrier.with_work(|work| work.remaining().map_err(mount_demand_error))?;
        let result = file.read_range(
            range, budget, cancellation,
        ).await;
        let read = carrier.with_work(|work| mount_accept(work, result, mount_engine_error))?;
        let logical_owned_bytes = read.logical_owned_bytes();
        let admitted = carrier.with_work(|work| {
            work.record_adoption(logical_owned_bytes).and_then(|()| work.check())
        });
        if let Err(error) = admitted {
            drop(read);
            carrier.with_work(|work| work.release(logical_owned_bytes)).map_err(mount_demand_error)?;
            return Err(mount_demand_error(error));
        }
        if self.cancellation.is_cancelled() {
            drop(read);
            carrier.with_work(|work| work.release(logical_owned_bytes)).map_err(mount_demand_error)?;
            return Err(MountSourceError::Cancelled);
        }
        Ok(crate::kernel::OwnedSourceChunk::claimed(read.bytes, logical_owned_bytes))
    }

    async fn change_content_locked_with_carrier(
        &self, state: &mut DetachedMountState<A, O>,
        change: ContentChange<std::convert::Infallible>,
        carrier: &mut NativeDemandCarrier<'_>, cancellation: &CancellationToken,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        use super::lazy::{mount_accept, mount_demand_error, mount_engine_error};
        let epoch = state.mutation_epoch.checked_add(1).ok_or(MountSourceError::Stale)?;
        let mut metadata = state.metadata;
        let stamped = metadata.stamp_content_change(content_change_time()?).then_some(metadata);
        record_view_change_with_carrier(
            &self.ledger, &ViewChange::Node(state.file.file_id()), carrier,
        )?;
        if let Some(original) = carrier.original_native_work() {
            let result = state.file.change_content_with_native_work(
                change, stamped, &original, cancellation,
            ).await;
            if result.is_ok() {
                state.metadata = metadata;
                state.mutation_epoch = epoch;
            }
            return result.map_err(MountSourceError::Filesystem);
        }
        let budget = carrier.with_work(|work| work.remaining().map_err(mount_demand_error))?;
        let receipt = state.file.change_content(change, stamped, budget, cancellation).await;
        if receipt.is_ok() {
            state.metadata = metadata;
            state.mutation_epoch = epoch;
        }
        carrier.with_work(|work| mount_accept(work, receipt, mount_engine_error))
    }

    async fn change_content_async_with_carrier(
        &self, change: ContentChange<std::convert::Infallible>, carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let cancellation = carrier.cancellation();
        let mut state = await_measured_lock_with_carrier(
            self.state.lock(), carrier, &cancellation, Some(&self.cancellation),
        ).await?;
        self.change_content_locked_with_carrier(&mut state, change, carrier, &cancellation).await
    }

    async fn mutate_attribute_async_with_carrier(
        &self, name: &[u8], value: Option<(&crate::kernel::OwnedSourceChunk, MountAttributeWriteMode)>,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        use super::lazy::{mount_accept, mount_demand_error};
        if let Some(original) = carrier.original_native_work() {
            let owned_name = native_attribute_name_with_native_work(
                self.profile, self.limits, name, &original,
            )?;
            let _name_allocation = owned_name.1;
            let name = owned_name.0;
            let cancellation = carrier.cancellation();
            let mut state = await_measured_lock_with_carrier(
                self.state.lock(), carrier, &cancellation, Some(&self.cancellation),
            ).await?;
            let epoch = state.mutation_epoch.checked_add(1).ok_or(MountSourceError::Stale)?;
            record_view_change_with_carrier(
                &self.ledger, &ViewChange::Node(state.file.file_id()), carrier,
            )?;
            let result = match value {
                Some((value, mode)) => state.file.write_named_attribute_with_native_work(
                    &name, &value.bytes, match mode {
                        MountAttributeWriteMode::Upsert => NamedAttributeWriteMode::Upsert,
                        MountAttributeWriteMode::Create => NamedAttributeWriteMode::Create,
                        MountAttributeWriteMode::Replace => NamedAttributeWriteMode::Replace,
                    }, &original, &cancellation,
                ).await,
                None => state.file.remove_named_attribute_with_native_work(
                    &name, &original, &cancellation,
                ).await,
            };
            if let Ok(metadata) = &result {
                state.metadata = *metadata;
                state.mutation_epoch = epoch;
            }
            return result.map(|_| ()).map_err(MountSourceError::Filesystem);
        }
        let name = carrier.with_work(|work| native_attribute_name_with_work(
            self.profile, self.limits, name, work,
        ))?;
        let bytes = name.retained_bytes() as u64;
        let mut name = NativeCarrierOwned { value: Some(name), carrier, bytes };
        let carrier = &mut *name.carrier;
        let cancellation = carrier.cancellation();
        let mut state = await_measured_lock_with_carrier(
            self.state.lock(), carrier, &cancellation, Some(&self.cancellation),
        ).await?;
        let epoch = state.mutation_epoch.checked_add(1).ok_or(MountSourceError::Stale)?;
        record_view_change_with_carrier(
            &self.ledger, &ViewChange::Node(state.file.file_id()), carrier,
        )?;
        let sdk_name = name.value.take().expect("attribute name is live");
        let budget = carrier.with_work(|work| work.remaining().map_err(mount_demand_error))?;
        let receipt = match value {
            Some((value, mode)) => state.file.write_named_attribute(
                sdk_name, value.bytes.clone(), match mode {
                    MountAttributeWriteMode::Upsert => NamedAttributeWriteMode::Upsert,
                    MountAttributeWriteMode::Create => NamedAttributeWriteMode::Create,
                    MountAttributeWriteMode::Replace => NamedAttributeWriteMode::Replace,
                }, budget, &cancellation,
            ).await,
            None => state.file.remove_named_attribute(sdk_name, budget, &cancellation).await,
        };
        if let Ok(receipt) = &receipt {
            state.metadata = receipt.value;
            state.mutation_epoch = epoch;
        }
        carrier.with_work(|work| mount_accept(work, receipt, fs_error)).map(|_| ())
    }

    async fn read_sparse_async_with_native_work(
        &self, offset: u64, length: u32, work: &super::NativeMountMeterWork,
    ) -> Result<(MountSparseRange, crate::NativeAllocation), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let end = offset.checked_add(u64::from(length)).ok_or(MountSourceError::NativeStaticText {
            kind: super::NativeSourceTextKind::Invalid,
            text: "sparse range overflows the file address space",
        })?;
        let original = work.native_materialization_work();
        let mut carrier = NativeDemandCarrier::Native(work);
        let cancellation = carrier.cancellation();
        let state = await_measured_lock_with_carrier(
            self.state.lock(), &mut carrier, &cancellation, Some(&self.cancellation),
        ).await?;
        let mut scratch = NativeCaptureScratchAllocation::new(&original)?;
        let mut range = MountSparseRange { logical_bytes: u64::from(length), spans: Vec::new() };
        let mut cursor = offset;
        while cursor < end {
            scratch.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            let Some(data) = state.file.seek_with_native_work(
                cursor, ExtentSeekTarget::Data, &original, &cancellation,
            ).await.map_err(MountSourceError::Filesystem)? else {
                break;
            };
            if data >= end { break; }
            let hole = state.file.seek_with_native_work(
                data, ExtentSeekTarget::Hole, &original, &cancellation,
            ).await.map_err(MountSourceError::Filesystem)?.unwrap_or(end).min(end);
            if data < cursor || hole <= data {
                return Err(MountSourceError::NativeStaticText {
                    kind: super::NativeSourceTextKind::Engine,
                    text: "sparse seek made no forward progress",
                });
            }
            if range.spans.len() == range.spans.capacity() {
                let count = range.spans.len().checked_add(1).ok_or(MountSourceError::Stale)?;
                let capacity = count.checked_next_power_of_two().ok_or(MountSourceError::Stale)?;
                let mut grown = scratch.vec::<super::MountSparseSpan>(capacity)?;
                let old_bytes = (range.spans.capacity() * std::mem::size_of::<super::MountSparseSpan>()) as u64;
                scratch.charge(crate::WorkCounters {
                    bytes_copied: (range.spans.len() * std::mem::size_of::<super::MountSparseSpan>()) as u64,
                    ..crate::WorkCounters::UNCHARGED
                })?;
                grown.append(&mut range.spans);
                drop(std::mem::replace(&mut range.spans, grown));
                scratch.release(old_bytes)?;
            }
            let packet = state.file.read_range_with_native_work(
                ByteRange { offset: data, length: hole - data }, &original, &cancellation,
            ).await.map_err(MountSourceError::Filesystem)?;
            if packet.bytes.len() as u64 != hole - data { return Err(MountSourceError::Stale); }
            range.spans.push(super::MountSparseSpan {
                relative_offset: data - offset, bytes: packet.bytes,
            });
            cursor = hole;
        }
        original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        Ok((range, scratch.retain()))
    }

    async fn write_sparse_async_with_carrier(
        &self, offset: u64, range: &MountSparseRange, carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        use super::lazy::mount_items;
        let original = carrier.original_native_work();
        let end = offset.checked_add(range.logical_bytes).ok_or_else(|| {
            if original.is_some() {
                MountSourceError::NativeStaticText {
                    kind: super::NativeSourceTextKind::Invalid,
                    text: "sparse range overflows the file address space",
                }
            } else {
                MountSourceError::Invalid("sparse range overflows the file address space".to_owned())
            }
        })?;
        let cancellation = carrier.cancellation();
        let mut state = await_measured_lock_with_carrier(
            self.state.lock(), carrier, &cancellation, Some(&self.cancellation),
        ).await?;
        if end > state.file.logical_bytes() {
            self.change_content_locked_with_carrier(
                &mut state, ContentChange::Resize { logical_bytes: end }, carrier, &cancellation,
            ).await?;
        }
        self.change_content_locked_with_carrier(
            &mut state, range_allocation(ByteRange { offset, length: range.logical_bytes },
                MountRangeAllocation::PunchHole), carrier, &cancellation,
        ).await?;
        for span in &range.spans {
            if let Some(original) = &original {
                original.charge(crate::WorkCounters {
                    items_examined: 1, ..crate::WorkCounters::UNCHARGED
                }).map_err(MountSourceError::NativeWork)?;
            } else {
                carrier.with_work(|work| mount_items(work, 1))?;
            }
            let destination = offset.checked_add(span.relative_offset).ok_or_else(|| {
                if original.is_some() {
                    MountSourceError::NativeStaticText {
                        kind: super::NativeSourceTextKind::Invalid,
                        text: "sparse span offset overflows",
                    }
                } else {
                    MountSourceError::Invalid("sparse span offset overflows".to_owned())
                }
            })?;
            self.change_content_locked_with_carrier(
                &mut state, ContentChange::Write { offset: destination, bytes: span.bytes.clone() },
                carrier, &cancellation,
            ).await?;
        }
        Ok(())
    }

    pub(super) fn mutation_epoch_if_idle(&self) -> Option<u64> {
        self.state.try_lock().ok().map(|state| state.mutation_epoch)
    }

    /// Reports the detached identity's live state within a caller's callback.
    pub(super) async fn lookup_async(&self) -> Result<MountLookup, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let state = self.state.lock().await;
        Ok(MountLookup {
            node: MountNode {
                file_id: state.file.file_id(),
                kind: MountNodeKind::Regular,
                logical_bytes: state.file.logical_bytes(),
                link_count: 0,
                device: None,
            },
            metadata: state.metadata,
        })
    }
}

impl<A, O> CheckoutDetachedFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Applies one content change to this detached file with its time
    /// stamp as one record transition, and records it.
    fn change_content(
        &self,
        change: ContentChange<std::convert::Infallible>,
    ) -> Result<(), MountSourceError> {
        self.runtime.wait(|| async {
            let mut state = self.state.lock().await;
            state
                .change_content(change, &self.ledger, &self.cancellation)
                .await
        })
    }
}

impl<A, O> MountOpenFile for CheckoutDetachedFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn lookup_with_meter(&self, work: &super::NativeMountMeterWork) -> Result<MountLookup, MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(|| async {
            let mut carrier = NativeDemandCarrier::Native(work);
            let cancellation = carrier.cancellation();
            let (record, metadata, _) = self.snapshot_with_carrier(&mut carrier, &cancellation).await?;
            let mut node = mount_node(record);
            node.link_count = 0;
            Ok(MountLookup { node, metadata })
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn read_range_with_meter(
        &self, offset: u64, length: u32, work: &super::NativeMountMeterWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(|| async {
            self.read_range_owned_async_with_carrier(
                offset, length, &mut NativeDemandCarrier::Native(work),
            ).await
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn read_up_to_with_meter(
        &self, offset: u64, maximum_bytes: u32, work: &super::NativeMountMeterWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(|| async {
            self.read_owned_async_with_carrier(
                offset, maximum_bytes, true, &mut NativeDemandCarrier::Native(work),
            ).await
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn seek_with_meter(
        &self, offset: u64, target: MountSeekTarget, work: &super::NativeMountMeterWork,
    ) -> Result<Option<u64>, MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(|| async {
            let mut carrier = NativeDemandCarrier::Native(work);
            let cancellation = carrier.cancellation();
            let state = await_measured_lock_with_carrier(
                self.state.lock(), &mut carrier, &cancellation, Some(&self.cancellation),
            ).await?;
            state.file.seek_with_native_work(offset, match target {
                MountSeekTarget::Data => ExtentSeekTarget::Data,
                MountSeekTarget::Hole => ExtentSeekTarget::Hole,
            }, &work.native_materialization_work(), &cancellation).await.map_err(MountSourceError::Filesystem)
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn write_range_with_meter(
        &self, offset: u64, bytes: crate::kernel::OwnedSourceChunk, work: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError> {
        let meter = work.meter();
        let mut carrier = NativeDemandCarrier::Native(work);
        let claim = if bytes.is_intrinsic() { 0 } else { bytes.logical_owned_bytes };
        let mut input = NativeCarrierOwned { value: Some(bytes), carrier: &mut carrier, bytes: claim };
        self.runtime.block_on_measured(|| async {
            let (packet, carrier) = input.parts();
            self.change_content_async_with_carrier(
                ContentChange::Write { offset, bytes: packet.bytes.clone() }, carrier,
            ).await
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn resize_with_meter(
        &self, logical_bytes: u64, work: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(|| async {
            self.change_content_async_with_carrier(
                ContentChange::Resize { logical_bytes }, &mut NativeDemandCarrier::Native(work),
            ).await
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn allocate_range_with_meter(
        &self, offset: u64, length: u64, operation: MountRangeAllocation, work: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(|| async {
            self.change_content_async_with_carrier(
                range_allocation(ByteRange { offset, length }, operation), &mut NativeDemandCarrier::Native(work),
            ).await
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn set_attributes_with_meter(
        &self, metadata: FileMetadata, logical_bytes: Option<u64>, work: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(|| async {
            self.set_attributes_async_with_carrier(
                metadata, logical_bytes, &mut NativeDemandCarrier::Native(work),
            ).await
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn read_attribute_with_meter(
        &self, name: &[u8], work: &super::NativeMountMeterWork,
    ) -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(|| async {
            let original = work.native_materialization_work();
            let owned_name = native_attribute_name_with_native_work(
                self.profile, self.limits, name, &original,
            )?;
            let _name_allocation = owned_name.1;
            let name = owned_name.0;
            let mut carrier = NativeDemandCarrier::Native(work);
            let cancellation = carrier.cancellation();
            let state = await_measured_lock_with_carrier(
                self.state.lock(), &mut carrier, &cancellation, Some(&self.cancellation),
            ).await?;
            let packet = state.file.read_named_attribute_owned_with_native_work(
                &name, &original, &cancellation,
            ).await.map_err(MountSourceError::Filesystem)?;
            original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            Ok(packet)
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn list_attributes_with_meter(
        &self, cursor: Option<&[u8]>, maximum_entries: u32, work: &super::NativeMountMeterWork,
    ) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(|| async {
            let original = work.native_materialization_work();
            let owned_cursor = cursor.map(|name| native_attribute_name_with_native_work(
                self.profile, self.limits, name, &original,
            )).transpose()?;
            let (_cursor_allocation, cursor) = match owned_cursor {
                Some((name, allocation)) => (Some(allocation), Some(name)),
                None => (None, None),
            };
            let mut carrier = NativeDemandCarrier::Native(work);
            let cancellation = carrier.cancellation();
            let state = await_measured_lock_with_carrier(
                self.state.lock(), &mut carrier, &cancellation, Some(&self.cancellation),
            ).await?;
            let (listing, allocation) = state.file.list_named_attributes_with_native_work(
                cursor.as_ref(), maximum_entries, &original, &cancellation,
            ).await.map_err(MountSourceError::Filesystem)?;
            native_attribute_page_with_native_work(
                listing, allocation, &original, Some(&self.cancellation),
            )
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn write_attribute_with_meter(
        &self, name: &[u8], value: crate::kernel::OwnedSourceChunk, mode: MountAttributeWriteMode,
        work: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError> {
        let meter = work.meter();
        let mut carrier = NativeDemandCarrier::Native(work);
        let bytes = if value.is_intrinsic() { 0 } else { value.logical_owned_bytes };
        let mut value = NativeCarrierOwned { value: Some(value), carrier: &mut carrier, bytes };
        self.runtime.block_on_measured(|| async {
            let (packet, carrier) = value.parts();
            self.mutate_attribute_async_with_carrier(name, Some((packet, mode)), carrier).await
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn remove_attribute_with_meter(
        &self, name: &[u8], work: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(|| async {
            self.mutate_attribute_async_with_carrier(
                name, None, &mut NativeDemandCarrier::Native(work),
            ).await
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn settle_with_meter(&self, work: &super::NativeMountMeterWork) -> Result<(), MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(|| async {
            let mut carrier = NativeDemandCarrier::Native(work);
            let cancellation = carrier.cancellation();
            // Detached writes stage immutable objects and install their record
            // before releasing this lock. There is no namespace publication
            // queue; this coherent barrier waits for the actual preceding write.
            let _state = await_measured_lock_with_carrier(
                self.state.lock(), &mut carrier, &cancellation, Some(&self.cancellation),
            ).await?;
            work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED)
                .map_err(MountSourceError::NativeWork)
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn read_sparse_range_with_meter(
        &self, offset: u64, length: u32, work: &super::NativeMountMeterWork,
    ) -> Result<(MountSparseRange, crate::NativeAllocation), MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(|| async {
            self.read_sparse_async_with_native_work(offset, length, work).await
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn write_sparse_range_with_meter(
        &self, offset: u64, range: (MountSparseRange, crate::NativeAllocation), work: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError> {
        let original = work.native_materialization_work();
        if !range.1.work().shares_ledger(&original)
            || !range.1.work().shares_resource_allowance(&original)
        {
            return Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::InvalidLayout));
        }
        original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        let runtime_meter = original.runtime_meter_owner();
        // The tuple drops its physical spans before their paid native guard,
        // after the canonical runtime has retired the accepted callback.
        let mut carrier = NativeDemandCarrier::Native(work);
        self.runtime.block_on_measured(|| async {
            self.write_sparse_async_with_carrier(offset, &range.0, &mut carrier).await
        }, runtime_meter.as_ref())
            .map_err(|error| MountSourceError::NativeWork(original.runtime_error(error)))?
    }

    fn lookup(&self) -> Result<MountLookup, MountSourceError> {
        self.runtime.wait(|| self.lookup_async())
    }

    fn read_range(&self, offset: u64, length: u32) -> Result<OwnedBytes, MountSourceError> {
        self.runtime.wait(|| async {
            let state = self.state.lock().await;
            state
                .file
                .read_range(
                    ByteRange {
                        offset,
                        length: u64::from(length),
                    },
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map(|receipt| receipt.value.bytes)
                .map_err(engine_error)
        })
    }

    fn read_up_to(&self, offset: u64, maximum_bytes: u32) -> Result<OwnedBytes, MountSourceError> {
        self.runtime.wait(|| async {
            let state = self.state.lock().await;
            let length = state
                .file
                .logical_bytes()
                .saturating_sub(offset)
                .min(u64::from(maximum_bytes));
            if length == 0 {
                return Ok(OwnedBytes::new());
            }
            state
                .file
                .read_range(
                    ByteRange { offset, length },
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map(|receipt| receipt.value.bytes)
                .map_err(engine_error)
        })
    }

    fn seek(&self, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError> {
        self.runtime.wait(|| async {
            let state = self.state.lock().await;
            state
                .file
                .seek(
                    offset,
                    match target {
                        MountSeekTarget::Data => ExtentSeekTarget::Data,
                        MountSeekTarget::Hole => ExtentSeekTarget::Hole,
                    },
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(engine_error)
        })
    }

    fn write_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
        self.change_content(ContentChange::Write { offset, bytes: bytes.into() })
    }

    fn resize(&self, logical_bytes: u64) -> Result<(), MountSourceError> {
        self.change_content(ContentChange::Resize { logical_bytes })
    }

    fn allocate_range(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        self.change_content(range_allocation(ByteRange { offset, length }, operation))
    }

    fn set_attributes(
        &self,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        self.runtime.wait(|| async {
            self.set_attributes_async_with_carrier(
                metadata, logical_bytes, &mut NativeDemandCarrier::Borrowed(&mut work),
            ).await
        })
    }

    fn read_attribute(&self, name: &[u8]) -> Result<Option<OwnedBytes>, MountSourceError> {
        let name = self.attribute_name(name)?;
        self.runtime.wait(|| async {
            let state = self.state.lock().await;
            state
                .file
                .read_named_attribute(&name, boundary_budget(), &self.cancellation)
                .await
                .map(|receipt| receipt.value)
                .map_err(engine_error)
        })
    }

    fn list_attributes(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        let cursor = cursor.map(|name| self.attribute_name(name)).transpose()?;
        self.runtime.wait(|| async {
            let state = self.state.lock().await;
            let receipt = state
                .file
                .list_named_attributes(
                    cursor.as_ref(),
                    maximum_entries,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            let has_more = receipt.value.has_more;
            let names = receipt
                .value
                .entries
                .into_iter()
                .map(|entry| {
                    if entry.name.class() != AttributeClass::PosixXattr {
                        return Err(MountSourceError::Unsupported(
                            "POSIX mount encountered a non-POSIX named attribute".to_owned(),
                        ));
                    }
                    Ok(entry.name.as_bytes().to_vec())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let next_cursor = has_more.then(|| names.last().cloned()).flatten();
            Ok(MountAttributePage { names, next_cursor })
        })
    }

    fn write_attribute(
        &self,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        let name = self.attribute_name(name)?;
        let mode = match mode {
            MountAttributeWriteMode::Upsert => NamedAttributeWriteMode::Upsert,
            MountAttributeWriteMode::Create => NamedAttributeWriteMode::Create,
            MountAttributeWriteMode::Replace => NamedAttributeWriteMode::Replace,
        };
        self.runtime.wait(|| async {
            let mut state = self.state.lock().await;
            let receipt = state
                .file
                .write_named_attribute(name, value, mode, boundary_budget(), &self.cancellation)
                .await
                .map_err(facade_error)?;
            state.metadata = receipt.value;
            state.record_change(&self.ledger)
        })
    }

    fn remove_attribute(&self, name: &[u8]) -> Result<(), MountSourceError> {
        let name = self.attribute_name(name)?;
        self.runtime.wait(|| async {
            let mut state = self.state.lock().await;
            let receipt = state
                .file
                .remove_named_attribute(name, boundary_budget(), &self.cancellation)
                .await
                .map_err(facade_error)?;
            state.metadata = receipt.value;
            state.record_change(&self.ledger)
        })
    }
}

/// The handle a creation through the mount opens: until its creation is
/// applied (see [`PendingCreate`]), it holds the file's bytes and facts
/// itself; afterwards it is the checkout's file.
struct PendingCreatedFile<A, O> {
    pending: Arc<PendingCreate>,
    attached: CheckoutAttachedFile<A, O>,
}

impl<A, O> PendingCreatedFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Applies the creation, if it is still pending, and reports how it went.
    fn apply(&self) -> Result<(), MountSourceError> {
        let pending = matches!(&*self.pending.state(), PendingState::Pending { .. });
        if pending {
            self.attached.runtime.wait(|| async {
                self.attached.checkout.settle_pending().await;
                Ok(())
            })?;
        }
        self.pending.applied()
    }

    fn native_view(&self, work: &super::NativeMountMeterWork) -> Result<ViewReadLease, MountSourceError> {
        let original = work.native_materialization_work();
        original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        self.attached.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        let cancellation = work.meter().lock().cancellation().clone();
        let meter = original.runtime_meter_owner();
        self.attached.runtime.block_on_measured(|| async {
            self.attached.checkout.view_gate.read_for_callback_with_carrier(
                ViewGate::callback_owner(), None, &mut NativeDemandCarrier::Native(work),
                &cancellation, Some(&self.attached.cancellation),
            ).await
        }, meter.as_ref()).map_err(|error| MountSourceError::NativeWork(original.runtime_error(error)))?
    }

    fn apply_with_native_work(&self, work: &super::NativeMountMeterWork) -> Result<(), MountSourceError> {
        let original = work.native_materialization_work();
        original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        self.attached.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        if matches!(&*self.pending.state(), PendingState::Pending { .. }) {
            let meter = original.runtime_meter_owner();
            let cancellation = work.meter().lock().cancellation().clone();
            self.attached.runtime.block_on_measured(|| async {
                self.attached.checkout.settle_pending_with_native_work(
                    work, &cancellation, Some(&self.attached.cancellation),
                ).await
            }, meter.as_ref()).map_err(|error| MountSourceError::NativeWork(original.runtime_error(error)))??;
        }
        self.pending.applied()
    }

    fn original_body_work(&self) -> Option<crate::NativeMaterializationWork> {
        match &*self.pending.state() {
            PendingState::Pending { bytes, .. } | PendingState::Applying { bytes, .. } => bytes.original_work(),
            PendingState::Applied | PendingState::Failed => None,
        }
    }

    fn write_held_with_native_work(
        &self, offset: u64, written: &[u8], work: &super::NativeMountMeterWork,
    ) -> Result<bool, MountSourceError> {
        let _view = self.native_view(work)?;
        let mut state = self.pending.state();
        let PendingState::Pending { metadata, bytes } = &mut *state else { return Ok(false); };
        let Some(end) = offset.checked_add(written.len() as u64) else { return Ok(false); };
        if end > self.pending.maximum_bytes { return Ok(false); }
        let (Ok(start), Ok(end), Ok(maximum)) =
            (usize::try_from(offset), usize::try_from(end), usize::try_from(self.pending.maximum_bytes))
        else { return Ok(false); };
        let original = work.native_materialization_work();
        let time = native_content_change_time()?;
        let length = bytes.len().max(end);
        let prepared = bytes.prepare_native_capacity(length, maximum, &original)?;
        let zeros = start.saturating_sub(bytes.len()) as u64;
        let copied = zeros.checked_add(written.len() as u64)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let copy = original.reserve_copy(copied).map_err(MountSourceError::NativeWork)?;
        let notification = self.attached.checkout.ledger
            .prepare_node_change_with_native_work(self.pending.file_id, &original)?;
        if let Some(prepared) = prepared { drop(std::mem::replace(bytes, prepared)); }
        let held = bytes.mutable();
        if held.len() < start { held.resize(start, 0); }
        let overwritten = held.len().saturating_sub(start).min(written.len());
        held[start..start + overwritten].copy_from_slice(&written[..overwritten]);
        held.extend_from_slice(&written[overwritten..]);
        metadata.stamp_content_change(time);
        let recorded = copy.record(copied).map_err(MountSourceError::NativeWork);
        drop(state);
        let _origin = self.pending.origin.enter();
        self.attached.checkout.revision.fetch_add(1, Ordering::AcqRel);
        let notified = self.attached.checkout.ledger.record_node_change_prepared(notification);
        recorded.and(notified).map(|()| true)
    }

    fn read_held_with_native_work(
        &self, offset: u64, length: u32, exact: bool, work: &super::NativeMountMeterWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        {
            let _view = self.native_view(work)?;
            let state = self.pending.state();
            if let PendingState::Pending { bytes, .. } = &*state
                && (!exact || offset.checked_add(u64::from(length)).is_some_and(|end| end <= bytes.len() as u64))
            {
                let start = usize::try_from(offset).unwrap_or(usize::MAX).min(bytes.len());
                let end = usize::try_from(offset.saturating_add(u64::from(length)))
                    .unwrap_or(usize::MAX).min(bytes.len());
                let original = work.native_materialization_work();
                let contents = bytes.get(start..end).unwrap_or_default();
                if contents.is_empty() {
                    original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
                    return Ok(crate::kernel::OwnedSourceChunk::intrinsic(OwnedBytes::new(), 0));
                }
                let (data, allocation) = original.copy_bytes(contents)
                    .map_err(MountSourceError::NativeWork)?;
                let capacity = data.capacity() as u64;
                let data = OwnedBytes::from_vec_with_guard(data, allocation, original.runtime_meter_owner())
                    .map_err(|error| MountSourceError::NativeWork(match error.into_error() {
                        acyclic_stream::PreparedOwnedBytesError::Admission(error) => original.runtime_error(error),
                        acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized
                        | acyclic_stream::PreparedOwnedBytesError::NotUnique => crate::NativeTreeMaterializationError::InvalidLayout,
                    }))?;
                return Ok(crate::kernel::OwnedSourceChunk::intrinsic(data, capacity));
            }
        }
        self.apply_with_native_work(work)?;
        if exact { self.attached.read_range_with_meter(offset, length, work) }
        else { self.attached.read_up_to_with_meter(offset, length, work) }
    }

    /// Runs `held` on the bytes and facts the handle holds while its
    /// creation is pending and they stay within their bound; otherwise
    /// applies the creation and runs `attached` on the checkout's file.
    fn held_or_attached<T>(
        &self,
        held: impl FnOnce(&mut Box<FileMetadata>, &mut PendingBytes, u64) -> Option<T>,
        attached: impl FnOnce(&CheckoutAttachedFile<A, O>) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        {
            let mut state = self.pending.state();
            if let PendingState::Pending { metadata, bytes } = &mut *state
                && let Some(value) = held(metadata, bytes, self.pending.maximum_bytes)
            {
                return Ok(value);
            }
        }
        self.apply()?;
        attached(&self.attached)
    }

    /// Records that the held file changed, as an applied change would.
    fn record_held_change(&self) {
        let _origin = self.pending.origin.enter();
        self.attached
            .checkout
            .ledger
            .record(&ViewChange::Node(self.pending.file_id), &mut NativeDemandCarrier::Borrowed(&mut super::view_ledger::unbounded_view_work()), &mut super::view_ledger::ViewLedgerAdmission::default()).expect("unbounded view recording");
        self.attached
            .checkout
            .revision
            .fetch_add(1, Ordering::AcqRel);
    }

    fn held_lookup(&self, metadata: FileMetadata, logical_bytes: u64) -> MountLookup {
        MountLookup {
            node: MountNode {
                file_id: self.pending.file_id,
                kind: MountNodeKind::Regular,
                logical_bytes,
                link_count: 1,
                device: None,
            },
            metadata,
        }
    }
}

/// Bytes `[offset, offset + length)` of `bytes`, clipped to its end.
fn held_range(bytes: &[u8], offset: u64, length: u64) -> OwnedBytes {
    let start = usize::try_from(offset)
        .unwrap_or(usize::MAX)
        .min(bytes.len());
    let end = usize::try_from(offset.saturating_add(length))
        .unwrap_or(usize::MAX)
        .min(bytes.len());
    OwnedBytes::copy_from_slice(bytes.get(start..end).unwrap_or_default())
}

impl<A, O> MountOpenFile for PendingCreatedFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn lookup_with_meter(&self, work: &super::NativeMountMeterWork) -> Result<MountLookup, MountSourceError> {
        {
            let _view = self.native_view(work)?;
            let state = self.pending.state();
            if let PendingState::Pending { metadata, bytes } = &*state {
                let original = work.native_materialization_work();
                let copy = original.reserve_copy(std::mem::size_of::<FileMetadata>() as u64)
                    .map_err(MountSourceError::NativeWork)?;
                let lookup = self.held_lookup(**metadata, bytes.len() as u64);
                copy.record(std::mem::size_of::<FileMetadata>() as u64).map_err(MountSourceError::NativeWork)?;
                return Ok(lookup);
            }
        }
        self.apply_with_native_work(work)?;
        self.attached.lookup_with_meter(work)
    }

    fn read_range_with_meter(&self, offset: u64, length: u32, work: &super::NativeMountMeterWork)
        -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        self.read_held_with_native_work(offset, length, true, work)
    }

    fn read_up_to_with_meter(&self, offset: u64, maximum_bytes: u32, work: &super::NativeMountMeterWork)
        -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        self.read_held_with_native_work(offset, maximum_bytes, false, work)
    }

    fn seek_with_meter(&self, offset: u64, target: MountSeekTarget, work: &super::NativeMountMeterWork)
        -> Result<Option<u64>, MountSourceError> {
        self.apply_with_native_work(work)?;
        self.attached.seek_with_meter(offset, target, work)
    }

    fn write_range_with_meter(&self, offset: u64, packet: crate::kernel::OwnedSourceChunk,
        work: &super::NativeMountMeterWork) -> Result<(), MountSourceError> {
        let mut carrier = NativeDemandCarrier::Native(work);
        let claim = if packet.is_intrinsic() { 0 } else { packet.logical_owned_bytes };
        let mut retained = NativeCarrierOwned { value: Some(packet), carrier: &mut carrier, bytes: claim };
        if self.write_held_with_native_work(
            offset, &retained.value.as_ref().expect("write input is live").bytes, work,
        )? { return Ok(()); }
        self.apply_with_native_work(work)?;
        let packet = retained.value.take().expect("native input transfers to attached file");
        // The attached producer becomes the sole manual claim owner; intrinsic
        // guards remain inseparable from the transferred packet.
        retained.bytes = 0;
        self.attached.write_range_with_meter(offset, packet, work)
    }

    fn resize_with_meter(&self, logical_bytes: u64, work: &super::NativeMountMeterWork)
        -> Result<(), MountSourceError> {
        {
            let _view = self.native_view(work)?;
            let mut state = self.pending.state();
            if let PendingState::Pending { metadata, bytes } = &mut *state
                && logical_bytes <= self.pending.maximum_bytes
                && let (Ok(length), Ok(maximum)) =
                    (usize::try_from(logical_bytes), usize::try_from(self.pending.maximum_bytes))
            {
                let original = work.native_materialization_work();
                let time = native_content_change_time()?;
                let shared_truncate = matches!(bytes, PendingBytes::Shared { .. }) && length <= bytes.len();
                let prepared = if shared_truncate { None }
                    else { bytes.prepare_native_capacity(length, maximum, &original)? };
                let copied = length.saturating_sub(bytes.len()) as u64;
                let copy = original.reserve_copy(copied).map_err(MountSourceError::NativeWork)?;
                let notification = self.attached.checkout.ledger
                    .prepare_node_change_with_native_work(self.pending.file_id, &original)?;
                if let Some(prepared) = prepared { drop(std::mem::replace(bytes, prepared)); }
                if shared_truncate {
                    let PendingBytes::Shared { bytes, .. } = bytes else { unreachable!("original shared body"); };
                    *bytes = bytes.slice(..length);
                } else {
                    bytes.resize(length, 0);
                }
                metadata.stamp_content_change(time);
                let recorded = copy.record(copied).map_err(MountSourceError::NativeWork);
                drop(state);
                let _origin = self.pending.origin.enter();
                self.attached.checkout.revision.fetch_add(1, Ordering::AcqRel);
                let notified = self.attached.checkout.ledger.record_node_change_prepared(notification);
                return recorded.and(notified);
            }
        }
        self.apply_with_native_work(work)?;
        self.attached.resize_with_meter(logical_bytes, work)
    }

    fn allocate_range_with_meter(&self, offset: u64, length: u64, operation: MountRangeAllocation,
        work: &super::NativeMountMeterWork) -> Result<(), MountSourceError> {
        self.apply_with_native_work(work)?;
        self.attached.allocate_range_with_meter(offset, length, operation, work)
    }

    fn set_attributes_with_meter(&self, metadata: FileMetadata, logical_bytes: Option<u64>,
        work: &super::NativeMountMeterWork) -> Result<(), MountSourceError> {
        self.apply_with_native_work(work)?;
        self.attached.set_attributes_with_meter(metadata, logical_bytes, work)
    }

    fn read_attribute_with_meter(&self, name: &[u8], work: &super::NativeMountMeterWork)
        -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError> {
        self.apply_with_native_work(work)?;
        self.attached.read_attribute_with_meter(name, work)
    }

    fn list_attributes_with_meter(&self, cursor: Option<&[u8]>, maximum_entries: u32,
        work: &super::NativeMountMeterWork) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError> {
        self.apply_with_native_work(work)?;
        self.attached.list_attributes_with_meter(cursor, maximum_entries, work)
    }

    fn write_attribute_with_meter(&self, name: &[u8], value: crate::kernel::OwnedSourceChunk,
        mode: MountAttributeWriteMode, work: &super::NativeMountMeterWork) -> Result<(), MountSourceError> {
        self.apply_with_native_work(work)?;
        self.attached.write_attribute_with_meter(name, value, mode, work)
    }

    fn remove_attribute_with_meter(&self, name: &[u8], work: &super::NativeMountMeterWork)
        -> Result<(), MountSourceError> {
        self.apply_with_native_work(work)?;
        self.attached.remove_attribute_with_meter(name, work)
    }

    fn settle_with_meter(&self, work: &super::NativeMountMeterWork) -> Result<(), MountSourceError> {
        self.apply_with_native_work(work)
    }

    fn read_sparse_range_with_meter(&self, offset: u64, length: u32, work: &super::NativeMountMeterWork)
        -> Result<(super::MountSparseRange, crate::NativeAllocation), MountSourceError> {
        self.apply_with_native_work(work)?;
        self.attached.read_sparse_range_with_meter(offset, length, work)
    }

    fn write_sparse_range_with_meter(&self, offset: u64, range: (super::MountSparseRange, crate::NativeAllocation),
        work: &super::NativeMountMeterWork) -> Result<(), MountSourceError> {
        self.apply_with_native_work(work)?;
        self.attached.write_sparse_range_with_meter(offset, range, work)
    }

    fn lookup(&self) -> Result<MountLookup, MountSourceError> {
        self.held_or_attached(
            |metadata, bytes, _| Some(self.held_lookup(**metadata, bytes.len() as u64)),
            MountOpenFile::lookup,
        )
    }

    fn read_range(&self, offset: u64, length: u32) -> Result<OwnedBytes, MountSourceError> {
        self.held_or_attached(
            |_, bytes, _| {
                let end = offset.checked_add(u64::from(length))?;
                (end <= bytes.len() as u64).then(|| held_range(bytes, offset, u64::from(length)))
            },
            |file| file.read_range(offset, length),
        )
    }

    fn read_up_to(&self, offset: u64, maximum_bytes: u32) -> Result<OwnedBytes, MountSourceError> {
        self.held_or_attached(
            |_, bytes, _| Some(held_range(bytes, offset, u64::from(maximum_bytes))),
            |file| file.read_up_to(offset, maximum_bytes),
        )
    }

    fn seek(&self, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError> {
        self.apply()?;
        self.attached.seek(offset, target)
    }

    fn write_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
        if let Some(original) = self.original_body_work() {
            let work = original.native_mount_work();
            if self.write_held_with_native_work(offset, &bytes, &work)? { return Ok(()); }
            self.apply_with_native_work(&work)?;
            // The ordinary input is borrowed caller storage, not a new native
            // allocation. Only a backend-retained native copy needs a new guard.
            let (data, allocation) = original.copy_bytes(&bytes).map_err(MountSourceError::NativeWork)?;
            let capacity = data.capacity() as u64;
            let data = OwnedBytes::from_vec_with_guard(data, allocation, original.runtime_meter_owner())
                .map_err(|error| MountSourceError::NativeWork(match error.into_error() {
                        acyclic_stream::PreparedOwnedBytesError::Admission(error) => original.runtime_error(error),
                        acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized
                        | acyclic_stream::PreparedOwnedBytesError::NotUnique => crate::NativeTreeMaterializationError::InvalidLayout,
                    }))?;
            return self.attached.write_range_with_meter(
                offset, crate::kernel::OwnedSourceChunk::intrinsic(data, capacity), &work,
            );
        }
        let time = content_change_time()?;
        let written = bytes.clone();
        let held = self.held_or_attached(
            |metadata, held, maximum| {
                let end = offset.checked_add(written.len() as u64)?;
                if end > maximum {
                    return None;
                }
                let start = usize::try_from(offset).ok()?;
                let end = usize::try_from(end).ok()?;
                if held.len() < end {
                    held.resize(end, 0);
                }
                held.get_mut(start..end)?.copy_from_slice(&written);
                metadata.stamp_content_change(time);
                Some(true)
            },
            |file| file.write_range(offset, bytes).map(|()| false),
        )?;
        if held {
            self.record_held_change();
        }
        Ok(())
    }

    fn resize(&self, logical_bytes: u64) -> Result<(), MountSourceError> {
        if let Some(original) = self.original_body_work() {
            return self.resize_with_meter(logical_bytes, &original.native_mount_work());
        }
        let time = content_change_time()?;
        let held = self.held_or_attached(
            |metadata, held, maximum| {
                if logical_bytes > maximum {
                    return None;
                }
                held.resize(usize::try_from(logical_bytes).ok()?, 0);
                metadata.stamp_content_change(time);
                Some(true)
            },
            |file| file.resize(logical_bytes).map(|()| false),
        )?;
        if held {
            self.record_held_change();
        }
        Ok(())
    }

    fn allocate_range(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        self.apply()?;
        self.attached.allocate_range(offset, length, operation)
    }

    fn set_attributes(
        &self,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        self.apply()?;
        self.attached.set_attributes(metadata, logical_bytes)
    }

    fn read_attribute(&self, name: &[u8]) -> Result<Option<OwnedBytes>, MountSourceError> {
        self.apply()?;
        self.attached.read_attribute(name)
    }

    fn list_attributes(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        self.apply()?;
        self.attached.list_attributes(cursor, maximum_entries)
    }

    fn write_attribute(
        &self,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        self.apply()?;
        self.attached.write_attribute(name, value, mode)
    }

    fn remove_attribute(&self, name: &[u8]) -> Result<(), MountSourceError> {
        self.apply()?;
        self.attached.remove_attribute(name)
    }

    fn settle(&self) -> Result<(), MountSourceError> {
        self.apply()
    }
}

impl<A, O> CheckoutAttachedFile<A, O> {
    fn attribute_name(&self, bytes: &[u8]) -> Result<AttributeName, MountSourceError> {
        if self.profile != FilesystemProfile::Posix {
            return Err(MountSourceError::Unsupported(
                "native POSIX attributes require a POSIX volume profile".to_owned(),
            ));
        }
        AttributeName::new(
            AttributeClass::PosixXattr,
            bytes.to_vec(),
            self.limits.maximum_component_bytes,
        )
        .map_err(engine_error)
    }
}

impl<A, O> CheckoutAttachedFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    async fn measured_observer<'w>(
        &self,
        mut carrier: NativeDemandCarrier<'w>,
    ) -> Result<(ViewReadLease, LookupObserverOwned<'static, 'w, A, O>), MountSourceError> {
        let cancellation = carrier.with_work(|work| {
            work.check().map_err(super::lazy::mount_demand_error)?;
            Ok::<_, MountSourceError>(work.cancellation().clone())
        })?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        let observation = self.checkout.observe_owned_with_carrier(
            ViewGate::callback_owner(), &mut carrier, &cancellation, Some(&self.cancellation),
        ).await?;
        let observer = LookupObserverOwned::new_in(observation.observer, carrier)?;
        Ok((observation._view, observer))
    }

    fn accept_observed<T, E>(
        observer: &mut LookupObserverOwned<'_, '_, A, O>,
        result: crate::MeasuredResult<crate::OperationReceipt<T>, E>,
        map: impl FnOnce(E) -> MountSourceError,
    ) -> Result<T, MountSourceError> {
        let accepted = observer.work.with_work(|work| super::lazy::mount_accept(work, result, map));
        observer.receipt_delivered();
        accepted
    }

    fn retain_read<'a, 'w>(
        packet: crate::kernel::OwnedSourceChunk,
        carrier: &'a mut NativeDemandCarrier<'w>,
    ) -> Result<NativeCarrierOwned<'a, 'w, crate::kernel::OwnedSourceChunk>, MountSourceError> {
        use super::lazy::mount_demand_error;
        let bytes = packet.logical_owned_bytes;
        let retained = NativeCarrierOwned { value: Some(packet), carrier, bytes };
        retained.carrier.with_work(|work| {
            work.record_adoption(bytes).and_then(|()| work.check())
        }).map_err(mount_demand_error)?;
        Ok(retained)
    }

    async fn read_owned_with_carrier(
        &self,
        offset: u64,
        length: u32,
        up_to: bool,
        carrier: NativeDemandCarrier<'_>,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        use super::lazy::{mount_demand_error, mount_engine_error};
        let (_view, mut observer) = self.measured_observer(carrier).await?;
        let cancellation = observer.work.cancellation();
        if let Some(original) = observer.work.original_native_work() {
            let read = if up_to {
                observer.read_file_up_to_by_id_with_native_work(
                    self.file_id, offset, length, &original, &cancellation,
                ).await
            } else {
                observer.read_file_range_by_id_with_native_work(
                    self.file_id, ByteRange { offset, length: u64::from(length) },
                    &original, &cancellation,
                ).await
            };
            let finished = observer.finish();
            let read = read.map_err(MountSourceError::Filesystem)?;
            finished?;
            observer.work.with_work(|work| work.check()).map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            let logical_owned_bytes = read.logical_owned_bytes();
            return Ok(crate::kernel::OwnedSourceChunk::intrinsic(read.bytes, logical_owned_bytes));
        }
        let budget = observer.work.with_work(|work| work.remaining().map_err(mount_demand_error))?;
        let read = if up_to {
            observer.read_file_up_to_by_id(self.file_id, offset, length, budget, &cancellation).await
        } else {
            observer.read_file_range_by_id(
                self.file_id, ByteRange { offset, length: u64::from(length) }, budget, &cancellation,
            ).await
        };
        let read = Self::accept_observed(&mut observer, read, mount_engine_error);
        let finished = observer.finish();
        let read = read?;
        let logical_owned_bytes = read.logical_owned_bytes();
        let mut output = Self::retain_read(
            crate::kernel::OwnedSourceChunk::claimed(read.bytes, logical_owned_bytes), &mut observer.work,
        )?;
        finished?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        output.bytes = 0;
        Ok(output.value.take().expect("attached read output remains live"))
    }

    async fn change_content_with_carrier(
        &self,
        change: ContentChange<FileId>,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        let cancellation = carrier.cancellation();
        self.checkout.change_content_by_id_with_carrier(
            self.file_id, change, carrier, &cancellation, Some(&self.cancellation),
            self.native_callback_guard.as_ref(),
        ).await
    }

    /// Applies one content change to this file with its time stamp as one
    /// mutation.
    fn change_content(&self, change: ContentChange<FileId>) -> Result<(), MountSourceError> {
        let change = GroupedChange::Content {
            file_id: self.file_id,
            change,
            times: ContentTimes::Stamp(content_change_time()?),
        };
        self.runtime.wait(|| async {
            self.checkout
                .apply_grouped(change, &self.cancellation, self.native_callback_guard.as_ref().map(Arc::clone))
                .await
                .map(|_| ())
        })
    }
}

impl<A, O> MountOpenFile for CheckoutAttachedFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn lookup_with_meter(&self, work: &super::NativeMountMeterWork) -> Result<MountLookup, MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(|| async {
            use super::lazy::mount_demand_error;
            let (_view, mut observer) = self.measured_observer(NativeDemandCarrier::Native(work)).await?;
            let cancellation = meter.lock().cancellation().clone();
            let result: Result<MountLookup, MountSourceError> = async {
                let original = work.native_materialization_work();
                let record = observer.read_file_record_by_id_with_native_work(
                    self.file_id, &original, &cancellation,
                ).await.map_err(MountSourceError::Filesystem)?;
                let metadata = observer.read_metadata_by_id_with_native_work(
                    self.file_id, &original, &cancellation,
                ).await.map_err(MountSourceError::Filesystem)?;
                meter.lock().check().map_err(mount_demand_error)?;
                self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
                Ok(MountLookup { node: mount_node(record), metadata })
            }.await;
            let finished = observer.finish();
            let value = result?;
            finished?;
            Ok(value)
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn read_range_with_meter(&self, offset: u64, length: u32, work: &super::NativeMountMeterWork)
        -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(
            || self.read_owned_with_carrier(offset, length, false, NativeDemandCarrier::Native(work)), meter.as_ref(),
        ).map_err(capture_io_error)?
    }

    fn read_up_to_with_meter(&self, offset: u64, maximum_bytes: u32, work: &super::NativeMountMeterWork)
        -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(
            || self.read_owned_with_carrier(offset, maximum_bytes, true, NativeDemandCarrier::Native(work)), meter.as_ref(),
        ).map_err(capture_io_error)?
    }

    fn seek_with_meter(&self, offset: u64, target: MountSeekTarget, work: &super::NativeMountMeterWork)
        -> Result<Option<u64>, MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(|| async {
            use super::lazy::mount_demand_error;
            let (_view, mut observer) = self.measured_observer(NativeDemandCarrier::Native(work)).await?;
            let cancellation = meter.lock().cancellation().clone();
            let original = work.native_materialization_work();
            let accepted = observer.seek_file_extent_by_id_with_native_work(self.file_id, offset, match target {
                MountSeekTarget::Data => ExtentSeekTarget::Data,
                MountSeekTarget::Hole => ExtentSeekTarget::Hole,
            }, &original, &cancellation).await.map_err(MountSourceError::Filesystem);
            let finished = observer.finish();
            let value = accepted?;
            finished?;
            meter.lock().check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            Ok(value)
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn write_range_with_meter(&self, offset: u64, bytes: crate::kernel::OwnedSourceChunk,
        work: &super::NativeMountMeterWork) -> Result<(), MountSourceError> {
        let meter = work.meter();
        let mut carrier = NativeDemandCarrier::Native(work);
        // Only the manually held ledger portion is released here; intrinsic
        // payload capacity remains truthful and owned by the packet's guard.
        let claim = if bytes.is_intrinsic() { 0 } else { bytes.logical_owned_bytes };
        let mut input = NativeCarrierOwned { value: Some(bytes), carrier: &mut carrier, bytes: claim };
        self.runtime.block_on_measured(|| async {
            let (packet, carrier) = input.parts();
            self.change_content_with_carrier(
                ContentChange::Write { offset, bytes: packet.bytes.clone() }, carrier,
            ).await
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn resize_with_meter(&self, logical_bytes: u64, work: &super::NativeMountMeterWork)
        -> Result<(), MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(|| async {
            self.change_content_with_carrier(
                ContentChange::Resize { logical_bytes }, &mut NativeDemandCarrier::Native(work),
            ).await
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn allocate_range_with_meter(&self, offset: u64, length: u64, operation: MountRangeAllocation,
        work: &super::NativeMountMeterWork) -> Result<(), MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(|| async {
            self.change_content_with_carrier(
                range_allocation(ByteRange { offset, length }, operation),
                &mut NativeDemandCarrier::Native(work),
            ).await
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn set_attributes_with_meter(&self, metadata: FileMetadata, logical_bytes: Option<u64>,
        work: &super::NativeMountMeterWork) -> Result<(), MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(|| async {
            let cancellation = meter.lock().cancellation().clone();
            self.checkout.set_attributes_by_id_with_carrier(
                self.file_id, metadata, logical_bytes, &mut NativeDemandCarrier::Native(work),
                &cancellation, Some(&self.cancellation),
                self.native_callback_guard.as_ref(),
            ).await
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn read_attribute_with_meter(&self, name: &[u8], work: &super::NativeMountMeterWork)
        -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(|| async {
            let original = work.native_materialization_work();
            let owned_name = native_attribute_name_with_native_work(
                self.profile, self.limits, name, &original,
            )?;
            let _name_allocation = owned_name.1;
            let name = owned_name.0;
            let (_view, mut observer) = self.measured_observer(NativeDemandCarrier::Native(work)).await?;
            let cancellation = meter.lock().cancellation().clone();
            let accepted = observer.read_named_attribute_owned_by_id_with_native_work(
                self.file_id, &name,
                &original, &cancellation,
            ).await.map_err(MountSourceError::Filesystem);
            let finished = observer.finish();
            let output = accepted?;
            finished?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            Ok(output)
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn list_attributes_with_meter(&self, cursor: Option<&[u8]>, maximum_entries: u32,
        work: &super::NativeMountMeterWork) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(|| async {
            use super::lazy::mount_demand_error;
            let original = work.native_materialization_work();
            let cursor = cursor.map(|name| native_attribute_name_with_native_work(
                self.profile, self.limits, name, &original,
            )).transpose()?;
            let (_view, mut observer) = self.measured_observer(NativeDemandCarrier::Native(work)).await?;
            let cancellation = meter.lock().cancellation().clone();
            let accepted = observer.list_named_attributes_by_id_with_native_work(
                self.file_id, cursor.as_ref().map(|(name, _allocation)| name),
                maximum_entries, &original, &cancellation,
            ).await.map_err(MountSourceError::Filesystem);
            let finished = observer.finish();
            let (listing, allocation) = accepted?;
            let page = native_attribute_page_with_native_work(
                listing, allocation, &original, Some(&self.cancellation),
            )?;
            finished?;
            meter.lock().check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            Ok(page)
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn write_attribute_with_meter(&self, name: &[u8], value: crate::kernel::OwnedSourceChunk,
        mode: MountAttributeWriteMode, work: &super::NativeMountMeterWork) -> Result<(), MountSourceError> {
        let meter = work.meter();
        let mut carrier = NativeDemandCarrier::Native(work);
        // Intrinsic packet guards, not this manual ledger portion, own native capacity.
        let bytes = if value.is_intrinsic() { 0 } else { value.logical_owned_bytes };
        let mut input = NativeCarrierOwned { value: Some(value), carrier: &mut carrier, bytes };
        self.runtime.block_on_measured(|| async {
            let (packet, carrier) = input.parts();
            let original = work.native_materialization_work();
            let owned_name = native_attribute_name_with_native_work(
                self.profile, self.limits, name, &original,
            )?;
            let _name_allocation = owned_name.1;
            let name = owned_name.0;
            let cancellation = carrier.cancellation();
            self.checkout.mutate_named_attribute_by_id_with_carrier(
                self.file_id, name, Some((packet.bytes.clone(), match mode {
                    MountAttributeWriteMode::Upsert => NamedAttributeWriteMode::Upsert,
                    MountAttributeWriteMode::Create => NamedAttributeWriteMode::Create,
                    MountAttributeWriteMode::Replace => NamedAttributeWriteMode::Replace,
                })), carrier, &cancellation, Some(&self.cancellation),
                self.native_callback_guard.as_ref(),
            ).await
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn remove_attribute_with_meter(&self, name: &[u8], work: &super::NativeMountMeterWork)
        -> Result<(), MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(|| async {
            let mut carrier = NativeDemandCarrier::Native(work);
            let original = work.native_materialization_work();
            let owned_name = native_attribute_name_with_native_work(
                self.profile, self.limits, name, &original,
            )?;
            let _name_allocation = owned_name.1;
            let name = owned_name.0;
            let cancellation = carrier.cancellation();
            self.checkout.mutate_named_attribute_by_id_with_carrier(
                self.file_id, name, None, &mut carrier, &cancellation, Some(&self.cancellation),
                self.native_callback_guard.as_ref(),
            ).await
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn settle_with_meter(&self, work: &super::NativeMountMeterWork) -> Result<(), MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(|| async {
            let cancellation = meter.lock().cancellation().clone();
            self.checkout.settle_with_carrier(
                &mut NativeDemandCarrier::Native(work), &cancellation, Some(&self.cancellation),
            ).await
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn read_sparse_range_with_meter(&self, offset: u64, length: u32, work: &super::NativeMountMeterWork)
        -> Result<(super::MountSparseRange, crate::NativeAllocation), MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(|| async {
            use super::lazy::mount_demand_error;
            let logical_bytes = u64::from(length);
            let end = offset.checked_add(logical_bytes).ok_or(MountSourceError::NativeStaticText {
                kind: super::NativeSourceTextKind::Invalid,
                text: "sparse range overflows the file address space",
            })?;
            let (_view, mut observer) = self.measured_observer(NativeDemandCarrier::Native(work)).await?;
            let cancellation = meter.lock().cancellation().clone();
            let original = work.native_materialization_work();
            let mut scratch = NativeCaptureScratchAllocation::new(&original)?;
            let mut output = super::MountSparseRange { logical_bytes, spans: Vec::new() };
            let result = async {
                let mut cursor = offset;
                while cursor < end {
                    let Some(data_offset) = observer.seek_file_extent_by_id_with_native_work(
                        self.file_id, cursor, ExtentSeekTarget::Data, &original, &cancellation,
                    ).await.map_err(MountSourceError::Filesystem)? else { break; };
                    if data_offset >= end { break; }
                    if data_offset < cursor {
                        return Err(MountSourceError::NativeStaticText {
                            kind: super::NativeSourceTextKind::Engine,
                            text: "sparse seek moved backwards",
                        });
                    }
                    let hole_offset = observer.seek_file_extent_by_id_with_native_work(
                        self.file_id, data_offset, ExtentSeekTarget::Hole, &original, &cancellation,
                    ).await.map_err(MountSourceError::Filesystem)?.unwrap_or(end).min(end);
                    if hole_offset <= data_offset {
                        return Err(MountSourceError::NativeStaticText {
                            kind: super::NativeSourceTextKind::Engine,
                            text: "sparse seek made no forward progress",
                        });
                    }
                    let spans = &mut output.spans;
                    if spans.len() == spans.capacity() {
                        let old_bytes = spans.capacity().checked_mul(std::mem::size_of::<super::MountSparseSpan>())
                            .and_then(|bytes| u64::try_from(bytes).ok())
                            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
                        let requested = spans.capacity().checked_mul(2)
                            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?.max(1);
                        let mut grown = scratch.vec::<super::MountSparseSpan>(requested)?;
                        scratch.charge(crate::WorkCounters {
                            bytes_copied: (spans.len() * std::mem::size_of::<super::MountSparseSpan>()) as u64,
                            ..crate::WorkCounters::UNCHARGED
                        })?;
                        grown.append(spans);
                        drop(std::mem::replace(spans, grown));
                        scratch.release(old_bytes)?;
                    }
                    scratch.charge(crate::WorkCounters {
                        items_examined: 1,
                        bytes_copied: std::mem::size_of::<super::MountSparseSpan>() as u64,
                        ..crate::WorkCounters::UNCHARGED
                    })?;
                    let read = observer.read_file_range_by_id_with_native_work(self.file_id, ByteRange {
                        offset: data_offset, length: hole_offset - data_offset,
                    }, &original, &cancellation).await.map_err(MountSourceError::Filesystem)?;
                    if read.bytes.len() as u64 != hole_offset - data_offset {
                        return Err(MountSourceError::Stale);
                    }
                    output.spans.push(super::MountSparseSpan {
                        relative_offset: data_offset - offset,
                        bytes: read.bytes,
                    });
                    cursor = hole_offset;
                }
                meter.lock().check().map_err(mount_demand_error)?;
                self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
                Ok::<_, MountSourceError>(())
            }.await;
            let finished = observer.finish();
            result?;
            finished?;
            Ok((output, scratch.retain()))
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn write_sparse_range_with_meter(&self, offset: u64, range: (super::MountSparseRange, crate::NativeAllocation),
        work: &super::NativeMountMeterWork) -> Result<(), MountSourceError> {
        let original = work.native_materialization_work();
        if !range.1.work().shares_ledger(&original)
            || !range.1.work().shares_resource_allowance(&original)
        {
            return Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::InvalidLayout));
        }
        original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        let runtime_meter = original.runtime_meter_owner();
        // Keep data and its paid guard together through the physical terminal.
        let mut carrier = NativeDemandCarrier::Native(work);
        self.runtime.block_on_measured(|| async {
            let cancellation = carrier.cancellation();
            self.checkout.write_sparse_range_by_id_with_carrier(
                self.file_id, offset, &range.0, &mut carrier, &cancellation, Some(&self.cancellation),
                self.native_callback_guard.as_ref(),
            ).await
        }, runtime_meter.as_ref())
            .map_err(|error| MountSourceError::NativeWork(original.runtime_error(error)))?
    }

    fn lookup(&self) -> Result<MountLookup, MountSourceError> {
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| async {
            let observation = self.checkout.observe(owner).await?;
            let mut checkout = observation.observer();
            let record = checkout
                .read_file_record_by_id(self.file_id, boundary_budget(), &self.cancellation)
                .await
                .map_err(engine_error)?
                .value;
            let metadata = checkout
                .read_metadata_by_id(self.file_id, boundary_budget(), &self.cancellation)
                .await
                .map_err(engine_error)?
                .value;
            Ok(MountLookup {
                node: mount_node(record),
                metadata,
            })
        })
    }

    fn read_range(&self, offset: u64, length: u32) -> Result<OwnedBytes, MountSourceError> {
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| async {
            let observation = self.checkout.observe(owner).await?;
            let mut checkout = observation.observer();
            checkout
                .read_file_range_by_id(
                    self.file_id,
                    ByteRange {
                        offset,
                        length: u64::from(length),
                    },
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map(|receipt| receipt.value.bytes)
                .map_err(engine_error)
        })
    }

    fn read_up_to(&self, offset: u64, maximum_bytes: u32) -> Result<OwnedBytes, MountSourceError> {
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| async {
            let observation = self.checkout.observe(owner).await?;
            let mut checkout = observation.observer();
            checkout
                .read_file_up_to_by_id(
                    self.file_id,
                    offset,
                    maximum_bytes,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map(|receipt| receipt.value.bytes)
                .map_err(engine_error)
        })
    }

    fn seek(&self, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError> {
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| async {
            let observation = self.checkout.observe(owner).await?;
            let mut checkout = observation.observer();
            checkout
                .seek_file_extent_by_id(
                    self.file_id,
                    offset,
                    match target {
                        MountSeekTarget::Data => ExtentSeekTarget::Data,
                        MountSeekTarget::Hole => ExtentSeekTarget::Hole,
                    },
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(engine_error)
        })
    }

    fn write_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
        self.change_content(ContentChange::Write { offset, bytes: bytes.into() })
    }

    fn resize(&self, logical_bytes: u64) -> Result<(), MountSourceError> {
        self.change_content(ContentChange::Resize { logical_bytes })
    }

    fn allocate_range(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        self.change_content(range_allocation(ByteRange { offset, length }, operation))
    }

    fn set_attributes(
        &self,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| {
            let mut carrier = NativeDemandCarrier::Shared(meter);
            self.runtime.block_on_measured(|| async {
                self.checkout.set_attributes_by_id_with_carrier(
                    self.file_id, metadata, logical_bytes, &mut carrier,
                    &self.cancellation, Some(&self.cancellation),
                    self.native_callback_guard.as_ref(),
                ).await
            }, meter.as_ref()).map_err(capture_io_error)?
        })
    }

    fn read_attribute(&self, name: &[u8]) -> Result<Option<OwnedBytes>, MountSourceError> {
        let name = self.attribute_name(name)?;
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| async {
            let observation = self.checkout.observe(owner).await?;
            let mut checkout = observation.observer();
            checkout
                .read_named_attribute_by_id(
                    self.file_id,
                    &name,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(engine_error)
        })
    }

    fn list_attributes(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        let cursor = cursor.map(|name| self.attribute_name(name)).transpose()?;
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| async {
            let observation = self.checkout.observe(owner).await?;
            let mut checkout = observation.observer();
            let receipt = checkout
                .list_named_attributes_by_id(
                    self.file_id,
                    cursor.as_ref(),
                    maximum_entries,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            let has_more = receipt.value.has_more;
            let names = receipt
                .value
                .entries
                .into_iter()
                .map(|entry| {
                    if entry.name.class() != AttributeClass::PosixXattr {
                        return Err(MountSourceError::Unsupported(
                            "POSIX mount encountered a non-POSIX named attribute".to_owned(),
                        ));
                    }
                    Ok(entry.name.as_bytes().to_vec())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let next_cursor = has_more.then(|| names.last().cloned()).flatten();
            Ok(MountAttributePage { names, next_cursor })
        })
    }

    fn write_attribute(
        &self,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| {
            let mut carrier = NativeDemandCarrier::Shared(meter);
            let name = carrier.with_work(|work| native_attribute_name_with_work(
                self.profile, self.limits, name, work,
            ))?;
            let bytes = name.retained_bytes() as u64;
            let mut name = NativeCarrierOwned { value: Some(name), carrier: &mut carrier, bytes };
            let (value, bytes) = name.carrier.with_work(|work| {
                let mut scratch = CaptureScratchAllocation::new(work);
                let mut bytes = scratch.vec::<u8>(value.len())?;
                scratch.work.charge(crate::WorkCounters {
                    bytes_copied: value.len() as u64,
                    ..crate::WorkCounters::UNCHARGED
                }).map_err(super::lazy::mount_demand_error)?;
                bytes.extend_from_slice(&value);
                let retained = scratch.retain();
                Ok::<_, MountSourceError>((OwnedBytes::from(bytes), retained))
            })?;
            let mut value = NativeCarrierOwned { value: Some(value), carrier: &mut *name.carrier, bytes };
            let mode = match mode {
                MountAttributeWriteMode::Upsert => NamedAttributeWriteMode::Upsert,
                MountAttributeWriteMode::Create => NamedAttributeWriteMode::Create,
                MountAttributeWriteMode::Replace => NamedAttributeWriteMode::Replace,
            };
            self.runtime.block_on_measured(|| async {
                let admitted_name = name.value.take().expect("native attribute name is live");
                name.bytes = 0;
                let (value, carrier) = value.parts();
                self.checkout.mutate_named_attribute_by_id_with_carrier(
                    self.file_id, admitted_name, Some((value.clone(), mode)), carrier,
                    &self.cancellation, Some(&self.cancellation),
                    self.native_callback_guard.as_ref(),
                ).await
            }, meter.as_ref()).map_err(capture_io_error)?
        })
    }

    fn remove_attribute(&self, name: &[u8]) -> Result<(), MountSourceError> {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| {
            let mut carrier = NativeDemandCarrier::Shared(meter);
            let name = carrier.with_work(|work| native_attribute_name_with_work(
                self.profile, self.limits, name, work,
            ))?;
            let bytes = name.retained_bytes() as u64;
            let mut name = NativeCarrierOwned { value: Some(name), carrier: &mut carrier, bytes };
            self.runtime.block_on_measured(|| async {
                let admitted_name = name.value.take().expect("native attribute name is live");
                name.bytes = 0;
                self.checkout.mutate_named_attribute_by_id_with_carrier(
                    self.file_id, admitted_name, None, name.carrier,
                    &self.cancellation, Some(&self.cancellation),
                    self.native_callback_guard.as_ref(),
                ).await
            }, meter.as_ref()).map_err(capture_io_error)?
        })
    }
}

/// Encodes one authenticated name as this host's native mount component.
pub(super) fn native_mount_name(name: &LogicalName) -> Result<Vec<u8>, MountSourceError> {
    match (name.encoding(), std::env::consts::OS) {
        (NameEncoding::Utf8 | NameEncoding::PosixBytes, "linux" | "macos") => {
            Ok(name.as_bytes().to_vec())
        }
        (NameEncoding::WindowsUtf16Le, "windows") => Ok(name.as_bytes().to_vec()),
        (NameEncoding::Utf8, "windows") => {
            let text = std::str::from_utf8(name.as_bytes()).map_err(|_| {
                MountSourceError::Invalid("authenticated UTF-8 name is malformed".to_owned())
            })?;
            Ok(text.encode_utf16().flat_map(u16::to_le_bytes).collect())
        }
        _ => Err(MountSourceError::Unsupported(
            "authenticated name encoding is incompatible with this native mount".to_owned(),
        )),
    }
}

fn native_mount_name_with_scratch<S: CaptureInputScratch>(
    name: &LogicalName,
    scratch: &mut S,
) -> Result<Vec<u8>, NativeNameProductionError> {
    scratch.items(1)?;
    match (name.encoding(), std::env::consts::OS) {
        (NameEncoding::Utf8 | NameEncoding::PosixBytes, "linux" | "macos")
        | (NameEncoding::WindowsUtf16Le, "windows") => {
            let delta = crate::WorkCounters {
                bytes_copied: name.as_bytes().len() as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            scratch.admit(delta)?;
            let mut bytes = scratch.vec(name.as_bytes().len())?;
            bytes.extend_from_slice(name.as_bytes());
            scratch.record(delta)?;
            Ok(bytes)
        }
        (NameEncoding::Utf8, "windows") => {
            let text = std::str::from_utf8(name.as_bytes()).map_err(|_| {
                NativeNameProductionError::Reason(
                    super::NativeSourceTextKind::Invalid,
                    "authenticated UTF-8 name is malformed",
                )
            })?;
            let mut length = 0_usize;
            for scalar in text.chars() {
                scratch.items(1)?;
                length = length.checked_add(scalar.len_utf16() * 2)
                    .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            }
            let mut bytes = scratch.vec(length)?;
            for unit in text.encode_utf16() {
                let delta = crate::WorkCounters {
                    items_examined: 1, bytes_encoded: 2,
                    ..crate::WorkCounters::UNCHARGED
                };
                scratch.admit(delta)?;
                bytes.extend_from_slice(&unit.to_le_bytes());
                scratch.record(delta)?;
            }
            Ok(bytes)
        }
        _ => Err(NativeNameProductionError::Reason(
            super::NativeSourceTextKind::Unsupported,
            "authenticated name encoding is incompatible with this native mount",
        )),
    }
}

pub(super) fn native_mount_name_with_work(
    name: &LogicalName,
    work: &mut crate::demand::DemandWork,
) -> Result<Vec<u8>, MountSourceError> {
    let result = {
        let mut scratch = CaptureScratchAllocation::new(work);
        match native_mount_name_with_scratch(name, &mut scratch) {
            Ok(name) => { scratch.retain(); Ok(name) }
            Err(error) => Err(error),
        }
    };
    match result {
        Ok(name) => Ok(name),
        Err(NativeNameProductionError::Source(error)) => Err(error),
        Err(NativeNameProductionError::Reason(kind, reason)) => {
            let message = measured_native_message(reason, work)?;
            Err(match kind {
                super::NativeSourceTextKind::Engine => MountSourceError::Engine(message),
                super::NativeSourceTextKind::Invalid => MountSourceError::Invalid(message),
                super::NativeSourceTextKind::Unsupported => MountSourceError::Unsupported(message),
            })
        }
    }
}

pub(super) fn native_mount_name_with_native_work(
    name: &LogicalName,
    original: &crate::NativeMaterializationWork,
) -> Result<(Vec<u8>, crate::NativeAllocation), MountSourceError> {
    let mut scratch = NativeCaptureScratchAllocation::new(original)?;
    match native_mount_name_with_scratch(name, &mut scratch) {
        Ok(name) => Ok((name, scratch.retain())),
        Err(NativeNameProductionError::Source(error)) => Err(error),
        Err(NativeNameProductionError::Reason(kind, text)) =>
            Err(MountSourceError::NativeStaticText { kind, text }),
    }
}

fn measured_native_name_error<T>(
    message: &str,
    invalid: bool,
    work: &mut crate::demand::DemandWork,
) -> Result<T, MountSourceError> {
    let message = measured_native_message(message, work)?;
    Err(if invalid { MountSourceError::Invalid(message) } else { MountSourceError::Unsupported(message) })
}

fn measured_native_message(
    message: &str,
    work: &mut crate::demand::DemandWork,
) -> Result<String, MountSourceError> {
    use super::lazy::mount_demand_error;
    let mut scratch = CaptureScratchAllocation::new(work);
    let delta = crate::WorkCounters {
        bytes_copied: message.len() as u64, ..crate::WorkCounters::UNCHARGED
    };
    scratch.work.work.admit(&delta, &scratch.work.budget).map_err(MountSourceError::Work)?;
    let mut bytes = scratch.vec(message.len())?;
    bytes.extend_from_slice(message.as_bytes());
    scratch.work.record(delta).map_err(mount_demand_error)?;
    let message = String::from_utf8(bytes).expect("native name errors are UTF-8");
    scratch.retain();
    Ok(message)
}

enum NativeNameProductionError {
    Source(MountSourceError),
    Reason(super::NativeSourceTextKind, &'static str),
}

impl From<MountSourceError> for NativeNameProductionError {
    fn from(error: MountSourceError) -> Self { Self::Source(error) }
}

fn native_attribute_name_with_scratch<S: CaptureInputScratch>(
    profile: FilesystemProfile,
    limits: VolumeLimits,
    bytes: &[u8],
    scratch: &mut S,
) -> Result<AttributeName, NativeNameProductionError> {
    if profile != FilesystemProfile::Posix {
        return Err(NativeNameProductionError::Reason(
            super::NativeSourceTextKind::Unsupported,
            "native POSIX attributes require a POSIX volume profile",
        ));
    }
    let mut owned = scratch.vec::<u8>(bytes.len())?;
    let owned_bytes = owned.capacity() as u64;
    let delta = crate::WorkCounters {
        bytes_copied: bytes.len() as u64, ..crate::WorkCounters::UNCHARGED
    };
    scratch.admit(delta)?;
    owned.extend_from_slice(bytes);
    scratch.record(delta)?;
    scratch.items(1)?;
    if !bytes.is_empty() {
        let mut examined = 0;
        for byte in bytes {
            scratch.items(1)?;
            examined += 1;
            if *byte == 0 { break; }
        }
        // AttributeName::new performs this same validation traversal.
        scratch.items(examined)?;
    }
    match AttributeName::new(AttributeClass::PosixXattr, owned, limits.maximum_component_bytes) {
        Ok(name) => Ok(name),
        Err(_) => {
            scratch.release(owned_bytes)?;
            Err(NativeNameProductionError::Reason(
                super::NativeSourceTextKind::Engine, "attribute name is invalid",
            ))
        }
    }
}

fn native_attribute_name_with_work(
    profile: FilesystemProfile,
    limits: VolumeLimits,
    bytes: &[u8],
    work: &mut crate::demand::DemandWork,
) -> Result<AttributeName, MountSourceError> {
    let result = {
        let mut scratch = CaptureScratchAllocation::new(work);
        match native_attribute_name_with_scratch(profile, limits, bytes, &mut scratch) {
            Ok(name) => { scratch.retain(); Ok(name) }
            Err(error) => Err(error),
        }
    };
    match result {
        Ok(name) => Ok(name),
        Err(NativeNameProductionError::Source(error)) => Err(error),
        Err(NativeNameProductionError::Reason(kind, reason)) => {
            let message = measured_native_message(reason, work)?;
            Err(match kind {
                super::NativeSourceTextKind::Engine => MountSourceError::Engine(message),
                super::NativeSourceTextKind::Invalid => MountSourceError::Invalid(message),
                super::NativeSourceTextKind::Unsupported => MountSourceError::Unsupported(message),
            })
        }
    }
}

fn native_attribute_name_with_native_work(
    profile: FilesystemProfile,
    limits: VolumeLimits,
    bytes: &[u8],
    original: &crate::NativeMaterializationWork,
) -> Result<(AttributeName, crate::NativeAllocation), MountSourceError> {
    let mut scratch = NativeCaptureScratchAllocation::new(original)?;
    match native_attribute_name_with_scratch(profile, limits, bytes, &mut scratch) {
        Ok(name) => Ok((name, scratch.retain())),
        Err(NativeNameProductionError::Source(error)) => Err(error),
        Err(NativeNameProductionError::Reason(kind, text)) => {
            Err(MountSourceError::NativeStaticText { kind, text })
        }
    }
}

fn native_attribute_page_with_carrier(
    page: crate::kernel::AttributeListing,
    carrier: &mut NativeDemandCarrier<'_>,
    lifetime: Option<&CancellationToken>,
) -> Result<MountAttributePage, MountSourceError> {
    use super::lazy::{mount_demand_error, mount_items};
    let bytes = carrier.with_work(|work| {
        let slots = page.entries.capacity().checked_mul(std::mem::size_of::<crate::kernel::AttributeEntry>())
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
        page.entries.iter().try_fold(slots, |bytes, entry| {
            mount_items(work, 1)?;
            bytes.checked_add(entry.name.retained_bytes() as u64)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))
        })
    })?;
    let mut listing = NativeCarrierOwned { value: Some(page), carrier, bytes };
    listing.carrier.with_work(|work| work.record_adoption(bytes).map_err(mount_demand_error))?;
    let input = listing.value.take().expect("attribute listing is live");
    let remaining_source_bytes = &mut listing.bytes;
    let (page, bytes) = listing.carrier.with_work(|work| {
        let mut scratch = CaptureScratchAllocation::new(work);
        let result = (|| {
            let mut names = scratch.vec::<Vec<u8>>(input.entries.len())?;
            for entry in input.entries {
                scratch.work.check().map_err(mount_demand_error)?;
                if let Some(lifetime) = lifetime {
                    lifetime.check().map_err(|_| MountSourceError::Cancelled)?;
                }
                scratch.work.charge(crate::WorkCounters {
                    items_examined: 1, bytes_copied: std::mem::size_of::<Vec<u8>>() as u64,
                    ..crate::WorkCounters::UNCHARGED
                }).map_err(mount_demand_error)?;
                if entry.name.class() != AttributeClass::PosixXattr {
                    return measured_native_name_error(
                        "POSIX mount encountered a non-POSIX named attribute", false, scratch.work,
                    );
                }
                let name = entry.name.into_bytes();
                let bytes = name.capacity() as u64;
                *remaining_source_bytes = remaining_source_bytes.checked_sub(bytes)
                    .expect("transferred attribute name belongs to this listing");
                scratch.track(bytes);
                names.push(name);
            }
            // The source iterator has physically retired its vector capacity.
            let slots = std::mem::take(remaining_source_bytes);
            scratch.work.release(slots).map_err(mount_demand_error)?;
            let next_cursor = if input.has_more {
                if let Some(last) = names.last() {
                    let mut cursor = scratch.vec::<u8>(last.len())?;
                    let delta = crate::WorkCounters {
                        items_examined: 1, bytes_copied: last.len() as u64,
                        ..crate::WorkCounters::UNCHARGED
                    };
                    scratch.work.work.admit(&delta, &scratch.work.budget).map_err(MountSourceError::Work)?;
                    cursor.extend_from_slice(last);
                    scratch.work.record(delta).map_err(mount_demand_error)?;
                    Some(cursor)
                } else { None }
            } else { None };
            Ok(MountAttributePage { names, next_cursor })
        })();
        result.map(|page| (page, scratch.retain()))
    })?;
    let mut output = NativeCarrierOwned { value: Some(page), carrier: &mut *listing.carrier, bytes };
    output.carrier.with_work(|work| {
        work.check().map_err(mount_demand_error)?;
        if let Some(lifetime) = lifetime {
            lifetime.check().map_err(|_| MountSourceError::Cancelled)?;
        }
        Ok::<_, MountSourceError>(())
    })?;
    let page = output.value.take().expect("attribute page is live");
    output.bytes = 0;
    Ok(page)
}

fn native_attribute_page_with_native_work(
    page: crate::kernel::AttributeListing,
    allocation: crate::NativeAllocation,
    original: &crate::NativeMaterializationWork,
    lifetime: Option<&CancellationToken>,
) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError> {
    let mut source_allocation = allocation;
    let entries = page.entries;
    let has_more = page.has_more;
    let mut scratch = NativeCaptureScratchAllocation::new(original)?;
    let mut names = scratch.vec::<Vec<u8>>(entries.len())?;
    for entry in entries {
        original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        if let Some(lifetime) = lifetime {
            lifetime.check().map_err(|_| MountSourceError::Cancelled)?;
        }
        scratch.charge(crate::WorkCounters {
            items_examined: 1, bytes_copied: std::mem::size_of::<Vec<u8>>() as u64,
            ..crate::WorkCounters::UNCHARGED
        })?;
        if entry.name.class() != AttributeClass::PosixXattr {
            return Err(MountSourceError::NativeStaticText {
                kind: super::NativeSourceTextKind::Unsupported,
                text: "POSIX mount encountered a non-POSIX named attribute",
            });
        }
        let name = entry.name.into_bytes();
        scratch.absorb(source_allocation.split_off(name.capacity() as u64)
            .map_err(MountSourceError::NativeWork)?);
        names.push(name);
    }
    // Source slots have physically retired; transferred names remain live.
    drop(source_allocation);
    let next_cursor = if has_more {
        if let Some(last) = names.last() {
            let mut cursor = scratch.vec::<u8>(last.len())?;
            let delta = crate::WorkCounters {
                items_examined: 1, bytes_copied: last.len() as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            scratch.admit(delta)?;
            cursor.extend_from_slice(last);
            scratch.record(delta)?;
            Some(cursor)
        } else { None }
    } else { None };
    let output = MountAttributePage { names, next_cursor };
    original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
    if let Some(lifetime) = lifetime {
        lifetime.check().map_err(|_| MountSourceError::Cancelled)?;
    }
    Ok((output, scratch.retain()))
}

impl<A, O> CheckoutMountSource<A, O> {
    /// Attaches the actual callback authority before capture or driver start.
    #[must_use]
    pub fn with_native_callback_guard(
        mut self, guard: Arc<dyn super::NativeMountCallbackGuard>,
    ) -> Self {
        self.native_callback_guard = Some(guard);
        self
    }
    pub(super) fn publication_guard(
        &self,
    ) -> impl for<'m> FnMut(&'m crate::NativeMaterializationWork) -> SourcePublicationFuture + Clone + Send + 'static {
        source_publication_guard(
            self.native_callback_guard.as_ref().map(Arc::clone), Some(self.cancellation.clone()),
        )
    }
    pub(super) fn sdk_publication_policy(
        &self,
    ) -> impl for<'m> FnMut(&'m crate::NativeMaterializationWork) -> SourcePublicationFuture + Clone + Send + 'static {
        source_publication_guard(None, Some(self.cancellation.clone()))
    }

    pub(super) fn callback_guard(&self) -> Option<&Arc<dyn super::NativeMountCallbackGuard>> {
        self.native_callback_guard.as_ref()
    }

    /// Whether the volume resolves every case of a name to one entry.
    pub(super) fn folds_names(&self) -> bool {
        self.folds_names
    }

    pub(super) fn shared_checkout(&self) -> &SharedCheckout<A, O> {
        &self.checkout
    }

    pub(super) fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Returns constructor accounting custody, not callback authority.
    pub(super) fn native_original_work(
        &self,
    ) -> Result<Option<super::NativeMountMeterWork>, MountSourceError> {
        self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
        Ok(self.native_original.as_ref().map(crate::NativeMaterializationWork::native_mount_work))
    }

    pub(super) fn captures_pending(&self) -> bool {
        let gate = self.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
        gate.producers != 0 || !gate.active.is_empty()
    }

    /// The checkout's current generation, which publication advances.
    #[cfg(all(test, target_os = "macos"))]
    pub(super) fn generation_id(&self) -> Result<crate::GenerationId, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync,
        O: AsyncObjectStore + Send + Sync,
    {
        let owner = ViewGate::callback_owner();
        self.runtime
            .wait(|| async { Ok(self.checkout.observe(owner).await?.generation_id()) })
    }

    /// The checkout path a mounted path names, as view changes key it.
    pub(super) fn namespace_path(
        &self,
        path: &MountPath,
    ) -> Result<NamespacePath, MountSourceError> {
        self.path(path)
    }

    /// Records a change a composing source makes to its own projection of
    /// this checkout. The caller holds that source's exclusive view, so no
    /// lookup through it overlaps the change.
    pub(super) fn record_projection_change(&self, change: &ViewChange<'_>) {
        self.checkout.ledger.record(change, &mut NativeDemandCarrier::Borrowed(&mut super::view_ledger::unbounded_view_work()), &mut super::view_ledger::ViewLedgerAdmission::default()).expect("unbounded view recording");
    }

    pub(super) fn record_projection_change_with_carrier(
        &self,
        change: &ViewChange<'_>,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        record_view_change_with_carrier(&self.checkout.ledger, change, carrier)
    }

    pub(super) fn record_projection_change_with_native_work(
        &self, change: &ViewChange<'_>, original: &crate::NativeMaterializationWork,
        view: &mut super::view_ledger::ViewLedgerAdmission,
        mut private_check: impl FnMut() -> Result<(), MountSourceError>,
    ) -> Result<(), MountSourceError> {
        private_check()?;
        let recorded = self.checkout.ledger.record(change, &mut NativeDemandCarrier::Native(&super::NativeMountMeterWork::from_native_materialization_work(original)), view);
        // The canonical ledger records its real prefix before notifying.
        // A stop raised by an observer prevents subsequent source publication,
        // but cannot erase keys, notifications or work already performed.
        let checked = private_check();
        recorded?;
        checked
    }

    pub(super) fn take_view_admission_for_work(
        &self, work: &mut crate::demand::DemandWork,
    ) -> Result<(usize, super::view_ledger::ViewLedgerAdmission), MountSourceError> {
        self.checkout.ledger.take_for_work(work)
    }

    /// Binds an existing source handle to the same stable identity once a
    /// peer stages that identity in the mounted checkout candidate.
    pub(super) fn attached_file_by_id(
        &self,
        file_id: FileId,
    ) -> Result<Option<Arc<dyn MountOpenFile>>, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| {
            let mut carrier = NativeDemandCarrier::Shared(meter);
            let mut owned = NativeCarrierOwned { value: None, carrier: &mut carrier, bytes: 0 };
            self.runtime.block_on_measured(|| async {
                let handle = self.attached_file_by_id_with_carrier(
                    file_id, ViewGate::callback_owner(), owned.carrier,
                ).await?;
                owned.bytes = handle.as_ref().map_or(0, |(_, bytes)| *bytes);
                owned.value = Some(handle);
                Ok::<_, MountSourceError>(())
            }, meter.as_ref()).map_err(capture_io_error)??;
            let handle = owned.value.take().expect("native identity result is live");
            owned.bytes = 0;
            Ok(handle.map(|(handle, _)| handle))
        })
    }

    /// Resolves one mounted path within a caller's callback future, so a
    /// composed source spends one runtime entry on its whole callback.
    pub(super) async fn lookup_async(
        &self,
        path: &MountPath,
        owner: CallbackOwner,
    ) -> Result<Option<MountLookup>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        in_heap(move || async move {
            let path = self.path(path)?;
            // A pending creation answers for its own name and changes no
            // other name's answer (see `PendingCreate`).
            if let Some(pending) = self.checkout.pending_lookup(&path) {
                return Ok(Some(pending));
            }
            let observation = self.checkout.observe_as_is(owner).await?;
            let mut checkout = observation.observer();
            let receipt = checkout
                .lookup_no_follow_with_metadata(&path, boundary_budget(), &self.cancellation)
                .await
                .map_err(engine_error)?;
            Ok(receipt.value.map(|value| MountLookup {
                node: mount_node(value.record),
                metadata: value.metadata,
            }))
        })
        .await
    }

    async fn read_bytes_async_with_native_work(
        &self,
        path: &MountPath,
        range: Option<ByteRange>,
        owner: CallbackOwner,
        native: &super::NativeMountMeterWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>
    where A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
        let original = native.native_materialization_work();
        original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        let mut carrier = NativeDemandCarrier::Native(native);
        let cancellation = carrier.cancellation();
        let namespace_pair = self.namespace_path_with_carrier(path, &mut carrier)?;
        let _namespace_allocation = namespace_pair.1;
        let namespace = namespace_pair.0;
        let observation = self.checkout.observe_owned_with_carrier(
            owner, &mut carrier, &cancellation, Some(&self.cancellation),
        ).await?;
        let _view = observation._view;
        let mut observer = LookupObserverOwned::new_in(observation.observer, carrier.reborrow())?;
        original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        let result = match range {
            Some(range) => observer.read_file_range_with_native_work(
                &namespace, range, &original, &cancellation,
            ).await.map(|read| {
                let capacity = read.logical_owned_bytes();
                crate::kernel::OwnedSourceChunk::intrinsic(read.bytes, capacity)
            }),
            None => observer.read_symbolic_link_owned_with_native_work(
                &namespace, &original, &cancellation,
            ).await,
        };
        let completed = observer.finish();
        let output = result.map_err(MountSourceError::Filesystem)?;
        completed?;
        original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        Ok(output)
    }

    async fn read_bytes_async_with_work(
        &self,
        path: &MountPath,
        range: Option<ByteRange>,
        owner: CallbackOwner,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>
    where
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    {
        use super::lazy::{mount_accept, mount_demand_error, mount_engine_error};
        let (namespace, cancellation) = {
            let mut work = meter.lock();
            (self.namespace_path_with_work(path, &mut work)?, work.cancellation().clone())
        };
        let namespace_bytes = namespace.retained_bytes();
        let namespace = CaptureMeterOwned {
            value: Some(namespace),
            _allocation: CaptureOwnedAllocation { meter: Arc::clone(meter), bytes: namespace_bytes },
        };
        let observation = self.checkout.observe_owned_with_carrier(
            owner, &mut NativeDemandCarrier::Shared(meter), &cancellation, Some(&self.cancellation),
        ).await?;
        let _view = observation._view;
        let mut observer = LookupObserverOwned::new(observation.observer, meter)?;
        let budget = {
            let mut work = meter.lock();
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            work.remaining().map_err(mount_demand_error)?
        };
        let result = match range {
            Some(range) => observer.read_file_range(&namespace, range, budget, &cancellation)
                .await.map(|receipt| crate::OperationReceipt {
                    value: {
                        let logical_owned_bytes = receipt.value.logical_owned_bytes();
                        crate::kernel::OwnedSourceChunk::claimed(receipt.value.bytes, logical_owned_bytes)
                    },
                    work: receipt.work,
                }),
            None => observer.read_symbolic_link_owned(&namespace, budget, &cancellation).await,
        };
        let mut output = {
            let mut work = meter.lock();
            let accepted = mount_accept(&mut work, result, mount_engine_error);
            observer.receipt_delivered();
            let output = accepted?;
            let bytes = output.logical_owned_bytes;
            if let Err(error) = work.record_adoption(bytes) {
                drop(output);
                work.release(bytes).map_err(mount_demand_error)?;
                return Err(mount_demand_error(error));
            }
            CaptureMeterOwned {
                value: Some(output),
                _allocation: CaptureOwnedAllocation { meter: Arc::clone(meter), bytes },
            }
        };
        observer.finish()?;
        {
            let mut work = meter.lock();
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        }
        let owned = output.value.take().expect("read output remains owned");
        output._allocation.bytes = 0;
        Ok(owned)
    }

    pub(super) async fn read_range_owned_async_with_carrier(
        &self,
        path: &MountPath,
        offset: u64,
        length: u32,
        owner: CallbackOwner,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>
    where
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    {
        if let Some(original) = carrier.original_native_work() {
            original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        } else {
            carrier.with_work(|work| work.check()).map_err(super::lazy::mount_demand_error)?;
        }
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        let range = Some(ByteRange { offset, length: u64::from(length) });
        match carrier {
            NativeDemandCarrier::Native(native) =>
                self.read_bytes_async_with_native_work(path, range, owner, native).await,
            NativeDemandCarrier::Shared(meter) =>
                self.read_bytes_async_with_work(path, range, owner, meter).await,
            NativeDemandCarrier::Borrowed(work) =>
                BorrowedNativeDemandMeter::new(work)?.run_async(|meter| async move {
                    self.read_bytes_async_with_work(path, range, owner, &meter).await
                }).await,
        }
    }

    pub(super) async fn read_link_owned_async_with_carrier(
        &self,
        path: &MountPath,
        owner: CallbackOwner,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>
    where
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    {
        if let Some(original) = carrier.original_native_work() {
            original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        } else {
            carrier.with_work(|work| work.check()).map_err(super::lazy::mount_demand_error)?;
        }
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        match carrier {
            NativeDemandCarrier::Native(native) =>
                self.read_bytes_async_with_native_work(path, None, owner, native).await,
            NativeDemandCarrier::Shared(meter) =>
                self.read_bytes_async_with_work(path, None, owner, meter).await,
            NativeDemandCarrier::Borrowed(work) =>
                BorrowedNativeDemandMeter::new(work)?.run_async(|meter| async move {
                    self.read_bytes_async_with_work(path, None, owner, &meter).await
                }).await,
        }
    }

    async fn read_directory_async_with_native_work(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        owner: CallbackOwner,
        native: &super::NativeMountMeterWork,
    ) -> Result<MountDirectoryPage, MountSourceError>
    where A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
        let original = native.native_materialization_work();
        original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        let mut carrier = NativeDemandCarrier::Native(native);
        let cancellation = carrier.cancellation();
        let namespace_pair = self.namespace_path_with_carrier(path, &mut carrier)?;
        let _namespace_allocation = namespace_pair.1;
        let namespace = namespace_pair.0;
        let cursor_pair = if let Some(bytes) = cursor {
            let mut scratch = NativeCaptureScratchAllocation::new(&original)?;
            let (encoding, length) = capture_name_layout(
                bytes, self.profile, self.limits, &mut scratch,
            )?;
            let name = capture_native_name(
                bytes, encoding, length, self.profile, self.limits, &mut scratch,
            )?;
            Some((name, scratch.retain()))
        } else { None };
        let (_cursor_allocation, cursor) = match cursor_pair {
            Some((name, allocation)) => (Some(allocation), Some(name)),
            None => (None, None),
        };
        let observation = self.checkout.observe_owned_with_carrier(
            owner, &mut carrier, &cancellation, Some(&self.cancellation),
        ).await?;
        let _view = observation._view;
        let mut observer = LookupObserverOwned::new_in(observation.observer, carrier.reborrow())?;
        let result = observer.list_directory_records_with_native_work(
            &namespace, cursor.as_ref(), maximum_entries, &original, &cancellation,
        ).await;
        let completed = observer.finish();
        let (page, allocation) = result.map_err(MountSourceError::Filesystem)?;
        let mut source_allocation = allocation;
        let source_entries = page.entries;
        let has_more = page.has_more;
        completed?;
        let mut scratch = NativeCaptureScratchAllocation::new(&original)?;
        let mut entries = scratch.vec::<MountDirectoryEntry>(source_entries.len())?;
        for entry in source_entries {
            original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            let name_pair = native_mount_name_with_native_work(&entry.name, &original)?;
            scratch.absorb(name_pair.1);
            let name = name_pair.0;
            let source_name_bytes = entry.name.retained_bytes() as u64;
            drop(entry.name);
            drop(source_allocation.split_off(source_name_bytes).map_err(MountSourceError::NativeWork)?);
            scratch.charge(crate::WorkCounters {
                items_examined: 1, bytes_copied: std::mem::size_of::<MountDirectoryEntry>() as u64,
                ..crate::WorkCounters::UNCHARGED
            })?;
            entries.push(MountDirectoryEntry {
                name, node: mount_node(entry.record), metadata: entry.metadata, pin: None,
            });
        }
        // The iterator has physically retired the Root record vector.
        drop(source_allocation);
        let next_cursor = if has_more {
            if let Some(last) = entries.last() {
                let delta = crate::WorkCounters {
                    bytes_copied: last.name.len() as u64, ..crate::WorkCounters::UNCHARGED
                };
                scratch.admit(delta)?;
                let mut cursor = scratch.vec(last.name.len())?;
                cursor.extend_from_slice(&last.name);
                scratch.record(delta)?;
                Some(cursor)
            } else { None }
        } else { None };
        let page = MountDirectoryPage::new(entries, next_cursor, Some(scratch.retain()));
        original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        Ok(page)
    }

    fn new_pending_with_carrier(
        &self, path: &NamespacePath, file_id: FileId, metadata: FileMetadata,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<Arc<PendingCreate>, MountSourceError> {
        use super::lazy::{mount_clone_namespace, mount_demand_error};
        if let Some(original) = carrier.original_native_work() {
            let cancellation = original.cancellation();
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, cancellation);
            let (path, path_allocation) = path.clone_with_accounting(&mut accounting, cancellation)
                .map_err(MountSourceError::Filesystem)?;
            let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(mut path_allocation) =
                path_allocation else { unreachable!("native creation path retains original custody"); };
            let bytes = (std::mem::size_of::<PendingCreate>() + std::mem::size_of::<FileMetadata>()) as u64;
            let mut allocation = original.claim_allocation(bytes, 2).map_err(MountSourceError::NativeWork)?;
            allocation.absorb(&mut path_allocation).map_err(MountSourceError::NativeWork)?;
            original.charge(crate::WorkCounters {
                bytes_copied: std::mem::size_of::<FileMetadata>() as u64,
                ..crate::WorkCounters::UNCHARGED
            }).map_err(MountSourceError::NativeWork)?;
            let metadata = Box::new(metadata);
            let pending = Arc::new(PendingCreate {
                path, file_id, origin: ViewOrigin::current(),
                native_callback_guard: self.native_callback_guard.as_ref().map(Arc::clone),
                source_lifetime: self.cancellation.clone(),
                maximum_bytes: PENDING_CREATE_BYTES.min(self.limits.maximum_read_bytes),
                state: StdMutex::new(PendingState::Pending {
                    metadata, bytes: PendingBytes::Mutable { bytes: Vec::new(), allocation: None },
                }),
                failure: std::sync::OnceLock::new(), custody: None,
                _native_allocation: Some(allocation),
            });
            original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            return Ok(pending);
        }
        carrier.with_work(|work| {
            let custody = super::NativeMountAllocationCustody::new(work).map_err(super::lazy::mount_demand_error)?;
            let mut scratch = CaptureScratchAllocation::new(work);
            let path = mount_clone_namespace(path.components(), self.limits, scratch.work)?;
            scratch.track(path.retained_bytes());
            scratch.work.charge(crate::WorkCounters {
                bytes_copied: (std::mem::size_of::<FileMetadata>() + std::mem::size_of::<PendingCreate>()) as u64,
                ..crate::WorkCounters::UNCHARGED
            }).map_err(mount_demand_error)?;
            custody.claim(std::mem::size_of::<FileMetadata>() as u64, scratch.work).map_err(super::lazy::mount_demand_error)?;
            let metadata = Box::new(metadata);
            custody.retain_admitted(path.retained_bytes(), scratch.work).map_err(super::lazy::mount_demand_error)?;
            scratch.retain();
            custody.claim(std::mem::size_of::<PendingCreate>() as u64, work).map_err(super::lazy::mount_demand_error)?;
            Ok(Arc::new(PendingCreate {
                path, file_id, origin: ViewOrigin::current(),
                native_callback_guard: self.native_callback_guard.as_ref().map(Arc::clone),
                source_lifetime: self.cancellation.clone(),
                maximum_bytes: PENDING_CREATE_BYTES.min(self.limits.maximum_read_bytes),
                state: StdMutex::new(PendingState::Pending {
                    metadata, bytes: PendingBytes::Mutable { bytes: Vec::new(), allocation: None },
                }),
                failure: std::sync::OnceLock::new(),
                custody: Some(custody),
                _native_allocation: None,
            }))
        })
    }

    fn create_file_on_meter(
        &self, path: &MountPath, metadata: FileMetadata, absent_since: Option<ViewStamp>,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<MountLookup, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        self.create_file_on_carrier(
            path, metadata, absent_since, NativeDemandCarrier::Shared(meter), meter,
        )
    }

    fn create_file_on_carrier(
        &self, path: &MountPath, metadata: FileMetadata, absent_since: Option<ViewStamp>,
        mut carrier: NativeDemandCarrier<'_>,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<MountLookup, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        self.runtime.block_on_measured(
            || async {
                let absent_since = if let Some(stamp) = absent_since { stamp } else {
                    // Sample before lookup: the stamp proves what it found.
                    let stamp = self.checkout.ledger.stamp();
                    if self.lookup_async_with_carrier(path, ViewGate::callback_owner(), &mut carrier).await?.is_some() {
                        return Err(MountSourceError::AlreadyExists);
                    }
                    stamp
                };
                self.create_absent_file_async_with_carrier(path, metadata, absent_since, &mut carrier).await
            }, meter.as_ref(),
        ).map_err(capture_io_error)?
    }

    pub(super) async fn create_absent_file_async_with_carrier(
        &self, mount_path: &MountPath, metadata: FileMetadata, absent_since: ViewStamp,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<MountLookup, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        use super::lazy::{mount_demand_error, mount_items};
        let (path, cancellation) = carrier.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            Ok::<_, MountSourceError>((self.namespace_path_with_work(mount_path, work)?, work.cancellation().clone()))
        })?;
        let bytes = path.retained_bytes();
        let mut path = NativeCarrierOwned { value: Some(path), carrier, bytes };
        let (namespace, carrier) = path.parts();
        match self.checkout.state.try_read() {
            Ok(state) => state.ensure_publication_resolved()?,
            Err(_) => {
                let state = await_measured_lock_with_carrier(
                    self.checkout.state.read(), carrier, &cancellation, Some(&self.cancellation),
                ).await?;
                state.ensure_publication_resolved()?;
            }
        }
        let waiting = self.checkout.pending.lock().unwrap_or_else(PoisonError::into_inner).len();
        if waiting >= MAXIMUM_GROUPED_CHANGES {
            match carrier {
                NativeDemandCarrier::Borrowed(work) => self.checkout.settle_pending_measured(
                    work, &mut super::view_ledger::ViewLedgerAdmission::default(), &cancellation, Some(&self.cancellation),
                ).await?,
                NativeDemandCarrier::Shared(meter) => self.checkout.settle_pending_with_meter(
                    meter, &cancellation, Some(&self.cancellation),
                ).await?,
                NativeDemandCarrier::Native(original) => self.checkout.settle_pending_with_native_work(
                    original, &cancellation, Some(&self.cancellation),
                ).await?,
            }
        }
        carrier.with_work(|work| {
            mount_items(work, 1)?;
            work.charge(crate::WorkCounters {
                bytes_copied: std::mem::size_of::<MountLookup>() as u64,
                ..crate::WorkCounters::UNCHARGED
            }).map_err(mount_demand_error)
        })?;
        let file_id = FileId::new();
        let pending = self.new_pending_with_carrier(namespace, file_id, metadata, carrier)?;
        if !self.checkout.hold_pending_with_carrier(pending, Some(absent_since), carrier)? {
            // Resolve the original absence race before retrying the same FileId
            // in the same deferred queue, without the superseded stamp.
            if self.lookup_async_with_carrier(mount_path, ViewGate::callback_owner(), carrier).await?.is_some() {
                return Err(MountSourceError::AlreadyExists);
            }
            let pending = self.new_pending_with_carrier(namespace, file_id, metadata, carrier)?;
            if !self.checkout.hold_pending_with_carrier(pending, None, carrier)? {
                return Err(MountSourceError::AlreadyExists);
            }
        }
        Ok(MountLookup {
            node: MountNode { file_id, kind: MountNodeKind::Regular, logical_bytes: 0, link_count: 1, device: None },
            metadata,
        })
    }

    async fn create_authored_async_with_carrier<F>(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        kind: MountNodeKind,
        logical_bytes: u64,
        device: Option<(u32, u32)>,
        carrier: &mut NativeDemandCarrier<'_>,
        build: F,
    ) -> Result<MountLookup, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        F: FnOnce(NamespacePath) -> AuthoredMutation,
    {
        use super::lazy::{mount_clone_namespace, mount_demand_error, mount_engine_error};
        let cancellation = carrier.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            Ok::<_, MountSourceError>(work.cancellation().clone())
        })?;
        let namespace_allocation;
        let namespace;
        (namespace, namespace_allocation) = self.namespace_path_with_carrier(path, carrier)?;
        let bytes = if namespace_allocation.is_some() { 0 } else { namespace.retained_bytes() };
        let mut namespace = NativeCarrierOwned { value: Some(namespace), carrier, bytes };
        let (mut candidate, epoch, stamp) = self.checkout.prepare_native_phase_with_carrier(
            namespace.carrier, &cancellation, Some(&self.cancellation),
        ).await?;
        let mut native_input = None;
        let accepted = if let Some(original) = namespace.carrier.original_native_work() {
            native_input = Some(prepare_native_authored_phase_input(
                namespace.value.take().expect("native create path is live"), &original, build,
            )?);
            let input = native_input.as_ref().expect("native create slots are live");
            candidate.checkout.apply_borrowed_authored_transaction_with_native_work(
                &input.authored, &input.slots_allocation, &original, &cancellation,
            ).await.map_err(MountSourceError::Filesystem).and_then(|(value, allocation)| {
                let output = (value, allocation);
                original.charge(crate::WorkCounters {
                    bytes_copied: std::mem::size_of::<MountLookup>() as u64,
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                }).map_err(MountSourceError::NativeWork)?;
                output.0.created_file_ids.first().copied().flatten()
                    .ok_or_else(|| MountSourceError::Engine("create omitted file identity".to_owned()))
            })
        } else {
            let (path, carrier) = namespace.parts();
            let (authored, bytes) = carrier.with_work(|work| {
                let mut scratch = CaptureScratchAllocation::new(work);
                let mut authored = scratch.vec::<AuthoredMutation>(1)?;
                let consumed = mount_clone_namespace(path.components(), self.limits, scratch.work)?;
                scratch.track(consumed.retained_bytes());
                scratch.work.charge(crate::WorkCounters {
                    bytes_copied: std::mem::size_of::<AuthoredMutation>() as u64,
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                }).map_err(mount_demand_error)?;
                authored.push(build(consumed));
                Ok::<_, MountSourceError>((authored, scratch.retain()))
            })?;
            let mut authored = NativeCarrierOwned { value: Some(authored), carrier, bytes };
            let (budget, prior_live) = authored.carrier.with_work(|work| {
                Ok::<_, MountSourceError>((
                    work.remaining().map_err(mount_demand_error)?, work.live_allocation_bytes(),
                ))
            })?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            let receipt = candidate.checkout.apply_authored_transaction(
                authored.value.take().expect("native create input is live"), budget, &cancellation,
            ).await;
            authored.carrier.accept_with_prior_live(receipt, prior_live, mount_engine_error)
                .and_then(|value| {
                    let bytes = value.created_file_ids.capacity()
                        .checked_mul(std::mem::size_of::<Option<FileId>>())
                        .and_then(|bytes| u64::try_from(bytes).ok())
                        .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
                    let mut result = NativeCarrierOwned {
                        value: Some(value), carrier: &mut *authored.carrier, bytes,
                    };
                    result.carrier.with_work(|work| {
                        work.record_adoption(bytes).map_err(mount_demand_error)
                    })?;
                    let value = &result.value;
                    result.carrier.with_work(|work| {
                        work.charge(crate::WorkCounters {
                            bytes_copied: std::mem::size_of::<MountLookup>() as u64,
                            items_examined: 1,
                            ..crate::WorkCounters::UNCHARGED
                        }).map_err(mount_demand_error)?;
                        value.as_ref().expect("native create result is live")
                            .created_file_ids.first().copied().flatten()
                            .ok_or_else(|| MountSourceError::Engine("create omitted file identity".to_owned()))
                    })
                })
        };
        let installed = {
            let path = match native_input.as_ref() {
                Some(input) => match input.authored.first().expect("native create input is live") {
                    AuthoredMutation::CreateDirectory { path, .. }
                    | AuthoredMutation::CreateSymbolicLink { path, .. }
                    | AuthoredMutation::CreateEmptySpecial { path, .. }
                    | AuthoredMutation::CreateDevice { path, .. } => path,
                    _ => unreachable!("authored creation keeps its original bound path"),
                },
                None => namespace.value.as_ref().expect("native create path is live"),
            };
            let carrier = &mut *namespace.carrier;
            self.checkout.finish_native_phase_with_carrier(
                candidate, epoch, stamp, &ViewChange::Bound(path), carrier,
                &cancellation, Some(&self.cancellation),
            ).await
        };
        drop(native_input);
        drop(namespace);
        drop(namespace_allocation);
        let file_id = accepted?;
        installed?;
        self.checkout.publish_mutation_with_carrier(
            carrier, &cancellation, Some(&self.cancellation),
            self.native_callback_guard.as_ref(),
        ).await?;
        Ok(MountLookup {
            node: MountNode { file_id, kind, logical_bytes, link_count: 1, device },
            metadata,
        })
    }

    fn create_directory_on_meter(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<MountLookup, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        self.runtime.block_on_measured(
            || async {
                self.create_authored_async_with_carrier(
                    path, metadata, MountNodeKind::Directory, 0, None,
                    &mut NativeDemandCarrier::Shared(meter),
                    |path| AuthoredMutation::CreateDirectory { path, metadata },
                ).await
            },
            meter.as_ref(),
        ).map_err(capture_io_error)?
    }

    fn hard_link_on_meter(
        &self,
        source: &MountPath,
        destination: &MountPath,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        self.runtime.block_on_measured(
            || async {
                self.hard_link_async_with_carrier(
                    source, destination, &mut NativeDemandCarrier::Shared(meter),
                ).await
            },
            meter.as_ref(),
        ).map_err(capture_io_error)?
    }

    pub(super) async fn hard_link_async_with_carrier(
        &self,
        source: &MountPath,
        destination: &MountPath,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        use super::lazy::{mount_clone_namespace, mount_demand_error, mount_engine_error};
        let cancellation = carrier.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            Ok::<_, MountSourceError>(work.cancellation().clone())
        })?;
        let source_allocation;
        let source_namespace;
        (source_namespace, source_allocation) = self.namespace_path_with_carrier(source, carrier)?;
        let bytes = if source_allocation.is_some() { 0 } else { source_namespace.retained_bytes() };
        let mut source = NativeCarrierOwned { value: Some(source_namespace), carrier, bytes };
        let destination_allocation;
        let destination_namespace;
        (destination_namespace, destination_allocation) = self.namespace_path_with_carrier(destination, source.carrier)?;
        let bytes = if destination_allocation.is_some() { 0 } else { destination_namespace.retained_bytes() };
        let mut destination = NativeCarrierOwned {
            value: Some(destination_namespace), carrier: &mut *source.carrier, bytes,
        };
        let (mut candidate, epoch, stamp) = self.checkout.prepare_native_phase_with_carrier(
            destination.carrier, &cancellation, Some(&self.cancellation),
        ).await?;
        let node = native_node_if_bound_with_carrier(
            &candidate.checkout, source.value.as_ref().expect("native link source is live"),
            destination.carrier, &cancellation,
        ).await?.ok_or(MountSourceError::NotFound)?;
        let mut native_input = None;
        let accepted = if let Some(original) = destination.carrier.original_native_work() {
            native_input = Some(prepare_native_authored_phase_input(
                destination.value.take().expect("native link destination is live"), &original,
                |destination| AuthoredMutation::HardLink {
                    source: source.value.take().expect("native link source is live"), destination,
                },
            )?);
            let input = native_input.as_ref().expect("native link slots are live");
            candidate.checkout.apply_borrowed_authored_transaction_with_native_work(
                &input.authored, &input.slots_allocation, &original, &cancellation,
            ).await.map_err(MountSourceError::Filesystem).map(|(value, allocation)| drop((value, allocation)))
        } else {
            let (destination_path, carrier) = destination.parts();
            let consumed = carrier.with_work(|work| {
                mount_clone_namespace(destination_path.components(), self.limits, work)
            })?;
            let bytes = consumed.retained_bytes();
            let mut consumed = NativeCarrierOwned { value: Some(consumed), carrier, bytes };
            let (budget, prior_live) = consumed.carrier.with_work(|work| {
                Ok::<_, MountSourceError>((
                    work.remaining().map_err(mount_demand_error)?, work.live_allocation_bytes(),
                ))
            })?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            let receipt = candidate.checkout.hard_link(
                source.value.take().expect("native link source is live"),
                consumed.value.take().expect("native link destination is live"), budget, &cancellation,
            ).await;
            consumed.carrier.accept_with_prior_live(receipt, prior_live, mount_engine_error)
        };
        let installed = {
            let destination_path = match native_input.as_ref() {
                Some(input) => match input.authored.first().expect("native link input is live") {
                    AuthoredMutation::HardLink { destination, .. } => destination,
                    _ => unreachable!("native hard link keeps its original destination path"),
                },
                None => destination.value.as_ref().expect("native link destination is live"),
            };
            let carrier = &mut *destination.carrier;
            self.checkout.finish_native_phase_with_carrier(
                candidate, epoch, stamp, &ViewChange::Linked(destination_path, node), carrier,
                &cancellation, Some(&self.cancellation),
            ).await
        };
        drop(native_input);
        drop(destination);
        drop(destination_allocation);
        drop(source);
        drop(source_allocation);
        accepted?;
        installed?;
        self.checkout.publish_mutation_with_carrier(
            carrier, &cancellation, Some(&self.cancellation),
            self.native_callback_guard.as_ref(),
        ).await
    }

    fn remove_on_meter(
        &self,
        path: &MountPath,
        expected: Option<FileId>,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        self.runtime.block_on_measured(
            || async {
                self.remove_async_with_carrier(
                    path, expected, &mut NativeDemandCarrier::Shared(meter),
                ).await
            },
            meter.as_ref(),
        ).map_err(capture_io_error)?
    }

    pub(super) async fn remove_async_with_carrier(
        &self,
        path: &MountPath,
        expected: Option<FileId>,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        use super::lazy::{mount_clone_namespace, mount_demand_error, mount_engine_error};
        let cancellation = carrier.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            Ok::<_, MountSourceError>(work.cancellation().clone())
        })?;
        let namespace_allocation;
        let namespace;
        (namespace, namespace_allocation) = self.namespace_path_with_carrier(path, carrier)?;
        let bytes = if namespace_allocation.is_some() { 0 } else { namespace.retained_bytes() };
        let mut input = NativeCarrierOwned { value: Some(namespace), carrier, bytes };
        let (namespace, phase_carrier) = input.parts();
        let (mut candidate, epoch, stamp) = self.checkout.prepare_native_phase_with_carrier(
            phase_carrier, &cancellation, Some(&self.cancellation),
        ).await?;
        let node = native_node_if_bound_with_carrier(
            &candidate.checkout, namespace, phase_carrier, &cancellation,
        ).await?.ok_or(MountSourceError::NotFound)?;
        let accepted = if let Some(original) = phase_carrier.original_native_work() {
            let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
            let mut operations = crate::kernel::Owned::<Vec<crate::kernel::Mutation>>::allocate(
                1, &mut accounting,
            ).map_err(MountSourceError::Filesystem)?;
            accounting.charge(crate::WorkCounters {
                bytes_copied: std::mem::size_of::<crate::kernel::Mutation>() as u64,
                ..crate::WorkCounters::UNCHARGED
            }).map_err(MountSourceError::Filesystem)?;
            let mut path = namespace.clone_with_accounting(
                &mut accounting, &cancellation,
            ).map_err(MountSourceError::Filesystem)?;
            operations.allocation.absorb(&mut path.1).map_err(MountSourceError::Filesystem)?;
            operations.value.push(crate::kernel::Mutation::Remove {
                path: path.0, expected_file_id: expected.map_or(MetadataField::Unavailable, MetadataField::Value),
            });
            let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) =
                operations.allocation else { unreachable!("native mutation keeps original custody"); };
            candidate.checkout.mutate_with_native_work(
                operations.value, allocation, &original, &cancellation,
            ).await.map_err(MountSourceError::Filesystem)
        } else {
            let consumed = phase_carrier.with_work(|work| {
                mount_clone_namespace(namespace.components(), self.limits, work)
            })?;
            let bytes = consumed.retained_bytes();
            let mut consumed = NativeCarrierOwned {
                value: Some(consumed), carrier: phase_carrier, bytes,
            };
            let (budget, prior_live) = consumed.carrier.with_work(|work| {
                Ok::<_, MountSourceError>((
                    work.remaining().map_err(mount_demand_error)?, work.live_allocation_bytes(),
                ))
            })?;
            let path = consumed.value.take().expect("native remove input is live");
            let receipt = candidate.checkout.remove(path, expected, budget, &cancellation).await;
            consumed.carrier.accept_with_prior_live(receipt, prior_live, mount_engine_error)
        };
        let installed = self.checkout.finish_native_phase_with_carrier(
            candidate, epoch, stamp, &ViewChange::Unbound(namespace, node),
            phase_carrier, &cancellation, Some(&self.cancellation),
        ).await;
        drop(input);
        drop(namespace_allocation);
        accepted?;
        installed?;
        self.checkout.publish_mutation_with_carrier(
            carrier, &cancellation, Some(&self.cancellation),
            self.native_callback_guard.as_ref(),
        ).await
    }

    async fn node_pair_for_rename_with_carrier(
        &self,
        checkout: &Checkout<A, O>,
        source: &NamespacePath,
        destination: &NamespacePath,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(FileId, Option<FileId>), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        use super::lazy::{mount_demand_error, mount_engine_error};
        let original = carrier.original_native_work();
        let inspector = if let Some(original) = original.as_ref() {
            checkout.inspector_with_native_work(original, cancellation).map_err(MountSourceError::Filesystem)?
        } else {
            let (budget, prior_live) = carrier.with_work(|work| {
                Ok::<_, MountSourceError>((
                    work.remaining().map_err(mount_demand_error)?, work.live_allocation_bytes(),
                ))
            })?;
            let inspected = checkout.inspector_measured(budget, cancellation);
            carrier.accept_with_prior_live(inspected, prior_live, mount_engine_error)?
        };
        let mut inspector = LookupObserverOwned::new_in(inspector, carrier.reborrow())?;
        let result = async {
            let mut nodes = [None, None];
            for (index, path) in [source, destination].into_iter().enumerate() {
                let accepted = if let Some(original) = original.as_ref() {
                    inspector.lookup_no_follow_with_native_work(
                        path, original, cancellation,
                    ).await.map_err(MountSourceError::Filesystem)
                } else {
                    let (budget, prior_live) = inspector.work.with_work(|work| {
                        Ok::<_, MountSourceError>((
                            work.remaining().map_err(mount_demand_error)?, work.live_allocation_bytes(),
                        ))
                    })?;
                    let lookup = inspector.lookup_no_follow(path, budget, cancellation).await;
                    let accepted = inspector.work.accept_with_prior_live(lookup, prior_live, mount_engine_error);
                    inspector.receipt_delivered();
                    accepted
                };
                let value = accepted?;
                nodes[index] = inspector.work.with_work(|work| {
                    work.check().map_err(mount_demand_error)?;
                    if value.record.is_some() {
                        work.charge(crate::WorkCounters {
                            bytes_copied: std::mem::size_of::<FileId>() as u64,
                            ..crate::WorkCounters::UNCHARGED
                        }).map_err(mount_demand_error)?;
                    }
                    Ok::<_, MountSourceError>(value.record.map(|record| record.file_id))
                })?;
                if index == 0 && nodes[0].is_none() {
                    return Err(MountSourceError::NotFound);
                }
            }
            Ok((nodes[0].ok_or(MountSourceError::NotFound)?, nodes[1]))
        }.await;
        let finished = inspector.finish();
        let nodes = result?;
        finished?;
        Ok(nodes)
    }

    fn rename_on_meter(
        &self,
        source: &MountPath,
        destination: &MountPath,
        replace: bool,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        self.runtime.block_on_measured(
            || async {
                self.rename_async_with_carrier(
                    source, destination, replace, &mut NativeDemandCarrier::Shared(meter),
                ).await
            },
            meter.as_ref(),
        ).map_err(capture_io_error)?
    }

    pub(super) async fn rename_async_with_carrier(
        &self,
        source: &MountPath,
        destination: &MountPath,
        replace: bool,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        use super::lazy::{mount_clone_namespace, mount_demand_error, mount_engine_error};
        let cancellation = carrier.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            Ok::<_, MountSourceError>(work.cancellation().clone())
        })?;
        let source_allocation;
        let source_namespace;
        (source_namespace, source_allocation) = self.namespace_path_with_carrier(source, carrier)?;
        let bytes = if source_allocation.is_some() { 0 } else { source_namespace.retained_bytes() };
        let mut source_input = NativeCarrierOwned { value: Some(source_namespace), carrier, bytes };
        let (source, source_carrier) = source_input.parts();
        let destination_allocation;
        let destination_namespace;
        (destination_namespace, destination_allocation) = self.namespace_path_with_carrier(destination, source_carrier)?;
        let bytes = if destination_allocation.is_some() { 0 } else { destination_namespace.retained_bytes() };
        let mut destination_input = NativeCarrierOwned {
            value: Some(destination_namespace), carrier: source_carrier, bytes,
        };
        let (destination, phase_carrier) = destination_input.parts();
        let (mut candidate, epoch, stamp) = self.checkout.prepare_native_phase_with_carrier(
            phase_carrier, &cancellation, Some(&self.cancellation),
        ).await?;
        let (moved, replaced) = self.node_pair_for_rename_with_carrier(
            &candidate.checkout, source, destination, phase_carrier, &cancellation,
        ).await?;
        let accepted = if let Some(original) = phase_carrier.original_native_work() {
            let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
            let mut operations = crate::kernel::Owned::<Vec<crate::kernel::Mutation>>::allocate(
                1, &mut accounting,
            ).map_err(MountSourceError::Filesystem)?;
            accounting.charge(crate::WorkCounters {
                bytes_copied: std::mem::size_of::<crate::kernel::Mutation>() as u64,
                ..crate::WorkCounters::UNCHARGED
            }).map_err(MountSourceError::Filesystem)?;
            let mut source_path = source.clone_with_accounting(
                &mut accounting, &cancellation,
            ).map_err(MountSourceError::Filesystem)?;
            let mut destination_path = destination.clone_with_accounting(
                &mut accounting, &cancellation,
            ).map_err(MountSourceError::Filesystem)?;
            operations.allocation.absorb(&mut source_path.1).map_err(MountSourceError::Filesystem)?;
            operations.allocation.absorb(&mut destination_path.1).map_err(MountSourceError::Filesystem)?;
            operations.value.push(crate::kernel::Mutation::Rename {
                source: source_path.0, destination: destination_path.0, replace,
            });
            let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) =
                operations.allocation else { unreachable!("native mutation keeps original custody"); };
            candidate.checkout.mutate_with_native_work(
                operations.value, allocation, &original, &cancellation,
            ).await.map_err(MountSourceError::Filesystem)
        } else {
            let consumed_source = phase_carrier.with_work(|work| {
                mount_clone_namespace(source.components(), self.limits, work)
            })?;
            let bytes = consumed_source.retained_bytes();
            let mut consumed_source = NativeCarrierOwned {
                value: Some(consumed_source), carrier: phase_carrier, bytes,
            };
            let consumed_destination = consumed_source.carrier.with_work(|work| {
                mount_clone_namespace(destination.components(), self.limits, work)
            })?;
            let bytes = consumed_destination.retained_bytes();
            let mut consumed_destination = NativeCarrierOwned {
                value: Some(consumed_destination), carrier: &mut *consumed_source.carrier, bytes,
            };
            let (budget, prior_live) = consumed_destination.carrier.with_work(|work| {
                Ok::<_, MountSourceError>((
                    work.remaining().map_err(mount_demand_error)?, work.live_allocation_bytes(),
                ))
            })?;
            let source_path = consumed_source.value.take().expect("native rename source is live");
            let destination_path = consumed_destination.value.take().expect("native rename destination is live");
            let carrier = &mut *consumed_destination.carrier;
            let receipt = candidate.checkout.rename(
                source_path, destination_path, replace, budget, &cancellation,
            ).await;
            carrier.accept_with_prior_live(receipt, prior_live, mount_engine_error)
        };
        let installed = self.checkout.finish_native_phase_with_carrier(
            candidate, epoch, stamp,
            &ViewChange::Moved { from: source, to: destination, moved, replaced },
            phase_carrier, &cancellation, Some(&self.cancellation),
        ).await;
        drop(destination_input);
        drop(destination_allocation);
        drop(source_input);
        drop(source_allocation);
        accepted?;
        installed?;
        self.checkout.publish_mutation_with_carrier(
            carrier, &cancellation, Some(&self.cancellation),
            self.native_callback_guard.as_ref(),
        ).await
    }

    fn attached_from_id_with_carrier(
        &self,
        file_id: FileId,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>
    where A: AsyncAuthorityStore + Send + Sync + 'static,
          O: AsyncObjectStore + Send + Sync + 'static {
        use super::lazy::mount_demand_error;
        let bytes = std::mem::size_of::<CheckoutAttachedFile<A, O>>() as u64;
        if let Some(original) = carrier.original_native_work() {
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            let allocation = original.claim_allocation(bytes, 1).map_err(MountSourceError::NativeWork)?;
            let handle = Arc::new(CheckoutAttachedFile {
                checkout: Arc::clone(&self.checkout), file_id, runtime: self.runtime.clone(),
                cancellation: self.cancellation.clone(), profile: self.profile, limits: self.limits,
                native_callback_guard: self.native_callback_guard.as_ref().map(Arc::clone),
                _native_allocation: Some(allocation),
            });
            original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            return Ok((handle, 0));
        }
        let copied = crate::WorkCounters { bytes_copied: bytes, ..crate::WorkCounters::UNCHARGED };
        carrier.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            work.work.admit(&copied, &work.budget).map_err(MountSourceError::Work)?;
            work.allocate(bytes).map_err(mount_demand_error)
        })?;
        let mut owned = NativeCarrierOwned { value: None, carrier, bytes };
        owned.value = Some(Arc::new(CheckoutAttachedFile {
            checkout: Arc::clone(&self.checkout), file_id, runtime: self.runtime.clone(),
            cancellation: self.cancellation.clone(), profile: self.profile, limits: self.limits,
            native_callback_guard: self.native_callback_guard.as_ref().map(Arc::clone),
            _native_allocation: None,
        }));
        owned.carrier.with_work(|work| {
            work.record(copied).map_err(mount_demand_error)?;
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)
        })?;
        let handle = owned.value.take().expect("attached native handle is live");
        owned.bytes = 0;
        Ok((handle, bytes))
    }

    pub(super) async fn attached_file_by_id_with_carrier(
        &self,
        file_id: FileId,
        owner: CallbackOwner,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<Option<(Arc<dyn MountOpenFile>, u64)>, MountSourceError>
    where A: AsyncAuthorityStore + Send + Sync + 'static,
          O: AsyncObjectStore + Send + Sync + 'static {
        let record = match self.record_by_id_with_carrier(file_id, owner, carrier).await {
            Ok(record) => record,
            Err(MountSourceError::NotFound) => return Ok(None),
            Err(error) => return Err(error),
        };
        if record.kind != FileKind::Regular { return Err(MountSourceError::Stale); }
        self.attached_from_id_with_carrier(record.file_id, carrier).map(Some)
    }

    fn open_file_on_meter(
        &self,
        path: &MountPath,
        with_lookup: bool,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<((Arc<dyn MountOpenFile>, u64), Option<MountLookup>), MountSourceError>
    where A: AsyncAuthorityStore + Send + Sync + 'static,
          O: AsyncObjectStore + Send + Sync + 'static {
        let owner = ViewGate::callback_owner();
        let mut carrier = NativeDemandCarrier::Shared(meter);
        let mut owned = NativeCarrierOwned { value: None, carrier: &mut carrier, bytes: 0 };
        self.runtime.block_on_measured(|| async {
            let output = self.open_file_async_with_carrier(
                path, with_lookup, owner, owned.carrier,
            ).await?;
            owned.bytes = output.0.1;
            owned.value = Some(output);
            Ok::<_, MountSourceError>(())
        }, meter.as_ref()).map_err(capture_io_error)??;
        let output = owned.value.take().expect("native open result is live");
        owned.bytes = 0;
        Ok(output)
    }

    async fn open_file_async_with_carrier(
        &self,
        path: &MountPath,
        with_lookup: bool,
        owner: CallbackOwner,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<((Arc<dyn MountOpenFile>, u64), Option<MountLookup>), MountSourceError>
    where A: AsyncAuthorityStore + Send + Sync + 'static,
          O: AsyncObjectStore + Send + Sync + 'static {
        use super::lazy::{mount_accept, mount_demand_error, mount_engine_error};
        let (namespace, cancellation) = carrier.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            Ok::<_, MountSourceError>((
                self.namespace_path_with_work(path, work)?, work.cancellation().clone(),
            ))
        })?;
        let bytes = namespace.retained_bytes();
        let mut input = NativeCarrierOwned { value: Some(namespace), carrier, bytes };
        let (namespace, carrier) = input.parts();
        let observation = self.checkout.observe_owned_with_carrier(
            owner, carrier, &cancellation, Some(&self.cancellation),
        ).await?;
        let _view = observation._view;
        let mut observer = LookupObserverOwned::new_in(observation.observer, carrier.reborrow())?;
        let budget = observer.work.with_work(|work| work.remaining().map_err(mount_demand_error))?;
        let accepted = if with_lookup {
            let result = observer.lookup_no_follow_with_metadata(namespace, budget, &cancellation).await;
            observer.work.with_work(|work| mount_accept(work, result, mount_engine_error))
                .map(|value| value.map(|value| (value.record, Some(value.metadata))))
        } else {
            let result = observer.lookup_no_follow(namespace, budget, &cancellation).await;
            observer.work.with_work(|work| mount_accept(work, result, mount_engine_error))
                .map(|value| value.record.map(|record| (record, None)))
        };
        observer.receipt_delivered();
        let finished = observer.finish();
        let (record, metadata) = accepted?.ok_or(MountSourceError::NotFound)?;
        finished?;
        if record.kind != FileKind::Regular {
            return observer.work.with_work(|work| measured_native_name_error(
                "only regular files can own native file handles", true, work,
            ));
        }
        let lookup = observer.work.with_work(|work| {
            if metadata.is_some() {
                work.charge(crate::WorkCounters {
                    bytes_copied: std::mem::size_of::<MountLookup>() as u64,
                    ..crate::WorkCounters::UNCHARGED
                }).map_err(mount_demand_error)?;
            }
            Ok::<_, MountSourceError>(metadata.map(|metadata| MountLookup {
                node: mount_node(record), metadata,
            }))
        })?;
        let handle = self.attached_from_id_with_carrier(record.file_id, &mut observer.work)?;
        Ok((handle, lookup))
    }

    fn open_created_on_meter(
        &self,
        created: &MountLookup,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>
    where A: AsyncAuthorityStore + Send + Sync + 'static,
          O: AsyncObjectStore + Send + Sync + 'static {
        self.open_created_with_carrier(created, NativeDemandCarrier::Shared(meter), meter.as_ref())
    }

    fn open_created_with_carrier(
        &self, created: &MountLookup, mut carrier: NativeDemandCarrier<'_>,
        runtime_meter: &dyn acyclic_native_runtime::NativeReadMeter,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>
    where A: AsyncAuthorityStore + Send + Sync + 'static,
          O: AsyncObjectStore + Send + Sync + 'static {
        use super::lazy::{mount_demand_error, mount_items};
        let pending = if let Some(original) = carrier.original_native_work() {
            original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            if created.node.kind != MountNodeKind::Regular {
                return Err(MountSourceError::NativeStaticText {
                    kind: super::NativeSourceTextKind::Invalid,
                    text: "only regular files can own native file handles",
                });
            }
            let held = self.checkout.pending.lock().unwrap_or_else(PoisonError::into_inner);
            let mut found = None;
            for pending in held.iter() {
                original.charge(crate::WorkCounters {
                    items_examined: 1, ..crate::WorkCounters::UNCHARGED
                }).map_err(MountSourceError::NativeWork)?;
                if pending.file_id == created.node.file_id {
                    found = Some(Arc::clone(pending));
                    break;
                }
            }
            found
        } else {
            carrier.with_work(|work| {
                work.check().map_err(mount_demand_error)?;
                self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
                if created.node.kind != MountNodeKind::Regular {
                    return measured_native_name_error(
                        "only regular files can own native file handles", true, work,
                    );
                }
                let held = self.checkout.pending.lock().unwrap_or_else(PoisonError::into_inner);
                for pending in held.iter() {
                    mount_items(work, 1)?;
                    if pending.file_id == created.node.file_id {
                        work.charge(crate::WorkCounters {
                            bytes_copied: std::mem::size_of::<Arc<PendingCreate>>() as u64,
                            ..crate::WorkCounters::UNCHARGED
                        }).map_err(mount_demand_error)?;
                        return Ok::<_, MountSourceError>(Some(Arc::clone(pending)));
                    }
                }
                Ok(None)
            })?
        };
        let Some(pending) = pending else {
            let mut owned = NativeCarrierOwned { value: None, carrier: &mut carrier, bytes: 0 };
            self.runtime.block_on_measured(|| async {
                let handle = self.attached_file_by_id_with_carrier(
                    created.node.file_id, ViewGate::callback_owner(), owned.carrier,
                ).await?.ok_or(MountSourceError::NotFound)?;
                owned.bytes = handle.1;
                owned.value = Some(handle);
                Ok::<_, MountSourceError>(())
            }, runtime_meter).map_err(capture_io_error)??;
            let handle = owned.value.take().expect("created native identity is live");
            owned.bytes = 0;
            return Ok(handle);
        };
        let bytes = std::mem::size_of::<PendingCreatedFile<A, O>>() as u64;
        if let Some(original) = carrier.original_native_work() {
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            let allocation = original.claim_allocation(bytes, 1).map_err(MountSourceError::NativeWork)?;
            let handle = Arc::new(PendingCreatedFile {
                pending,
                attached: CheckoutAttachedFile {
                    checkout: Arc::clone(&self.checkout), file_id: created.node.file_id,
                    runtime: self.runtime.clone(), cancellation: self.cancellation.clone(),
                    profile: self.profile, limits: self.limits,
                    native_callback_guard: self.native_callback_guard.as_ref().map(Arc::clone),
                    _native_allocation: Some(allocation),
                },
            });
            original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            return Ok((handle, 0));
        }
        let copied = crate::WorkCounters { bytes_copied: bytes, ..crate::WorkCounters::UNCHARGED };
        carrier.with_work(|work| {
            work.work.admit(&copied, &work.budget).map_err(MountSourceError::Work)?;
            work.allocate(bytes).map_err(mount_demand_error)
        })?;
        let mut owned = NativeCarrierOwned { value: None, carrier: &mut carrier, bytes };
        owned.value = Some(Arc::new(PendingCreatedFile {
            pending,
            attached: CheckoutAttachedFile {
                checkout: Arc::clone(&self.checkout), file_id: created.node.file_id,
                runtime: self.runtime.clone(), cancellation: self.cancellation.clone(),
                profile: self.profile, limits: self.limits,
                native_callback_guard: self.native_callback_guard.as_ref().map(Arc::clone),
                _native_allocation: None,
            },
        }));
        owned.carrier.with_work(|work| {
            work.record(copied).map_err(mount_demand_error)?;
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)
        })?;
        let handle = owned.value.take().expect("pending native handle is live");
        owned.bytes = 0;
        Ok((handle, bytes))
    }

    fn detach_file_on_meter(
        &self,
        path: &MountPath,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>
    where A: AsyncAuthorityStore + Send + Sync + 'static,
          O: AsyncObjectStore + Send + Sync + 'static {
        let owner = ViewGate::callback_owner();
        let mut carrier = NativeDemandCarrier::Shared(meter);
        let mut owned = NativeCarrierOwned { value: None, carrier: &mut carrier, bytes: 0 };
        self.runtime.block_on_measured(|| async {
            let output = self.detach_file_async_with_carrier(path, owner, owned.carrier).await?;
            owned.bytes = output.1;
            owned.value = Some(output);
            Ok::<_, MountSourceError>(())
        }, meter.as_ref()).map_err(capture_io_error)??;
        let output = owned.value.take().expect("native detach result is live");
        owned.bytes = 0;
        Ok(output)
    }

    async fn detach_file_async_with_carrier(
        &self,
        path: &MountPath,
        owner: CallbackOwner,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>
    where A: AsyncAuthorityStore + Send + Sync + 'static,
          O: AsyncObjectStore + Send + Sync + 'static {
        use super::lazy::{mount_accept, mount_demand_error, mount_engine_error};
        if let Some(original) = carrier.original_native_work() {
            original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            let cancellation = carrier.cancellation();
            let namespace = self.namespace_path_with_carrier(path, carrier)?;
            let observation = self.checkout.observe_owned_with_carrier(
                owner, carrier, &cancellation, Some(&self.cancellation),
            ).await?;
            let _view = observation._view;
            let mut observer = LookupObserverOwned::new_in(observation.observer, carrier.reborrow())?;
            let lookup = observer.lookup_no_follow_with_metadata_with_native_work(
                &namespace.0, &original, &cancellation,
            ).await.map_err(mount_engine_error);
            observer.receipt_delivered();
            let lookup = lookup?.ok_or(MountSourceError::NotFound)?;
            if lookup.record.kind != FileKind::Regular {
                return Err(MountSourceError::NativeStaticText {
                    kind: super::NativeSourceTextKind::Unsupported,
                    text: "only regular files can own detached native handles",
                });
            }
            let file = observer.detached_from_record_with_native_work(
                lookup.record, &original, &cancellation,
            ).map_err(mount_engine_error)?;
            observer.receipt_delivered();
            observer.finish()?;
            let custody = observer.work.new_custody()?;
            let handle = self.detached_handle_from_identity_with_carrier(
                file, lookup.metadata, custody, &mut observer.work,
            )?;
            return Ok((handle, 0));
        }
        let (namespace, cancellation) = carrier.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            Ok::<_, MountSourceError>((
                self.namespace_path_with_work(path, work)?, work.cancellation().clone(),
            ))
        })?;
        let bytes = namespace.retained_bytes();
        let mut input = NativeCarrierOwned { value: Some(namespace), carrier, bytes };
        let (namespace, carrier) = input.parts();
        let observation = self.checkout.observe_owned_with_carrier(
            owner, carrier, &cancellation, Some(&self.cancellation),
        ).await?;
        let _view = observation._view;
        let mut observer = LookupObserverOwned::new_in(observation.observer, carrier.reborrow())?;
        let budget = observer.work.with_work(|work| work.remaining().map_err(mount_demand_error))?;
        let result = observer.lookup_no_follow_with_metadata(namespace, budget, &cancellation).await;
        let accepted = observer.work.with_work(|work| mount_accept(work, result, mount_engine_error));
        observer.receipt_delivered();
        let lookup = accepted?.ok_or(MountSourceError::NotFound)?;
        if lookup.record.kind != FileKind::Regular {
            return observer.work.with_work(|work| measured_native_name_error(
                "only regular files can own detached native handles", true, work,
            ));
        }
        let budget = observer.work.with_work(|work| work.remaining().map_err(mount_demand_error))?;
        let result = observer.detached_from_record_measured(lookup.record, budget, &cancellation);
        let (file, receipt) = match result {
            Ok(receipt) => (Some(receipt.value), Ok(crate::OperationReceipt {
                value: (), work: receipt.work,
            })),
            Err(failure) => (None, Err(failure)),
        };
        let accepted = observer.work.with_work(|work| mount_accept(work, receipt, mount_engine_error));
        observer.receipt_delivered();
        let finished = observer.finish();
        let Some(file) = file else { accepted?; unreachable!("failed detached receipt has no value") };
        let file_bytes = file.logical_owned_bytes();
        let mut file = NativeCarrierOwned {
            value: Some(file), carrier: &mut observer.work, bytes: 0,
        };
        let retained = &mut file.bytes;
        let adopted = file.carrier.with_work(|work| {
            let result = work.record_adoption(file_bytes).map_err(mount_demand_error);
            *retained = file_bytes;
            result
        });
        accepted?;
        finished?;
        adopted?;
        let payload_bytes = std::mem::size_of::<CheckoutDetachedFile<A, O>>() as u64;
        let bytes = file_bytes.checked_add(payload_bytes)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let copied = crate::WorkCounters {
            bytes_copied: payload_bytes, ..crate::WorkCounters::UNCHARGED
        };
        file.carrier.with_work(|work| {
            work.work.admit(&copied, &work.budget).map_err(MountSourceError::Work)?;
            work.allocate(payload_bytes).map_err(mount_demand_error)
        })?;
        file.bytes = bytes;
        let mut owned = NativeCarrierOwned {
            value: None, carrier: &mut *file.carrier, bytes: 0,
        };
        owned.value = Some(Arc::new(CheckoutDetachedFile {
            state: tokio::sync::Mutex::new(DetachedMountState {
                file: file.value.take().expect("detached native input is live"),
                metadata: lookup.metadata, mutation_epoch: 0,
            }),
            ledger: Arc::clone(&self.checkout.ledger), runtime: self.runtime.clone(),
            cancellation: self.cancellation.clone(), profile: self.profile, limits: self.limits,
            _custody: None,
            native_allocation: None,
        }));
        owned.bytes = bytes;
        file.bytes = 0;
        owned.carrier.with_work(|work| {
            work.record(copied).map_err(mount_demand_error)?;
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)
        })?;
        let handle = owned.value.take().expect("detached native handle is live");
        owned.bytes = 0;
        Ok((handle, bytes))
    }

    fn lookup_on_native_work(
        &self,
        path: &MountPath,
        native: &super::NativeMountMeterWork,
    ) -> Result<Option<MountLookup>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let owner = ViewGate::callback_owner();
        let original = native.native_materialization_work();
        let runtime_meter = original.runtime_meter_owner();
        self.runtime.block_on_measured(
            || async {
                self.lookup_async_with_carrier(
                    path, owner, &mut NativeDemandCarrier::Native(native),
                ).await
            },
            runtime_meter.as_ref(),
        ).map_err(|error| MountSourceError::NativeWork(original.runtime_error(error)))?
    }

    /// Resolves through the same observer engine using the caller's complete
    /// original native capability, or its explicit ordinary demand scope.
    pub(super) async fn lookup_async_with_carrier(
        &self,
        path: &MountPath,
        owner: CallbackOwner,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<Option<MountLookup>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        use super::lazy::{mount_accept, mount_demand_error, mount_engine_error};
        let cancellation = carrier.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            Ok::<_, MountSourceError>(work.cancellation().clone())
        })?;
        let native_allocation;
        let namespace;
        (namespace, native_allocation) = self.namespace_path_with_carrier(path, carrier)?;
        let bytes = if native_allocation.is_some() { 0 } else { namespace.retained_bytes() };
        let mut input = NativeCarrierOwned { value: Some(namespace), carrier, bytes };
        let (namespace, carrier) = input.parts();
        if let Some(pending) = carrier.with_work(|work| {
            self.checkout.pending_lookup_with_work(namespace, work)
        })? {
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            return Ok(Some(pending));
        }
        let checkout = self.checkout.observe_as_is_with_carrier(
            owner, carrier, &cancellation, Some(&self.cancellation),
        ).await?;
        let observer = if let Some(original) = carrier.original_native_work() {
            checkout.observer_with_native_work(&original, &cancellation).map_err(mount_engine_error)?
        } else {
            let budget = carrier.with_work(|work| work.remaining().map_err(mount_demand_error))?;
            let observed = checkout.observer_measured(budget, &cancellation);
            carrier.with_work(|work| mount_accept(work, observed, mount_engine_error))?
        };
        let mut observer = LookupObserverOwned::new_in(observer, carrier.reborrow())?;
        self.lookup_observer_with_native_work(namespace, &cancellation, &mut observer).await
    }

    pub(super) async fn lookup_in_with_carrier(
        &self,
        checkout: &Checkout<A, O>,
        path: &MountPath,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<Option<MountLookup>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        use super::lazy::{mount_accept, mount_demand_error, mount_engine_error};
        let cancellation = carrier.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            Ok::<_, MountSourceError>(work.cancellation().clone())
        })?;
        let native_allocation;
        let namespace;
        (namespace, native_allocation) = self.namespace_path_with_carrier(path, carrier)?;
        let bytes = if native_allocation.is_some() { 0 } else { namespace.retained_bytes() };
        let mut input = NativeCarrierOwned { value: Some(namespace), carrier, bytes };
        let (namespace, carrier) = input.parts();
        let observer = if let Some(original) = carrier.original_native_work() {
            checkout.inspector_with_native_work(&original, &cancellation).map_err(mount_engine_error)?
        } else {
            let budget = carrier.with_work(|work| work.remaining().map_err(mount_demand_error))?;
            let inspected = checkout.inspector_measured(budget, &cancellation);
            carrier.with_work(|work| mount_accept(work, inspected, mount_engine_error))?
        };
        let mut observer = LookupObserverOwned::new_in(observer, carrier.reborrow())?;
        self.lookup_observer_with_native_work(namespace, &cancellation, &mut observer).await
    }

    async fn lookup_observer_with_native_work(
        &self,
        namespace: &NamespacePath,
        cancellation: &CancellationToken,
        observer: &mut LookupObserverOwned<'_, '_, A, O>,
    ) -> Result<Option<MountLookup>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        use super::lazy::{mount_accept, mount_demand_error, mount_engine_error};
        observer.work.with_work(|work| work.check().map_err(mount_demand_error))?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        // SDK providers may meter their actual leaf allocations through this
        // same carrier: never retain its mutex across a canonical SDK await.
        let accepted = if let Some(original) = observer.work.original_native_work() {
            // Native accounting is already applied to this exact original
            // ledger, including failure prefixes and intrinsic output guards.
            observer.lookup_no_follow_with_metadata_with_native_work(
                namespace, &original, cancellation,
            ).await.map_err(mount_engine_error)
        } else {
            let budget = observer.work.with_work(|work| work.remaining().map_err(mount_demand_error))?;
            let result = observer.lookup_no_follow_with_metadata(namespace, budget, cancellation).await;
            observer.work.with_work(|work| mount_accept(work, result, mount_engine_error))
        };
        observer.receipt_delivered();
        let finished = observer.finish();
        let value = accepted?;
        finished?;
        observer.work.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            if value.is_some() {
                work.charge(crate::WorkCounters {
                    bytes_copied: std::mem::size_of::<MountLookup>() as u64,
                    ..crate::WorkCounters::UNCHARGED
                }).map_err(mount_demand_error)?;
            }
            Ok(value.map(|value| MountLookup {
                node: mount_node(value.record), metadata: value.metadata,
            }))
        })
    }

    /// Tests the same authored stamp using measured canonical namespace
    /// conversion and the caller's original cancellation and resource scope.
    pub(super) fn unchanged_since_with_carrier(
        &self,
        path: &MountPath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<bool, MountSourceError> {
        use super::lazy::mount_demand_error;
        carrier.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)
        })?;
        let native_allocation;
        let namespace;
        (namespace, native_allocation) = self.namespace_path_with_carrier(path, carrier)?;
        let bytes = if native_allocation.is_some() { 0 } else { namespace.retained_bytes() };
        let mut input = NativeCarrierOwned { value: Some(namespace), carrier, bytes };
        let (namespace, carrier) = input.parts();
        carrier.with_work(|work| {
            work.charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            }).map_err(mount_demand_error)?;
            if !self.checkout.view_gate.is_stable() { return Ok(false); }
            let unchanged = self.checkout.ledger.unchanged_since(namespace, file_id, stamp, work)?;
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            Ok(unchanged)
        })
    }

    pub(super) fn record_by_id(&self, file_id: FileId) -> Result<FileRecord, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync,
        O: AsyncObjectStore + Send + Sync,
    {
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| async {
            let observation = self.checkout.observe(owner).await?;
            observation.ensure_publication_resolved()?;
            observation
                .observer()
                .read_file_record_by_id(file_id, boundary_budget(), &self.cancellation)
                .await
                .map(|receipt| receipt.value)
                .map_err(engine_error)
        })
    }

    pub(super) fn detached_file_from_record(
        &self,
        record: FileRecord,
        metadata: FileMetadata,
    ) -> Result<Arc<CheckoutDetachedFile<A, O>>, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync,
        O: AsyncObjectStore + Send + Sync,
    {
        let owner = ViewGate::callback_owner();
        let file = self.runtime.wait(|| async {
            Ok(self
                .checkout
                .observe(owner)
                .await?
                .detached_from_record(record))
        })?;
        Ok(Arc::new(CheckoutDetachedFile {
            state: tokio::sync::Mutex::new(DetachedMountState {
                file,
                metadata,
                mutation_epoch: 0,
            }),
            ledger: Arc::clone(&self.checkout.ledger),
            runtime: self.runtime.clone(),
            cancellation: self.cancellation.clone(),
            profile: self.profile,
            limits: self.limits,
            _custody: None,
            native_allocation: None,
        }))
    }

    pub(super) async fn record_by_id_with_carrier(
        &self,
        file_id: FileId,
        owner: CallbackOwner,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<FileRecord, MountSourceError>
    where A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
        use super::lazy::{mount_accept, mount_demand_error, mount_engine_error};
        let cancellation = carrier.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            Ok::<_, MountSourceError>(work.cancellation().clone())
        })?;
        let observation = self.checkout.observe_owned_with_carrier(
            owner, carrier, &cancellation, Some(&self.cancellation),
        ).await?;
        let mut observer = LookupObserverOwned::new_in(observation.observer, carrier.reborrow())?;
        let budget = observer.work.with_work(|work| work.remaining().map_err(mount_demand_error))?;
        let result = observer.read_file_record_by_id(file_id, budget, &cancellation).await;
        let accepted = observer.work.with_work(|work| mount_accept(work, result, mount_engine_error));
        observer.receipt_delivered();
        let finished = observer.finish();
        let record = accepted?;
        finished?;
        observer.work.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)
        })?;
        Ok(record)
    }

    pub(super) async fn detached_file_from_record_with_carrier(
        &self,
        record: FileRecord,
        metadata: FileMetadata,
        owner: CallbackOwner,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<Arc<CheckoutDetachedFile<A, O>>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        use super::lazy::mount_demand_error;
        let custody = carrier.new_custody()?;
        carrier.with_work(|work| work.check()).map_err(mount_demand_error)?;
        let cancellation = carrier.cancellation();
        let file = self.detached_identity_from_record_with_carrier(
            record, owner, carrier, &cancellation,
        ).await?;
        self.detached_handle_from_identity_with_carrier(file, metadata, custody, carrier)
    }

    fn detached_handle_from_identity_with_carrier(
        &self,
        file: crate::DetachedFile<A, O>,
        metadata: FileMetadata,
        custody: super::NativeMountAllocationCustody,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<Arc<CheckoutDetachedFile<A, O>>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let original = carrier.original_native_work();
        let bytes = if original.is_some() { 0 } else { file.logical_owned_bytes() };
        let mut owned = NativeCarrierOwned { value: Some(file), carrier, bytes };
        let native_allocation = if let Some(original) = &original {
            Some(original.claim_allocation(std::mem::size_of::<CheckoutDetachedFile<A, O>>() as u64, 1)
                .map_err(MountSourceError::NativeWork)?)
        } else {
            owned.carrier.with_work(|work| {
                custody.retain_admitted(bytes, work).map_err(super::lazy::mount_demand_error)
            })?;
            owned.bytes = 0;
            owned.carrier.with_work(|work| {
                custody.claim(std::mem::size_of::<CheckoutDetachedFile<A, O>>() as u64, work)
                    .map_err(super::lazy::mount_demand_error)
            })?;
            None
        };
        Ok(Arc::new(CheckoutDetachedFile {
            state: tokio::sync::Mutex::new(DetachedMountState {
                file: owned.value.take().expect("detached native identity is live"),
                metadata, mutation_epoch: 0,
            }),
            ledger: Arc::clone(&self.checkout.ledger),
            runtime: self.runtime.clone(),
            cancellation: self.cancellation.clone(),
            profile: self.profile,
            limits: self.limits,
            _custody: Some(custody),
            native_allocation,
        }))
    }

    pub(super) async fn detached_identity_from_record_with_carrier(
        &self,
        record: FileRecord,
        owner: CallbackOwner,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::DetachedFile<A, O>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let observation = self.checkout.observe_with_carrier(
            owner, carrier, cancellation, Some(&self.cancellation),
        ).await?;
        // The existing SDK constructor is synchronous; no checkout-state or
        // original-meter guard survives an SDK await here.
        if let Some(original) = carrier.original_native_work() {
            observation.detached_from_record_with_native_work(record, &original, cancellation)
                .map_err(super::lazy::mount_engine_error)
        } else {
            self.detached_identity_create_with_carrier(carrier, cancellation, |budget| {
                observation.detached_from_record_measured(record, budget, cancellation)
            })
        }
    }

    fn detached_identity_create_with_carrier(
        &self,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        create: impl FnOnce(WorkBudget) -> Result<
            crate::OperationReceipt<crate::DetachedFile<A, O>>,
            crate::OperationFailure<FsError>,
        >,
    ) -> Result<crate::DetachedFile<A, O>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        use super::lazy::{mount_accept, mount_demand_error, mount_engine_error};
        let file = carrier.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
            let budget = work.remaining().map_err(mount_demand_error)?;
            let file = mount_accept(work, create(budget), mount_engine_error)?;
            if let Err(error) = work.adopt(file.logical_owned_bytes()) {
                drop(file);
                return Err(mount_demand_error(error));
            }
            Ok(file)
        })?;
        let bytes = file.logical_owned_bytes();
        let mut owned = NativeCarrierOwned { value: Some(file), carrier, bytes };
        owned.carrier.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            self.cancellation.check().map_err(|_| MountSourceError::Stale)
        })?;
        let file = owned.value.take().expect("detached native identity is live");
        owned.bytes = 0;
        Ok(file)
    }

    /// Creates an independently cancellable adapter using the shared callback runtime.
    ///
    /// # Errors
    ///
    /// Returns a driver failure when the bounded callback runtime cannot start.
    pub fn new(
        checkout: Arc<SharedCheckout<A, O>>,
        config: VolumeConfig,
    ) -> Result<Self, NativeMountError> {
        let root = NamespacePath::new(Vec::new(), config.limits)
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        Self::new_at(checkout, config, root)
    }

    /// Creates an adapter rooted at an authenticated workspace subtree.
    ///
    /// # Errors
    ///
    /// Returns a profile or runtime failure before any native mount starts.
    pub fn new_at(
        checkout: Arc<SharedCheckout<A, O>>,
        config: VolumeConfig,
        root: NamespacePath,
    ) -> Result<Self, NativeMountError> {
        if !profile_is_native(config.profile) {
            return Err(NativeMountError::ProfileUnavailable {
                profile: profile_name(config.profile),
                platform: std::env::consts::OS,
            });
        }
        initialize_native_mount_services()?;
        Self::new_at_measured(
            checkout, config, root,
            &mut crate::kernel::allocation::AllocationLedger::default(),
            &mut crate::WorkCounters::UNCHARGED, WorkBudget::UNBOUNDED,
        )
    }

    /// Transfers the exact owned namespace and its original allocation guard.
    /// No allocator, SDK constructor or destructor runs under the work mutex.
    pub(crate) fn new_at_meter(
        checkout: Arc<SharedCheckout<A, O>>, config: VolumeConfig,
        root: NamespacePath, root_allocation: crate::NativeAllocation,
        original: &super::NativeMountMeterWork,
        native_original: Option<&crate::NativeMaterializationWork>,
    ) -> Result<Self, NativeMountError> {
        // This aggregate also orders cleanup on every early return.
        struct OwnedRoot {
            path: NamespacePath,
            allocation: crate::NativeAllocation,
        }
        let owned = OwnedRoot { path: root, allocation: root_allocation };
        let work = original.native_materialization_work().allocation_work();
        let native_io_binding = validate_native_source_io_binding(&work, native_original)?;
        if !owned.allocation.work().shares_ledger(&work)
            || !owned.allocation.work().shares_resource_allowance(&work)
            || owned.allocation.bytes() != owned.path.retained_bytes()
        {
            return Err(MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::InvalidLayout,
            ).into());
        }
        work.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        if !profile_is_native(config.profile) {
            return Err(NativeMountError::ProfileUnavailable {
                profile: profile_name(config.profile),
                platform: std::env::consts::OS,
            });
        }
        let runtime = CallbackRuntime::existing()?;
        let cancellation = CancellationToken::new_with_native_work(&work)
            .map_err(MountSourceError::NativeWork)?;
        let mut cleanup_credit = work.reserve_work(crate::WorkCounters {
            items_examined: 2, ..crate::WorkCounters::UNCHARGED
        }).map_err(MountSourceError::NativeWork)?;
        let final_credit = cleanup_credit.partition_items(1).map_err(MountSourceError::NativeWork)?;
        Ok(Self {
            checkout,
            root: owned.path,
            limits: config.limits,
            profile: config.profile,
            folds_names: config.case_sensitivity == CaseSensitivity::ProfileFolded
                && matches!(config.profile, FilesystemProfile::Portable | FilesystemProfile::Browser),
            cancellation,
            capture_gate: StdMutex::new(CaptureSourceGate::default()),
            capture_quiesced: std::sync::Condvar::new(),
            runtime,
            native_original: Some(work),
            native_io_binding,
            native_callback_guard: None,
            cleanup_credit: StdMutex::new(Some(cleanup_credit)),
            cleanup_final_credit: StdMutex::new(Some(final_credit)),
            _namespace_allocation: Some(owned.allocation),
            _source_allocation: None,
        })
    }

    /// Admits the actual shared payload before allocating its `Arc`.
    /// Its independent namespace and cancellation claims are not adopted.
    pub(crate) fn into_arc_meter(
        mut self, original: &super::NativeMountMeterWork,
    ) -> Result<Arc<Self>, NativeMountError>
    where A: Send + Sync + 'static, O: Send + Sync + 'static,
    {
        let work = original.native_materialization_work();
        let Some(stored) = self.native_original.as_ref() else {
            return Err(MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::InvalidLayout,
            ).into());
        };
        if !stored.shares_ledger(&work) || !stored.shares_resource_allowance(&work) {
            return Err(MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::InvalidLayout,
            ).into());
        }
        self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
        let allocation = work.claim_allocation(std::mem::size_of::<Self>() as u64, 1)
            .map_err(MountSourceError::NativeWork)?;
        self._source_allocation = Some(allocation);
        // Physical allocation is outside the original accounting leaf. The
        // guard is intrinsic and retires only after this actual Self drops.
        Ok(Arc::new(self))
    }

    pub(super) fn new_at_measured(
        checkout: Arc<SharedCheckout<A, O>>,
        config: VolumeConfig,
        root: NamespacePath,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut crate::WorkCounters,
        budget: WorkBudget,
    ) -> Result<Self, NativeMountError> {
        if !profile_is_native(config.profile) {
            return Err(NativeMountError::ProfileUnavailable {
                profile: profile_name(config.profile),
                platform: std::env::consts::OS,
            });
        }
        let runtime = CallbackRuntime::existing()?;
        let cancellation = CancellationToken::new_measured(allocations, work, budget)?;
        Ok(Self {
            checkout,
            root,
            limits: config.limits,
            profile: config.profile,
            // Only UTF-8 names have case variants; the others fold to
            // themselves.
            folds_names: config.case_sensitivity == CaseSensitivity::ProfileFolded
                && matches!(
                    config.profile,
                    FilesystemProfile::Portable | FilesystemProfile::Browser
                ),
            cancellation,
            capture_gate: StdMutex::new(CaptureSourceGate::default()),
            capture_quiesced: std::sync::Condvar::new(),
            runtime,
            native_original: None,
            native_io_binding: NativeSourceIoBinding::Ordinary,
            native_callback_guard: None,
            cleanup_credit: StdMutex::new(None),
            cleanup_final_credit: StdMutex::new(None),
            _namespace_allocation: None,
            _source_allocation: None,
        })
    }

    /// Whether `path` still names what it named after `stamp`, whatever
    /// changed beneath it.
    pub(super) fn binding_unchanged_since(&self, path: &MountPath, stamp: ViewStamp) -> bool {
        self.path(path)
            .is_ok_and(|path| self.checkout.binding_unchanged_since(&path, stamp))
    }

    /// Returns the checkout's owning volume without filesystem I/O.
    ///
    /// # Errors
    ///
    /// Returns stale when callback admission exceeds the finite deadline.
    pub fn volume_id(&self) -> Result<VolumeId, MountSourceError>
    where
        A: Send + Sync,
        O: Send + Sync,
    {
        let owner = ViewGate::callback_owner();
        self.runtime
            .wait(|| async { Ok(self.checkout.observe_as_is(owner).await?.volume_id()) })
    }

    pub(super) fn with_capture_observer_dispatch<T>(
        &self, dispatch: impl FnOnce() -> T,
    ) -> T {
        super::lazy::with_capture_observer_dispatch(
            std::ptr::from_ref(&self.capture_gate) as usize, dispatch,
        )
    }

    fn cancel_capture_engine(
        &self,
        wait: Option<&CancellationToken>,
        mut phase: impl FnMut(bool) -> Result<Option<crate::materializer::NativeItemReservation>, MountSourceError>,
        mut cancel: impl FnMut(&CancellationToken) -> Result<crate::cancellation::PreparedCancellation, MountSourceError>,
    ) -> Result<(), MountSourceError> {
        if self.capture_retired() { return Ok(()); }
        if let Some(wait) = wait { wait.check().map_err(|_| MountSourceError::Cancelled)?; }
        let prepared = {
            let mut gate = self.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
            if gate.cancelled { None } else {
                let mut credit = phase(false)?;
                let prepared = cancel(&self.cancellation);
                if let Some(credit) = &mut credit {
                    credit.consume(1).map_err(MountSourceError::NativeWork)?;
                }
                let prepared = prepared?;
                gate.cancelled = true;
                Some(prepared)
            }
        };
        if let Some(prepared) = prepared { prepared.finish(); }
        loop {
            if let Some(wait) = wait { wait.check().map_err(|_| MountSourceError::Cancelled)?; }
            let step = {
                let mut gate = self.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
                if gate.cancel_next == gate.active.len() { None } else {
                    let index = gate.cancel_next;
                    let request = gate.active[index].request.upgrade();
                    let visit = match gate.active[index].cancel_visit.take() {
                        Some(visit) => Some(visit),
                        None => phase(false)?,
                    };
                    let prepared = request.as_ref().map(|request| cancel(&request.cancellation)).transpose();
                    if let Some(mut visit) = visit {
                        visit.consume(1).map_err(MountSourceError::NativeWork)?;
                    }
                    let prepared = prepared?;
                    if let Some(request) = request {
                        let _ = request.state.compare_exchange(
                            CAPTURE_PENDING, CAPTURE_CANCELLED, Ordering::AcqRel, Ordering::Acquire,
                        );
                    }
                    gate.cancel_next += 1;
                    Some(prepared)
                }
            };
            let Some(prepared) = step else { break; };
            if let Some(prepared) = prepared { prepared.finish(); }
        }
        if super::lazy::is_current_capture_observer_dispatch(
            std::ptr::from_ref(&self.capture_gate) as usize,
        ) { return Err(MountSourceError::Stale); }
        loop {
            if self.capture_retired() { return Ok(()); }
            if let Some(wait) = wait { wait.check().map_err(|_| MountSourceError::Cancelled)?; }
            // The existing last-producer notification maintains this hint.
            // The dedicated final credit has been owned since Source birth;
            // the hint is bookkeeping, never a terminal proof or IO grant.
            let quiescent_hint = {
                let gate = self.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
                !gate.producers_pending && !gate.retiring
            };
            let mut credit = phase(quiescent_hint)?;
            let mut gate = self.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
            if gate.retired { return Ok(()); }
            if quiescent_hint {
                // Cancellation forbids every new registration/producer birth.
                // Now make the actual, originally funded terminal predicate.
                let terminal = gate.producers == 0 && !gate.retiring;
                if !terminal {
                    if let Some(credit) = &mut credit {
                        credit.consume(1).map_err(MountSourceError::NativeWork)?;
                    }
                    return Err(MountSourceError::Stale);
                }
                gate.retiring = true;
                let vector = std::mem::take(&mut gate.active);
                let allocation = gate.allocation.take();
                let native = gate.native_allocation.take();
                gate.cancel_next = 0;
                drop(gate);
                retire_capture_vector(vector, allocation, native);
                if let Some(credit) = &mut credit {
                    credit.consume(1).map_err(MountSourceError::NativeWork)?;
                }
                let unused = self.cleanup_credit.lock().unwrap_or_else(PoisonError::into_inner).take();
                drop(unused);
                self.capture_gate.lock().unwrap_or_else(PoisonError::into_inner).retired = true;
                self.capture_quiesced.notify_all();
                return Ok(());
            }
            let producers_pending = gate.producers != 0 || gate.retiring;
            if let Some(credit) = &mut credit {
                credit.consume(1).map_err(MountSourceError::NativeWork)?;
            }
            if !producers_pending { drop(gate); continue; }
            gate = self.capture_quiesced.wait_timeout(
                gate, std::time::Duration::from_millis(10),
            ).unwrap_or_else(PoisonError::into_inner).0;
            drop(gate);
        }
    }

    pub(super) fn capture_retired(&self) -> bool {
        let gate = self.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
        gate.retired
    }

    pub(super) fn cancel_with_cleanup_wait(
        &self, wait: &CancellationToken,
    ) -> Result<(), MountSourceError> {
        self.cancel_with_cleanup(Some(wait))
    }

    pub(super) fn cancel_accepted_cleanup(&self) -> Result<(), MountSourceError> {
        self.cancel_with_cleanup(None)
    }

    fn cancel_with_cleanup(&self, wait: Option<&CancellationToken>) -> Result<(), MountSourceError> {
        self.cancel_capture_engine(wait, |final_predicate| {
            if final_predicate {
                if let Some(wait) = wait { wait.check().map_err(|_| MountSourceError::Cancelled)?; }
                return self.cleanup_final_credit.lock().unwrap_or_else(PoisonError::into_inner)
                    .take().map(Some).ok_or(MountSourceError::NativeWork(
                        crate::NativeTreeMaterializationError::InvalidLayout,
                    ));
            }
            let mut credit = self.cleanup_credit.lock().unwrap_or_else(PoisonError::into_inner);
            let credit = credit.as_mut().ok_or(MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::InvalidLayout,
            ))?;
            let loan = match wait {
                Some(wait) => credit.cleanup_loan(wait),
                None => credit.accepted_cleanup_loan(),
            };
            loan.reserve_items(1).map(Some).map_err(MountSourceError::NativeWork)
        }, |token| match wait {
            Some(wait) => token.prepare_cancel_with_cleanup_wait(wait),
            None => token.prepare_cancel_accepted_cleanup(),
        }.map_err(MountSourceError::NativeWork))
    }

    pub(crate) fn native_source_io_lease(
        self: &Arc<Self>,
    ) -> Result<NativeSourceIoLease, MountSourceError>
    where Self: Send + Sync + 'static {
        let owner: Arc<dyn NativeSourceIoRetirement> = Arc::clone(self) as Arc<dyn NativeSourceIoRetirement>;
        self.native_source_io_lease_for_owner(owner)
    }

    pub(super) fn native_source_io_lease_for_owner(
        &self, owner: Arc<dyn NativeSourceIoRetirement>,
    ) -> Result<NativeSourceIoLease, MountSourceError> {
        {
            let gate = self.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
            if gate.cancelled { return Err(MountSourceError::Stale); }
        }
        let original = self.native_original.as_ref().ok_or(MountSourceError::NativeWork(
            crate::NativeTreeMaterializationError::InvalidLayout,
        ))?;
        let mut poll_credit = self.reserve_capture_poll()?;
        let allocation = original.claim_allocation(std::mem::size_of::<NativeSourceIoLeaseInner>() as u64, 1)
            .map_err(MountSourceError::NativeWork)?;
        let mut gate = self.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
        if gate.cancelled { return Err(MountSourceError::Stale); }
        self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
        let producers = gate.producers.checked_add(1)
            .ok_or(MountSourceError::Work(WorkError::Overflow))?;
        self.accept_capture_poll(&mut poll_credit)?;
        gate.producers = producers;
        gate.producers_pending = true;
        gate.ever_used = true;
        let inner = Arc::new(NativeSourceIoLeaseInner {
            owner, binding: self.native_io_binding, _allocation: allocation,
        });
        drop(gate);
        Ok(NativeSourceIoLease { inner })
    }

    fn reserve_capture_poll(&self) -> Result<Option<crate::materializer::NativeWorkReservation>, MountSourceError> {
        self.native_original.as_ref().map(|original| original.reserve_work(crate::WorkCounters {
            items_examined: 1, ..crate::WorkCounters::UNCHARGED
        }).map_err(MountSourceError::NativeWork)).transpose()
    }

    fn accept_capture_poll(
        &self, credit: &mut Option<crate::materializer::NativeWorkReservation>,
    ) -> Result<(), MountSourceError> {
        if let Some(credit) = credit {
            self.cleanup_credit.lock().unwrap_or_else(PoisonError::into_inner)
                .as_mut().ok_or(MountSourceError::NativeWork(
                    crate::NativeTreeMaterializationError::InvalidLayout,
                ))?.absorb(credit).map_err(MountSourceError::NativeWork)?;
        }
        Ok(())
    }

    pub(super) fn cancel_with_carrier(
        &self, carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        if let Some(original) = carrier.original_native_work() {
            return self.cancel_with_cleanup_wait(original.cancellation());
        }
        let wait = carrier.with_work(|work| work.cancellation().clone());
        let carrier = std::cell::RefCell::new(carrier);
        self.cancel_capture_engine(Some(&wait), |_final_predicate| {
            carrier.borrow_mut().with_work(|work| {
                work.check().map_err(super::lazy::mount_demand_error)?;
                super::lazy::mount_items(work, 1).map(|()| None)
            })
        }, |token| carrier.borrow_mut().with_work(|work| {
            token.prepare_cancel_measured(&mut work.work, work.budget).map_err(MountSourceError::Work)
        }))
    }

    /// Cancels future and in-flight canonical operations owned by this mount.
    pub fn cancel(&self) {
        let cleanup = CancellationToken::new();
        let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cleanup);
        self.cancel_with_carrier(&mut NativeDemandCarrier::Borrowed(&mut work))
            .expect("ordinary SDK cancellation uses the same unbounded cleanup engine");
    }

    /// Publishes every pending mount mutation regardless of automatic policy.
    ///
    /// # Errors
    ///
    /// Returns the exact fenced publication failure and retains retry identity.
    pub fn sync(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync,
        O: AsyncObjectStore + Send + Sync,
    {
        self.runtime.wait(|| self.sync_async())
    }

    /// Asynchronously publishes every pending mount mutation.
    ///
    /// This is the native customer handle path and does not enter the blocking
    /// foreign-thread callback bridge.
    ///
    /// # Errors
    ///
    /// Returns the exact fenced publication failure and retains retry identity.
    pub async fn sync_async(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.sync_async_with_permit_budgeted(
            crate::PublicationPermit::Unrestricted, boundary_budget(),
        ).await
    }

    /// Publishes pending mutations only while the supplied operation lease is
    /// still active in the shared authority provider.
    pub async fn sync_async_with_permit(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.sync_async_with_permit_budgeted(permit, boundary_budget())
            .await
    }

    pub(super) async fn sync_async_with_permit_budgeted(
        &self,
        permit: crate::PublicationPermit,
        budget: WorkBudget,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let span = crate::obs::span!(
            DEBUG,
            "acyclic.fs.mount.sync",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
        );
        let result = self.sync_async_with_permit_measured(permit, budget, &self.cancellation).await;
        crate::obs::measured_on(&span, result, |receipt| &receipt.work)
            .map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    /// Publishes the same mounted candidate under per-operation bounds.
    pub async fn sync_async_with_permit_measured(
        &self,
        permit: crate::PublicationPermit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<()>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let mut work = crate::demand::DemandWork::new(budget, cancellation);
        let mut view = super::view_ledger::ViewLedgerAdmission::default();
        let result = self.sync_async_with_permit_work(permit, false, &mut work, &mut view, cancellation).await;
        result.map(|value| crate::OperationReceipt { value, work: work.work })
            .map_err(|error| crate::OperationFailure::new(error, work.work))
    }

    pub(super) async fn sync_async_with_permit_work(
        &self,
        permit: crate::PublicationPermit,
        force: bool,
        work: &mut crate::demand::DemandWork,
        view: &mut super::view_ledger::ViewLedgerAdmission,
        cancellation: &CancellationToken,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        self.sync_async_with_permit_work_guarded(
            permit, force, work, view, cancellation,
            &mut self.sdk_publication_policy(),
        ).await
    }

    pub(super) async fn sync_async_with_permit_work_guarded<F, Fut, E>(
        &self,
        permit: crate::PublicationPermit,
        force: bool,
        work: &mut crate::demand::DemandWork,
        view: &mut super::view_ledger::ViewLedgerAdmission,
        cancellation: &CancellationToken,
        before_publication: &mut F,
    ) -> Result<(), E>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        self.sync_async_with_permit_carrier_guarded(
            permit, force, &mut NativeDemandCarrier::Borrowed(work),
            view, cancellation, before_publication,
        ).await
    }

    async fn sync_async_with_permit_meter(
        &self,
        permit: crate::PublicationPermit,
        force: bool,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
        cancellation: &CancellationToken,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        self.sync_async_with_permit_carrier_guarded(
            permit, force, &mut NativeDemandCarrier::Shared(meter),
            &mut super::view_ledger::ViewLedgerAdmission::default(), cancellation,
            &mut self.sdk_publication_policy(),
        ).await
    }

    pub(crate) async fn sync_async_with_permit_meter_guarded<F, Fut, E>(
        &self,
        permit: crate::PublicationPermit,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
        cancellation: &CancellationToken,
        before_publication: &mut F,
    ) -> Result<(), E>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        self.sync_async_with_permit_carrier_guarded(
            permit, false, &mut NativeDemandCarrier::Shared(meter),
            &mut super::view_ledger::ViewLedgerAdmission::default(),
            cancellation, before_publication,
        ).await
    }

    pub(crate) async fn sync_async_with_permit_native_work_guarded<F, Fut, E>(
        &self, permit: crate::PublicationPermit, original: &super::NativeMountMeterWork,
        cancellation: &CancellationToken, mut before_publication: F,
    ) -> Result<(), E>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        self.sync_async_with_permit_carrier_guarded(
            permit, false, &mut NativeDemandCarrier::Native(original),
            &mut super::view_ledger::ViewLedgerAdmission::default(),
            cancellation, &mut before_publication,
        ).await
    }

    pub(super) async fn sync_async_with_permit_carrier_guarded<F, Fut, E>(
        &self,
        permit: crate::PublicationPermit,
        force: bool,
        carrier: &mut NativeDemandCarrier<'_>,
        view: &mut super::view_ledger::ViewLedgerAdmission,
        cancellation: &CancellationToken,
        before_publication: &mut F,
    ) -> Result<(), E>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        self.sync_async_with_permit_carrier_guarded_evidence(
            permit, force, carrier, view, cancellation, before_publication, None, None,
        ).await.map(|_| ())
    }

    pub(crate) async fn sync_async_with_permit_qualified_native_work_guarded<F, Fut, E>(
        &self, permit: crate::PublicationPermit, original: &super::NativeMountMeterWork,
        cancellation: &CancellationToken, context: crate::lazy_workspace::NativeSnapshotContext,
        before_publication: F,
    ) -> Result<crate::lazy_workspace::NativePublicationSnapshot, (E, Option<MountSourceError>)>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        let mut acknowledged = None;
        self.sync_async_with_permit_qualified_native_work_guarded_with_ack(
            permit, original, cancellation, context, &mut acknowledged, before_publication,
        ).await
    }

    pub(crate) async fn sync_async_with_permit_qualified_native_work_guarded_with_ack<F, Fut, E>(
        &self, permit: crate::PublicationPermit, original: &super::NativeMountMeterWork,
        cancellation: &CancellationToken, context: crate::lazy_workspace::NativeSnapshotContext,
        acknowledged: &mut Option<crate::lazy_workspace::NativePublicationSnapshot>,
        mut before_publication: F,
    ) -> Result<crate::lazy_workspace::NativePublicationSnapshot, (E, Option<MountSourceError>)>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        self.sync_async_with_permit_carrier_guarded_context_with_ack(
            permit, false, &mut NativeDemandCarrier::Native(original),
            &mut super::view_ledger::ViewLedgerAdmission::default(), cancellation,
            &mut before_publication, Some(context), acknowledged,
        ).await
    }

    pub(super) async fn retained_publication_context_with_carrier(
        &self, carrier: &mut NativeDemandCarrier<'_>, cancellation: &CancellationToken,
    ) -> Result<Option<crate::lazy_workspace::NativeSnapshotContext>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
    {
        self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
        let checkout = await_measured_lock_with_carrier(
            self.checkout.lock_settled(), carrier, cancellation, Some(&self.cancellation),
        ).await?;
        Ok(checkout.publication_evidence.as_ref().map(|retained| retained.context))
    }

    pub(super) async fn sync_async_with_permit_carrier_guarded_context_with_ack<F, Fut, E>(
        &self, permit: crate::PublicationPermit, force: bool,
        carrier: &mut NativeDemandCarrier<'_>,
        view: &mut super::view_ledger::ViewLedgerAdmission,
        cancellation: &CancellationToken, before_publication: &mut F,
        context: Option<crate::lazy_workspace::NativeSnapshotContext>,
        acknowledged: &mut Option<crate::lazy_workspace::NativePublicationSnapshot>,
    ) -> Result<crate::lazy_workspace::NativePublicationSnapshot, (E, Option<MountSourceError>)>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        if context.is_none() && self.retained_publication_context_with_carrier(carrier, cancellation)
            .await.map_err(|error| (E::from(error), None))?.is_none()
        {
            return Err((E::from(MountSourceError::OriginalSnapshotUnavailable), None));
        }
        let mut accounting_failure = None;
        let result = self.sync_async_with_permit_carrier_guarded_publication(
            permit, force, carrier, view, cancellation, before_publication,
            None, None, context, acknowledged, &mut accounting_failure,
        ).await;
        let (_, snapshot) = result.map_err(|primary| (primary, accounting_failure))?;
        snapshot.ok_or_else(|| (E::from(MountSourceError::OriginalSnapshotUnavailable), None))
    }

    pub(super) async fn sync_async_with_permit_carrier_guarded_evidence<F, Fut, E>(
        &self,
        permit: crate::PublicationPermit,
        force: bool,
        carrier: &mut NativeDemandCarrier<'_>,
        view: &mut super::view_ledger::ViewLedgerAdmission,
        cancellation: &CancellationToken,
        before_publication: &mut F,
        operation_context: Option<crate::Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
    ) -> Result<NativeSourcePublicationAck, E>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        let mut acknowledged = None;
        let mut accounting_failure = None;
        self.sync_async_with_permit_carrier_guarded_publication(
            permit, force, carrier, view, cancellation, before_publication,
            operation_context, operation_evidence, None, &mut acknowledged, &mut accounting_failure,
        ).await.map(|(ack, _)| ack)
    }

    fn sync_async_with_permit_carrier_guarded_publication<'a, 'work, F, Fut, E>(
        &'a self, permit: crate::PublicationPermit, force: bool,
        carrier: &'a mut NativeDemandCarrier<'work>,
        view: &'a mut super::view_ledger::ViewLedgerAdmission,
        cancellation: &'a CancellationToken, before_publication: &'a mut F,
        operation_context: Option<crate::Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
        snapshot_context: Option<crate::lazy_workspace::NativeSnapshotContext>,
        acknowledged: &'a mut Option<crate::lazy_workspace::NativePublicationSnapshot>,
        accounting_failure: &'a mut Option<MountSourceError>,
    ) -> Pin<Box<dyn Future<Output = Result<(NativeSourcePublicationAck,
        Option<crate::lazy_workspace::NativePublicationSnapshot>), E>> + Send + 'a>>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        Box::pin(async move {
            use super::lazy::mount_demand_error;
            carrier.with_work(|work| work.check().map_err(mount_demand_error))?;
            self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
            match carrier {
                NativeDemandCarrier::Borrowed(work) => self.checkout.settle_pending_measured(
                    work, view, cancellation, Some(&self.cancellation),
                ).await?,
                NativeDemandCarrier::Shared(meter) => self.checkout.settle_pending_with_meter(
                    meter, cancellation, Some(&self.cancellation),
                ).await?,
                NativeDemandCarrier::Native(original) => self.checkout.settle_pending_with_native_work(
                    original, cancellation, Some(&self.cancellation),
                ).await?,
            }
            let source_guard = self.publication_guard();
            let caller_guard = before_publication.clone();
            let mut guarded = move |original: &crate::NativeMaterializationWork| {
                let mut source_guard = source_guard.clone();
                let mut caller_guard = caller_guard.clone();
                let original = original.clone();
                async move {
                    source_guard(&original).await.map_err(E::from)?;
                    caller_guard(&original).await
                }
            };
            self.checkout.seal_original_with_carrier_guarded_context(
                permit, force, carrier, cancellation, Some(&self.cancellation), &mut guarded,
                operation_context, operation_evidence, snapshot_context, acknowledged, accounting_failure,
            ).await
        })
    }

    /// Safely advances a clean mounted checkout to the workspace head.
    ///
    /// Callers must publish pending native mutations first. Exact read
    /// dependencies are retained and checked against the candidate head;
    /// conflict leaves the mounted generation unchanged.
    pub async fn refresh_async(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let span = crate::obs::span!(
            DEBUG,
            "acyclic.fs.mount.refresh",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
        );
        let result = crate::obs::in_span(&span, async move {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let base = checkout.private_candidate();
            let decision = checkout
                .rebase_head(
                    self.limits.maximum_checkout_dependencies,
                    WorkBudget::UNBOUNDED,
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            match decision.value {
                RebaseDecision::Safe { .. } => {
                    self.record_advance(&mut checkout, &base).await;
                    Ok(())
                }
                RebaseDecision::Conflicted { .. } => Err(MountSourceError::Stale),
            }
        })
        .await;
        crate::obs::outcome_on(&span, result)
    }

    /// Adopts the workspace head after this checkout's mutations were
    /// synchronized and a separate fenced operation published that head.
    pub async fn advance_to_head_async(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let span = crate::obs::span!(
            DEBUG,
            "acyclic.fs.mount.advance_to_head",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
        );
        let result = crate::obs::in_span(&span, async move {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let base = checkout.private_candidate();
            checkout
                .refresh_head(WorkBudget::UNBOUNDED, &self.cancellation)
                .await
                .map_err(engine_error)?;
            self.record_advance(&mut checkout, &base).await;
            Ok(())
        })
        .await;
        crate::obs::outcome_on(&span, result)
    }

    /// Records exactly what advancing the checkout from `base` changed in
    /// the view, as a binding transition; an advance that changed nothing,
    /// such as one to an unmoved head, records nothing, so drivers keep
    /// every cache and a revalidation barrier has nothing to wait for.
    async fn record_advance(
        &self,
        checkout: &mut SharedCheckoutGuard<'_, A, O>,
        base: &Checkout<A, O>,
    ) where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        if checkout.root().file_table == base.root().file_table {
            return;
        }
        // Names that changed in directories at unknown paths are covered
        // by the whole checkout, whichever root a composing source mounts.
        let installed = match NamespacePath::new(Vec::new(), self.limits) {
            Ok(root) => {
                installed_changes(
                    base,
                    checkout,
                    InstallScope::Subtree(&root),
                    &self.cancellation,
                )
                .await
            }
            Err(_) => ViewEffect {
                everything: true,
                ..ViewEffect::default()
            },
        };
        self.checkout.view_gate.begin_transition();
        checkout.record(&ViewChange::Effect(&installed));
        self.checkout.view_gate.finish_transition();
    }

    fn path(&self, path: &MountPath) -> Result<NamespacePath, MountSourceError> {
        let mut components = self.root.components().to_vec();
        components.extend(
            path.components()
                .iter()
                .map(|component| self.logical_name(component))
                .collect::<Result<Vec<_>, _>>()?,
        );
        NamespacePath::new(components, self.limits).map_err(engine_error)
    }

    /// The native spelling every case of `bytes` resolves to, or `None` for
    /// a name this volume cannot represent (which no lookup resolves).
    fn folded_name(&self, bytes: &[u8]) -> Option<Vec<u8>> {
        let name = self.logical_name(bytes).ok()?;
        let folded = LogicalName::new(
            name.encoding(),
            name.case_fold_key(),
            self.limits.maximum_component_bytes,
        )
        .ok()?;
        native_mount_name(&folded).ok()
    }

    fn folded_name_with_work(
        &self,
        component: &[u8],
        work: &mut crate::demand::DemandWork,
    ) -> Result<Vec<u8>, MountSourceError> {
        use super::lazy::{mount_demand_error, mount_items};

        fn copy(
            component: &[u8],
            work: &mut crate::demand::DemandWork,
        ) -> Result<Vec<u8>, MountSourceError> {
            let mut scratch = CaptureScratchAllocation::new(work);
            let delta = crate::WorkCounters {
                bytes_copied: component.len() as u64, ..crate::WorkCounters::UNCHARGED
            };
            scratch.work.work.admit(&delta, &scratch.work.budget).map_err(MountSourceError::Work)?;
            let mut bytes = if component.is_empty() { Vec::new() } else { scratch.vec(component.len())? };
            bytes.extend_from_slice(component);
            scratch.work.record(delta).map_err(mount_demand_error)?;
            scratch.retain();
            Ok(bytes)
        }

        // Raw-name case keys and every unrepresentable-name fallback are
        // byte-identical to the original native component.
        if !matches!(self.profile, FilesystemProfile::Portable | FilesystemProfile::Browser)
            || !matches!(std::env::consts::OS, "linux" | "macos" | "windows")
        {
            return copy(component, work);
        }
        let mut scratch = CaptureScratchAllocation::new(work);
        let mut native_unicode = None;
        if cfg!(windows) {
            let Some(units) = crate::kernel::types::utf16le_units(component) else {
                return copy(component, scratch.work);
            };
            let mut length = 0_usize;
            for scalar in char::decode_utf16(units) {
                mount_items(scratch.work, 1)?;
                let Ok(scalar) = scalar else { return copy(component, scratch.work); };
                if matches!(scalar, '/' | '\0') {
                    return copy(component, scratch.work);
                }
                length = length.checked_add(scalar.len_utf8())
                    .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            }
            if length == 0 || length > self.limits.maximum_component_bytes as usize
                || component == [b'.', 0] || component == [b'.', 0, b'.', 0]
            {
                return copy(component, scratch.work);
            }
            let delta = crate::WorkCounters {
                bytes_encoded: length as u64, ..crate::WorkCounters::UNCHARGED
            };
            scratch.work.work.admit(&delta, &scratch.work.budget).map_err(MountSourceError::Work)?;
            let mut bytes = if length == 0 { Vec::new() } else { scratch.vec(length)? };
            for scalar in char::decode_utf16(
                crate::kernel::types::utf16le_units(component).expect("native UTF-16 was validated"),
            ) {
                mount_items(scratch.work, 1)?;
                let mut encoded = [0_u8; 4];
                bytes.extend_from_slice(scalar.expect("native Unicode was validated").encode_utf8(&mut encoded).as_bytes());
            }
            scratch.work.record(delta).map_err(mount_demand_error)?;
            native_unicode = Some(String::from_utf8(bytes).expect("native Unicode encodes exact UTF-8"));
        }
        let text = if let Some(value) = native_unicode.as_deref() {
            value
        } else {
            let Ok(value) = std::str::from_utf8(component) else {
                return copy(component, scratch.work);
            };
            value
        };
        mount_items(scratch.work, text.len())?;
        if LogicalName::validate_unicode(NameEncoding::Utf8, text, self.limits.maximum_component_bytes).is_err() {
            if let Some(value) = native_unicode.take() {
                let bytes = value.capacity() as u64;
                drop(value);
                scratch.release(bytes).map_err(mount_demand_error)?;
            }
            return copy(component, scratch.work);
        }
        let folded = crate::demand::lowercase_unicode_with_work(text, scratch.work)
            .map_err(mount_demand_error)?;
        let folded_bytes = folded.capacity() as u64;
        scratch.track(folded_bytes);
        if let Some(value) = native_unicode.take() {
            let bytes = value.capacity() as u64;
            drop(value);
            scratch.release(bytes).map_err(mount_demand_error)?;
        }
        let text = std::str::from_utf8(&folded).expect("canonical lowercase emits exact UTF-8");
        mount_items(scratch.work, text.len())?;
        if LogicalName::validate_unicode(NameEncoding::Utf8, text, self.limits.maximum_component_bytes).is_err() {
            drop(folded);
            scratch.release(folded_bytes).map_err(mount_demand_error)?;
            return copy(component, scratch.work);
        }
        if !cfg!(windows) {
            scratch.retain();
            return Ok(folded);
        }
        let mut length = 0_usize;
        for scalar in text.chars() {
            mount_items(scratch.work, 1)?;
            length = length.checked_add(scalar.len_utf16() * 2)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        }
        let delta = crate::WorkCounters {
            bytes_encoded: length as u64, ..crate::WorkCounters::UNCHARGED
        };
        scratch.work.work.admit(&delta, &scratch.work.budget).map_err(MountSourceError::Work)?;
        let mut bytes = scratch.vec(length)?;
        for unit in text.encode_utf16() {
            mount_items(scratch.work, 1)?;
            bytes.extend_from_slice(&unit.to_le_bytes());
        }
        scratch.work.record(delta).map_err(mount_demand_error)?;
        drop(folded);
        scratch.release(folded_bytes).map_err(mount_demand_error)?;
        scratch.retain();
        Ok(bytes)
    }

    fn logical_name(&self, bytes: &[u8]) -> Result<LogicalName, MountSourceError> {
        let (encoding, logical_bytes) = match (self.profile, std::env::consts::OS) {
            (FilesystemProfile::Posix, "linux" | "macos") => {
                (NameEncoding::PosixBytes, bytes.to_vec())
            }
            (FilesystemProfile::Portable | FilesystemProfile::Browser, "linux" | "macos") => {
                std::str::from_utf8(bytes).map_err(|_| {
                    MountSourceError::Unsupported(
                        "portable volume cannot represent a non-UTF-8 native name".to_owned(),
                    )
                })?;
                (NameEncoding::Utf8, bytes.to_vec())
            }
            (FilesystemProfile::Windows, "windows") => {
                (NameEncoding::WindowsUtf16Le, bytes.to_vec())
            }
            (FilesystemProfile::Portable | FilesystemProfile::Browser, "windows") => {
                let units = crate::kernel::types::utf16le_units(bytes)
                    .ok_or_else(|| {
                        MountSourceError::Invalid(
                            "native UTF-16 name has an odd byte length".to_owned(),
                        )
                    })?
                    .collect::<Vec<_>>();
                let value = String::from_utf16(&units).map_err(|_| {
                    MountSourceError::Unsupported(
                        "portable volume cannot represent an unpaired UTF-16 native name"
                            .to_owned(),
                    )
                })?;
                (NameEncoding::Utf8, value.into_bytes())
            }
            _ => {
                return Err(MountSourceError::Unsupported(
                    "volume name profile is incompatible with this native mount".to_owned(),
                ));
            }
        };
        LogicalName::new(encoding, logical_bytes, self.limits.maximum_component_bytes)
            .map_err(engine_error)
    }

    fn posix_attribute_name_with_work(
        &self,
        bytes: &[u8],
        work: &mut crate::demand::DemandWork,
    ) -> Result<AttributeName, MountSourceError> {
        native_attribute_name_with_work(self.profile, self.limits, bytes, work)
    }

    /// Converts the same exact native names used by capture, admitting each
    /// real root/name/vector capacity before its owned allocation is created.
    pub(super) fn namespace_path_with_work(
        &self,
        path: &MountPath,
        work: &mut crate::demand::DemandWork,
    ) -> Result<NamespacePath, MountSourceError> {
        let mut scratch = CaptureScratchAllocation::new(work);
        let result = self.namespace_path_with_scratch(path, &mut scratch);
        if result.is_ok() { scratch.retain(); }
        result
    }

    /// Bind the returned native guard before the physical namespace owner so
    /// the path drops first; native paths must not be released with raw work.
    pub(super) fn namespace_path_with_carrier(
        &self,
        path: &MountPath,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(NamespacePath, Option<crate::NativeAllocation>), MountSourceError> {
        if let Some(work) = carrier.original_native_work() {
            let mut scratch = NativeCaptureScratchAllocation::new(&work)?;
            let path = self.namespace_path_with_scratch(path, &mut scratch)?;
            Ok((path, Some(scratch.retain())))
        } else {
            carrier.with_work(|work| self.namespace_path_with_work(path, work).map(|path| (path, None)))
        }
    }

    fn namespace_path_with_scratch<S: CaptureInputScratch>(
        &self,
        path: &MountPath,
        scratch: &mut S,
    ) -> Result<NamespacePath, MountSourceError> {
        scratch.items(1)?;
        let mut layouts = scratch.vec::<(NameEncoding, usize)>(path.components().len())?;
        for component in path.components() {
            layouts.push(capture_name_layout(component, self.profile, self.limits, scratch)?);
        }
        let count = self.root.depth().checked_add(layouts.len())
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        scratch.items(count)?;
        NamespacePath::validate_component_lengths(
            self.root.components().iter().map(|name| name.as_bytes().len())
                .chain(layouts.iter().map(|(_, length)| *length)),
            self.limits,
        ).map_err(|error| scratch.engine_error(error))?;
        let mut names = scratch.vec::<LogicalName>(count)?;
        for name in self.root.components() {
            names.push(capture_copy_name(name.encoding(), name.as_bytes(), self.limits, scratch)?);
        }
        for (component, (encoding, length)) in path.components().iter().zip(&layouts) {
            names.push(capture_native_name(
                component, *encoding, *length, self.profile, self.limits, scratch,
            )?);
        }
        scratch.items(names.len())?;
        let rooted = NamespacePath::new(names, self.limits).map_err(|error| scratch.engine_error(error))?;
        let bytes = layouts.capacity() as u64 * std::mem::size_of::<(NameEncoding, usize)>() as u64;
        drop(layouts);
        scratch.release(bytes)?;
        Ok(rooted)
    }

    fn capture_inputs(
        &self,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: NativeRootIdentity,
        scope: HostCaptureScope,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<CaptureInputs, MountSourceError> {
        let mut work = meter.lock();
        let mut scratch = CaptureScratchAllocation::new(&mut work);
        let (paths, options) = self.capture_inputs_with_scratch(
            source_root, paths, expected_root_identity, scope, &mut scratch,
        )?;
        let bytes = scratch.retain();
        Ok(CaptureInputs {
            paths, options,
            _allocation: CaptureInputsAllocation::Ordinary { _allocation: CaptureOwnedAllocation { meter: Arc::clone(meter), bytes } },
        })
    }

    fn capture_inputs_with_native_work(
        &self,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: NativeRootIdentity,
        scope: HostCaptureScope,
        work: &crate::NativeMaterializationWork,
    ) -> Result<CaptureInputs, MountSourceError> {
        let mut scratch = NativeCaptureScratchAllocation::new(work)?;
        let (paths, options) = self.capture_inputs_with_scratch(
            source_root, paths, expected_root_identity, scope, &mut scratch,
        )?;
        Ok(CaptureInputs {
            paths, options, _allocation: CaptureInputsAllocation::Native { _allocation: scratch.retain() },
        })
    }

    fn capture_inputs_with_scratch<S: CaptureInputScratch>(
        &self,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: NativeRootIdentity,
        scope: HostCaptureScope,
        scratch: &mut S,
    ) -> Result<(Vec<NamespacePath>, CaptureOptions), MountSourceError> {
        if matches!(scope, HostCaptureScope::ExactPaths) && paths.len() > MAX_NATIVE_EXACT_CAPTURE_PATHS {
            return Err(MountSourceError::Invalid("capture path set exceeds the native per-operation limit".to_owned()));
        }
        let mut rooted = scratch.vec::<NamespacePath>(paths.len())?;
        for path in paths {
            rooted.push(self.namespace_path_with_scratch(path, scratch)?);
        }
        let source_root = scratch.clone_path(source_root)?;
        let maximum_paths = match scope {
            HostCaptureScope::ExactPaths => u32::try_from(rooted.len())
                .map_err(|_| MountSourceError::Work(crate::WorkError::Overflow))?,
            HostCaptureScope::Subtree => MAX_NATIVE_SUBTREE_CAPTURE_PATHS,
        };
        Ok((rooted, CaptureOptions {
            source_root, expected_root_identity, maximum_paths, maximum_extent_spans: 65_536,
        }))
    }

    fn register_capture(
        self: &Arc<Self>,
        request: Arc<CaptureCommitGate>,
        native: &super::NativeMountMeterWork,
        retirement: CaptureRegistrationRetirement,
        wait: Arc<StdMutex<CaptureCallerWait>>,
    ) -> Result<CaptureRegistration<A, O>, MountSourceError> {
        let original = native.native_materialization_work();
        let cancel_visit = original.reserve_items(1).map_err(MountSourceError::NativeWork)?;
        let mut visit = original.reserve_items(1).map_err(MountSourceError::NativeWork)?;
        let mut poll_credit = self.reserve_capture_poll()?;
        let (old, copied_result) = {
            let mut gate = self.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
            visit.consume(1).map_err(MountSourceError::NativeWork)?;
            if gate.cancelled { return Err(MountSourceError::Stale); }
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            let producers = gate.producers.checked_add(1)
                .ok_or(MountSourceError::Work(WorkError::Overflow))?;
            let old = if gate.active.len() == gate.active.capacity() {
                let length = gate.active.len().checked_add(1)
                    .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
                let copied = gate.active.len().checked_mul(std::mem::size_of::<CaptureSourceSlot>())
                    .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
                let copied_credit = original.reserve_copy(copied).map_err(MountSourceError::NativeWork)?;
                let mut scratch = NativeCaptureScratchAllocation::new(&original)?;
                let mut replacement = scratch.vec::<CaptureSourceSlot>(length)?;
                let allocation = scratch.retain();
                replacement.append(&mut gate.active);
                let vector = std::mem::replace(&mut gate.active, replacement);
                let legacy = gate.allocation.take();
                let previous = gate.native_allocation.replace(allocation);
                (Some((vector, legacy, previous)),
                    copied_credit.record(copied).map_err(MountSourceError::NativeWork))
            } else { (None, Ok(())) };
            self.accept_capture_poll(&mut poll_credit)?;
            gate.producers = producers;
            gate.producers_pending = true;
            gate.ever_used = true;
            request.slot.store(gate.active.len(), Ordering::Release);
            gate.active.push(CaptureSourceSlot {
                request: Arc::downgrade(&request), cancel_visit: Some(cancel_visit),
            });
            old
        };
        if let Some((vector, legacy, native)) = old { retire_capture_vector(vector, legacy, native); }
        let registration = CaptureRegistration {
            source: Arc::clone(self), request, wait, retirement: Some(retirement),
        };
        copied_result?;
        Ok(registration)
    }

    pub(crate) fn capture_host_paths_with_identity(
        &self,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: NativeRootIdentity,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        self.capture_host_with_identity(
            source_root,
            paths,
            expected_root_identity,
            HostCaptureScope::ExactPaths,
            None,
        )
    }

    fn capture_host_with_identity(
        &self,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: NativeRootIdentity,
        scope: HostCaptureScope,
        baseline: Option<&NativeViewBaseline>,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        if paths.is_empty() { return Ok(()); }
        let native = capture_meter(boundary_budget(), &self.cancellation)
            .map_err(|failure| failure.error)?;
        let meter = native.meter();
        let inputs = self.capture_inputs(source_root, paths, expected_root_identity, scope, &meter)?;
        let original = native.native_materialization_work();
        self.runtime.block_on_measured(
            || tokio::time::timeout(CALLBACK_TIMEOUT, self.capture_host_with_identity_async(
                &inputs.paths, &inputs.options, scope,
                HostCapturePolicy { baseline, request: None, meter: &meter, native: &native },
            )),
            original.runtime_meter(),
        ).map_err(|error| MountSourceError::NativeWork(original.runtime_error(error)))?
            .map_err(|_| MountSourceError::Stale)?.map(|_| ())
    }

    /// Reuses an already owned runtime carrier without moving out its operative
    /// work or replacing the actual native provider's measured TLS context.
    fn capture_host_with_identity_on_meter(
        &self,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: NativeRootIdentity,
        scope: HostCaptureScope,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
        native: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let original = native.native_materialization_work();
        self.runtime.block_on_measured(
            || self.capture_host_with_identity_async_on_meter(
                source_root, paths, expected_root_identity, scope, None, meter, native,
            ),
            original.runtime_meter(),
        ).map_err(|error| MountSourceError::NativeWork(original.runtime_error(error)))?
    }

    /// Captures through the original invocation, retaining a supplied baseline
    /// for the complete canonical capture rather than probing a new identity.
    pub(super) async fn capture_host_with_identity_with_native_work(
        &self,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: NativeRootIdentity,
        scope: HostCaptureScope,
        baseline: Option<Arc<NativeViewBaseline>>,
        native: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore + Send + Sync + 'static,
          O: AsyncObjectStore + Send + Sync + 'static {
        self.capture_host_with_identity_async_on_meter(
            source_root, paths, expected_root_identity, scope, baseline, native.meter(), native,
        ).await
    }

    async fn capture_host_with_identity_async_on_meter(
        &self,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: NativeRootIdentity,
        scope: HostCaptureScope,
        baseline: Option<Arc<NativeViewBaseline>>,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
        native: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError>
    where A: AsyncAuthorityStore + Send + Sync + 'static,
          O: AsyncObjectStore + Send + Sync + 'static {
        meter.lock().check().map_err(super::lazy::mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        if paths.is_empty() { return Ok(()); }
        let inputs = self.capture_inputs_with_native_work(
            source_root, paths, expected_root_identity, scope, &native.native_materialization_work(),
        )?;
        self.capture_host_with_identity_async(
            &inputs.paths, &inputs.options, scope,
            HostCapturePolicy { baseline: baseline.as_deref(), request: None, meter, native },
        ).await.map(|_| ())
    }

    /// The SDK lifecycle path retains one admitted meter through native worker
    /// completion, including cancellation and all pre-worker failures.
    pub(super) async fn capture_host_with_identity_budgeted(
        self: &Arc<Self>,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: NativeRootIdentity,
        scope: HostCaptureScope,
        baseline: Arc<NativeViewBaseline>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<u64>, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        cancellation.check().map_err(|_| OperationFailure::before_work(MountSourceError::Cancelled))?;
        if paths.is_empty() {
            return Ok(crate::OperationReceipt { value: 0, work: crate::WorkCounters::UNCHARGED });
        }
        let native = capture_meter(budget, cancellation)?;
        let meter = native.meter();
        let result = async {
            let inputs = self.capture_inputs(source_root, paths, expected_root_identity, scope, &meter)?;
            let request = capture_request(&meter)?;
            {
                let mut work = meter.lock();
                let previous = std::mem::replace(
                    &mut *work, crate::demand::DemandWork::new(budget, &request.cancellation),
                );
                let (counters, allocations, cancellation_roots) = previous.into_current();
                *work = crate::demand::DemandWork::from_current(
                    budget, &request.cancellation, counters, allocations, cancellation_roots,
                );
            }
            let cancel_on_drop = CancelCaptureOnDrop(Arc::clone(&request));
            let (wait, retirement) = {
                let original = native.native_materialization_work();
                CaptureCallerWait::prepare_capture(&original, cancellation)?
            };
            let mut resources = CaptureWorkerResources {
                inputs: Some(inputs), registration: None, retirement: Some(retirement), wait: Arc::clone(&wait),
            };
            let source = Arc::clone(self);
            let worker_native = native.clone();
            let worker_request = Arc::clone(&request);
            let runtime_meter = native.native_materialization_work().runtime_meter_owner();
            let mut worker = acyclic_native_runtime::run_blocking_io_measured(
                crate::obs::in_context(crate::obs::caller_context(), move || {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let worker_meter = worker_native.meter();
                        resources.registration = Some(source.register_capture(
                            Arc::clone(&worker_request), &worker_native,
                            resources.retirement.take().expect("capture worker owns its native retirement credits"),
                            Arc::clone(&resources.wait),
                        )?);
                        let inputs = resources.inputs.as_ref().expect("capture inputs remain owned");
                        let request = &resources.registration.as_ref().expect("capture registration remains owned").request;
                        let original = worker_native.native_materialization_work();
                        source.runtime.block_on_measured(
                            || source.capture_host_with_identity_async(
                                &inputs.paths, &inputs.options, scope,
                                HostCapturePolicy { baseline: Some(&baseline), request: Some(request), meter: &worker_meter, native: &worker_native },
                            ),
                            original.runtime_meter(),
                        ).map_err(|error| MountSourceError::NativeWork(original.runtime_error(error)))
                            .and_then(std::convert::identity)
                    }));
                    drop(resources);
                    result
                }),
                runtime_meter,
            ).map_err(|error| MountSourceError::NativeWork(native.native_materialization_work().runtime_error(error)))?;
            let cancelled = std::future::poll_fn(|context| {
                let mut wait = wait.lock().unwrap_or_else(PoisonError::into_inner);
                wait.prepared.as_mut().map_or(Poll::Pending, |prepared| Pin::new(prepared).poll(context))
            });
            let result = tokio::select! {
                result = &mut worker => result,
                () = cancelled => {
                    drop(CancelCaptureOnDrop(Arc::clone(&request)));
                    (&mut worker).await
                }
            }.map_err(|error| MountSourceError::NativeWork(native.native_materialization_work().runtime_error(error)));
            drop(worker);
            drop(cancel_on_drop);
            drop(request);
            let cleanup_error = {
                let mut wait = wait.lock().unwrap_or_else(PoisonError::into_inner);
                wait.finish();
                wait.error.take()
            };
            drop(wait);
            match result {
                Ok(Ok(result)) => result.and_then(|value| cleanup_error.map_or(Ok(value), Err)),
                Ok(Err(payload)) => std::panic::resume_unwind(payload),
                Err(error) => Err(error),
            }
        }.await;
        let work = meter.lock().work;
        result.map(|value| crate::OperationReceipt { value, work })
            .map_err(|error| OperationFailure::new(error, work))
    }

    async fn capture_host_with_identity_async(
        &self,
        paths: &[NamespacePath],
        options: &CaptureOptions,
        scope: HostCaptureScope,
        policy: HostCapturePolicy<'_>,
    ) -> Result<u64, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let caller_cancellation = policy.meter.lock().cancellation().clone();
        let cancellation = policy.request.map_or(&caller_cancellation, |request| &request.cancellation);
        cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        let mut poll_credit = self.reserve_capture_poll()?;
        let _producer = {
            let mut gate = self.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
            if gate.cancelled { return Err(MountSourceError::Stale); }
            self.accept_capture_poll(&mut poll_credit)?;
            gate.producers = gate.producers.checked_add(1)
                .ok_or(MountSourceError::Work(WorkError::Overflow))?;
            gate.producers_pending = true;
            gate.ever_used = true;
            CaptureProducerLease { gate: &self.capture_gate, quiesced: &self.capture_quiesced }
        };
        let mut view = super::view_ledger::ViewLedgerAdmission::default();
        for _ in 0..3 {
            let mut carrier = NativeDemandCarrier::Native(policy.native);
            let mut candidate = {
                let checkout = self.checkout.lock_with_carrier(
                    &mut carrier, cancellation, Some(&self.cancellation),
                ).await?;
                checkout.candidate_for_operation_with_carrier(None, &mut carrier, cancellation)?
            };
            // Projected reads may call back into this checkout. Its measured
            // view lock is released before observing any virtualization root.
            let captured = {
                let work = policy.native.native_materialization_work();
                match scope {
                    HostCaptureScope::ExactPaths => capture_paths_batched_with_native_work(
                        &mut candidate.checkout, paths, options, 64,
                        cancellation, policy.baseline, &work,
                    ).await,
                    HostCaptureScope::Subtree => capture_subtrees_with_native_work(
                        &mut candidate.checkout,
                        std::slice::from_ref(paths.first().ok_or(MountSourceError::Stale)?),
                        options, &crate::CapturePolicy::allow_all(),
                        cancellation, policy.baseline, &work,
                    ).await,
                }.map_err(|failure| capture_mount_error(failure.error))
            };
            if let Err(error) = captured {
                drop(candidate);
                return Err(error);
            }
            let install_scope = match scope {
                HostCaptureScope::ExactPaths => InstallScope::Paths(paths),
                HostCaptureScope::Subtree => InstallScope::Subtree(paths.first().ok_or(MountSourceError::Stale)?),
            };
            let installed = self.commit_host_capture(
                candidate, install_scope, policy.request, policy.meter, &mut carrier, &mut view, cancellation, &mut 0,
            ).await;
            if let Some(retained_data) = installed? { return Ok(retained_data); }
        }
        Err(MountSourceError::Stale)
    }

    async fn commit_host_capture(
        &self,
        candidate: CheckoutCandidate<A, O>,
        scope: InstallScope<'_>,
        request: Option<&CaptureCommitGate>,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
        carrier: &mut NativeDemandCarrier<'_>,
        view: &mut super::view_ledger::ViewLedgerAdmission,
        cancellation: &CancellationToken,
        candidate_bytes: &mut u64,
    ) -> Result<Option<u64>, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        use super::lazy::{mount_accept, mount_demand_error};
        let mut installed_allocation = CaptureOwnedAllocation { meter: Arc::clone(meter), bytes: 0 };
        let native_work = carrier.original_native_work();
        let installed = if let Some(work) = native_work.as_ref() {
            installed_changes_with_native_work(
                candidate.base(), &candidate.checkout, scope, work, cancellation,
            ).await?
        } else {
            let budget = meter.lock().remaining().map_err(mount_demand_error)?;
            let installed = candidate.installed_changes_measured(scope, budget, cancellation).await;
            let mut work = meter.lock();
            let installed = mount_accept(&mut work, installed, std::convert::identity)?;
            let bytes = capture_effect_bytes(&installed, &mut work)?;
            work.adopt(bytes).map_err(mount_demand_error)?;
            installed_allocation.bytes = bytes;
            InstalledCaptureEffect { value: installed, _allocation: None }
        };
        let (epoch, stamp) = {
            let checkout = self.checkout.lock_with_carrier(
                carrier, cancellation, Some(&self.cancellation),
            ).await?;
            checkout.ensure_publication_resolved()?;
            if request.is_some() && checkout.publishes_at_native_boundary() {
                return measured_native_name_error(
                    "lifecycle capture requires manual publication", true, &mut meter.lock(),
                );
            }
            (self.checkout.view_gate.generation(), self.checkout.ledger.stamp())
        };
        let base_bytes = if native_work.is_none() { candidate.base().logical_owned_bytes() } else { 0 };
        let publication = self.checkout.finish_native_phase_with_commit(
            candidate, epoch, stamp, carrier, cancellation, Some(&self.cancellation),
            |carrier| self.with_capture_observer_dispatch(|| {
                self.checkout.ledger.record(
                    &ViewChange::Effect(&installed.value), carrier, view,
                )
            }),
            |checkout, candidate, prepared| {
                let source_gate = self.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
                if source_gate.cancelled {
                    return Err(MountSourceError::Stale);
                }
                let commit = || {
                    if let Some(request) = request {
                        request.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
                        request.state.compare_exchange(
                            CAPTURE_PENDING, CAPTURE_COMMITTED, Ordering::AcqRel, Ordering::Acquire,
                        ).map_err(|_| MountSourceError::Stale)?;
                    }
                    let adopted = checkout.adopt_prepared_candidate(
                        &candidate.base, candidate.checkout, prepared,
                    );
                    if !adopted && request.is_some() {
                        return Err(MountSourceError::Stale);
                    }
                    Ok(adopted)
                };
                cancellation.commit_if_both_live(&self.cancellation, commit)
                    .map_err(|_| MountSourceError::Cancelled)
                    .and_then(std::convert::identity)
            },
        ).await?;
        let Some(publication) = publication else { return Ok(None); };
        // The physical swap won before cancellation. Only its optimistic
        // baseline is dropped; the installed checkout remains source-owned.
        if native_work.is_none() { *candidate_bytes = base_bytes; }
        if publication != MountPublication::Manual {
            let mut before = self.publication_guard();
            self.checkout.seal_original_with_carrier_guarded(
                crate::PublicationPermit::Unrestricted, false, carrier,
                cancellation, Some(&self.cancellation), &mut before,
            ).await?;
        }
        let checkout = self.checkout.read_current_state().await;
        let data = if let Some(work) = native_work.as_ref() {
            checkout.proof_data_snapshot_with_native_work(work, cancellation)
                .map_err(MountSourceError::Filesystem)?
        } else {
            let mut work = meter.lock();
            let data = checkout.proof_data_snapshot_measured(
                work.remaining().map_err(mount_demand_error)?, cancellation,
            );
            mount_accept(&mut work, data, |error| engine_error(error))?
        };
        Ok(Some(data.total_bytes().map_err(MountSourceError::Work)?))
    }
}

fn profile_is_native(profile: FilesystemProfile) -> bool {
    match std::env::consts::OS {
        "linux" | "macos" => !matches!(profile, FilesystemProfile::Windows),
        "windows" => !matches!(profile, FilesystemProfile::Posix),
        _ => false,
    }
}

fn profile_name(profile: FilesystemProfile) -> &'static str {
    match profile {
        FilesystemProfile::Portable => "portable",
        FilesystemProfile::Posix => "posix",
        FilesystemProfile::Windows => "windows",
        FilesystemProfile::Browser => "browser",
    }
}

impl<A, O> CheckoutMountSource<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Applies one content change at `path` with its time stamp as one
    /// mutation.
    fn change_content(
        &self,
        path: NamespacePath,
        change: ContentChange<NamespacePath>,
    ) -> Result<(), MountSourceError> {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| {
            let mut carrier = NativeDemandCarrier::Shared(meter);
            self.runtime.block_on_measured(|| async {
                self.change_content_async_with_carrier(path, change, &mut carrier).await
            }, meter.as_ref()).map_err(capture_io_error)?
        })
    }

    async fn change_content_async_with_carrier(
        &self,
        path: NamespacePath,
        change: ContentChange<NamespacePath>,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        use super::lazy::{mount_demand_error, mount_engine_error};
        let cancellation = &self.cancellation;
        let (mut candidate, epoch, stamp) = self.checkout.prepare_native_phase_with_carrier(
            carrier, cancellation, Some(cancellation),
        ).await?;
        let node = native_node_if_bound_with_carrier(
            &candidate.checkout, &path, carrier, cancellation,
        ).await?.ok_or(MountSourceError::NotFound)?;
        let times = ContentTimes::Stamp(content_change_time()?);
        let (budget, prior_live) = carrier.with_work(|work| {
            Ok::<_, MountSourceError>((
                work.remaining().map_err(mount_demand_error)?, work.live_allocation_bytes(),
            ))
        })?;
        // These inputs remain caller-owned; the original path-based SDK
        // engine runs only against the detached candidate.
        let result = candidate.checkout.change_content(
            path, change, times, budget, cancellation,
        ).await;
        let accepted = carrier.accept_with_prior_live(result, prior_live, mount_engine_error);
        let installed = self.checkout.finish_native_phase_with_carrier(
            candidate, epoch, stamp, &ViewChange::Node(node), carrier,
            cancellation, Some(cancellation),
        ).await;
        accepted?;
        installed?;
        self.checkout.publish_mutation_with_carrier(
            carrier, cancellation, Some(cancellation),
            self.native_callback_guard.as_ref(),
        ).await
    }

    fn write_attribute_on_meter(
        &self,
        path: &MountPath,
        name: &[u8],
        value: crate::kernel::OwnedSourceChunk,
        mode: MountAttributeWriteMode,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<(), MountSourceError> {
        let mut carrier = NativeDemandCarrier::Shared(meter);
        let bytes = if value.is_intrinsic() { 0 } else { value.logical_owned_bytes };
        let mut value = NativeCarrierOwned { value: Some(value), carrier: &mut carrier, bytes };
        self.runtime.block_on_measured(|| async {
            let (packet, carrier) = value.parts();
            self.mutate_attribute_async_with_carrier(
                path, name, Some((packet, mode)), carrier,
            ).await
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn remove_attribute_on_meter(
        &self,
        path: &MountPath,
        name: &[u8],
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<(), MountSourceError> {
        self.runtime.block_on_measured(|| async {
            self.mutate_attribute_async_with_carrier(
                path, name, None, &mut NativeDemandCarrier::Shared(meter),
            ).await
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    async fn mutate_attribute_async_with_carrier(
        &self,
        path: &MountPath,
        name: &[u8],
        value: Option<(&crate::kernel::OwnedSourceChunk, MountAttributeWriteMode)>,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        use super::lazy::{mount_demand_error, mount_engine_error};
        let original = carrier.original_native_work();
        let cancellation = carrier.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
            Ok::<_, MountSourceError>(work.cancellation().clone())
        })?;
        let namespace_allocation;
        let namespace;
        (namespace, namespace_allocation) = self.namespace_path_with_carrier(path, carrier)?;
        let bytes = if namespace_allocation.is_some() { 0 } else { namespace.retained_bytes() };
        let mut namespace = NativeCarrierOwned { value: Some(namespace), carrier, bytes };
        let name_allocation;
        let name = if let Some(original) = original.as_ref() {
            let (name, allocation) = native_attribute_name_with_native_work(
                self.profile, self.limits, name, original,
            )?;
            name_allocation = Some(allocation);
            name
        } else {
            name_allocation = None;
            namespace.carrier.with_work(|work| self.posix_attribute_name_with_work(name, work))?
        };
        let bytes = if name_allocation.is_some() { 0 } else { name.retained_bytes() as u64 };
        let mut name = NativeCarrierOwned { value: Some(name), carrier: &mut *namespace.carrier, bytes };
        let (mut candidate, epoch, stamp) = self.checkout.prepare_native_phase_with_carrier(
            name.carrier, &cancellation, Some(&self.cancellation),
        ).await?;
        let node = native_node_if_bound_with_carrier(
            &candidate.checkout, namespace.value.as_ref().expect("native attribute path is live"),
            name.carrier, &cancellation,
        ).await?.ok_or(MountSourceError::NotFound)?;
        let map_error = |error| match error {
            FsError::CreationRejected | FsError::NotFound | FsError::Mutation(
                GenerationMutationError::AlreadyExists |
                GenerationMutationError::MissingParent |
                GenerationMutationError::MissingSource,
            ) => fs_error(error),
            other => mount_engine_error(other),
        };
        let mode = |mode| match mode {
            MountAttributeWriteMode::Upsert => NamedAttributeWriteMode::Upsert,
            MountAttributeWriteMode::Create => NamedAttributeWriteMode::Create,
            MountAttributeWriteMode::Replace => NamedAttributeWriteMode::Replace,
        };
        let accepted = if let Some(original) = original.as_ref() {
            let attribute = name.value.as_ref().expect("native attribute name is live");
            match value {
                Some((packet, write_mode)) => candidate.checkout.write_named_attribute_by_id_with_native_work(
                    node, attribute, &packet.bytes, mode(write_mode), original, &cancellation,
                ).await,
                None => candidate.checkout.remove_named_attribute_by_id_with_native_work(
                    node, attribute, original, &cancellation,
                ).await,
            }.map_err(MountSourceError::Filesystem)
        } else {
            let (budget, prior_live) = name.carrier.with_work(|work| {
                Ok::<_, MountSourceError>((
                    work.remaining().map_err(mount_demand_error)?, work.live_allocation_bytes(),
                ))
            })?;
            let attribute = name.value.take().expect("native attribute name is live");
            let receipt = match value {
                Some((packet, write_mode)) => candidate.checkout.write_named_attribute_by_id(
                    node, attribute, packet.bytes.clone(), mode(write_mode), budget, &cancellation,
                ).await,
                None => candidate.checkout.remove_named_attribute_by_id(
                    node, attribute, budget, &cancellation,
                ).await,
            };
            name.carrier.accept_with_prior_live(receipt, prior_live, map_error)
        };
        drop(name);
        drop(name_allocation);
        drop(namespace);
        drop(namespace_allocation);
        let installed = self.checkout.finish_native_phase_with_carrier(
            candidate, epoch, stamp, &ViewChange::Node(node),
            carrier, &cancellation, Some(&self.cancellation),
        ).await;
        accepted?;
        installed?;
        self.checkout.publish_mutation_with_carrier(
            carrier, &cancellation, Some(&self.cancellation),
            self.native_callback_guard.as_ref(),
        ).await
    }

    fn clone_range_by_id_on_meter(
        &self,
        source_file_id: FileId,
        source_offset: u64,
        destination_file_id: FileId,
        destination_offset: u64,
        length: u64,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<(), MountSourceError> {
        self.runtime.block_on_measured(|| async {
            self.clone_range_by_id_async_with_carrier(
                source_file_id, source_offset, destination_file_id, destination_offset, length,
                &mut NativeDemandCarrier::Shared(meter),
            ).await
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    async fn clone_range_by_id_async_with_carrier(
        &self,
        source_file_id: FileId,
        source_offset: u64,
        destination_file_id: FileId,
        destination_offset: u64,
        length: u64,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        use super::lazy::mount_demand_error;
        let cancellation = carrier.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            Ok::<_, MountSourceError>(work.cancellation().clone())
        })?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        self.checkout.change_content_by_id_with_carrier(
            destination_file_id,
            ContentChange::CloneFrom {
                source: source_file_id, source_offset, offset: destination_offset, length,
            },
            carrier, &cancellation, Some(&self.cancellation), None,
        ).await
    }

    fn create_symbolic_link_on_meter(
        &self,
        path: &MountPath,
        target: crate::kernel::OwnedSourceChunk,
        metadata: FileMetadata,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<MountLookup, MountSourceError> {
        self.runtime.block_on_measured(|| async {
            self.create_symbolic_link_async_with_carrier(
                path, target, metadata, &mut NativeDemandCarrier::Shared(meter),
            ).await
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    async fn create_symbolic_link_async_with_carrier(
        &self,
        path: &MountPath,
        target: crate::kernel::OwnedSourceChunk,
        metadata: FileMetadata,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<MountLookup, MountSourceError> {
        let bytes = if target.is_intrinsic() { 0 } else { target.logical_owned_bytes };
        let mut target = NativeCarrierOwned { value: Some(target), carrier, bytes };
        let packet = target.value.take().expect("symbolic-link packet is live");
        let logical_bytes = u64::try_from(packet.bytes.len()).map_err(|_| {
            MountSourceError::Unsupported("symbolic-link target is too large".to_owned())
        })?;
        self.create_authored_async_with_carrier(
            path, metadata, MountNodeKind::SymbolicLink, logical_bytes, None, target.carrier,
            |path| AuthoredMutation::CreateSymbolicLink {
                path, target: packet.bytes, metadata,
            },
        ).await
    }

    fn create_special_on_meter(
        &self,
        path: &MountPath,
        kind: MountNodeKind,
        device: Option<(u32, u32)>,
        metadata: FileMetadata,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<MountLookup, MountSourceError> {
        self.runtime.block_on_measured(|| async {
            self.create_special_async_with_carrier(
                path, kind, device, metadata, &mut NativeDemandCarrier::Shared(meter),
            ).await
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    async fn create_special_async_with_carrier(
        &self,
        path: &MountPath,
        kind: MountNodeKind,
        device: Option<(u32, u32)>,
        metadata: FileMetadata,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<MountLookup, MountSourceError> {
        let file_kind = match (kind, device) {
            (MountNodeKind::Fifo, None) => FileKind::Fifo,
            (MountNodeKind::Socket, None) => FileKind::Socket,
            (MountNodeKind::CharacterDevice, Some(_)) => FileKind::CharacterDevice,
            (MountNodeKind::BlockDevice, Some(_)) => FileKind::BlockDevice,
            _ => return Err(MountSourceError::Unsupported(
                "special-file kind and device identity disagree".to_owned(),
            )),
        };
        self.create_authored_async_with_carrier(
            path, metadata, kind, 0, device, carrier,
            |path| match device {
                Some((major, minor)) => AuthoredMutation::CreateDevice {
                    path, kind: file_kind, major, minor, metadata,
                },
                None => AuthoredMutation::CreateEmptySpecial { path, kind: file_kind, metadata },
            },
        ).await
    }

    fn set_attributes_on_meter(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<(), MountSourceError> {
        self.runtime.block_on_measured(
            || self.set_attributes_async_with_meter(path, metadata, logical_bytes, meter),
            meter.as_ref(),
        ).map_err(capture_io_error)?
    }

    async fn set_attributes_async_with_meter(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<(), MountSourceError> {
        self.set_attributes_async_with_carrier(
            path, metadata, logical_bytes, &mut NativeDemandCarrier::Shared(meter),
        ).await
    }

    pub(super) async fn set_attributes_async_with_carrier(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        use super::lazy::{mount_clone_namespace, mount_demand_error, mount_engine_error};
        let cancellation = carrier.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
            Ok::<_, MountSourceError>(work.cancellation().clone())
        })?;
        let namespace_allocation;
        let namespace;
        (namespace, namespace_allocation) = self.namespace_path_with_carrier(path, carrier)?;
        let bytes = if namespace_allocation.is_some() { 0 } else { namespace.retained_bytes() };
        let mut namespace = NativeCarrierOwned {
            value: Some(namespace), carrier: &mut *carrier, bytes,
        };
        let (path, work) = namespace.parts();
        let (mut candidate, epoch, stamp) = self.checkout.prepare_native_phase_with_carrier(
            work, &cancellation, Some(&self.cancellation),
        ).await?;
        let node = native_node_if_bound_with_carrier(
            &candidate.checkout, path, work, &cancellation,
        ).await?.ok_or(MountSourceError::NotFound)?;
        let accepted = if let Some(original) = namespace.carrier.original_native_work() {
            let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
            let count = if logical_bytes.is_some() { 2 } else { 1 };
            let mut authored = crate::kernel::Owned::<Vec<AuthoredMutation>>::allocate(
                count, &mut accounting,
            ).map_err(MountSourceError::Filesystem)?;
            accounting.charge(crate::WorkCounters {
                bytes_copied: (count * std::mem::size_of::<AuthoredMutation>()) as u64,
                ..crate::WorkCounters::UNCHARGED
            }).map_err(MountSourceError::Filesystem)?;
            let path = namespace.value.as_ref().expect("native metadata path is live");
            let mut metadata_path = path.clone_with_accounting(
                &mut accounting, &cancellation,
            ).map_err(MountSourceError::Filesystem)?;
            let resized_path = if logical_bytes.is_some() {
                Some(path.clone_with_accounting(&mut accounting, &cancellation).map_err(MountSourceError::Filesystem)?)
            } else {
                None
            };
            authored.allocation.absorb(&mut metadata_path.1).map_err(MountSourceError::Filesystem)?;
            authored.value.push(AuthoredMutation::SetMetadata { path: metadata_path.0, metadata });
            if let (Some(logical_bytes), Some(mut path)) = (logical_bytes, resized_path) {
                authored.allocation.absorb(&mut path.1).map_err(MountSourceError::Filesystem)?;
                authored.value.push(AuthoredMutation::Resize { path: path.0, logical_bytes });
            }
            self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
            let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) =
                authored.allocation else { unreachable!("native authored input keeps original custody"); };
            candidate.checkout.apply_authored_transaction_with_native_work(
                authored.value, allocation, &original, &cancellation,
            ).await.map(|(value, allocation)| {
                drop(value);
                drop(allocation);
            }).map_err(MountSourceError::Filesystem)
        } else {
            let value = &mut namespace.value;
            let (authored, bytes) = namespace.carrier.with_work(|work| {
                let mut scratch = CaptureScratchAllocation::new(work);
                let count = if logical_bytes.is_some() { 2 } else { 1 };
                let mut authored = scratch.vec::<AuthoredMutation>(count)?;
                let resized_path = if logical_bytes.is_some() {
                    let path = mount_clone_namespace(
                        value.as_ref().expect("native metadata path is live").components(),
                        self.limits, scratch.work,
                    )?;
                    scratch.track(path.retained_bytes());
                    Some(path)
                } else {
                    None
                };
                scratch.work.charge(crate::WorkCounters {
                    bytes_copied: (count * std::mem::size_of::<AuthoredMutation>()) as u64,
                    ..crate::WorkCounters::UNCHARGED
                }).map_err(mount_demand_error)?;
                authored.push(AuthoredMutation::SetMetadata {
                    path: value.take().expect("native metadata path is live"),
                    metadata,
                });
                if let (Some(logical_bytes), Some(path)) = (logical_bytes, resized_path) {
                    authored.push(AuthoredMutation::Resize { path, logical_bytes });
                }
                Ok::<_, MountSourceError>((authored, scratch.retain()))
            })?;
            let mut authored = NativeCarrierOwned {
                value: Some(authored), carrier: &mut *namespace.carrier, bytes,
            };
            let (budget, prior_live) = authored.carrier.with_work(|work| {
                Ok::<_, MountSourceError>((
                    work.remaining().map_err(mount_demand_error)?, work.live_allocation_bytes(),
                ))
            })?;
            self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
            let receipt = candidate.checkout.apply_authored_transaction(
                authored.value.take().expect("native authored input is live"), budget, &cancellation,
            ).await;
            authored.carrier.accept_with_prior_live(receipt, prior_live, mount_engine_error)
                .and_then(|value| {
                    let bytes = value.created_file_ids.capacity()
                        .checked_mul(std::mem::size_of::<Option<FileId>>())
                        .and_then(|bytes| u64::try_from(bytes).ok())
                        .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
                    let mut result = NativeCarrierOwned {
                        value: Some(value), carrier: &mut *authored.carrier, bytes,
                    };
                    result.carrier.with_work(|work| {
                        work.record_adoption(bytes).map_err(mount_demand_error)
                    })
                })
        };
        drop(namespace);
        drop(namespace_allocation);
        let installed = self.checkout.finish_native_phase_with_carrier(
            candidate, epoch, stamp, &ViewChange::Node(node),
            carrier, &cancellation, Some(&self.cancellation),
        ).await;
        accepted?;
        installed?;
        self.checkout.publish_mutation_with_carrier(
            carrier, &cancellation, Some(&self.cancellation),
            self.native_callback_guard.as_ref(),
        ).await
    }

    fn read_attribute_on_meter(
        &self,
        path: &MountPath,
        name: &[u8],
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError> {
        self.runtime.block_on_measured(|| async {
            use super::lazy::{mount_accept, mount_demand_error, mount_engine_error};
            let mut carrier = NativeDemandCarrier::Shared(meter);
            let (namespace, cancellation) = carrier.with_work(|work| {
                work.check().map_err(mount_demand_error)?;
                self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
                Ok::<_, MountSourceError>((
                    self.namespace_path_with_work(path, work)?, work.cancellation().clone(),
                ))
            })?;
            let bytes = namespace.retained_bytes();
            let mut namespace = NativeCarrierOwned { value: Some(namespace), carrier: &mut carrier, bytes };
            let name = namespace.carrier.with_work(|work| self.posix_attribute_name_with_work(name, work))?;
            let bytes = name.retained_bytes() as u64;
            let mut name = NativeCarrierOwned { value: Some(name), carrier: &mut *namespace.carrier, bytes };
            let observation = self.checkout.observe_with_carrier(
                ViewGate::callback_owner(), name.carrier, &cancellation, Some(&self.cancellation),
            ).await?;
            let budget = name.carrier.with_work(|work| work.remaining().map_err(mount_demand_error))?;
            let observed = observation.observer_measured(budget, &cancellation);
            let observed = name.carrier.with_work(|work| mount_accept(work, observed, mount_engine_error))?;
            let mut observer = LookupObserverOwned::new(observed, meter)?;
            let budget = name.carrier.with_work(|work| work.remaining().map_err(mount_demand_error))?;
            let result = observer.read_named_attribute_owned(
                namespace.value.as_ref().expect("attribute namespace is live"),
                name.value.as_ref().expect("attribute name is live"), budget, &cancellation,
            ).await;
            let accepted = name.carrier.with_work(|work| mount_accept(work, result, mount_engine_error));
            observer.receipt_delivered();
            let value = accepted?;
            let bytes = value.as_ref().map_or(0, |chunk| chunk.logical_owned_bytes);
            let mut output_carrier = NativeDemandCarrier::Shared(meter);
            let mut output = NativeCarrierOwned { value: Some(value), carrier: &mut output_carrier, bytes };
            output.carrier.with_work(|work| work.record_adoption(bytes).map_err(mount_demand_error))?;
            observer.finish()?;
            output.carrier.with_work(|work| {
                work.check().map_err(mount_demand_error)?;
                self.cancellation.check().map_err(|_| MountSourceError::Cancelled)
            })?;
            let value = output.value.take().expect("attribute output is live");
            output.bytes = 0;
            Ok(value)
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn list_attributes_on_meter(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<MountAttributePage, MountSourceError> {
        self.runtime.block_on_measured(|| async {
            use super::lazy::{mount_accept, mount_demand_error, mount_engine_error};
            let mut carrier = NativeDemandCarrier::Shared(meter);
            let (namespace, cancellation) = carrier.with_work(|work| {
                work.check().map_err(mount_demand_error)?;
                self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
                Ok::<_, MountSourceError>((
                    self.namespace_path_with_work(path, work)?, work.cancellation().clone(),
                ))
            })?;
            let bytes = namespace.retained_bytes();
            let mut namespace = NativeCarrierOwned { value: Some(namespace), carrier: &mut carrier, bytes };
            let cursor = namespace.carrier.with_work(|work| {
                cursor.map(|name| self.posix_attribute_name_with_work(name, work)).transpose()
            })?;
            let bytes = cursor.as_ref().map_or(0, |name| name.retained_bytes() as u64);
            let mut cursor = NativeCarrierOwned { value: Some(cursor), carrier: &mut *namespace.carrier, bytes };
            let observation = self.checkout.observe_with_carrier(
                ViewGate::callback_owner(), cursor.carrier, &cancellation, Some(&self.cancellation),
            ).await?;
            let budget = cursor.carrier.with_work(|work| work.remaining().map_err(mount_demand_error))?;
            let observed = observation.observer_measured(budget, &cancellation);
            let observed = cursor.carrier.with_work(|work| mount_accept(work, observed, mount_engine_error))?;
            let mut observer = LookupObserverOwned::new(observed, meter)?;
            let budget = cursor.carrier.with_work(|work| work.remaining().map_err(mount_demand_error))?;
            let result = observer.list_named_attributes(
                namespace.value.as_ref().expect("attribute namespace is live"),
                cursor.value.as_ref().expect("attribute cursor is live").as_ref(),
                maximum_entries, budget, &cancellation,
            ).await;
            let accepted = cursor.carrier.with_work(|work| mount_accept(work, result, mount_engine_error));
            observer.receipt_delivered();
            let listing = accepted?;
            observer.finish()?;
            native_attribute_page_with_carrier(listing, cursor.carrier, Some(&self.cancellation))
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn flush_on_meter(
        &self,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<(), MountSourceError> {
        self.runtime.block_on_measured(
            || self.flush_async_with_meter(meter), meter.as_ref(),
        ).map_err(capture_io_error)?
    }

    async fn flush_async_with_meter(
        &self,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<(), MountSourceError> {
        use super::lazy::mount_demand_error;
        let cancellation = {
            let work = meter.lock();
            work.check().map_err(mount_demand_error)?;
            work.cancellation().clone()
        };
        self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
        let publishes = {
            let state = await_measured_lock_with_meter(
                self.checkout.state.read(), meter, &cancellation, Some(&self.cancellation),
            ).await?;
            state.publishes_at_native_boundary()
        };
        if publishes {
            self.sync_async_with_permit_meter(
                crate::PublicationPermit::Unrestricted, false, meter, &cancellation,
            ).await?;
        }
        meter.lock().check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Stale)
    }

    fn observe_view_on_meter(
        &self,
        observer: Weak<dyn ViewObserver>,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<(), MountSourceError> {
        use super::lazy::mount_demand_error;
        {
            let mut work = meter.lock();
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
            work.charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            }).map_err(mount_demand_error)?;
        }
        // Metadata callbacks run outside the original-work mutex.
        let custody = observer.upgrade().and_then(|observer| observer.allocation_custody());
        self.checkout.ledger.observe_measured_with_meter(observer, custody, meter)?;
        meter.lock().check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Stale)
    }

    fn acquire_binding_lease_on_carrier(
        &self,
        expected_epoch: Option<u64>,
        carrier: &mut NativeDemandCarrier<'_>,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<Box<dyn MountViewLease>, MountSourceError> {
        use super::lazy::mount_demand_error;
        meter.lock().check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
        let owner = ViewGate::callback_owner();
        let mut lease = self.runtime.block_on_measured(
            || self.binding_lease_async_with_carrier(owner, expected_epoch, carrier),
            meter.as_ref(),
        ).map_err(capture_io_error)??;
        let copied = crate::WorkCounters {
            bytes_copied: std::mem::size_of::<ViewReadLease>() as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        {
            let mut work = meter.lock();
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
            work.work.admit(&copied, &work.budget).map_err(MountSourceError::Work)?;
            let bytes = std::mem::size_of::<ViewReadLease>() as u64;
            work.allocate(bytes).map_err(mount_demand_error)?;
            if let Err(error) = lease.attach_owned_payload(bytes) {
                work.release(bytes).map_err(mount_demand_error)?;
                return Err(error);
            }
        }
        // The original gate proof reconciles physical Box retirement.
        // Neither the returned permit nor its proof retains the meter.
        let lease = Box::new(lease) as Box<dyn MountViewLease>;
        meter.lock().record(copied).map_err(mount_demand_error)?;
        Ok(lease)
    }

    fn fence_changes_on_carrier(
        &self,
        carrier: &mut NativeDemandCarrier<'_>,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<(), MountSourceError> {
        use super::lazy::mount_demand_error;
        meter.lock().check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
        self.runtime.block_on_measured(|| async {
            let cancellation = meter.lock().cancellation().clone();
            let mut view = super::view_ledger::ViewLedgerAdmission::default();
            self.checkout.settle_pending_with_carrier(
                carrier, &mut view, &cancellation, Some(&self.cancellation),
            ).await?;
            let publishes = {
                let state = await_measured_lock_with_carrier(
                    self.checkout.read_current_state(), carrier, &cancellation,
                    Some(&self.cancellation),
                ).await?;
                state.publishes_at_native_boundary()
            };
            if publishes {
                let mut before = self.sdk_publication_policy();
                self.sync_async_with_permit_carrier_guarded(
                    crate::PublicationPermit::Unrestricted, false, carrier, &mut view,
                    &cancellation, &mut before,
                ).await?;
            }
            meter.lock().check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Stale)
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    async fn binding_lease_async_with_carrier(
        &self,
        owner: CallbackOwner,
        expected_epoch: Option<u64>,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<ViewReadLease, MountSourceError> {
        use super::lazy::mount_demand_error;
        let cancellation = carrier.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            Ok::<_, MountSourceError>(work.cancellation().clone())
        })?;
        self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
        self.checkout.settle_pending_for_callback_with_carrier(
            owner, carrier, &cancellation, Some(&self.cancellation),
        ).await?;
        let lease = self.checkout.view_gate.read_for_callback_with_carrier(
            owner, expected_epoch, carrier, &cancellation, Some(&self.cancellation),
        ).await?;
        carrier.with_work(|work| work.check().map_err(mount_demand_error))?;
        self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
        Ok(lease)
    }
}

impl<A, O> MountFilesystem for CheckoutMountSource<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn native_namespace_lineage(&self) -> Option<super::NativeMountSourceLineage<'_>> {
        Some(super::NativeMountSourceLineage {
            owner: &self.checkout,
            root: &self.root,
            native_original: match self.native_io_binding {
                NativeSourceIoBinding::Native => self.native_original.as_ref(),
                NativeSourceIoBinding::Ordinary => None,
            },
            lazy_owner: None,
            lazy_root: None,
            limits: self.limits,
            capture_gate: &self.capture_gate,
            capture_cancellation: &self.cancellation,
        })
    }

    fn native_callback_guard(&self) -> Option<Arc<dyn super::NativeMountCallbackGuard>> {
        self.native_callback_guard.as_ref().map(Arc::clone)
    }

    fn native_original_work(
        &self,
    ) -> Result<Option<super::NativeMountMeterWork>, MountSourceError> {
        CheckoutMountSource::native_original_work(self)
    }

    fn supports_posix_named_attributes(&self) -> bool {
        self.profile == FilesystemProfile::Posix
    }

    fn flush_publishes(&self) -> bool {
        // The policy is fixed at construction; an unreadable answer keeps
        // the conservative default.
        self.runtime
            .wait(|| async { Ok(self.checkout.publishes_at_native_boundary().await) })
            .unwrap_or(true)
    }

    fn view_stamp(&self) -> Option<ViewStamp> {
        self.checkout
            .view_gate
            .is_stable()
            .then(|| self.checkout.ledger.stamp())
    }

    fn observe_view(&self, observer: Weak<dyn ViewObserver>) {
        self.checkout.ledger.observe(observer).expect("unbounded view observer registration");
    }

    fn observe_view_with_work(
        &self,
        observer: Weak<dyn ViewObserver>,
        work: &mut super::NativeMountWork<'_>,
    ) -> Result<(), MountSourceError> {
        work.run_with_meter(|original| self.observe_view_with_meter(observer, original))
    }

    fn observe_view_with_meter(
        &self,
        observer: Weak<dyn ViewObserver>,
        work: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError> {
        self.observe_view_on_meter(observer, work.meter())
    }

    fn fence_changes_with_work(
        &self,
        work: &mut super::NativeMountWork<'_>,
    ) -> Result<(), MountSourceError> {
        work.run_with_meter(|original| self.fence_changes_with_meter(original))
    }

    fn fence_changes_with_meter(
        &self,
        work: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError> {
        self.fence_changes_on_carrier(&mut NativeDemandCarrier::Native(work), work.meter())
    }

    fn unchanged_since(&self, path: &MountPath, file_id: Option<FileId>, stamp: ViewStamp) -> bool {
        self.path(path)
            .is_ok_and(|path| self.checkout.unchanged_since(&path, file_id, stamp))
    }

    fn reported_unchanged_since(
        &self,
        path: &MountPath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
    ) -> bool {
        self.path(path).is_ok_and(|path| {
            self.checkout
                .reported_unchanged_since(&path, file_id, stamp)
        })
    }

    fn node_unchanged_since(&self, file_id: FileId, stamp: ViewStamp) -> bool {
        self.checkout.node_unchanged_since(file_id, stamp)
    }

    fn listing_unchanged_since(&self, path: &MountPath, stamp: ViewStamp) -> bool {
        self.path(path)
            .is_ok_and(|path| self.checkout.listing_unchanged_since(&path, stamp))
    }

    fn node_unchanged_since_with_meter(
        &self,
        file_id: FileId,
        stamp: ViewStamp,
        work: &super::NativeMountMeterWork,
    ) -> Result<bool, MountSourceError> {
        use super::lazy::{mount_demand_error, mount_items};
        let mut work = work.meter().lock();
        work.check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        mount_items(&mut work, 1)?;
        let unchanged = if self.checkout.view_gate.is_stable() {
            self.checkout.ledger.node_unchanged_since(file_id, stamp, &mut work)?
        } else {
            false
        };
        work.check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        Ok(unchanged)
    }

    fn listing_unchanged_since_with_meter(
        &self,
        path: &MountPath,
        stamp: ViewStamp,
        work: &super::NativeMountMeterWork,
    ) -> Result<bool, MountSourceError> {
        use super::lazy::{mount_demand_error, mount_items};
        let mut carrier = NativeDemandCarrier::Shared(work.meter());
        let namespace = carrier.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            self.namespace_path_with_work(path, work)
        })?;
        let bytes = namespace.retained_bytes();
        let mut input = NativeCarrierOwned { value: Some(namespace), carrier: &mut carrier, bytes };
        let (namespace, carrier) = input.parts();
        carrier.with_work(|work| {
            mount_items(work, 1)?;
            let unchanged = if self.checkout.view_gate.is_stable() {
                self.checkout.ledger.listing_unchanged_since(namespace, stamp, work)?
            } else {
                false
            };
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            Ok(unchanged)
        })
    }

    fn folded_path(&self, path: &MountPath) -> Option<MountPath> {
        self.folds_names().then(|| {
            path.components()
                .iter()
                .fold(MountPath::root(), |folded, component| {
                    folded.child(
                        self.folded_name(component)
                            .unwrap_or_else(|| component.clone()),
                    )
                })
        })
    }

    fn folded_path_with_meter(
        &self,
        path: &MountPath,
        work: &super::NativeMountMeterWork,
    ) -> Result<Option<(MountPath, u64)>, MountSourceError> {
        use super::lazy::{mount_demand_error, mount_items};
        let mut work = work.meter().lock();
        work.check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        mount_items(&mut work, 1)?;
        if !self.folds_names() {
            return Ok(None);
        }
        let mut scratch = CaptureScratchAllocation::new(&mut work);
        let mut components = if path.components().is_empty() {
            Vec::new()
        } else {
            scratch.vec(path.components().len())?
        };
        for component in path.components() {
            scratch.work.charge(crate::WorkCounters {
                source_path_components: 1, items_examined: 1, ..crate::WorkCounters::UNCHARGED
            }).map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            let folded = self.folded_name_with_work(component, scratch.work)?;
            scratch.track(folded.capacity() as u64);
            components.push(folded);
        }
        scratch.work.check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        let bytes = scratch.retain();
        Ok(Some((MountPath { components }, bytes)))
    }

    fn binding_epoch(&self) -> Option<u64> {
        Some(self.checkout.view_gate.generation())
    }

    fn view_is_stable(&self) -> bool {
        self.checkout.view_gate.is_stable()
    }

    fn view_is_stable_with_meter(
        &self,
        work: &super::NativeMountMeterWork,
    ) -> Result<bool, MountSourceError> {
        use super::lazy::{mount_demand_error, mount_items};
        let mut work = work.meter().lock();
        work.check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        mount_items(&mut work, 1)?;
        let stable = self.checkout.view_gate.is_stable();
        work.check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        Ok(stable)
    }

    fn view_stamp_with_meter(
        &self,
        work: &super::NativeMountMeterWork,
    ) -> Result<Option<ViewStamp>, MountSourceError> {
        use super::lazy::{mount_demand_error, mount_items};
        let mut work = work.meter().lock();
        work.check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        mount_items(&mut work, 1)?;
        let stamp = if self.checkout.view_gate.is_stable() {
            mount_items(&mut work, 1)?;
            Some(self.checkout.ledger.stamp())
        } else {
            None
        };
        work.check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        Ok(stamp)
    }

    fn binding_epoch_with_meter(
        &self,
        work: &super::NativeMountMeterWork,
    ) -> Result<Option<u64>, MountSourceError> {
        use super::lazy::{mount_demand_error, mount_items};
        let mut work = work.meter().lock();
        work.check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        mount_items(&mut work, 1)?;
        let epoch = self.checkout.view_gate.generation();
        work.check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        Ok(Some(epoch))
    }

    fn unchanged_since_with_meter(
        &self,
        path: &MountPath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
        work: &super::NativeMountMeterWork,
    ) -> Result<bool, MountSourceError> {
        self.unchanged_since_with_carrier(
            path, file_id, stamp, &mut NativeDemandCarrier::Shared(work.meter()),
        )
    }

    fn reported_unchanged_since_with_meter(
        &self,
        path: &MountPath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
        work: &super::NativeMountMeterWork,
    ) -> Result<bool, MountSourceError> {
        use super::lazy::{mount_demand_error, mount_items};
        let mut carrier = NativeDemandCarrier::Shared(work.meter());
        let namespace = carrier.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            self.namespace_path_with_work(path, work)
        })?;
        let bytes = namespace.retained_bytes();
        let mut input = NativeCarrierOwned { value: Some(namespace), carrier: &mut carrier, bytes };
        let (namespace, carrier) = input.parts();
        carrier.with_work(|work| {
            mount_items(work, 1)?;
            let unchanged = if self.checkout.view_gate.is_stable() {
                self.checkout.ledger.reported_unchanged_since(
                    namespace, file_id, stamp, work,
                )?
            } else {
                false
            };
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            Ok(unchanged)
        })
    }

    fn acquire_view_lease(&self) -> Result<Box<dyn MountViewLease>, MountSourceError> {
        self.acquire_binding_lease(None)
    }

    /// Leases the view for a callback, once every pending creation is
    /// applied: whatever the callback then reads sees them.
    fn acquire_binding_lease(
        &self,
        expected_epoch: Option<u64>,
    ) -> Result<Box<dyn MountViewLease>, MountSourceError> {
        let owner = ViewGate::callback_owner();
        let lease = self.runtime.wait(|| async {
            if !self.checkout.view_gate.reads_for_callback(owner) {
                self.checkout.settle_pending().await;
            }
            Arc::clone(&self.checkout.view_gate)
                .read_for_callback(owner, expected_epoch)
                .await
        })?;
        Ok(Box::new(lease))
    }

    fn acquire_binding_lease_with_work(
        &self,
        expected_epoch: Option<u64>,
        work: &mut super::NativeMountWork<'_>,
    ) -> Result<Box<dyn MountViewLease>, MountSourceError> {
        work.run_with_meter(|original| self.acquire_binding_lease_with_meter(expected_epoch, original))
    }

    fn acquire_binding_lease_with_meter(
        &self,
        expected_epoch: Option<u64>,
        work: &super::NativeMountMeterWork,
    ) -> Result<Box<dyn MountViewLease>, MountSourceError> {
        self.acquire_binding_lease_on_carrier(
            expected_epoch, &mut NativeDemandCarrier::Native(work), work.meter(),
        )
    }

    fn lookup(&self, path: &MountPath) -> Result<Option<MountLookup>, MountSourceError> {
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| self.lookup_async(path, owner))
    }

    fn lookup_with_work(
        &self,
        path: &MountPath,
        work: &mut super::NativeMountWork<'_>,
    ) -> Result<Option<MountLookup>, MountSourceError> {
        work.run_with_meter(|original| self.lookup_with_meter(path, original))
    }

    fn lookup_with_meter(
        &self,
        path: &MountPath,
        work: &super::NativeMountMeterWork,
    ) -> Result<Option<MountLookup>, MountSourceError> {
        self.lookup_on_native_work(path, work)
    }

    fn lookup_pinned_with_work(
        &self,
        path: &MountPath,
        work: &mut super::NativeMountWork<'_>,
    ) -> Result<Option<(MountLookup, Option<super::MountContentPin>)>, MountSourceError> {
        work.run_with_meter(|original| self.lookup_pinned_with_meter(path, original))
    }

    fn read_pinned_with_work(
        &self,
        _path: &MountPath,
        _pin: super::MountContentPin,
        _offset: u64,
        _length: u64,
        _piece: u32,
        _sink: &mut super::NativeMountContentSink<'_>,
        work: &mut super::NativeMountWork<'_>,
    ) -> Result<(), MountSourceError> {
        work.run_with_meter(|original| self.read_pinned_with_meter(_path, _pin, _offset, _length, _piece, _sink, original))
    }

    fn lookup_pinned_with_meter(
        &self,
        path: &MountPath,
        work: &super::NativeMountMeterWork,
    ) -> Result<Option<(MountLookup, Option<super::MountContentPin>)>, MountSourceError> {
        Ok(self.lookup_with_meter(path, work)?.map(|lookup| (lookup, None)))
    }

    fn read_pinned_with_meter(
        &self,
        _path: &MountPath,
        _pin: super::MountContentPin,
        _offset: u64,
        _length: u64,
        _piece: u32,
        _sink: &mut super::NativeMountContentSink<'_>,
        work: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError> {
        work.native_materialization_work().admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        Err(MountSourceError::Stale)
    }

    fn open_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| {
            self.open_file_on_meter(path, false, meter).map(|((handle, _), _)| handle)
        })
    }

    fn open_file_with_meter(
        &self, path: &MountPath, work: &super::NativeMountMeterWork,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        self.open_file_on_meter(path, false, work.meter()).map(|(handle, _)| handle)
    }

    fn open_file_with_lookup(
        &self, path: &MountPath,
    ) -> Result<(Arc<dyn MountOpenFile>, MountLookup), MountSourceError> {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| {
            self.open_file_on_meter(path, true, meter).map(|((handle, _), lookup)| {
                (handle, lookup.expect("native lookup was requested"))
            })
        })
    }

    fn open_file_with_lookup_with_meter(
        &self, path: &MountPath, work: &super::NativeMountMeterWork,
    ) -> Result<((Arc<dyn MountOpenFile>, u64), MountLookup), MountSourceError> {
        self.open_file_on_meter(path, true, work.meter()).map(|(handle, lookup)| {
            (handle, lookup.expect("native lookup was requested"))
        })
    }

    fn open_created(
        &self, _path: &MountPath, created: &MountLookup,
    ) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| {
            self.open_created_on_meter(created, meter).map(|(handle, _)| handle)
        })
    }

    fn open_created_with_meter(
        &self, _path: &MountPath, created: &MountLookup, work: &super::NativeMountMeterWork,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        let original = work.native_materialization_work();
        self.open_created_with_carrier(created, NativeDemandCarrier::Native(work), original.runtime_meter())
    }

    fn detach_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| {
            self.detach_file_on_meter(path, meter).map(|(handle, _)| handle)
        })
    }

    fn detach_file_with_meter(
        &self, path: &MountPath, work: &super::NativeMountMeterWork,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        self.detach_file_on_meter(path, work.meter())
    }

    fn shutdown_with_meter(
        &self, work: &super::NativeMountMeterWork,
    ) -> Result<bool, MountSourceError> {
        let changed = !self.capture_retired();
        self.cancel_with_carrier(&mut NativeDemandCarrier::Native(work))?;
        Ok(changed)
    }

    fn shutdown_with_cleanup_wait(
        &self, wait: &CancellationToken,
    ) -> Result<bool, MountSourceError> {
        let changed = !self.capture_retired();
        self.cancel_with_cleanup_wait(wait)?;
        Ok(changed)
    }

    fn shutdown_accepted_cleanup(&self) -> Result<bool, MountSourceError> {
        let changed = !self.capture_retired();
        self.cancel_accepted_cleanup()?;
        Ok(changed)
    }

    fn read_link(&self, path: &MountPath) -> Result<OwnedBytes, MountSourceError> {
        let path = self.path(path)?;
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| async {
            let observation = self.checkout.observe(owner).await?;
            let mut checkout = observation.observer();
            checkout
                .read_symbolic_link(&path, boundary_budget(), &self.cancellation)
                .await
                .map(|receipt| receipt.value)
                .map_err(engine_error)
        })
    }

    fn read_link_with_work(
        &self,
        path: &MountPath,
        work: &mut super::NativeMountWork<'_>,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        work.run_with_meter(|original| self.read_link_with_meter(path, original))
    }

    fn read_link_with_meter(
        &self,
        path: &MountPath,
        work: &super::NativeMountMeterWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        let original = work.native_materialization_work();
        original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        let owner = ViewGate::callback_owner();
        let runtime_meter = original.runtime_meter_owner();
        self.runtime.block_on_measured(
            || self.read_bytes_async_with_native_work(path, None, owner, work),
            runtime_meter.as_ref(),
        ).map_err(|error| MountSourceError::NativeWork(original.runtime_error(error)))?
    }

    fn read_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u32,
    ) -> Result<OwnedBytes, MountSourceError> {
        let path = self.path(path)?;
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| async {
            let observation = self.checkout.observe(owner).await?;
            let mut checkout = observation.observer();
            checkout
                .read_file_range(
                    &path,
                    ByteRange {
                        offset,
                        length: u64::from(length),
                    },
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map(|receipt| receipt.value.bytes)
                .map_err(engine_error)
        })
    }

    fn read_range_with_work(
        &self,
        path: &MountPath,
        offset: u64,
        length: u32,
        work: &mut super::NativeMountWork<'_>,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        work.run_with_meter(|original| self.read_range_with_meter(path, offset, length, original))
    }

    fn read_range_with_meter(
        &self,
        path: &MountPath,
        offset: u64,
        length: u32,
        work: &super::NativeMountMeterWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        let original = work.native_materialization_work();
        original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        let owner = ViewGate::callback_owner();
        let runtime_meter = original.runtime_meter_owner();
        self.runtime.block_on_measured(
            || self.read_bytes_async_with_native_work(
                path, Some(ByteRange { offset, length: u64::from(length) }), owner, work,
            ),
            runtime_meter.as_ref(),
        ).map_err(|error| MountSourceError::NativeWork(original.runtime_error(error)))?
    }

    fn seek(
        &self,
        path: &MountPath,
        offset: u64,
        target: MountSeekTarget,
    ) -> Result<Option<u64>, MountSourceError> {
        let path = self.path(path)?;
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| async {
            let observation = self.checkout.observe(owner).await?;
            let mut checkout = observation.observer();
            checkout
                .seek_file_extent(
                    &path,
                    offset,
                    match target {
                        MountSeekTarget::Data => ExtentSeekTarget::Data,
                        MountSeekTarget::Hole => ExtentSeekTarget::Hole,
                    },
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(engine_error)
        })
    }

    fn read_directory(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountDirectoryPage, MountSourceError> {
        let path = self.path(path)?;
        let cursor = cursor.map(|bytes| self.logical_name(bytes)).transpose()?;
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| async {
            let observation = self.checkout.observe(owner).await?;
            let mut checkout = observation.observer();
            let receipt = checkout
                .list_directory_records(
                    &path,
                    cursor.as_ref(),
                    maximum_entries,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            let has_more = receipt.value.has_more;
            let entries = receipt
                .value
                .entries
                .into_iter()
                .map(|entry| {
                    Ok(MountDirectoryEntry {
                        name: native_mount_name(&entry.name)?,
                        node: mount_node(entry.record),
                        metadata: entry.metadata,
                        // A checkout lookup issues no content pin.
                        pin: None,
                    })
                })
                .collect::<Result<Vec<_>, MountSourceError>>()?;
            let next_cursor = has_more
                .then(|| entries.last().map(|entry| entry.name.clone()))
                .flatten();
            Ok(MountDirectoryPage::new(entries, next_cursor, None))
        })
    }

    fn read_directory_with_work(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        work: &mut super::NativeMountWork<'_>,
    ) -> Result<MountDirectoryPage, MountSourceError> {
        work.run_with_meter(|original| self.read_directory_with_meter(path, cursor, maximum_entries, original))
    }

    fn read_directory_with_meter(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        work: &super::NativeMountMeterWork,
    ) -> Result<MountDirectoryPage, MountSourceError> {
        let original = work.native_materialization_work();
        original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        let owner = ViewGate::callback_owner();
        let runtime_meter = original.runtime_meter_owner();
        self.runtime.block_on_measured(
            || self.read_directory_async_with_native_work(path, cursor, maximum_entries, owner, work),
            runtime_meter.as_ref(),
        ).map_err(|error| MountSourceError::NativeWork(original.runtime_error(error)))?
    }

    fn create_file(
        &self, path: &MountPath, metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| self.create_file_on_meter(path, metadata, None, meter))
    }

    fn create_file_with_meter(
        &self, path: &MountPath, metadata: FileMetadata, work: &super::NativeMountMeterWork,
    ) -> Result<MountLookup, MountSourceError> {
        self.create_file_on_carrier(path, metadata, None, NativeDemandCarrier::Native(work), work.meter())
    }

    fn create_absent_file(
        &self, path: &MountPath, metadata: FileMetadata, absent_since: ViewStamp,
    ) -> Result<MountLookup, MountSourceError> {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| self.create_file_on_meter(path, metadata, Some(absent_since), meter))
    }

    fn create_absent_file_with_meter(
        &self, path: &MountPath, metadata: FileMetadata, absent_since: ViewStamp,
        work: &super::NativeMountMeterWork,
    ) -> Result<MountLookup, MountSourceError> {
        self.create_file_on_carrier(path, metadata, Some(absent_since), NativeDemandCarrier::Native(work), work.meter())
    }

    fn create_directory(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| {
            self.create_directory_on_meter(path, metadata, meter)
        })
    }

    fn create_directory_with_meter(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        work: &super::NativeMountMeterWork,
    ) -> Result<MountLookup, MountSourceError> {
        self.runtime.block_on_measured(|| async {
            self.create_authored_async_with_carrier(
                path, metadata, MountNodeKind::Directory, 0, None,
                &mut NativeDemandCarrier::Native(work),
                |path| AuthoredMutation::CreateDirectory { path, metadata },
            ).await
        }, work.meter().as_ref()).map_err(capture_io_error)?
    }

    fn create_symbolic_link(
        &self,
        path: &MountPath,
        target: Bytes,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| {
            let packet = {
                let mut work = meter.lock();
                let mut scratch = CaptureScratchAllocation::new(&mut work);
                let mut bytes = scratch.vec::<u8>(target.len())?;
                scratch.work.charge(crate::WorkCounters {
                    bytes_copied: target.len() as u64,
                    ..crate::WorkCounters::UNCHARGED
                }).map_err(super::lazy::mount_demand_error)?;
                bytes.extend_from_slice(&target);
                let logical_owned_bytes = scratch.retain();
                crate::kernel::OwnedSourceChunk::claimed(OwnedBytes::from(bytes), logical_owned_bytes)
            };
            drop(target);
            self.create_symbolic_link_on_meter(path, packet, metadata, meter)
        })
    }

    fn create_symbolic_link_with_meter(
        &self,
        path: &MountPath,
        target: crate::kernel::OwnedSourceChunk,
        metadata: FileMetadata,
        work: &super::NativeMountMeterWork,
    ) -> Result<MountLookup, MountSourceError> {
        self.runtime.block_on_measured(|| async {
            self.create_symbolic_link_async_with_carrier(
                path, target, metadata, &mut NativeDemandCarrier::Native(work),
            ).await
        }, work.meter().as_ref()).map_err(capture_io_error)?
    }

    fn create_special(
        &self,
        path: &MountPath,
        kind: MountNodeKind,
        device: Option<(u32, u32)>,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| {
            self.create_special_on_meter(path, kind, device, metadata, meter)
        })
    }

    fn create_special_with_meter(
        &self,
        path: &MountPath,
        kind: MountNodeKind,
        device: Option<(u32, u32)>,
        metadata: FileMetadata,
        work: &super::NativeMountMeterWork,
    ) -> Result<MountLookup, MountSourceError> {
        self.runtime.block_on_measured(|| async {
            self.create_special_async_with_carrier(
                path, kind, device, metadata, &mut NativeDemandCarrier::Native(work),
            ).await
        }, work.meter().as_ref()).map_err(capture_io_error)?
    }

    fn set_attributes(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| {
            self.set_attributes_on_meter(path, metadata, logical_bytes, meter)
        })
    }

    fn set_attributes_with_work(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        work: &mut super::NativeMountWork<'_>,
    ) -> Result<(), MountSourceError> {
        work.run_with_meter(|original| self.set_attributes_with_meter(path, metadata, logical_bytes, original))
    }

    fn set_attributes_with_meter(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        work: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError> {
        self.runtime.block_on_measured(|| async {
            self.set_attributes_async_with_carrier(
                path, metadata, logical_bytes, &mut NativeDemandCarrier::Native(work),
            ).await
        }, work.meter().as_ref()).map_err(capture_io_error)?
    }

    fn read_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
    ) -> Result<Option<OwnedBytes>, MountSourceError> {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| {
            self.read_attribute_on_meter(path, name, meter).map(|value| value.map(|chunk| chunk.bytes))
        })
    }

    fn read_attribute_with_meter(
        &self,
        path: &MountPath,
        name: &[u8],
        work: &super::NativeMountMeterWork,
    ) -> Result<Option<crate::kernel::OwnedSourceChunk>, MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(|| async {
            let original = work.native_materialization_work();
            original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            let mut carrier = NativeDemandCarrier::Native(work);
            let cancellation = carrier.cancellation();
            let namespace_pair = self.namespace_path_with_carrier(path, &mut carrier)?;
            let _namespace_allocation = namespace_pair.1;
            let namespace = namespace_pair.0;
            let name_pair = native_attribute_name_with_native_work(
                self.profile, self.limits, name, &original,
            )?;
            let _name_allocation = name_pair.1;
            let name = name_pair.0;
            let observation = self.checkout.observe_owned_with_carrier(
                ViewGate::callback_owner(), &mut carrier, &cancellation, Some(&self.cancellation),
            ).await?;
            let _view = observation._view;
            let mut observer = LookupObserverOwned::new_in(observation.observer, carrier.reborrow())?;
            original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            let result = observer.read_named_attribute_owned_with_native_work(
                &namespace, &name, &original, &cancellation,
            ).await;
            let completed = observer.finish();
            let output = result.map_err(MountSourceError::Filesystem)?;
            completed?;
            original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            Ok(output)
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn list_attributes(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| {
            self.list_attributes_on_meter(path, cursor, maximum_entries, meter)
        })
    }

    fn list_attributes_with_meter(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        work: &super::NativeMountMeterWork,
    ) -> Result<(MountAttributePage, crate::NativeAllocation), MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(|| async {
            let original = work.native_materialization_work();
            original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            let mut carrier = NativeDemandCarrier::Native(work);
            let cancellation = carrier.cancellation();
            let namespace_pair = self.namespace_path_with_carrier(path, &mut carrier)?;
            let _namespace_allocation = namespace_pair.1;
            let namespace = namespace_pair.0;
            let cursor_pair = cursor.map(|name| native_attribute_name_with_native_work(
                self.profile, self.limits, name, &original,
            )).transpose()?;
            let (_cursor_allocation, cursor) = match cursor_pair {
                Some((name, allocation)) => (Some(allocation), Some(name)),
                None => (None, None),
            };
            let observation = self.checkout.observe_owned_with_carrier(
                ViewGate::callback_owner(), &mut carrier, &cancellation, Some(&self.cancellation),
            ).await?;
            let _view = observation._view;
            let mut observer = LookupObserverOwned::new_in(observation.observer, carrier.reborrow())?;
            original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            let result = observer.list_named_attributes_with_native_work(
                &namespace, cursor.as_ref(), maximum_entries, &original, &cancellation,
            ).await;
            let completed = observer.finish();
            let listing_pair = result.map_err(MountSourceError::Filesystem)?;
            let allocation = listing_pair.1;
            let listing = listing_pair.0;
            completed?;
            let output_pair = native_attribute_page_with_native_work(
                listing, allocation, &original, Some(&self.cancellation),
            )?;
            let allocation = output_pair.1;
            let page = output_pair.0;
            original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            Ok((page, allocation))
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn write_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| {
            let packet = {
                let mut work = meter.lock();
                let mut scratch = CaptureScratchAllocation::new(&mut work);
                let mut bytes = scratch.vec::<u8>(value.len())?;
                scratch.work.charge(crate::WorkCounters {
                    bytes_copied: value.len() as u64,
                    ..crate::WorkCounters::UNCHARGED
                }).map_err(super::lazy::mount_demand_error)?;
                bytes.extend_from_slice(&value);
                let logical_owned_bytes = scratch.retain();
                crate::kernel::OwnedSourceChunk::claimed(OwnedBytes::from(bytes), logical_owned_bytes)
            };
            self.write_attribute_on_meter(path, name, packet, mode, meter)
        })
    }

    fn write_attribute_with_meter(
        &self,
        path: &MountPath,
        name: &[u8],
        value: crate::kernel::OwnedSourceChunk,
        mode: MountAttributeWriteMode,
        work: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError> {
        let mut carrier = NativeDemandCarrier::Native(work);
        let bytes = if value.is_intrinsic() { 0 } else { value.logical_owned_bytes };
        let mut value = NativeCarrierOwned { value: Some(value), carrier: &mut carrier, bytes };
        self.runtime.block_on_measured(|| async {
            let (packet, carrier) = value.parts();
            self.mutate_attribute_async_with_carrier(path, name, Some((packet, mode)), carrier).await
        }, work.meter().as_ref()).map_err(capture_io_error)?
    }

    fn remove_attribute(&self, path: &MountPath, name: &[u8]) -> Result<(), MountSourceError> {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| {
            self.remove_attribute_on_meter(path, name, meter)
        })
    }

    fn remove_attribute_with_meter(
        &self,
        path: &MountPath,
        name: &[u8],
        work: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError> {
        self.runtime.block_on_measured(|| async {
            self.mutate_attribute_async_with_carrier(
                path, name, None, &mut NativeDemandCarrier::Native(work),
            ).await
        }, work.meter().as_ref()).map_err(capture_io_error)?
    }

    fn write_range(
        &self,
        path: &MountPath,
        offset: u64,
        bytes: Bytes,
    ) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        self.change_content(path, ContentChange::Write { offset, bytes: bytes.into() })
    }

    fn resize(&self, path: &MountPath, logical_bytes: u64) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        self.change_content(path, ContentChange::Resize { logical_bytes })
    }

    fn allocate_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        self.change_content(
            path,
            range_allocation(ByteRange { offset, length }, operation),
        )
    }

    fn clone_range(
        &self,
        source: &MountPath,
        source_offset: u64,
        destination: &MountPath,
        destination_offset: u64,
        length: u64,
    ) -> Result<(), MountSourceError> {
        let source = self.path(source)?;
        let destination = self.path(destination)?;
        self.change_content(
            destination,
            ContentChange::CloneFrom {
                source,
                source_offset,
                offset: destination_offset,
                length,
            },
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
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| {
            self.clone_range_by_id_on_meter(
                source_file_id, source_offset, destination_file_id, destination_offset, length, meter,
            )
        })
    }

    fn clone_range_by_id_with_meter(
        &self,
        source_file_id: FileId,
        source_offset: u64,
        destination_file_id: FileId,
        destination_offset: u64,
        length: u64,
        work: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError> {
        self.runtime.block_on_measured(|| async {
            self.clone_range_by_id_async_with_carrier(
                source_file_id, source_offset, destination_file_id, destination_offset, length,
                &mut NativeDemandCarrier::Native(work),
            ).await
        }, work.meter().as_ref()).map_err(capture_io_error)?
    }

    fn remove(&self, path: &MountPath, expected: Option<FileId>) -> Result<(), MountSourceError> {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| {
            self.remove_on_meter(path, expected, meter)
        })
    }

    fn remove_with_work(
        &self,
        path: &MountPath,
        expected: Option<FileId>,
        work: &mut super::NativeMountWork<'_>,
    ) -> Result<(), MountSourceError> {
        work.run_with_meter(|original| self.remove_with_meter(path, expected, original))
    }

    fn remove_with_meter(
        &self,
        path: &MountPath,
        expected: Option<FileId>,
        work: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError> {
        self.runtime.block_on_measured(|| async {
            self.remove_async_with_carrier(path, expected, &mut NativeDemandCarrier::Native(work)).await
        }, work.meter().as_ref()).map_err(capture_io_error)?
    }

    fn rename(
        &self,
        source: &MountPath,
        destination: &MountPath,
        replace: bool,
    ) -> Result<(), MountSourceError> {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| {
            self.rename_on_meter(source, destination, replace, meter)
        })
    }

    fn rename_with_work(
        &self,
        source: &MountPath,
        destination: &MountPath,
        replace: bool,
        work: &mut super::NativeMountWork<'_>,
    ) -> Result<(), MountSourceError> {
        work.run_with_meter(|original| self.rename_with_meter(source, destination, replace, original))
    }

    fn rename_with_meter(
        &self,
        source: &MountPath,
        destination: &MountPath,
        replace: bool,
        work: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError> {
        self.runtime.block_on_measured(|| async {
            self.rename_async_with_carrier(
                source, destination, replace, &mut NativeDemandCarrier::Native(work),
            ).await
        }, work.meter().as_ref()).map_err(capture_io_error)?
    }

    fn hard_link(
        &self,
        source: &MountPath,
        destination: &MountPath,
    ) -> Result<(), MountSourceError> {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| {
            self.hard_link_on_meter(source, destination, meter)
        })
    }

    fn hard_link_with_meter(
        &self,
        source: &MountPath,
        destination: &MountPath,
        work: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError> {
        self.runtime.block_on_measured(|| async {
            self.hard_link_async_with_carrier(
                source, destination, &mut NativeDemandCarrier::Native(work),
            ).await
        }, work.meter().as_ref()).map_err(capture_io_error)?
    }

    fn flush(&self) -> Result<(), MountSourceError> {
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
        BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| self.flush_on_meter(meter))
    }

    fn flush_with_work(
        &self,
        work: &mut super::NativeMountWork<'_>,
    ) -> Result<(), MountSourceError> {
        work.run_with_meter(|original| self.flush_with_meter(original))
    }

    fn flush_with_meter(
        &self,
        work: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError> {
        self.flush_on_meter(work.meter())
    }

    fn capture_host_path(
        &self,
        source_root: &Path,
        path: &MountPath,
    ) -> Result<(), MountSourceError> {
        self.capture_host_paths(source_root, std::slice::from_ref(path))
    }

    fn capture_host_paths(
        &self,
        source_root: &Path,
        paths: &[MountPath],
    ) -> Result<(), MountSourceError> {
        if paths.is_empty() {
            return Ok(());
        }
        let expected_root_identity = capture_root_identity(source_root).map_err(engine_error)?;
        self.capture_host_paths_with_identity(source_root, paths, expected_root_identity)
    }

    fn capture_host_paths_with_work(
        &self,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: NativeRootIdentity,
        work: &mut super::NativeMountWork<'_>,
    ) -> Result<(), MountSourceError> {
        work.run_with_meter(|original| self.capture_host_paths_with_meter(source_root, paths, expected_root_identity, original))
    }

    fn capture_host_paths_with_meter(
        &self,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: NativeRootIdentity,
        work: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError> {
        self.capture_host_with_identity_on_meter(
            source_root, paths, expected_root_identity, HostCaptureScope::ExactPaths, work.meter(), work,
        )
    }

    fn capture_host_subtree_with_work(
        &self,
        source_root: &Path,
        path: &MountPath,
        expected_root_identity: NativeRootIdentity,
        work: &mut super::NativeMountWork<'_>,
    ) -> Result<(), MountSourceError> {
        work.run_with_meter(|original| self.capture_host_subtree_with_meter(source_root, path, expected_root_identity, original))
    }

    fn capture_host_subtree_with_meter(
        &self,
        source_root: &Path,
        path: &MountPath,
        expected_root_identity: NativeRootIdentity,
        work: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError> {
        self.capture_host_with_identity_on_meter(
            source_root, std::slice::from_ref(path), expected_root_identity,
            HostCaptureScope::Subtree, work.meter(), work,
        )
    }

    fn capture_host_subtree(
        &self,
        source_root: &Path,
        path: &MountPath,
    ) -> Result<(), MountSourceError> {
        let expected_root_identity = capture_root_identity(source_root).map_err(engine_error)?;
        self.capture_host_with_identity(
            source_root, std::slice::from_ref(path), expected_root_identity,
            HostCaptureScope::Subtree, None,
        )
    }
}

fn mount_node(record: FileRecord) -> MountNode {
    let kind = match record.kind {
        FileKind::Regular => MountNodeKind::Regular,
        FileKind::Directory => MountNodeKind::Directory,
        FileKind::SymbolicLink => MountNodeKind::SymbolicLink,
        FileKind::Fifo => MountNodeKind::Fifo,
        FileKind::Socket => MountNodeKind::Socket,
        FileKind::CharacterDevice => MountNodeKind::CharacterDevice,
        FileKind::BlockDevice => MountNodeKind::BlockDevice,
        FileKind::ReparsePoint | FileKind::MountBoundary => MountNodeKind::Unsupported,
    };
    let logical_bytes = match &record.payload {
        FilePayload::InlineRegular(data) => {
            u64::try_from(data.as_bytes().len()).unwrap_or(u64::MAX)
        }
        FilePayload::Regular { logical_bytes, .. } => *logical_bytes,
        FilePayload::SymbolicLink { target_bytes, .. } => *target_bytes,
        FilePayload::Directory { .. }
        | FilePayload::Empty
        | FilePayload::Device { .. }
        | FilePayload::ReparsePoint { .. } => 0,
    };
    let device = match &record.payload {
        FilePayload::Device { major, minor } => Some((*major, *minor)),
        _ => None,
    };
    MountNode {
        file_id: record.file_id,
        kind,
        logical_bytes,
        link_count: record.link_count,
        device,
    }
}

/// Sets every modification and status-change time the metadata represents
/// to now, and returns whether it represents either.
/// What one grouped change alters in the checkout's view.
enum GroupedView {
    Bound(NamespacePath),
    Node(FileId),
}

fn native_content_change_time() -> Result<i64, MountSourceError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).ok()
        .and_then(|since| i64::try_from(since.as_nanos()).ok())
        .ok_or(MountSourceError::NativeStaticText {
            kind: super::NativeSourceTextKind::Engine, text: "the clock is outside file time",
        })
}

/// The current instant as a content-change time, in signed Unix-epoch nanoseconds.
fn content_change_time() -> Result<i64, MountSourceError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|since| i64::try_from(since.as_nanos()).ok())
        .ok_or_else(|| MountSourceError::Engine("the clock is outside file time".to_owned()))
}

/// The content change one native range allocation requests.
fn range_allocation<F>(range: ByteRange, operation: MountRangeAllocation) -> ContentChange<F> {
    match operation {
        MountRangeAllocation::PunchHole => ContentChange::ZeroRange {
            range,
            allocated: false,
            extend: false,
        },
        MountRangeAllocation::ZeroRange { extend } => ContentChange::ZeroRange {
            range,
            allocated: true,
            extend,
        },
        MountRangeAllocation::Preallocate { keep_size } => {
            ContentChange::Preallocate { range, keep_size }
        }
    }
}

pub(super) fn native_custody_error(error: super::NativeMountStartupAdmissionError) -> MountSourceError {
    match error {
        super::NativeMountStartupAdmissionError::Work(error) => MountSourceError::Work(error),
        super::NativeMountStartupAdmissionError::Demand(error) => super::lazy::mount_demand_error(error),
        super::NativeMountStartupAdmissionError::Materialization(error) => MountSourceError::NativeWork(error),
        super::NativeMountStartupAdmissionError::Allocation(error) => MountSourceError::Allocation(error),
        super::NativeMountStartupAdmissionError::ServicesCold => MountSourceError::ServicesNotInitialized,
        super::NativeMountStartupAdmissionError::ServiceUnavailable(error) => MountSourceError::ServiceUnavailable(error),
    }
}

fn engine_error(error: impl std::fmt::Display) -> MountSourceError {
    MountSourceError::Engine(error.to_string())
}

fn facade_error(error: OperationFailure<FsError>) -> MountSourceError {
    fs_error(error.error)
}

fn fs_error(error: FsError) -> MountSourceError {
    match error {
        FsError::CreationRejected | FsError::Mutation(GenerationMutationError::AlreadyExists) => {
            MountSourceError::AlreadyExists
        }
        FsError::NotFound
        | FsError::Mutation(
            GenerationMutationError::MissingParent | GenerationMutationError::MissingSource,
        ) => MountSourceError::NotFound,
        other => super::lazy::mount_engine_error(other),
    }
}

fn boundary_budget() -> WorkBudget {
    const OPERATIONS: u64 = 1_000_000;
    const BYTES: u64 = 256 * 1024 * 1024;
    WorkBudget {
        authority_records_read: OPERATIONS,
        authority_records_appended: OPERATIONS,
        authority_bytes_read: BYTES,
        authority_bytes_written: BYTES,
        object_probes: OPERATIONS,
        backend_read_operations: OPERATIONS,
        backend_write_operations: OPERATIONS,
        durability_operations: OPERATIONS,
        page_reads: OPERATIONS,
        page_writes: OPERATIONS,
        object_bytes_read: BYTES,
        object_bytes_written: BYTES,
        bytes_hashed: BYTES,
        bytes_copied: BYTES,
        bytes_encoded: BYTES,
        source_bytes_read: BYTES,
        source_path_components: OPERATIONS,
        source_entries_visited: OPERATIONS,
        output_bytes: BYTES,
        items_examined: OPERATIONS,
        items_returned: OPERATIONS,
        allocation_operations: OPERATIONS,
        peak_allocation_bytes: BYTES,
        materializations: OPERATIONS,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Fs;
    use crate::model::{
        AccessMode, CheckoutMode, ConsistencyMode, GenerationSelector, Lifecycle, MutationMode,
    };
    #[cfg(target_os = "linux")]
    use std::os::unix::ffi::OsStringExt;

    #[test]
    fn measured_callback_bridge_denies_owned_future_before_running_leaf()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = CallbackRuntime::create()?;
        let token = CancellationToken::new();
        let meter = crate::demand::NativeDemandReadMeter(StdMutex::new(
            crate::demand::DemandWork::new(WorkBudget {
                peak_allocation_bytes: 0, ..WorkBudget::UNBOUNDED
            }, &token),
        ));
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("not-issued");
        let issued = AtomicBool::new(false);
        let result = runtime.block_on_measured(|| async {
            issued.store(true, Ordering::Release);
            std::fs::write(&path, b"not admitted")
        }, &meter);
        let error = result.expect_err("future allocation must be admitted before polling");
        assert!(error.get_ref().is_some_and(|cause|
            cause.downcast_ref::<crate::DemandError>()
                .is_some_and(|cause| matches!(cause, crate::DemandError::Work(_)))));
        assert!(!issued.load(Ordering::Acquire));
        assert!(!path.exists());
        assert_eq!(meter.lock().work.allocation_operations, 0);
        assert_eq!(meter.lock().live_allocation_bytes(), 0);
        Ok(())
    }

    #[test]
    fn measured_callback_bridge_releases_real_poll_envelope()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = CallbackRuntime::create()?;
        let token = CancellationToken::new();
        let meter = crate::demand::NativeDemandReadMeter(StdMutex::new(
            crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &token),
        ));
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("issued");
        runtime.block_on_measured(|| async {
            std::fs::write(&path, b"real leaf")
        }, &meter)??;
        assert_eq!(std::fs::read(path)?, b"real leaf");
        assert_eq!(meter.lock().work.allocation_operations, 2);
        assert!(meter.lock().work.peak_allocation_bytes > 0);
        assert_eq!(meter.lock().live_allocation_bytes(), 0);
        Ok(())
    }

    #[test]
    fn borrowed_native_meter_denial_preserves_original_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let token = CancellationToken::new();
        let budget = WorkBudget { peak_allocation_bytes: 0, ..WorkBudget::UNBOUNDED };
        let mut work = crate::demand::DemandWork::new(budget, &token);
        work.record(crate::WorkCounters { items_examined: 7, ..crate::WorkCounters::UNCHARGED })?;
        let prefix = work.work;
        assert!(matches!(BorrowedNativeDemandMeter::new(&mut work), Err(MountSourceError::Work(_))));
        assert_eq!(work.work, prefix);
        assert_eq!(work.budget, budget);
        assert!(work.cancellation().same_state(&token));
        assert_eq!(work.live_allocation_bytes(), 0);
        Ok(())
    }

    #[test]
    fn borrowed_native_meter_restores_cancelled_live_ownership_and_wait_roots()
    -> Result<(), Box<dyn std::error::Error>> {
        let token = CancellationToken::new();
        let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &token);
        work.allocate(std::mem::size_of::<[u8; 32]>() as u64)?;
        let resident = Box::new([0_u8; 32]);
        let prepared = work.prepare_cancelled(&token)?;
        let prefix = work.work;
        let result: Result<(), MountSourceError> = BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| {
            meter.lock().record(crate::WorkCounters {
                items_examined: 3, backend_read_operations: 1,
                ..crate::WorkCounters::UNCHARGED
            }).map_err(super::super::lazy::mount_demand_error)?;
            token.cancel();
            Err(MountSourceError::Cancelled)
        });
        assert!(matches!(result, Err(MountSourceError::Cancelled)));
        assert!(matches!(work.check(), Err(crate::DemandError::Cancelled)));
        assert!(work.cancellation().same_state(&token));
        assert_eq!(work.work.items_examined, prefix.items_examined + 3);
        assert_eq!(work.work.backend_read_operations, prefix.backend_read_operations + 1);
        work.finish_cancelled(prepared)?;
        assert_eq!(work.live_allocation_bytes(), std::mem::size_of_val(resident.as_ref()) as u64);
        drop(resident);
        work.release(std::mem::size_of::<[u8; 32]>() as u64)?;
        assert_eq!(work.live_allocation_bytes(), 0);
        Ok(())
    }

    #[test]
    fn borrowed_native_meter_restores_poisoned_work_before_resuming_panic()
    -> Result<(), Box<dyn std::error::Error>> {
        let token = CancellationToken::new();
        let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &token);
        work.record(crate::WorkCounters { items_examined: 4, ..crate::WorkCounters::UNCHARGED })?;
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            BorrowedNativeDemandMeter::new(&mut work).expect("meter admission").run(|meter| {
                let mut held = meter.lock();
                held.record(crate::WorkCounters {
                    items_examined: 2, ..crate::WorkCounters::UNCHARGED
                }).expect("actual completed prefix");
                panic!("controlled borrowed callback panic");
            });
        }));
        assert!(panic.is_err());
        assert_eq!(work.work.items_examined, 6);
        assert_eq!(work.budget, WorkBudget::UNBOUNDED);
        assert!(work.cancellation().same_state(&token));
        assert_eq!(work.live_allocation_bytes(), 0);
        Ok(())
    }

    #[test]
    fn borrowed_async_meter_denies_frame_before_construction()
    -> Result<(), Box<dyn std::error::Error>> {
        let token = CancellationToken::new();
        let budget = WorkBudget { allocation_operations: 2, ..WorkBudget::UNBOUNDED };
        let mut work = crate::demand::DemandWork::new(budget, &token);
        work.allocate(std::mem::size_of::<[u8; 32]>() as u64)?;
        let resident = Box::new([0_u8; 32]);
        let constructed = std::cell::Cell::new(false);
        let result = futures::executor::block_on(
            BorrowedNativeDemandMeter::new(&mut work)?.run_async(|_| {
                constructed.set(true);
                async { Ok::<(), MountSourceError>(()) }
            }),
        );
        assert!(matches!(result, Err(MountSourceError::Work(_))));
        assert!(!constructed.get());
        assert_eq!(work.budget, budget);
        assert_eq!(work.work.allocation_operations, 2);
        assert!(work.cancellation().same_state(&token));
        assert_eq!(work.live_allocation_bytes(), std::mem::size_of_val(resident.as_ref()) as u64);
        drop(resident);
        work.release(std::mem::size_of::<[u8; 32]>() as u64)?;
        Ok(())
    }

    #[test]
    fn borrowed_async_meter_restores_poisoned_original_before_panic()
    -> Result<(), Box<dyn std::error::Error>> {
        let token = CancellationToken::new();
        let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &token);
        work.record(crate::WorkCounters { items_examined: 4, ..crate::WorkCounters::UNCHARGED })?;
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            futures::executor::block_on(
                BorrowedNativeDemandMeter::new(&mut work).expect("meter admission")
                    .run_async::<(), _, MountSourceError>(|meter| async move {
                        let mut held = meter.lock();
                        held.record(crate::WorkCounters {
                            items_examined: 2, ..crate::WorkCounters::UNCHARGED
                        }).expect("actual completed prefix");
                        panic!("controlled borrowed async poll panic");
                    }),
            )
        }));
        assert!(panic.is_err());
        assert_eq!(work.work.items_examined, 6);
        assert_eq!(work.budget, WorkBudget::UNBOUNDED);
        assert!(work.cancellation().same_state(&token));
        assert_eq!(work.live_allocation_bytes(), 0);
        Ok(())
    }

    #[test]
    fn borrowed_async_meter_drop_restores_cancelled_live_original()
    -> Result<(), Box<dyn std::error::Error>> {
        let token = CancellationToken::new();
        let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &token);
        work.allocate(std::mem::size_of::<[u8; 32]>() as u64)?;
        let resident = Box::new([0_u8; 32]);
        let prefix = work.work;
        {
            let cancellation = &token;
            let mut future = std::pin::pin!(
                BorrowedNativeDemandMeter::new(&mut work)?.run_async(|meter| async move {
                    meter.lock().record(crate::WorkCounters {
                        items_examined: 1, ..crate::WorkCounters::UNCHARGED
                    }).map_err(super::super::lazy::mount_demand_error)?;
                    cancellation.cancel();
                    std::future::pending::<Result<(), MountSourceError>>().await
                }),
            );
            let waker = futures::task::noop_waker();
            let mut context = Context::from_waker(&waker);
            assert!(future.as_mut().poll(&mut context).is_pending());
        }
        assert!(matches!(work.check(), Err(crate::DemandError::Cancelled)));
        assert!(work.cancellation().same_state(&token));
        assert_eq!(work.work.items_examined, prefix.items_examined + 1);
        assert_eq!(work.live_allocation_bytes(), std::mem::size_of_val(resident.as_ref()) as u64);
        drop(resident);
        work.release(std::mem::size_of::<[u8; 32]>() as u64)?;
        Ok(())
    }

    #[test]
    fn borrowed_native_meter_retires_actual_native_io_before_restoration()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = CallbackRuntime::create()?;
        let token = CancellationToken::new();
        let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &token);
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("actual-borrowed-leaf");
        let output = BorrowedNativeDemandMeter::new(&mut work)?.run(|meter| {
            let leaf_meter = Arc::clone(meter);
            let leaf = path.clone();
            let task = acyclic_native_runtime::run_blocking_io_measured(move || {
                use acyclic_native_runtime::NativeReadMeter;
                leaf_meter.admit_operations(1)?;
                let result = std::fs::write(leaf, b"retired");
                leaf_meter.record_operations(1)?;
                result
            }, Arc::clone(meter) as Arc<dyn acyclic_native_runtime::NativeReadMeter>)
                .map_err(capture_io_error)?;
            runtime.block_on_measured(|| task, meter.as_ref()).map_err(capture_io_error)?
                .map_err(capture_io_error)?.map_err(capture_io_error)
        });
        output?;
        assert_eq!(std::fs::read(path)?, b"retired");
        assert_eq!(work.work.backend_read_operations, 1);
        assert_eq!(work.live_allocation_bytes(), 0);
        assert!(work.cancellation().same_state(&token));
        Ok(())
    }

    #[cfg(target_os = "macos")]
    #[allow(unsafe_code, reason = "declares the NFS transport's C test hooks")]
    unsafe extern "C" {
        fn nfs4_test_exclusive_replay_identity() -> std::ffi::c_int;
        fn nfs4_test_namedattr_exclusive_replay_identity() -> std::ffi::c_int;
        fn nfs4_test_readdir_cookie_verifier() -> std::ffi::c_int;
        fn nfs4_test_sync_acknowledgement() -> std::ffi::c_int;
    }

    #[cfg(target_os = "macos")]
    #[test]
    #[allow(unsafe_code, reason = "calls the NFS transport's C test hook")]
    fn exclusive_replay_is_bound_to_the_opened_file_handle() {
        // SAFETY: the test hook has no arguments and owns all callback state.
        assert_eq!(unsafe { nfs4_test_exclusive_replay_identity() }, 0);
    }

    #[cfg(target_os = "macos")]
    #[test]
    #[allow(unsafe_code, reason = "calls the NFS transport's C test hook")]
    fn named_attribute_exclusive_replay_is_bound_to_owner_and_name() {
        assert_eq!(
            // SAFETY: the test hook has no arguments and owns all callback state.
            unsafe { nfs4_test_namedattr_exclusive_replay_identity() },
            0
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    #[allow(unsafe_code, reason = "calls the NFS transport's C test hook")]
    fn readdir_continuations_are_bound_to_the_namespace_revision() {
        // SAFETY: the test hook has no arguments and mutates no shared state.
        assert_eq!(unsafe { nfs4_test_readdir_cookie_verifier() }, 0);
    }

    #[cfg(target_os = "macos")]
    #[test]
    #[allow(unsafe_code, reason = "calls the NFS transport's C test hook")]
    fn nfs_sync_acknowledgement_requires_successful_callback() {
        // SAFETY: the test hook owns its callback state.
        assert_eq!(unsafe { nfs4_test_sync_acknowledgement() }, 0);
    }

    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "requires a live macOS NFS mount"]
    fn macos_explicit_file_fsync_reaches_mount_source() -> Result<(), Box<dyn std::error::Error>> {
        use std::io::Write as _;

        let source = Arc::new(source(FilesystemProfile::Posix)?);
        let temporary = tempfile::tempdir()?;
        let mut mount = crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id: source.volume_id()?,
                destination: temporary.path().to_path_buf(),
                writable: true,
            },
            Arc::clone(&source) as Arc<dyn MountFilesystem>,
        )?;
        let path = temporary.path().join("explicit-fsync");
        let mut file = std::fs::File::create(&path)?;
        file.write_all(b"durable")?;
        file.sync_all()?;
        assert_eq!(std::fs::read(&path)?, b"durable");
        drop(file);
        assert!(mount.stop()?);
        Ok(())
    }

    /// Objects in memory, refusing metadata while `refuse_metadata` is set.
    #[derive(Default)]
    struct MetadataRefusingObjects {
        inner: crate::memory::MemoryObjectStore,
        refuse_metadata: AtomicBool,
        metadata_attempts: AtomicU64,
    }

    impl MetadataRefusingObjects {
        fn admit(&self, object_id: ObjectId) -> Result<(), crate::storage::ObjectFailure> {
            if object_id.kind == crate::storage::ObjectKind::Metadata {
                self.metadata_attempts.fetch_add(1, Ordering::AcqRel);
            }
            if object_id.kind == crate::storage::ObjectKind::Metadata
                && self.refuse_metadata.load(Ordering::Acquire)
            {
                return Err(crate::storage::ObjectFailure::before_work(
                    crate::storage::ObjectStoreError::Corrupt,
                ));
            }
            Ok(())
        }
    }

    impl crate::storage::ObjectStore for MetadataRefusingObjects {
        fn put(
            &self,
            object_id: ObjectId,
            bytes: OwnedBytes,
            budget: WorkBudget,
        ) -> crate::storage::ObjectResult<()> {
            self.admit(object_id)?;
            crate::storage::ObjectStore::put(&self.inner, object_id, bytes, budget)
        }

        fn put_many(
            &self,
            writes: &[crate::storage::ObjectWrite],
            budget: WorkBudget,
        ) -> crate::storage::ObjectResult<()> {
            for write in writes {
                self.admit(write.object_id)?;
            }
            crate::storage::ObjectStore::put_many(&self.inner, writes, budget)
        }

        fn read(
            &self,
            object_id: ObjectId,
            maximum_bytes: u64,
            budget: WorkBudget,
        ) -> crate::storage::ObjectResult<crate::storage::ObjectRead> {
            crate::storage::ObjectStore::read(&self.inner, object_id, maximum_bytes, budget)
        }

        fn read_many(
            &self,
            requests: &[crate::storage::ObjectReadRequest],
            budget: WorkBudget,
        ) -> crate::storage::ObjectResult<Vec<crate::storage::ObjectRead>> {
            crate::storage::ObjectStore::read_many(&self.inner, requests, budget)
        }

        fn contains(
            &self,
            object_id: ObjectId,
            budget: WorkBudget,
        ) -> crate::storage::ObjectResult<bool> {
            crate::storage::ObjectStore::contains(&self.inner, object_id, budget)
        }
    }

    impl crate::async_storage::ImmediateObjectStore for MetadataRefusingObjects {}

    #[test]
    fn a_detached_write_that_cannot_stamp_changes_nothing() -> Result<(), Box<dyn std::error::Error>>
    {
        let objects = Arc::new(MetadataRefusingObjects::default());
        let fs = Fs::new(
            crate::memory::MemoryAuthorityStore::default(),
            Arc::clone(&objects),
            crate::EmbeddedCapabilities::MEMORY,
        );
        let config = VolumeConfig::portable(Lifecycle::Ephemeral);
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let checkout = runtime.block_on(async {
            let cancellation = CancellationToken::new();
            let volume = fs
                .create_volume(config, WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value;
            volume
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
                .await
                .map(|receipt| receipt.value)
        })?;
        let source = CheckoutMountSource::new(Arc::new(SharedCheckout::new(checkout)), config)?;
        let path = native_test_path("stamped.bin");
        let timed = FileMetadata {
            modified_ns: crate::kernel::MetadataField::Value(1),
            changed_ns: crate::kernel::MetadataField::Value(1),
            ..metadata()
        };
        source.create_file(&path, timed)?;
        source.write_range(&path, 0, Bytes::from_static(b"before"))?;
        let detached = source.detach_file(&path)?;
        let before = detached.lookup()?;

        objects.refuse_metadata.store(true, Ordering::Release);
        assert!(
            detached
                .write_range(0, Bytes::from_static(b"after!"))
                .is_err()
        );
        assert!(detached.resize(64).is_err());
        objects.refuse_metadata.store(false, Ordering::Release);

        // Content and stamp are one transition: neither happened, so no
        // cached fact about the file went stale without being invalidated.
        assert_eq!(detached.lookup()?, before);
        assert_eq!(detached.read_range(0, 6)?.as_ref(), b"before");
        detached.write_range(0, Bytes::from_static(b"after!"))?;
        let after = detached.lookup()?;
        assert_eq!(detached.read_range(0, 6)?.as_ref(), b"after!");
        assert_ne!(after.metadata, before.metadata);
        Ok(())
    }

    type MemorySource = CheckoutMountSource<
        crate::facade::MemoryAuthorityBackend,
        crate::facade::MemoryObjectBackend,
    >;

    /// Publication changes the checkout without changing its view. A
    /// candidate begun before it must not install over it: that would
    /// restore the published-over head, and every later publication would
    /// then conflict with it.
    #[test]
    fn a_candidate_never_installs_over_a_publication() -> Result<(), Box<dyn std::error::Error>> {
        let (source, _) =
            shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
        source.create_file(&native_test_path("first"), FileMetadata::default())?;
        let candidate = source.runtime.block_on(|| source.checkout.candidate())?;
        source.sync()?;
        let installed = source.runtime.block_on(|| async {
            Ok::<_, MountSourceError>(
                source
                    .checkout
                    .lock()
                    .await
                    .install_candidate(candidate, &ViewEffect::default()),
            )
        })?;
        assert!(!installed, "a candidate installed over a publication");
        source.create_file(&native_test_path("second"), FileMetadata::default())?;
        source.sync()?;
        Ok(())
    }

    #[test]
    fn cancelled_lifecycle_capture_cannot_commit_late() -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let (source, _) =
            shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
        let source = Arc::new(source);
        let revision = source.checkout.revision();
        let token = CancellationToken::new();
        let native = capture_meter(boundary_budget(), &token)?;
        let meter = native.meter();
        for cancel_source in [false, true] {
            let request = capture_request(&meter)?;
            let mut view = super::super::view_ledger::ViewLedgerAdmission::default();
            let mut carrier = super::NativeDemandCarrier::Native(&native);
            let original = native.native_materialization_work();
            let candidate = source.runtime.block_on_measured(|| async {
                let checkout = source.checkout.lock_with_carrier(
                    &mut carrier, &token, Some(source.cancellation()),
                ).await?;
                checkout.candidate_for_operation_with_carrier(None, &mut carrier, &token)
            }, original.runtime_meter()).map_err(capture_io_error)??;
            let mut resident = 0;
            let (wait, retirement) = CaptureCallerWait::prepare_capture(&original, &token)?;
            let registration = source.register_capture(Arc::clone(&request), &native, retirement, wait)?;
            if cancel_source {
                source.cancel();
            } else {
                drop(CancelCaptureOnDrop(Arc::clone(&request)));
            }
            assert!(request.cancellation.is_cancelled());
            let result = source.runtime.block_on_measured(|| source.commit_host_capture(
                candidate, InstallScope::Paths(&[]), Some(&request), &meter, &mut carrier, &mut view,
                &request.cancellation, &mut resident,
            ), original.runtime_meter()).map_err(capture_io_error)?;
            drop(registration);
            assert!(result.is_err());
            assert_eq!(source.checkout.revision(), revision);
        }
        assert!(source.capture_gate.lock().unwrap_or_else(PoisonError::into_inner).active.is_empty());
        Ok(())
    }

    #[test]
    fn finite_nonempty_capture_retains_prefix_and_refuses_work_before_worker()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let (source, _) =
            shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
        let source = Arc::new(source);
        let directory = tempfile::tempdir()?;
        std::fs::write(directory.path().join("file"), b"actual")?;
        let identity = capture_root_identity(directory.path())?;
        let baseline = Arc::new(NativeViewBaseline::new(identity));
        let cancellation = CancellationToken::new();
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        runtime.block_on(async {
            let paths = [native_test_path("file")];
            for budget in [WorkBudget::default(), WorkBudget {
                allocation_operations: 1, ..boundary_budget()
            }] {
                let failure = source.capture_host_with_identity_budgeted(
                    directory.path(), &paths, identity, HostCaptureScope::ExactPaths,
                    Arc::clone(&baseline), budget, &cancellation,
                ).await.expect_err("finite input denial must not start a worker");
                assert!(matches!(failure.error, MountSourceError::Work(_)));
                assert_eq!(failure.work.allocation_operations, budget.allocation_operations);
                assert_eq!(failure.work.backend_read_operations, 0);
                assert_eq!(failure.work.source_entries_visited, 0);
                assert!(source.capture_gate.lock().unwrap_or_else(PoisonError::into_inner).active.is_empty());
            }
            let captured = source.capture_host_with_identity_budgeted(
                directory.path(), &paths, identity, HostCaptureScope::ExactPaths,
                baseline, boundary_budget(), &cancellation,
            ).await?;
            assert!(captured.work.has_finite_allowances());
            assert!(captured.work.allocation_operations > 1);
            assert!(captured.work.items_examined > 0);
            assert!(captured.work.source_entries_visited > 0);
            assert!(captured.work.source_bytes_read >= 6);
            captured.work.verify(boundary_budget())?;
            Ok::<_, Box<dyn std::error::Error>>(())
        })?;
        assert_eq!(source.read_range(&native_test_path("file"), 0, 6)?.as_ref(), b"actual");
        Ok(())
    }

    #[test]
    fn borrowed_driver_capture_input_denial_restores_same_live_invocation()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let (source, _) =
            shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
        let directory = tempfile::tempdir()?;
        std::fs::write(directory.path().join("file"), b"not imported")?;
        let identity = capture_root_identity(directory.path())?;
        let path = native_test_path("file");
        for subtree in [false, true] {
            let cancellation = CancellationToken::new();
            let budget = WorkBudget { allocation_operations: 2, ..boundary_budget() };
            let work = crate::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(error, _)| error)?;
            let resident_allocation = work.claim_allocation(std::mem::size_of::<[u8; 16]>() as u64, 1)?;
            let resident = Box::new([0_u8; 16]);
            work.charge(crate::WorkCounters { items_examined: 9, ..crate::WorkCounters::UNCHARGED })?;
            let live = work.live_allocation_bytes();
            let remaining = work.remaining()?;
            let failure = {
                let mut invocation = super::super::NativeMountWork::new(&work);
                if subtree {
                    source.capture_host_subtree_with_work(directory.path(), &path, identity, &mut invocation)
                } else {
                    source.capture_host_paths_with_work(
                        directory.path(), std::slice::from_ref(&path), identity, &mut invocation,
                    )
                }
            };
            assert!(matches!(failure, Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Work(_)))));
            assert_eq!(work.remaining()?, remaining);
            assert_eq!(work.receipt().allocation_operations, 2);
            assert_eq!(work.receipt().items_examined, 9);
            assert_eq!(work.receipt().backend_read_operations, 0);
            assert_eq!(work.live_allocation_bytes(), live);
            assert!(work.cancellation().same_state(&cancellation));
            assert!(source.lookup(&path)?.is_none());
            drop(resident);
            drop(resident_allocation);
        }
        Ok(())
    }

    #[test]
    fn borrowed_driver_capture_observer_cancellation_blocks_physical_candidate_install()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let (source, _) =
            shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
        let directory = tempfile::tempdir()?;
        let path = native_test_path("file");
        std::fs::write(directory.path().join("file"), b"host-after")?;
        source.create_file(&path, metadata())?;
        source.write_range(&path, 0, Bytes::from_static(b"before"))?;
        let file_id = source.lookup(&path)?.ok_or("missing original file")?.node.file_id;
        let revision = source.checkout.revision();
        let stamp = source.view_stamp().ok_or("original source stamp")?;
        let identity = capture_root_identity(directory.path())?;
        let cancellation = CancellationToken::new();
        struct CancelDriverDuringRecord {
            original_driver: CancellationToken,
            dispatched: AtomicBool,
        }
        impl ViewObserver for CancelDriverDuringRecord {
            fn view_changed(&self, _position: ViewStamp, _origin: ViewOrigin) {
                if !self.dispatched.swap(true, Ordering::AcqRel) {
                    assert!(!self.original_driver.is_cancelled());
                    self.original_driver.cancel();
                }
            }
        }
        let observer = Arc::new(CancelDriverDuringRecord {
            original_driver: cancellation.clone(), dispatched: AtomicBool::new(false),
        });
        let weak: Weak<dyn ViewObserver> = Arc::downgrade(&observer) as Weak<dyn ViewObserver>;
        source.observe_view(weak)?;
        let work = crate::NativeMaterializationWork::new(boundary_budget(), &cancellation)
            .map_err(|(error, _)| error)?;
        let error = source.capture_host_paths_with_work(
            directory.path(), std::slice::from_ref(&path), identity,
            &mut super::super::NativeMountWork::new(&work),
        ).expect_err("real ledger callback cancelled the original driver before physical adoption");
        assert!(matches!(error.semantic_error(), MountSourceError::Cancelled));
        assert!(observer.dispatched.load(Ordering::Acquire));
        assert!(work.cancellation().same_state(&cancellation));
        assert!(cancellation.is_cancelled());
        assert!(!source.cancellation().is_cancelled());
        assert_eq!(source.checkout.revision(), revision);
        assert_eq!(source.lookup(&path)?.ok_or("original file was lost")?.node.file_id, file_id);
        assert_eq!(source.read_range(&path, 0, 6)?.as_ref(), b"before");
        assert!(!source.unchanged_since(&path, Some(file_id), stamp));
        assert!(work.receipt().source_entries_visited > 0);
        assert!(work.receipt().source_bytes_read >= 10);
        assert!(work.receipt().backend_read_operations > 0);
        assert_eq!(work.receipt().authority_records_appended, 0);
        work.receipt().verify(boundary_budget())?;
        assert!(source.capture_gate.lock().unwrap_or_else(PoisonError::into_inner).active.is_empty());
        let current_driver = CancellationToken::new();
        let retry = crate::NativeMaterializationWork::new(boundary_budget(), &current_driver)
            .map_err(|(error, _)| error)?;
        source.capture_host_paths_with_work(
            directory.path(), std::slice::from_ref(&path), identity,
            &mut super::super::NativeMountWork::new(&retry),
        )?;
        assert!(source.checkout.revision() > revision);
        assert_eq!(source.read_range(&path, 0, 10)?.as_ref(), b"host-after");
        assert_eq!(std::fs::read(directory.path().join("file"))?, b"host-after");
        assert_eq!(retry.receipt().authority_records_appended, 0);
        assert!(retry.cancellation().same_state(&current_driver));
        assert!(source.capture_gate.lock().unwrap_or_else(PoisonError::into_inner).active.is_empty());
        Ok(())
    }

    #[test]
    fn borrowed_driver_capture_cancellation_never_replaces_original_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let (source, _) =
            shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
        let directory = tempfile::tempdir()?;
        std::fs::write(directory.path().join("file"), b"not imported")?;
        let identity = capture_root_identity(directory.path())?;
        let path = native_test_path("file");
        let cancellation = CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(boundary_budget(), &cancellation)
            .map_err(|(error, _)| error)?;
        work.charge(crate::WorkCounters {
            items_examined: 11, backend_read_operations: 2, ..crate::WorkCounters::UNCHARGED
        })?;
        let prefix = work.receipt();
        cancellation.cancel();
        let error = source.capture_host_paths_with_work(
            directory.path(), std::slice::from_ref(&path), identity,
            &mut super::super::NativeMountWork::new(&work),
        ).expect_err("original cancellation precedes every host boundary");
        assert!(matches!(error, MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled)));
        assert_eq!(work.receipt(), prefix);
        assert!(work.cancellation().same_state(&cancellation));
        assert!(source.lookup(&path)?.is_none());
        Ok(())
    }

    #[test]
    fn capture_input_scratch_ignores_another_roots_retired_backing()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &cancellation);
        work.allocate(std::mem::size_of::<[u8; 64]>() as u64)?;
        let old_root = Box::new([0_u8; 64]);
        {
            let mut scratch = CaptureScratchAllocation::new(&mut work);
            let owned = scratch.vec::<u8>(16)?;
            drop(old_root);
            scratch.work.release(std::mem::size_of::<[u8; 64]>() as u64)?;
            drop(owned);
        }
        assert_eq!(work.live_allocation_bytes(), 0);
        assert_eq!(work.work.allocation_operations, 2);
        assert_eq!(work.work.peak_allocation_bytes, 80);
        assert!(work.cancellation().same_state(&cancellation));
        Ok(())
    }

    #[test]
    fn measured_native_namespace_inputs_release_every_denied_owned_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let (source, _) =
            shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
        let path = native_test_path("first").child(native_test_path("second").components()[0].clone());
        for allocations in 1..8 {
            let cancellation = CancellationToken::new();
            let budget = WorkBudget { allocation_operations: allocations, ..boundary_budget() };
            let mut work = crate::demand::DemandWork::new(budget, &cancellation);
            work.allocate(std::mem::size_of::<[u8; 8]>() as u64)?;
            let resident = Box::new([0_u8; 8]);
            work.record(crate::WorkCounters { items_examined: 5, ..crate::WorkCounters::UNCHARGED })?;
            match source.namespace_path_with_work(&path, &mut work) {
                Ok(namespace) => {
                    let bytes = namespace.retained_bytes();
                    drop(namespace);
                    work.release(bytes)?;
                }
                Err(error) => assert!(matches!(error, MountSourceError::Work(_))),
            }
            assert_eq!(work.live_allocation_bytes(), std::mem::size_of_val(resident.as_ref()) as u64);
            assert_eq!(work.budget, budget);
            assert!(work.work.items_examined >= 5);
            assert_eq!(work.work.backend_read_operations, 0);
            assert!(work.cancellation().same_state(&cancellation));
            drop(resident);
            work.release(std::mem::size_of::<[u8; 8]>() as u64)?;
        }
        Ok(())
    }

    #[test]
    fn borrowed_source_readers_preserve_original_cancelled_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let (source, _) =
            shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
        let path = native_test_path("unread");
        let cancellation = CancellationToken::new();
        let budget = boundary_budget();
        let work = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let resident_allocation = work.claim_allocation(16, 1)?;
        let resident = Box::new([0_u8; 16]);
        work.charge(crate::WorkCounters {
            items_examined: 19, backend_read_operations: 2, ..crate::WorkCounters::UNCHARGED
        })?;
        let prefix = work.receipt();
        let live = work.live_allocation_bytes();
        cancellation.cancel();
        let mut invocation = super::super::NativeMountWork { work: &work };
        assert!(matches!(source.lookup_pinned_with_work(&path, &mut invocation), Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled))));
        assert!(matches!(source.read_link_with_work(&path, &mut invocation), Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled))));
        assert!(matches!(source.read_range_with_work(&path, 0, 8, &mut invocation), Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled))));
        assert!(matches!(source.read_directory_with_work(&MountPath::root(), None, 1, &mut invocation), Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled))));
        let mut delivered = 0;
        assert!(matches!(source.read_pinned_with_work(
            &path, super::super::MountContentPin([7; 32]), 0, 8, 4,
            &mut |_, _| { delivered += 1; Ok(()) }, &mut invocation,
        ), Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled))));
        assert_eq!(delivered, 0);
        assert_eq!(work.receipt(), prefix);
        assert_eq!(work.live_allocation_bytes(), live);
        work.receipt().verify(budget)?;
        assert!(work.cancellation().same_state(&cancellation));
        drop(resident);
        drop(resident_allocation);
        Ok(())
    }

    #[test]
    fn shared_source_readers_cancel_on_the_original_arc_without_a_loan()
    -> Result<(), Box<dyn std::error::Error>> {
        let (source, _) =
            shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
        let path = native_test_path("unread");
        let cancellation = CancellationToken::new();
        let budget = WorkBudget { allocation_operations: 0, ..boundary_budget() };
        let mut work = crate::demand::DemandWork::new(budget, &cancellation);
        work.record(crate::WorkCounters {
            items_examined: 29, backend_read_operations: 3, ..crate::WorkCounters::UNCHARGED
        })?;
        let prefix = work.work;
        let meter = Arc::new(crate::demand::NativeDemandReadMeter(StdMutex::new(work)));
        let invocation = super::super::NativeMountMeterWork::from_meter(Arc::clone(&meter));
        cancellation.cancel();
        assert!(matches!(source.lookup_pinned_with_meter(&path, &invocation), Err(MountSourceError::Cancelled)));
        assert!(matches!(source.read_link_with_meter(&path, &invocation), Err(MountSourceError::Cancelled)));
        assert!(matches!(source.read_range_with_meter(&path, 0, 8, &invocation), Err(MountSourceError::Cancelled)));
        assert!(matches!(source.read_directory_with_meter(&MountPath::root(), None, 1, &invocation), Err(MountSourceError::Cancelled)));
        let mut delivered = 0;
        assert!(matches!(source.read_pinned_with_meter(
            &path, super::super::MountContentPin([7; 32]), 0, 8, 4,
            &mut |_, _| { delivered += 1; Ok(()) }, &invocation,
        ), Err(MountSourceError::Cancelled)));
        assert_eq!(delivered, 0);
        let original = meter.lock();
        assert_eq!(original.work, prefix);
        assert_eq!(original.budget, budget);
        assert!(original.cancellation().same_state(&cancellation));
        drop(original);
        assert_eq!(Arc::strong_count(&meter), 2, "no scoped SDK meter alias survives cancellation");
        Ok(())
    }

    #[test]
    fn borrowed_source_readers_use_actual_checkout_content_and_owned_pages()
    -> Result<(), Box<dyn std::error::Error>> {
        let (source, _) =
            shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
        let path = native_test_path("actual");
        source.create_file(&path, metadata())?;
        source.write_range(&path, 0, Bytes::from_static(b"content"))?;
        let link = native_test_path("link");
        source.create_symbolic_link(&link, Bytes::from_static(b"actual"), metadata())?;
        let cancellation = CancellationToken::new();
        let budget = boundary_budget();
        let work = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let resident_allocation = work.claim_allocation(16, 1)?;
        let resident = Box::new([0_u8; 16]);
        work.charge(crate::WorkCounters { items_examined: 23, ..crate::WorkCounters::UNCHARGED })?;
        let retained_prefix = work.live_allocation_bytes();
        let remaining_prefix = work.remaining()?;
        let lookup = source.lookup_pinned_with_work(
            &path, &mut super::super::NativeMountWork { work: &work },
        )?.ok_or("actual file disappeared")?;
        assert!(lookup.1.is_none(), "canonical checkout content has no external pin");
        let bytes = source.read_range_with_work(
            &path, 1, 3, &mut super::super::NativeMountWork { work: &work },
        )?;
        assert_eq!(bytes.bytes.as_ref(), b"ont");
        assert!(bytes.logical_owned_bytes >= 3);
        let target = source.read_link_with_work(
            &link, &mut super::super::NativeMountWork { work: &work },
        )?;
        assert_eq!(target.bytes.as_ref(), b"actual");
        assert!(target.logical_owned_bytes >= 6);
        let first = source.read_directory_with_work(
            &MountPath::root(), None, 1, &mut super::super::NativeMountWork { work: &work },
        )?;
        assert_eq!(first.entries().len(), 1);
        assert_eq!(first.entries()[0].name, path.components()[0]);
        let cursor = first.next_cursor().ok_or("owned continuation missing")?;
        let second = source.read_directory_with_work(
            &MountPath::root(), Some(cursor), 1, &mut super::super::NativeMountWork { work: &work },
        )?;
        assert_eq!(second.entries().len(), 1);
        assert_eq!(second.entries()[0].name, link.components()[0]);
        assert!(second.next_cursor().is_none());
        let mut delivered = 0;
        assert!(matches!(source.read_pinned_with_work(
            &path, super::super::MountContentPin([7; 32]), 0, 7, 2,
            &mut |_, _| { delivered += 1; Ok(()) },
            &mut super::super::NativeMountWork { work: &work },
        ), Err(MountSourceError::Stale)));
        assert_eq!(delivered, 0);
        assert!(work.receipt().items_examined > 23);
        let remaining = work.remaining()?;
        assert!(remaining.items_examined < remaining_prefix.items_examined);
        assert!(remaining.backend_read_operations < remaining_prefix.backend_read_operations);
        assert!(remaining.peak_allocation_bytes < remaining_prefix.peak_allocation_bytes);
        let bytes_capacity = bytes.logical_owned_bytes;
        let target_capacity = target.logical_owned_bytes;
        let page_capacities = [first.logical_owned_bytes()?, second.logical_owned_bytes()?];
        let verify_owned_page = |page: MountDirectoryPage, expected: u64| {
            let (entries, next_cursor, ownership) = page.into_parts();
            let allocation = ownership.into_native_allocation().expect("native page retains intrinsic custody");
            assert_eq!(allocation.bytes(), expected);
            MountDirectoryPage::new(entries, next_cursor, Some(allocation))
        };
        let first = verify_owned_page(first, page_capacities[0]);
        let second = verify_owned_page(second, page_capacities[1]);
        let page_capacity = page_capacities.iter().sum::<u64>();
        let retained = work.live_allocation_bytes();
        assert!(retained >= retained_prefix + bytes_capacity + target_capacity + page_capacity);
        work.receipt().verify(budget)?;
        assert!(work.cancellation().same_state(&cancellation));
        drop(bytes);
        drop(target);
        drop(first);
        drop(second);
        assert_eq!(work.live_allocation_bytes(), retained - bytes_capacity - target_capacity - page_capacity);
        drop(resident);
        drop(resident_allocation);
        Ok(())
    }

    #[test]
    fn native_driver_lookup_name_failure_preserves_original_work()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let (source, _) =
            shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
        let path = MountPath::root().child(if cfg!(windows) { vec![0, 0xd8] } else { vec![0xff] });
        let cancellation = CancellationToken::new();
        let budget = boundary_budget();
        let work = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let live = work.live_allocation_bytes();
        work.charge(crate::WorkCounters { items_examined: 8, ..crate::WorkCounters::UNCHARGED })?;
        let error = source.lookup_with_work(
            &path, &mut super::super::NativeMountWork::new(&work),
        ).expect_err("canonical native name conversion must reject an opaque portable name");
        assert!(matches!(error, MountSourceError::NativeStaticText {
            kind: super::super::NativeSourceTextKind::Unsupported | super::super::NativeSourceTextKind::Invalid,
            ..
        }));
        assert_eq!(work.native_read_meter().lock().budget, budget);
        assert!(work.receipt().items_examined >= 8);
        assert!(work.receipt().allocation_operations > 0);
        assert_eq!(work.receipt().backend_read_operations, 0);
        assert_eq!(work.live_allocation_bytes(), live);
        assert!(work.cancellation().same_state(&cancellation));
        let prefix = work.receipt();
        cancellation.cancel();
        assert!(matches!(source.lookup_with_work(
            &path, &mut super::super::NativeMountWork::new(&work),
        ), Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled))));
        assert_eq!(work.receipt(), prefix);
        assert!(work.cancellation().same_state(&cancellation));
        Ok(())
    }

    #[test]
    fn native_driver_lookup_keeps_original_live_ownership_and_error_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let (source, _) =
            shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
        let cancellation = CancellationToken::new();
        let budget = WorkBudget {
            allocation_operations: 8_192, items_examined: 65_536,
            peak_allocation_bytes: 1_048_576, backend_read_operations: 128,
            backend_read_bytes: 1_048_576, bytes_copied: 1_048_576,
            ..boundary_budget()
        };
        let work = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let resident_allocation = work.claim(std::mem::size_of::<[u8; 64]>() as u64)?;
        let resident = Box::new([0_u8; 64]);
        let initial_live = work.live_allocation_bytes();
        work.charge(crate::WorkCounters { items_examined: 9, ..crate::WorkCounters::UNCHARGED })?;
        let root = source.lookup_with_work(
            &MountPath::root(), &mut super::super::NativeMountWork::new(&work),
        )?.expect("real checkout root");
        assert_eq!(root.node.kind, MountNodeKind::Directory);
        assert_eq!(work.native_read_meter().lock().budget, budget);
        assert!(work.cancellation().same_state(&cancellation));
        assert!(work.receipt().items_examined > 9);
        assert!(work.receipt().peak_allocation_bytes <= budget.peak_allocation_bytes);
        assert!(work.live_allocation_bytes() >= initial_live);
        let prefix = work.receipt();
        let live = work.live_allocation_bytes();
        cancellation.cancel();
        assert!(matches!(source.lookup_with_work(
            &MountPath::root(), &mut super::super::NativeMountWork::new(&work),
        ), Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled))));
        assert_eq!(work.receipt(), prefix);
        assert_eq!(work.live_allocation_bytes(), live);
        drop(resident);
        drop(resident_allocation);
        assert_eq!(work.live_allocation_bytes(), live - std::mem::size_of::<[u8; 64]>() as u64);
        Ok(())
    }

    #[test]
    fn shared_driver_lookup_retains_original_ledger_and_resource_view()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let (source, _) =
            shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
        let cancellation = CancellationToken::new();
        let budget = WorkBudget {
            allocation_operations: 8_192, items_examined: 65_536,
            peak_allocation_bytes: 1_048_576, backend_read_operations: 128,
            backend_read_bytes: 1_048_576, bytes_copied: 1_048_576,
            ..boundary_budget()
        };
        let original = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let work = original.native_volume_work(budget)?;
        work.charge(crate::WorkCounters { items_examined: 11, ..crate::WorkCounters::UNCHARGED })?;
        let invocation = work.native_mount_work();
        assert!(invocation.native_materialization_work().shares_resource_allowance(&work));
        let root = source.lookup_with_meter(&MountPath::root(), &invocation)?
            .expect("real checkout root");
        assert_eq!(root.node.kind, MountNodeKind::Directory);
        assert!(Arc::ptr_eq(invocation.meter(), original.native_read_meter()));
        assert!(invocation.native_materialization_work().shares_resource_allowance(&work));
        assert!(work.receipt().items_examined > 11);
        assert!(work.receipt().peak_allocation_bytes <= budget.peak_allocation_bytes);
        let prefix = work.receipt();
        let live = work.live_allocation_bytes();
        cancellation.cancel();
        assert!(matches!(source.lookup_with_meter(
            &MountPath::root(), &invocation,
        ), Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled))));
        assert_eq!(work.receipt(), prefix);
        assert_eq!(work.live_allocation_bytes(), live);
        assert_eq!(work.native_read_meter().lock().budget, budget);
        assert!(work.cancellation().same_state(&cancellation));
        Ok(())
    }

    #[test]
    fn borrowed_driver_capture_success_keeps_original_live_budget_and_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        for subtree in [false, true] {
            let (source, _) =
                shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
            let directory = tempfile::tempdir()?;
            std::fs::write(directory.path().join("file"), b"actual")?;
            let identity = capture_root_identity(directory.path())?;
            let path = native_test_path("file");
            let cancellation = CancellationToken::new();
            let budget = boundary_budget();
            let work = crate::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(error, _)| error)?;
            let resident_allocation = work.claim_allocation(std::mem::size_of::<[u8; 64]>() as u64, 1)?;
            let resident = Box::new([0_u8; 64]);
            work.charge(crate::WorkCounters { items_examined: 13, ..crate::WorkCounters::UNCHARGED })?;
            let remaining = work.remaining()?;
            let live = work.live_allocation_bytes();
            {
                let mut invocation = super::super::NativeMountWork::new(&work);
                if subtree {
                    source.capture_host_subtree_with_work(
                        directory.path(), &MountPath::root(), identity, &mut invocation,
                    )?;
                } else {
                    source.capture_host_paths_with_work(
                        directory.path(), std::slice::from_ref(&path), identity, &mut invocation,
                    )?;
                }
            }
            assert_eq!(source.read_range(&path, 0, 6)?.as_ref(), b"actual");
            assert!(work.remaining()?.items_examined < remaining.items_examined);
            assert!(work.receipt().items_examined > 13);
            assert!(work.receipt().backend_read_operations > 0);
            assert!(work.receipt().source_entries_visited > 0);
            assert!(work.receipt().source_bytes_read >= 6);
            assert!(work.live_allocation_bytes() >= live);
            assert!(work.cancellation().same_state(&cancellation));
            work.receipt().verify(budget)?;
            drop(resident);
            drop(resident_allocation);
        }
        Ok(())
    }

    #[test]
    fn finite_native_name_decode_failure_retains_preworker_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let (source, _) =
            shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
        let source = Arc::new(source);
        let directory = tempfile::tempdir()?;
        let identity = capture_root_identity(directory.path())?;
        let path = MountPath::root().child(if cfg!(windows) {
            vec![0, 0xd8]
        } else {
            vec![0xff]
        });
        let cancellation = CancellationToken::new();
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        let failure = runtime.block_on(source.capture_host_with_identity_budgeted(
            directory.path(), &[path], identity, HostCaptureScope::ExactPaths,
            Arc::new(NativeViewBaseline::new(identity)), boundary_budget(), &cancellation,
        )).expect_err("portable capture must reject malformed native code units");
        assert!(matches!(failure.error, MountSourceError::Unsupported(_)));
        assert_eq!(failure.work.source_path_components, 1);
        assert!(failure.work.allocation_operations > 1);
        assert!(failure.work.items_examined > 0);
        assert_eq!(failure.work.backend_read_operations, 0);
        assert!(source.capture_gate.lock().unwrap_or_else(PoisonError::into_inner).active.is_empty());
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn finite_subtree_capture_retains_physical_name_decode_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let (source, _) =
            shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
        let source = Arc::new(source);
        let directory = tempfile::tempdir()?;
        let bad_name = std::ffi::OsString::from_vec(vec![b'b', 0xff]);
        std::fs::write(directory.path().join(bad_name), b"physical")?;
        let identity = capture_root_identity(directory.path())?;
        let cancellation = CancellationToken::new();
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        let failure = runtime.block_on(source.capture_host_with_identity_budgeted(
            directory.path(), &[MountPath::root()], identity, HostCaptureScope::Subtree,
            Arc::new(NativeViewBaseline::new(identity)), boundary_budget(), &cancellation,
        )).expect_err("portable physical traversal must reject opaque host bytes");
        assert!(failure.work.backend_read_operations > 0);
        assert!(failure.work.allocation_operations > 1);
        assert!(failure.work.items_examined > 0);
        assert!(source.capture_gate.lock().unwrap_or_else(PoisonError::into_inner).active.is_empty());
        assert!(!source.runtime.block_on(|| async { source.checkout.lock().await.has_pending_mutations() }));
        Ok(())
    }

    #[test]
    fn finite_capture_cancellation_quiesces_worker_and_retires_owned_registration()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let (source, _) =
            shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
        let source = Arc::new(source);
        let directory = tempfile::tempdir()?;
        std::fs::write(directory.path().join("file"), b"actual")?;
        let identity = capture_root_identity(directory.path())?;
        let cancellation = CancellationToken::new();
        let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build()?;
        runtime.block_on(async {
            let held = source.checkout.lock().await;
            let revision = source.checkout.revision();
            let worker = Arc::clone(&source);
            let root = directory.path().to_path_buf();
            let caller = cancellation.clone();
            let capture = tokio::spawn(async move {
                worker.capture_host_with_identity_budgeted(
                    &root, &[native_test_path("file")], identity, HostCaptureScope::ExactPaths,
                    Arc::new(NativeViewBaseline::new(identity)), boundary_budget(), &caller,
                ).await
            });
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if !source.capture_gate.lock().unwrap_or_else(PoisonError::into_inner).active.is_empty() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }).await?;
            cancellation.cancel();
            let failure = tokio::time::timeout(Duration::from_secs(5), capture).await??
                .expect_err("cancelled queued capture must retain its prefix");
            assert!(matches!(failure.error, MountSourceError::Cancelled));
            assert!(failure.work.allocation_operations > 1);
            assert!(failure.work.items_examined > 0);
            assert_eq!(failure.work.source_entries_visited, 0);
            let gate = source.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
            assert!(gate.active.is_empty());
            assert_eq!(gate.active.capacity(), 0);
            assert!(gate.allocation.is_none());
            assert!(gate.native_allocation.is_none());
            drop(gate);
            assert_eq!(source.checkout.revision(), revision);
            drop(held);
            assert!(!source.checkout.lock().await.has_pending_mutations());
            Ok::<_, Box<dyn std::error::Error>>(())
        })?;
        Ok(())
    }

    #[test]
    fn dropping_active_capture_fences_its_detached_worker() -> Result<(), Box<dyn std::error::Error>>
    {
        acyclic_native_runtime::initialize_native_io_services()?;
        let (source, _) =
            shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
        let source = Arc::new(source);
        let directory = tempfile::tempdir()?;
        std::fs::write(directory.path().join("file"), b"child")?;
        let identity = capture_root_identity(directory.path())?;
        let baseline = Arc::new(NativeViewBaseline::new(identity));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let held = source.checkout.lock().await;
            let revision = source.checkout.revision();
            let worker = Arc::clone(&source);
            let root = directory.path().to_path_buf();
            let capture = tokio::spawn(async move {
                worker
                    .capture_host_with_identity_budgeted(
                        &root,
                        &[native_test_path("file")],
                        identity,
                        HostCaptureScope::ExactPaths,
                        baseline,
                        boundary_budget(),
                        worker.cancellation(),
                    )
                    .await
            });
            let request = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if let Some(request) = source
                        .capture_gate
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .active
                        .last()
                        .map(|slot| slot.request.clone())
                    {
                        break request;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await?;
            capture.abort();
            let _ = capture.await;
            drop(held);
            tokio::time::timeout(Duration::from_secs(5), async {
                while request.strong_count() > 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await?;
            assert_eq!(source.checkout.revision(), revision);
            assert!(!source.checkout.lock().await.has_pending_mutations());
            Ok::<_, Box<dyn std::error::Error>>(())
        })?;
        Ok(())
    }

    #[test]
    fn batched_capture_budget_is_cumulative_across_paths() -> Result<(), Box<dyn std::error::Error>>
    {
        acyclic_native_runtime::initialize_native_io_services()?;
        let source = source(FilesystemProfile::Portable)?;
        let directory = tempfile::tempdir()?;
        std::fs::write(directory.path().join("first"), b"first")?;
        std::fs::write(directory.path().join("second"), b"other")?;
        let first = source.path(&native_test_path("first"))?;
        let second = source.path(&native_test_path("second"))?;
        let options = CaptureOptions {
            source_root: directory.path().to_path_buf(),
            expected_root_identity: capture_root_identity(directory.path())?,
            maximum_paths: 2,
            maximum_extent_spans: 64,
        };
        let cancellation = CancellationToken::new();
        let mut zero_candidate = source
            .runtime
            .block_on(|| async { source.checkout.lock().await.private_candidate() });
        let zero_result = source.runtime.block_on(|| {
            capture_paths_batched_with_baseline(
                &mut zero_candidate,
                std::slice::from_ref(&first),
                &options,
                1,
                WorkBudget::default(),
                &cancellation,
                None,
            )
        });
        let Err(zero_failure) = zero_result else {
            return Err(std::io::Error::other("zero budget admitted host observation").into());
        };
        assert_eq!(zero_failure.work.source_entries_visited, 0);
        assert!(!zero_candidate.has_pending_mutations());
        let mut candidate = source
            .runtime
            .block_on(|| async { source.checkout.lock().await.private_candidate() });
        let single = source.runtime.block_on(|| {
            capture_paths_batched_with_baseline(
                &mut candidate,
                std::slice::from_ref(&first),
                &options,
                1,
                WorkBudget::UNBOUNDED,
                &cancellation,
                None,
            )
        })?;
        assert!(single.work.source_bytes_read > 0);
        let mut budget = WorkBudget::UNBOUNDED;
        budget.source_bytes_read = single.work.source_bytes_read;
        let mut candidate = source
            .runtime
            .block_on(|| async { source.checkout.lock().await.private_candidate() });
        let paths = [first, second];
        let result = source.runtime.block_on(|| {
            capture_paths_batched_with_baseline(
                &mut candidate,
                &paths,
                &options,
                1,
                budget,
                &cancellation,
                None,
            )
        });
        let Err(failure) = result else {
            return Err(std::io::Error::other("aggregate source budget was ignored").into());
        };
        assert!(failure.error.to_string().contains("source_bytes_read"));
        assert!(!candidate.has_pending_mutations());
        Ok(())
    }

    #[test]
    fn callbacks_run_on_their_driver_threads_without_helper_threads()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = CallbackRuntime::create()?;
        let callers = (0..32)
            .map(|index| {
                let runtime = runtime.clone();
                std::thread::Builder::new()
                    .name(format!("native-driver-{index}"))
                    .spawn(move || {
                        runtime.wait(|| async {
                            // A real reactor wait parks the driver thread
                            // until a runtime worker wakes it.
                            tokio::time::sleep(Duration::from_millis(1)).await;
                            Ok::<_, MountSourceError>(
                                std::thread::current().name().unwrap_or_default().to_owned(),
                            )
                        })
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        for (index, caller) in callers.into_iter().enumerate() {
            assert_eq!(
                caller
                    .join()
                    .map_err(|_| std::io::Error::other("native driver thread panicked"))??,
                format!("native-driver-{index}")
            );
        }
        Ok(())
    }

    #[test]
    fn nested_callbacks_reenter_on_the_same_driver_thread() -> Result<(), Box<dyn std::error::Error>>
    {
        let runtime = CallbackRuntime::create()?;
        let nested = runtime.clone();
        let observed = std::thread::Builder::new()
            .name("native-driver".to_owned())
            .spawn(move || {
                runtime.wait(|| async {
                    tokio::task::yield_now().await;
                    nested.wait(|| async {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        Ok(std::thread::current().name().unwrap_or_default().to_owned())
                    })
                })
            })?
            .join()
            .map_err(|_| "native driver callback panicked")??;
        assert_eq!(observed, "native-driver");
        Ok(())
    }

    fn metadata() -> FileMetadata {
        FileMetadata {
            posix_mode: crate::kernel::MetadataField::Unavailable,
            posix_uid: crate::kernel::MetadataField::Unavailable,
            posix_gid: crate::kernel::MetadataField::Unavailable,
            posix_flags: crate::kernel::MetadataField::Unavailable,
            windows_attributes: crate::kernel::MetadataField::Unavailable,
            created_ns: crate::kernel::MetadataField::Unavailable,
            modified_ns: crate::kernel::MetadataField::Unavailable,
            accessed_ns: crate::kernel::MetadataField::Unavailable,
            changed_ns: crate::kernel::MetadataField::Unavailable,
            named_attributes: crate::kernel::MetadataField::Unavailable,
            acl: crate::kernel::MetadataField::Unavailable,
            security_descriptor: crate::kernel::MetadataField::Unavailable,
        }
    }

    #[test]
    fn native_shared_constructor_retains_escaped_gate_and_ledger_until_physical_drop()
    -> Result<(), Box<dyn std::error::Error>> {
        let borrowed = source(FilesystemProfile::Portable)?;
        let cancellation = CancellationToken::new();
        // This old fixture checkout is deliberately borrowed service state,
        // not newly adopted wholesale by the following native invocation.
        let checkout = borrowed.runtime.block_on(|| async {
            let checkout = borrowed.checkout.lock().await;
            checkout.private_candidate_measured(WorkBudget::UNBOUNDED, &cancellation)
                .map(|receipt| receipt.value).map_err(|failure| failure.error)
        })?;
        let original = crate::NativeMaterializationWork::new(boundary_budget(), &cancellation)
            .map_err(|(cause, _)| cause)?;
        let baseline = original.receipt().retained_allocation_bytes;
        let prefix = original.receipt().allocation_operations;
        let shared = SharedCheckout::with_publication_meter(
            checkout, MountPublication::CloseAndSync, &original.native_mount_work(),
        )?;
        let gate = Arc::clone(&shared.view_gate);
        let ledger = Arc::clone(&shared.ledger);
        let weak = Arc::downgrade(&shared);
        assert_eq!(original.receipt().allocation_operations, prefix + 4);
        drop(shared);
        assert!(weak.upgrade().is_none());
        assert_eq!(original.receipt().retained_allocation_bytes,
            baseline + std::mem::size_of::<ViewGate>() as u64 + std::mem::size_of::<ViewLedger>() as u64);
        drop(ledger);
        assert_eq!(original.receipt().retained_allocation_bytes,
            baseline + std::mem::size_of::<ViewGate>() as u64);
        drop(gate);
        assert_eq!(original.receipt().retained_allocation_bytes, baseline);
        drop(weak);
        Ok(())
    }

    #[test]
    fn native_source_intrinsic_guards_follow_self_and_cancellation_aliases()
    -> Result<(), Box<dyn std::error::Error>> {
        let borrowed = source(FilesystemProfile::Portable)?;
        assert!(borrowed.native_original_work()?.is_none());
        let config = VolumeConfig::portable(Lifecycle::Ephemeral);
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(boundary_budget(), &cancellation)
            .map_err(|(cause, _)| cause)?;
        let baseline = original.receipt().retained_allocation_bytes;
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
        let (root, allocation) = NamespacePath::parse_in_profile_with_accounting(
            "/branch", config.profile, config.limits, &mut accounting, &cancellation,
        )?;
        let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) = allocation
            else { panic!("native namespace must retain its original guard"); };
        assert_eq!(allocation.bytes(), root.retained_bytes());
        let native = original.native_mount_work();
        let source = CheckoutMountSource::new_at_meter(
            Arc::clone(&borrowed.checkout), config, root, allocation, &native, Some(&original),
        )?;
        let observed_cancellation = source.cancellation.clone();
        let cancellation_bytes = observed_cancellation.logical_owned_bytes();
        let source = source.into_arc_meter(&native)?;
        let before_binding = original.receipt();
        let bound = source.native_original_work()?.expect("native source retains its actual header");
        let bound = bound.native_materialization_work();
        assert!(bound.shares_ledger(&original));
        assert!(bound.shares_resource_allowance(&original));
        assert_eq!(original.receipt(), before_binding);
        drop(bound);
        observed_cancellation.cancel();
        let before_revoked_binding = original.receipt();
        assert!(matches!(source.native_original_work(), Err(MountSourceError::Stale)));
        assert_eq!(original.receipt(), before_revoked_binding);
        let weak = Arc::downgrade(&source);
        assert!(original.receipt().retained_allocation_bytes > baseline + cancellation_bytes);
        drop(source);
        assert!(weak.upgrade().is_none());
        // Opaque Weak backing is not a live SDK Self, whereas the real
        // cancellation state is still physically owned by its token clone.
        assert_eq!(original.receipt().retained_allocation_bytes, baseline + cancellation_bytes);
        drop(observed_cancellation);
        assert_eq!(original.receipt().retained_allocation_bytes, baseline);
        drop(weak);
        Ok(())
    }

    #[test]
    fn native_source_birth_credit_retires_after_original_root_stop()
    -> Result<(), Box<dyn std::error::Error>> {
        let borrowed = source(FilesystemProfile::Portable)?;
        let config = VolumeConfig::portable(Lifecycle::Ephemeral);
        let root_stop = CancellationToken::new();
        let root = crate::NativeMaterializationWork::new(boundary_budget(), &root_stop)
            .map_err(|(cause, _)| cause)?;
        let original = root.native_volume_work(WorkBudget { items_examined: 2, ..boundary_budget() })?;
        let source = CheckoutMountSource::new_at_meter(
            Arc::clone(&borrowed.checkout), config, NamespacePath::root(),
            original.claim_allocation(0, 0)?, &original.native_mount_work(), Some(&original),
        )?;
        assert_eq!(original.remaining()?.items_examined, 0);
        let before = root.receipt();
        root_stop.cancel();
        assert!(original.reserve_items(1).is_err(), "cleanup does not issue fresh work");
        let wait = CancellationToken::new();
        assert!(source.shutdown_with_cleanup_wait(&wait)?);
        assert!(source.capture_retired());
        let completed = root.receipt();
        assert_eq!(completed.items_examined, before.items_examined + 2);
        assert_eq!(completed.allocation_operations, before.allocation_operations);
        wait.cancel();
        assert!(!source.shutdown_with_cleanup_wait(&wait)?);
        assert_eq!(root.receipt(), completed, "already terminal needs no wait or admission");
        Ok(())
    }

    #[test]
    fn native_source_stopped_cleanup_wait_preserves_original_credit()
    -> Result<(), Box<dyn std::error::Error>> {
        let borrowed = source(FilesystemProfile::Portable)?;
        let root_stop = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(
            WorkBudget { items_examined: 2, ..boundary_budget() }, &root_stop,
        ).map_err(|(cause, _)| cause)?;
        let source = CheckoutMountSource::new_at_meter(
            Arc::clone(&borrowed.checkout), VolumeConfig::portable(Lifecycle::Ephemeral),
            NamespacePath::root(), original.claim_allocation(0, 0)?, &original.native_mount_work(), Some(&original),
        )?;
        let wait = CancellationToken::new();
        wait.cancel();
        let before = original.receipt();
        assert!(matches!(source.shutdown_with_cleanup_wait(&wait), Err(MountSourceError::Cancelled)));
        assert_eq!(original.receipt(), before);
        assert!(!source.capture_gate.lock().unwrap_or_else(PoisonError::into_inner).cancelled);
        root_stop.cancel();
        source.shutdown_with_cleanup_wait(&CancellationToken::new())?;
        assert_eq!(original.receipt().items_examined, before.items_examined + 2);
        Ok(())
    }

    #[test]
    fn native_source_funded_tombstones_visit_each_original_slot_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let borrowed = source(FilesystemProfile::Portable)?;
        let root_stop = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(
            WorkBudget { items_examined: 5, ..boundary_budget() }, &root_stop,
        ).map_err(|(cause, _)| cause)?;
        let source = CheckoutMountSource::new_at_meter(
            Arc::clone(&borrowed.checkout), VolumeConfig::portable(Lifecycle::Ephemeral),
            NamespacePath::root(), original.claim_allocation(0, 0)?, &original.native_mount_work(), Some(&original),
        )?;
        let mut scratch = NativeCaptureScratchAllocation::new(&original)?;
        let mut slots = scratch.vec::<CaptureSourceSlot>(3)?;
        for _ in 0..3 {
            slots.push(CaptureSourceSlot { request: Weak::new(), cancel_visit: Some(original.reserve_items(1)?) });
        }
        {
            let mut gate = source.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
            gate.active = slots;
            gate.native_allocation = Some(scratch.retain());
        }
        let before = original.receipt();
        root_stop.cancel();
        source.shutdown_with_cleanup_wait(&CancellationToken::new())?;
        let completed = original.receipt();
        assert_eq!(completed.items_examined, before.items_examined + 5);
        assert_eq!(completed.allocation_operations, before.allocation_operations);
        assert!(completed.retained_allocation_bytes < before.retained_allocation_bytes);
        let gate = source.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(gate.retired);
        assert!(gate.active.is_empty());
        assert_eq!(gate.active.capacity(), 0);
        assert!(gate.native_allocation.is_none());
        Ok(())
    }

    fn native_source_io_control(
        borrowed: &MemorySource, original: &crate::NativeMaterializationWork,
    ) -> Result<Arc<MemorySource>, Box<dyn std::error::Error>> {
        let native = original.native_mount_work();
        Ok(CheckoutMountSource::new_at_meter(
            Arc::clone(&borrowed.checkout), VolumeConfig::portable(Lifecycle::Ephemeral),
            NamespacePath::root(), original.claim_allocation(0, 0)?, &native, Some(original),
        )?.into_arc_meter(&native)?)
    }

    #[test]
    fn native_source_io_projection_keeps_original_volume_read_ceiling()
    -> Result<(), Box<dyn std::error::Error>> {
        use acyclic_native_runtime::NativeFile as NativeRuntimeFile;
        acyclic_native_runtime::initialize_native_io_services()?;
        let borrowed = source(FilesystemProfile::Portable)?;
        let fixture = tempfile::tempdir()?;
        let path = fixture.path().join("borrowed-payload");
        std::fs::write(&path, b"original")?;
        let file = std::fs::File::open(&path)?;
        let root_stop = CancellationToken::new();
        let root = crate::NativeMaterializationWork::new(boundary_budget(), &root_stop)
            .map_err(|(cause, _)| cause)?;
        let volume = root.native_volume_work(WorkBudget {
            backend_read_operations: 1, ..boundary_budget()
        })?;
        let source = native_source_io_control(&borrowed, &volume)?;
        let lease = source.native_source_io_lease()?;
        let (incoming, local_stop) = root.invocation_view()?;
        let job = incoming.with_source_io_lease(lease)?;
        assert!(job.shares_ledger(&root));
        assert!(job.shares_resource_allowance(&volume));
        let native_file = NativeRuntimeFile::from_file_measured_owned(file, job.runtime_meter_owner())
            .map_err(|(error, file)| { drop(file); error })?;
        let before = root.receipt();
        let first = borrowed.runtime.block_on(|| {
            native_file.read_bounded_async(0, 8, job.runtime_meter_owner())
                .expect("first read submission")
        })?;
        assert_eq!(first.bytes.as_ref(), b"original");
        let prefix = root.receipt();
        assert_eq!(prefix.backend_read_operations, before.backend_read_operations + 1);
        assert_eq!(prefix.source_bytes_read, before.source_bytes_read + 8);
        assert_eq!(volume.remaining()?.backend_read_operations, 0);
        assert!(root.remaining()?.backend_read_operations > 0, "global spare cannot replenish the Source");
        let second = native_file.read_bounded_async(0, 8, job.runtime_meter_owner())
            .and_then(|completion| borrowed.runtime.block_on(|| completion))
            .err().expect("second read must fail under the original volume ceiling");
        assert!(matches!(job.runtime_error(second), crate::NativeTreeMaterializationError::Work(_)));
        assert_eq!(root.receipt().backend_read_operations, prefix.backend_read_operations);
        assert_eq!(root.receipt().source_bytes_read, prefix.source_bytes_read);
        assert!(!local_stop.is_cancelled());
        assert!(!root_stop.is_cancelled());
        drop(first);
        drop(native_file);
        drop(job);
        source.shutdown_accepted_cleanup()?;
        Ok(())
    }

    #[test]
    fn native_source_io_projection_keeps_current_local_cancellation()
    -> Result<(), Box<dyn std::error::Error>> {
        use acyclic_native_runtime::NativeFile as NativeRuntimeFile;
        acyclic_native_runtime::initialize_native_io_services()?;
        let borrowed = source(FilesystemProfile::Portable)?;
        let fixture = tempfile::tempdir()?;
        let path = fixture.path().join("borrowed-payload");
        std::fs::write(&path, b"original")?;
        let file = std::fs::File::open(&path)?;
        let root_stop = CancellationToken::new();
        let root = crate::NativeMaterializationWork::new(boundary_budget(), &root_stop)
            .map_err(|(cause, _)| cause)?;
        let volume = root.native_volume_work(WorkBudget {
            backend_read_operations: 1, ..boundary_budget()
        })?;
        let source = native_source_io_control(&borrowed, &volume)?;
        let (incoming, local_stop) = root.invocation_view()?;
        let job = incoming.with_source_io_lease(source.native_source_io_lease()?)?;
        let native_file = NativeRuntimeFile::from_file_measured_owned(file, job.runtime_meter_owner())
            .map_err(|(error, file)| { drop(file); error })?;
        local_stop.cancel();
        let prefix = root.receipt();
        let error = native_file.read_bounded_async(0, 8, job.runtime_meter_owner())
            .err().expect("the current invocation stop rejects the native Source job");
        assert!(matches!(job.runtime_error(error), crate::NativeTreeMaterializationError::Cancelled));
        assert_eq!(root.receipt(), prefix, "local refusal precedes request allocation and IO");
        assert!(!root_stop.is_cancelled());
        assert!(!source.cancellation.is_cancelled());
        root.cancellation().check()?;
        drop(native_file);
        drop(job);
        source.shutdown_accepted_cleanup()?;
        Ok(())
    }

    #[test]
    fn native_source_io_detached_data_and_cleanup_do_not_pin_accepted_job()
    -> Result<(), Box<dyn std::error::Error>> {
        use acyclic_native_runtime::{NativeFile as NativeRuntimeFile, ReadAllocation};
        acyclic_native_runtime::initialize_native_io_services()?;
        let borrowed = source(FilesystemProfile::Portable)?;
        let fixture = tempfile::tempdir()?;
        let path = fixture.path().join("borrowed-payload");
        std::fs::write(&path, b"original")?;
        let file = std::fs::File::open(&path)?;
        let root_stop = CancellationToken::new();
        let root = crate::NativeMaterializationWork::new(boundary_budget(), &root_stop)
            .map_err(|(cause, _)| cause)?;
        let volume = root.native_volume_work(WorkBudget {
            backend_read_operations: 1, ..boundary_budget()
        })?;
        let source = native_source_io_control(&borrowed, &volume)?;
        let weak_source = Arc::downgrade(&source);
        let lease = source.native_source_io_lease()?;
        let last_lease = lease.clone();
        let (incoming, local_stop) = root.invocation_view()?;
        let job = incoming.with_source_io_lease(lease)?;
        let meter = job.runtime_meter_owner();
        let data_guard = ReadAllocation::new(&meter, 8)?;
        let data = Box::new(*b"retained");
        let demand = crate::demand::OriginalDemandWork::from_native_materialization_work(&job)
            .allocation_work();
        assert!(demand.source_execution_view().is_none());
        let credit = crate::materializer::NativeOwnedCleanupCredit::from_reserved(
            job.reserve_work(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            })?,
        )?;
        let original_work = credit.original_work().clone();
        assert!(original_work.source_execution_view().is_none());
        assert!(original_work.shares_ledger(&root));
        assert!(original_work.shares_resource_allowance(&volume));
        let native_file = NativeRuntimeFile::from_file_measured_owned(file, meter.clone())
            .map_err(|(error, file)| { drop(file); error })?;
        let result = borrowed.runtime.block_on(|| {
            native_file.read_bounded_async(0, 8, meter.clone()).expect("actual read submission")
        })?;
        assert_eq!(result.bytes.as_ref(), b"original");
        drop(native_file);
        drop(meter);
        drop(job);
        drop(incoming);
        drop(source);
        assert!(weak_source.upgrade().is_some(), "the last real lease still owns the Source");
        drop(last_lease);
        assert!(weak_source.upgrade().is_none(),
            "DATA, ReadAllocation, cleanup credit, original_work and demand keeper are not operative jobs");
        assert_eq!(*data, *b"retained");
        assert_eq!(result.bytes.as_ref(), b"original");
        assert_eq!(demand.counters()?.backend_read_operations, 1);
        assert!(credit.remaining_work().items_examined > 0);
        assert!(!local_stop.is_cancelled());
        assert_eq!(root.receipt().source_bytes_read, 8);
        // Retire physical payloads before their custody guards.
        drop(data);
        drop(data_guard);
        drop(result);
        drop(credit);
        drop(original_work);
        drop(demand);
        Ok(())
    }

    #[test]
    fn native_source_io_projection_rejects_foreign_root_and_resource_before_allocation()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let borrowed = source(FilesystemProfile::Portable)?;
        let cancellation = CancellationToken::new();
        let root = crate::NativeMaterializationWork::new(boundary_budget(), &cancellation)
            .map_err(|(cause, _)| cause)?;
        let volume = root.native_volume_work(boundary_budget())?;
        let source = native_source_io_control(&borrowed, &volume)?;
        let lease = source.native_source_io_lease()?;
        let foreign_root = crate::NativeMaterializationWork::new(boundary_budget(), &cancellation)
            .map_err(|(cause, _)| cause)?;
        let unrelated_volume = root.native_volume_work(boundary_budget())?;
        for incoming in [&foreign_root, &unrelated_volume] {
            let prefix = root.receipt();
            let foreign_prefix = foreign_root.receipt();
            assert!(matches!(incoming.with_source_io_lease(lease.clone()),
                Err(crate::NativeTreeMaterializationError::InvalidLayout)));
            assert_eq!(root.receipt(), prefix, "Root/resource rejection precedes view allocation");
            assert_eq!(foreign_root.receipt(), foreign_prefix);
        }
        assert_eq!(source.capture_gate.lock().unwrap_or_else(PoisonError::into_inner).producers, 1);
        drop(lease);
        source.shutdown_accepted_cleanup()?;
        Ok(())
    }

    #[test]
    fn native_source_io_last_physical_lease_retires_after_poll_bank_exhaustion()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let borrowed = source(FilesystemProfile::Portable)?;
        let root_stop = CancellationToken::new();
        let root = crate::NativeMaterializationWork::new(boundary_budget(), &root_stop)
            .map_err(|(cause, _)| cause)?;
        let original = root.native_volume_work(WorkBudget {
            items_examined: 3, ..boundary_budget()
        })?;
        let source = native_source_io_control(&borrowed, &original)?;
        let weak_source = Arc::downgrade(&source);
        let lease = source.native_source_io_lease()?;
        let last_lease = lease.clone();
        assert_eq!(original.remaining()?.items_examined, 0,
            "constructor two and accepted producer birth poll one are the entire original credit");
        root_stop.cancel();
        let prefix = root.receipt();
        assert!(matches!(source.shutdown_accepted_cleanup(),
            Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Work(_)))));
        assert!(!source.capture_retired());
        assert_eq!(root.receipt().items_examined, prefix.items_examined + 2);
        assert!(source.cleanup_final_credit.lock().unwrap_or_else(PoisonError::into_inner).is_some());
        let closing_prefix = root.receipt();
        assert!(matches!(source.native_source_io_lease(), Err(MountSourceError::Stale)));
        assert_eq!(root.receipt(), closing_prefix, "closing rejects new lease before allocation");
        drop(lease);
        assert_eq!(source.capture_gate.lock().unwrap_or_else(PoisonError::into_inner).producers, 1);
        drop(source);
        let retained_source = weak_source.upgrade().expect("last inner Arc physically retains Source");
        assert!(!retained_source.capture_retired());
        drop(last_lease);
        assert_eq!(retained_source.capture_gate.lock().unwrap_or_else(PoisonError::into_inner).producers, 0);
        assert!(retained_source.shutdown_accepted_cleanup()?,
            "the originally dedicated final predicate completes without fresh poll credit");
        assert!(retained_source.capture_retired());
        assert_eq!(root.receipt().items_examined, prefix.items_examined + 3);
        assert_eq!(root.receipt().allocation_operations, closing_prefix.allocation_operations);
        assert!(retained_source.cleanup_final_credit.lock().unwrap_or_else(PoisonError::into_inner).is_none());
        assert!(!retained_source.shutdown_accepted_cleanup()?);
        drop(retained_source);
        assert!(weak_source.upgrade().is_none());
        Ok(())
    }

    #[test]
    fn native_source_qualified_guard_keeps_primary_and_actual_accounting_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        #[derive(Debug)]
        enum GuardFailure {
            RootPublicationRejected(u64),
            Source(MountSourceError),
        }
        impl From<MountSourceError> for GuardFailure {
            fn from(cause: MountSourceError) -> Self { Self::Source(cause) }
        }

        acyclic_native_runtime::initialize_native_io_services()?;
        let borrowed = source(FilesystemProfile::Portable)?;
        let workspace_id = crate::WorkspaceId::from_volume_id(borrowed.volume_id()?);
        let config = VolumeConfig::portable(Lifecycle::Ephemeral);
        let context = crate::lazy_workspace::NativeSnapshotContext::Authored { workspace_id, config };
        let path = native_test_path("qualified-guard-original");
        let created = borrowed.create_file(&path, metadata())?;
        let file = borrowed.open_created(&path, &created)?;
        let reached = Arc::new(AtomicBool::new(false));
        let observed_prefix = Arc::new(StdMutex::new(None));
        let root_stop = CancellationToken::new();
        let root = crate::NativeMaterializationWork::new(boundary_budget(), &root_stop)
            .map_err(|(cause, _)| cause)?;
        let volume = root.native_volume_work(boundary_budget())?;
        let source = native_source_io_control(&borrowed, &volume)?;
        let (incoming, local_stop) = root.invocation_view()?;
        let job = incoming.with_source_io_lease(source.native_source_io_lease()?)?;
        let native = job.native_mount_work();
        file.write_range_with_meter(0, crate::kernel::OwnedSourceChunk::intrinsic(
            OwnedBytes::from_static(b"native publication prefix"), 0,
        ), &native)?;
        let before = root.receipt();
        let guard_stop = local_stop.clone();
        let guard_reached = Arc::clone(&reached);
        let guard_prefix = Arc::clone(&observed_prefix);
        let expected_meter = Arc::clone(root.native_read_meter());
        let mut before_publication = move |original: &crate::NativeMaterializationWork| {
            assert!(Arc::ptr_eq(original.native_read_meter(), &expected_meter));
            assert!(original.cancellation().same_state(&guard_stop));
            let stop = guard_stop.clone();
            let reached = Arc::clone(&guard_reached);
            let prefix = Arc::clone(&guard_prefix);
            let original = original.allocation_work();
            async move {
                reached.store(true, Ordering::Release);
                *prefix.lock().unwrap_or_else(PoisonError::into_inner) = Some(original.receipt());
                stop.cancel();
                Err::<(), GuardFailure>(GuardFailure::RootPublicationRejected(0x51_0c))
            }
        };
        let mut acknowledged = None;
        let failure = source.runtime.block_on(|| async {
            source.sync_async_with_permit_carrier_guarded_context_with_ack(
                crate::PublicationPermit::Unrestricted, true,
                &mut NativeDemandCarrier::Native(&native),
                &mut super::super::view_ledger::ViewLedgerAdmission::default(),
                &local_stop, &mut before_publication, Some(context), &mut acknowledged,
            ).await
        }).expect_err("the actual Root guard must reject the prepared SDK DATA CAS");
        match failure.0 {
            GuardFailure::RootPublicationRejected(code) => assert_eq!(code, 0x51_0c),
            GuardFailure::Source(cause) => panic!("SDK failure replaced the Root primary: {cause:?}"),
        }
        assert!(matches!(failure.1, Some(MountSourceError::Cancelled)),
            "the original post-restore accounting record must preserve its real local-stop failure");
        assert!(reached.load(Ordering::Acquire), "the genuine kernel publication guard must run");
        assert!(acknowledged.is_none(), "guard rejection is before the real DATA CAS");
        let at_guard = observed_prefix.lock().unwrap_or_else(PoisonError::into_inner)
            .expect("guard observed the cumulative original SDK prefix");
        let completed = root.receipt();
        assert!(at_guard.object_bytes_written > before.object_bytes_written);
        assert!(at_guard.items_examined > before.items_examined);
        assert_eq!(completed.backend_read_operations, at_guard.backend_read_operations);
        assert_eq!(completed.backend_write_operations, at_guard.backend_write_operations);
        assert_eq!(completed.object_bytes_written, at_guard.object_bytes_written);
        assert_eq!(completed.bytes_encoded, at_guard.bytes_encoded);
        assert_eq!(completed.authority_records_appended, before.authority_records_appended);
        assert!(local_stop.is_cancelled());
        assert!(!root_stop.is_cancelled());
        assert!(!source.cancellation.is_cancelled(), "SDK Source authority has its own original stop");
        let retained_operation = source.runtime.block_on(|| async {
            let checkout = source.checkout.lock().await;
            assert!(checkout.has_pending_mutations());
            assert!(checkout.publication_evidence.is_some());
            checkout.publication_operation.expect("failed publication retains the actual SDK operation")
        });
        drop(native);
        drop(job);
        drop(incoming);

        // A current invocation may retry the retained SDK operation without
        // resurrecting the stopped caller or cancelling the Source's authority.
        let (retry, retry_stop) = root.invocation_view()?;
        let retry = retry.with_source_io_lease(source.native_source_io_lease()?)?;
        let retry_native = retry.native_mount_work();
        let published = source.runtime.block_on(|| {
            source.sync_async_with_permit_qualified_native_work_guarded_with_ack(
                crate::PublicationPermit::Unrestricted, &retry_native, &retry_stop,
                context, &mut acknowledged,
                |_original: &crate::NativeMaterializationWork| std::future::ready(Ok::<(), GuardFailure>(())),
            )
        }).expect("healthy current invocation retries the original retained SDK operation");
        assert_eq!(published.operation(), Some(retained_operation));
        assert_eq!(published.snapshot().workspace_id(), workspace_id);
        assert_eq!(published.snapshot().volume_config(), config);
        assert_eq!(acknowledged, Some(published));
        assert!(!retry_stop.is_cancelled());
        assert!(!root_stop.is_cancelled());
        assert!(!source.cancellation.is_cancelled());
        assert_eq!(root.receipt().authority_records_appended, before.authority_records_appended + 1);
        drop(retry_native);
        drop(retry);
        drop(file);
        source.shutdown_accepted_cleanup()?;
        Ok(())
    }

    #[test]
    fn native_source_arc_denial_drops_private_constructor_without_losing_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let borrowed = source(FilesystemProfile::Portable)?;
        let config = VolumeConfig::portable(Lifecycle::Ephemeral);
        let cancellation = CancellationToken::new();
        let budget = WorkBudget { allocations: 2, ..boundary_budget() };
        let original = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let baseline = original.receipt().retained_allocation_bytes;
        let constructor_prefix = original.receipt().allocation_operations;
        let native = original.native_mount_work();
        let namespace_guard = original.claim_allocation(0, 0)?;
        let source = CheckoutMountSource::new_at_meter(
            Arc::clone(&borrowed.checkout), config, NamespacePath::root(), namespace_guard, &native, Some(&original),
        )?;
        assert_eq!(original.receipt().allocation_operations, constructor_prefix + 1);
        let result = source.into_arc_meter(&native);
        assert!(matches!(result, Err(NativeMountError::Materialization(
            crate::NativeTreeMaterializationError::Work(_)
        ))));
        assert_eq!(original.receipt().allocation_operations, constructor_prefix + 1);
        assert_eq!(original.receipt().retained_allocation_bytes, baseline);
        Ok(())
    }

    #[test]
    fn native_source_rejects_foreign_namespace_custody_before_new_allocation()
    -> Result<(), Box<dyn std::error::Error>> {
        let borrowed = source(FilesystemProfile::Portable)?;
        let config = VolumeConfig::portable(Lifecycle::Ephemeral);
        let cancellation = CancellationToken::new();
        let first = crate::NativeMaterializationWork::new(boundary_budget(), &cancellation)
            .map_err(|(cause, _)| cause)?;
        let second = crate::NativeMaterializationWork::new(boundary_budget(), &cancellation)
            .map_err(|(cause, _)| cause)?;
        let first_baseline = first.receipt().retained_allocation_bytes;
        let second_prefix = second.receipt();
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(&first, &cancellation);
        let (root, allocation) = NamespacePath::parse_in_profile_with_accounting(
            "/foreign", config.profile, config.limits, &mut accounting, &cancellation,
        )?;
        let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) = allocation
            else { panic!("native namespace must retain its original guard"); };
        let result = CheckoutMountSource::new_at_meter(
            Arc::clone(&borrowed.checkout), config, root, allocation, &second.native_mount_work(), Some(&second),
        );
        assert!(matches!(result, Err(NativeMountError::Materialization(
            crate::NativeTreeMaterializationError::InvalidLayout
        ))));
        assert_eq!(second.receipt(), second_prefix);
        assert_eq!(first.receipt().retained_allocation_bytes, first_baseline);
        Ok(())
    }

    fn checkout_state(source: &MemorySource) -> (crate::GenerationId, bool) {
        source.runtime.block_on(|| async {
            let checkout = source.checkout.lock().await;
            (checkout.generation_id(), checkout.has_pending_mutations())
        })
    }

    fn source(profile: FilesystemProfile) -> Result<MemorySource, Box<dyn std::error::Error>> {
        Ok(shared_sources(profile)?.0)
    }

    fn shared_sources(
        profile: FilesystemProfile,
    ) -> Result<(MemorySource, MemorySource), Box<dyn std::error::Error>> {
        shared_sources_with_publication(profile, MountPublication::CloseAndSync)
    }

    /// A file created through the mount holds its first bytes until its
    /// handle settles; anything else that reads the checkout meanwhile
    /// applies the creation first, and bytes past the bound apply it too.
    /// A creation proves its path absent as it registers: a second creation
    /// of a path another holds pending, or one already applied, fails, and
    /// a stale proof of absence is checked again.
    #[test]
    fn creating_an_existing_path_fails() -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Portable)?;
        let path = native_test_path("once");
        let stale = source.view_stamp().ok_or("ledger stamp")?;
        source.create_file(&path, metadata())?;
        assert!(matches!(
            source.create_file(&path, metadata()),
            Err(MountSourceError::AlreadyExists)
        ));
        assert!(matches!(
            source.create_absent_file(&path, metadata(), stale),
            Err(MountSourceError::AlreadyExists)
        ));
        source.sync()?;
        assert!(matches!(
            source.create_absent_file(&path, metadata(), stale),
            Err(MountSourceError::AlreadyExists)
        ));
        Ok(())
    }

    #[test]
    fn a_created_file_is_applied_with_its_first_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Portable)?;
        let (held, read, large) = (
            native_test_path("held"),
            native_test_path("read"),
            native_test_path("large"),
        );

        let created = source.create_file(&held, metadata())?;
        let file = source.open_created(&held, &created)?;
        file.write_range(0, Bytes::from_static(b"first bytes"))?;
        assert_eq!(file.lookup()?.node.logical_bytes, 11);
        assert_eq!(file.read_up_to(0, 64)?.as_ref(), b"first bytes");
        file.settle()?;
        let applied = source.lookup(&held)?.ok_or("settled creation is absent")?;
        assert_eq!(applied.node.file_id, created.node.file_id);
        assert_eq!(applied.node.logical_bytes, 11);
        assert_eq!(applied.metadata, file.lookup()?.metadata);

        // Another reader applies a creation its handle still holds.
        let created = source.create_file(&read, metadata())?;
        let file = source.open_created(&read, &created)?;
        file.write_range(4, Bytes::from_static(b"tail"))?;
        let reopened = source.open_file(&read)?;
        assert_eq!(reopened.read_up_to(0, 64)?.as_ref(), b"\0\0\0\0tail");
        file.write_range(0, Bytes::from_static(b"head"))?;
        assert_eq!(reopened.read_up_to(0, 64)?.as_ref(), b"headtail");

        // Bytes past the bound apply the creation and continue in place.
        let created = source.create_file(&large, metadata())?;
        let file = source.open_created(&large, &created)?;
        let past = usize::try_from(PENDING_CREATE_BYTES)? + 1;
        file.write_range(0, Bytes::from(vec![7_u8; past]))?;
        assert_eq!(
            source
                .lookup(&large)?
                .ok_or("large creation is absent")?
                .node
                .logical_bytes,
            u64::try_from(past)?
        );
        Ok(())
    }

    #[test]
    fn bounded_queued_creations_restore_their_original_buffers()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Portable)?;
        let path = native_test_path("bounded-queued");
        let created = source.create_file(&path, metadata())?;
        let file = source.open_created(&path, &created)?;
        file.write_range(0, Bytes::from_static(b"first bytes"))?;
        file.resize(3)?;
        let pending = source.checkout.pending_create(created.node.file_id)
            .ok_or("creation was not queued")?;
        let (pointer, capacity) = match &*pending.state() {
            PendingState::Pending { bytes, .. } => (bytes.as_ptr(), bytes.capacity()),
            _ => return Err("creation was not pending".into()),
        };
        assert!(capacity > 3, "the real queued buffer has spare capacity");
        let second_path = native_test_path("bounded-queued-second");
        let second_created = source.create_absent_file(
            &second_path, metadata(), source.checkout.ledger.stamp(),
        )?;
        let second_file = source.open_created(&second_path, &second_created)?;
        second_file.write_range(0, Bytes::from_static(b"two"))?;
        let second_pending = source.checkout.pending_create(second_created.node.file_id)
            .ok_or("second creation was not queued")?;
        let (second_pointer, second_capacity) = match &*second_pending.state() {
            PendingState::Pending { bytes, .. } => (bytes.as_ptr(), bytes.capacity()),
            _ => return Err("second creation was not pending".into()),
        };
        let originals = [
            (&pending, pointer, capacity, b"fir".as_slice()),
            (&second_pending, second_pointer, second_capacity, b"two".as_slice()),
        ];
        let generation = source.runtime.block_on(|| async {
            source.checkout.state.read().await.checkout.generation_id()
        });
        let mut before_begin = WorkBudget::UNBOUNDED;
        before_begin.items_examined = 0;
        let mut before_write = WorkBudget::UNBOUNDED;
        before_write.object_bytes_written = 0;
        for budget in [before_begin, before_write] {
            let (result, measured) = source.runtime.block_on(|| async {
                let cancellation = CancellationToken::new();
                let mut work = crate::demand::DemandWork::new(budget, &cancellation);
                let mut view = super::super::view_ledger::ViewLedgerAdmission::default();
                let result = source.checkout.lock_measured(&mut work, &mut view, &cancellation, None)
                    .await.map(drop);
                (result, work.work)
            });
            assert!(matches!(result, Err(MountSourceError::Work(_))));
            measured.verify(budget)?;
            for (index, (pending, pointer, capacity, expected)) in originals.iter().enumerate() {
                assert!(Arc::ptr_eq(
                    pending,
                    source.checkout.pending.lock().map_err(|_| "poisoned pending queue")?
                        .get(index).ok_or("denied creation lost its queue identity")?,
                ));
                match &*pending.state() {
                    PendingState::Pending { bytes, .. } => {
                        assert_eq!(bytes.as_ptr(), *pointer);
                        assert_eq!(bytes.capacity(), *capacity);
                        assert_eq!(bytes.as_slice(), *expected);
                    }
                    _ => return Err("denied creation did not restore Pending".into()),
                }
            }
            assert_eq!(source.runtime.block_on(|| async {
                source.checkout.state.read().await.checkout.generation_id()
            }), generation);
            if budget.object_bytes_written == 0 {
                assert!(measured.items_examined > 0);
                assert!(measured.allocation_operations > 0);
                assert!(measured.bytes_copied > 0);
            }
        }
        let measured = source.runtime.block_on(|| async {
            let cancellation = CancellationToken::new();
            let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
            let mut view = super::super::view_ledger::ViewLedgerAdmission::default();
            source.checkout.lock_measured(&mut work, &mut view, &cancellation, None).await?;
            Ok::<_, MountSourceError>(work.work)
        })?;
        assert!(measured.object_bytes_written > 0);
        assert!(source.checkout.pending.lock().map_err(|_| "poisoned pending queue")?.is_empty());
        assert_eq!(source.read_range(&path, 0, 3)?.as_ref(), b"fir");
        assert!(matches!(&*pending.state(), PendingState::Applied));
        assert_eq!(source.read_range(&second_path, 0, 3)?.as_ref(), b"two");
        assert!(matches!(&*second_pending.state(), PendingState::Applied));
        Ok(())
    }

    #[test]
    fn shared_queued_settlement_preserves_original_queue_and_meter()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Portable)?;
        let path = native_test_path("shared-queued");
        let created = source.create_file(&path, metadata())?;
        let file = source.open_created(&path, &created)?;
        file.write_range(0, Bytes::from_static(b"immutable original"))?;
        file.resize(9)?;
        let pending = source.checkout.pending_create(created.node.file_id)
            .ok_or("creation was not queued")?;
        let original = match &*pending.state() {
            PendingState::Pending { bytes, .. } => (bytes.as_ptr(), bytes.capacity()),
            _ => return Err("creation was not pending".into()),
        };
        for deny_before_begin in [true, false] {
            let cancellation = CancellationToken::new();
            let mut budget = WorkBudget::UNBOUNDED;
            if deny_before_begin { budget.items_examined = 0; }
            else { budget.object_bytes_written = 0; }
            let meter = Arc::new(crate::demand::NativeDemandReadMeter(StdMutex::new(
                crate::demand::DemandWork::new(budget, &cancellation),
            )));
            let result = source.runtime.block_on(|| source.checkout.settle_pending_with_meter(
                &meter, &cancellation, None,
            ));
            assert!(matches!(result, Err(MountSourceError::Work(_))));
            meter.lock().work.verify(budget)?;
            assert!(Arc::ptr_eq(&pending,
                source.checkout.pending.lock().map_err(|_| "poisoned queue")?
                    .first().ok_or("shared denied queue lost original entry")?,
            ));
            match &*pending.state() {
                PendingState::Pending { bytes, .. } => {
                    assert_eq!(bytes.as_ptr(), original.0);
                    assert_eq!(bytes.capacity(), original.1);
                    assert_eq!(bytes.as_slice(), b"immutable");
                }
                _ => return Err("shared denied creation was not restored".into()),
            }
            assert!(meter.0.try_lock().is_ok(), "settlement retained the work mutex");
        }
        let cancellation = CancellationToken::new();
        let meter = Arc::new(crate::demand::NativeDemandReadMeter(StdMutex::new(
            crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation),
        )));
        source.runtime.block_on(|| source.checkout.settle_pending_with_meter(
            &meter, &cancellation, None,
        ))?;
        assert!(meter.lock().work.object_bytes_written > 0);
        assert!(source.checkout.pending.lock().map_err(|_| "poisoned queue")?.is_empty());
        assert!(matches!(&*pending.state(), PendingState::Applied));
        assert_eq!(source.read_range(&path, 0, 9)?.as_ref(), b"immutable");
        Ok(())
    }

    #[test]
    fn native_pending_body_keeps_batching_and_intrinsic_last_alias_custody()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let source = source(FilesystemProfile::Portable)?;
        let path = native_test_path("native-held-body");
        let created = source.create_file(&path, metadata())?;
        let file = source.open_created(&path, &created)?;
        let pending = source.checkout.pending_create(created.node.file_id).ok_or("missing pending creation")?;
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(boundary_budget(), &cancellation)
            .map_err(|(error, _)| error)?;
        let invocation = original.native_mount_work();
        let before = original.receipt();
        file.write_range_with_meter(0, crate::kernel::OwnedSourceChunk::intrinsic(
            OwnedBytes::from_static(b"first"), 0,
        ), &invocation)?;
        let (pointer, capacity) = match &*pending.state() {
            PendingState::Pending { bytes: PendingBytes::Mutable { bytes, allocation: Some(allocation) }, .. } => {
                assert_eq!(allocation.bytes(), bytes.capacity() as u64);
                assert!(allocation.work().shares_ledger(&original));
                (bytes.as_ptr(), bytes.capacity())
            }
            _ => return Err("native held write lost mutable body custody or applied prematurely".into()),
        };
        assert_eq!(original.receipt().backend_write_operations, before.backend_write_operations);
        assert_eq!(original.receipt().object_bytes_written, before.object_bytes_written);
        file.write_range_with_meter(1, crate::kernel::OwnedSourceChunk::intrinsic(
            OwnedBytes::from_static(b"i"), 0,
        ), &invocation)?;
        match &*pending.state() {
            PendingState::Pending { bytes, .. } => {
                assert_eq!((bytes.as_ptr(), bytes.capacity()), (pointer, capacity));
                assert_eq!(bytes.as_slice(), b"first");
            }
            _ => return Err("bounded overwrite applied the creation".into()),
        }
        let mut preparation = original.claim_allocation(0, 0)?;
        let change = pending.begin_applying_with_native_work(
            &original, &mut preparation, &cancellation,
        )?.ok_or("native creation did not prepare")?;
        let retained = match &change {
            GroupedChange::CreateFile { bytes, .. } => bytes.clone(),
            _ => return Err("wrong native grouped change".into()),
        };
        drop(change);
        drop(preparation);
        pending.restore_applying_retained();
        assert!(retained.matches_meter(&original.runtime_meter_owner()));
        assert_eq!(retained.as_ptr(), pointer);
        assert_eq!(retained.as_ref(), b"first");
        drop(retained);
        // Exercise actual last-alias physical retirement at the same pending
        // body leaf, without fabricating a successful checkout application.
        let empty = PendingBytes::Mutable { bytes: Vec::new(), allocation: None };
        let mut body = empty.prepare_native_capacity(4, 64, &original)?.ok_or("body did not grow")?;
        let copy = original.reserve_copy(4)?;
        body.mutable().extend_from_slice(b"last");
        copy.record(4)?;
        let capacity = body.capacity() as u64;
        let retained = body.share(Some(&original))?;
        let live = original.native_read_meter().lock().allocations.live_bytes();
        drop(body);
        assert_eq!(original.native_read_meter().lock().allocations.live_bytes(), live,
            "dropping the pending alias released a still-owned native body");
        assert_eq!(retained.as_ref(), b"last");
        drop(retained);
        assert!(live - original.native_read_meter().lock().allocations.live_bytes() >= capacity);
        Ok(())
    }

    #[test]
    fn native_pending_cow_denial_and_cancel_preserve_original_published_alias()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let source = source(FilesystemProfile::Portable)?;
        let path = native_test_path("native-held-cow");
        let created = source.create_file(&path, metadata())?;
        let file = source.open_created(&path, &created)?;
        let pending = source.checkout.pending_create(created.node.file_id).ok_or("missing pending creation")?;
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(boundary_budget(), &cancellation)
            .map_err(|(error, _)| error)?;
        let invocation = original.native_mount_work();
        file.write_range_with_meter(0, crate::kernel::OwnedSourceChunk::intrinsic(
            OwnedBytes::from_static(b"original"), 0,
        ), &invocation)?;
        let mut preparation = original.claim_allocation(0, 0)?;
        let change = pending.begin_applying_with_native_work(
            &original, &mut preparation, &cancellation,
        )?.ok_or("native creation did not prepare")?;
        let retained = match &change {
            GroupedChange::CreateFile { bytes, .. } => bytes.clone(),
            _ => return Err("wrong native grouped change".into()),
        };
        let pointer = retained.as_ptr();
        drop(change);
        drop(preparation);
        pending.restore_applying_retained();
        let mut denied_budget = boundary_budget();
        denied_budget.allocation_operations = 0;
        let denied = original.native_volume_work(denied_budget)?;
        let prefix = original.receipt();
        assert!(file.write_range_with_meter(0, crate::kernel::OwnedSourceChunk::intrinsic(
            OwnedBytes::from_static(b"changed!"), 0,
        ), &denied.native_mount_work()).is_err());
        assert_eq!(original.receipt().backend_write_operations, prefix.backend_write_operations);
        assert_eq!(original.receipt().object_bytes_written, prefix.object_bytes_written);
        match &*pending.state() {
            PendingState::Pending { bytes: PendingBytes::Shared { bytes, .. }, .. } => {
                assert_eq!(bytes.as_ptr(), pointer);
                assert_eq!(bytes.as_ref(), b"original");
            }
            _ => return Err("denied COW replaced the original body".into()),
        }
        let prefix = original.receipt();
        file.write_range(0, Bytes::from_static(b"changed!"))?;
        assert!(original.receipt().bytes_copied > prefix.bytes_copied,
            "ordinary API escaped the existing native body's original work");
        match &*pending.state() {
            PendingState::Pending { bytes: PendingBytes::Mutable { bytes, allocation: Some(allocation) }, .. } => {
                assert_eq!(allocation.bytes(), bytes.capacity() as u64);
                assert!(allocation.work().shares_ledger(&original));
                assert!(allocation.work().shares_resource_allowance(&original));
            }
            _ => return Err("ordinary COW discarded original native funding".into()),
        }
        assert_eq!(retained.as_ref(), b"original");
        let read = file.read_up_to_with_meter(0, 8, &invocation)?;
        assert_eq!(read.bytes.as_ref(), b"changed!");
        drop(read);
        cancellation.cancel();
        let prefix = original.receipt();
        assert!(file.resize_with_meter(0, &invocation).is_err());
        assert!(file.resize(0).is_err());
        assert!(file.write_range(0, Bytes::from_static(b"bypass!!")).is_err());
        assert_eq!(original.receipt(), prefix);
        match &*pending.state() {
            PendingState::Pending { bytes, .. } => assert_eq!(bytes.as_slice(), b"changed!"),
            _ => return Err("cancelled resize applied the creation".into()),
        }
        Ok(())
    }

    #[test]
    fn native_queued_settlement_obeys_resource_denial_and_keeps_original_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Portable)?;
        let path = native_test_path("native-queued");
        let created = source.create_file(&path, metadata())?;
        let file = source.open_created(&path, &created)?;
        file.write_range(0, Bytes::from_static(b"original native queue"))?;
        file.resize(8)?;
        let pending = source.checkout.pending_create(created.node.file_id)
            .ok_or("creation was not queued")?;
        let original = match &*pending.state() {
            PendingState::Pending { bytes, .. } => (bytes.as_ptr(), bytes.capacity()),
            _ => return Err("creation was not pending".into()),
        };
        for deny_allocation in [true, false] {
            let cancellation = CancellationToken::new();
            let root = crate::NativeMaterializationWork::new(
                WorkBudget::UNBOUNDED, &cancellation,
            ).map_err(|(error, _)| error)?;
            let mut resource_budget = WorkBudget::UNBOUNDED;
            if deny_allocation { resource_budget.allocation_operations = 0; }
            else { resource_budget.object_bytes_written = 0; }
            let resource = root.native_volume_work(resource_budget)?;
            let prefix = root.receipt();
            let invocation = resource.native_mount_work();
            let result = source.runtime.block_on(|| source.checkout.settle_pending_with_native_work(
                &invocation, &cancellation, None,
            ));
            assert!(result.is_err(), "real queued creation ignored the native resource quota");
            let completed = root.receipt();
            if deny_allocation {
                assert_eq!(completed.allocation_operations, prefix.allocation_operations);
                assert_eq!(completed.backend_write_operations, prefix.backend_write_operations);
                assert_eq!(completed.object_bytes_written, prefix.object_bytes_written);
            } else {
                assert!(completed.allocation_operations > prefix.allocation_operations);
                assert!(completed.bytes_copied > prefix.bytes_copied);
                assert_eq!(completed.object_bytes_written, prefix.object_bytes_written);
            }
            assert!(Arc::ptr_eq(&pending,
                source.checkout.pending.lock().map_err(|_| "poisoned queue")?
                    .first().ok_or("native denied queue lost original entry")?,
            ));
            match &*pending.state() {
                PendingState::Pending { bytes, .. } => {
                    assert_eq!(bytes.as_ptr(), original.0);
                    assert_eq!(bytes.capacity(), original.1);
                    assert_eq!(bytes.as_slice(), b"original");
                }
                _ => return Err("native denied creation was not restored".into()),
            }
            assert!(root.native_read_meter().0.try_lock().is_ok(),
                "queued native preparation retained the work mutex");
        }
        let cancellation = CancellationToken::new();
        let root = crate::NativeMaterializationWork::new(
            WorkBudget::UNBOUNDED, &cancellation,
        ).map_err(|(error, _)| error)?;
        let invocation = root.native_mount_work();
        source.runtime.block_on(|| source.checkout.settle_pending_with_native_work(
            &invocation, &cancellation, None,
        ))?;
        assert!(source.checkout.pending.lock().map_err(|_| "poisoned queue")?.is_empty());
        assert!(matches!(&*pending.state(), PendingState::Applied));
        assert!(root.receipt().object_bytes_written > 0);
        assert_eq!(source.read_range(&path, 0, 8)?.as_ref(), b"original");
        Ok(())
    }

    #[test]
    fn cancelled_queued_lock_never_begins_a_creation()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Portable)?;
        let path = native_test_path("cancelled-queued");
        let created = source.create_file(&path, metadata())?;
        let file = source.open_created(&path, &created)?;
        file.write_range(0, Bytes::from_static(b"untouched"))?;
        let pending = source.checkout.pending_create(created.node.file_id)
            .ok_or("creation was not queued")?;
        let pointer = match &*pending.state() {
            PendingState::Pending { bytes, .. } => bytes.as_ptr(),
            _ => return Err("creation was not pending".into()),
        };
        let (result, measured) = source.runtime.block_on(|| async {
            let cancellation = CancellationToken::new();
            cancellation.cancel();
            let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
            let mut view = super::super::view_ledger::ViewLedgerAdmission::default();
            let result = source.checkout.lock_measured(&mut work, &mut view, &cancellation, None)
                .await.map(drop);
            (result, work.work)
        });
        assert!(matches!(result, Err(MountSourceError::Cancelled)));
        assert_eq!(measured, crate::WorkCounters::default());
        assert!(Arc::ptr_eq(
            &pending,
            source.checkout.pending.lock().map_err(|_| "poisoned pending queue")?
                .first().ok_or("cancelled creation lost its queue identity")?,
        ));
        match &*pending.state() {
            PendingState::Pending { bytes, .. } => {
                assert_eq!(bytes.as_ptr(), pointer);
                assert_eq!(bytes.as_slice(), b"untouched");
            }
            _ => return Err("cancelled creation started applying".into()),
        }
        let cancellation = CancellationToken::new();
        let root = crate::NativeMaterializationWork::new(
            WorkBudget::UNBOUNDED, &cancellation,
        ).map_err(|(error, _)| error)?;
        let invocation = root.native_mount_work();
        let prefix = root.receipt();
        cancellation.cancel();
        let result = source.runtime.block_on(|| async {
            let mut carrier = NativeDemandCarrier::Native(&invocation);
            source.checkout.lock_with_carrier(&mut carrier, &cancellation, None).await.map(drop)
        });
        assert!(matches!(&result, Err(error)
            if matches!(error.semantic_error(), MountSourceError::Cancelled)));
        assert_eq!(root.receipt(), prefix, "pre-cancelled native lock performed hidden work");
        assert!(Arc::ptr_eq(&pending,
            source.checkout.pending.lock().map_err(|_| "poisoned queue")?
                .first().ok_or("native cancelled queue lost original entry")?,
        ));
        match &*pending.state() {
            PendingState::Pending { bytes, .. } => {
                assert_eq!(bytes.as_ptr(), pointer);
                assert_eq!(bytes.as_slice(), b"untouched");
            }
            _ => return Err("native cancelled creation started applying".into()),
        }
        file.settle()?;
        assert_eq!(source.read_range(&path, 0, 9)?.as_ref(), b"untouched");
        Ok(())
    }

    #[test]
    fn restored_queued_bytes_preserve_external_immutable_owners()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Portable)?;
        let path = native_test_path("retained-queued");
        let created = source.create_file(&path, metadata())?;
        let file = source.open_created(&path, &created)?;
        file.write_range(0, Bytes::from_static(b"immutable"))?;
        let pending = source.checkout.pending_create(created.node.file_id)
            .ok_or("creation was not queued")?;
        let original = match &*pending.state() {
            PendingState::Pending { bytes, .. } => (bytes.as_ptr(), bytes.capacity()),
            _ => return Err("creation was not pending".into()),
        };
        let cancellation = CancellationToken::new();
        let mut first = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let change = pending.begin_applying_measured(source.limits, &mut first, &mut 0)?
            .ok_or("creation did not begin")?;
        let retained = match &change {
            GroupedChange::CreateFile { bytes, .. } => bytes.clone(),
            _ => return Err("creation prepared another change".into()),
        };
        drop(change);
        pending.restore_applying_retained();
        match &*pending.state() {
            PendingState::Pending { bytes, .. } => {
                assert_eq!((bytes.as_ptr(), bytes.capacity()), original);
                assert!(matches!(bytes, PendingBytes::Shared { .. }));
            }
            _ => return Err("creation was not restored".into()),
        };
        let mut second = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let change = pending.begin_applying_measured(source.limits, &mut second, &mut 0)?
            .ok_or("restored creation did not begin")?;
        assert_eq!(first.work.allocation_operations, second.work.allocation_operations + 1);
        drop(change);
        pending.restore_applying_retained();
        file.write_range(0, Bytes::from_static(b"changed!!"))?;
        assert_eq!(retained.as_ref(), b"immutable");
        assert_eq!(file.read_up_to(0, 9)?.as_ref(), b"changed!!");
        file.settle()?;
        assert_eq!(source.read_range(&path, 0, 9)?.as_ref(), b"changed!!");
        Ok(())
    }

    #[test]
    fn detached_metered_operations_keep_identity_sparse_claims_and_failure_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let source = source(FilesystemProfile::Posix)?;
        let from_path = native_test_path("metered-detached-source");
        let to_path = native_test_path("metered-detached-target");
        let created = source.create_file(&from_path, metadata())?;
        source.resize(&from_path, 128)?;
        source.write_range(&from_path, 64, Bytes::from_static(b"payload"))?;
        source.create_file(&to_path, metadata())?;
        let from = source.detach_file(&from_path)?;
        let to = source.detach_file(&to_path)?;
        source.remove(&from_path, Some(created.node.file_id))?;
        let caller = CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(boundary_budget(), &caller)
            .map_err(|(error, _)| error)?;
        let meter = Arc::clone(work.native_read_meter());
        let invocation = work.native_mount_work();
        let lookup = from.lookup_with_meter(&invocation)?;
        assert_eq!(lookup.node.file_id, created.node.file_id);
        assert_eq!(lookup.node.link_count, 0);
        to.set_attributes_with_meter(lookup.metadata, Some(128), &invocation)?;
        from.allocate_range_with_meter(80, 8, MountRangeAllocation::ZeroRange { extend: false }, &invocation)?;
        let foreign = crate::NativeMaterializationWork::new(boundary_budget(), &caller)
            .map_err(|(error, _)| error)?;
        let foreign_input = from.read_sparse_range_with_meter(0, 128, &foreign.native_mount_work())?;
        let prefix = work.receipt();
        assert!(matches!(to.write_sparse_range_with_meter(0, foreign_input, &invocation),
            Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::InvalidLayout))));
        assert_eq!(work.receipt(), prefix, "foreign-root input is rejected before native work");
        let resource = work.native_volume_work(boundary_budget())?;
        let retained_before_foreign = work.receipt().retained_allocation_bytes;
        let foreign_input = from.read_sparse_range_with_meter(0, 128, &resource.native_mount_work())?;
        let prefix = work.receipt();
        assert!(matches!(to.write_sparse_range_with_meter(0, foreign_input, &invocation),
            Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::InvalidLayout))));
        let after = work.receipt();
        assert_eq!(after.allocation_operations, prefix.allocation_operations);
        assert_eq!(after.backend_read_operations, prefix.backend_read_operations);
        assert_eq!(after.retained_allocation_bytes, retained_before_foreign,
            "rejected payload and span metadata retire their separate original guards");
        let sparse = from.read_sparse_range_with_meter(0, 128, &invocation)?;
        assert_eq!(sparse.0.logical_bytes, 128);
        assert!(sparse.1.bytes() >= (sparse.0.spans.capacity() * std::mem::size_of::<super::super::MountSparseSpan>()) as u64);
        to.write_sparse_range_with_meter(0, sparse, &invocation)?;
        assert_eq!(to.seek_with_meter(0, MountSeekTarget::Data, &invocation)?, Some(64));
        assert_eq!(to.seek_with_meter(64, MountSeekTarget::Hole, &invocation)?, Some(71));
        assert_eq!(to.seek_with_meter(71, MountSeekTarget::Data, &invocation)?, Some(80));
        let packet = to.read_range_with_meter(80, 8, &invocation)?;
        assert_eq!(packet.bytes.as_ref(), &[0; 8]);
        drop(packet);
        let before = meter.lock().live_allocation_bytes();
        let packet = from.read_up_to_with_meter(64, 7, &invocation)?;
        assert_eq!(packet.bytes.as_ref(), b"payload");
        let retained = meter.lock().live_allocation_bytes();
        assert!(retained >= before + packet.logical_owned_bytes);
        let alias = packet.bytes.slice(..);
        let empty_alias = packet.bytes.slice(0..0);
        let (bytes, capacity, intrinsic) = packet.into_owned_parts();
        assert!(intrinsic);
        drop(bytes);
        assert_eq!(meter.lock().live_allocation_bytes(), retained,
            "source aliases retain the original payload guard");
        drop(empty_alias);
        assert_eq!(meter.lock().live_allocation_bytes(), retained);
        let packet = crate::kernel::OwnedSourceChunk::intrinsic(alias, capacity);
        to.write_range_with_meter(0, packet, &invocation)?;
        let empty = from.read_up_to_with_meter(128, 32, &invocation)?;
        assert!(empty.bytes.is_empty());
        assert_eq!(empty.logical_owned_bytes, 0);
        to.resize_with_meter(96, &invocation)?;
        assert_eq!(to.lookup_with_meter(&invocation)?.node.logical_bytes, 96);
        let value = to.read_range_with_meter(0, 7, &invocation)?;
        to.write_attribute_with_meter(
            b"user.metered", value, MountAttributeWriteMode::Create, &invocation,
        )?;
        let value = to.read_attribute_with_meter(b"user.metered", &invocation)?
            .ok_or("missing metered detached attribute")?;
        assert_eq!(value.bytes.as_ref(), b"payload");
        drop(value);
        let before = meter.lock().live_allocation_bytes();
        let unchanged = to.lookup_with_meter(&invocation)?;
        let value = to.read_range_with_meter(0, 7, &invocation)?;
        let prefix = meter.lock().work;
        assert!(to.write_attribute_with_meter(
            b"user.metered", value, MountAttributeWriteMode::Create, &invocation,
        ).is_err());
        assert!(meter.lock().work.backend_read_operations > prefix.backend_read_operations);
        assert_eq!(meter.lock().live_allocation_bytes(), before);
        assert_eq!(to.lookup_with_meter(&invocation)?, unchanged);
        let page = to.list_attributes_with_meter(None, 8, &invocation)?;
        assert_eq!(page.names, vec![b"user.metered".to_vec()]);
        drop(page);
        to.remove_attribute_with_meter(b"user.metered", &invocation)?;
        assert!(to.read_attribute_with_meter(b"user.metered", &invocation)?.is_none());
        to.settle_with_meter(&invocation)?;
        let observed = to.lookup_with_meter(&invocation)?;
        let prefix = meter.lock().work;
        assert!(prefix.backend_read_operations > 0);
        assert!(prefix.allocation_operations > 0);
        caller.cancel();
        assert!(matches!(to.resize_with_meter(0, &invocation), Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled))));
        assert_eq!(meter.lock().work, prefix);
        assert_eq!(to.lookup()?, observed);
        let denied = Arc::new(crate::demand::NativeDemandReadMeter(StdMutex::new(
            crate::demand::DemandWork::new(WorkBudget::default(), &CancellationToken::new()),
        )));
        let denied = super::super::NativeMountMeterWork::from_meter(denied);
        assert!(matches!(to.resize_with_meter(0, &denied), Err(MountSourceError::Work(_))));
        assert_eq!(to.lookup()?, observed);
        Ok(())
    }

    #[test]
    fn detached_snapshot_ready_and_precancelled_locks_need_no_wait_allocation()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Portable)?;
        let path = native_test_path("snapshot-cancellation");
        let created = source.create_file(&path, metadata())?;
        let record = source.record_by_id(created.node.file_id)?;
        let detached = source.detached_file_from_record(record, created.metadata)?;
        source.runtime.block_on(|| async {
            let caller = CancellationToken::new();
            let mut work = crate::demand::DemandWork::new(WorkBudget::default(), &caller);
            let snapshot = detached.snapshot_with_work(&mut work, &caller).await?;
            assert_eq!(snapshot, (record, created.metadata, 0));
            assert_eq!(work.work, crate::WorkCounters::default());
            let meter = Arc::new(crate::demand::NativeDemandReadMeter(StdMutex::new(
                crate::demand::DemandWork::new(WorkBudget::default(), &caller),
            )));
            let shared = detached.snapshot_with_carrier(
                &mut NativeDemandCarrier::Shared(&meter), &caller,
            ).await?;
            assert_eq!(shared, snapshot);
            assert_eq!(meter.lock().work, crate::WorkCounters::default());

            let held = detached.state.lock().await;
            caller.cancel();
            let mut work = crate::demand::DemandWork::new(WorkBudget::default(), &caller);
            assert!(matches!(
                detached.snapshot_with_work(&mut work, &caller).await,
                Err(MountSourceError::Cancelled),
            ));
            assert_eq!(work.work, crate::WorkCounters::default());
            assert!(matches!(
                detached.snapshot_with_carrier(
                    &mut NativeDemandCarrier::Shared(&meter), &caller,
                ).await,
                Err(MountSourceError::Cancelled),
            ));
            assert_eq!(meter.lock().work, crate::WorkCounters::default());
            assert_eq!(Arc::strong_count(&meter), 1);

            let caller = CancellationToken::new();
            detached.cancellation.cancel();
            let mut work = crate::demand::DemandWork::new(WorkBudget::default(), &caller);
            assert!(matches!(
                detached.snapshot_with_work(&mut work, &caller).await,
                Err(MountSourceError::Stale),
            ));
            assert_eq!(work.work, crate::WorkCounters::default());
            assert_eq!(held.file.record(), record);
            assert_eq!(held.mutation_epoch, 0);
            Ok::<_, MountSourceError>(())
        })?;
        Ok(())
    }

    #[test]
    fn detached_snapshot_contended_locks_keep_original_cancellation_tokens()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Portable)?;
        let path = native_test_path("snapshot-held");
        let created = source.create_file(&path, metadata())?;
        let record = source.record_by_id(created.node.file_id)?;
        let detached = source.detached_file_from_record(record, created.metadata)?;
        source.runtime.block_on(|| async {
            let held = detached.state.lock().await;
            let budget = WorkBudget::UNBOUNDED;
            let caller = CancellationToken::new();
            let mut work = crate::demand::DemandWork::new(budget, &caller);
            let result = {
                let mut snapshot = std::pin::pin!(detached.snapshot_with_work(&mut work, &caller));
                assert!(futures::poll!(snapshot.as_mut()).is_pending());
                caller.cancel();
                snapshot.await
            };
            assert!(matches!(result, Err(MountSourceError::Cancelled)));
            assert_eq!(work.budget, budget);
            work.work.verify(budget).map_err(MountSourceError::Work)?;
            assert!(work.work.allocation_operations > 0);
            assert!(work.work.items_examined > 0);

            let caller = CancellationToken::new();
            let mut work = crate::demand::DemandWork::new(budget, &caller);
            let result = {
                let mut snapshot = std::pin::pin!(detached.snapshot_with_work(&mut work, &caller));
                assert!(futures::poll!(snapshot.as_mut()).is_pending());
                detached.cancellation.cancel();
                snapshot.await
            };
            assert!(matches!(result, Err(MountSourceError::Stale)));
            assert_eq!(work.budget, budget);
            work.work.verify(budget).map_err(MountSourceError::Work)?;
            assert!(work.work.allocation_operations > 0);
            assert!(work.work.items_examined > 0);
            assert_eq!(held.file.record(), record);
            assert_eq!(held.mutation_epoch, 0);
            Ok::<_, MountSourceError>(())
        })?;
        Ok(())
    }

    #[test]
    fn detached_snapshot_repeated_lifetime_waits_reuse_the_same_registry_claim()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Portable)?;
        let path = native_test_path("snapshot-lifetime-registry");
        let created = source.create_file(&path, metadata())?;
        let record = source.record_by_id(created.node.file_id)?;
        let detached = source.detached_file_from_record(record, created.metadata)?;
        source.runtime.block_on(|| async {
            let caller = CancellationToken::new();
            let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &caller);
            let mut first = None;
            for _ in 0..2 {
                let held = detached.state.lock().await;
                {
                    let mut snapshot = std::pin::pin!(detached.snapshot_with_work(&mut work, &caller));
                    assert!(futures::poll!(snapshot.as_mut()).is_pending());
                    drop(held);
                    assert_eq!(snapshot.await?.0, record);
                }
                let live = work.live_allocation_bytes();
                if let Some((operations, previous_live)) = first {
                    // Both original token slots survive; only the two actual
                    // waiter payloads are newly allocated on this second wait.
                    assert_eq!(work.work.allocation_operations - operations, 2);
                    assert_eq!(live, previous_live);
                } else {
                    first = Some((work.work.allocation_operations, live));
                }
            }
            let before_cancel = work.live_allocation_bytes();
            detached.cancellation.cancel();
            let caller_registry = work.live_allocation_bytes();
            assert!(caller_registry > 0 && caller_registry < before_cancel);
            let owned = super::super::lazy::mount_vec::<u8>(7, &mut work)?;
            let bytes = owned.capacity() as u64;
            assert_eq!(work.live_allocation_bytes(), caller_registry + bytes);
            drop(owned);
            work.release(bytes).map_err(super::super::lazy::mount_demand_error)?;
            caller.cancel();
            assert_eq!(work.live_allocation_bytes(), 0);
            Ok::<_, MountSourceError>(())
        })?;
        Ok(())
    }

    #[test]
    fn measured_observation_precancellation_never_enters_held_view()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Portable)?;
        source.runtime.block_on(|| async {
            let held = source.checkout.lock_settled().await;
            let generation = held.checkout.generation_id();
            let owner = ViewGate::callback_owner();
            let caller = CancellationToken::new();
            caller.cancel();
            let mut work = crate::demand::DemandWork::new(WorkBudget::default(), &caller);
            assert!(matches!(
                source.checkout.observe_as_is_with_work(owner, &mut work, &caller, None).await,
                Err(MountSourceError::Cancelled),
            ));
            assert_eq!(work.work, crate::WorkCounters::default());

            let caller = CancellationToken::new();
            source.cancellation.cancel();
            let mut work = crate::demand::DemandWork::new(WorkBudget::default(), &caller);
            assert!(matches!(
                source.checkout.observe_as_is_with_work(
                    owner, &mut work, &caller, Some(&source.cancellation),
                ).await,
                Err(MountSourceError::Stale),
            ));
            assert_eq!(work.work, crate::WorkCounters::default());
            assert_eq!(held.checkout.generation_id(), generation);
            Ok::<_, MountSourceError>(())
        })?;
        Ok(())
    }

    #[test]
    fn measured_reentrant_observation_keeps_original_pending_creation()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Portable)?;
        let path = native_test_path("reentrant-measured-pending");
        let created = source.create_file(&path, metadata())?;
        let pending = source.checkout.pending_create(created.node.file_id)
            .ok_or("original creation was not queued")?;
        source.runtime.block_on(|| async {
            let caller = CancellationToken::new();
            let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &caller);
            let owner = ViewGate::callback_owner();
            let outer = source.checkout.observe_as_is_with_work(owner, &mut work, &caller, None).await?;
            let mut writer = std::pin::pin!(source.checkout.lock_settled());
            assert!(futures::poll!(writer.as_mut()).is_pending());
            let nested = tokio::time::timeout(
                Duration::from_secs(5),
                source.checkout.observe_with_work(owner, &mut work, &caller, None),
            ).await.map_err(|_| MountSourceError::Stale)??;
            assert!(Arc::ptr_eq(
                &pending,
                &source.checkout.pending_create(created.node.file_id).ok_or(MountSourceError::Stale)?,
            ));
            assert!(matches!(&*pending.state(), PendingState::Pending { .. }));
            let before = work.work.items_examined;
            drop(outer);
            drop(nested);
            drop(writer.await);
            let receipt = work.finish(Ok(())).map_err(|failure| {
                super::super::lazy::mount_demand_error(failure.error)
            })?;
            assert_eq!(receipt.work.items_examined, before + 2);
            Ok::<_, MountSourceError>(())
        })?;
        assert_eq!(source.lookup(&path)?.ok_or("ordinary settlement lost creation")?.node.file_id, created.node.file_id);
        Ok(())
    }

    #[test]
    fn a_grouped_failure_reuses_its_original_admission_without_a_cycle() {
        let admission = Arc::new(GroupedAdmission::new(GROUPED_CLAIMED));
        let original = admission.retain_failure(MountSourceError::Filesystem(FsError::NotFound));
        let retained = admission.retain_failure(MountSourceError::Retained(Arc::clone(&original)));
        assert!(Arc::ptr_eq(&original, &retained));
        assert!(matches!(retained.semantic_error(), MountSourceError::Filesystem(FsError::NotFound)));
        drop(original);
        drop(retained);
        assert_eq!(Arc::strong_count(&admission), 1);
    }

    #[test]
    fn a_grouped_retry_preserves_individual_results() -> Result<(), Box<dyn std::error::Error>> {
        let (source, _) = shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
        let target = native_test_path("grouped-conflict");
        let path = source.path(&target)?;
        let first_id = FileId::new();
        let (first_reply, first) = tokio::sync::oneshot::channel();
        let (second_reply, second) = tokio::sync::oneshot::channel();
        let requests = vec![
            GroupedRequest {
                change: GroupedChange::CreateFile {
                    path: path.clone(), metadata: Box::new(metadata()), file_id: first_id,
                    bytes: OwnedBytes::from_static(b"first"),
                },
                admission: Arc::new(GroupedAdmission::new(GROUPED_CLAIMED)),
                reply: GroupedReply::Callback(first_reply), origin: ViewOrigin::new(), recorded: false,
                native_callback_guard: source.native_callback_guard.as_ref().map(Arc::clone),
                source_lifetime: Some(source.cancellation.clone()),
            },
            GroupedRequest {
                change: GroupedChange::CreateFile {
                    path, metadata: Box::new(metadata()), file_id: FileId::new(),
                    bytes: OwnedBytes::from_static(b"second"),
                },
                admission: Arc::new(GroupedAdmission::new(GROUPED_CLAIMED)),
                reply: GroupedReply::Callback(second_reply), origin: ViewOrigin::new(), recorded: false,
                native_callback_guard: source.native_callback_guard.as_ref().map(Arc::clone),
                source_lifetime: Some(source.cancellation.clone()),
            },
        ];
        let (batch, first, second, work) = source.runtime.block_on(|| async {
            let cancellation = CancellationToken::new();
            let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
            let batch = source.checkout.apply_grouped_requests_measured(
                requests, &mut work, &cancellation, None,
            ).await;
            Ok::<_, MountSourceError>((batch,
                first.await.map_err(|_| MountSourceError::Stale)?,
                second.await.map_err(|_| MountSourceError::Stale)?, work.work))
        })?;
        assert!(matches!(first, Ok(GroupedOutcome::Created(id)) if id == first_id));
        assert!(matches!(&second, Err(error) if matches!(error.semantic_error(), MountSourceError::AlreadyExists)));
        assert!(matches!(&batch, Err(error) if matches!(error.semantic_error(), MountSourceError::AlreadyExists)));
        assert!(matches!((&batch, &second),
            (Err(MountSourceError::Retained(batch)), Err(MountSourceError::Retained(second)))
                if Arc::ptr_eq(batch, second)));
        assert!(work.items_examined > 0 && work.allocation_operations > 0);
        assert_eq!(source.lookup(&target)?.ok_or("successful retry is absent")?.node.file_id, first_id);
        assert_eq!(source.read_range(&target, 0, 5)?.as_ref(), b"first");
        Ok(())
    }

    #[test]
    fn grouped_failed_work_consumes_the_next_changes_remainder()
    -> Result<(), Box<dyn std::error::Error>> {
        let objects = Arc::new(MetadataRefusingObjects::default());
        let fs = Fs::new(crate::memory::MemoryAuthorityStore::default(), Arc::clone(&objects),
            crate::EmbeddedCapabilities::MEMORY);
        let config = VolumeConfig::portable(Lifecycle::Ephemeral);
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let checkout = runtime.block_on(async {
            let cancellation = CancellationToken::new();
            let volume = fs.create_volume(config, WorkBudget::UNBOUNDED, &cancellation).await?.value;
            volume.checkout(GenerationSelector::Head, CheckoutMode {
                access: AccessMode::ReadWrite, consistency: ConsistencyMode::Pinned,
                mutations: MutationMode::PrivateOverlay,
            }, WorkBudget::UNBOUNDED, &cancellation).await.map(|receipt| receipt.value)
        })?;
        let source = CheckoutMountSource::new(Arc::new(SharedCheckout::with_publication(
            checkout, MountPublication::Manual,
        )), config)?;
        let attempts = objects.metadata_attempts.load(Ordering::Acquire);
        objects.refuse_metadata.store(true, Ordering::Release);
        let encoded_bytes = crate::kernel::encode_file_metadata(metadata())?.len() as u64;
        let (first_reply, first) = tokio::sync::oneshot::channel();
        let (second_reply, second) = tokio::sync::oneshot::channel();
        let first_target = native_test_path("grouped-refused-first");
        let second_target = native_test_path("grouped-refused-second");
        let mut requests = Vec::new();
        for (target, reply) in [(&first_target, first_reply), (&second_target, second_reply)] {
            requests.push(GroupedRequest {
                change: GroupedChange::CreateFile {
                    path: source.path(target)?, metadata: Box::new(metadata()), file_id: FileId::new(),
                    bytes: OwnedBytes::new(),
                },
                admission: Arc::new(GroupedAdmission::new(GROUPED_CLAIMED)),
                reply: GroupedReply::Callback(reply), origin: ViewOrigin::new(), recorded: false,
                native_callback_guard: source.native_callback_guard.as_ref().map(Arc::clone),
                source_lifetime: Some(source.cancellation.clone()),
            });
        }
        let (batch, first, second, work) = source.runtime.block_on(|| async {
            let cancellation = CancellationToken::new();
            let mut work = crate::demand::DemandWork::new(WorkBudget {
                bytes_encoded: encoded_bytes, ..WorkBudget::UNBOUNDED
            }, &cancellation);
            let batch = source.checkout.apply_grouped_requests_measured(
                requests, &mut work, &cancellation, None,
            ).await;
            Ok::<_, MountSourceError>((batch,
                first.await.map_err(|_| MountSourceError::Stale)?,
                second.await.map_err(|_| MountSourceError::Stale)?, work.work))
        })?;
        assert!(matches!(&first, Err(error) if matches!(error.semantic_error(), MountSourceError::Filesystem(_))));
        assert!(matches!(&second, Err(error) if matches!(error.semantic_error(), MountSourceError::Work(_))));
        assert!(matches!(&batch, Err(error) if matches!(error.semantic_error(), MountSourceError::Filesystem(_))));
        assert!(matches!((&batch, &first),
            (Err(MountSourceError::Retained(batch)), Err(MountSourceError::Retained(first)))
                if Arc::ptr_eq(batch, first)));
        assert_eq!(work.bytes_encoded, encoded_bytes);
        assert_eq!(objects.metadata_attempts.load(Ordering::Acquire) - attempts, 1);
        assert!(source.lookup(&first_target)?.is_none());
        assert!(source.lookup(&second_target)?.is_none());
        Ok(())
    }

    #[test]
    fn a_grouped_change_takes_its_requesters_origin() -> Result<(), Box<dyn std::error::Error>> {
        struct Origins(StdMutex<Vec<ViewOrigin>>);
        impl ViewObserver for Origins {
            fn view_changed(&self, _position: ViewStamp, origin: ViewOrigin) {
                self.0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(origin);
            }
        }

        let source = source(FilesystemProfile::Portable)?;
        let observer = Arc::new(Origins(StdMutex::new(Vec::new())));
        let weak: Weak<dyn ViewObserver> = Arc::<Origins>::downgrade(&observer);
        source.observe_view(weak)?;
        let (requester, applier) = (ViewOrigin::new(), ViewOrigin::new());
        let target = native_test_path("applied");
        source.create_file(&target, metadata())?;
        // Applies the creation, which waits for its first bytes.
        assert!(source.lookup(&target)?.is_some());
        // A request queued under one origin, then applied in the group of a
        // caller under another.
        let (reply, _outcome) = tokio::sync::oneshot::channel();
        source
            .checkout
            .grouped
            .lock()
            .map_err(|_| "poisoned queue")?
            .requests
            .push_back(GroupedRequest {
                change: GroupedChange::CreateFile {
                    path: source.path(&native_test_path("queued"))?,
                    metadata: Box::new(metadata()),
                    file_id: FileId::new(),
                    bytes: OwnedBytes::new(),
                },
                admission: Arc::new(GroupedAdmission::new(GROUPED_QUEUED)),
                reply: GroupedReply::Callback(reply),
                origin: requester,
                recorded: false,
                native_callback_guard: source.native_callback_guard.as_ref().map(Arc::clone),
                source_lifetime: Some(source.cancellation.clone()),
            });
        {
            let _applier = applier.enter();
            source
                .open_file(&target)?
                .write_range(0, Bytes::from_static(b"applied"))?;
        }
        let origins = observer.0.lock().map_err(|_| "poisoned origins")?.clone();
        assert_eq!(
            origins.get(origins.len().saturating_sub(2)..),
            Some(&[requester, applier][..])
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn in_place_callback_outlasts_its_callers_cooperative_budget()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = CallbackRuntime::create()?;
        let lock = tokio::sync::Mutex::new(());
        // Each acquisition spends cooperative budget; far more than one
        // task's budget must complete while the calling task is blocked.
        let acquired = runtime.wait(|| async {
            let mut acquired = 0_u32;
            for _ in 0..4_096 {
                drop(lock.lock().await);
                acquired += 1;
            }
            Ok(acquired)
        });
        assert_eq!(acquired?, 4_096);
        Ok(())
    }

    #[test]
    fn an_abandoned_grouped_change_never_applies() -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Portable)?;
        let abandoned = native_test_path("abandoned.bin");
        let kept = native_test_path("kept.bin");
        let create = |path: &MountPath| -> Result<GroupedChange, MountSourceError> {
            Ok(GroupedChange::CreateFile {
                path: source.path(path)?,
                metadata: Box::new(metadata()),
                file_id: FileId::new(),
                bytes: OwnedBytes::new(),
            })
        };
        let (abandoned_change, kept_change) = (create(&abandoned)?, create(&kept)?);
        source.runtime.block_on(|| async {
            // While another holder keeps the checkout, one caller queues its
            // change and gives up waiting, as a callback deadline does.
            let held = source.checkout.lock().await;
            let gave_up = tokio::time::timeout(
                Duration::from_millis(20),
                source
                    .checkout
                    .apply_grouped(abandoned_change, &source.cancellation, source.native_callback_guard.as_ref().map(Arc::clone)),
            )
            .await;
            assert!(gave_up.is_err(), "the checkout was held");
            drop(held);
            source
                .checkout
                .apply_grouped(kept_change, &source.cancellation, source.native_callback_guard.as_ref().map(Arc::clone))
                .await
        })?;
        assert!(source.lookup(&kept)?.is_some());
        assert!(
            source.lookup(&abandoned)?.is_none(),
            "a change its caller saw fail applied later"
        );
        Ok(())
    }

    #[test]
    fn a_drain_outlives_the_caller_that_started_it() -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Portable)?;
        let abandoned = native_test_path("drainer.bin");
        let kept = native_test_path("waiter.bin");
        let create = |path: &MountPath| -> Result<GroupedChange, MountSourceError> {
            Ok(GroupedChange::CreateFile {
                path: source.path(path)?,
                metadata: Box::new(metadata()),
                file_id: FileId::new(),
                bytes: OwnedBytes::new(),
            })
        };
        let (abandoned_change, kept_change) = (create(&abandoned)?, create(&kept)?);
        source.runtime.block_on(|| async {
            let held = source.checkout.lock().await;
            // The first caller starts the drain in place, which waits for
            // the held checkout; a second caller queues behind it.
            let mut drainer = Box::pin(
                source
                    .checkout
                    .apply_grouped(abandoned_change, &source.cancellation, source.native_callback_guard.as_ref().map(Arc::clone)),
            );
            assert!(futures::poll!(drainer.as_mut()).is_pending());
            let checkout = Arc::clone(&source.checkout);
            let cancellation = source.cancellation.clone();
            let callback_guard = source.native_callback_guard.as_ref().map(Arc::clone);
            let waiter =
                tokio::spawn(
                    async move { checkout.apply_grouped(kept_change, &cancellation, callback_guard).await },
                );
            tokio::time::sleep(Duration::from_millis(20)).await;
            // The drainer's caller gives up; the drain continues without it.
            drop(drainer);
            drop(held);
            tokio::time::timeout(Duration::from_secs(5), waiter)
                .await
                .map_err(|_| MountSourceError::Stale)?
                .map_err(|_| MountSourceError::Stale)?
        })?;
        assert!(source.lookup(&kept)?.is_some());
        assert!(source.lookup(&abandoned)?.is_none());
        Ok(())
    }

    #[test]
    fn direct_checkout_view_lease_excludes_concurrent_mutation()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = Arc::new(source(FilesystemProfile::Portable)?);
        let lease = source.acquire_view_lease()?;
        let writer = Arc::clone(&source);
        let (completed, receive) = std::sync::mpsc::sync_channel(1);
        let task = std::thread::spawn(move || {
            let result =
                writer.create_directory(&MountPath::root().child(b"leased".to_vec()), metadata());
            assert!(completed.send(result).is_ok(), "mutation result receiver");
        });
        assert!(
            receive.recv_timeout(Duration::from_millis(50)).is_err(),
            "mutation crossed a retained native callback view"
        );
        drop(lease);
        receive.recv_timeout(Duration::from_secs(5))??;
        task.join()
            .map_err(|_| std::io::Error::other("mutation thread panicked"))?;
        Ok(())
    }

    #[test]
    fn callback_reads_overlap_a_held_observation() -> Result<(), Box<dyn std::error::Error>> {
        let (source, _) = tracking_sources(FilesystemProfile::Portable)?;
        let source = Arc::new(source);
        let path = native_test_path("shared.bin");
        source.create_file(&path, metadata())?;
        source.write_range(&path, 0, Bytes::from_static(b"shared"))?;
        let owner = ViewGate::callback_owner();
        let held = source.runtime.block_on(|| source.checkout.observe(owner))?;
        let reader = Arc::clone(&source);
        let (completed, receive) = std::sync::mpsc::sync_channel(1);
        let task = std::thread::spawn(move || {
            let result = reader.read_range(&path, 0, 6);
            assert!(completed.send(result).is_ok(), "read result receiver");
        });
        let read = receive.recv_timeout(Duration::from_secs(5)).map_err(|_| {
            std::io::Error::other("a callback read waited for another callback's read")
        })??;
        assert_eq!(read.as_ref(), b"shared");
        drop(held);
        task.join()
            .map_err(|_| std::io::Error::other("read thread panicked"))?;
        Ok(())
    }

    #[test]
    fn concurrent_observations_never_see_a_mutation_half_applied()
    -> Result<(), Box<dyn std::error::Error>> {
        const ROUNDS: u8 = 32;
        let (source, _) = tracking_sources(FilesystemProfile::Portable)?;
        let source = Arc::new(source);
        let pair = [
            native_test_path("first.bin"),
            native_test_path("second.bin"),
        ];
        for path in &pair {
            source.create_file(path, metadata())?;
            source.write_range(path, 0, Bytes::from_static(&[0]))?;
        }
        let pair = [source.path(&pair[0])?, source.path(&pair[1])?];
        let done = Arc::new(AtomicBool::new(false));
        let readers = (0..4)
            .map(|_| {
                let source = Arc::clone(&source);
                let pair = pair.clone();
                let done = Arc::clone(&done);
                std::thread::spawn(move || -> Result<u32, MountSourceError> {
                    let owner = ViewGate::callback_owner();
                    let mut observed = 0;
                    loop {
                        let finished = done.load(Ordering::Acquire);
                        let [first, second] = source.runtime.wait(|| async {
                            let observation = source.checkout.observe(owner).await?;
                            let mut checkout = observation.observer();
                            let mut values = [0; 2];
                            for (value, path) in values.iter_mut().zip(&pair) {
                                let range = ByteRange {
                                    offset: 0,
                                    length: 1,
                                };
                                *value = checkout
                                    .read_file_range(
                                        path,
                                        range,
                                        boundary_budget(),
                                        &source.cancellation,
                                    )
                                    .await
                                    .map_err(engine_error)?
                                    .value
                                    .bytes[0];
                                tokio::task::yield_now().await;
                            }
                            Ok(values)
                        })?;
                        assert_eq!(first, second, "an observation crossed a mutation");
                        observed += 1;
                        if finished {
                            return Ok(observed);
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        for round in 1..=ROUNDS {
            source.runtime.wait(|| async {
                let mut checkout = source.checkout.lock().await;
                for path in &pair {
                    checkout
                        .write_file(
                            path.clone(),
                            0,
                            Bytes::from(vec![round]),
                            boundary_budget(),
                            &source.cancellation,
                        )
                        .await
                        .map_err(engine_error)?;
                    tokio::task::yield_now().await;
                }
                checkout.record(&ViewChange::Everything);
                drop(checkout);
                let mut work = crate::demand::DemandWork::new(
                    boundary_budget(), &source.cancellation,
                );
                source.checkout.publish_mutation_with_carrier(
                    &mut NativeDemandCarrier::Borrowed(&mut work),
                    &source.cancellation, Some(&source.cancellation),
                    source.native_callback_guard.as_ref(),
                ).await
            })?;
        }
        done.store(true, Ordering::Release);
        for reader in readers {
            let observed = reader
                .join()
                .map_err(|_| std::io::Error::other("observation thread panicked"))??;
            assert!(observed > 0);
        }
        Ok(())
    }

    #[test]
    fn observers_cannot_mutate_or_advance_their_checkout() -> Result<(), Box<dyn std::error::Error>>
    {
        let (source, _) = tracking_sources(FilesystemProfile::Portable)?;
        let path = native_test_path("observed.bin");
        source.create_file(&path, metadata())?;
        let path = source.path(&path)?;
        let owner = ViewGate::callback_owner();
        let (write, refresh) = source.runtime.block_on(|| async {
            let observation = source.checkout.observe(owner).await?;
            let mut observer = observation.observer();
            let write = observer
                .write_file(
                    path,
                    0,
                    Bytes::from_static(b"lost"),
                    boundary_budget(),
                    &source.cancellation,
                )
                .await;
            let refresh = observer
                .refresh_head(boundary_budget(), &source.cancellation)
                .await;
            Ok::<_, MountSourceError>((write, refresh))
        })?;
        assert!(
            matches!(write, Err(failure) if matches!(failure.error, FsError::MutationNotAllowed))
        );
        assert!(
            matches!(refresh, Err(failure) if matches!(failure.error, FsError::RefreshNotAllowed))
        );
        Ok(())
    }

    #[test]
    fn native_mutation_invalidates_cache_without_rebinding_the_mount()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Portable)?;
        let binding = source.binding_epoch();
        let created = native_test_path("new-file");
        let stamp = source.view_stamp().ok_or("checkout has no view stamp")?;
        source.create_file(&created, metadata())?;
        assert_eq!(source.binding_epoch(), binding);
        assert!(!source.unchanged_since(&created, None, stamp));
        let _lease = source.acquire_binding_lease(binding)?;
        Ok(())
    }

    #[test]
    fn view_changes_invalidate_exactly_the_lookups_they_change()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Portable)?;
        let directory = native_test_path("d");
        let other = native_test_path("e");
        let child = |parent: &MountPath, name: &str| {
            let name = native_test_path(name);
            parent.child(name.components()[0].clone())
        };
        let (a, b, c, x) = (
            child(&directory, "a"),
            child(&directory, "b"),
            child(&directory, "c"),
            child(&other, "x"),
        );
        let directory_id = source
            .create_directory(&directory, metadata())?
            .node
            .file_id;
        let other_id = source.create_directory(&other, metadata())?.node.file_id;
        let a_id = source.create_file(&a, metadata())?.node.file_id;
        let b_id = source.create_file(&b, metadata())?.node.file_id;
        let x_id = source.create_file(&x, metadata())?.node.file_id;
        let current = |stamp| {
            [
                source.unchanged_since(&directory, Some(directory_id), stamp),
                source.unchanged_since(&a, Some(a_id), stamp),
                source.unchanged_since(&b, Some(b_id), stamp),
                source.unchanged_since(&c, None, stamp),
                source.unchanged_since(&other, Some(other_id), stamp),
                source.unchanged_since(&x, Some(x_id), stamp),
            ]
        };
        let stamp = source.view_stamp().ok_or("checkout has no view stamp")?;
        assert_eq!(current(stamp), [true; 6]);

        source.write_range(&a, 0, Bytes::from_static(b"a"))?;
        assert_eq!(current(stamp), [true, false, true, true, true, true]);

        let stamp = source.view_stamp().ok_or("checkout has no view stamp")?;
        source.create_file(&c, metadata())?;
        assert_eq!(current(stamp), [false, true, true, false, true, true]);

        let stamp = source.view_stamp().ok_or("checkout has no view stamp")?;
        source.remove(&b, Some(b_id))?;
        assert_eq!(current(stamp), [false, true, false, true, true, true]);
        Ok(())
    }

    #[test]
    fn host_captures_invalidate_exactly_what_they_capture() -> Result<(), Box<dyn std::error::Error>>
    {
        let (source, _) =
            shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
        let host = tempfile::tempdir()?;
        for directory in ["captured", "untouched"] {
            std::fs::create_dir(host.path().join(directory))?;
            source.create_directory(&native_test_path(directory), metadata())?;
            for name in ["a", "b"] {
                std::fs::write(host.path().join(directory).join(name), b"host")?;
            }
        }
        let child = |directory: &str, name: &str| {
            native_test_path(directory).child(native_test_path(name).components()[0].clone())
        };
        let (a, b) = (child("captured", "a"), child("captured", "b"));
        let untouched = child("untouched", "a");
        source.capture_host_paths(host.path(), std::slice::from_ref(&a))?;
        let lookup = |path: &MountPath| -> Result<_, MountSourceError> {
            Ok(source.lookup(path)?.map(|lookup| lookup.node.file_id))
        };
        let current = |stamp| -> Result<[bool; 4], MountSourceError> {
            Ok([
                source.unchanged_since(&a, lookup(&a)?, stamp),
                source.unchanged_since(&b, lookup(&b)?, stamp),
                source.unchanged_since(&untouched, lookup(&untouched)?, stamp),
                source.unchanged_since(&native_test_path("untouched"), None, stamp),
            ])
        };

        // Recapturing a changed file invalidates it alone.
        let stamp = source.view_stamp().ok_or("checkout has no view stamp")?;
        std::fs::write(host.path().join("captured").join("a"), b"changed")?;
        source.capture_host_paths(host.path(), std::slice::from_ref(&a))?;
        assert_eq!(current(stamp)?, [false, true, true, true]);

        // A first capture binds a new name in its directory.
        let stamp = source.view_stamp().ok_or("checkout has no view stamp")?;
        source.capture_host_paths(host.path(), std::slice::from_ref(&b))?;
        assert_eq!(current(stamp)?, [true, false, true, true]);

        // A subtree capture invalidates beneath its root only.
        let stamp = source.view_stamp().ok_or("checkout has no view stamp")?;
        std::fs::write(host.path().join("captured").join("b"), b"changed")?;
        source.capture_host_subtree(host.path(), &native_test_path("captured"))?;
        assert_eq!(current(stamp)?[2..], [true, true]);
        assert!(!source.unchanged_since(&b, lookup(&b)?, stamp));
        Ok(())
    }

    #[test]
    fn an_advance_records_exactly_what_it_changed() -> Result<(), Box<dyn std::error::Error>> {
        let (reader, writer) = tracking_sources(FilesystemProfile::Portable)?;
        let untouched = native_test_path("untouched");
        let advances: [&dyn Fn() -> Result<(), MountSourceError>; 2] = [
            &|| reader.runtime.block_on(|| reader.refresh_async()),
            &|| reader.runtime.block_on(|| reader.advance_to_head_async()),
        ];
        for (round, advance) in advances.into_iter().enumerate() {
            let (published, written) = (
                native_test_path(&format!("published-{round}")),
                native_test_path(&format!("written-{round}")),
            );
            let written_id = writer.create_file(&written, metadata())?.node.file_id;
            writer.sync()?;
            advance()?;

            let stamp = reader.view_stamp().ok_or("checkout has no view stamp")?;
            advance()?;
            assert_eq!(
                reader.view_stamp(),
                Some(stamp),
                "an advance to an unmoved head records nothing"
            );

            let root = reader
                .lookup(&MountPath::root())?
                .map(|lookup| lookup.node.file_id);
            writer.create_file(&published, metadata())?;
            writer.write_range(&written, 0, Bytes::from_static(b"changed"))?;
            writer.sync()?;
            advance()?;
            assert!(!reader.unchanged_since(&published, None, stamp));
            assert!(!reader.unchanged_since(&written, Some(written_id), stamp));
            assert!(!reader.unchanged_since(&MountPath::root(), root, stamp));
            assert!(
                reader.unchanged_since(&untouched, None, stamp),
                "a name the advance left alone stays cached"
            );
        }
        Ok(())
    }

    #[test]
    fn a_write_through_one_name_invalidates_every_alias() -> Result<(), Box<dyn std::error::Error>>
    {
        let source = source(FilesystemProfile::Portable)?;
        let (first, alias) = (native_test_path("first"), native_test_path("alias"));
        let file_id = source.create_file(&first, metadata())?.node.file_id;
        source.hard_link(&first, &alias)?;
        let stamp = source.view_stamp().ok_or("checkout has no view stamp")?;
        let attached = source.open_file(&first)?;
        attached.write_range(0, Bytes::from_static(b"shared"))?;
        assert!(!source.unchanged_since(&alias, Some(file_id), stamp));
        Ok(())
    }

    #[test]
    fn an_unrecorded_checkout_change_invalidates_every_lookup()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Portable)?;
        let path = native_test_path("external.bin");
        let namespace = source.path(&path)?;
        let stamp = source.view_stamp().ok_or("checkout has no view stamp")?;
        let untouched = native_test_path("untouched");
        source.runtime.block_on(|| async {
            let mut checkout = source.checkout.lock().await;
            checkout
                .create_file(
                    namespace,
                    Bytes::new(),
                    boundary_budget(),
                    &source.cancellation,
                )
                .await
                .map(|_| ())
        })?;
        assert!(!source.unchanged_since(&untouched, None, stamp));
        Ok(())
    }

    #[test]
    fn open_handle_read_up_to_clips_at_eof_without_a_separate_lookup()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Portable)?;
        let path = native_test_path("clipped-read");
        source.create_file(&path, metadata())?;
        source.write_range(&path, 0, Bytes::from_static(b"abc"))?;
        let open = source.open_file(&path)?;
        assert_eq!(open.read_up_to(0, 128)?.as_ref(), b"abc");
        assert_eq!(open.read_up_to(1, 1)?.as_ref(), b"b");
        assert!(open.read_up_to(3, 128)?.is_empty());
        assert!(open.read_up_to(u64::MAX, 128)?.is_empty());
        source.write_range(&path, 0, Bytes::from_static(b"xyz"))?;
        assert_eq!(open.read_up_to(0, 128)?.as_ref(), b"xyz");
        Ok(())
    }

    fn shared_sources_with_publication(
        profile: FilesystemProfile,
        publication: MountPublication,
    ) -> Result<(MemorySource, MemorySource), Box<dyn std::error::Error>> {
        let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
        config.profile = profile;
        let fs = Fs::memory();
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let checkout = runtime.block_on(async {
            let cancellation = CancellationToken::new();
            let volume = fs
                .create_volume(config, WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value;
            volume
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
                .await
                .map(|receipt| receipt.value)
        })?;
        let checkout = Arc::new(SharedCheckout::with_publication(checkout, publication));
        Ok((
            CheckoutMountSource::new(Arc::clone(&checkout), config)?,
            CheckoutMountSource::new(checkout, config)?,
        ))
    }

    fn tracking_sources(
        profile: FilesystemProfile,
    ) -> Result<(MemorySource, MemorySource), Box<dyn std::error::Error>> {
        let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
        config.profile = profile;
        let fs = Fs::memory();
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let (first, second) = runtime.block_on(async {
            let cancellation = CancellationToken::new();
            let volume = fs
                .create_volume(config, WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value;
            let mode = CheckoutMode {
                access: AccessMode::ReadWrite,
                consistency: ConsistencyMode::TrackingSafe,
                mutations: MutationMode::PrivateOverlay,
            };
            let first = volume
                .checkout(
                    GenerationSelector::Head,
                    mode,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await?
                .value;
            let second = volume
                .checkout(
                    GenerationSelector::Head,
                    mode,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await?
                .value;
            Ok::<_, OperationFailure<FsError>>((first, second))
        })?;
        Ok((
            CheckoutMountSource::new(Arc::new(SharedCheckout::new(first)), config)?,
            CheckoutMountSource::new(Arc::new(SharedCheckout::new(second)), config)?,
        ))
    }

    fn native_test_path(name: &str) -> MountPath {
        #[cfg(target_os = "windows")]
        let bytes = name.encode_utf16().flat_map(u16::to_le_bytes).collect();
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        let bytes = name.as_bytes().to_vec();
        MountPath::root().child(bytes)
    }

    #[test]
    fn callback_runtime_is_safe_inside_multi_and_current_thread_runtimes()
    -> Result<(), Box<dyn std::error::Error>> {
        let callback = CallbackRuntime::create()?;
        let multi = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()?;
        let current = tokio::runtime::Builder::new_current_thread().build()?;
        let profile = if cfg!(target_os = "windows") {
            FilesystemProfile::Windows
        } else {
            FilesystemProfile::Posix
        };
        for runtime in [&multi, &current] {
            let source = source(profile)?;
            runtime.block_on(async {
                let value = callback.block_on(|| async {
                    // This future is deliberately non-Send across a real reactor wait.
                    let local = std::rc::Rc::new(42_u8);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    *local
                });
                assert_eq!(value, 42);
                let path = native_test_path("runtime.bin");
                source.volume_id()?;
                let created = source.create_file(&path, metadata())?;
                source.write_range(&path, 0, Bytes::from_static(b"base"))?;
                let attached = source.open_file(&path)?;
                attached.write_range(4, Bytes::from_static(b"-open"))?;
                assert_eq!(attached.read_range(0, 9)?.as_ref(), b"base-open");
                let detached = source.detach_file(&path)?;
                source.remove(&path, Some(created.node.file_id))?;
                detached.write_range(0, Bytes::from_static(b"live"))?;
                assert_eq!(detached.read_range(0, 9)?.as_ref(), b"live-open");
                source.sync()?;
                Ok::<(), MountSourceError>(())
            })?;
        }
        Ok(())
    }

    #[test]
    fn publication_policies_are_exact_and_subtree_roots_do_not_scan_or_escape()
    -> Result<(), Box<dyn std::error::Error>> {
        let profile = if cfg!(target_os = "windows") {
            FilesystemProfile::Windows
        } else {
            FilesystemProfile::Posix
        };
        let (manual, _) = shared_sources_with_publication(profile, MountPublication::Manual)?;
        let initial = checkout_state(&manual).0;
        manual.create_directory(&native_test_path("src"), metadata())?;
        manual.flush()?;
        assert_eq!(checkout_state(&manual), (initial, true));
        manual.sync()?;
        let published = checkout_state(&manual).0;
        assert_ne!(published, initial);

        let config = {
            let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
            config.profile = profile;
            config
        };
        let root = NamespacePath::new(
            vec![manual.logical_name(native_test_path("src").components()[0].as_slice())?],
            config.limits,
        )?;
        let subtree = CheckoutMountSource::new_at(Arc::clone(&manual.checkout), config, root)?;
        let nested = native_test_path("nested.bin");
        subtree.create_file(&nested, metadata())?;
        assert!(manual.lookup(&native_test_path("nested.bin"))?.is_none());
        assert!(
            manual
                .lookup(&native_test_path("src"))?
                .is_some_and(|entry| entry.node.kind == MountNodeKind::Directory)
        );
        assert!(subtree.lookup(&nested)?.is_some());

        let (per_mutation, _) =
            shared_sources_with_publication(profile, MountPublication::PerMutation)?;
        let before = checkout_state(&per_mutation).0;
        per_mutation.create_file(&native_test_path("published.bin"), metadata())?;
        let published_directory = native_test_path("published-directory");
        let created_directory = per_mutation.create_directory(&published_directory, metadata())?;
        assert_eq!(
            per_mutation.lookup(&published_directory)?,
            Some(created_directory)
        );
        let (after, pending) = checkout_state(&per_mutation);
        assert_ne!(after, before);
        assert!(!pending);

        let (portable, _) = shared_sources_with_publication(
            FilesystemProfile::Portable,
            MountPublication::PerMutation,
        )?;
        let portable_directory = native_test_path("portable-directory");
        let created_directory = portable.create_directory(&portable_directory, metadata())?;
        assert_eq!(
            portable.lookup(&portable_directory)?,
            Some(created_directory)
        );
        Ok(())
    }

    #[test]
    fn refresh_preserves_overlapping_read_conflicts_and_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let profile = if cfg!(target_os = "windows") {
            FilesystemProfile::Windows
        } else {
            FilesystemProfile::Posix
        };
        let (reader, writer) = tracking_sources(profile)?;
        let path = native_test_path("observed.bin");
        writer.create_file(&path, metadata())?;
        writer.write_range(&path, 0, Bytes::from_static(b"first"))?;
        writer.sync()?;
        let binding_before = reader.binding_epoch();
        reader.runtime.block_on(|| reader.refresh_async())?;
        let binding_after = reader.binding_epoch();
        assert_ne!(binding_after, binding_before);
        assert_eq!(reader.read_range(&path, 0, 5)?.as_ref(), b"first");
        let observed_generation = checkout_state(&reader).0;

        writer.write_range(&path, 0, Bytes::from_static(b"other"))?;
        writer.sync()?;
        assert!(matches!(
            reader.runtime.block_on(|| reader.refresh_async()),
            Err(MountSourceError::Stale)
        ));
        assert_eq!(checkout_state(&reader).0, observed_generation);
        assert_eq!(reader.binding_epoch(), binding_after);
        assert_eq!(reader.read_range(&path, 0, 5)?.as_ref(), b"first");
        Ok(())
    }

    #[test]
    fn detached_mount_file_survives_last_binding_with_sparse_mutation_and_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        let profile = if cfg!(target_os = "windows") {
            FilesystemProfile::Windows
        } else {
            FilesystemProfile::Posix
        };
        let source = source(profile)?;
        let file = native_test_path("detached.bin");
        source.create_file(&file, metadata())?;
        source.resize(&file, 64)?;
        source.write_range(&file, 8, Bytes::from_static(b"visible"))?;

        let detached = source.detach_file(&file)?;
        let expected = source.lookup(&file)?.map(|lookup| lookup.node.file_id);
        source.remove(&file, expected)?;
        assert_eq!(source.lookup(&file)?, None);
        assert_eq!(detached.lookup()?.node.link_count, 0);
        assert_eq!(detached.read_range(8, 7)?.as_ref(), b"visible");

        detached.write_range(32, Bytes::from_static(b"open"))?;
        detached.allocate_range(16, 8, MountRangeAllocation::Preallocate { keep_size: true })?;
        let changed = FileMetadata {
            modified_ns: crate::kernel::MetadataField::Value(42),
            ..metadata()
        };
        detached.set_attributes(changed, Some(40))?;
        let lookup = detached.lookup()?;
        assert_eq!(lookup.node.logical_bytes, 40);
        assert_eq!(lookup.metadata, changed);
        assert_eq!(detached.read_range(32, 4)?.as_ref(), b"open");
        assert_eq!(detached.seek(0, MountSeekTarget::Data)?, Some(8));
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let attribute = b"user.acyclic.detached";
            detached.write_attribute(
                attribute,
                Bytes::from_static(b"retained"),
                MountAttributeWriteMode::Create,
            )?;
            assert_eq!(
                detached.read_attribute(attribute)?.as_deref(),
                Some(b"retained".as_slice())
            );
            let attributes = detached.list_attributes(None, 8)?;
            assert_eq!(attributes.names, vec![attribute.to_vec()]);
            detached.remove_attribute(attribute)?;
            assert_eq!(detached.read_attribute(attribute)?, None);
        }
        Ok(())
    }

    #[test]
    fn shared_checkout_fence_blocks_every_adapter_until_exact_resolution()
    -> Result<(), Box<dyn std::error::Error>> {
        let profile = if cfg!(target_os = "windows") {
            FilesystemProfile::Windows
        } else {
            FilesystemProfile::Posix
        };
        let (first, second) = shared_sources(profile)?;
        let operation_id = OperationId::new();
        first.runtime.block_on(|| async {
            first
                .checkout
                .lock()
                .await
                .retain_operation_id(operation_id)
        })?;
        let path = native_test_path("fenced.bin");
        assert!(matches!(
            second.create_file(&path, metadata()),
            Err(MountSourceError::Stale)
        ));
        first.runtime.block_on(|| async {
            first
                .checkout
                .lock()
                .await
                .clear_retained_operation(operation_id);
        });
        second.create_file(&path, metadata())?;
        Ok(())
    }

    #[test]
    fn detached_sparse_transfer_preserves_holes_and_allocated_spans()
    -> Result<(), Box<dyn std::error::Error>> {
        let profile = if cfg!(target_os = "windows") {
            FilesystemProfile::Windows
        } else {
            FilesystemProfile::Posix
        };
        let source = source(profile)?;
        let from = native_test_path("sparse-source.bin");
        let to = native_test_path("sparse-destination.bin");
        source.create_file(&from, metadata())?;
        source.resize(&from, 128)?;
        source.write_range(&from, 64, Bytes::from_static(b"payload"))?;
        source.create_file(&to, metadata())?;
        source.write_range(&to, 0, Bytes::from_static(b"dense"))?;
        let from = source.detach_file(&from)?;
        let to = source.detach_file(&to)?;
        let sparse = from.read_sparse_range(0, 128)?;
        to.write_sparse_range(0, &sparse)?;
        assert_eq!(to.lookup()?.node.logical_bytes, 128);
        assert_eq!(to.seek(0, MountSeekTarget::Data)?, Some(64));
        assert_eq!(to.seek(64, MountSeekTarget::Hole)?, Some(71));
        assert_eq!(to.read_range(64, 7)?.as_ref(), b"payload");
        Ok(())
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn host_capture_preserves_unobservable_named_metadata() -> Result<(), Box<dyn std::error::Error>>
    {
        let source = source(FilesystemProfile::Posix)?;
        let path = native_test_path("metadata.bin");
        source.create_file(&path, metadata())?;
        source.write_attribute(
            &path,
            b"user.acyclic.retained",
            Bytes::from_static(b"opaque"),
            MountAttributeWriteMode::Create,
        )?;
        let root = tempfile::tempdir()?;
        std::fs::write(root.path().join("metadata.bin"), b"host")?;
        source.capture_host_path(root.path(), &path)?;
        assert_eq!(
            source
                .read_attribute(&path, b"user.acyclic.retained")?
                .as_deref(),
            Some(b"opaque".as_slice())
        );
        Ok(())
    }

    #[test]
    fn attached_open_file_tracks_identity_across_unlink_hard_link_and_rename()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let profile = if cfg!(target_os = "windows") {
            FilesystemProfile::Windows
        } else {
            FilesystemProfile::Posix
        };
        let source = source(profile)?;
        let original = native_test_path("original.bin");
        let alias = native_test_path("alias.bin");
        let renamed = native_test_path("renamed.bin");
        let created = source.create_file(&original, metadata())?;
        source.write_range(&original, 0, Bytes::from_static(b"base"))?;
        let open = source.open_file(&original)?;
        source.hard_link(&original, &alias)?;
        source.remove(&original, Some(created.node.file_id))?;

        assert_eq!(source.lookup(&original)?, None);
        assert_eq!(open.lookup()?.node.link_count, 1);
        open.write_range(4, Bytes::from_static(b"-open"))?;
        assert_eq!(source.read_range(&alias, 0, 9)?.as_ref(), b"base-open");

        source.rename(&alias, &renamed, false)?;
        open.write_range(0, Bytes::from_static(b"live"))?;
        assert_eq!(source.lookup(&alias)?, None);
        assert_eq!(source.read_range(&renamed, 0, 9)?.as_ref(), b"live-open");
        assert_eq!(open.lookup()?.node.file_id, created.node.file_id);

        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let name = b"user.acyclic.attached";
            open.write_attribute(
                name,
                Bytes::from_static(b"identity"),
                MountAttributeWriteMode::Create,
            )?;
            assert_eq!(
                open.read_attribute(name)?.as_deref(),
                Some(b"identity".as_slice())
            );
            assert_eq!(
                source.read_attribute(&renamed, name)?.as_deref(),
                Some(b"identity".as_slice())
            );
        }
        let cancellation = CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(boundary_budget(), &cancellation)
            .map_err(|(error, _)| error)?;
        work.charge(crate::WorkCounters { items_examined: 7, ..crate::WorkCounters::UNCHARGED })?;
        let meter = Arc::clone(work.native_read_meter());
        let invocation = work.native_mount_work();
        assert_eq!(open.lookup_with_meter(&invocation)?.node.file_id, created.node.file_id);
        let read = open.read_up_to_with_meter(7, 8, &invocation)?;
        assert_eq!(read.bytes.as_ref(), b"en");
        drop(read);
        assert_eq!(open.seek_with_meter(0, MountSeekTarget::Data, &invocation)?, Some(0));
        let foreign = crate::NativeMaterializationWork::new(boundary_budget(), &cancellation)
            .map_err(|(error, _)| error)?;
        let foreign_input = open.read_sparse_range_with_meter(0, 9, &foreign.native_mount_work())?;
        let prefix = work.receipt();
        assert!(matches!(open.write_sparse_range_with_meter(16, foreign_input, &invocation),
            Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::InvalidLayout))));
        assert_eq!(work.receipt(), prefix);
        let resource = work.native_volume_work(boundary_budget())?;
        let retained_before_foreign = work.receipt().retained_allocation_bytes;
        let foreign_input = open.read_sparse_range_with_meter(0, 9, &resource.native_mount_work())?;
        let prefix = work.receipt();
        assert!(matches!(open.write_sparse_range_with_meter(16, foreign_input, &invocation),
            Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::InvalidLayout))));
        let after = work.receipt();
        assert_eq!(after.allocation_operations, prefix.allocation_operations);
        assert_eq!(after.backend_read_operations, prefix.backend_read_operations);
        assert_eq!(after.retained_allocation_bytes, retained_before_foreign);
        let sparse = open.read_sparse_range_with_meter(0, 9, &invocation)?;
        assert_eq!(sparse.0.logical_bytes, 9);
        assert_eq!(sparse.0.spans[0].relative_offset, 0);
        assert_eq!(sparse.0.spans[0].bytes.as_ref(), b"live-open");
        open.write_sparse_range_with_meter(16, sparse, &invocation)?;
        assert_eq!(source.read_range(&renamed, 16, 9)?.as_ref(), b"live-open");
        open.settle_with_meter(&invocation)?;
        assert!(meter.lock().work.items_examined > 7);
        let prefix = meter.lock().work;
        let live = meter.lock().live_allocation_bytes();
        cancellation.cancel();
        assert!(matches!(open.lookup_with_meter(&invocation), Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled))));
        assert!(matches!(open.read_range_with_meter(0, 4, &invocation), Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled))));
        assert_eq!(meter.lock().work, prefix);
        assert_eq!(meter.lock().live_allocation_bytes(), live);
        let cancellation = CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(boundary_budget(), &cancellation)
            .map_err(|(error, _)| error)?;
        work.charge(crate::WorkCounters { items_examined: 11, ..crate::WorkCounters::UNCHARGED })?;
        let resource = work.native_volume_work(WorkBudget {
            allocation_operations: 0, ..boundary_budget()
        })?;
        let invocation = resource.native_mount_work();
        let live = work.live_allocation_bytes();
        assert!(matches!(open.lookup_with_meter(&invocation), Err(MountSourceError::NativeWork(_))));
        assert!(work.receipt().items_examined >= 11);
        assert_eq!(work.live_allocation_bytes(), live);
        Ok(())
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn sparse_seek_and_special_nodes_preserve_canonical_native_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Posix)?;
        let file = MountPath::root().child(b"sparse".to_vec());
        source.create_file(&file, metadata())?;
        source.resize(&file, 100)?;
        source.write_range(&file, 20, Bytes::from_static(b"data"))?;
        assert_eq!(source.seek(&file, 0, MountSeekTarget::Data)?, Some(20));
        assert_eq!(source.seek(&file, 20, MountSeekTarget::Hole)?, Some(24));
        assert_eq!(source.seek(&file, 24, MountSeekTarget::Data)?, None);
        assert_eq!(source.seek(&file, 100, MountSeekTarget::Hole)?, Some(100));

        let fifo = MountPath::root().child(b"fifo".to_vec());
        let created = source.create_special(&fifo, MountNodeKind::Fifo, None, metadata())?;
        assert_eq!(created.node.kind, MountNodeKind::Fifo);
        let device = MountPath::root().child(b"device".to_vec());
        let created = source.create_special(
            &device,
            MountNodeKind::CharacterDevice,
            Some((12, 34)),
            metadata(),
        )?;
        assert_eq!(created.node.device, Some((12, 34)));
        assert_eq!(source.lookup(&device)?, Some(created));
        Ok(())
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn windows_mount_path_preserves_exact_utf16_components()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Windows)?;
        let (initial, pending) = checkout_state(&source);
        assert!(!pending);
        let name = "exact-α-😀"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        let path = MountPath::root().child(name.clone());
        let exact_metadata = FileMetadata {
            windows_attributes: crate::kernel::MetadataField::Value(0x20),
            created_ns: crate::kernel::MetadataField::Value(1_234_500),
            modified_ns: crate::kernel::MetadataField::Value(2_345_600),
            ..metadata()
        };
        let created = source.create_file(&path, exact_metadata)?;
        assert_eq!(source.lookup(&path)?, Some(created));
        let page = source.read_directory(&MountPath::root(), None, 8)?;
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].name, name);
        assert_eq!(page.entries[0].metadata, exact_metadata);
        assert!(checkout_state(&source).1);
        source.flush()?;
        source.flush()?;
        let (published, pending) = checkout_state(&source);
        assert_ne!(published, initial);
        assert!(!pending);

        let host = tempfile::tempdir()?;
        let captured_text = "captured-β.bin";
        std::fs::write(host.path().join(captured_text), b"captured")?;
        let captured_name = captured_text
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        let captured_path = MountPath::root().child(captured_name);
        source.capture_host_path(host.path(), &captured_path)?;
        assert_eq!(
            source.read_range(&captured_path, 0, 8)?.as_ref(),
            b"captured"
        );
        std::fs::remove_file(host.path().join(captured_text))?;
        source.capture_host_path(host.path(), &captured_path)?;
        assert_eq!(source.lookup(&captured_path)?, None);
        Ok(())
    }

    #[cfg(target_os = "windows")]
    #[test]
    #[ignore = "requires the live Windows ProjFS optional feature"]
    fn writable_projfs_captures_closes_renames_links_and_deletes()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = Arc::new(source(FilesystemProfile::Windows)?);
        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("projection");
        std::fs::create_dir(&destination)?;
        let mut session = crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id: source.volume_id()?,
                destination: destination.clone(),
                writable: true,
            },
            Arc::clone(&source) as Arc<dyn MountFilesystem>,
        )?;

        let first_name = "written-α.bin";
        let second_name = "renamed-β.bin";
        let linked_name = "linked-γ.bin";
        let write_status = acyclic_native_runtime::process_status(std::process::Command::new("powershell.exe")
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "[IO.File]::WriteAllBytes($env:ACYCLIC_FS_TEST_PATH, [Text.Encoding]::UTF8.GetBytes('projected'))",
            ])
            .env("ACYCLIC_FS_TEST_PATH", destination.join(first_name)), std::time::Duration::from_secs(120))?;
        assert!(write_status.success());
        // Captures of external operations are deferred until callbacks flush.
        session.flush_callbacks()?;
        let first = windows_path(first_name);
        assert_eq!(source.read_range(&first, 0, 9)?.as_ref(), b"projected");

        let rename_status = acyclic_native_runtime::process_status(std::process::Command::new("powershell.exe")
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "[IO.File]::Move($env:ACYCLIC_FS_TEST_SOURCE, $env:ACYCLIC_FS_TEST_DESTINATION)",
            ])
            .env("ACYCLIC_FS_TEST_SOURCE", destination.join(first_name))
            .env("ACYCLIC_FS_TEST_DESTINATION", destination.join(second_name)), std::time::Duration::from_secs(120))?;
        assert!(rename_status.success());
        session.flush_callbacks()?;
        let second = windows_path(second_name);
        assert_eq!(source.lookup(&first)?, None);
        assert!(source.lookup(&second)?.is_some());

        let link_status = acyclic_native_runtime::process_status(std::process::Command::new("powershell.exe")
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "New-Item -ItemType HardLink -Path $env:ACYCLIC_FS_TEST_DESTINATION -Target $env:ACYCLIC_FS_TEST_SOURCE | Out-Null",
            ])
            .env("ACYCLIC_FS_TEST_SOURCE", destination.join(second_name))
            .env("ACYCLIC_FS_TEST_DESTINATION", destination.join(linked_name)), std::time::Duration::from_secs(120))?;
        assert!(link_status.success());
        session.flush_callbacks()?;
        let linked = windows_path(linked_name);
        let second_id = source
            .lookup(&second)?
            .ok_or("renamed source absent")?
            .node
            .file_id;
        assert_eq!(
            source
                .lookup(&linked)?
                .ok_or("hard link absent")?
                .node
                .file_id,
            second_id
        );

        let delete_status = acyclic_native_runtime::process_status(
            std::process::Command::new("powershell.exe")
                .args([
                    "-NoProfile",
                    "-NonInteractive",
                    "-Command",
                    "[IO.File]::Delete($env:ACYCLIC_FS_TEST_PATH)",
                ])
                .env("ACYCLIC_FS_TEST_PATH", destination.join(second_name)),
            std::time::Duration::from_secs(120),
        )?;
        assert!(delete_status.success());
        session.flush_callbacks()?;
        assert_eq!(source.lookup(&second)?, None);
        assert!(source.lookup(&linked)?.is_some());
        assert!(session.stop()?);
        Ok(())
    }

    #[cfg(target_os = "windows")]
    fn windows_path(value: &str) -> MountPath {
        MountPath::root().child(value.encode_utf16().flat_map(u16::to_le_bytes).collect())
    }

    // Every Unix backend must support independent sequential sessions. On
    // macOS this also proves that each loopback NFS server receives its own
    // kernel-assigned port; on Linux it exercises two distinct FUSE sessions.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    #[ignore = "mounts two live Unix sessions; requires the host's native mount capability"]
    fn two_sequential_unix_mounts_in_one_process_both_stay_independently_writable()
    -> Result<(), Box<dyn std::error::Error>> {
        let source_a = Arc::new(source(FilesystemProfile::Posix)?);
        let source_b = Arc::new(source(FilesystemProfile::Posix)?);
        let temporary_a = tempfile::tempdir()?;
        let temporary_b = tempfile::tempdir()?;

        let mut mount_a = crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id: source_a.volume_id()?,
                destination: temporary_a.path().to_path_buf(),
                writable: true,
            },
            Arc::clone(&source_a) as Arc<dyn MountFilesystem>,
        )?;
        let mut mount_b = crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id: source_b.volume_id()?,
                destination: temporary_b.path().to_path_buf(),
                writable: true,
            },
            Arc::clone(&source_b) as Arc<dyn MountFilesystem>,
        )?;

        std::fs::write(temporary_a.path().join("a.txt"), b"from-a")?;
        std::fs::write(temporary_b.path().join("b.txt"), b"from-b")?;
        assert_eq!(std::fs::read(temporary_a.path().join("a.txt"))?, b"from-a");
        assert_eq!(std::fs::read(temporary_b.path().join("b.txt"))?, b"from-b");

        assert!(mount_a.stop()?);
        assert!(mount_b.stop()?);
        Ok(())
    }

    // `O_CREAT|O_EXCL` is what mkstemp, git's index.lock and editors' atomic
    // saves use. On macOS the NFS client sends it as an EXCLUSIVE4 create,
    // whose verifier the server parks in the new file's timestamps until the
    // client's follow-up SETATTR replaces them (RFC 7530 s16.16.5). The
    // second create must see the existing file, and the timestamps a caller
    // observes must be real ones, not the verifier. Removing and replacing
    // the path must not let a remembered verifier reopen the replacement.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    #[ignore = "mounts a live Unix session; requires the host's native mount capability"]
    fn exclusive_create_on_a_unix_mount_creates_once_with_real_timestamps()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = Arc::new(source(FilesystemProfile::Posix)?);
        let temporary = tempfile::tempdir()?;
        let mut mount = crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id: source.volume_id()?,
                destination: temporary.path().to_path_buf(),
                writable: true,
            },
            Arc::clone(&source) as Arc<dyn MountFilesystem>,
        )?;

        let path = temporary.path().join("index.lock");
        let before = std::time::SystemTime::now() - std::time::Duration::from_secs(60);
        let mut first = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        std::io::Write::write_all(&mut first, b"locked")?;
        drop(first);
        let second = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path);
        assert_eq!(
            second.err().map(|error| error.kind()),
            Some(std::io::ErrorKind::AlreadyExists)
        );
        let metadata = std::fs::metadata(&path)?;
        assert!(
            metadata.modified()? >= before && metadata.accessed()? >= before,
            "timestamps still carry the create verifier: {metadata:?}"
        );
        assert_eq!(std::fs::read(&path)?, b"locked");

        std::fs::remove_file(&path)?;
        std::fs::write(&path, b"replacement")?;
        let after_replacement = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path);
        assert_eq!(
            after_replacement.err().map(|error| error.kind()),
            Some(std::io::ErrorKind::AlreadyExists)
        );
        assert_eq!(std::fs::read(&path)?, b"replacement");

        assert!(mount.stop()?);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "mounts a live FUSE session; requires the host's native mount capability"]
    fn linux_negative_lookup_clears_on_create_and_invalidation()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::io::{Read as _, Seek as _, SeekFrom};

        let source = Arc::new(source(FilesystemProfile::Posix)?);
        let temporary = tempfile::tempdir()?;
        let mut mount = crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id: source.volume_id()?,
                destination: temporary.path().to_path_buf(),
                writable: true,
            },
            Arc::clone(&source) as Arc<dyn MountFilesystem>,
        )?;

        let local = temporary.path().join("created-locally");
        assert_eq!(
            std::fs::metadata(&local).err().map(|error| error.kind()),
            Some(std::io::ErrorKind::NotFound)
        );
        std::fs::write(&local, b"local")?;
        assert_eq!(std::fs::read(&local)?, b"local");

        let external = temporary.path().join("created-by-source");
        assert_eq!(
            std::fs::metadata(&external).err().map(|error| error.kind()),
            Some(std::io::ErrorKind::NotFound)
        );
        source.create_file(
            &MountPath::root().child(b"created-by-source".to_vec()),
            metadata(),
        )?;
        mount.invalidate(b"/created-by-source")?;
        assert!(
            external.is_file(),
            "invalidation must evict a negative dentry"
        );

        let nested = temporary.path().join("nested");
        std::fs::create_dir(&nested)?;
        let nested_external = nested.join("created-by-source");
        assert_eq!(
            std::fs::metadata(&nested_external)
                .err()
                .map(|error| error.kind()),
            Some(std::io::ErrorKind::NotFound)
        );
        source.create_file(
            &MountPath::root()
                .child(b"nested".to_vec())
                .child(b"created-by-source".to_vec()),
            metadata(),
        )?;
        mount.invalidate(b"/nested/created-by-source")?;
        assert!(
            nested_external.is_file(),
            "nested invalidation must evict a negative dentry"
        );

        let data_path = MountPath::root().child(b"changed-by-source".to_vec());
        source.create_file(&data_path, metadata())?;
        source.write_range(&data_path, 0, Bytes::from_static(b"before"))?;
        mount.invalidate(b"/changed-by-source")?;
        let mut opened = std::fs::File::open(temporary.path().join("changed-by-source"))?;
        let mut body = [0_u8; 6];
        opened.read_exact(&mut body)?;
        assert_eq!(&body, b"before");
        source.write_range(&data_path, 0, Bytes::from_static(b"after!"))?;
        mount.invalidate(b"/changed-by-source")?;
        opened.seek(SeekFrom::Start(0))?;
        opened.read_exact(&mut body)?;
        assert_eq!(&body, b"after!");
        drop(opened);

        assert!(mount.stop()?);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "opens a live FUSE file with O_TRUNC; requires the host's native mount capability"]
    fn linux_truncate_open_keeps_its_own_handle() -> Result<(), Box<dyn std::error::Error>> {
        use std::io::Write as _;

        let source = Arc::new(source(FilesystemProfile::Posix)?);
        let temporary = tempfile::tempdir()?;
        let mut mount = crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id: source.volume_id()?,
                destination: temporary.path().to_path_buf(),
                writable: true,
            },
            Arc::clone(&source) as Arc<dyn MountFilesystem>,
        )?;
        let path = temporary.path().join("incremental-output");
        std::fs::write(&path, b"old")?;
        for iteration in 0..8 {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&path)?;
            write!(file, "new-{iteration}")?;
            file.sync_all()?;
            assert_eq!(std::fs::read_to_string(&path)?, format!("new-{iteration}"));
        }
        assert!(mount.stop()?);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "requires a live Linux FUSE mount"]
    fn linux_hard_link_alias_survives_mounted_write_and_rename()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::MetadataExt as _;

        let source = Arc::new(source(FilesystemProfile::Posix)?);
        let temporary = tempfile::tempdir()?;
        let mut mount = crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id: source.volume_id()?,
                destination: temporary.path().to_path_buf(),
                writable: true,
            },
            Arc::clone(&source) as Arc<dyn MountFilesystem>,
        )?;
        let original = temporary.path().join("original");
        let alias = temporary.path().join("alias");
        let renamed = temporary.path().join("renamed");
        std::fs::write(&original, b"before")?;
        std::fs::hard_link(&original, &alias)?;
        assert_eq!(
            std::fs::metadata(&original)?.ino(),
            std::fs::metadata(&alias)?.ino()
        );
        assert_eq!(std::fs::metadata(&alias)?.nlink(), 2);

        std::fs::write(&alias, b"after")?;
        assert_eq!(std::fs::read(&original)?, b"after");
        std::fs::rename(&original, &renamed)?;
        assert_eq!(
            std::fs::metadata(&renamed)?.ino(),
            std::fs::metadata(&alias)?.ino()
        );
        assert_eq!(std::fs::read(&renamed)?, b"after");
        std::fs::remove_file(&alias)?;
        assert_eq!(std::fs::metadata(&renamed)?.nlink(), 1);
        assert_eq!(std::fs::read(&renamed)?, b"after");
        assert!(mount.stop()?);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "requires a live Linux FUSE mount"]
    fn linux_fsync_waits_for_an_admitted_write() -> Result<(), Box<dyn std::error::Error>> {
        use std::io::Write as _;

        let source = Arc::new(source(FilesystemProfile::Posix)?);
        let temporary = tempfile::tempdir()?;
        let mut mount = crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id: source.volume_id()?,
                destination: temporary.path().to_path_buf(),
                writable: true,
            },
            Arc::clone(&source) as Arc<dyn MountFilesystem>,
        )?;
        let path = temporary.path().join("ordered-write");
        std::fs::write(&path, b"old")?;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;
        let mut writer = file.try_clone()?;
        let syncer = file.try_clone()?;
        let gate = super::super::fuse::pause_next_write_after_admission();
        let writer_thread = std::thread::spawn(move || writer.write_all(b"new"));
        assert!(
            gate.wait_until_admitted(Duration::from_secs(5)),
            "write must reach the per-handle gate"
        );

        let (started, started_rx) = std::sync::mpsc::sync_channel(1);
        let (finished, finished_rx) = std::sync::mpsc::sync_channel(1);
        let sync_thread = std::thread::spawn(move || {
            let _ = started.send(());
            let result = syncer.sync_all();
            let _ = finished.send(result);
        });
        started_rx.recv_timeout(Duration::from_secs(5))?;
        assert!(
            finished_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "fsync must not overtake a write admitted on the same handle"
        );
        assert!(gate.release(), "release paused write");
        writer_thread
            .join()
            .map_err(|_| std::io::Error::other("mounted writer panicked"))??;
        sync_thread
            .join()
            .map_err(|_| std::io::Error::other("mounted fsync panicked"))?;
        finished_rx.recv_timeout(Duration::from_secs(5))??;
        drop(file);
        assert_eq!(std::fs::read(&path)?, b"new");
        assert!(
            !checkout_state(&source).1,
            "fsync published the admitted write"
        );
        assert!(mount.stop()?);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "requires a live Linux FUSE mount"]
    fn linux_fsync_on_clean_descriptor_publishes_other_descriptor_write()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::io::Write as _;

        let source = Arc::new(source(FilesystemProfile::Posix)?);
        let temporary = tempfile::tempdir()?;
        let mut mount = crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id: source.volume_id()?,
                destination: temporary.path().to_path_buf(),
                writable: true,
            },
            Arc::clone(&source) as Arc<dyn MountFilesystem>,
        )?;
        let path = temporary.path().join("cross-descriptor-fsync");
        std::fs::write(&path, b"old")?;
        let syncer = std::fs::OpenOptions::new().read(true).open(&path)?;
        let mut writer = std::fs::OpenOptions::new().write(true).open(&path)?;
        writer.write_all(b"new")?;
        assert!(checkout_state(&source).1, "write remains pending");
        syncer.sync_all()?;
        assert!(
            !checkout_state(&source).1,
            "explicit fsync published the write"
        );
        assert_eq!(std::fs::read(&path)?, b"new");
        writer.write_all(b"2")?;
        assert!(checkout_state(&source).1, "second write remains pending");
        std::fs::File::open(temporary.path())?.sync_all()?;
        assert!(
            !checkout_state(&source).1,
            "directory fsync published the write"
        );
        drop(writer);
        drop(syncer);
        assert!(mount.stop()?);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "requires a live Linux FUSE mount"]
    fn linux_concurrent_truncates_do_not_fence_unrelated_files()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::io::Write as _;

        let source = Arc::new(source(FilesystemProfile::Posix)?);
        let temporary = tempfile::tempdir()?;
        let mut mount = crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id: source.volume_id()?,
                destination: temporary.path().to_path_buf(),
                writable: true,
            },
            Arc::clone(&source) as Arc<dyn MountFilesystem>,
        )?;

        // Unrelated mounted mutations may advance the global cache epoch
        // while another file is opening. They must not fence its attached
        // handle or make O_TRUNC report ESTALE.
        let barrier = Arc::new(std::sync::Barrier::new(9));
        let mut workers = Vec::new();
        for worker in 0..8 {
            let path = temporary.path().join(format!("concurrent-{worker}"));
            std::fs::write(&path, b"old")?;
            let barrier = Arc::clone(&barrier);
            workers.push(std::thread::spawn(move || -> std::io::Result<()> {
                barrier.wait();
                for iteration in 0..8 {
                    let mut file = std::fs::OpenOptions::new()
                        .write(true)
                        .truncate(true)
                        .open(&path)
                        .map_err(|error| {
                            std::io::Error::other(format!(
                                "concurrent open {worker}/{iteration}: {error}"
                            ))
                        })?;
                    write!(file, "new-{worker}-{iteration}")?;
                    file.sync_all()?;
                }
                assert_eq!(std::fs::read_to_string(&path)?, format!("new-{worker}-7"));
                Ok(())
            }));
        }
        barrier.wait();
        for worker in workers {
            worker
                .join()
                .map_err(|_| std::io::Error::other("mounted writer panicked"))??;
        }
        assert!(mount.stop()?);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "mounts a live FUSE session and invokes the installed Rust compiler"]
    fn rustc_compiles_inside_a_linux_mount() -> Result<(), Box<dyn std::error::Error>> {
        let source = Arc::new(source(FilesystemProfile::Posix)?);
        let temporary = tempfile::tempdir()?;
        let mut mount = crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id: source.volume_id()?,
                destination: temporary.path().to_path_buf(),
                writable: true,
            },
            Arc::clone(&source) as Arc<dyn MountFilesystem>,
        )?;

        std::fs::write(temporary.path().join("main.rs"), b"fn main() {}\n")?;
        let output = acyclic_native_runtime::process_output(
            std::process::Command::new("rustc")
                .current_dir(temporary.path())
                .args(["--edition", "2021", "main.rs", "-o", "main"]),
            std::time::Duration::from_secs(120),
            8 * 1024 * 1024,
        )?;
        assert!(
            output.status.success(),
            "rustc failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(temporary.path().join("main").is_file());
        assert!(
            acyclic_native_runtime::process_status(
                &mut std::process::Command::new(temporary.path().join("main")),
                std::time::Duration::from_secs(120)
            )?
            .success(),
            "compiled executable did not run from the mount"
        );

        assert!(mount.stop()?);
        Ok(())
    }

    // Simultaneous callers must receive independent servers, callback state,
    // kernel mounts, and teardown lifecycles.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    #[ignore = "mounts two live Unix sessions concurrently; requires the host's native mount capability"]
    fn two_concurrent_unix_mounts_in_one_process_remain_independent()
    -> Result<(), Box<dyn std::error::Error>> {
        let source_a = Arc::new(source(FilesystemProfile::Posix)?);
        let source_b = Arc::new(source(FilesystemProfile::Posix)?);
        let temporary_a = tempfile::tempdir()?;
        let temporary_b = tempfile::tempdir()?;
        let request_a = crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id: source_a.volume_id()?,
            destination: temporary_a.path().to_path_buf(),
            writable: true,
        };
        let request_b = crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id: source_b.volume_id()?,
            destination: temporary_b.path().to_path_buf(),
            writable: true,
        };
        let dyn_source_a = Arc::clone(&source_a) as Arc<dyn MountFilesystem>;
        let dyn_source_b = Arc::clone(&source_b) as Arc<dyn MountFilesystem>;

        let (result_a, result_b) = std::thread::scope(|scope| {
            let handle_a = scope.spawn(|| crate::mount_native(request_a, dyn_source_a));
            let handle_b = scope.spawn(|| crate::mount_native(request_b, dyn_source_b));
            Ok::<_, &'static str>((
                handle_a.join().map_err(|_| "mount a thread panicked")?,
                handle_b.join().map_err(|_| "mount b thread panicked")?,
            ))
        })?;

        let mut mount_a = result_a?;
        let mut mount_b = result_b?;
        std::fs::write(temporary_a.path().join("a.txt"), b"concurrent-a")?;
        std::fs::write(temporary_b.path().join("b.txt"), b"concurrent-b")?;
        assert_eq!(
            std::fs::read(temporary_a.path().join("a.txt"))?,
            b"concurrent-a"
        );
        assert_eq!(
            std::fs::read(temporary_b.path().join("b.txt"))?,
            b"concurrent-b"
        );
        assert!(!temporary_a.path().join("b.txt").exists());
        assert!(!temporary_b.path().join("a.txt").exists());
        assert!(mount_a.stop()?);
        assert!(mount_b.stop()?);
        Ok(())
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn run_unix_mount_cross_process_worker() -> Result<(), Box<dyn std::error::Error>> {
        let destination =
            std::env::var_os("ACYCLIC_FS_UNIX_CHILD_MOUNT").ok_or("child mount path is absent")?;
        let ready =
            std::env::var_os("ACYCLIC_FS_UNIX_CHILD_READY").ok_or("child ready path is absent")?;
        let release = std::env::var_os("ACYCLIC_FS_UNIX_CHILD_RELEASE")
            .ok_or("child release path is absent")?;
        let identity = std::env::var("ACYCLIC_FS_UNIX_CHILD_ID")?;
        let source = Arc::new(source(FilesystemProfile::Posix)?);
        let mut mount = crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id: source.volume_id()?,
                destination: destination.clone().into(),
                writable: true,
            },
            source as Arc<dyn MountFilesystem>,
        )?;
        let filename = format!("{identity}.txt");
        std::fs::write(
            std::path::Path::new(&destination).join(filename),
            identity.as_bytes(),
        )?;
        std::fs::write(&ready, identity.as_bytes())?;
        wait_for_path(
            std::path::Path::new(&release),
            std::time::Duration::from_secs(10),
        )?;
        assert!(mount.stop()?);
        Ok(())
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    #[ignore = "starts two real mount-owning processes; requires the host's native mount capability"]
    fn unix_mounts_in_distinct_processes_remain_independent()
    -> Result<(), Box<dyn std::error::Error>> {
        if std::env::var_os("ACYCLIC_FS_UNIX_CHILD_MOUNT").is_some() {
            return run_unix_mount_cross_process_worker();
        }
        let temporary = tempfile::tempdir()?;
        let mount_a = temporary.path().join("mount-a");
        let mount_b = temporary.path().join("mount-b");
        std::fs::create_dir(&mount_a)?;
        std::fs::create_dir(&mount_b)?;
        let ready_a = temporary.path().join("ready-a");
        let ready_b = temporary.path().join("ready-b");
        let release = temporary.path().join("release");
        let executable = std::env::current_exe()?;
        let worker =
            "native_mount::adapter::tests::unix_mounts_in_distinct_processes_remain_independent";
        let mut child_a = acyclic_native_runtime::spawn_process_tree(
            std::process::Command::new(&executable)
                .args(["--ignored", "--exact", worker, "--test-threads=1"])
                .env("ACYCLIC_FS_UNIX_CHILD_MOUNT", &mount_a)
                .env("ACYCLIC_FS_UNIX_CHILD_READY", &ready_a)
                .env("ACYCLIC_FS_UNIX_CHILD_RELEASE", &release)
                .env("ACYCLIC_FS_UNIX_CHILD_ID", "a"),
        )?;
        let mut child_b = acyclic_native_runtime::spawn_process_tree(
            std::process::Command::new(&executable)
                .args(["--ignored", "--exact", worker, "--test-threads=1"])
                .env("ACYCLIC_FS_UNIX_CHILD_MOUNT", &mount_b)
                .env("ACYCLIC_FS_UNIX_CHILD_READY", &ready_b)
                .env("ACYCLIC_FS_UNIX_CHILD_RELEASE", &release)
                .env("ACYCLIC_FS_UNIX_CHILD_ID", "b"),
        )?;

        let validation = (|| -> Result<(), Box<dyn std::error::Error>> {
            wait_for_path(&ready_a, std::time::Duration::from_secs(10))?;
            wait_for_path(&ready_b, std::time::Duration::from_secs(10))?;
            assert_eq!(std::fs::read(mount_a.join("a.txt"))?, b"a");
            assert_eq!(std::fs::read(mount_b.join("b.txt"))?, b"b");
            assert!(!mount_a.join("b.txt").exists());
            assert!(!mount_b.join("a.txt").exists());
            Ok(())
        })();
        std::fs::write(&release, b"release")?;
        let status_a = wait_for_child(&mut child_a, std::time::Duration::from_secs(10))?;
        let status_b = wait_for_child(&mut child_b, std::time::Duration::from_secs(10))?;
        validation?;
        assert!(status_a.success(), "first mount process failed: {status_a}");
        assert!(
            status_b.success(),
            "second mount process failed: {status_b}"
        );
        Ok(())
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn wait_for_path(
        path: &std::path::Path,
        timeout: std::time::Duration,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let deadline = std::time::Instant::now() + timeout;
        while !path.exists() {
            if std::time::Instant::now() >= deadline {
                return Err(format!("timed out waiting for {}", path.display()).into());
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        Ok(())
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn wait_for_child(
        child: &mut acyclic_native_runtime::ProcessTree,
        timeout: std::time::Duration,
    ) -> Result<std::process::ExitStatus, Box<dyn std::error::Error>> {
        let result = child.wait(timeout);
        child.terminate()?;
        Ok(result?)
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn posix_mount_path_preserves_non_utf8_components() -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Posix)?;
        let (initial, pending) = checkout_state(&source);
        assert!(!pending);
        let name = vec![b'r', 0xff, b'w'];
        let path = MountPath::root().child(name.clone());
        let created = source.create_file(&path, metadata())?;
        assert_eq!(source.lookup(&path)?, Some(created));
        source.write_attribute(
            &path,
            b"user.acyclic",
            Bytes::from_static(b"native-xattr"),
            MountAttributeWriteMode::Create,
        )?;
        assert_eq!(
            source.read_attribute(&path, b"user.acyclic")?.as_deref(),
            Some(b"native-xattr".as_slice())
        );
        let attributes = source.list_attributes(&path, None, 8)?;
        assert_eq!(attributes.names, vec![b"user.acyclic".to_vec()]);
        assert_eq!(attributes.next_cursor, None);
        source.remove_attribute(&path, b"user.acyclic")?;
        assert_eq!(source.read_attribute(&path, b"user.acyclic")?, None);
        let page = source.read_directory(&MountPath::root(), None, 8)?;
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].name, name);
        assert!(checkout_state(&source).1);
        source.flush()?;
        source.flush()?;
        let (published, pending) = checkout_state(&source);
        assert_ne!(published, initial);
        assert!(!pending);
        Ok(())
    }

    // The FUSE release callback flushes (seals) the checkout before the
    // kernel's next namespace mutation arrives, so mutations must stay
    // valid across a publication boundary.
    #[test]
    fn rename_after_flush_preserves_namespace() -> Result<(), Box<dyn std::error::Error>> {
        let profile = if cfg!(target_os = "windows") {
            FilesystemProfile::Windows
        } else {
            FilesystemProfile::Posix
        };
        let source = source(profile)?;
        let path = native_test_path("payload.bin");
        source.create_file(&path, metadata())?;
        let open = source.open_file(&path)?;
        open.write_range(0, Bytes::from_static(b"BRAVO"))?;
        drop(open);
        source.flush()?;
        source.flush()?;
        let renamed = native_test_path("renamed.bin");
        source.rename(&path, &renamed, true)?;
        assert!(source.lookup(&path)?.is_none());
        let landed = source.lookup(&renamed)?;
        assert!(landed.is_some());
        Ok(())
    }

    #[test]
    fn preexisting_entry_survives_published_link_unlink_and_recreate_cycles()
    -> Result<(), Box<dyn std::error::Error>> {
        let profile = if cfg!(target_os = "windows") {
            FilesystemProfile::Windows
        } else {
            FilesystemProfile::Posix
        };
        let source = source(profile)?;
        let seed = native_test_path("shared-seed.bin");
        source.create_file(&seed, metadata())?;
        source.write_range(&seed, 0, Bytes::from_static(b"seed"))?;
        source.flush()?;

        let payload = native_test_path("payload.bin");
        source.create_file(&payload, metadata())?;
        source.write_range(&payload, 0, Bytes::from_static(b"BRAVO"))?;
        source.flush()?;
        let renamed = native_test_path("renamed.bin");
        source.rename(&payload, &renamed, true)?;
        let link = native_test_path("link.bin");
        source.hard_link(&renamed, &link)?;
        let renamed_id = source
            .lookup(&renamed)?
            .ok_or("renamed file is absent")?
            .node
            .file_id;
        source.remove(&renamed, Some(renamed_id))?;
        let link_id = source
            .lookup(&link)?
            .ok_or("linked file is absent")?
            .node
            .file_id;
        source.remove(&link, Some(link_id))?;
        source.create_file(&link, metadata())?;
        source.write_range(&link, 0, Bytes::from_static(b"replacement"))?;
        source.flush()?;

        assert_eq!(source.read_range(&seed, 0, 4)?.as_ref(), b"seed");
        let page = source.read_directory(&MountPath::root(), None, 16)?;
        let mut names = page
            .entries
            .into_iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>();
        names.sort();
        assert_eq!(
            names,
            vec![
                native_test_path("link.bin").components()[0].clone(),
                native_test_path("shared-seed.bin").components()[0].clone(),
            ]
        );
        assert_eq!(page.next_cursor, None);
        Ok(())
    }

    // Host-side capture of raw-byte names requires a host filesystem that
    // admits them; APFS enforces valid UTF-8 and rejects such names with
    // EILSEQ before this crate is involved, so the host half runs on Linux
    // only.
    #[cfg(target_os = "linux")]
    #[test]
    fn host_capture_preserves_non_utf8_names() -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let source = source(FilesystemProfile::Posix)?;
        let host = tempfile::tempdir()?;
        let captured_name = vec![b'c', 0xfe, b'p'];
        let host_path = host
            .path()
            .join(std::ffi::OsString::from_vec(captured_name.clone()));
        std::fs::write(&host_path, b"captured")?;
        let captured_path = MountPath::root().child(captured_name);
        source.capture_host_path(host.path(), &captured_path)?;
        assert_eq!(
            source.read_range(&captured_path, 0, 8)?.as_ref(),
            b"captured"
        );
        std::fs::remove_file(host_path)?;
        source.capture_host_path(host.path(), &captured_path)?;
        assert_eq!(source.lookup(&captured_path)?, None);
        Ok(())
    }
}

fn capture_meter(
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<super::NativeMountMeterWork, OperationFailure<MountSourceError>> {
    crate::NativeMaterializationWork::new(budget, cancellation)
        .map(|work| super::NativeMountMeterWork::from_native_materialization_work(&work))
        .map_err(|(error, completed)| OperationFailure::new(MountSourceError::NativeWork(error), completed))
}

fn capture_request(
    meter: &Arc<crate::demand::NativeDemandReadMeter>,
) -> Result<Arc<CaptureCommitGate>, MountSourceError> {
    use super::lazy::mount_demand_error;
    let mut work = meter.lock();
    let before = work.live_allocation_bytes();
    let cancellation = work.new_cancellation().map_err(MountSourceError::Work)?;
    let bytes = std::mem::size_of::<CaptureCommitGate>() as u64;
    if let Err(error) = work.allocate(bytes) {
        drop(cancellation);
        let owned = work.live_allocation_bytes() - before;
        work.release(owned).map_err(mount_demand_error)?;
        return Err(mount_demand_error(error));
    }
    Ok(Arc::new(CaptureCommitGate {
        cancellation, state: AtomicU8::new(CAPTURE_PENDING),
        slot: std::sync::atomic::AtomicUsize::new(0),
        _allocation: CaptureOwnedAllocation { meter: Arc::clone(meter), bytes },
    }))
}

fn capture_name_layout<S: CaptureInputScratch>(
    bytes: &[u8],
    profile: FilesystemProfile,
    limits: VolumeLimits,
    scratch: &mut S,
) -> Result<(NameEncoding, usize), MountSourceError> {
    scratch.charge(crate::WorkCounters {
        source_path_components: 1, items_examined: 1, ..crate::WorkCounters::UNCHARGED
    })?;
    match (profile, std::env::consts::OS) {
        (FilesystemProfile::Posix, "linux" | "macos") => Ok((NameEncoding::PosixBytes, bytes.len())),
        (FilesystemProfile::Windows, "windows") => Ok((NameEncoding::WindowsUtf16Le, bytes.len())),
        (FilesystemProfile::Portable | FilesystemProfile::Browser, "linux" | "macos") => {
            let value = std::str::from_utf8(bytes).map_err(|_| scratch.unsupported(
                "portable volume cannot represent a non-UTF-8 native name",
            ))?;
            let length = LogicalName::validate_unicode(NameEncoding::Utf8, value, limits.maximum_component_bytes)
                .map_err(|error| scratch.engine_error(crate::kernel::NamespacePathError::Name(error)))?;
            Ok((NameEncoding::Utf8, length))
        }
        (FilesystemProfile::Portable | FilesystemProfile::Browser, "windows") => {
            let units = crate::kernel::types::utf16le_units(bytes).ok_or_else(|| scratch.invalid(
                "native UTF-16 name has an odd byte length",
            ))?;
            let mut length = 0_usize;
            for value in char::decode_utf16(units) {
                scratch.items(1)?;
                let value = value.map_err(|_| scratch.unsupported(
                    "portable volume cannot represent an unpaired UTF-16 native name",
                ))?;
                length = length.checked_add(value.len_utf8())
                    .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            }
            Ok((NameEncoding::Utf8, length))
        }
        _ => Err(scratch.unsupported(
            "volume name profile is incompatible with this native mount",
        )),
    }
}

fn capture_copy_name<S: CaptureInputScratch>(
    encoding: NameEncoding,
    bytes: &[u8],
    limits: VolumeLimits,
    scratch: &mut S,
) -> Result<LogicalName, MountSourceError> {
    let delta = crate::WorkCounters { bytes_copied: bytes.len() as u64, ..crate::WorkCounters::UNCHARGED };
    scratch.admit(delta)?;
    let mut owned = scratch.vec(bytes.len())?;
    owned.extend_from_slice(bytes);
    scratch.record(delta)?;
    LogicalName::new(encoding, owned, limits.maximum_component_bytes)
        .map_err(|error| scratch.engine_error(crate::kernel::NamespacePathError::Name(error)))
}

fn capture_native_name<S: CaptureInputScratch>(
    bytes: &[u8],
    encoding: NameEncoding,
    length: usize,
    profile: FilesystemProfile,
    limits: VolumeLimits,
    scratch: &mut S,
) -> Result<LogicalName, MountSourceError> {
    if cfg!(windows) && matches!(profile, FilesystemProfile::Portable | FilesystemProfile::Browser) {
        scratch.admit(crate::WorkCounters { bytes_encoded: length as u64, ..crate::WorkCounters::UNCHARGED })?;
        let mut owned = scratch.vec(length)?;
        let units = crate::kernel::types::utf16le_units(bytes).ok_or(MountSourceError::Stale)?;
        for value in char::decode_utf16(units) {
            scratch.items(1)?;
            let value = value.map_err(|_| MountSourceError::Stale)?;
            let mut buffer = [0_u8; 4];
            let encoded = value.encode_utf8(&mut buffer);
            owned.extend_from_slice(encoded.as_bytes());
            scratch.record(crate::WorkCounters { bytes_encoded: encoded.len() as u64, ..crate::WorkCounters::UNCHARGED })?;
        }
        LogicalName::new(encoding, owned, limits.maximum_component_bytes)
            .map_err(|error| scratch.engine_error(crate::kernel::NamespacePathError::Name(error)))
    } else {
        capture_copy_name(encoding, bytes, limits, scratch)
    }
}

fn capture_effect_bytes(
    effect: &ViewEffect,
    work: &mut crate::demand::DemandWork,
) -> Result<u64, MountSourceError> {
    use super::lazy::mount_items;
    let bytes = effect.nodes.capacity().checked_mul(std::mem::size_of::<FileId>())
        .and_then(|bytes| effect.names.capacity().checked_mul(std::mem::size_of::<(NamespacePath, LogicalName)>())
            .and_then(|names| bytes.checked_add(names)))
        .and_then(|bytes| effect.subtrees.capacity().checked_mul(std::mem::size_of::<NamespacePath>())
            .and_then(|subtrees| bytes.checked_add(subtrees)))
        .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
    let mut bytes = u64::try_from(bytes).map_err(|_| MountSourceError::Work(crate::WorkError::Overflow))?;
    for (path, name) in &effect.names {
        mount_items(work, path.depth() + 1)?;
        bytes = bytes.checked_add(path.retained_bytes())
            .and_then(|bytes| bytes.checked_add(name.retained_bytes() as u64))
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
    }
    for path in &effect.subtrees {
        mount_items(work, path.depth() + 1)?;
        bytes = bytes.checked_add(path.retained_bytes())
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
    }
    Ok(bytes)
}

fn capture_mount_error(error: crate::CaptureError) -> MountSourceError {
    match error {
        crate::CaptureError::Work(error) => MountSourceError::Work(error),
        crate::CaptureError::Allocation(error) => MountSourceError::Allocation(error),
        crate::CaptureError::NativeKernel(error) => MountSourceError::NativeWork(error),
        crate::CaptureError::Kernel(error) => MountSourceError::Filesystem(error),
        crate::CaptureError::Cancelled => MountSourceError::Cancelled,
        crate::CaptureError::StaleVersion => MountSourceError::Stale,
        error => engine_error(error),
    }
}
