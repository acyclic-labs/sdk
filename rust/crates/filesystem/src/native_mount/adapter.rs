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
pub(super) struct LookupObserverOwned<'a, 'w, A, O> {
    value: Option<crate::facade::CheckoutObserver<'a, A, O>>,
    work: NativeDemandCarrier<'w>,
    bytes: u64,
    finished: bool,
}

/// One access seam to the same operative native invocation.
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

    /// Projects only an actual complete native capability, including its original resource allowance; a raw meter cannot recreate that authority.
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
    if matches!(&*carrier, NativeDemandCarrier::Native(_)) {
        return ledger.record(change, carrier, &mut super::view_ledger::ViewLedgerAdmission::default());
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
    let value = if let Some(original) = original_native.as_ref() {
        let mut inspector = checkout.inspector_with_native_work(original, cancellation)
            .map_err(MountSourceError::Filesystem)?;
        let accepted = inspector.lookup_no_follow_with_native_work(path, original, cancellation)
            .await.map_err(MountSourceError::Filesystem);
        inspector.proof_data_take_completed_work();
        accepted?
    } else {
        let (budget, prior_live) = carrier.with_work(|work| {
            Ok::<_, MountSourceError>((
                work.remaining().map_err(mount_demand_error)?, work.live_allocation_bytes(),
            ))
        })?;
        let inspected = checkout.inspector_measured(budget, cancellation);
        let inspector = carrier.accept_with_prior_live(inspected, prior_live, mount_engine_error)?;
        let mut inspector = LookupObserverOwned::new_in(inspector, carrier.reborrow())?;
        let (budget, prior_live) = inspector.work.with_work(|work| {
            Ok::<_, MountSourceError>((
                work.remaining().map_err(mount_demand_error)?, work.live_allocation_bytes(),
            ))
        })?;
        let lookup = inspector.lookup_no_follow(path, budget, cancellation).await;
        let accepted = inspector.work.accept_with_prior_live(lookup, prior_live, mount_engine_error);
        inspector.receipt_delivered();
        let finished = inspector.finish();
        let value = accepted?;
        finished?;
        value
    };
    carrier.with_work(|work| {
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
        _ => unreachable!("native guarded callback constructor or funded denial"),
    })
}

/// The original source's actual guard, not a substitute filesystem authority.
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
    /// Called after merging each SDK receipt, even when that merge returns a typed failure.
    pub(super) fn receipt_delivered(&mut self) {
        self.proof_data_take_completed_work();
    }

    /// Merges only undelivered proof work before reconciling new DATA.
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

/// Moves the complete caller invocation into a native callback and restores it only after the callback's owned inputs, futures, and accepted I/O retire.
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
        let mut meter = Arc::new(crate::demand::NativeDemandReadMeter(StdMutex::new(
            admission.work.native_queue_shell(),
        )));
        let target = Arc::get_mut(&mut meter).expect("new native meter has exclusive custody")
            .0.get_mut().unwrap_or_else(PoisonError::into_inner);
        admission.retain();
        *target = original.take_for_native_queue();
        Ok(Self { original, meter: Some(meter) })
    }

    /// Runs a scoped callback whose result retains no meter alias.
    pub(super) fn run<T>(
        mut self,
        operation: impl FnOnce(&Arc<crate::demand::NativeDemandReadMeter>) -> T,
    ) -> T {
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
    _allocation: crate::NativeAllocation,
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
    copied: crate::materializer::NativeCopyReservation,
}

impl CaptureRegistrationRetirement {
    fn new(work: &crate::NativeMaterializationWork) -> Result<Self, MountSourceError> {
        let items = work.reserve_items(2).map_err(MountSourceError::NativeWork)?;
        let copied = work.reserve_copy(std::mem::size_of::<CaptureSourceSlot>() as u64)
            .map_err(MountSourceError::NativeWork)?;
        Ok(Self { items, copied })
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
        let copied_error = retirement.copied.record(copied as u64).err();
        // Refund unused visits before the producer lease announces quiescence.
        drop(retirement.items);
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
#[derive(Clone)]
pub(super) struct CallbackRuntime {
    handle: tokio::runtime::Handle,
}

// Existing-only access must not implicitly initialize a bounded invocation.
static CALLBACK_RUNTIME: OnceLock<Result<tokio::runtime::Runtime, Arc<std::io::Error>>> = OnceLock::new();

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
    fn poll_to_completion_measured<F: Future + Unpin>(
        future: F, meter: Option<&dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<F::Output> {
        let _allocation = CaptureRuntimeAllocation::new(meter, std::mem::size_of::<Self>())?;
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

/// Initializes the process-shared mount callback runtime outside a bounded preparation invocation.
pub fn initialize_native_mount_services() -> Result<(), NativeMountError> {
    CallbackRuntime::create().map(|_| ())
}

impl CallbackRuntime {
    pub(super) fn create() -> Result<Self, NativeMountError> {
        CALLBACK_RUNTIME.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .thread_name("acyclic-fs-mount")
                .build()
                .map_err(Arc::new)
        });
        Self::existing()
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

    /// Polls the original producer in place, retaining its cancellation and quiescence protocol.
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

    /// Checks authentic callback authority on this existing runtime before the provider allocates its exchange frame or starts a source effect.
    pub(super) fn verify_native_callback_guard(
        &self, guard: &dyn super::NativeMountCallbackGuard,
        work: &super::NativeMountMeterWork,
    ) -> Result<(), NativeMountError> {
        self.verify_native_callback_role(guard, work, false)
    }

    /// Checks authored WRITE on the same runtime and original callback binding.
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
        let poll = || {
            // Built inside the runtime's context, which timers need.
            let _runtime = self.handle.enter();
            // This thread serves exactly this callback until it completes.
            let _inline = acyclic_native_runtime::InlineBlocking::enter();
            ParkedCallback::poll_to_completion_measured(tokio::task::unconstrained(in_heap(create)), None)
                .expect("unmetered callback has no denied allocations")
        };
        match tokio::runtime::Handle::try_current() {
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

/// One process-local serialization and publication-fencing boundary for every adapter, watcher, and transport view of the same checkout.
pub struct SharedCheckout<A, O> {
    state: tokio::sync::RwLock<SharedCheckoutState<A, O>>,
    revision: Arc<AtomicU64>,
    ledger: Arc<ViewLedger>,
    view_gate: Arc<ViewGate>,
    publishing: StdMutex<bool>,
    pending: StdMutex<Vec<Arc<PendingCreate>>>,
    pending_allocation: StdMutex<Option<super::NativeMountAllocationCustody>>,
    publication_return: StdMutex<Option<SharedCheckoutState<A, O>>>,
    publication_changed: tokio::sync::Notify,
    _native_revision_allocation: Option<crate::NativeAllocation>,
    _native_allocation: Option<crate::NativeAllocation>,
}

const PENDING_CREATE_BYTES: u64 = 1 << 20;

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

/// Ready locks need no waiter allocation.
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
            if let Some(wait) = caller_wait.as_mut() { wait.retire_waker(); }
            if let Some(wait) = lifetime_wait.as_mut() { wait.retire_waker(); }
            if self.native.is_some() {
                drop(caller_wait);
                drop(lifetime_wait);
                return Ok(());
            }
            let reserved = std::mem::take(&mut self.reserved_items);
            self.carrier.with_work(|work| {
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

/// A queued regular-file creation that settles with its first bytes.
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

enum PendingBytes {
    Mutable { bytes: Vec<u8>, allocation: Option<crate::NativeAllocation> },
    Shared { bytes: OwnedBytes, capacity: usize, original: Option<crate::NativeMaterializationWork> },
}

impl PendingBytes {
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
        let Self::Mutable { bytes, .. } = self else {
            unreachable!("pending capacity is prepared before mutation");
        };
        bytes
    }

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
        grown.extend_from_slice(&self[..copied]);
        scratch.record(crate::WorkCounters { bytes_copied: copied as u64, ..crate::WorkCounters::UNCHARGED })?;
        Ok(Some(Self::Mutable { bytes: grown, allocation: Some(scratch.retain()) }))
    }
}

impl Deref for PendingBytes {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Mutable { bytes, .. } => bytes,
            Self::Shared { bytes, .. } => bytes,
        }
    }
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
    Complete,
}

impl PendingCreate {
    fn state(&self) -> std::sync::MutexGuard<'_, PendingState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

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
        let prepared_bytes = bytes.share(original)?;
        let PendingState::Pending { metadata, bytes } =
            std::mem::replace(state, PendingState::Complete)
        else { return Err(MountSourceError::Stale); };
        let change = GroupedChange::CreateFile {
            path, metadata: prepared_metadata, file_id: self.file_id, bytes: prepared_bytes,
        };
        *state = PendingState::Applying { metadata, bytes };
        Ok(Some(change))
    }

    fn restore_applying_retained(&self) {
        let mut state = self.state();
        match std::mem::replace(&mut *state, PendingState::Complete) {
            PendingState::Applying { metadata, bytes, .. } => {
                *state = PendingState::Pending { metadata, bytes };
            }
            other => *state = other,
        }
    }

    fn finish_applying(&self, applied: Result<GroupedOutcome, MountSourceError>) {
        if let Err(error) = applied {
            let own_alias = matches!(&error, MountSourceError::Retained(cause)
                if Arc::as_ptr(cause) as *const () == self as *const Self as *const ());
            if own_alias {
                assert!(self.failure.get().is_some(), "original pending error cause exists");
            } else {
                assert!(self.failure.set(error).is_ok(), "original pending outcome delivered once");
            }
        }
        let old = std::mem::replace(&mut *self.state(), PendingState::Complete);
        let metadata_dropped = matches!(&old, PendingState::Pending { .. } | PendingState::Applying { .. });
        drop(old);
        if metadata_dropped && let Some(custody) = &self.custody {
            custody.retire(std::mem::size_of::<FileMetadata>() as u64)
                .expect("original pending metadata allocation custody");
        }
    }

    fn lookup(&self) -> Option<MountLookup> {
        match &*self.state() {
            PendingState::Pending { metadata, bytes } => Some(self.held_lookup(**metadata, bytes.len() as u64)),
            _ => None,
        }
    }

    fn held_lookup(&self, metadata: FileMetadata, logical_bytes: u64) -> MountLookup {
        MountLookup {
            node: MountNode {
                file_id: self.file_id,
                kind: MountNodeKind::Regular,
                logical_bytes,
                link_count: 1,
                device: None,
            },
            metadata,
        }
    }

    fn applied(self: &Arc<Self>) -> Result<(), MountSourceError> {
        match &*self.state() {
            PendingState::Complete if self.failure.get().is_some() => {
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

const MAXIMUM_GROUPED_CHANGES: usize = 64;

impl PendingCreate {
    fn reject_uninstalled(self: Arc<Self>, error: MountSourceError) {
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
        if retryable {
            self.restore_applying_retained();
        } else {
            self.finish_applying(Err(error));
        }
    }
}

struct GroupedDelivery {
    changes: Vec<GroupedChange>,
    replies: Vec<(Arc<GroupedAdmission>, Arc<PendingCreate>)>,
    results: Vec<Result<GroupedOutcome, FsError>>,
    installed: bool,
    // The changes backing physically retires before its transferable ticket.
    changes_allocation: Option<crate::NativeAllocation>,
    // All other native phase backing retires before this original ticket.
    native_phase_allocation: Option<crate::NativeAllocation>,
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
    carrier: &'a mut NativeDemandCarrier<'w>,
) -> Result<NativeCarrierOwned<'a, 'w, GroupedDelivery>, MountSourceError> {
    if let Some(native) = carrier.original_native_work() {
        let mut phase = NativeCaptureScratchAllocation::new(&native)?;
        let mut change_slots = NativeCaptureScratchAllocation::new(&native)?;
        let changes = change_slots.vec(requests.len())?;
        let replies = phase.vec(requests.len())?;
        let buffers = GroupedDelivery {
            changes, replies, results: Vec::new(), installed: false,
            changes_allocation: Some(change_slots.retain()),
            native_phase_allocation: Some(phase.retain()),
        };
        return Ok(NativeCarrierOwned { value: Some(buffers), carrier, bytes: 0 });
    }
    let (buffers, bytes) = carrier.with_work(|work| {
        let mut scratch = CaptureScratchAllocation::new(work);
        let changes = scratch.vec(requests.len())?;
        let replies = scratch.vec(requests.len())?;
        let buffers = GroupedDelivery {
            changes, replies, results: Vec::new(), installed: false,
            changes_allocation: None, native_phase_allocation: None,
        };
        Ok::<_, MountSourceError>((buffers, scratch.retain()))
    })?;
    Ok(NativeCarrierOwned { value: Some(buffers), carrier, bytes })
}

impl GroupedDelivery {
    fn complete(&mut self, completed: Result<(), MountSourceError>) -> Result<(), MountSourceError> {
        drop(std::mem::take(&mut self.changes));
        let mut completed_error = completed.err();
        let mut completed_cause: Option<Arc<dyn super::MountSourceFailureCause>> = None;
        let mut batch = Ok(());
        let results = std::mem::take(&mut self.results);
        if !results.is_empty() {
            for (result, (admission, reply)) in results.into_iter().zip(self.replies.drain(..))
            {
                let (answer, installed) = match result {
                    Ok(outcome) => {
                        if let Some(error) = completed_error.take() {
                            let cause = admission.retain_failure(error);
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
                        let error = if self.native_phase_allocation.is_some() {
                            MountSourceError::Filesystem(error)
                        } else { fs_error(error) };
                        let cause = admission.retain_failure(error);
                        if batch.is_ok() && completed_error.is_none() {
                            batch = Err(MountSourceError::Retained(Arc::clone(&cause)));
                        }
                        (Err(MountSourceError::Retained(cause)), false)
                    }
                };
                match answer {
                    Err(error) if !installed => reply.reject_uninstalled(error),
                    answer => reply.finish_applying(answer),
                }
            }
        } else if let Some((admission, _)) = self.replies.first() {
            let failed = completed_error.is_some();
            let cause = admission.retain_failure(
                completed_error.take().unwrap_or(MountSourceError::Cancelled),
            );
            if failed {
                batch = Err(MountSourceError::Retained(Arc::clone(&cause)));
            }
            for (_, reply) in self.replies.drain(..) {
                reply.reject_uninstalled(MountSourceError::Retained(Arc::clone(&cause)));
            }
        }
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

struct GroupedRequest {
    change: GroupedChange,
    admission: Arc<GroupedAdmission>,
    reply: Arc<PendingCreate>,
}

struct GroupedCallbackBindings {
    entries: Vec<(Option<Arc<dyn super::NativeMountCallbackGuard>>, CancellationToken)>,
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
                    Option<Arc<dyn super::NativeMountCallbackGuard>>, CancellationToken,
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
                    Option<Arc<dyn super::NativeMountCallbackGuard>>, CancellationToken,
                )>() as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            match native.as_ref() {
                Some(original) => original.charge(delta).map_err(MountSourceError::NativeWork)?,
                None => carrier.with_work(|work| work.charge(delta).map_err(mount_demand_error))?,
            }
            bindings.entries.push((
                request.reply.native_callback_guard.as_ref().map(Arc::clone), request.reply.source_lifetime.clone(),
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
                if lifetime.is_cancelled() {
                    return Err(super::NativeMountError::Source(MountSourceError::Stale));
                }
                if let Some(guard) = guard { guard.check_write(&owned_work)?.await?; }
                if lifetime.is_cancelled() {
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

struct GroupedAdmission {
    failure: OnceLock<MountSourceError>,
    callback_bindings: OnceLock<GroupedCallbackBindings>,
    _allocation: Option<super::NativeMountAllocationCustody>,
    _native_allocation: Option<crate::NativeAllocation>,
}

impl GroupedAdmission {
    fn new_measured(
        work: &mut crate::demand::DemandWork,
    ) -> Result<Arc<Self>, MountSourceError> {
        let allocation = super::NativeMountAllocationCustody::new(work).map_err(super::lazy::mount_demand_error)?;
        allocation.claim(std::mem::size_of::<Self>() as u64, work).map_err(super::lazy::mount_demand_error)?;
        Ok(Arc::new(Self {
            failure: OnceLock::new(),
            callback_bindings: OnceLock::new(),
            _allocation: Some(allocation),
            _native_allocation: None,
        }))
    }

    fn new_with_native_work(
        original: &crate::NativeMaterializationWork,
    ) -> Result<Arc<Self>, MountSourceError> {
        let allocation = original.claim(std::mem::size_of::<Self>() as u64)
            .map_err(MountSourceError::NativeWork)?;
        Ok(Arc::new(Self {
            failure: OnceLock::new(),
            callback_bindings: OnceLock::new(),
            _allocation: None, _native_allocation: Some(allocation),
        }))
    }

    fn retain_failure(self: &Arc<Self>, error: MountSourceError) -> Arc<dyn super::MountSourceFailureCause> {
        let error = match error {
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

/// Exclusive access to a shared checkout.
pub struct SharedCheckoutGuard<'a, A, O> {
    _view: ViewWriteLease,
    state: tokio::sync::RwLockWriteGuard<'a, SharedCheckoutState<A, O>>,
}

/// One callback's shared admission to the checkout's current view.
pub(super) struct SharedCheckoutObservation<'a, A, O> {
    _view: ViewReadLease,
    state: tokio::sync::RwLockReadGuard<'a, SharedCheckoutState<A, O>>,
}

/// The same pinned observer and view admission, without a checkout-state lock.
pub(super) struct SharedCheckoutOwnedObservation<A: 'static, O: 'static> {
    pub(super) observer: crate::facade::CheckoutObserver<'static, A, O>,
    pub(super) _view: ViewReadLease,
}

/// Locked checkout state.
pub struct SharedCheckoutState<A, O> {
    checkout: Checkout<A, O>,
    publication_operation: Option<OperationId>,
    publication_evidence: Option<NativeSourcePublicationEvidence>,
    publication: MountPublication,
    revision: Arc<AtomicU64>,
    ledger: Arc<ViewLedger>,
    recorded_view: ObjectId,
    checkout_custody: Option<Arc<crate::kernel::ProofDataCustody>>,
}

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
        *self.shared.publishing.lock().unwrap_or_else(PoisonError::into_inner) = false;
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
            publishing: StdMutex::new(false),
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

    /// Serializes external checkout access and excludes native callbacks, once every pending creation is applied.
    pub async fn lock(&self) -> SharedCheckoutGuard<'_, A, O>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.settle_pending().await;
        self.lock_settled().await
    }

    /// Settles queued creations with the caller's remaining work before exposing the checkout.
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
        self.settle_pending_with_carrier(&mut NativeDemandCarrier::Borrowed(work), cancellation, lifetime).await?;
        await_measured_lock_with_carrier(
            self.lock_settled(), &mut NativeDemandCarrier::Borrowed(work), cancellation, lifetime,
        ).await
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

    pub(super) async fn prepare_native_phase_for_operation_with_carrier(
        &self,
        operation: Option<OperationId>,
        carrier: &mut NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<(CheckoutCandidate<A, O>, u64, ViewStamp), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let checkout = await_measured_lock_with_carrier(
            self.lock_settled(), carrier, cancellation, lifetime,
        ).await?;
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
        record(carrier)?;
        let mut state = await_measured_lock_with_carrier(
            self.lock_settled(), carrier, cancellation, lifetime,
        ).await?;
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
            NativeDemandCarrier::Borrowed(work).accept_with_prior_live(result, prior_live, mount_engine_error)
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
            return carrier.with_work(|work| native_name_error(
                "sparse range overflows the file address space", true, work,
            ));
        };
        let record = {
            let observation = self.observe_owned_with_carrier(
                ViewGate::callback_owner(), carrier, cancellation, lifetime,
            ).await?;
            let _view = observation._view;
            let mut observer = observation.observer;
            if let Some(original) = &original {
                let accepted = observer.read_file_record_by_id_with_native_work(
                    file_id, original, cancellation,
                ).await.map_err(mount_engine_error);
                observer.proof_data_take_completed_work();
                accepted?
            } else {
                let mut observer = LookupObserverOwned::new_in(observer, carrier.reborrow())?;
                let (budget, prior_live) = observer.work.with_work(|work| {
                    Ok::<_, MountSourceError>((
                        work.remaining().map_err(mount_demand_error)?, work.live_allocation_bytes(),
                    ))
                })?;
                let result = observer.read_file_record_by_id(file_id, budget, cancellation).await;
                let accepted = observer.work.accept_with_prior_live(result, prior_live, mount_engine_error);
                observer.receipt_delivered();
                let finished = observer.finish();
                let record = accepted?;
                finished?;
                record
            }
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
                return carrier.with_work(|work| native_name_error(
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
        if let NativeDemandCarrier::Borrowed(work) = carrier {
            super::view_ledger::ViewLedgerAdmission::default().reconcile(&self.ledger, work)?;
        }
        self.settle_pending_with_carrier(carrier, cancellation, lifetime).await?;
        await_measured_lock_with_carrier(self.lock_settled(), carrier, cancellation, lifetime).await
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
            input.carrier.accept_with_prior_live(result, prior_live, fs_error)
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
        *self.publishing.lock().unwrap_or_else(PoisonError::into_inner)
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
        *self.publishing.lock().unwrap_or_else(PoisonError::into_inner) = true;
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
        let mut acknowledged = None;
        let mut accounting_failure = None;
        self.seal_original_with_carrier_guarded_context(
            permit, force, carrier, cancellation, lifetime, before_publication,
            None, None, None, &mut acknowledged, &mut accounting_failure,
        ).await.map(|_| ())
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
                    let original = super::NativeMountMeterWork::from_meter(Arc::clone(&meter));
                    self.seal_original_on_native_work_guarded(
                        permit, force, &original, cancellation, lifetime, before_publication,
                        operation_context, operation_evidence, snapshot_context, acknowledged, accounting_failure,
                    ).await
                }).await,
            NativeDemandCarrier::Shared(meter) => self.seal_original_on_native_work_guarded(
                permit, force, &super::NativeMountMeterWork::from_meter(Arc::clone(meter)), cancellation, lifetime, before_publication,
                operation_context, operation_evidence, snapshot_context, acknowledged, accounting_failure,
            ).await,
            NativeDemandCarrier::Native(original) => self.seal_original_on_native_work_guarded(
                permit, force, original, cancellation, lifetime, before_publication,
                operation_context, operation_evidence, snapshot_context, acknowledged, accounting_failure,
            ).await,
        }
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

    async fn settle_pending(&self)
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let cancellation = CancellationToken::new();
        let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let _ = self.settle_pending_with_carrier(&mut NativeDemandCarrier::Borrowed(&mut work), &cancellation, None).await;
    }

    async fn settle_pending_with_carrier(
        &self, carrier: &mut NativeDemandCarrier<'_>,
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
                        terminal &= matches!(&*create.state(), PendingState::Complete);
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
                        let admission = GroupedAdmission::new_with_native_work( &original)?;
                        let copied = original.reserve_copy(std::mem::size_of::<GroupedRequest>() as u64)
                            .map_err(MountSourceError::NativeWork)?;
                        if let Some(change) = create.begin_applying_with_native_work(
                            &original, preparation.native_allocation.as_mut().expect("prepared request vector"), cancellation,
                        )? {
                            requests.push(GroupedRequest {
                                change, admission, reply: Arc::clone(create),
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
                        let admission = GroupedAdmission::new_measured( work)?;
                        if let Some(change) = create.begin_applying_measured(
                            checkout.checkout.volume_config().limits, work, claimed,
                        )? {
                            requests.push(GroupedRequest {
                                change, admission, reply: Arc::clone(create),
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
                reply.restore_applying_retained();
            }
            let _ = preparation.finish_cleanup(restored, false);
            let _ = preparation.release(false);
            return Err(error);
        }
        drop(checkout);
        preparation.scan = true;
        let result = self.apply_grouped_requests(
            requests, preparation.carrier, cancellation, lifetime,
        ).await;
        let released = preparation.release(false);
        if preparation.carrier.original_native_work().is_none()
            && result.is_ok() && lifetime.is_some_and(CancellationToken::is_cancelled) {
            return Err(MountSourceError::Stale);
        }
        result.and(released)
    }

    /// Admits one callback to the current view alongside every other reader, once every pending creation is applied.
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

    async fn observe_as_is(
        &self,
        owner: CallbackOwner,
    ) -> Result<SharedCheckoutObservation<'_, A, O>, MountSourceError> {
        let view = self.view_gate.read_for_callback(owner, None).await?;
        let state = self.read_current_state().await;
        Ok(SharedCheckoutObservation { _view: view, state })
    }

    /// Observes the original shared view without settling queued creations.
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
            self.settle_pending_with_carrier(carrier, cancellation, lifetime).await?;
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

    /// Whether a native durability request (`fsync`, or a close that publishes) publishes; [`MountPublication::Manual`] leaves every publication to an explicit sync or unmount.
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

    /// Retains a caller-selected operation identity, or rejects a conflicting unresolved publication from another surface sharing this checkout.
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
                let checkout = NativeDemandCarrier::Borrowed(work)
                    .accept_with_prior_live(result, prior_live, mount_engine_error)?;
                work.retain_checkout_control(&checkout.private_control_custody())
                    .map_err(mount_demand_error)?;
                Ok(checkout)
            })
        };
        let base = clone(carrier)?;
        let checkout = clone(carrier)?;
        Ok(CheckoutCandidate { base, checkout })
    }

    /// Records one change to the checkout's view.
    pub(super) fn record(&mut self, change: &ViewChange<'_>) {
        self.ledger.record(
            change, &mut NativeDemandCarrier::Borrowed(&mut super::view_ledger::unbounded_view_work()),
            &mut super::view_ledger::ViewLedgerAdmission::default(),
        ).expect("unbounded view recording");
        self.revision.fetch_add(1, Ordering::AcqRel);
        self.recorded_view = self.checkout.root().file_table;
    }
}

impl<A, O> SharedCheckout<A, O> {
    async fn apply_grouped_requests(
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
        let (mut candidate, epoch, stamp) = match self.prepare_native_phase_for_operation_with_carrier(
            None, carrier, cancellation, lifetime,
        ).await {
            Ok(prepared) => prepared,
            Err(error) => return requests.reject(error),
        };
        let mut native_result_custody = None;
        let mut buffers = match prepare_grouped_buffers_with_carrier(
            &requests.0, carrier,
        ) {
            Ok(buffers) => buffers,
            Err(error) => {
                drop(candidate);
                return requests.reject(error);
            }
        };
        let NativeCarrierOwned { value, carrier, bytes } = &mut buffers;
        let delivery = value.as_mut().expect("grouped phase buffers are live");
        if let Err(error) = (|| {
            let copied = requests.0.len().checked_mul(
                std::mem::size_of::<GroupedChange>() + std::mem::size_of::<(Arc<GroupedAdmission>, Arc<PendingCreate>)>(),
            ).ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
            let delta = crate::WorkCounters {
                bytes_copied: copied,
                items_examined: requests.0.len() as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            match &original {
                Some(original) => original.charge(delta).map_err(MountSourceError::NativeWork),
                None => carrier.with_work(|work| work.charge(delta).map_err(mount_demand_error)),
            }
        })() {
            drop(candidate);
            return requests.reject(error);
        }
        for request in requests.0.drain(..) {
            delivery.changes.push(request.change);
            delivery.replies.push((request.admission, request.reply));
        }
        let captured = match &original {
            Some(original) => original.remaining().map(|budget|
                (budget, original.live_allocation_bytes())).map_err(MountSourceError::NativeWork),
            None => carrier.with_work(|work| {
                Ok::<_, MountSourceError>((
                    work.remaining().map_err(mount_demand_error)?, work.live_allocation_bytes(),
                ))
            }),
        };
        let (budget, prior_live) = match captured {
            Ok(captured) => captured,
            Err(error) => {
                drop(candidate);
                return delivery.complete(Err(error));
            }
        };
        let changes = std::mem::take(&mut delivery.changes);
        let change_bytes = (changes.capacity() * std::mem::size_of::<GroupedChange>()) as u64;
        let (accepted, released) = if let Some(original) = &original {
            let input_allocation = delivery.changes_allocation.take()
                .expect("native grouped slots retain their original allocation");
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
            let accepted = carrier.accept_with_prior_live(applied, prior_live, mount_engine_error);
            let released = carrier.with_work(|work|
                work.release(change_bytes).map_err(mount_demand_error));
            if released.is_ok() {
                *bytes = bytes.checked_sub(change_bytes)
                    .expect("grouped change slots have exact custody");
            }
            (accepted, released)
        };
        let results = match accepted {
            Ok(results) => results,
            Err(error) => {
                drop(candidate);
                return delivery.complete(Err(error));
            }
        };
        let result_bytes = (results.capacity()
            * std::mem::size_of::<Result<GroupedOutcome, FsError>>()) as u64;
        let adopted = if native_result_custody.is_some() {
            Ok(())
        } else {
            let adopted = carrier.with_work(|work|
                work.record_adoption(result_bytes).map_err(mount_demand_error));
            *bytes = bytes.checked_add(result_bytes)
                .expect("grouped outcome slots have exact custody");
            adopted
        };
        delivery.results = results;
        let mut completed = released.and(adopted).and_then(|()| check_lifetime());
        let changed = delivery.results.iter().any(Result::is_ok);
        if completed.is_ok() && changed {
            let installed = self.finish_native_phase_with_recording(
                candidate, epoch, stamp, carrier, cancellation, lifetime, |_| Ok(()),
            ).await;
            if installed.is_ok() {
                delivery.installed = true;
                completed = self.publish_mutation_with_carrier_guarded(
                    carrier, cancellation, lifetime, grouped_publication_guard(Arc::clone(&group_admission)),
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
                None => carrier.with_work(|work| work.check().map_err(mount_demand_error)),
            }.and_then(|()| check_lifetime());
        }
        delivery.complete(completed)
    }
}

/// One optimistic transaction: a copy of the checkout to change, and the exact checkout it began from, against which its effect is computed without holding any lock.
pub(super) struct CheckoutCandidate<A, O> {
    base: Checkout<A, O>,
    pub(super) checkout: Checkout<A, O>,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> CheckoutCandidate<A, O> {
    pub(super) fn logical_owned_bytes(&self) -> u64 {
        self.base.logical_owned_bytes() + self.checkout.logical_owned_bytes()
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
        Ok((installed.value, Some(installed._allocation)))
    }

    /// The checkout this transaction began from.
    pub(super) fn base(&self) -> &Checkout<A, O> {
        &self.base
    }
}

/// What a prepared candidate was built to change.
#[derive(Clone, Copy)]
pub(super) enum InstallScope<'a> {
    Paths(&'a [NamespacePath]),
    CombinedPaths(&'a [NamespacePath], &'a [NamespacePath]),
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
    _allocation: crate::NativeAllocation,
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
    let InstalledCaptureAccounting::Native(scratch) = accounting else {
        unreachable!("native installed effects retain their allocation");
    };
    Ok(InstalledCaptureEffect { value, _allocation: scratch.retain() })
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

macro_rules! read_sparse_spans {
    ($offset:expr, $end:expr, $scratch:ident, $spans:ident = $output:expr,
     $visit:block, $span:block, $growth:expr, $backwards:literal,
     |$position:ident, $target:ident| $seek:expr, |$range:ident| $read:expr) => {{
        async {
            let $spans = &mut $output;
            let mut cursor = $offset;
            while cursor < $end {
                $visit
                let $position = cursor;
                let $target = ExtentSeekTarget::Data;
                let Some(data) = $seek.await.map_err(MountSourceError::Filesystem)? else { break; };
                if data >= $end { break; }
                if data < cursor {
                    return Err(MountSourceError::NativeStaticText {
                        kind: super::NativeSourceTextKind::Engine, text: $backwards,
                    });
                }
                let $position = data;
                let $target = ExtentSeekTarget::Hole;
                let hole = $seek.await.map_err(MountSourceError::Filesystem)?.unwrap_or($end).min($end);
                if hole <= data {
                    return Err(MountSourceError::NativeStaticText {
                        kind: super::NativeSourceTextKind::Engine, text: "sparse seek made no forward progress",
                    });
                }
                if $spans.len() == $spans.capacity() {
                    let requested = $growth;
                    grow_sparse_spans($spans, requested, &mut $scratch)?;
                }
                $span
                let $range = ByteRange { offset: data, length: hole - data };
                let read = $read.await.map_err(MountSourceError::Filesystem)?;
                if read.bytes.len() as u64 != hole - data { return Err(MountSourceError::Stale); }
                $spans.push(super::MountSparseSpan { relative_offset: data - $offset, bytes: read.bytes });
                cursor = hole;
            }
            Ok::<_, MountSourceError>(())
        }.await
    }};
}

fn grow_sparse_spans(
    spans: &mut Vec<super::MountSparseSpan>, requested: usize,
    scratch: &mut NativeCaptureScratchAllocation<'_>,
) -> Result<(), MountSourceError> {
    let old_bytes = (spans.capacity() * std::mem::size_of::<super::MountSparseSpan>()) as u64;
    let mut grown = scratch.vec(requested)?;
    scratch.charge(crate::WorkCounters {
        bytes_copied: (spans.len() * std::mem::size_of::<super::MountSparseSpan>()) as u64,
        ..crate::WorkCounters::UNCHARGED
    })?;
    grown.append(spans);
    drop(std::mem::replace(spans, grown));
    scratch.release(old_bytes)
}

impl<A, O> CheckoutDetachedFile<A, O> {
    pub(super) async fn snapshot_async(&self) -> Result<(FileRecord, FileMetadata, u64), MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        let state = self.state.lock().await;
        Ok((state.file.record(), state.metadata, state.mutation_epoch))
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

    /// Reads the held identity on the original caller's meter, with no runtime reentry.
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
        use super::lazy::{mount_accept, mount_demand_error, mount_engine_error};
        let file = &state.file;
        let range = ByteRange { offset, length };
        if let Some(original) = carrier.original_native_work() {
            let read = file.read_range_with_native_work(range, &original, &cancellation)
                .await.map_err(MountSourceError::Filesystem)?;
            original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            let logical_owned_bytes = read.logical_owned_bytes();
            return Ok(crate::kernel::OwnedSourceChunk::intrinsic(read.bytes, logical_owned_bytes));
        }
        let budget = carrier.with_work(|work| work.remaining().map_err(mount_demand_error))?;
        let result = file.read_range(
            range, budget, &cancellation,
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
        let epoch = state.mutation_epoch.checked_add(1).ok_or(MountSourceError::Stale)?;
        let mut metadata = state.metadata;
        let stamped = metadata.stamp_content_change(content_change_time()?).then_some(metadata);
        record_view_change_with_carrier(
            &self.ledger, &ViewChange::Node(state.file.file_id()), carrier,
        )?;
        let original = carrier.original_native_work().expect("detached changes use native work");
        let result = state.file.change_content_with_native_work(
            change, stamped, &original, cancellation,
        ).await;
        if result.is_ok() {
            state.metadata = metadata;
            state.mutation_epoch = epoch;
        }
        return result.map_err(MountSourceError::Filesystem);
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
        let original = carrier.original_native_work().expect("detached attributes use native work");
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

macro_rules! ordinary_file_methods {
    ($this:ident, $boundary:ident) => {
    fn lookup(&$this) -> Result<MountLookup, MountSourceError> {
        ordinary_file_methods!(@call $this, $boundary,  |work| $this.lookup_with_meter(work))
    }

    fn read_range(&$this, offset: u64, length: u32) -> Result<OwnedBytes, MountSourceError> {
        ordinary_file_methods!(@call $this, $boundary,  |work| $this.read_range_with_meter(offset, length, work).map(|packet| packet.bytes))
    }

    fn read_up_to(&$this, offset: u64, maximum_bytes: u32) -> Result<OwnedBytes, MountSourceError> {
        ordinary_file_methods!(@call $this, $boundary,  |work| $this.read_up_to_with_meter(offset, maximum_bytes, work).map(|packet| packet.bytes))
    }

    fn seek(&$this, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError> {
        ordinary_file_methods!(@call $this, $boundary,  |work| $this.seek_with_meter(offset, target, work))
    }

    fn write_range(&$this, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
        ordinary_file_methods!(@call $this, $boundary,  |work| $this.write_range_with_meter(offset, crate::kernel::OwnedSourceChunk::claimed(bytes.into(), 0), work))
    }

    fn resize(&$this, logical_bytes: u64) -> Result<(), MountSourceError> {
        ordinary_file_methods!(@call $this, $boundary,  |work| $this.resize_with_meter(logical_bytes, work))
    }

    fn allocate_range(
        &$this,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        ordinary_file_methods!(@call $this, $boundary,  |work| $this.allocate_range_with_meter(offset, length, operation, work))
    }

    fn set_attributes(
        &$this,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        ordinary_file_methods!(@call $this, $boundary,  |work| $this.set_attributes_with_meter(metadata, logical_bytes, work))
    }

    fn read_attribute(&$this, name: &[u8]) -> Result<Option<OwnedBytes>, MountSourceError> {
        ordinary_file_methods!(@call $this, $boundary,  |work| $this.read_attribute_with_meter(name, work).map(|packet| packet.map(|packet| packet.bytes)))
    }

    fn write_attribute(
        &$this,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        ordinary_file_methods!(@call $this, $boundary,  |work| $this.write_attribute_with_meter(name, crate::kernel::OwnedSourceChunk::claimed(value.into(), 0), mode, work))
    }

    fn remove_attribute(&$this, name: &[u8]) -> Result<(), MountSourceError> {
        ordinary_file_methods!(@call $this, $boundary,  |work| $this.remove_attribute_with_meter(name, work))
    }
    };
    (@call $this:ident, ordinary, $operation:expr) => {
        boundary(&$this.cancellation, $operation)
    };
    (@call $this:ident, pending, $operation:expr) => {
        $this.boundary($operation)
    };
}

macro_rules! native_content_methods {
    ($this:ident, $change:ident) => {
    fn write_range_with_meter(
        &$this, offset: u64, bytes: crate::kernel::OwnedSourceChunk, work: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError> {
        let meter = work.meter();
        let mut carrier = NativeDemandCarrier::Native(work);
        let claim = if bytes.is_intrinsic() { 0 } else { bytes.logical_owned_bytes };
        let mut input = NativeCarrierOwned { value: Some(bytes), carrier: &mut carrier, bytes: claim };
        $this.runtime.block_on_measured(|| async {
            let (packet, carrier) = input.parts();
            $this.$change(
                ContentChange::Write { offset, bytes: packet.bytes.clone() }, carrier,
            ).await
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn resize_with_meter(
        &$this, logical_bytes: u64, work: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError> {
        let meter = work.meter();
        $this.runtime.block_on_measured(|| async {
            $this.$change(
                ContentChange::Resize { logical_bytes }, &mut NativeDemandCarrier::Native(work),
            ).await
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn allocate_range_with_meter(
        &$this, offset: u64, length: u64, operation: MountRangeAllocation, work: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError> {
        let meter = work.meter();
        $this.runtime.block_on_measured(|| async {
            $this.$change(
                range_allocation(ByteRange { offset, length }, operation), &mut NativeDemandCarrier::Native(work),
            ).await
        }, meter.as_ref()).map_err(capture_io_error)?
    }
    };
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
            read_sparse_spans!(offset, end, scratch, spans = range.spans,
                { scratch.items(1)?; }, {},
                spans.len().checked_add(1).and_then(usize::checked_next_power_of_two).ok_or(MountSourceError::Stale)?,
                "sparse seek made no forward progress",
                |position, target| state.file.seek_with_native_work(position, target, &original, &cancellation),
                |range| state.file.read_range_with_native_work(range, &original, &cancellation)
            )?;
            original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            Ok((range, scratch.retain()))
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
        let mut carrier = NativeDemandCarrier::Native(work);
        self.runtime.block_on_measured(|| async {
            let carrier = &mut carrier;
            let range = &range.0;
            let end = offset.checked_add(range.logical_bytes).ok_or(MountSourceError::NativeStaticText {
                kind: super::NativeSourceTextKind::Invalid, text: "sparse range overflows the file address space",
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
                original.charge(crate::WorkCounters {
                    items_examined: 1, ..crate::WorkCounters::UNCHARGED
                }).map_err(MountSourceError::NativeWork)?;
                let destination = offset.checked_add(span.relative_offset).ok_or(MountSourceError::NativeStaticText {
                    kind: super::NativeSourceTextKind::Invalid, text: "sparse span offset overflows",
                })?;
                self.change_content_locked_with_carrier(
                    &mut state, ContentChange::Write { offset: destination, bytes: span.bytes.clone() },
                    carrier, &cancellation,
                ).await?;
            }
            Ok(())
        }, runtime_meter.as_ref())
            .map_err(|error| MountSourceError::NativeWork(original.runtime_error(error)))?
    }

    fn list_attributes(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &self.cancellation);
        let cursor = cursor.map(|name| native_attribute_name_with_work(self.profile, self.limits, name, &mut work)).transpose()?;
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
            native_attribute_page_with_carrier(
                receipt.value, &mut NativeDemandCarrier::Borrowed(&mut work), Some(&self.cancellation),
            )
        })
    }

    ordinary_file_methods!(self, ordinary);

    native_content_methods!(self, change_content_async_with_carrier);

}

macro_rules! pending_file_methods {
    ($this:ident; $($name:ident($($argument:ident: $ty:ty),*) -> $output:ty;)*) => {
        $(fn $name(&$this, $($argument: $ty,)* work: &super::NativeMountMeterWork)
            -> Result<$output, MountSourceError> {
            $this.apply_with_native_work(work)?;
            $this.attached.$name($($argument,)* work)
        })*
    };
}

struct PendingCreatedFile<A, O> {
    pending: Arc<PendingCreate>,
    attached: CheckoutAttachedFile<A, O>,
}

impl<A, O> PendingCreatedFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{

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
                self.attached.checkout.settle_pending_with_carrier(&mut NativeDemandCarrier::Native(work), &cancellation, Some(&self.attached.cancellation)).await
            }, meter.as_ref()).map_err(|error| MountSourceError::NativeWork(original.runtime_error(error)))??;
        }
        self.pending.applied()
    }

    fn boundary<T>(
        &self, operation: impl FnOnce(&super::NativeMountMeterWork) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        let original = match &*self.pending.state() {
            PendingState::Pending { bytes, .. } | PendingState::Applying { bytes, .. } => bytes.original_work(),
            PendingState::Complete => None,
        };
        match original {
            Some(original) => operation(&original.native_mount_work()),
            None => boundary(&self.attached.cancellation, operation),
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
        let time = content_change_time()?;
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
                let lookup = self.pending.held_lookup(**metadata, bytes.len() as u64);
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
                let time = content_change_time()?;
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
                    bytes.mutable().resize(length, 0);
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

    fn settle_with_meter(&self, work: &super::NativeMountMeterWork) -> Result<(), MountSourceError> {
        self.apply_with_native_work(work)
    }

    fn list_attributes(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        self.boundary(|work| {
            self.apply_with_native_work(work)?;
            self.attached.list_attributes(cursor, maximum_entries)
        })
    }

    fn settle(&self) -> Result<(), MountSourceError> {
        self.boundary(|work| self.settle_with_meter(work))
    }
    ordinary_file_methods!(self, pending);
    pending_file_methods!(self;
        seek_with_meter(offset: u64, target: MountSeekTarget) -> Option<u64>;
        allocate_range_with_meter(offset: u64, length: u64, operation: MountRangeAllocation) -> ();
        set_attributes_with_meter(metadata: FileMetadata, logical_bytes: Option<u64>) -> ();
        read_attribute_with_meter(name: &[u8]) -> Option<crate::kernel::OwnedSourceChunk>;
        list_attributes_with_meter(cursor: Option<&[u8]>, maximum_entries: u32) -> (MountAttributePage, crate::NativeAllocation);
        write_attribute_with_meter(name: &[u8], value: crate::kernel::OwnedSourceChunk, mode: MountAttributeWriteMode) -> ();
        remove_attribute_with_meter(name: &[u8]) -> ();
        read_sparse_range_with_meter(offset: u64, length: u32) -> (MountSparseRange, crate::NativeAllocation);
        write_sparse_range_with_meter(offset: u64, range: (MountSparseRange, crate::NativeAllocation)) -> ();
    );

}

impl<A, O> CheckoutAttachedFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    async fn observer(
        &self, work: &super::NativeMountMeterWork,
    ) -> Result<(ViewReadLease, crate::facade::CheckoutObserver<'static, A, O>), MountSourceError> {
        let mut carrier = NativeDemandCarrier::Native(work);
        let cancellation = carrier.with_work(|work| {
            work.check().map_err(super::lazy::mount_demand_error)?;
            Ok::<_, MountSourceError>(work.cancellation().clone())
        })?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        let observation = self.checkout.observe_owned_with_carrier(
            ViewGate::callback_owner(), &mut carrier, &cancellation, Some(&self.cancellation),
        ).await?;
        Ok((observation._view, observation.observer))
    }

    async fn read_owned(
        &self,
        offset: u64,
        length: u32,
        up_to: bool,
        work: &super::NativeMountMeterWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        use super::lazy::mount_demand_error;
        let (_view, mut observer) = self.observer(work).await?;
        let cancellation = work.meter().lock().cancellation().clone();
        let original = work.native_materialization_work();
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
        observer.proof_data_take_completed_work();
        let read = read.map_err(MountSourceError::Filesystem)?;
        work.meter().lock().check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        let logical_owned_bytes = read.logical_owned_bytes();
        return Ok(crate::kernel::OwnedSourceChunk::intrinsic(read.bytes, logical_owned_bytes));
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
            let (_view, mut observer) = self.observer(work).await?;
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
            observer.proof_data_take_completed_work();
            let value = result?;
            Ok(value)
        }, meter.as_ref()).map_err(capture_io_error)?
    }

    fn read_range_with_meter(&self, offset: u64, length: u32, work: &super::NativeMountMeterWork)
        -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(
            || self.read_owned(offset, length, false, work), meter.as_ref(),
        ).map_err(capture_io_error)?
    }

    fn read_up_to_with_meter(&self, offset: u64, maximum_bytes: u32, work: &super::NativeMountMeterWork)
        -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(
            || self.read_owned(offset, maximum_bytes, true, work), meter.as_ref(),
        ).map_err(capture_io_error)?
    }

    fn seek_with_meter(&self, offset: u64, target: MountSeekTarget, work: &super::NativeMountMeterWork)
        -> Result<Option<u64>, MountSourceError> {
        let meter = work.meter();
        self.runtime.block_on_measured(|| async {
            use super::lazy::mount_demand_error;
            let (_view, mut observer) = self.observer(work).await?;
            let cancellation = meter.lock().cancellation().clone();
            let original = work.native_materialization_work();
            let accepted = observer.seek_file_extent_by_id_with_native_work(self.file_id, offset, match target {
                MountSeekTarget::Data => ExtentSeekTarget::Data,
                MountSeekTarget::Hole => ExtentSeekTarget::Hole,
            }, &original, &cancellation).await.map_err(MountSourceError::Filesystem);
            observer.proof_data_take_completed_work();
            let value = accepted?;
            meter.lock().check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            Ok(value)
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
            let (_view, mut observer) = self.observer(work).await?;
            let cancellation = meter.lock().cancellation().clone();
            let accepted = observer.read_named_attribute_owned_by_id_with_native_work(
                self.file_id, &name,
                &original, &cancellation,
            ).await.map_err(MountSourceError::Filesystem);
            observer.proof_data_take_completed_work();
            let output = accepted?;
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
            let (_view, mut observer) = self.observer(work).await?;
            let cancellation = meter.lock().cancellation().clone();
            let accepted = observer.list_named_attributes_by_id_with_native_work(
                self.file_id, cursor.as_ref().map(|(name, _allocation)| name),
                maximum_entries, &original, &cancellation,
            ).await.map_err(MountSourceError::Filesystem);
            observer.proof_data_take_completed_work();
            let (listing, allocation) = accepted?;
            let page = native_attribute_page_with_native_work(
                listing, allocation, &original, Some(&self.cancellation),
            )?;
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
            let (_view, mut observer) = self.observer(work).await?;
            let cancellation = meter.lock().cancellation().clone();
            let original = work.native_materialization_work();
            let mut scratch = NativeCaptureScratchAllocation::new(&original)?;
            let mut output = super::MountSparseRange { logical_bytes, spans: Vec::new() };
            let result = async {
                read_sparse_spans!(offset, end, scratch, spans = output.spans,
                    {}, { scratch.charge(crate::WorkCounters {
                        items_examined: 1, bytes_copied: std::mem::size_of::<super::MountSparseSpan>() as u64,
                        ..crate::WorkCounters::UNCHARGED
                    })?; },
                    spans.capacity().checked_mul(2).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?.max(1),
                    "sparse seek moved backwards",
                    |position, target| observer.seek_file_extent_by_id_with_native_work(
                        self.file_id, position, target, &original, &cancellation),
                    |range| observer.read_file_range_by_id_with_native_work(self.file_id, range, &original, &cancellation)
                )?;
                meter.lock().check().map_err(mount_demand_error)?;
                self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
                Ok::<_, MountSourceError>(())
            }.await;
            observer.proof_data_take_completed_work();
            result?;
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

    fn list_attributes(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &self.cancellation);
        let cursor = cursor.map(|name| native_attribute_name_with_work(self.profile, self.limits, name, &mut work)).transpose()?;
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
            native_attribute_page_with_carrier(
                receipt.value, &mut NativeDemandCarrier::Borrowed(&mut work), Some(&self.cancellation),
            )
        })
    }

    ordinary_file_methods!(self, ordinary);

    native_content_methods!(self, change_content_with_carrier);

}

/// Encodes one authenticated name as this host's native mount component.
pub(super) fn native_mount_name(name: &LogicalName) -> Result<Vec<u8>, MountSourceError> {
    let cancellation = CancellationToken::new();
    native_mount_name_with_work(name, &mut crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation))
}

fn native_mount_name_with_scratch<S: CaptureInputScratch>(
    name: &LogicalName,
    scratch: &mut S,
) -> Result<Vec<u8>, MountSourceError> {
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
                MountSourceError::NativeStaticText { kind: super::NativeSourceTextKind::Invalid, text: "authenticated UTF-8 name is malformed" }
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
        _ => Err(MountSourceError::NativeStaticText { kind: super::NativeSourceTextKind::Unsupported, text: "authenticated name encoding is incompatible with this native mount" }),
    }
}

pub(super) fn native_mount_name_with_work(
    name: &LogicalName,
    work: &mut crate::demand::DemandWork,
) -> Result<Vec<u8>, MountSourceError> {
    let mut scratch = CaptureScratchAllocation::new(work);
    let name = native_mount_name_with_scratch(name, &mut scratch)?;
    scratch.retain();
    Ok(name)
}

pub(super) fn native_mount_name_with_native_work(
    name: &LogicalName,
    original: &crate::NativeMaterializationWork,
) -> Result<(Vec<u8>, crate::NativeAllocation), MountSourceError> {
    let mut scratch = NativeCaptureScratchAllocation::new(original)?;
    let name = native_mount_name_with_scratch(name, &mut scratch)?;
    Ok((name, scratch.retain()))
}

fn native_name_error<T>(
    text: &'static str, invalid: bool, work: &mut crate::demand::DemandWork,
) -> Result<T, MountSourceError> {
    work.check().map_err(super::lazy::mount_demand_error)?;
    Err(MountSourceError::NativeStaticText {
        kind: if invalid { super::NativeSourceTextKind::Invalid } else { super::NativeSourceTextKind::Unsupported },
        text,
    })
}

fn native_attribute_name_with_scratch<S: CaptureInputScratch>(
    profile: FilesystemProfile,
    limits: VolumeLimits,
    bytes: &[u8],
    scratch: &mut S,
) -> Result<AttributeName, MountSourceError> {
    if profile != FilesystemProfile::Posix {
        return Err(MountSourceError::NativeStaticText { kind: super::NativeSourceTextKind::Unsupported, text: "native POSIX attributes require a POSIX volume profile" });
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
            Err(MountSourceError::NativeStaticText { kind: super::NativeSourceTextKind::Engine, text: "attribute name is invalid" })
        }
    }
}

fn native_attribute_name_with_work(
    profile: FilesystemProfile,
    limits: VolumeLimits,
    bytes: &[u8],
    work: &mut crate::demand::DemandWork,
) -> Result<AttributeName, MountSourceError> {
    let mut scratch = CaptureScratchAllocation::new(work);
    let name = native_attribute_name_with_scratch(profile, limits, bytes, &mut scratch)?;
    scratch.retain();
    Ok(name)
}

fn native_attribute_name_with_native_work(
    profile: FilesystemProfile,
    limits: VolumeLimits,
    bytes: &[u8],
    original: &crate::NativeMaterializationWork,
) -> Result<(AttributeName, crate::NativeAllocation), MountSourceError> {
    let mut scratch = NativeCaptureScratchAllocation::new(original)?;
    let name = native_attribute_name_with_scratch(profile, limits, bytes, &mut scratch)?;
    Ok((name, scratch.retain()))
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
                    return native_name_error(
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

}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> CheckoutMountSource<A, O> {
    /// The checkout's current generation, which publication advances.
    #[cfg(all(test, target_os = "macos"))]
    pub(super) fn generation_id(&self) -> Result<crate::GenerationId, MountSourceError>
    {
        let owner = ViewGate::callback_owner();
        self.runtime
            .wait(|| async { Ok(self.checkout.observe(owner).await?.generation_id()) })
    }

}

impl<A, O> CheckoutMountSource<A, O> {
    /// The checkout path a mounted path names, as view changes key it.
    pub(super) fn namespace_path(
        &self,
        path: &MountPath,
    ) -> Result<NamespacePath, MountSourceError> {
        self.path(path)
    }

    /// Records a change a composing source makes to its own projection of this checkout.
    pub(super) fn record_projection_change(&self, change: &ViewChange<'_>) {
        self.checkout.ledger.record(change);
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
        let recorded = self.checkout.ledger.record_with_native_work(change, original, view);
        let checked = private_check();
        recorded?;
        checked
    }

    pub(super) fn take_view_admission_for_work(
        &self, work: &mut crate::demand::DemandWork,
    ) -> Result<(usize, super::view_ledger::ViewLedgerAdmission), MountSourceError> {
        self.checkout.ledger.take_for_work(work)
    }

}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> CheckoutMountSource<A, O> {
    /// Binds an existing source handle to the same stable identity once a peer stages that identity in the mounted checkout candidate.
    pub(super) fn attached_file_by_id(
        &self,
        file_id: FileId,
    ) -> Result<Option<Arc<dyn MountOpenFile>>, MountSourceError>
    where
        A: 'static,
        O: 'static,
    {
        boundary(&self.cancellation, |work| self.runtime.block_on_measured(|| async {
            self.attached_file_by_id_with_carrier(
                file_id, ViewGate::callback_owner(), &mut NativeDemandCarrier::Native(work),
            ).await.map(|handle| handle.map(|(handle, _)| handle))
        }, work.meter().as_ref()).map_err(capture_io_error)?)
    }

    /// Resolves one mounted path within a caller's callback future, so a composed source spends one runtime entry on its whole callback.
    pub(super) async fn lookup_async(
        &self,
        path: &MountPath,
        owner: CallbackOwner,
    ) -> Result<Option<MountLookup>, MountSourceError>
    {
        in_heap(move || async move {
            let mut work = crate::demand::DemandWork::new(boundary_budget(), &self.cancellation);
            self.lookup_async_with_carrier(path, owner, &mut NativeDemandCarrier::Borrowed(&mut work)).await
        }).await
    }

    async fn read_bytes_async_with_native_work(
        &self,
        path: &MountPath,
        range: Option<ByteRange>,
        owner: CallbackOwner,
        native: &super::NativeMountMeterWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>
    where A: 'static, O: 'static,{
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
        let mut observer = observation.observer;
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
        observer.proof_data_take_completed_work();
        let output = result.map_err(MountSourceError::Filesystem)?;
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
        A: 'static,
        O: 'static,
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
        A: 'static,
        O: 'static,
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
        A: 'static,
        O: 'static,
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
    where A: 'static, O: 'static,{
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
        let mut observer = observation.observer;
        let result = observer.list_directory_records_with_native_work(
            &namespace, cursor.as_ref(), maximum_entries, &original, &cancellation,
        ).await;
        observer.proof_data_take_completed_work();
        let (page, allocation) = result.map_err(MountSourceError::Filesystem)?;
        let mut source_allocation = allocation;
        let source_entries = page.entries;
        let has_more = page.has_more;
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
        let page = MountDirectoryPage::with_native_allocation(entries, next_cursor, scratch.retain());
        original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        Ok(page)
    }

}

impl<A, O> CheckoutMountSource<A, O> {
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

}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> CheckoutMountSource<A, O> {
    fn create_file_on_carrier(
        &self, path: &MountPath, metadata: FileMetadata, absent_since: Option<ViewStamp>,
        mut carrier: NativeDemandCarrier<'_>,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<MountLookup, MountSourceError>
    {
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
    {
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
            self.checkout.settle_pending_with_carrier(carrier, &cancellation, Some(&self.cancellation)).await?;
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

        F: FnOnce(NamespacePath) -> AuthoredMutation,
    {
        use super::lazy::mount_demand_error;
        let original = carrier.original_native_work().expect("creation uses native work");
        let cancellation = carrier.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            Ok::<_, MountSourceError>(work.cancellation().clone())
        })?;
        let namespace = self.namespace_path_with_carrier(path, carrier)?;
        let (mut candidate, epoch, stamp) = self.checkout.prepare_native_phase_with_carrier(
            carrier, &cancellation, Some(&self.cancellation),
        ).await?;
        let input = prepare_native_authored_phase_input(namespace.0, &original, build)?;
        let accepted = candidate.checkout.apply_borrowed_authored_transaction_with_native_work(
            &input.authored, &input.slots_allocation, &original, &cancellation,
        ).await.map_err(MountSourceError::Filesystem).and_then(|(value, allocation)| {
            let output = (value, allocation);
            original.charge(crate::WorkCounters {
                bytes_copied: std::mem::size_of::<MountLookup>() as u64,
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            }).map_err(MountSourceError::NativeWork)?;
            output.0.created_file_ids.first().copied().flatten()
                .ok_or_else(|| MountSourceError::Engine("create omitted file identity".to_owned()))
        });
        let path = match input.authored.first().expect("native create input is live") {
            AuthoredMutation::CreateDirectory { path, .. }
            | AuthoredMutation::CreateSymbolicLink { path, .. }
            | AuthoredMutation::CreateEmptySpecial { path, .. }
            | AuthoredMutation::CreateDevice { path, .. } => path,
            _ => unreachable!("authored creation keeps its original bound path"),
        };
        let installed = self.checkout.finish_native_phase_with_carrier(
            candidate, epoch, stamp, &ViewChange::Bound(path), carrier,
            &cancellation, Some(&self.cancellation),
        ).await;
        drop(input);
        drop(namespace.1);
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

    pub(super) async fn hard_link_async_with_carrier(
        &self,
        source: &MountPath,
        destination: &MountPath,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError>
    {
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

    pub(super) async fn remove_async_with_carrier(
        &self,
        path: &MountPath,
        expected: Option<FileId>,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError>
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
    {
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

    pub(super) async fn rename_async_with_carrier(
        &self,
        source: &MountPath,
        destination: &MountPath,
        replace: bool,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError>
    {
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
    where A: 'static,
          O: 'static,{
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
    where A: 'static,
          O: 'static,{
        let record = match self.record_by_id_with_carrier(file_id, owner, carrier).await {
            Ok(record) => record,
            Err(MountSourceError::NotFound) => return Ok(None),
            Err(error) => return Err(error),
        };
        if record.kind != FileKind::Regular { return Err(MountSourceError::Stale); }
        self.attached_from_id_with_carrier(record.file_id, carrier).map(Some)
    }

    async fn open_file_async_with_carrier(
        &self,
        path: &MountPath,
        with_lookup: bool,
        owner: CallbackOwner,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<((Arc<dyn MountOpenFile>, u64), Option<MountLookup>), MountSourceError>
    where A: 'static,
          O: 'static,{
        let original = carrier.original_native_work().expect("opening uses native work");
        original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        let cancellation = carrier.cancellation();
        let namespace = self.namespace_path_with_carrier(path, carrier)?;
        let observation = self.checkout.observe_owned_with_carrier(
            owner, carrier, &cancellation, Some(&self.cancellation),
        ).await?;
        let _view = observation._view;
        let mut observer = observation.observer;
        let accepted = if with_lookup {
            observer.lookup_no_follow_with_metadata_with_native_work(&namespace.0, &original, &cancellation)
                .await.map(|value| value.map(|value| (value.record, Some(value.metadata))))
        } else {
            observer.lookup_no_follow_with_native_work(&namespace.0, &original, &cancellation)
                .await.map(|value| value.record.map(|record| (record, None)))
        };
        observer.proof_data_take_completed_work();
        observer.proof_data_take_completed_work();
        let (record, metadata) = accepted.map_err(MountSourceError::Filesystem)?.ok_or(MountSourceError::NotFound)?;
        if record.kind != FileKind::Regular {
            return Err(MountSourceError::NativeStaticText {
                kind: super::NativeSourceTextKind::Unsupported,
                text: "only regular files can own native file handles",
            });
        }
        if metadata.is_some() {
            original.charge(crate::WorkCounters {
                bytes_copied: std::mem::size_of::<MountLookup>() as u64, ..crate::WorkCounters::UNCHARGED
            }).map_err(MountSourceError::NativeWork)?;
        }
        let lookup = metadata.map(|metadata| MountLookup { node: mount_node(record), metadata });
        let handle = self.attached_from_id_with_carrier(record.file_id, carrier)?;
        Ok((handle, lookup))
    }

    fn open_created_with_carrier(
        &self, created: &MountLookup, mut carrier: NativeDemandCarrier<'_>,
        runtime_meter: &dyn acyclic_native_runtime::NativeReadMeter,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>
    where A: 'static,
          O: 'static,{
        let original = carrier.original_native_work().expect("opening created files uses native work");
        original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        if created.node.kind != MountNodeKind::Regular {
            return Err(MountSourceError::NativeStaticText {
                kind: super::NativeSourceTextKind::Invalid,
                text: "only regular files can own native file handles",
            });
        }
        let pending = {
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
        };
        let Some(pending) = pending else {
            return self.runtime.block_on_measured(|| async {
                self.attached_file_by_id_with_carrier(
                    created.node.file_id, ViewGate::callback_owner(), &mut carrier,
                ).await?.ok_or(MountSourceError::NotFound)
            }, runtime_meter).map_err(capture_io_error)?;
        };
        let allocation = original.claim_allocation(
            std::mem::size_of::<PendingCreatedFile<A, O>>() as u64, 1,
        ).map_err(MountSourceError::NativeWork)?;
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
        Ok((handle, 0))
    }

    async fn detach_file_async_with_carrier(
        &self,
        path: &MountPath,
        owner: CallbackOwner,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError>
    where A: 'static,
          O: 'static,{
        use super::lazy::mount_engine_error;
        let original = carrier.original_native_work().expect("detaching uses native work");
        original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        let cancellation = carrier.cancellation();
        let namespace = self.namespace_path_with_carrier(path, carrier)?;
        let observation = self.checkout.observe_owned_with_carrier(
            owner, carrier, &cancellation, Some(&self.cancellation),
        ).await?;
        let _view = observation._view;
        let mut observer = observation.observer;
        let lookup = observer.lookup_no_follow_with_metadata_with_native_work(
            &namespace.0, &original, &cancellation,
        ).await.map_err(mount_engine_error);
        observer.proof_data_take_completed_work();
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
        observer.proof_data_take_completed_work();
        observer.proof_data_take_completed_work();
        let custody = carrier.new_custody()?;
        let handle = self.detached_handle_from_identity_with_carrier(
            file, lookup.metadata, custody, carrier,
        )?;
        return Ok((handle, 0));

    }

    /// Resolves through the same observer engine using the caller's complete original native capability, or its explicit ordinary demand scope.
    pub(super) async fn lookup_async_with_carrier(
        &self,
        path: &MountPath,
        owner: CallbackOwner,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<Option<MountLookup>, MountSourceError>
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
    {
        use super::lazy::{mount_accept, mount_demand_error, mount_engine_error};
        observer.work.with_work(|work| work.check().map_err(mount_demand_error))?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        let accepted = if let Some(original) = observer.work.original_native_work() {
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

}

impl<A, O> CheckoutMountSource<A, O> {
    /// Tests the same authored stamp using measured canonical namespace conversion and the caller's original cancellation and resource scope.
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
            let unchanged = self.checkout.ledger.unchanged_since_measured(namespace, file_id, stamp, work)?;
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            Ok(unchanged)
        })
    }

}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> CheckoutMountSource<A, O> {
    pub(super) fn record_by_id(&self, file_id: FileId) -> Result<FileRecord, MountSourceError>
    where A: 'static, O: 'static {
        boundary(&self.cancellation, |work| self.runtime.block_on_measured(|| async {
            tokio::time::timeout(CALLBACK_TIMEOUT, self.record_by_id_with_carrier(
                file_id, ViewGate::callback_owner(), &mut NativeDemandCarrier::Native(work),
            )).await.map_err(|_| MountSourceError::Stale)?
        }, work.meter().as_ref()).map_err(capture_io_error)?)
    }

    pub(super) fn detached_file_from_record(
        &self,
        record: FileRecord,
        metadata: FileMetadata,
    ) -> Result<Arc<CheckoutDetachedFile<A, O>>, MountSourceError>
    {
        boundary(&self.cancellation, |work| self.runtime.block_on_measured(|| async {
            tokio::time::timeout(CALLBACK_TIMEOUT, self.detached_file_from_record_with_carrier(
                record, metadata, ViewGate::callback_owner(), &mut NativeDemandCarrier::Native(work),
            )).await.map_err(|_| MountSourceError::Stale)?
        }, work.meter().as_ref()).map_err(capture_io_error)?)
    }

    pub(super) async fn record_by_id_with_carrier(
        &self,
        file_id: FileId,
        owner: CallbackOwner,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<FileRecord, MountSourceError>
    where A: 'static, O: 'static,{
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
        let accepted = if let Some(original) = observer.work.original_native_work() {
            observer.read_file_record_by_id_with_native_work(file_id, &original, &cancellation)
                .await.map_err(MountSourceError::Filesystem)
        } else {
            let budget = observer.work.with_work(|work| work.remaining().map_err(mount_demand_error))?;
            let result = observer.read_file_record_by_id(file_id, budget, &cancellation).await;
            observer.work.with_work(|work| mount_accept(work, result, mount_engine_error))
        };
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
    {
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
    {
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
    {
        let observation = self.checkout.observe_with_carrier(
            owner, carrier, cancellation, Some(&self.cancellation),
        ).await?;
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
    {
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

}

impl<A, O> CheckoutMountSource<A, O> {
    /// Creates an independently cancellable adapter using the shared callback runtime.
    pub fn new(
        checkout: Arc<SharedCheckout<A, O>>,
        config: VolumeConfig,
    ) -> Result<Self, NativeMountError> {
        let root = NamespacePath::new(Vec::new(), config.limits)
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        Self::new_at(checkout, config, root)
    }

    /// Creates an adapter rooted at an authenticated workspace subtree.
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
        let runtime = CallbackRuntime::existing()?;
        let cancellation = CancellationToken::new();
        Ok(Self {
            checkout,
            root,
            limits: config.limits,
            profile: config.profile,
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

    /// Transfers the exact owned namespace and its original allocation guard.
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
        Ok(Arc::new(self))
    }

    /// Whether `path` still names what it named after `stamp`, whatever changed beneath it.
    pub(super) fn binding_unchanged_since(&self, path: &MountPath, stamp: ViewStamp) -> bool {
        self.path(path)
            .is_ok_and(|path| self.checkout.binding_unchanged_since(&path, stamp))
    }

    /// Returns the checkout's owning volume without filesystem I/O.
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
            let quiescent_hint = {
                let gate = self.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
                !gate.producers_pending && !gate.retiring
            };
            let mut credit = phase(quiescent_hint)?;
            let mut gate = self.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
            if gate.retired { return Ok(()); }
            if quiescent_hint {
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

}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> CheckoutMountSource<A, O> {
    /// Publishes every pending mount mutation regardless of automatic policy.
    pub fn sync(&self) -> Result<(), MountSourceError>
    {
        self.runtime.wait(|| self.sync_async())
    }

    /// Asynchronously publishes every pending mount mutation.
    pub async fn sync_async(&self) -> Result<(), MountSourceError>
    {
        self.sync_async_with_permit_budgeted(
            crate::PublicationPermit::Unrestricted, boundary_budget(),
        ).await
    }

    /// Publishes pending mutations only while the supplied operation lease is still active in the shared authority provider.
    pub async fn sync_async_with_permit(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountSourceError>
    {
        self.sync_async_with_permit_budgeted(permit, boundary_budget())
            .await
    }

    pub(super) async fn sync_async_with_permit_budgeted(
        &self,
        permit: crate::PublicationPermit,
        budget: WorkBudget,
    ) -> Result<(), MountSourceError>
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
    {
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

        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        self.sync_async_with_permit_carrier_guarded(
            permit, force, &mut NativeDemandCarrier::Borrowed(work),
            view, cancellation, before_publication,
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
    where
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
    where
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
    where
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
    where
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

    async fn sync_async_with_permit_carrier_guarded_publication<F, Fut, E>(
        &self, permit: crate::PublicationPermit, force: bool,
        carrier: &mut NativeDemandCarrier<'_>,
        view: &mut super::view_ledger::ViewLedgerAdmission,
        cancellation: &CancellationToken, before_publication: &mut F,
        operation_context: Option<crate::Digest>,
        operation_evidence: Option<acyclic_stream::OwnedBytes>,
        snapshot_context: Option<crate::lazy_workspace::NativeSnapshotContext>,
        acknowledged: &mut Option<crate::lazy_workspace::NativePublicationSnapshot>,
        accounting_failure: &mut Option<MountSourceError>,
    ) -> Result<(NativeSourcePublicationAck, Option<crate::lazy_workspace::NativePublicationSnapshot>), E>
    where
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        use super::lazy::mount_demand_error;
        carrier.with_work(|work| work.check().map_err(mount_demand_error))?;
        self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
        self.checkout.settle_pending_with_carrier(carrier, cancellation, Some(&self.cancellation)).await?;
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
    }

    /// Safely advances a clean mounted checkout to the workspace head.
    pub async fn refresh_async(&self) -> Result<(), MountSourceError>
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

    /// Adopts the workspace head after this checkout's mutations were synchronized and a separate fenced operation published that head.
    pub async fn advance_to_head_async(&self) -> Result<(), MountSourceError>
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

    async fn record_advance(
        &self,
        checkout: &mut SharedCheckoutGuard<'_, A, O>,
        base: &Checkout<A, O>,
    ) {
        if checkout.root().file_table == base.root().file_table {
            return;
        }
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

}

impl<A, O> CheckoutMountSource<A, O> {
    fn path(&self, path: &MountPath) -> Result<NamespacePath, MountSourceError> {
        self.namespace_path_with_work(path, &mut crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &self.cancellation))
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

    fn posix_attribute_name_with_work(
        &self,
        bytes: &[u8],
        work: &mut crate::demand::DemandWork,
    ) -> Result<AttributeName, MountSourceError> {
        native_attribute_name_with_work(self.profile, self.limits, bytes, work)
    }

    /// Converts the same exact native names used by capture, admitting each real root/name/vector capacity before its owned allocation is created.
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

    /// Bind the returned native guard before the physical namespace owner so the path drops first; native paths must not be released with raw work.
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
            paths, options, _allocation: scratch.retain(),
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

}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> CheckoutMountSource<A, O> {
    pub(crate) fn capture_host_paths_with_identity(
        &self,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: NativeRootIdentity,
    ) -> Result<(), MountSourceError>
    where
        A: 'static,
        O: 'static,
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
        A: 'static,
        O: 'static,
    {
        if paths.is_empty() { return Ok(()); }
        let native = capture_meter(boundary_budget(), &self.cancellation)
            .map_err(|failure| failure.error)?;
        let meter = native.meter();
        let inputs = self.capture_inputs_with_native_work(source_root, paths, expected_root_identity, scope, &native.native_materialization_work())?;
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

    fn capture_host_with_identity_on_meter(
        &self,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: NativeRootIdentity,
        scope: HostCaptureScope,
        native: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError>
    where
        A: 'static,
        O: 'static,
    {
        let original = native.native_materialization_work();
        self.runtime.block_on_measured(
            || self.capture_host_with_identity_with_native_work(
                source_root, paths, expected_root_identity, scope, None, native,
            ),
            original.runtime_meter(),
        ).map_err(|error| MountSourceError::NativeWork(original.runtime_error(error)))?
    }

    /// Captures through the original invocation, retaining a supplied baseline for the complete canonical capture rather than probing a new identity.
    pub(super) async fn capture_host_with_identity_with_native_work(
        &self,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: NativeRootIdentity,
        scope: HostCaptureScope,
        baseline: Option<Arc<NativeViewBaseline>>,
        native: &super::NativeMountMeterWork,
    ) -> Result<(), MountSourceError>
    where A: 'static,
          O: 'static,{
        native.meter().lock().check().map_err(super::lazy::mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        if paths.is_empty() { return Ok(()); }
        let inputs = self.capture_inputs_with_native_work(
            source_root, paths, expected_root_identity, scope, &native.native_materialization_work(),
        )?;
        self.capture_host_with_identity_async(
            &inputs.paths, &inputs.options, scope,
            HostCapturePolicy { baseline: baseline.as_deref(), request: None, meter: native.meter(), native },
        ).await.map(|_| ())
    }

    /// The SDK lifecycle path retains one admitted meter through native worker completion, including cancellation and all pre-worker failures.
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
        A: 'static,
        O: 'static,
    {
        cancellation.check().map_err(|_| OperationFailure::before_work(MountSourceError::Cancelled))?;
        if paths.is_empty() {
            return Ok(crate::OperationReceipt { value: 0, work: crate::WorkCounters::UNCHARGED });
        }
        let native = capture_meter(budget, cancellation)?;
        let meter = native.meter();
        let result = async {
            let inputs = self.capture_inputs_with_native_work(source_root, paths, expected_root_identity, scope, &native.native_materialization_work())?;
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
        A: 'static,
        O: 'static,
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
                candidate, install_scope, policy.request, policy.meter, &mut carrier, &mut view, cancellation,
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
    ) -> Result<Option<u64>, MountSourceError>
    where
        A: 'static,
        O: 'static,
    {

        let native_work = carrier.original_native_work().expect("capture uses its native invocation");
        let installed = installed_changes_with_native_work(
            &candidate.base, &candidate.checkout, scope, &native_work, cancellation,
        ).await?;
        let (epoch, stamp) = {
            let checkout = self.checkout.lock_with_carrier(
                carrier, cancellation, Some(&self.cancellation),
            ).await?;
            checkout.ensure_publication_resolved()?;
            if request.is_some() && checkout.publishes_at_native_boundary() {
                return native_name_error(
                    "lifecycle capture requires manual publication", true, &mut meter.lock(),
                );
            }
            (self.checkout.view_gate.generation(), self.checkout.ledger.stamp())
        };
        let publication = self.checkout.finish_native_phase_with_commit(
            candidate, epoch, stamp, carrier, cancellation, Some(&self.cancellation),
            |carrier| self.with_capture_observer_dispatch(|| {
                self.checkout.ledger.record_with_carrier(
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
        if publication != MountPublication::Manual {
            let mut before = self.publication_guard();
            self.checkout.seal_original_with_carrier_guarded(
                crate::PublicationPermit::Unrestricted, false, carrier,
                cancellation, Some(&self.cancellation), &mut before,
            ).await?;
        }
        let checkout = self.checkout.read_current_state().await;
        let data = checkout.proof_data_snapshot_with_native_work(&native_work, cancellation)
            .map_err(MountSourceError::Filesystem)?;
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

    async fn mutate_attribute_async_with_carrier(
        &self,
        path: &MountPath,
        name: &[u8],
        value: Option<(&crate::kernel::OwnedSourceChunk, MountAttributeWriteMode)>,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        use super::lazy::mount_demand_error;
        let original = carrier.original_native_work().expect("attributes use native work");
        let cancellation = carrier.with_work(|work| {
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
            Ok::<_, MountSourceError>(work.cancellation().clone())
        })?;
        let namespace = self.namespace_path_with_carrier(path, carrier)?;
        let name = native_attribute_name_with_native_work(self.profile, self.limits, name, &original)?;
        let (mut candidate, epoch, stamp) = self.checkout.prepare_native_phase_with_carrier(
            carrier, &cancellation, Some(&self.cancellation),
        ).await?;
        let node = native_node_if_bound_with_carrier(
            &candidate.checkout, &namespace.0, carrier, &cancellation,
        ).await?.ok_or(MountSourceError::NotFound)?;
        let accepted = match value {
            Some((packet, write_mode)) => candidate.checkout.write_named_attribute_by_id_with_native_work(
                node, &name.0, &packet.bytes, match write_mode {
                    MountAttributeWriteMode::Upsert => NamedAttributeWriteMode::Upsert,
                    MountAttributeWriteMode::Create => NamedAttributeWriteMode::Create,
                    MountAttributeWriteMode::Replace => NamedAttributeWriteMode::Replace,
                }, &original, &cancellation,
            ).await,
            None => candidate.checkout.remove_named_attribute_by_id_with_native_work(
                node, &name.0, &original, &cancellation,
            ).await,
        }.map_err(MountSourceError::Filesystem);
        drop(name);
        drop(namespace);
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

    pub(super) async fn set_attributes_async_with_carrier(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        carrier: &mut NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        use super::lazy::{mount_demand_error, mount_engine_error};
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
            candidate.checkout.set_attributes_by_id_with_native_work(
                node, metadata, logical_bytes, &original, &cancellation,
            ).await.map_err(MountSourceError::Filesystem)
        } else {
            let (budget, prior_live) = namespace.carrier.with_work(|work| {
                Ok::<_, MountSourceError>((work.remaining().map_err(mount_demand_error)?, work.live_allocation_bytes()))
            })?;
            let receipt = candidate.checkout.set_attributes_by_id(
                node, metadata, logical_bytes, budget, &cancellation,
            ).await;
            namespace.carrier.accept_with_prior_live(receipt, prior_live, mount_engine_error)
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
            self.sync_async_with_permit_carrier_guarded(
                crate::PublicationPermit::Unrestricted, false, &mut NativeDemandCarrier::Shared(meter),
                &mut Default::default(), &cancellation, &mut self.sdk_publication_policy(),
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
            self.checkout.settle_pending_with_carrier(carrier, &cancellation, Some(&self.cancellation)).await?;
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
            self.checkout.ledger.node_unchanged_since_measured(file_id, stamp, &mut work)?
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
                self.checkout.ledger.listing_unchanged_since_measured(namespace, stamp, work)?
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
            let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &self.cancellation);
            path.components()
                .iter()
                .fold(MountPath::root(), |folded, component| {
                    folded.child(
                        self.folded_name_with_work(component, &mut work).ok()
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
                self.checkout.ledger.reported_unchanged_since_measured(
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
        boundary(&self.cancellation, |work| self.lookup_with_meter(path, work))
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
        let owner = ViewGate::callback_owner();
        let original = work.native_materialization_work();
        let runtime_meter = original.runtime_meter_owner();
        self.runtime.block_on_measured(
            || async {
                self.lookup_async_with_carrier(
                    path, owner, &mut NativeDemandCarrier::Native(work),
                ).await
            },
            runtime_meter.as_ref(),
        ).map_err(|error| MountSourceError::NativeWork(original.runtime_error(error)))?
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
        boundary(&self.cancellation, |work| self.open_file_with_meter(path, work).map(|(handle, _)| handle))
    }

    fn open_file_with_meter(
        &self, path: &MountPath, work: &super::NativeMountMeterWork,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        self.runtime.block_on_measured(|| async {
            self.open_file_async_with_carrier(path, false, ViewGate::callback_owner(), &mut NativeDemandCarrier::Native(work)).await.map(|(handle, _)| handle)
        }, work.meter().as_ref()).map_err(capture_io_error)?
    }

    fn open_file_with_lookup(
        &self, path: &MountPath,
    ) -> Result<(Arc<dyn MountOpenFile>, MountLookup), MountSourceError> {
        boundary(&self.cancellation, |work| self.open_file_with_lookup_with_meter(path, work).map(|((handle, _), lookup)| (handle, lookup)))
    }

    fn open_file_with_lookup_with_meter(
        &self, path: &MountPath, work: &super::NativeMountMeterWork,
    ) -> Result<((Arc<dyn MountOpenFile>, u64), MountLookup), MountSourceError> {
        self.runtime.block_on_measured(|| async {
            self.open_file_async_with_carrier(path, true, ViewGate::callback_owner(), &mut NativeDemandCarrier::Native(work)).await.map(|(handle, lookup)| (handle, lookup.expect("native lookup was requested")))
        }, work.meter().as_ref()).map_err(capture_io_error)?
    }

    fn open_created(
        &self, _path: &MountPath, created: &MountLookup,
    ) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        boundary(&self.cancellation, |work| self.open_created_with_meter(_path, created, work).map(|(handle, _)| handle))
    }

    fn open_created_with_meter(
        &self, _path: &MountPath, created: &MountLookup, work: &super::NativeMountMeterWork,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        let original = work.native_materialization_work();
        self.open_created_with_carrier(created, NativeDemandCarrier::Native(work), original.runtime_meter())
    }

    fn detach_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        boundary(&self.cancellation, |work| self.detach_file_with_meter(path, work).map(|(handle, _)| handle))
    }

    fn detach_file_with_meter(
        &self, path: &MountPath, work: &super::NativeMountMeterWork,
    ) -> Result<(Arc<dyn MountOpenFile>, u64), MountSourceError> {
        self.runtime.block_on_measured(|| async {
            self.detach_file_async_with_carrier(path, ViewGate::callback_owner(), &mut NativeDemandCarrier::Native(work)).await
        }, work.meter().as_ref()).map_err(capture_io_error)?
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
        boundary(&self.cancellation, |work| self.read_link_with_meter(path, work).map(|packet| packet.bytes))
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
        boundary(&self.cancellation, |work| self.read_range_with_meter(path, offset, length, work).map(|packet| packet.bytes))
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
        boundary(&self.cancellation, |work| self.open_file_with_meter(path, work)?.0.seek_with_meter(offset, target, work))
    }

    fn read_directory(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountDirectoryPage, MountSourceError> {
        boundary(&self.cancellation, |work| self.read_directory_with_meter(path, cursor, maximum_entries, work))
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
        boundary(&self.cancellation, |work| self.create_file_with_meter(path, metadata, work))
    }

    fn create_file_with_meter(
        &self, path: &MountPath, metadata: FileMetadata, work: &super::NativeMountMeterWork,
    ) -> Result<MountLookup, MountSourceError> {
        self.create_file_on_carrier(path, metadata, None, NativeDemandCarrier::Native(work), work.meter())
    }

    fn create_absent_file(
        &self, path: &MountPath, metadata: FileMetadata, absent_since: ViewStamp,
    ) -> Result<MountLookup, MountSourceError> {
        boundary(&self.cancellation, |work| self.create_absent_file_with_meter(path, metadata, absent_since, work))
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
        boundary(&self.cancellation, |work| self.create_directory_with_meter(path, metadata, work))
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
        boundary(&self.cancellation, |work| self.create_symbolic_link_with_meter(path, crate::kernel::OwnedSourceChunk::claimed(target.into(), 0), metadata, work))
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
        boundary(&self.cancellation, |work| self.create_special_with_meter(path, kind, device, metadata, work))
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
        boundary(&self.cancellation, |work| self.set_attributes_with_meter(path, metadata, logical_bytes, work))
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
        boundary(&self.cancellation, |work| self.read_attribute_with_meter(path, name, work).map(|packet| packet.map(|packet| packet.bytes)))
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
            let mut observer = observation.observer;
            original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            let result = observer.read_named_attribute_owned_with_native_work(
                &namespace, &name, &original, &cancellation,
            ).await;
            observer.proof_data_take_completed_work();
            let output = result.map_err(MountSourceError::Filesystem)?;
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
            let mut observer = observation.observer;
            original.admit(crate::WorkCounters::UNCHARGED).map_err(MountSourceError::NativeWork)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            let result = observer.list_named_attributes_with_native_work(
                &namespace, cursor.as_ref(), maximum_entries, &original, &cancellation,
            ).await;
            observer.proof_data_take_completed_work();
            let listing_pair = result.map_err(MountSourceError::Filesystem)?;
            let allocation = listing_pair.1;
            let listing = listing_pair.0;
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
        boundary(&self.cancellation, |work| self.write_attribute_with_meter(path, name, crate::kernel::OwnedSourceChunk::claimed(value.into(), 0), mode, work))
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
        boundary(&self.cancellation, |work| self.remove_attribute_with_meter(path, name, work))
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
        boundary(&self.cancellation, |work| self.open_file_with_meter(path, work)?.0.write_range_with_meter(offset, crate::kernel::OwnedSourceChunk::claimed(bytes.into(), 0), work))
    }

    fn resize(&self, path: &MountPath, logical_bytes: u64) -> Result<(), MountSourceError> {
        boundary(&self.cancellation, |work| self.open_file_with_meter(path, work)?.0.resize_with_meter(logical_bytes, work))
    }

    fn allocate_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        boundary(&self.cancellation, |work| self.open_file_with_meter(path, work)?.0.allocate_range_with_meter(offset, length, operation, work))
    }

    fn clone_range(
        &self,
        source: &MountPath,
        source_offset: u64,
        destination: &MountPath,
        destination_offset: u64,
        length: u64,
    ) -> Result<(), MountSourceError> {
        boundary(&self.cancellation, |work| {
            self.runtime.block_on_measured(|| async {
                let mut carrier = NativeDemandCarrier::Native(work);
                let source = self.namespace_path_with_carrier(source, &mut carrier)?;
                let destination = self.namespace_path_with_carrier(destination, &mut carrier)?;
                self.change_content_async_with_carrier(destination.0, ContentChange::CloneFrom {
                    source: source.0, source_offset, offset: destination_offset, length,
                }, &mut carrier).await
            }, work.meter().as_ref()).map_err(capture_io_error)?
        })
    }

    fn clone_range_by_id(
        &self,
        source_file_id: FileId,
        source_offset: u64,
        destination_file_id: FileId,
        destination_offset: u64,
        length: u64,
    ) -> Result<(), MountSourceError> {
        boundary(&self.cancellation, |work| self.clone_range_by_id_with_meter(source_file_id, source_offset, destination_file_id, destination_offset, length, work))
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
        boundary(&self.cancellation, |work| self.remove_with_meter(path, expected, work))
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
        boundary(&self.cancellation, |work| self.rename_with_meter(source, destination, replace, work))
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
        boundary(&self.cancellation, |work| self.hard_link_with_meter(source, destination, work))
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
        boundary(&self.cancellation, |work| self.flush_with_meter(work))
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
            source_root, paths, expected_root_identity, HostCaptureScope::ExactPaths, work,
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
            HostCaptureScope::Subtree, work,
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

fn content_change_time() -> Result<i64, MountSourceError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).ok()
        .and_then(|since| i64::try_from(since.as_nanos()).ok())
        .ok_or(MountSourceError::NativeStaticText {
            kind: super::NativeSourceTextKind::Engine, text: "the clock is outside file time",
        })
}

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

fn boundary<T>(
    cancellation: &CancellationToken,
    operation: impl FnOnce(&super::NativeMountMeterWork) -> Result<T, MountSourceError>,
) -> Result<T, MountSourceError> {
    let work = capture_meter(boundary_budget(), cancellation).map_err(|failure| failure.error)?;
    operation(&work)
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

    #[cfg(target_os = "macos")]
    #[allow(unsafe_code, reason = "declares the NFS transport's C test hooks")]
    unsafe extern "C" {
        fn nfs4_test_exclusive_replay_identity() -> std::ffi::c_int;
        fn nfs4_test_namedattr_exclusive_replay_identity() -> std::ffi::c_int;
        fn nfs4_test_readdir_cookie_verifier() -> std::ffi::c_int;
        fn nfs4_test_sync_acknowledgement() -> std::ffi::c_int;
    }

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
                &request.cancellation,
            ), original.runtime_meter()).map_err(capture_io_error)?;
            drop(registration);
            assert!(result.is_err());
            assert_eq!(source.checkout.revision(), revision);
        }
        assert!(source.capture_gate.lock().unwrap_or_else(PoisonError::into_inner).active.is_empty());
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
            MountDirectoryPage::with_native_allocation(entries, next_cursor, allocation)
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

    fn native_source_io_control(
        borrowed: &MemorySource, original: &crate::NativeMaterializationWork,
    ) -> Result<Arc<MemorySource>, Box<dyn std::error::Error>> {
        let native = original.native_mount_work();
        Ok(CheckoutMountSource::new_at_meter(
            Arc::clone(&borrowed.checkout), VolumeConfig::portable(Lifecycle::Ephemeral),
            NamespacePath::new(Vec::new(), borrowed.limits)?, original.claim_allocation(0, 0)?, &native, Some(original),
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
    fn native_queued_settlement_obeys_resource_denial_and_keeps_original_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = source(FilesystemProfile::Portable)?;
        let path = native_test_path("native-queued");
        let created = source.create_file(&path, metadata())?;
        let file = source.open_created(&path, &created)?;
        file.write_range(0, Bytes::from_static(b"original native queue"))?;
        file.resize(8)?;
        for deny_allocation in [true, false] {
            let cancellation = CancellationToken::new();
            let root = crate::NativeMaterializationWork::new(
                WorkBudget::UNBOUNDED, &cancellation,
            ).map_err(|(error, _)| error)?;
            let mut resource_budget = WorkBudget::UNBOUNDED;
            if deny_allocation { resource_budget.allocation_operations = 0; }
            else { resource_budget.object_bytes_written = 0; }
            let resource = root.native_volume_work(resource_budget)?;
            let invocation = resource.native_mount_work();
            let result = source.runtime.block_on(|| source.checkout.settle_pending_with_carrier(&mut NativeDemandCarrier::Native(&invocation), &cancellation, None));
            assert!(result.is_err(), "real queued creation ignored the native resource quota");
            assert_eq!(file.read_up_to(0, 64)?.as_ref(), b"original");
        }
        let cancellation = CancellationToken::new();
        let root = crate::NativeMaterializationWork::new(
            WorkBudget::UNBOUNDED, &cancellation,
        ).map_err(|(error, _)| error)?;
        let invocation = root.native_mount_work();
        source.runtime.block_on(|| source.checkout.settle_pending_with_carrier(&mut NativeDemandCarrier::Native(&invocation), &cancellation, None))?;
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
        assert_eq!(file.read_up_to(0, 64)?.as_ref(), b"untouched");
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
        assert_eq!(file.read_up_to(0, 64)?.as_ref(), b"untouched");
        file.settle()?;
        assert_eq!(source.read_range(&path, 0, 9)?.as_ref(), b"untouched");
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

    #[tokio::test(flavor = "current_thread")]
    async fn in_place_callback_outlasts_its_callers_cooperative_budget()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = CallbackRuntime::create()?;
        let lock = tokio::sync::Mutex::new(());
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
            vec![manual.path(&native_test_path("src"))?.components()[0].clone()],
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
        let retained_before_foreign = work.live_allocation_bytes();
        let foreign_input = open.read_sparse_range_with_meter(0, 9, &resource.native_mount_work())?;
        let prefix = work.receipt();
        assert!(matches!(open.write_sparse_range_with_meter(16, foreign_input, &invocation),
            Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::InvalidLayout))));
        let after = work.receipt();
        assert_eq!(after.allocation_operations, prefix.allocation_operations);
        assert_eq!(after.backend_read_operations, prefix.backend_read_operations);
        assert_eq!(work.live_allocation_bytes(), retained_before_foreign);
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
