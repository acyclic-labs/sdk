//! One native callback adapter for every embedded checkout consumer.

use super::view_gate::{CallbackOwner, ViewGate, ViewReadLease, ViewWriteLease};
use super::view_ledger::{ViewChange, ViewEffect, ViewLedger, ViewStamp};
use super::{
    CaptureOptions, MountAttributePage, MountAttributeWriteMode, MountDirectoryEntry,
    MountDirectoryPage, MountFilesystem, MountLookup, MountNode, MountNodeKind, MountOpenFile,
    MountPath, MountPublication, MountRangeAllocation, MountSeekTarget, MountSourceError,
    MountViewLease, NativeMountError, ViewObserver, ViewOrigin, capture_root_identity,
    seal_checkout,
};
use crate::heap_future::in_heap;
use crate::kernel::{
    AttributeClass, AttributeName, ExtentSeekTarget, FileKind, FileMetadata, FilePayload,
    FileRecord, GenerationMutationError, LogicalName, NameEncoding, NamespacePath, RebaseDecision,
};
use crate::model::{CaseSensitivity, FilesystemProfile, VolumeConfig, VolumeLimits};
use crate::native_capture::{
    MAX_NATIVE_EXACT_CAPTURE_PATHS, NativeViewBaseline, capture_paths_batched_with_work,
    capture_subtrees_with_work,
};
#[cfg(test)]
use crate::native_capture::capture_paths_batched_with_baseline;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, AuthoredMutation, ByteRange, CancellationToken,
    Checkout, ContentChange, ContentTimes, DetachedFile, FileId, FsError, GroupedChange,
    GroupedOutcome, NamedAttributeWriteMode, NativeRootIdentity, ObjectId, OperationFailure,
    OperationId, VolumeId, WorkBudget,
};
use bytes::Bytes;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::OnceLock;
use std::sync::PoisonError;
use std::sync::Weak;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
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

#[derive(Default)]
struct CaptureSourceGate {
    cancelled: bool,
    producers: u64,
    active: Vec<Weak<CaptureCommitGate>>,
    allocation: Option<CaptureOwnedAllocation>,
}

/// Stack custody of a real direct producer, including request-less driver calls.
/// This constant gate bookkeeping owns no allocation or namespace item.
struct CaptureProducerLease<'a>(&'a StdMutex<CaptureSourceGate>);

impl Drop for CaptureProducerLease<'_> {
    fn drop(&mut self) {
        let mut gate = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        gate.producers = gate.producers.checked_sub(1).expect("original capture producer custody");
    }
}

struct HostCapturePolicy<'a> {
    baseline: Option<&'a NativeViewBaseline>,
    request: Option<&'a CaptureCommitGate>,
    meter: &'a Arc<crate::demand::NativeDemandReadMeter>,
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
struct CaptureScratchAllocation<'a> {
    work: &'a mut crate::demand::DemandWork,
    bytes: u64,
}

impl<'a> CaptureScratchAllocation<'a> {
    fn new(work: &'a mut crate::demand::DemandWork) -> Self { Self { work, bytes: 0 } }

    fn track(&mut self, bytes: u64) {
        self.bytes = self.bytes.checked_add(bytes)
            .expect("scratch ownership fits the original allocation ledger");
    }

    fn vec<T>(&mut self, count: usize) -> Result<Vec<T>, MountSourceError> {
        let vector = super::lazy::mount_vec::<T>(count, self.work)?;
        self.track((vector.capacity() * std::mem::size_of::<T>()) as u64);
        Ok(vector)
    }

    fn release(&mut self, bytes: u64) -> Result<(), crate::DemandError> {
        self.work.release(bytes)?;
        self.bytes = self.bytes.checked_sub(bytes).expect("scratch releases its own input");
        Ok(())
    }

    fn retain(mut self) -> u64 { std::mem::take(&mut self.bytes) }
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
    fn check(&self) -> Result<(), Self::Error> { self.work.check() }
    fn admit(&mut self, delta: crate::WorkCounters) -> Result<(), Self::Error> {
        crate::native_host::NativeHostWork::admit(self.work, delta)
    }
    fn record(&mut self, delta: crate::WorkCounters) -> Result<(), Self::Error> {
        self.work.record(delta)
    }
    fn claim(&mut self, bytes: u64) -> Result<u64, Self::Error> {
        self.work.allocate(bytes)?;
        self.track(bytes);
        Ok(bytes)
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
    work: LookupObserverWork<'w>,
    bytes: u64,
    finished: bool,
}

enum LookupObserverWork<'w> {
    Borrowed(&'w mut crate::demand::DemandWork),
    Shared(Arc<crate::demand::NativeDemandReadMeter>),
}

impl LookupObserverWork<'_> {
    fn with_work<T>(&mut self, operation: impl FnOnce(&mut crate::demand::DemandWork) -> T) -> T {
        match self {
            Self::Borrowed(work) => operation(work),
            Self::Shared(meter) => operation(&mut meter.lock()),
        }
    }
}

impl<'a, A: AsyncAuthorityStore, O: AsyncObjectStore> LookupObserverOwned<'a, 'static, A, O> {
    pub(super) fn new(
        observer: crate::facade::CheckoutObserver<'a, A, O>,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<Self, MountSourceError> {
        Self::new_in(observer, LookupObserverWork::Shared(Arc::clone(meter)))
    }
}

impl<'a, 'w, A: AsyncAuthorityStore, O: AsyncObjectStore> LookupObserverOwned<'a, 'w, A, O> {
    fn new_borrowed(
        observer: crate::facade::CheckoutObserver<'a, A, O>,
        work: &'w mut crate::demand::DemandWork,
    ) -> Result<Self, MountSourceError> {
        Self::new_in(observer, LookupObserverWork::Borrowed(work))
    }

    fn new_in(
        observer: crate::facade::CheckoutObserver<'a, A, O>,
        work: LookupObserverWork<'w>,
    ) -> Result<Self, MountSourceError> {
        use super::lazy::mount_demand_error;
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
    pub(super) fn finish(&mut self) -> Result<(), MountSourceError> {
        if self.finished { return Ok(()); }
        let pending = self.proof_data_pending_work();
        self.finished = true;
        let result = self.work.with_work(|work| {
            work.merge(pending).map_err(super::lazy::mount_demand_error)
        });
        self.proof_data_take_completed_work();
        result
    }

    fn borrowed_parts(&mut self) -> (
        &mut crate::facade::CheckoutObserver<'a, A, O>,
        &mut crate::demand::DemandWork,
    ) {
        let LookupObserverWork::Borrowed(work) = &mut self.work else {
            unreachable!("direct observer borrows its original invocation");
        };
        (self.value.as_mut().expect("measured observer is live"), work)
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
    vector: Vec<Weak<CaptureCommitGate>>,
    mut allocation: Option<CaptureOwnedAllocation>,
) {
    if let Some(allocation) = allocation.as_mut() {
        // Another live capture can still be using this allocation's ledger.
        // Keep the old buffer physically resident until its owner is available.
        let mut work = allocation.meter.lock();
        drop(vector);
        work.release(allocation.bytes).expect("capture registry ownership balances");
        allocation.bytes = 0;
    } else {
        drop(vector);
    }
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
        original.allocate(std::mem::size_of::<crate::demand::NativeDemandReadMeter>() as u64)
            .map_err(super::lazy::mount_demand_error)?;
        let work = original.take_for_native_queue();
        Ok(Self {
            original,
            meter: Some(Arc::new(crate::demand::NativeDemandReadMeter(StdMutex::new(work)))),
        })
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

    /// Polls the same canonical async producer without runtime reentry. Its
    /// actual future payload is admitted before construction. Future drop and
    /// every accepted SDK alias retire before the borrowed invocation returns;
    /// construction, poll, and destructor panics retain the original prefix.
    pub(super) async fn run_async<T, F>(
        mut self,
        create: impl FnOnce(Arc<crate::demand::NativeDemandReadMeter>) -> F,
    ) -> Result<T, MountSourceError>
    where
        F: Future<Output = Result<T, MountSourceError>>,
    {
        let (result, drop_panic) = {
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
            match created {
                Err(panic) => (Err(panic), None),
                Ok(Err(error)) => (Ok(Err(error)), None),
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
                    // Also catch a destructor panic before recovering the
                    // original carrier. Preserve the first poll panic, if any.
                    let destroyed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        drop(future);
                    })).err();
                    (result, destroyed)
                }
            }
        };
        self.restore();
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
    _allocation: CaptureOwnedAllocation,
}

struct CaptureCallerWait {
    meter: Arc<crate::demand::NativeDemandReadMeter>,
    budget: WorkBudget,
    prepared: Option<crate::cancellation::PreparedCancelled>,
    error: Option<MountSourceError>,
}

impl CaptureCallerWait {
    fn finish(&mut self) {
        let Some(mut prepared) = self.prepared.take() else { return; };
        prepared.retire_waker();
        let mut work = self.meter.lock();
        work.budget = self.budget;
        self.error = work.finish_cancelled(prepared)
            .err().map(super::lazy::mount_wait_error);
    }
}

impl Drop for CaptureCallerWait {
    fn drop(&mut self) {
        self.finish();
        self.meter.lock().release(std::mem::size_of::<StdMutex<Self>>() as u64)
            .expect("capture cancellation carrier balances");
    }
}

struct CaptureRegistration<A, O> {
    source: Arc<CheckoutMountSource<A, O>>,
    request: Arc<CaptureCommitGate>,
    meter: Arc<crate::demand::NativeDemandReadMeter>,
    budget: WorkBudget,
}

impl<A, O> Drop for CaptureRegistration<A, O> {
    fn drop(&mut self) {
        let (removed, vector, allocation, copied) = {
            let mut gate = self.source.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
            let slot = self.request.slot.load(Ordering::Acquire);
            let copied = usize::from(slot + 1 != gate.active.len())
                * std::mem::size_of::<Weak<CaptureCommitGate>>();
            let removed = gate.active.swap_remove(slot);
            if let Some(moved) = gate.active.get(slot).and_then(Weak::upgrade) {
                moved.slot.store(slot, Ordering::Release);
            }
            if gate.active.is_empty() {
                (removed, Some(std::mem::take(&mut gate.active)), gate.allocation.take(), copied)
            } else {
                (removed, None, None, copied)
            }
        };
        drop(removed);
        if let Some(vector) = vector { retire_capture_vector(vector, allocation); }
        let mut work = self.meter.lock();
        work.budget = self.budget;
        work.record(crate::WorkCounters {
            items_examined: 1 + u64::from(copied != 0), bytes_copied: copied as u64,
            ..crate::WorkCounters::UNCHARGED
        }).expect("capture retirement work was reserved");
    }
}

struct CaptureWorkerResources<A, O> {
    inputs: Option<CaptureInputs>,
    registration: Option<CaptureRegistration<A, O>>,
    wait: Arc<StdMutex<CaptureCallerWait>>,
}

impl<A, O> Drop for CaptureWorkerResources<A, O> {
    fn drop(&mut self) {
        drop(self.inputs.take());
        drop(self.registration.take());
        self.wait.lock().unwrap_or_else(PoisonError::into_inner).finish();
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
    runtime: CallbackRuntime,
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
        let runtime = CALLBACK_RUNTIME.get().ok_or(NativeMountError::ServicesNotInitialized)?;
        Self::from_service(runtime)
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
            }),
            bytes,
        })
    }

    /// Polls the original producer in place, retaining its cancellation and
    /// quiescence protocol. No independent timeout drops already-started work.
    /// Release the prepared owner's bytes only after this call returns.
    pub(super) fn wait_prepared<T, F: Future<Output = Result<T, MountSourceError>>>(
        &self, prepared: PreparedCallbackWait, create: impl FnOnce() -> F,
    ) -> Result<T, MountSourceError> {
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
        ledger.record(&ViewChange::Node(self.file.file_id()));
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
}

struct CheckoutAttachedFile<A, O> {
    checkout: Arc<SharedCheckout<A, O>>,
    file_id: FileId,
    runtime: CallbackRuntime,
    cancellation: CancellationToken,
    profile: FilesystemProfile,
    limits: VolumeLimits,
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
}

/// Most bytes a created file holds before its creation is applied.
const PENDING_CREATE_BYTES: u64 = 1 << 20;

/// Waits on the original lock future under the invocation's same meter.
/// Ready locks need no waiter allocation; blocked locks retain their exact
/// cancellation admission through normal completion and future drop.
pub(super) async fn await_measured_lock<F: Future>(
    lock: F,
    work: &mut crate::demand::DemandWork,
    cancellation: &CancellationToken,
    lifetime: Option<&CancellationToken>,
) -> Result<F::Output, MountSourceError> {
        use super::lazy::{mount_demand_error, mount_wait_error};
        fn reserve_cleanup(
            work: &mut crate::demand::DemandWork,
            count: u64,
            reserved: &mut u64,
        ) -> Result<(), MountSourceError> {
            let next_reserved = reserved.checked_add(count)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            let mut budget = work.budget;
            budget.items_examined = budget.items_examined.checked_sub(count)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            work.work.verify(budget).map_err(MountSourceError::Work)?;
            work.budget = budget;
            *reserved = next_reserved;
            Ok(())
        }
        struct LockWait<'a> {
            work: &'a mut crate::demand::DemandWork,
            reserved_items: u64,
            caller_wait: Option<crate::cancellation::PreparedCancelled>,
            lifetime_wait: Option<crate::cancellation::PreparedCancelled>,
        }
        impl LockWait<'_> {
            fn finish(&mut self) -> Result<(), MountSourceError> {
                let quota = self.work.budget.items_examined
                    .checked_add(std::mem::take(&mut self.reserved_items))
                    .ok_or(MountSourceError::Work(crate::WorkError::Overflow))
                    .map(|items| self.work.budget.items_examined = items);
                let caller = self.caller_wait.take().map(|wait| {
                    self.work.finish_cancelled(wait)
                        .map_err(mount_wait_error)
                }).unwrap_or(Ok(()));
                let lifetime = self.lifetime_wait.take().map(|wait| {
                    self.work.finish_cancelled(wait)
                        .map_err(mount_wait_error)
                }).unwrap_or(Ok(()));
                quota.and(caller).and(lifetime)
            }
        }
        impl Drop for LockWait<'_> {
            fn drop(&mut self) {
                // Registration cleanup was reserved before the await. A
                // dropped lock future returns that budget and proof as well.
                let _ = self.finish();
            }
        }
        cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        if lifetime.is_some_and(CancellationToken::is_cancelled) {
            return Err(MountSourceError::Stale);
        }
        work.check().map_err(mount_demand_error)?;
        let mut waits = LockWait {
            work,
            reserved_items: 0,
            caller_wait: None,
            lifetime_wait: None,
        };
        let mut lock = std::pin::pin!(lock);
        let result = std::future::poll_fn(|context| {
            // Poll the original future first: an immediately ready guard
            // requires no cancellation waiter or registry allocation.
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
                let prepared = match waits.work.prepare_cancelled(cancellation) {
                    Ok(prepared) => prepared,
                    Err(error) => return Poll::Ready(Err(mount_wait_error(error))),
                };
                let cleanup = prepared.cleanup_items();
                waits.caller_wait = Some(prepared);
                if let Err(error) = reserve_cleanup(waits.work, cleanup, &mut waits.reserved_items) {
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
                    let prepared = match waits.work.prepare_cancelled(token) {
                        Ok(prepared) => prepared,
                        Err(error) => return Poll::Ready(Err(mount_wait_error(error))),
                    };
                    let cleanup = prepared.cleanup_items();
                    waits.lifetime_wait = Some(prepared);
                    if let Err(error) = reserve_cleanup(waits.work, cleanup, &mut waits.reserved_items) {
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
        waits.work.check().map_err(mount_demand_error)?;
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
    maximum_bytes: u64,
    state: StdMutex<PendingState>,
}

/// A creation's same original body, mutable until its first application.
/// Restored shared bytes stay immutable if a backend retains a clone.
enum PendingBytes {
    Mutable(Vec<u8>),
    Shared { bytes: Bytes, capacity: usize },
}

impl PendingBytes {
    fn capacity(&self) -> usize {
        match self {
            Self::Mutable(bytes) => bytes.capacity(),
            Self::Shared { capacity, .. } => *capacity,
        }
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Mutable(bytes) => bytes,
            Self::Shared { bytes, .. } => bytes,
        }
    }

    fn sharing_allocation(&self) -> u64 {
        match self {
            Self::Mutable(bytes) => u64::from(bytes.capacity() != 0),
            Self::Shared { .. } => 0,
        }
    }

    fn share(self) -> (Self, Bytes) {
        let (bytes, capacity) = match self {
            Self::Mutable(bytes) => {
                let capacity = bytes.capacity();
                (Bytes::from(bytes), capacity)
            }
            Self::Shared { bytes, capacity } => (bytes, capacity),
        };
        let change = bytes.clone();
        (Self::Shared { bytes, capacity }, change)
    }

    fn mutable(&mut self) -> &mut Vec<u8> {
        match self {
            Self::Mutable(bytes) => bytes,
            Self::Shared { .. } => {
                let previous = std::mem::replace(self, Self::Mutable(Vec::new()));
                let mutable = match previous {
                    Self::Mutable(bytes) => bytes,
                    Self::Shared { bytes, capacity } => match bytes.try_into_mut() {
                        Ok(bytes) => bytes.into(),
                        Err(bytes) => {
                            // A cache may still own immutable bytes. Only an
                            // ordinary subsequent write needs this COW copy.
                            let mut mutable = Vec::with_capacity(capacity);
                            mutable.extend_from_slice(&bytes);
                            mutable
                        }
                    },
                };
                *self = Self::Mutable(mutable);
                self.mutable()
            }
        }
    }

    fn resize(&mut self, length: usize, value: u8) {
        self.mutable().resize(length, value);
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
    Failed(MountSourceError),
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
    ) -> Result<Option<GroupedChange>, MountSourceError> {
        use super::lazy::{mount_clone_namespace, mount_demand_error};
        let mut state = self.state();
        let PendingState::Pending { metadata, bytes } = &*state else {
            return Ok(None);
        };
        let retained_bytes = (std::mem::size_of::<FileMetadata>() as u64)
            .checked_add(bytes.capacity() as u64)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        work.adopt(retained_bytes).map_err(mount_demand_error)?;
        let path = mount_clone_namespace(self.path.components(), limits, work)?;
        work.allocate(std::mem::size_of::<FileMetadata>() as u64)
            .map_err(mount_demand_error)?;
        // A spare-capacity Vec eagerly acquires Bytes' sharing owner; a
        // full nonempty Vec acquires it on the first required clone below.
        // Its private refcount/allocator bookkeeping is not logical payload,
        // but the actual allocation operation is part of this preparation.
        work.charge(crate::WorkCounters {
            allocation_operations: bytes.sharing_allocation(),
            bytes_copied: std::mem::size_of::<FileMetadata>() as u64,
            ..crate::WorkCounters::default()
        }).map_err(mount_demand_error)?;
        let prepared_metadata = Box::new(**metadata);
        let PendingState::Pending { metadata, bytes } =
            std::mem::replace(&mut *state, PendingState::Applied)
        else {
            return Err(MountSourceError::Stale);
        };
        let (retained, prepared_bytes) = bytes.share();
        let change = GroupedChange::CreateFile {
            path,
            metadata: prepared_metadata,
            file_id: self.file_id,
            bytes: prepared_bytes,
        };
        *state = PendingState::Applying { metadata, bytes: retained };
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

    fn logical_identity_bytes(&self) -> Result<u64, MountSourceError> {
        (std::mem::size_of::<Self>() as u64)
            .checked_add(super::lazy::mount_namespace_bytes(&self.path))
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))
    }

    fn finish_applying(&self, applied: Result<GroupedOutcome, MountSourceError>) {
        *self.state() = match applied {
            Ok(_) => PendingState::Applied,
            Err(error) => PendingState::Failed(error),
        };
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
    fn applied(&self) -> Result<(), MountSourceError> {
        match &*self.state() {
            PendingState::Failed(error) => Err(error.clone()),
            _ => Ok(()),
        }
    }
}

/// Most queued changes one hold of the checkout applies as one mutation.
const MAXIMUM_GROUPED_CHANGES: usize = 64;

/// Changes waiting for group commit, and whether a drain is running.
#[derive(Default)]
struct GroupedQueue {
    requests: std::collections::VecDeque<GroupedRequest>,
    draining: bool,
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
        match self {
            Self::Pending(create) if matches!(error, MountSourceError::Work(_) | MountSourceError::Cancelled | MountSourceError::Stale) => {
                create.restore_applying_retained();
            }
            reply => reply.send(Err(error)),
        }
    }
}

/// Owns the same reply and outcome buffers across cancellable awaits.
#[derive(Default)]
struct GroupedDelivery {
    replies: Vec<GroupedReply>,
    views: Vec<GroupedView>,
    results: Option<Vec<Result<GroupedOutcome, FsError>>>,
    installed: bool,
}

impl GroupedDelivery {
    fn complete(&mut self, completed: Result<(), MountSourceError>) -> Result<(), MountSourceError> {
        let mut batch = completed.clone();
        if let Some(results) = self.results.take() {
            for ((result, view), reply) in results.into_iter()
                .zip(std::mem::take(&mut self.views)).zip(self.replies.drain(..))
            {
                let (answer, installed) = match result {
                    Ok(outcome) => (completed.clone().map(|()| outcome), self.installed),
                    Err(error) => (Err(grouped_error(error, &view)), false),
                };
                if let Err(error) = &answer {
                    if batch.is_ok() { batch = Err(error.clone()); }
                }
                match answer {
                    Err(error) if !installed => reply.reject_uninstalled(error),
                    answer => reply.send(answer),
                }
            }
        } else {
            let error = completed.err().unwrap_or(MountSourceError::Cancelled);
            for reply in self.replies.drain(..) { reply.reject_uninstalled(error.clone()); }
        }
        batch
    }
}

impl Drop for GroupedDelivery {
    fn drop(&mut self) {
        let _ = self.complete(Err(MountSourceError::Cancelled));
    }
}

fn grouped_error(error: FsError, view: &GroupedView) -> MountSourceError {
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
}

/// Whether a queued change was claimed for application or abandoned by its
/// caller first. Exactly one of the two ever happens.
struct GroupedAdmission(AtomicU8);

const GROUPED_QUEUED: u8 = 0;
const GROUPED_CLAIMED: u8 = 1;
const GROUPED_ABANDONED: u8 = 2;

impl GroupedAdmission {
    fn claim(&self) -> bool {
        self.0
            .compare_exchange(
                GROUPED_QUEUED,
                GROUPED_CLAIMED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
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
        let _ = self.0.0.compare_exchange(
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

/// Locked checkout state. Dereferencing reaches the canonical checkout while
/// publication helpers retain an indeterminate operation in the same lock.
pub struct SharedCheckoutState<A, O> {
    checkout: Checkout<A, O>,
    publication_operation: Option<OperationId>,
    publication: MountPublication,
    revision: Arc<AtomicU64>,
    ledger: Arc<ViewLedger>,
    /// The file table every change up to now has been recorded against.
    recorded_view: ObjectId,
}

impl<A, O> SharedCheckout<A, O> {
    /// Creates the sole shared adapter boundary for one canonical checkout.
    #[must_use]
    pub fn new(checkout: Checkout<A, O>) -> Self {
        Self::with_publication(checkout, MountPublication::CloseAndSync)
    }

    /// Creates a shared checkout with one explicit native publication policy.
    #[must_use]
    pub(crate) fn with_publication_measured(
        checkout: Checkout<A, O>,
        publication: MountPublication,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut crate::WorkCounters,
        budget: WorkBudget,
    ) -> Result<Self, MountSourceError> {
        let before = allocations.live_bytes();
        let result = (|| {
            allocations.claim_bytes(std::mem::size_of::<AtomicU64>() as u64, 1, work, budget)
                .map_err(|error| super::lazy::mount_engine_error(crate::collection::handle_allocation_error(error)))?;
            let revision = Arc::new(AtomicU64::new(1));
            allocations.claim_bytes(std::mem::size_of::<ViewLedger>() as u64, 1, work, budget)
                .map_err(|error| super::lazy::mount_engine_error(crate::collection::handle_allocation_error(error)))?;
            let ledger = Arc::new(ViewLedger::new());
            allocations.claim_bytes(std::mem::size_of::<ViewGate>() as u64, 1, work, budget)
                .map_err(|error| super::lazy::mount_engine_error(crate::collection::handle_allocation_error(error)))?;
            let view_gate = Arc::new(ViewGate::new());
            Ok(Self {
                state: tokio::sync::RwLock::new(SharedCheckoutState {
                    recorded_view: checkout.root().file_table,
                    checkout,
                    publication_operation: None,
                    publication,
                    revision: Arc::clone(&revision),
                    ledger: Arc::clone(&ledger),
                }),
                revision,
                ledger,
                view_gate,
                pending: StdMutex::new(Vec::new()),
                grouped: StdMutex::new(GroupedQueue::default()),
            })
        })();
        if result.is_err() {
            let released = allocations.live_bytes().checked_sub(before)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            allocations.release(released)
                .map_err(|error| super::lazy::mount_engine_error(crate::collection::handle_allocation_error(error)))?;
        }
        result
    }

    /// Creates a shared checkout with one explicit native publication policy.
    #[must_use]
    pub fn with_publication(checkout: Checkout<A, O>, publication: MountPublication) -> Self {
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let mut work = crate::WorkCounters::default();
        Self::with_publication_measured(checkout, publication, &mut allocations, &mut work, WorkBudget::UNBOUNDED)
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
    ) -> Result<GroupedOutcome, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        // A change may depend on a pending creation: apply those first.
        self.settle_pending().await;
        let (reply, outcome) = tokio::sync::oneshot::channel();
        let admission = Arc::new(GroupedAdmission(AtomicU8::new(GROUPED_QUEUED)));
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

    /// Applies queued changes, one group per hold of the checkout, until the
    /// queue is empty. Abandoned changes are discarded unapplied.
    async fn drain_grouped(&self, cancellation: &CancellationToken)
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        in_heap(move || async move {
            loop {
                let mut checkout = self.lock_settled().await;
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
                checkout
                    .apply_grouped_requests(requests, cancellation)
                    .await;
            }
        })
        .await;
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

    async fn lock_settled(&self) -> SharedCheckoutGuard<'_, A, O> {
        let view = self.view_gate.write().await;
        let state = self.state.write().await;
        SharedCheckoutGuard { _view: view, state }
    }

    async fn lock_settled_measured(
        &self,
        work: &mut crate::demand::DemandWork,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<SharedCheckoutGuard<'_, A, O>, MountSourceError> {
        await_measured_lock(self.lock_settled(), work, cancellation, lifetime).await
    }

    /// Records `pending` as created: its name changed now, though its
    /// creation is applied later (see [`PendingCreate`]).
    /// Registers a pending creation, atomically with checking that no other
    /// pending creation holds its path and, given `absent_since`, that
    /// nothing rebound the path since it was found absent. Answers whether
    /// it registered.
    fn hold_pending(&self, pending: Arc<PendingCreate>, absent_since: Option<ViewStamp>) -> bool {
        let mut held = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        if held.iter().any(|other| other.path == pending.path)
            || absent_since.is_some_and(|stamp| !self.binding_unchanged_since(&pending.path, stamp))
        {
            return false;
        }
        {
            let _origin = pending.origin.enter();
            self.ledger.record(&ViewChange::Bound(&pending.path));
        }
        self.revision.fetch_add(1, Ordering::AcqRel);
        held.push(pending);
        true
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
        &self,
        work: &mut crate::demand::DemandWork,
        view: &mut super::view_ledger::ViewLedgerAdmission,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        use super::lazy::{mount_demand_error, mount_items, mount_vec};
        work.check().map_err(mount_demand_error)?;
        cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        if lifetime.is_some_and(CancellationToken::is_cancelled) {
            return Err(MountSourceError::Stale);
        }
        if self.pending.lock().unwrap_or_else(PoisonError::into_inner).is_empty() {
            return Ok(());
        }
        let mut checkout = self.lock_settled_measured(work, cancellation, lifetime).await?;
        // This baseline excludes the measured lock's retained waiter proof.
        // Only local pending/request preparation claims are released below;
        // engine, candidate, and view-ledger ownership keep their own meters.
        let baseline = work.live_allocation_bytes();
        let prepared = {
            let held = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
            let mut requests = Vec::new();
            let result = (|| {
                let queue_bytes = held.capacity()
                    .checked_mul(std::mem::size_of::<Arc<PendingCreate>>())
                    .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
                work.adopt(queue_bytes as u64).map_err(mount_demand_error)?;
                for create in held.iter() {
                    mount_items(work, 1)?;
                    work.adopt(create.logical_identity_bytes()?).map_err(mount_demand_error)?;
                }
                requests = mount_vec::<GroupedRequest>(held.len(), work)?;
                for create in held.iter() {
                    mount_items(work, 1)?;
                    work.allocate(std::mem::size_of::<GroupedAdmission>() as u64)
                        .map_err(mount_demand_error)?;
                    let admission = Arc::new(GroupedAdmission(AtomicU8::new(GROUPED_CLAIMED)));
                    cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
                    if lifetime.is_some_and(CancellationToken::is_cancelled) {
                        return Err(MountSourceError::Stale);
                    }
                    if let Some(change) = create.begin_applying_measured(checkout.checkout.volume_config().limits, work)? {
                        requests.push(GroupedRequest {
                            change,
                            admission,
                            reply: GroupedReply::Pending(Arc::clone(create)),
                            origin: create.origin,
                            recorded: true,
                        });
                    } else {
                        work.release(std::mem::size_of::<GroupedAdmission>() as u64)
                            .map_err(mount_demand_error)?;
                    }
                }
                // This unconditional scan completes after the engine, even
                // when it rejects a prefix. Clearing a wholly terminal queue
                // moves no remaining Arc slots; a restored queue stays intact.
                mount_items(work, held.len())?;
                Ok(())
            })();
            match result {
                Ok(()) => Ok(requests),
                Err(error) => {
                    for request in requests {
                        let GroupedRequest { change, reply, .. } = request;
                        drop(change);
                        if let GroupedReply::Pending(create) = reply {
                            create.restore_applying_retained();
                        }
                    }
                    Err(error)
                }
            }
        };
        let preparation_bytes = work.live_allocation_bytes()
            .checked_sub(baseline)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let requests = match prepared {
            Ok(requests) => requests,
            Err(error) => {
                work.release(preparation_bytes).map_err(mount_demand_error)?;
                return Err(error);
            }
        };
        struct PreparedBytes<'a> {
            work: &'a mut crate::demand::DemandWork,
            bytes: u64,
        }
        impl PreparedBytes<'_> {
            fn release(&mut self) -> Result<(), MountSourceError> {
                self.work.release(std::mem::take(&mut self.bytes)).map_err(mount_demand_error)
            }
        }
        impl Drop for PreparedBytes<'_> {
            fn drop(&mut self) { let _ = self.release(); }
        }
        let mut preparation = PreparedBytes { work, bytes: preparation_bytes };
        let result = checkout
            .apply_grouped_requests_measured(requests, preparation.work, view, cancellation, lifetime)
            .await;
        {
            let mut held = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
            let mut terminal = true;
            for create in held.iter() {
                terminal &= matches!(&*create.state(), PendingState::Applied | PendingState::Failed(_));
            }
            if terminal {
                held.clear();
            }
        }
        let released = preparation.release();
        if result.is_ok() && lifetime.is_some_and(CancellationToken::is_cancelled) {
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
        let state = self.state.read().await;
        Ok(SharedCheckoutObservation { _view: view, state })
    }

    /// Observes the original shared view without settling queued creations.
    /// Reentrant callbacks remain readers even when a writer is queued.
    pub(super) async fn observe_as_is_with_work(
        &self,
        owner: CallbackOwner,
        work: &mut crate::demand::DemandWork,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<SharedCheckoutObservation<'_, A, O>, MountSourceError> {
        let view = self.view_gate.read_for_callback_with_work(
            owner, None, work, cancellation, lifetime,
        ).await?;
        let state = await_measured_lock(self.state.read(), work, cancellation, lifetime).await?;
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
        use super::lazy::mount_demand_error;
        cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        if lifetime.is_some_and(CancellationToken::is_cancelled) {
            return Err(MountSourceError::Stale);
        }
        work.check().map_err(mount_demand_error)?;
        let already_reading = {
            let (proofs, allocations, counters, budget) =
                work.view_gate_admission_parts().map_err(mount_demand_error)?;
            self.view_gate.reads_for_callback_measured(owner, proofs, allocations, counters, budget)
        }?;
        if !already_reading {
            self.settle_pending_measured(work, &mut Default::default(), cancellation, lifetime).await?;
        }
        self.observe_as_is_with_work(owner, work, cancellation, lifetime).await
    }

    /// Whether a native durability request (`fsync`, or a close that
    /// publishes) publishes; [`MountPublication::Manual`] leaves every
    /// publication to an explicit sync or unmount.
    pub(super) async fn publishes_at_native_boundary(&self) -> bool {
        self.state.read().await.publishes_at_native_boundary()
    }

    /// Begins an optimistic transaction on a copy of the current candidate.
    /// No reader is excluded: the copy records nothing into this checkout.
    pub(super) async fn candidate(&self) -> Result<CheckoutCandidate<A, O>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.state.read().await.candidate()
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
        self.view_gate.is_stable() && self.ledger.unchanged_since(path, file_id, stamp)
    }

    fn reported_unchanged_since(
        &self,
        path: &NamespacePath,
        file_id: Option<FileId>,
        stamp: ViewStamp,
    ) -> bool {
        self.view_gate.is_stable() && self.ledger.reported_unchanged_since(path, file_id, stamp)
    }

    fn node_unchanged_since(&self, file_id: FileId, stamp: ViewStamp) -> bool {
        self.view_gate.is_stable() && self.ledger.node_unchanged_since(file_id, stamp)
    }

    fn listing_unchanged_since(&self, path: &NamespacePath, stamp: ViewStamp) -> bool {
        self.view_gate.is_stable() && self.ledger.listing_unchanged_since(path, stamp)
    }

    fn binding_unchanged_since(&self, path: &NamespacePath, stamp: ViewStamp) -> bool {
        self.view_gate.is_stable() && self.ledger.binding_unchanged_since(path, stamp)
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

    async fn seal_with_permit(
        &mut self,
        permit: crate::PublicationPermit,
        force: bool,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.seal_with_permit_measured(permit, force, budget, cancellation).await
            .map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    async fn seal_with_permit_measured(
        &mut self,
        permit: crate::PublicationPermit,
        force: bool,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<()>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let operation_id = self.retained_operation_id();
        let result = if force {
            super::publication::seal_checkout_with_permit_force_measured(
                &mut self.checkout,
                operation_id,
                permit,
                budget,
                cancellation,
            )
            .await
        } else {
            super::publication::seal_checkout_with_permit_measured(
                &mut self.checkout,
                operation_id,
                permit,
                budget,
                cancellation,
            )
            .await
        };
        if result.is_ok()
            || (matches!(&result, Err(failure) if matches!(failure.error, MountSourceError::Stale))
                && self.checkout.mode().mutations == crate::model::MutationMode::PrivateOverlay)
        {
            self.clear_retained_operation(operation_id);
        }
        result
    }

    /// Applies one content change by stable identity and stamps its
    /// modification and status-change times in the same mutation, as a
    /// native write, truncation, or allocation does, then publishes it like
    /// any mutation. Times a profile does not represent stay unavailable.
    pub(super) async fn change_content_by_id(
        &mut self,
        file_id: FileId,
        change: ContentChange<FileId>,
        cancellation: &CancellationToken,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.checkout
            .change_content_by_id(
                file_id,
                change,
                ContentTimes::Stamp(content_change_time()?),
                boundary_budget(),
                cancellation,
            )
            .await
            .map_err(engine_error)?;
        self.publish_after_mutation(ViewChange::Node(file_id), cancellation)
            .await
    }

    /// Applies one content change at an exact path with its time stamp as
    /// one mutation, exactly as [`Self::change_content_by_id`] does.
    pub(super) async fn change_content(
        &mut self,
        path: NamespacePath,
        change: ContentChange<NamespacePath>,
        cancellation: &CancellationToken,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let node = self.node_at(&path, cancellation).await?;
        self.checkout
            .change_content(
                path,
                change,
                ContentTimes::Stamp(content_change_time()?),
                boundary_budget(),
                cancellation,
            )
            .await
            .map_err(engine_error)?;
        self.publish_after_mutation(ViewChange::Node(node), cancellation)
            .await
    }

    /// Records exactly what one mutation changed, then publishes it when the
    /// policy publishes every mutation.
    pub(super) async fn publish_after_mutation(
        &mut self,
        change: ViewChange<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.record(&change);
        self.publish_mutation(cancellation).await
    }

    /// Publishes one recorded mutation when the policy publishes every
    /// mutation.
    pub(super) async fn publish_mutation(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        in_heap(move || async move {
            if self.publication == MountPublication::PerMutation {
                self.seal(cancellation).await?;
            }
            Ok(())
        })
        .await
    }

    async fn publish_at_native_boundary(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        if self.publishes_at_native_boundary() {
            self.seal(cancellation).await?;
        }
        Ok(())
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
        }
    }

    /// The node a mutation is about to change through `path`, read without
    /// recording an observation.
    async fn node_at(
        &self,
        path: &NamespacePath,
        cancellation: &CancellationToken,
    ) -> Result<FileId, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.node_if_bound(path, cancellation)
            .await?
            .ok_or(MountSourceError::NotFound)
    }

    async fn node_if_bound(
        &self,
        path: &NamespacePath,
        cancellation: &CancellationToken,
    ) -> Result<Option<FileId>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        Ok(self
            .checkout
            .inspector()
            .lookup_no_follow(path, boundary_budget(), cancellation)
            .await
            .map_err(engine_error)?
            .value
            .record
            .map(|record| record.file_id))
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

    pub(super) fn candidate_measured(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<CheckoutCandidate<A, O>>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        self.candidate_measured_for_operation(None, budget, cancellation)
    }

    /// Resumes preparation only for this checkout's same retained operation.
    pub(super) fn candidate_for_operation_measured(
        &self,
        operation: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<CheckoutCandidate<A, O>>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        self.candidate_measured_for_operation(Some(operation), budget, cancellation)
    }

    fn candidate_measured_for_operation(
        &self,
        operation: Option<OperationId>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<CheckoutCandidate<A, O>>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        use super::lazy::{mount_accept, mount_demand_error, mount_engine_error};
        let mut meter = crate::demand::DemandWork::new(budget, cancellation);
        let result = (|| {
            match operation {
                Some(operation) if self.publication_operation == Some(operation) => {}
                Some(_) => return Err(MountSourceError::Stale),
                None => self.ensure_publication_resolved()?,
            }
            let result = self.private_candidate_measured(
                meter.remaining().map_err(mount_demand_error)?, cancellation,
            );
            let base = mount_accept(&mut meter, result, mount_engine_error)?;
            meter.adopt(base.logical_owned_bytes()).map_err(mount_demand_error)?;
            let result = self.private_candidate_measured(
                meter.remaining().map_err(mount_demand_error)?, cancellation,
            );
            let checkout = mount_accept(&mut meter, result, mount_engine_error)?;
            Ok(CheckoutCandidate { base, checkout })
        })();
        result.map(|value| crate::OperationReceipt { value, work: meter.work })
            .map_err(|error| crate::OperationFailure::new(error, meter.work))
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

    pub(super) fn install_candidate_measured(
        &mut self,
        candidate: CheckoutCandidate<A, O>,
        installed: &ViewEffect,
        work: &mut crate::demand::DemandWork,
        view: &mut super::view_ledger::ViewLedgerAdmission,
    ) -> Result<bool, MountSourceError> {
        if !self.admits(&candidate) { return Ok(false); }
        // Failed admission can only conservatively invalidate a prefix. It
        // must not install a candidate whose complete effect was not recorded.
        self.ledger.record_measured(&ViewChange::Effect(installed), work, view)?;
        if !self.checkout.adopt_candidate(&candidate.base, candidate.checkout) { return Ok(false); }
        self.revision.fetch_add(1, Ordering::AcqRel);
        self.recorded_view = self.checkout.root().file_table;
        Ok(true)
    }

    /// Installs and fences the supplied operation in the same exclusive hold.
    pub(super) fn install_candidate_with_operation_measured(
        &mut self,
        candidate: CheckoutCandidate<A, O>,
        installed: &ViewEffect,
        operation: OperationId,
        work: &mut crate::demand::DemandWork,
        view: &mut super::view_ledger::ViewLedgerAdmission,
    ) -> Result<bool, MountSourceError> {
        if self.publication_operation.is_some_and(|retained| retained != operation) {
            return Err(MountSourceError::Stale);
        }
        let installed = self.install_candidate_measured(candidate, installed, work, view)?;
        if installed { self.publication_operation = Some(operation); }
        Ok(installed)
    }

    /// Applies queued changes as one group, records exactly what each
    /// successful one changed, publishes once when the policy publishes
    /// every mutation, and answers every caller.
    async fn apply_grouped_requests(
        &mut self,
        requests: Vec<GroupedRequest>,
        cancellation: &CancellationToken,
    ) where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, cancellation);
        let mut view = super::view_ledger::ViewLedgerAdmission::default();
        let _ = self.apply_grouped_requests_measured(requests, &mut work, &mut view, cancellation, None).await;
    }

    async fn apply_grouped_requests_measured(
        &mut self,
        requests: Vec<GroupedRequest>,
        work: &mut crate::demand::DemandWork,
        view: &mut super::view_ledger::ViewLedgerAdmission,
        cancellation: &CancellationToken,
        lifetime: Option<&CancellationToken>,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        use super::lazy::{
            mount_accept, mount_clone_namespace, mount_demand_error, mount_engine_error,
            mount_items, mount_namespace_bytes, mount_vec,
        };
        let check_lifetime = || {
            if lifetime.is_some_and(CancellationToken::is_cancelled) { Err(MountSourceError::Stale) } else { Ok(()) }
        };
        if requests.is_empty() {
            check_lifetime()?;
            return work.check().map_err(mount_demand_error);
        }
        let mut delivery = GroupedDelivery::default();
        let preflight_live = work.live_allocation_bytes();
        // Original requests survive preflight. A private checkout keeps a
        // denied ledger prefix conservative without hiding an installed change.
        let prepared = (|| {
            self.ensure_publication_resolved()?;
            check_lifetime()?;
            mount_items(work, requests.len())?;
            let mut origins = mount_vec::<(ViewOrigin, bool)>(requests.len(), work)?;
            let changes = mount_vec::<GroupedChange>(requests.len(), work)?;
            let replies = mount_vec::<GroupedReply>(requests.len(), work)?;
            let mut views = mount_vec::<GroupedView>(requests.len(), work)?;
            for request in &requests {
                origins.push((request.origin, request.recorded));
                views.push(match &request.change {
                    GroupedChange::CreateFile { path, .. } => GroupedView::Bound(
                        mount_clone_namespace(path.components(), self.checkout.volume_config().limits, work)?,
                    ),
                    GroupedChange::Content { file_id, .. } => GroupedView::Node(*file_id),
                });
            }
            let cloned = self.checkout.private_candidate_measured(
                work.remaining().map_err(mount_demand_error)?, cancellation,
            );
            let candidate = mount_accept(work, cloned, mount_engine_error)?;
            work.adopt(candidate.logical_owned_bytes()).map_err(mount_demand_error)?;
            Ok::<_, MountSourceError>((origins, changes, replies, views, candidate))
        })();
        let (origins, mut changes, replies, views, mut candidate) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                let released = work.live_allocation_bytes() - preflight_live;
                let _ = work.release(released);
                for request in requests {
                    drop(request.change);
                    request.reply.reject_uninstalled(error.clone());
                }
                return Err(error);
            }
        };
        let change_bytes = (changes.capacity() * std::mem::size_of::<GroupedChange>()) as u64;
        let reply_bytes = (replies.capacity() * std::mem::size_of::<GroupedReply>()) as u64;
        let origin_bytes = (origins.capacity() * std::mem::size_of::<(ViewOrigin, bool)>()) as u64;
        let view_bytes = (views.capacity() * std::mem::size_of::<GroupedView>()) as u64
            + views.iter().map(|view| match view {
                GroupedView::Bound(path) => mount_namespace_bytes(path),
                GroupedView::Node(_) => 0,
            }).sum::<u64>();
        delivery.replies = replies;
        delivery.views = views;
        for request in requests {
            changes.push(request.change);
            delivery.replies.push(request.reply);
        }
        let candidate_bytes = candidate.logical_owned_bytes();
        let applied = match work.remaining().map_err(mount_demand_error) {
            Ok(budget) => {
                let applied = candidate.apply_group(changes, budget, cancellation).await;
                mount_accept(work, applied, mount_engine_error)
            }
            Err(error) => { drop(changes); Err(error) }
        };
        let released = work.release(change_bytes).map_err(mount_demand_error);
        let results = match applied.and_then(|results| released.map(|()| results)) {
            Ok(results) => results,
            Err(error) => {
                drop(candidate);
                let _ = delivery.complete(Err(error.clone()));
                drop(delivery);
                drop(origins);
                let _ = work.release(candidate_bytes + origin_bytes + view_bytes + reply_bytes);
                return Err(error);
            }
        };
        let result_bytes = (results.capacity() * std::mem::size_of::<Result<GroupedOutcome, FsError>>()) as u64;
        delivery.results = Some(results);
        let mut result_owned = 0;
        let mut candidate_owned = candidate_bytes;
        let mut completed = (|| {
            check_lifetime()?;
            work.adopt(result_bytes).map_err(mount_demand_error)?;
            result_owned = result_bytes;
            work.release(candidate_owned).map_err(mount_demand_error)?;
            candidate_owned = 0;
            work.adopt(candidate.logical_owned_bytes()).map_err(mount_demand_error)?;
            candidate_owned = candidate.logical_owned_bytes();
            for ((result, change), (origin, recorded)) in delivery.results.as_ref()
                .into_iter().flatten().zip(&delivery.views).zip(&origins)
            {
                if result.is_ok() && !*recorded {
                    let _origin = origin.enter();
                    let change = match change {
                        GroupedView::Bound(path) => ViewChange::Bound(path),
                        GroupedView::Node(file_id) => ViewChange::Node(*file_id),
                    };
                    self.ledger.record_measured(&change, work, view)?;
                }
            }
            work.check().map_err(mount_demand_error)?;
            check_lifetime()
        })();
        let changed = delivery.results.as_ref().into_iter().flatten().any(Result::is_ok);
        let installed = completed.is_ok() && changed;
        if installed {
            // This exclusive guard has stayed held since copying the source.
            self.checkout = candidate;
            delivery.installed = true;
            self.revision.fetch_add(1, Ordering::AcqRel);
            self.recorded_view = self.checkout.root().file_table;
        } else {
            drop(candidate);
            if let Err(error) = work.release(candidate_owned).map_err(mount_demand_error) {
                if completed.is_ok() { completed = Err(error); }
            }
        }
        drop(origins);
        if let Err(error) = work.release(origin_bytes).map_err(mount_demand_error) {
            if completed.is_ok() { completed = Err(error); }
        }
        if installed && self.publication == MountPublication::PerMutation {
            completed = match check_lifetime().and_then(|()| work.remaining().map_err(mount_demand_error)) {
                Ok(budget) => {
                    let published = self.seal_with_permit_measured(
                        crate::PublicationPermit::Unrestricted, false, budget, cancellation,
                    ).await;
                    mount_accept(work, published, |error| error)
                }
                Err(error) => Err(error),
            };
        }
        if completed.is_ok() { completed = check_lifetime(); }
        let mut batch = delivery.complete(completed);
        drop(delivery);
        if let Err(error) = work.release(result_owned + view_bytes + reply_bytes).map_err(mount_demand_error) {
            if batch.is_ok() { batch = Err(error); }
        }
        batch
    }

    /// Records one change to the checkout's view. The exclusive guard keeps
    /// every lookup from overlapping the change it records.
    pub(super) fn record(&mut self, change: &ViewChange<'_>) {
        self.ledger.record(change);
        self.revision.fetch_add(1, Ordering::AcqRel);
        self.recorded_view = self.checkout.root().file_table;
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
    /// Anything beneath this path.
    Subtree(&'a NamespacePath),
}

impl InstallScope<'_> {
    fn paths(&self) -> &[NamespacePath] {
        match self {
            Self::Paths(paths) => paths,
            Self::Subtree(root) => std::slice::from_ref(*root),
        }
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

fn installed_directory_position(
    entries: &[(FileId, Option<NamespacePath>)],
    file_id: FileId,
    meter: &mut crate::demand::DemandWork,
) -> Result<Result<usize, usize>, MountSourceError> {
    let (mut low, mut high) = (0, entries.len());
    while low < high {
        super::lazy::mount_items(meter, 1)?;
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

async fn installed_changes_measured<A, O>(
    base: &Checkout<A, O>,
    candidate: &Checkout<A, O>,
    scope: InstallScope<'_>,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> crate::MeasuredResult<crate::OperationReceipt<ViewEffect>, MountSourceError>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    use super::lazy::{
        mount_accept, mount_clone_namespace, mount_demand_error, mount_engine_error,
        mount_items, mount_namespace_bytes, mount_vec,
    };
    let mut meter = crate::demand::DemandWork::new(budget, cancellation);
    let result = async {
        let result = base.candidate_diff(candidate, MAXIMUM_EXACT_INSTALL_CHANGES,
            meter.remaining().map_err(mount_demand_error)?, cancellation).await;
        let diff = mount_accept(&mut meter, result, mount_engine_error)?;
        if diff.truncated {
            return Ok(ViewEffect { everything: true, ..ViewEffect::default() });
        }
        // Adopt returned buffers so concurrent prefix/output allocations share
        // the same logical peak rather than each resetting it.
        let file_buffer_bytes = (diff.files.capacity() * std::mem::size_of::<crate::FileRecordChange>()) as u64;
        let binding_buffer_bytes = (diff.bindings.capacity() * std::mem::size_of::<crate::DirectoryBindingChange>()) as u64;
        let diff_bytes = file_buffer_bytes + binding_buffer_bytes
            + diff.bindings.iter().map(|binding| binding.name.as_bytes().len() as u64).sum::<u64>();
        meter.adopt(diff_bytes).map_err(mount_demand_error)?;
        let mut installed = ViewEffect {
            nodes: mount_vec(diff.files.len(), &mut meter)?,
            names: mount_vec(diff.bindings.len(), &mut meter)?,
            ..ViewEffect::default()
        };
        for change in diff.files {
            mount_items(&mut meter, 1)?;
            installed.nodes.push(change.file_id);
        }
        meter.release(file_buffer_bytes).map_err(mount_demand_error)?;
        // A sorted owned index admits only changed parent identities and
        // resolves each scope prefix once, without an unbounded hash/tree clone.
        let mut directories = mount_vec::<(FileId, Option<NamespacePath>)>(diff.bindings.len(), &mut meter)?;
        for binding in &diff.bindings {
            if let Err(position) = installed_directory_position(&directories, binding.directory_id, &mut meter)? {
                meter.charge(crate::WorkCounters {
                    bytes_copied: ((directories.len() - position) * std::mem::size_of::<(FileId, Option<NamespacePath>)>()) as u64,
                    ..crate::WorkCounters::default()
                }).map_err(mount_demand_error)?;
                directories.insert(position, (binding.directory_id, None));
            }
        }
        if !directories.is_empty() {
            for path in scope.paths() {
                for length in 0..=path.depth() {
                    let names = path.components().get(..length).ok_or(MountSourceError::Stale)?;
                    let prefix = mount_clone_namespace(names, base.volume_config().limits, &mut meter)?;
                    let bytes = mount_namespace_bytes(&prefix);
                    for checkout in [base, candidate] {
                        let result = checkout.inspector().lookup_no_follow(&prefix,
                            meter.remaining().map_err(mount_demand_error)?, cancellation).await;
                        let lookup = mount_accept(&mut meter, result, mount_engine_error)?;
                        if let Some(record) = lookup.record {
                            if let Ok(position) = installed_directory_position(&directories, record.file_id, &mut meter)? {
                                let entry = directories.get_mut(position).ok_or(MountSourceError::Stale)?;
                                if entry.1.is_none() {
                                    entry.1 = Some(mount_clone_namespace(
                                        prefix.components(), base.volume_config().limits, &mut meter)?);
                                }
                            }
                        }
                    }
                    drop(prefix);
                    meter.release(bytes).map_err(mount_demand_error)?;
                }
            }
        }
        for binding in diff.bindings {
            mount_items(&mut meter, 1)?;
            let position = installed_directory_position(&directories, binding.directory_id, &mut meter)?
                .map_err(|_| MountSourceError::Stale)?;
            let directory = directories.get(position).and_then(|entry| entry.1.as_ref());
            let name_bytes = binding.name.as_bytes().len() as u64;
            match (directory, scope) {
                (Some(directory), _) => installed.names.push((mount_clone_namespace(
                    directory.components(), base.volume_config().limits, &mut meter,
                )?, binding.name)),
                (None, InstallScope::Subtree(root)) => {
                    meter.release(name_bytes).map_err(mount_demand_error)?;
                    if installed.subtrees.is_empty() {
                        installed.subtrees = mount_vec(1, &mut meter)?;
                        installed.subtrees.push(mount_clone_namespace(
                            root.components(), base.volume_config().limits, &mut meter)?);
                    }
                }
                (None, InstallScope::Paths(_)) => {
                    meter.release(name_bytes).map_err(mount_demand_error)?;
                    installed.everything = true;
                }
            }
        }
        meter.release(binding_buffer_bytes).map_err(mount_demand_error)?;
        Ok(installed)
    }.await;
    result.map(|value| crate::OperationReceipt { value, work: meter.work })
        .map_err(|error| crate::OperationFailure::new(error, meter.work))
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
        let state = await_measured_lock(
            self.state.lock(), work, cancellation, Some(&self.cancellation),
        ).await?;
        Ok((state.file.record(), state.metadata, state.mutation_epoch))
    }

    /// Reads the held identity on the original caller's meter, with no runtime
    /// reentry. The returned producer capacity remains live until its caller
    /// physically retires or transfers the owned packet.
    pub(super) async fn read_range_owned_async_with_work(
        &self,
        offset: u64,
        length: u32,
        work: &mut crate::demand::DemandWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        use super::lazy::{mount_accept, mount_demand_error, mount_engine_error};
        let cancellation = work.cancellation().clone();
        let state = await_measured_lock(
            self.state.lock(), work, &cancellation, Some(&self.cancellation),
        ).await?;
        let result = state.file.read_range(
            ByteRange { offset, length: u64::from(length) },
            work.remaining().map_err(mount_demand_error)?, &cancellation,
        ).await;
        let read = mount_accept(work, result, mount_engine_error)?;
        let logical_owned_bytes = read.logical_owned_bytes();
        let admitted = work.record_adoption(logical_owned_bytes)
            .and_then(|()| work.check());
        if let Err(error) = admitted {
            drop(read);
            work.release(logical_owned_bytes).map_err(mount_demand_error)?;
            return Err(mount_demand_error(error));
        }
        if self.cancellation.is_cancelled() {
            drop(read);
            work.release(logical_owned_bytes).map_err(mount_demand_error)?;
            return Err(MountSourceError::Cancelled);
        }
        Ok(crate::kernel::OwnedSourceChunk { bytes: read.bytes, logical_owned_bytes })
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
    fn lookup(&self) -> Result<MountLookup, MountSourceError> {
        self.runtime.wait(|| self.lookup_async())
    }

    fn read_range(&self, offset: u64, length: u32) -> Result<Bytes, MountSourceError> {
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

    fn read_up_to(&self, offset: u64, maximum_bytes: u32) -> Result<Bytes, MountSourceError> {
        self.runtime.wait(|| async {
            let state = self.state.lock().await;
            let length = state
                .file
                .logical_bytes()
                .saturating_sub(offset)
                .min(u64::from(maximum_bytes));
            if length == 0 {
                return Ok(Bytes::new());
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
        self.change_content(ContentChange::Write { offset, bytes })
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
        self.runtime.wait(|| async {
            let mut state = self.state.lock().await;
            state
                .file
                .set_attributes(
                    metadata,
                    logical_bytes,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            state.metadata = metadata;
            state.record_change(&self.ledger)
        })
    }

    fn read_attribute(&self, name: &[u8]) -> Result<Option<Bytes>, MountSourceError> {
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
            .record(&ViewChange::Node(self.pending.file_id));
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
fn held_range(bytes: &[u8], offset: u64, length: u64) -> Bytes {
    let start = usize::try_from(offset)
        .unwrap_or(usize::MAX)
        .min(bytes.len());
    let end = usize::try_from(offset.saturating_add(length))
        .unwrap_or(usize::MAX)
        .min(bytes.len());
    Bytes::copy_from_slice(bytes.get(start..end).unwrap_or_default())
}

impl<A, O> MountOpenFile for PendingCreatedFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn lookup(&self) -> Result<MountLookup, MountSourceError> {
        self.held_or_attached(
            |metadata, bytes, _| Some(self.held_lookup(**metadata, bytes.len() as u64)),
            MountOpenFile::lookup,
        )
    }

    fn read_range(&self, offset: u64, length: u32) -> Result<Bytes, MountSourceError> {
        self.held_or_attached(
            |_, bytes, _| {
                let end = offset.checked_add(u64::from(length))?;
                (end <= bytes.len() as u64).then(|| held_range(bytes, offset, u64::from(length)))
            },
            |file| file.read_range(offset, length),
        )
    }

    fn read_up_to(&self, offset: u64, maximum_bytes: u32) -> Result<Bytes, MountSourceError> {
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

    fn read_attribute(&self, name: &[u8]) -> Result<Option<Bytes>, MountSourceError> {
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
                .apply_grouped(change, &self.cancellation)
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

    fn read_range(&self, offset: u64, length: u32) -> Result<Bytes, MountSourceError> {
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

    fn read_up_to(&self, offset: u64, maximum_bytes: u32) -> Result<Bytes, MountSourceError> {
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
        self.change_content(ContentChange::Write { offset, bytes })
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
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            checkout
                .set_attributes_by_id(
                    self.file_id,
                    metadata,
                    logical_bytes,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            checkout
                .publish_after_mutation(ViewChange::Node(self.file_id), &self.cancellation)
                .await
        })
    }

    fn read_attribute(&self, name: &[u8]) -> Result<Option<Bytes>, MountSourceError> {
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
        let name = self.attribute_name(name)?;
        let mode = match mode {
            MountAttributeWriteMode::Upsert => NamedAttributeWriteMode::Upsert,
            MountAttributeWriteMode::Create => NamedAttributeWriteMode::Create,
            MountAttributeWriteMode::Replace => NamedAttributeWriteMode::Replace,
        };
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            checkout
                .write_named_attribute_by_id(
                    self.file_id,
                    name,
                    value,
                    mode,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(facade_error)?;
            checkout
                .publish_after_mutation(ViewChange::Node(self.file_id), &self.cancellation)
                .await
        })
    }

    fn remove_attribute(&self, name: &[u8]) -> Result<(), MountSourceError> {
        let name = self.attribute_name(name)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            checkout
                .remove_named_attribute_by_id(
                    self.file_id,
                    name,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(facade_error)?;
            checkout
                .publish_after_mutation(ViewChange::Node(self.file_id), &self.cancellation)
                .await
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

pub(super) fn native_mount_name_with_work(
    name: &LogicalName,
    work: &mut crate::demand::DemandWork,
) -> Result<Vec<u8>, MountSourceError> {
    use super::lazy::{mount_demand_error, mount_items};
    let mut scratch = CaptureScratchAllocation::new(work);
    mount_items(scratch.work, 1)?;
    let output = match (name.encoding(), std::env::consts::OS) {
        (NameEncoding::Utf8 | NameEncoding::PosixBytes, "linux" | "macos")
        | (NameEncoding::WindowsUtf16Le, "windows") => {
            let delta = crate::WorkCounters {
                bytes_copied: name.as_bytes().len() as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            scratch.work.work.admit(&delta, &scratch.work.budget).map_err(MountSourceError::Work)?;
            let mut bytes = scratch.vec(name.as_bytes().len())?;
            bytes.extend_from_slice(name.as_bytes());
            scratch.work.record(delta).map_err(mount_demand_error)?;
            bytes
        }
        (NameEncoding::Utf8, "windows") => {
            let text = match std::str::from_utf8(name.as_bytes()) {
                Ok(text) => text,
                Err(_) => {
                    return measured_native_name_error(
                        "authenticated UTF-8 name is malformed", true, scratch.work,
                    );
                }
            };
            let mut length = 0_usize;
            for scalar in text.chars() {
                mount_items(scratch.work, 1)?;
                length = length.checked_add(scalar.len_utf16() * 2)
                    .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            }
            let mut bytes = scratch.vec(length)?;
            for unit in text.encode_utf16() {
                let delta = crate::WorkCounters {
                    items_examined: 1, bytes_encoded: 2,
                    ..crate::WorkCounters::UNCHARGED
                };
                scratch.work.work.admit(&delta, &scratch.work.budget).map_err(MountSourceError::Work)?;
                scratch.work.check().map_err(mount_demand_error)?;
                bytes.extend_from_slice(&unit.to_le_bytes());
                scratch.work.record(delta).map_err(mount_demand_error)?;
            }
            bytes
        }
        _ => {
            return measured_native_name_error(
                "authenticated name encoding is incompatible with this native mount",
                false, scratch.work,
            );
        }
    };
    // The actual capacity stays live in the original invocation, with no
    // borrowed meter alias in the returned name.
    scratch.retain();
    Ok(output)
}

fn measured_native_name_error<T>(
    message: &str,
    invalid: bool,
    work: &mut crate::demand::DemandWork,
) -> Result<T, MountSourceError> {
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
    Err(if invalid { MountSourceError::Invalid(message) } else { MountSourceError::Unsupported(message) })
}

impl<A, O> CheckoutMountSource<A, O> {
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
        self.checkout.ledger.record(change);
    }

    pub(super) fn record_projection_change_measured(
        &self,
        change: &ViewChange<'_>,
        work: &mut crate::demand::DemandWork,
        view: &mut super::view_ledger::ViewLedgerAdmission,
    ) -> Result<(), MountSourceError> {
        self.checkout.ledger.record_measured(change, work, view)
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
        let owner = ViewGate::callback_owner();
        let present = self.runtime.wait(|| async {
            let observation = self.checkout.observe(owner).await?;
            let mut checkout = observation.observer();
            match checkout
                .read_file_record_by_id(file_id, boundary_budget(), &self.cancellation)
                .await
            {
                Ok(record) if record.value.kind == FileKind::Regular => Ok(true),
                Ok(_) => Err(MountSourceError::Stale),
                Err(failure) if matches!(failure.error, FsError::NotFound) => Ok(false),
                Err(failure) => Err(engine_error(failure)),
            }
        })?;
        Ok(present.then(|| {
            Arc::new(CheckoutAttachedFile {
                checkout: Arc::clone(&self.checkout),
                file_id,
                runtime: self.runtime.clone(),
                cancellation: self.cancellation.clone(),
                profile: self.profile,
                limits: self.limits,
            }) as Arc<dyn MountOpenFile>
        }))
    }

    /// What a lookup of `path` would report in `checkout`, a transaction's
    /// candidate or base, without recording an observation.
    pub(super) async fn lookup_in(
        &self,
        checkout: &Checkout<A, O>,
        path: &MountPath,
    ) -> Result<Option<MountLookup>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let path = self.path(path)?;
        let receipt = checkout
            .inspector()
            .lookup_no_follow_with_metadata(&path, boundary_budget(), &self.cancellation)
            .await
            .map_err(engine_error)?;
        Ok(receipt.value.map(|value| MountLookup {
            node: mount_node(value.record),
            metadata: value.metadata,
        }))
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

    async fn read_bytes_async_with_work(
        &self,
        path: &MountPath,
        range: Option<ByteRange>,
        owner: CallbackOwner,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
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
        let observation = {
            let mut work = meter.lock();
            self.checkout.observe_with_work(
                owner, &mut work, &cancellation, Some(&self.cancellation),
            ).await?
        };
        let observer = {
            let mut work = meter.lock();
            let result = observation.observer_measured(
                work.remaining().map_err(mount_demand_error)?, &cancellation,
            );
            mount_accept(&mut work, result, mount_engine_error)?
        };
        let mut observer = LookupObserverOwned::new(observer, meter)?;
        let result = {
            let work = meter.lock();
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            let budget = work.remaining().map_err(mount_demand_error)?;
            match range {
                Some(range) => observer.read_file_range(&namespace, range, budget, &cancellation)
                    .await.map(|receipt| crate::OperationReceipt {
                        value: crate::kernel::OwnedSourceChunk {
                            logical_owned_bytes: receipt.value.logical_owned_bytes(),
                            bytes: receipt.value.bytes,
                        },
                        work: receipt.work,
                    }),
                None => observer.read_symbolic_link_owned(&namespace, budget, &cancellation).await,
            }
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
            let work = meter.lock();
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        }
        let owned = output.value.take().expect("read output remains owned");
        output._allocation.bytes = 0;
        Ok(owned)
    }

    pub(super) async fn read_range_owned_async_with_demand_work(
        &self,
        path: &MountPath,
        offset: u64,
        length: u32,
        owner: CallbackOwner,
        work: &mut crate::demand::DemandWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        work.check().map_err(super::lazy::mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        BorrowedNativeDemandMeter::new(work)?.run_async(|meter| async move {
            self.read_bytes_async_with_work(
                path, Some(ByteRange { offset, length: u64::from(length) }), owner, &meter,
            ).await
        }).await
    }

    pub(super) async fn read_link_owned_async_with_demand_work(
        &self,
        path: &MountPath,
        owner: CallbackOwner,
        work: &mut crate::demand::DemandWork,
    ) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        work.check().map_err(super::lazy::mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        BorrowedNativeDemandMeter::new(work)?.run_async(|meter| async move {
            self.read_bytes_async_with_work(path, None, owner, &meter).await
        }).await
    }

    async fn read_directory_async_with_work(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        owner: CallbackOwner,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<MountDirectoryPage, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        use super::lazy::{mount_accept, mount_demand_error, mount_engine_error, mount_items};
        let (namespace, cancellation) = {
            let mut work = meter.lock();
            (self.namespace_path_with_work(path, &mut work)?, work.cancellation().clone())
        };
        let namespace_bytes = namespace.retained_bytes();
        let namespace = CaptureMeterOwned {
            value: Some(namespace),
            _allocation: CaptureOwnedAllocation { meter: Arc::clone(meter), bytes: namespace_bytes },
        };
        let cursor = if let Some(bytes) = cursor {
            let mut work = meter.lock();
            let (encoding, length) = capture_name_layout(bytes, self.profile, self.limits, &mut work)?;
            let name = capture_native_name(bytes, encoding, length, self.profile, self.limits, &mut work)?;
            let bytes = name.retained_bytes() as u64;
            Some(CaptureMeterOwned {
                value: Some(name),
                _allocation: CaptureOwnedAllocation { meter: Arc::clone(meter), bytes },
            })
        } else { None };
        let observation = {
            let mut work = meter.lock();
            self.checkout.observe_with_work(
                owner, &mut work, &cancellation, Some(&self.cancellation),
            ).await?
        };
        let observer = {
            let mut work = meter.lock();
            let result = observation.observer_measured(
                work.remaining().map_err(mount_demand_error)?, &cancellation,
            );
            mount_accept(&mut work, result, mount_engine_error)?
        };
        let mut observer = LookupObserverOwned::new(observer, meter)?;
        let result = {
            let work = meter.lock();
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            observer.list_directory_records(
                &namespace, cursor.as_ref().map(|name| &**name), maximum_entries,
                work.remaining().map_err(mount_demand_error)?, &cancellation,
            ).await
        };
        let mut page = {
            let mut work = meter.lock();
            let accepted = mount_accept(&mut work, result, mount_engine_error);
            observer.receipt_delivered();
            let page = accepted?;
            let mut bytes = page.entries.capacity().checked_mul(
                std::mem::size_of::<crate::facade::DirectoryRecordEntry>(),
            ).ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
            for entry in &page.entries {
                mount_items(&mut work, 1)?;
                bytes = bytes.checked_add(entry.name.retained_bytes() as u64)
                    .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            }
            if let Err(error) = work.record_adoption(bytes) {
                drop(page);
                work.release(bytes).map_err(mount_demand_error)?;
                return Err(mount_demand_error(error));
            }
            CaptureMeterOwned {
                value: Some(page),
                _allocation: CaptureOwnedAllocation { meter: Arc::clone(meter), bytes },
            }
        };
        observer.finish()?;
        let input = page.value.take().expect("directory records remain owned");
        let has_more = input.has_more;
        let mut work = meter.lock();
        let mut scratch = CaptureScratchAllocation::new(&mut work);
        let result = (|| {
            let mut entries = scratch.vec::<MountDirectoryEntry>(input.entries.len())?;
            for entry in input.entries {
                scratch.work.check().map_err(mount_demand_error)?;
                self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
                let name = native_mount_name_with_work(&entry.name, scratch.work)?;
                scratch.track(name.capacity() as u64);
                let source_name_bytes = entry.name.retained_bytes() as u64;
                drop(entry.name);
                page._allocation.bytes = page._allocation.bytes.checked_sub(source_name_bytes)
                    .expect("the consumed name belongs to this directory page");
                scratch.work.release(source_name_bytes).map_err(mount_demand_error)?;
                scratch.work.charge(crate::WorkCounters {
                    items_examined: 1, bytes_copied: std::mem::size_of::<MountDirectoryEntry>() as u64,
                    ..crate::WorkCounters::UNCHARGED
                }).map_err(mount_demand_error)?;
                entries.push(MountDirectoryEntry {
                    name, node: mount_node(entry.record), metadata: entry.metadata, pin: None,
                });
            }
            // The consumed source vector has physically retired; only native
            // output names and its vector remain live before cursor copying.
            let source_vector_bytes = std::mem::take(&mut page._allocation.bytes);
            scratch.work.release(source_vector_bytes).map_err(mount_demand_error)?;
            let next_cursor = if has_more {
                if let Some(last) = entries.last() {
                    let delta = crate::WorkCounters {
                        bytes_copied: last.name.len() as u64, ..crate::WorkCounters::UNCHARGED
                    };
                    scratch.work.work.admit(&delta, &scratch.work.budget).map_err(MountSourceError::Work)?;
                    let mut cursor = scratch.vec(last.name.len())?;
                    cursor.extend_from_slice(&last.name);
                    scratch.work.record(delta).map_err(mount_demand_error)?;
                    Some(cursor)
                } else { None }
            } else { None };
            scratch.work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            Ok(MountDirectoryPage { entries, next_cursor })
        })();
        if result.is_ok() { scratch.retain(); }
        result
    }

    /// Uses the same as-is reentrant read lane and original shared proof as the
    /// callback lookup. No runtime entry, meter allocation, queue, or independent
    /// budget is introduced when an existing native producer already owns work.
    pub(super) async fn lookup_async_with_native_work(
        &self,
        path: &MountPath,
        owner: CallbackOwner,
        work: &mut crate::demand::DemandWork,
    ) -> Result<Option<MountLookup>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        use super::lazy::{mount_accept, mount_demand_error, mount_engine_error};
        work.check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        let cancellation = work.cancellation().clone();
        let mut scratch = CaptureScratchAllocation::new(work);
        let namespace = self.namespace_path_with_work(path, scratch.work)?;
        scratch.track(namespace.retained_bytes());
        if let Some(pending) = self.checkout.pending_lookup_with_work(&namespace, scratch.work)? {
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            return Ok(Some(pending));
        }
        let checkout = self.checkout.observe_as_is_with_work(
            owner, scratch.work, &cancellation, Some(&self.cancellation),
        ).await?;
        let observed = checkout.observer_measured(
            scratch.work.remaining().map_err(mount_demand_error)?, &cancellation,
        );
        let observer = mount_accept(scratch.work, observed, mount_engine_error)?;
        let mut observer = LookupObserverOwned::new_borrowed(observer, scratch.work)?;
        self.lookup_observer_with_native_work(&namespace, &cancellation, &mut observer).await
    }

    /// Candidate lookup preserves the canonical nonobserving inspector policy.
    pub(super) async fn lookup_in_with_work(
        &self,
        checkout: &Checkout<A, O>,
        path: &MountPath,
        work: &mut crate::demand::DemandWork,
    ) -> Result<Option<MountLookup>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        use super::lazy::{mount_accept, mount_demand_error, mount_engine_error};
        work.check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        let cancellation = work.cancellation().clone();
        let mut scratch = CaptureScratchAllocation::new(work);
        let namespace = self.namespace_path_with_work(path, scratch.work)?;
        scratch.track(namespace.retained_bytes());
        let inspected = checkout.inspector_measured(
            scratch.work.remaining().map_err(mount_demand_error)?, &cancellation,
        );
        let observer = mount_accept(scratch.work, inspected, mount_engine_error)?;
        let mut observer = LookupObserverOwned::new_borrowed(observer, scratch.work)?;
        self.lookup_observer_with_native_work(&namespace, &cancellation, &mut observer).await
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
        let accepted = {
            let (reader, work) = observer.borrowed_parts();
            work.check().map_err(mount_demand_error)?;
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            let result = reader.lookup_no_follow_with_metadata(
                namespace, work.remaining().map_err(mount_demand_error)?, cancellation,
            ).await;
            // First post-query mutation bridges the exact SDK receipt and
            // reconciles actual DATA custody, including every failure prefix.
            mount_accept(work, result, mount_engine_error)
        };
        observer.receipt_delivered();
        let finished = observer.finish();
        let value = accepted?;
        finished?;
        let (_, work) = observer.borrowed_parts();
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
        }))
    }

    pub(super) async fn detached_file_from_record_with_work(
        &self,
        record: FileRecord,
        metadata: FileMetadata,
        work: &mut crate::demand::DemandWork,
        view: &mut super::view_ledger::ViewLedgerAdmission,
        cancellation: &CancellationToken,
    ) -> Result<Arc<CheckoutDetachedFile<A, O>>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        use super::lazy::mount_demand_error;
        let file = self.detached_identity_from_record_with_work(record, work, view, cancellation).await?;
        if let Err(error) = work.allocate(std::mem::size_of::<CheckoutDetachedFile<A, O>>() as u64) {
            let bytes = file.logical_owned_bytes();
            drop(file);
            work.release(bytes).map_err(mount_demand_error)?;
            return Err(mount_demand_error(error));
        }
        Ok(Arc::new(CheckoutDetachedFile {
            state: tokio::sync::Mutex::new(DetachedMountState { file, metadata, mutation_epoch: 0 }),
            ledger: Arc::clone(&self.checkout.ledger),
            runtime: self.runtime.clone(),
            cancellation: self.cancellation.clone(),
            profile: self.profile,
            limits: self.limits,
        }))
    }

    /// Pins the exact detached input, without another mutable facade or Arc.
    pub(super) async fn detached_identity_from_record_with_work(
        &self,
        record: FileRecord,
        work: &mut crate::demand::DemandWork,
        view: &mut super::view_ledger::ViewLedgerAdmission,
        cancellation: &CancellationToken,
    ) -> Result<crate::DetachedFile<A, O>, MountSourceError>
    where A: AsyncAuthorityStore, O: AsyncObjectStore {
        use super::lazy::{mount_accept, mount_demand_error, mount_engine_error};
        work.check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
        let checkout = self.checkout.lock_measured(work, view, cancellation, Some(&self.cancellation)).await?;
        self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
        let result = checkout.detached_from_record_measured(record,
            work.remaining().map_err(mount_demand_error)?, cancellation);
        let file = mount_accept(work, result, mount_engine_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
        work.adopt(file.logical_owned_bytes()).map_err(mount_demand_error)?;
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
            runtime,
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

    /// Cancels future and in-flight canonical operations owned by this mount.
    pub fn cancel(&self) {
        {
            let mut gate = self.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
            gate.cancelled = true;
            // Private request wakers only wake their parked capture worker.
            // Keep registrations in place until that worker has quiesced.
            for request in &gate.active {
                if let Some(request) = request.upgrade() {
                    let _ = request.state.compare_exchange(
                        CAPTURE_PENDING, CAPTURE_CANCELLED, Ordering::AcqRel, Ordering::Acquire,
                    );
                    request.cancellation.cancel();
                }
            }
        }
        self.cancellation.cancel();
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
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.seal(&self.cancellation).await
        })
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
        let mut checkout = self.checkout.lock().await;
        checkout.seal(&self.cancellation).await
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

    pub(super) async fn sync_async_with_permit_force(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.sync_async_with_permit_force_measured(permit, boundary_budget(), &self.cancellation)
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    pub(super) async fn sync_async_with_permit_force_measured(
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
        let result = self.sync_async_with_permit_work(permit, true, &mut work, &mut view, cancellation).await;
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
            &mut || std::future::ready(Ok(())),
        ).await
    }

    pub(super) async fn sync_async_with_permit_work_guarded<F, Fut>(
        &self,
        permit: crate::PublicationPermit,
        force: bool,
        work: &mut crate::demand::DemandWork,
        view: &mut super::view_ledger::ViewLedgerAdmission,
        cancellation: &CancellationToken,
        before_publication: &mut F,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<(), MountSourceError>> + Send,
    {
        use super::lazy::{mount_accept, mount_demand_error};
        work.check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
        let mut checkout = self.checkout.lock_measured(work, view, cancellation, Some(&self.cancellation)).await?;
        work.check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
        before_publication().await?;
        work.check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
        let result = checkout.seal_with_permit_measured(
            permit, force, work.remaining().map_err(mount_demand_error)?, cancellation,
        ).await;
        mount_accept(work, result, |error| error)
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

    fn posix_attribute_name(&self, bytes: &[u8]) -> Result<AttributeName, MountSourceError> {
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

    /// Converts the same exact native names used by capture, admitting each
    /// real root/name/vector capacity before its owned allocation is created.
    pub(super) fn namespace_path_with_work(
        &self,
        path: &MountPath,
        work: &mut crate::demand::DemandWork,
    ) -> Result<NamespacePath, MountSourceError> {
        use super::lazy::{mount_demand_error, mount_items};
        let mut scratch = CaptureScratchAllocation::new(work);
        let result = (|| {
            mount_items(scratch.work, 1)?;
            let mut layouts = scratch.vec::<(NameEncoding, usize)>(path.components().len())?;
            for component in path.components() {
                layouts.push(capture_name_layout(component, self.profile, self.limits, scratch.work)?);
            }
            mount_items(scratch.work, self.root.depth() + layouts.len())?;
            NamespacePath::validate_component_lengths(
                self.root.components().iter().map(|name| name.as_bytes().len())
                    .chain(layouts.iter().map(|(_, length)| *length)),
                self.limits,
            ).map_err(engine_error)?;
            let mut names = scratch.vec::<LogicalName>(self.root.depth() + layouts.len())?;
            for name in self.root.components() {
                let name = capture_copy_name(name.encoding(), name.as_bytes(), self.limits, scratch.work)?;
                scratch.track(name.retained_bytes() as u64);
                names.push(name);
            }
            for (component, (encoding, length)) in path.components().iter().zip(&layouts) {
                let name = capture_native_name(
                    component, *encoding, *length, self.profile, self.limits, scratch.work,
                )?;
                scratch.track(name.retained_bytes() as u64);
                names.push(name);
            }
            mount_items(scratch.work, names.len())?;
            let rooted = NamespacePath::new(names, self.limits).map_err(engine_error)?;
            let bytes = layouts.capacity() as u64 * std::mem::size_of::<(NameEncoding, usize)>() as u64;
            drop(layouts);
            scratch.release(bytes).map_err(mount_demand_error)?;
            Ok(rooted)
        })();
        if result.is_ok() { scratch.retain(); }
        result
    }

    fn capture_inputs(
        &self,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: NativeRootIdentity,
        scope: HostCaptureScope,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<CaptureInputs, MountSourceError> {
        use super::lazy::mount_demand_error;
        if matches!(scope, HostCaptureScope::ExactPaths) && paths.len() > MAX_NATIVE_EXACT_CAPTURE_PATHS {
            return Err(MountSourceError::Invalid("capture path set exceeds the native per-operation limit".to_owned()));
        }
        let mut work = meter.lock();
        let mut scratch = CaptureScratchAllocation::new(&mut work);
        let result = (|| {
            let mut rooted = scratch.vec::<NamespacePath>(paths.len())?;
            for path in paths {
                let path = self.namespace_path_with_work(path, scratch.work)?;
                scratch.track(path.retained_bytes());
                rooted.push(path);
            }
            let (source_root, _) = crate::native_host::clone_path_with_work(source_root, &mut scratch)
                .map_err(mount_demand_error)?;
            let maximum_paths = match scope {
                HostCaptureScope::ExactPaths => u32::try_from(rooted.len())
                    .map_err(|_| MountSourceError::Work(crate::WorkError::Overflow))?,
                HostCaptureScope::Subtree => MAX_NATIVE_SUBTREE_CAPTURE_PATHS,
            };
            Ok((rooted, CaptureOptions {
                source_root, expected_root_identity, maximum_paths, maximum_extent_spans: 65_536,
            }))
        })();
        match result {
            Ok((paths, options)) => {
                let bytes = scratch.retain();
                Ok(CaptureInputs {
                    paths, options, _allocation: CaptureOwnedAllocation { meter: Arc::clone(meter), bytes },
                })
            }
            Err(error) => Err(error),
        }
    }

    fn register_capture(
        self: &Arc<Self>,
        request: Arc<CaptureCommitGate>,
        meter: Arc<crate::demand::NativeDemandReadMeter>,
        budget: WorkBudget,
    ) -> Result<CaptureRegistration<A, O>, MountSourceError> {
        use super::lazy::{mount_demand_error, mount_items, mount_vec};
        let old = {
            let mut work = meter.lock();
            let mut gate = self.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
            mount_items(&mut work, 1)?;
            if gate.cancelled { return Err(MountSourceError::Stale); }
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            let old = if gate.active.len() == gate.active.capacity() {
                let length = gate.active.len().checked_add(1)
                    .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
                let copied = gate.active.len() as u64 * std::mem::size_of::<Weak<CaptureCommitGate>>() as u64;
                work.work.admit(&crate::WorkCounters { bytes_copied: copied, ..crate::WorkCounters::UNCHARGED }, &work.budget)
                    .map_err(MountSourceError::Work)?;
                let mut replacement = mount_vec(length, &mut work)?;
                replacement.append(&mut gate.active);
                work.record(crate::WorkCounters { bytes_copied: copied, ..crate::WorkCounters::UNCHARGED })
                    .map_err(mount_demand_error)?;
                let bytes = replacement.capacity() as u64 * std::mem::size_of::<Weak<CaptureCommitGate>>() as u64;
                let vector = std::mem::replace(&mut gate.active, replacement);
                let allocation = gate.allocation.replace(CaptureOwnedAllocation { meter: Arc::clone(&meter), bytes });
                Some((vector, allocation))
            } else { None };
            request.slot.store(gate.active.len(), Ordering::Release);
            gate.active.push(Arc::downgrade(&request));
            old
        };
        if let Some((vector, allocation)) = old { retire_capture_vector(vector, allocation); }
        Ok(CaptureRegistration { source: Arc::clone(self), request, meter, budget })
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
        let meter = capture_meter(boundary_budget(), &self.cancellation)
            .map_err(|failure| failure.error)?;
        let inputs = self.capture_inputs(source_root, paths, expected_root_identity, scope, &meter)?;
        self.runtime.block_on_measured(
            || tokio::time::timeout(CALLBACK_TIMEOUT, self.capture_host_with_identity_async(
                &inputs.paths, &inputs.options, scope,
                HostCapturePolicy { baseline, request: None, meter: &meter },
            )),
            meter.as_ref(),
        ).map_err(capture_io_error)?.map_err(|_| MountSourceError::Stale)?.map(|_| ())
    }

    /// Uses the driver's complete original invocation for input ownership and
    /// the existing canonical capture/install engine. No root identity probe
    /// or independently operative budget is created at this boundary.
    fn capture_host_with_identity_with_work(
        &self,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: NativeRootIdentity,
        scope: HostCaptureScope,
        work: &mut crate::demand::DemandWork,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        work.check().map_err(super::lazy::mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        if paths.is_empty() { return Ok(()); }
        BorrowedNativeDemandMeter::new(work)?.run(|meter| {
            let inputs = self.capture_inputs(
                source_root, paths, expected_root_identity, scope, meter,
            )?;
            self.runtime.block_on_measured(
                || self.capture_host_with_identity_async(
                    &inputs.paths, &inputs.options, scope,
                    HostCapturePolicy { baseline: None, request: None, meter },
                ),
                meter.as_ref(),
            ).map_err(capture_io_error)?.map(|_| ())
        })
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
        let meter = capture_meter(budget, cancellation)?;
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
            let wait = {
                let mut work = meter.lock();
                let prepared = work.prepare_cancelled(cancellation)
                    .map_err(super::lazy::mount_wait_error)?;
                let cleanup = prepared.cleanup_items().checked_add(2)
                    .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
                if let Err(error) = work.work.admit(
                    &crate::WorkCounters {
                        items_examined: cleanup,
                        bytes_copied: std::mem::size_of::<Weak<CaptureCommitGate>>() as u64,
                        ..crate::WorkCounters::UNCHARGED
                    }, &budget,
                ) {
                    drop(work);
                    drop(inputs);
                    meter.lock().finish_cancelled(prepared).map_err(super::lazy::mount_wait_error)?;
                    return Err(MountSourceError::Work(error));
                }
                if let Err(error) = work.allocate(std::mem::size_of::<StdMutex<CaptureCallerWait>>() as u64) {
                    drop(work);
                    drop(inputs);
                    meter.lock().finish_cancelled(prepared).map_err(super::lazy::mount_wait_error)?;
                    return Err(super::lazy::mount_demand_error(error));
                }
                work.budget.items_examined = budget.items_examined - cleanup;
                work.budget.bytes_copied = budget.bytes_copied - std::mem::size_of::<Weak<CaptureCommitGate>>() as u64;
                Arc::new(StdMutex::new(CaptureCallerWait {
                    meter: Arc::clone(&meter), budget, prepared: Some(prepared), error: None,
                }))
            };
            let mut resources = CaptureWorkerResources {
                inputs: Some(inputs), registration: None, wait: Arc::clone(&wait),
            };
            let source = Arc::clone(self);
            let worker_meter = Arc::clone(&meter);
            let worker_request = Arc::clone(&request);
            let mut worker = acyclic_native_runtime::run_blocking_io_measured(
                crate::obs::in_context(crate::obs::caller_context(), move || {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        resources.registration = Some(source.register_capture(
                            Arc::clone(&worker_request), Arc::clone(&worker_meter), budget,
                        )?);
                        let inputs = resources.inputs.as_ref().expect("capture inputs remain owned");
                        let request = &resources.registration.as_ref().expect("capture registration remains owned").request;
                        source.runtime.block_on_measured(
                            || source.capture_host_with_identity_async(
                                &inputs.paths, &inputs.options, scope,
                                HostCapturePolicy { baseline: Some(&baseline), request: Some(request), meter: &worker_meter },
                            ),
                            worker_meter.as_ref(),
                        ).map_err(capture_io_error).and_then(std::convert::identity)
                    }));
                    drop(resources);
                    result
                }),
                Arc::clone(&meter) as Arc<dyn acyclic_native_runtime::NativeReadMeter>,
            ).map_err(capture_io_error)?;
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
            }.map_err(capture_io_error);
            drop(worker);
            drop(cancel_on_drop);
            drop(request);
            let cleanup = {
                let mut wait = wait.lock().unwrap_or_else(PoisonError::into_inner);
                wait.finish();
                wait.error.take().map_or(Ok(()), Err)
            };
            drop(wait);
            match result {
                Ok(Ok(result)) => cleanup.and(result),
                Ok(Err(payload)) => std::panic::resume_unwind(payload),
                Err(error) => cleanup.and(Err(error)),
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
        use super::lazy::{mount_accept, mount_demand_error};
        let caller_cancellation = policy.meter.lock().cancellation().clone();
        let cancellation = policy.request.map_or(&caller_cancellation, |request| &request.cancellation);
        cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        let _producer = {
            let mut gate = self.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
            if gate.cancelled { return Err(MountSourceError::Stale); }
            gate.producers = gate.producers.checked_add(1)
                .ok_or(MountSourceError::Work(WorkError::Overflow))?;
            CaptureProducerLease(&self.capture_gate)
        };
        let mut view = super::view_ledger::ViewLedgerAdmission::default();
        for _ in 0..3 {
            let mut allocation = CaptureOwnedAllocation { meter: Arc::clone(policy.meter), bytes: 0 };
            let mut candidate = {
                let mut work = policy.meter.lock();
                let checkout = self.checkout.lock_measured(
                    &mut work, &mut view, cancellation, Some(&self.cancellation),
                ).await?;
                let result = checkout.candidate_measured(work.remaining().map_err(mount_demand_error)?, cancellation);
                let candidate = mount_accept(&mut work, result, std::convert::identity)?;
                let bytes = candidate.logical_owned_bytes();
                work.adopt(bytes).map_err(mount_demand_error)?;
                allocation.bytes = bytes;
                candidate
            };
            // Projected reads may call back into this checkout. Its measured
            // view lock is released before observing any virtualization root.
            let captured = {
                let mut work = policy.meter.lock();
                match scope {
                    HostCaptureScope::ExactPaths => capture_paths_batched_with_work(
                        &mut candidate.checkout, paths, options, 64,
                        cancellation, policy.baseline, &mut work,
                    ).await,
                    HostCaptureScope::Subtree => capture_subtrees_with_work(
                        &mut candidate.checkout,
                        std::slice::from_ref(paths.first().ok_or(MountSourceError::Stale)?),
                        options, &crate::CapturePolicy::allow_all(),
                        cancellation, policy.baseline, &mut work,
                    ).await,
                }.map_err(|failure| capture_mount_error(failure.error))
            };
            if let Err(error) = captured {
                drop(candidate);
                drop(allocation);
                return Err(error);
            }
            {
                let mut work = policy.meter.lock();
                let before = candidate.base().proof_data_snapshot_measured(
                    work.remaining().map_err(mount_demand_error)?, cancellation,
                );
                let before = mount_accept(&mut work, before, |error| engine_error(error))?;
                let after = candidate.checkout.proof_data_snapshot_measured(
                    work.remaining().map_err(mount_demand_error)?, cancellation,
                );
                let after = mount_accept(&mut work, after, |error| engine_error(error))?;
                let captured_data = after.newly_retained_since(before)
                    .map_err(|error| MountSourceError::Work(error))?;
                work.release(allocation.bytes).map_err(mount_demand_error)?;
                allocation.bytes = 0;
                let bytes = candidate.logical_owned_bytes().checked_add(captured_data)
                    .ok_or(MountSourceError::Work(WorkError::Overflow))?;
                work.adopt(bytes).map_err(mount_demand_error)?;
                allocation.bytes = bytes;
            }
            let install_scope = match scope {
                HostCaptureScope::ExactPaths => InstallScope::Paths(paths),
                HostCaptureScope::Subtree => InstallScope::Subtree(paths.first().ok_or(MountSourceError::Stale)?),
            };
            let installed = self.commit_host_capture(
                candidate, install_scope, policy.request, policy.meter, &mut view, cancellation, &mut allocation.bytes,
            ).await;
            drop(allocation);
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
        let budget = meter.lock().remaining().map_err(mount_demand_error)?;
        let installed = candidate.installed_changes_measured(scope, budget, cancellation).await;
        let installed = {
            let mut work = meter.lock();
            let installed = mount_accept(&mut work, installed, std::convert::identity)?;
            let bytes = capture_effect_bytes(&installed, &mut work)?;
            work.adopt(bytes).map_err(mount_demand_error)?;
            installed_allocation.bytes = bytes;
            installed
        };
        let mut work = meter.lock();
        let mut checkout = self.checkout.lock_measured(
            &mut work, view, cancellation, Some(&self.cancellation),
        ).await?;
        checkout.ensure_publication_resolved()?;
        if !checkout.admits(&candidate) { return Ok(None); }
        let base_bytes = candidate.base().logical_owned_bytes();
        {
            let source_gate = self.capture_gate.lock().unwrap_or_else(PoisonError::into_inner);
            if source_gate.cancelled { return Err(MountSourceError::Stale); }
            self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            if let Some(request) = request {
                request.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
                if checkout.publishes_at_native_boundary() {
                    return Err(MountSourceError::Invalid("lifecycle capture requires manual publication".to_owned()));
                }
                request.state.compare_exchange(
                    CAPTURE_PENDING, CAPTURE_COMMITTED, Ordering::AcqRel, Ordering::Acquire,
                ).map_err(|_| MountSourceError::Stale)?;
            }
            if !checkout.install_candidate_measured(candidate, &installed, &mut work, view)? {
                return Ok(None);
            }
            // The installed checkout stays resident in the source; only the
            // optimistic baseline is dropped when this attempt completes.
            *candidate_bytes = base_bytes;
        }
        if checkout.publishes_at_native_boundary() {
            let published = checkout.seal_with_permit_measured(
                crate::PublicationPermit::Unrestricted, false,
                work.remaining().map_err(mount_demand_error)?, cancellation,
            ).await;
            mount_accept(&mut work, published, std::convert::identity)?;
        }
        let data = checkout.proof_data_snapshot_measured(
            work.remaining().map_err(mount_demand_error)?, cancellation,
        );
        let data = mount_accept(&mut work, data, |error| engine_error(error))?;
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
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            checkout
                .change_content(path, change, &self.cancellation)
                .await
        })
    }

    async fn binding_lease_async_with_work(
        &self,
        owner: CallbackOwner,
        expected_epoch: Option<u64>,
        meter: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> Result<ViewReadLease, MountSourceError> {
        use super::lazy::mount_demand_error;
        let mut work = meter.lock();
        let cancellation = work.cancellation().clone();
        work.check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
        let already_reading = {
            let (proofs, allocations, counters, budget) =
                work.view_gate_admission_parts().map_err(mount_demand_error)?;
            self.checkout.view_gate.reads_for_callback_measured(
                owner, proofs, allocations, counters, budget,
            )
        }?;
        if !already_reading {
            self.checkout.settle_pending_measured(
                &mut work, &mut super::view_ledger::ViewLedgerAdmission::default(),
                &cancellation, Some(&self.cancellation),
            ).await?;
        }
        let lease = self.checkout.view_gate.read_for_callback_with_work(
            owner, expected_epoch, &mut work, &cancellation, Some(&self.cancellation),
        ).await?;
        work.check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
        Ok(lease)
    }
}

impl<A, O> MountFilesystem for CheckoutMountSource<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
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
        self.checkout.ledger.observe(observer);
    }

    fn observe_view_with_work(
        &self,
        observer: Weak<dyn ViewObserver>,
        work: &mut super::NativeMountWork<'_>,
    ) -> Result<(), MountSourceError> {
        use super::lazy::mount_demand_error;
        work.work.check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
        self.checkout.ledger.observe_measured(observer, work.work)?;
        // Registration remains installed if cancellation wins after its
        // admitted push; its live backing stays in the original root proof.
        work.work.check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Stale)
    }

    fn fence_changes_with_work(
        &self,
        work: &mut super::NativeMountWork<'_>,
    ) -> Result<(), MountSourceError> {
        use super::lazy::mount_demand_error;
        work.work.check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
        BorrowedNativeDemandMeter::new(work.work)?.run(|meter| {
            self.runtime.block_on_measured(|| async {
                let mut work = meter.lock();
                let cancellation = work.cancellation().clone();
                let mut view = super::view_ledger::ViewLedgerAdmission::default();
                self.checkout.settle_pending_measured(
                    &mut work, &mut view, &cancellation, Some(&self.cancellation),
                ).await?;
                let publishes = {
                    let state = await_measured_lock(
                        self.checkout.state.read(), &mut work, &cancellation,
                        Some(&self.cancellation),
                    ).await?;
                    state.publishes_at_native_boundary()
                };
                if publishes {
                    self.sync_async_with_permit_work(
                        crate::PublicationPermit::Unrestricted, false,
                        &mut work, &mut view, &cancellation,
                    ).await?;
                }
                work.check().map_err(mount_demand_error)?;
                self.cancellation.check().map_err(|_| MountSourceError::Stale)
            }, meter.as_ref()).map_err(capture_io_error)?
        })
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

    fn binding_epoch(&self) -> Option<u64> {
        Some(self.checkout.view_gate.generation())
    }

    fn view_is_stable(&self) -> bool {
        self.checkout.view_gate.is_stable()
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
        use super::lazy::mount_demand_error;
        work.work.check().map_err(mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Stale)?;
        let owner = ViewGate::callback_owner();
        BorrowedNativeDemandMeter::new(work.work)?.run(|meter| {
            let mut lease = self.runtime.block_on_measured(
                || self.binding_lease_async_with_work(owner, expected_epoch, meter),
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
            // The original gate proof owns the admitted Box payload. Its
            // physical release is reconciled by that same proof, without
            // retaining an alias of the borrowed callback meter.
            let lease = Box::new(lease) as Box<dyn MountViewLease>;
            meter.lock().record(copied).map_err(mount_demand_error)?;
            Ok(lease)
        })
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
        work.work.check().map_err(super::lazy::mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        let owner = ViewGate::callback_owner();
        BorrowedNativeDemandMeter::new(work.work)?.run(|meter| {
            self.runtime.block_on_measured(
                || async {
                    let mut work = meter.lock();
                    self.lookup_async_with_native_work(path, owner, &mut work).await
                },
                meter.as_ref(),
            ).map_err(capture_io_error)?
        })
    }

    fn lookup_pinned_with_work(
        &self,
        path: &MountPath,
        work: &mut super::NativeMountWork<'_>,
    ) -> Result<Option<(MountLookup, Option<super::MountContentPin>)>, MountSourceError> {
        // Checkout content has no external source to pin, exactly as in the
        // canonical lookup_pinned default.
        Ok(self.lookup_with_work(path, work)?.map(|lookup| (lookup, None)))
    }

    fn read_pinned_with_work(
        &self,
        _path: &MountPath,
        _pin: super::MountContentPin,
        _offset: u64,
        _length: u64,
        _piece: u32,
        _sink: &mut super::ContentSink<'_>,
        work: &mut super::NativeMountWork<'_>,
    ) -> Result<(), MountSourceError> {
        work.work.check().map_err(super::lazy::mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        // lookup_pinned never issues a checkout content pin. The inherited
        // reader rejects every foreign pin before resolving or reading a path.
        Err(MountSourceError::Stale)
    }

    fn open_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let path = self.path(path)?;
        let owner = ViewGate::callback_owner();
        let file_id = self.runtime.wait(|| async {
            let observation = self.checkout.observe(owner).await?;
            let mut checkout = observation.observer();
            let record = checkout
                .lookup_no_follow(&path, boundary_budget(), &self.cancellation)
                .await
                .map_err(engine_error)?
                .value
                .record
                .ok_or(MountSourceError::NotFound)?;
            if record.kind != FileKind::Regular {
                return Err(MountSourceError::Invalid(
                    "only regular files can own native file handles".to_owned(),
                ));
            }
            Ok(record.file_id)
        })?;
        Ok(Arc::new(CheckoutAttachedFile {
            checkout: Arc::clone(&self.checkout),
            file_id,
            runtime: self.runtime.clone(),
            cancellation: self.cancellation.clone(),
            profile: self.profile,
            limits: self.limits,
        }))
    }

    fn open_created(
        &self,
        _path: &MountPath,
        created: &MountLookup,
    ) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let attached = CheckoutAttachedFile {
            checkout: Arc::clone(&self.checkout),
            file_id: created.node.file_id,
            runtime: self.runtime.clone(),
            cancellation: self.cancellation.clone(),
            profile: self.profile,
            limits: self.limits,
        };
        Ok(match self.checkout.pending_create(created.node.file_id) {
            Some(pending) => Arc::new(PendingCreatedFile { pending, attached }),
            None => Arc::new(attached),
        })
    }

    fn detach_file(&self, path: &MountPath) -> Result<Arc<dyn MountOpenFile>, MountSourceError> {
        let path = self.path(path)?;
        let owner = ViewGate::callback_owner();
        let (file, metadata) = self.runtime.wait(|| async {
            let observation = self.checkout.observe(owner).await?;
            observation.ensure_publication_resolved()?;
            let mut checkout = observation.observer();
            let lookup = checkout
                .lookup_no_follow_with_metadata(&path, boundary_budget(), &self.cancellation)
                .await
                .map_err(engine_error)?
                .value
                .ok_or(MountSourceError::NotFound)?;
            if lookup.record.kind != FileKind::Regular {
                return Err(MountSourceError::Invalid(
                    "only regular files can own detached native handles".to_owned(),
                ));
            }
            let file = checkout
                .detach_regular_file(&path, boundary_budget(), &self.cancellation)
                .await
                .map_err(engine_error)?
                .value;
            Ok((file, lookup.metadata))
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
        }))
    }

    fn read_link(&self, path: &MountPath) -> Result<Bytes, MountSourceError> {
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
        work.work.check().map_err(super::lazy::mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        let owner = ViewGate::callback_owner();
        BorrowedNativeDemandMeter::new(work.work)?.run(|meter| {
            self.runtime.block_on_measured(
                || self.read_bytes_async_with_work(path, None, owner, meter),
                meter.as_ref(),
            ).map_err(capture_io_error)?
        })
    }

    fn read_range(
        &self,
        path: &MountPath,
        offset: u64,
        length: u32,
    ) -> Result<Bytes, MountSourceError> {
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
        work.work.check().map_err(super::lazy::mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        let owner = ViewGate::callback_owner();
        BorrowedNativeDemandMeter::new(work.work)?.run(|meter| {
            self.runtime.block_on_measured(
                || self.read_bytes_async_with_work(
                    path, Some(ByteRange { offset, length: u64::from(length) }), owner, meter,
                ),
                meter.as_ref(),
            ).map_err(capture_io_error)?
        })
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
            Ok(MountDirectoryPage {
                entries,
                next_cursor,
            })
        })
    }

    fn read_directory_with_work(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        work: &mut super::NativeMountWork<'_>,
    ) -> Result<MountDirectoryPage, MountSourceError> {
        work.work.check().map_err(super::lazy::mount_demand_error)?;
        self.cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        let owner = ViewGate::callback_owner();
        BorrowedNativeDemandMeter::new(work.work)?.run(|meter| {
            self.runtime.block_on_measured(
                || self.read_directory_async_with_work(path, cursor, maximum_entries, owner, meter),
                meter.as_ref(),
            ).map_err(capture_io_error)?
        })
    }

    fn create_file(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        // Sampled before the lookup, so it proves what the lookup found.
        let absent_since = self.checkout.ledger.stamp();
        if self.lookup(path)?.is_some() {
            return Err(MountSourceError::AlreadyExists);
        }
        self.create_absent_file(path, metadata, absent_since)
    }

    fn create_absent_file(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        absent_since: ViewStamp,
    ) -> Result<MountLookup, MountSourceError> {
        let mount_path = path;
        let path = self.path(path)?;
        // A creation waits to be applied, but not for publication to resolve.
        // An uncontended state answers in place, without entering the runtime.
        match self.checkout.state.try_read() {
            Ok(state) => state.ensure_publication_resolved()?,
            Err(_) => self.runtime.wait(|| async {
                self.checkout
                    .state
                    .read()
                    .await
                    .ensure_publication_resolved()
            })?,
        }
        let waiting = self
            .checkout
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len();
        if waiting >= MAXIMUM_GROUPED_CHANGES {
            self.runtime.wait(|| async {
                self.checkout.settle_pending().await;
                Ok(())
            })?;
        }
        let file_id = FileId::new();
        let pending = || {
            Arc::new(PendingCreate {
                path: path.clone(),
                file_id,
                origin: ViewOrigin::current(),
                maximum_bytes: PENDING_CREATE_BYTES.min(self.limits.maximum_read_bytes),
                state: StdMutex::new(PendingState::Pending {
                    metadata: Box::new(metadata),
                    bytes: PendingBytes::Mutable(Vec::new()),
                }),
            })
        };
        if !self.checkout.hold_pending(pending(), Some(absent_since)) {
            // Something rebound the path since the caller looked, or another
            // creation holds it: this lookup decides instead.
            if self.lookup(mount_path)?.is_some() || !self.checkout.hold_pending(pending(), None) {
                return Err(MountSourceError::AlreadyExists);
            }
        }
        Ok(MountLookup {
            node: MountNode {
                file_id,
                kind: MountNodeKind::Regular,
                logical_bytes: 0,
                link_count: 1,
                device: None,
            },
            metadata,
        })
    }

    fn create_directory(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let path = self.path(path)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let receipt = checkout
                .apply_authored_transaction(
                    vec![AuthoredMutation::CreateDirectory {
                        path: path.clone(),
                        metadata,
                    }],
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(facade_error)?;
            let file_id = receipt
                .value
                .created_file_ids
                .first()
                .copied()
                .flatten()
                .ok_or_else(|| {
                    MountSourceError::Engine("create omitted file identity".to_owned())
                })?;
            checkout
                .publish_after_mutation(ViewChange::Bound(&path), &self.cancellation)
                .await?;
            Ok(MountLookup {
                node: MountNode {
                    file_id,
                    kind: MountNodeKind::Directory,
                    logical_bytes: 0,
                    link_count: 1,
                    device: None,
                },
                metadata,
            })
        })
    }

    fn create_symbolic_link(
        &self,
        path: &MountPath,
        target: Bytes,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let path = self.path(path)?;
        let logical_bytes = u64::try_from(target.len()).map_err(|_| {
            MountSourceError::Unsupported("symbolic-link target is too large".to_owned())
        })?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let receipt = checkout
                .apply_authored_transaction(
                    vec![AuthoredMutation::CreateSymbolicLink {
                        path: path.clone(),
                        target,
                        metadata,
                    }],
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(facade_error)?;
            let file_id = receipt
                .value
                .created_file_ids
                .first()
                .copied()
                .flatten()
                .ok_or_else(|| {
                    MountSourceError::Engine("create omitted file identity".to_owned())
                })?;
            checkout
                .publish_after_mutation(ViewChange::Bound(&path), &self.cancellation)
                .await?;
            Ok(MountLookup {
                node: MountNode {
                    file_id,
                    kind: MountNodeKind::SymbolicLink,
                    logical_bytes,
                    link_count: 1,
                    device: None,
                },
                metadata,
            })
        })
    }

    fn create_special(
        &self,
        path: &MountPath,
        kind: MountNodeKind,
        device: Option<(u32, u32)>,
        metadata: FileMetadata,
    ) -> Result<MountLookup, MountSourceError> {
        let path = self.path(path)?;
        let (file_kind, mutation) = match (kind, device) {
            (MountNodeKind::Fifo, None) => (
                FileKind::Fifo,
                AuthoredMutation::CreateEmptySpecial {
                    path: path.clone(),
                    kind: FileKind::Fifo,
                    metadata,
                },
            ),
            (MountNodeKind::Socket, None) => (
                FileKind::Socket,
                AuthoredMutation::CreateEmptySpecial {
                    path: path.clone(),
                    kind: FileKind::Socket,
                    metadata,
                },
            ),
            (MountNodeKind::CharacterDevice, Some((major, minor))) => (
                FileKind::CharacterDevice,
                AuthoredMutation::CreateDevice {
                    path: path.clone(),
                    kind: FileKind::CharacterDevice,
                    major,
                    minor,
                    metadata,
                },
            ),
            (MountNodeKind::BlockDevice, Some((major, minor))) => (
                FileKind::BlockDevice,
                AuthoredMutation::CreateDevice {
                    path: path.clone(),
                    kind: FileKind::BlockDevice,
                    major,
                    minor,
                    metadata,
                },
            ),
            _ => {
                return Err(MountSourceError::Unsupported(
                    "special-file kind and device identity disagree".to_owned(),
                ));
            }
        };
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let receipt = checkout
                .apply_authored_transaction(vec![mutation], boundary_budget(), &self.cancellation)
                .await
                .map_err(facade_error)?;
            let file_id = receipt
                .value
                .created_file_ids
                .first()
                .copied()
                .flatten()
                .ok_or_else(|| {
                    MountSourceError::Engine("create omitted file identity".to_owned())
                })?;
            checkout
                .publish_after_mutation(ViewChange::Bound(&path), &self.cancellation)
                .await?;
            Ok(MountLookup {
                node: MountNode {
                    file_id,
                    kind,
                    logical_bytes: 0,
                    link_count: 1,
                    device: match file_kind {
                        FileKind::CharacterDevice | FileKind::BlockDevice => device,
                        _ => None,
                    },
                },
                metadata,
            })
        })
    }

    fn set_attributes(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let node = checkout.node_at(&path, &self.cancellation).await?;
            let mut authored = vec![AuthoredMutation::SetMetadata {
                path: path.clone(),
                metadata,
            }];
            if let Some(logical_bytes) = logical_bytes {
                authored.push(AuthoredMutation::Resize {
                    path,
                    logical_bytes,
                });
            }
            checkout
                .apply_authored_transaction(authored, boundary_budget(), &self.cancellation)
                .await
                .map_err(facade_error)?;
            checkout
                .publish_after_mutation(ViewChange::Node(node), &self.cancellation)
                .await
        })
    }

    fn read_attribute(
        &self,
        path: &MountPath,
        name: &[u8],
    ) -> Result<Option<Bytes>, MountSourceError> {
        let path = self.path(path)?;
        let name = self.posix_attribute_name(name)?;
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| async {
            let observation = self.checkout.observe(owner).await?;
            let mut checkout = observation.observer();
            checkout
                .read_named_attribute(&path, &name, boundary_budget(), &self.cancellation)
                .await
                .map(|receipt| receipt.value)
                .map_err(engine_error)
        })
    }

    fn list_attributes(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        let path = self.path(path)?;
        let cursor = cursor
            .map(|name| self.posix_attribute_name(name))
            .transpose()?;
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| async {
            let observation = self.checkout.observe(owner).await?;
            let mut checkout = observation.observer();
            let receipt = checkout
                .list_named_attributes(
                    &path,
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
        path: &MountPath,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        let name = self.posix_attribute_name(name)?;
        let mode = match mode {
            MountAttributeWriteMode::Upsert => NamedAttributeWriteMode::Upsert,
            MountAttributeWriteMode::Create => NamedAttributeWriteMode::Create,
            MountAttributeWriteMode::Replace => NamedAttributeWriteMode::Replace,
        };
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let node = checkout.node_at(&path, &self.cancellation).await?;
            checkout
                .write_named_attribute(
                    path,
                    name,
                    value,
                    mode,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(facade_error)?;
            checkout
                .publish_after_mutation(ViewChange::Node(node), &self.cancellation)
                .await
        })
    }

    fn remove_attribute(&self, path: &MountPath, name: &[u8]) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        let name = self.posix_attribute_name(name)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let node = checkout.node_at(&path, &self.cancellation).await?;
            checkout
                .remove_named_attribute(path, name, boundary_budget(), &self.cancellation)
                .await
                .map_err(facade_error)?;
            checkout
                .publish_after_mutation(ViewChange::Node(node), &self.cancellation)
                .await
        })
    }

    fn write_range(
        &self,
        path: &MountPath,
        offset: u64,
        bytes: Bytes,
    ) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        self.change_content(path, ContentChange::Write { offset, bytes })
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
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            checkout
                .change_content_by_id(
                    destination_file_id,
                    ContentChange::CloneFrom {
                        source: source_file_id,
                        source_offset,
                        offset: destination_offset,
                        length,
                    },
                    &self.cancellation,
                )
                .await
        })
    }

    fn remove(&self, path: &MountPath, expected: Option<FileId>) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let node = match expected {
                Some(node) => node,
                None => checkout.node_at(&path, &self.cancellation).await?,
            };
            checkout
                .remove(
                    path.clone(),
                    expected,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            checkout
                .publish_after_mutation(ViewChange::Unbound(&path, node), &self.cancellation)
                .await
        })
    }

    fn rename(
        &self,
        source: &MountPath,
        destination: &MountPath,
        replace: bool,
    ) -> Result<(), MountSourceError> {
        let source = self.path(source)?;
        let destination = self.path(destination)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let moved = checkout.node_at(&source, &self.cancellation).await?;
            let replaced = checkout
                .node_if_bound(&destination, &self.cancellation)
                .await?;
            checkout
                .rename(
                    source.clone(),
                    destination.clone(),
                    replace,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            checkout
                .publish_after_mutation(
                    ViewChange::Moved {
                        from: &source,
                        to: &destination,
                        moved,
                        replaced,
                    },
                    &self.cancellation,
                )
                .await
        })
    }

    fn hard_link(
        &self,
        source: &MountPath,
        destination: &MountPath,
    ) -> Result<(), MountSourceError> {
        let source = self.path(source)?;
        let destination = self.path(destination)?;
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            let node = checkout.node_at(&source, &self.cancellation).await?;
            checkout
                .hard_link(
                    source,
                    destination.clone(),
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
            checkout
                .publish_after_mutation(ViewChange::Linked(&destination, node), &self.cancellation)
                .await
        })
    }

    fn flush(&self) -> Result<(), MountSourceError> {
        self.runtime.wait(|| async {
            // A manual checkout publishes nothing here, so it never excludes
            // every callback just to decide that.
            if !self.checkout.publishes_at_native_boundary().await {
                return Ok(());
            }
            let mut checkout = self.checkout.lock().await;
            checkout
                .publish_at_native_boundary(&self.cancellation)
                .await
        })
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
        self.capture_host_with_identity_with_work(
            source_root, paths, expected_root_identity, HostCaptureScope::ExactPaths, work.work,
        )
    }

    fn capture_host_subtree_with_work(
        &self,
        source_root: &Path,
        path: &MountPath,
        expected_root_identity: NativeRootIdentity,
        work: &mut super::NativeMountWork<'_>,
    ) -> Result<(), MountSourceError> {
        self.capture_host_with_identity_with_work(
            source_root, std::slice::from_ref(path), expected_root_identity,
            HostCaptureScope::Subtree, work.work,
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
        other => MountSourceError::Engine(other.to_string()),
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
                    .run_async::<(), _>(|meter| async move {
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
            bytes: Bytes,
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
        let meter = capture_meter(boundary_budget(), &token)?;
        for cancel_source in [false, true] {
            let request = capture_request(&meter)?;
            let mut view = super::super::view_ledger::ViewLedgerAdmission::default();
            let candidate = source.runtime.block_on_measured(|| async {
                let mut work = meter.lock();
                let checkout = source.checkout.lock_measured(
                    &mut work, &mut view, &token, Some(source.cancellation()),
                ).await?;
                let result = checkout.candidate_measured(
                    work.remaining().map_err(super::super::lazy::mount_demand_error)?, &token,
                );
                let candidate = super::super::lazy::mount_accept(&mut work, result, std::convert::identity)?;
                work.adopt(candidate.logical_owned_bytes()).map_err(super::super::lazy::mount_demand_error)?;
                Ok::<_, MountSourceError>(candidate)
            }, meter.as_ref()).map_err(capture_io_error)??;
            let mut resident = candidate.logical_owned_bytes();
            let registration = source.register_capture(Arc::clone(&request), Arc::clone(&meter), boundary_budget())?;
            if cancel_source {
                source.cancel();
            } else {
                drop(CancelCaptureOnDrop(Arc::clone(&request)));
            }
            assert!(request.cancellation.is_cancelled());
            let result = source.runtime.block_on_measured(|| source.commit_host_capture(
                candidate, InstallScope::Paths(&[]), Some(&request), &meter, &mut view,
                &request.cancellation, &mut resident,
            ), meter.as_ref()).map_err(capture_io_error)?;
            meter.lock().release(resident).map_err(super::super::lazy::mount_demand_error)?;
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
        let (source, _) =
            shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
        let directory = tempfile::tempdir()?;
        std::fs::write(directory.path().join("file"), b"not imported")?;
        let identity = capture_root_identity(directory.path())?;
        let path = native_test_path("file");
        for subtree in [false, true] {
            let cancellation = CancellationToken::new();
            let budget = WorkBudget { allocation_operations: 2, ..boundary_budget() };
            let mut work = crate::demand::DemandWork::new(budget, &cancellation);
            work.allocate(std::mem::size_of::<[u8; 16]>() as u64)?;
            let resident = Box::new([0_u8; 16]);
            work.record(crate::WorkCounters { items_examined: 9, ..crate::WorkCounters::UNCHARGED })?;
            let failure = {
                let mut invocation = super::super::NativeMountWork { work: &mut work };
                if subtree {
                    source.capture_host_subtree_with_work(directory.path(), &path, identity, &mut invocation)
                } else {
                    source.capture_host_paths_with_work(
                        directory.path(), std::slice::from_ref(&path), identity, &mut invocation,
                    )
                }
            };
            assert!(matches!(failure, Err(MountSourceError::Work(_))));
            assert_eq!(work.budget, budget);
            assert_eq!(work.work.allocation_operations, 2);
            assert_eq!(work.work.items_examined, 9);
            assert_eq!(work.work.backend_read_operations, 0);
            assert_eq!(work.live_allocation_bytes(), std::mem::size_of_val(resident.as_ref()) as u64);
            assert!(work.cancellation().same_state(&cancellation));
            assert!(source.lookup(&path)?.is_none());
            drop(resident);
            work.release(std::mem::size_of::<[u8; 16]>() as u64)?;
        }
        Ok(())
    }

    #[test]
    fn borrowed_driver_capture_cancellation_never_replaces_original_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let (source, _) =
            shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
        let directory = tempfile::tempdir()?;
        std::fs::write(directory.path().join("file"), b"not imported")?;
        let identity = capture_root_identity(directory.path())?;
        let path = native_test_path("file");
        let cancellation = CancellationToken::new();
        let mut work = crate::demand::DemandWork::new(boundary_budget(), &cancellation);
        work.record(crate::WorkCounters {
            items_examined: 11, backend_read_operations: 2, ..crate::WorkCounters::UNCHARGED
        })?;
        let prefix = work.work;
        cancellation.cancel();
        let error = source.capture_host_paths_with_work(
            directory.path(), std::slice::from_ref(&path), identity,
            &mut super::super::NativeMountWork { work: &mut work },
        ).expect_err("original cancellation precedes every host boundary");
        assert!(matches!(error, MountSourceError::Cancelled));
        assert_eq!(work.work, prefix);
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
        let mut work = crate::demand::DemandWork::new(budget, &cancellation);
        work.record(crate::WorkCounters {
            items_examined: 19, backend_read_operations: 2, ..crate::WorkCounters::UNCHARGED
        })?;
        let prefix = work.work;
        cancellation.cancel();
        let mut invocation = super::super::NativeMountWork { work: &mut work };
        assert!(matches!(source.lookup_pinned_with_work(&path, &mut invocation), Err(MountSourceError::Cancelled)));
        assert!(matches!(source.read_link_with_work(&path, &mut invocation), Err(MountSourceError::Cancelled)));
        assert!(matches!(source.read_range_with_work(&path, 0, 8, &mut invocation), Err(MountSourceError::Cancelled)));
        assert!(matches!(source.read_directory_with_work(&MountPath::root(), None, 1, &mut invocation), Err(MountSourceError::Cancelled)));
        let mut delivered = 0;
        assert!(matches!(source.read_pinned_with_work(
            &path, super::super::MountContentPin([7; 32]), 0, 8, 4,
            &mut |_, _| { delivered += 1; Ok(()) }, &mut invocation,
        ), Err(MountSourceError::Cancelled)));
        assert_eq!(delivered, 0);
        assert_eq!(work.work, prefix);
        assert_eq!(work.budget, budget);
        assert!(work.cancellation().same_state(&cancellation));
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
        let mut work = crate::demand::DemandWork::new(budget, &cancellation);
        work.allocate(16)?;
        let resident = Box::new([0_u8; 16]);
        work.record(crate::WorkCounters { items_examined: 23, ..crate::WorkCounters::UNCHARGED })?;
        let lookup = source.lookup_pinned_with_work(
            &path, &mut super::super::NativeMountWork { work: &mut work },
        )?.ok_or("actual file disappeared")?;
        assert!(lookup.1.is_none(), "canonical checkout content has no external pin");
        let bytes = source.read_range_with_work(
            &path, 1, 3, &mut super::super::NativeMountWork { work: &mut work },
        )?;
        assert_eq!(bytes.bytes.as_ref(), b"ont");
        assert_eq!(bytes.logical_owned_bytes, 3);
        let target = source.read_link_with_work(
            &link, &mut super::super::NativeMountWork { work: &mut work },
        )?;
        assert_eq!(target.bytes.as_ref(), b"actual");
        assert_eq!(target.logical_owned_bytes, 6);
        let first = source.read_directory_with_work(
            &MountPath::root(), None, 1, &mut super::super::NativeMountWork { work: &mut work },
        )?;
        assert_eq!(first.entries.len(), 1);
        assert_eq!(first.entries[0].name, path.components()[0]);
        let cursor = first.next_cursor.as_deref().ok_or("owned continuation missing")?;
        let second = source.read_directory_with_work(
            &MountPath::root(), Some(cursor), 1, &mut super::super::NativeMountWork { work: &mut work },
        )?;
        assert_eq!(second.entries.len(), 1);
        assert_eq!(second.entries[0].name, link.components()[0]);
        assert!(second.next_cursor.is_none());
        let mut delivered = 0;
        assert!(matches!(source.read_pinned_with_work(
            &path, super::super::MountContentPin([7; 32]), 0, 7, 2,
            &mut |_, _| { delivered += 1; Ok(()) },
            &mut super::super::NativeMountWork { work: &mut work },
        ), Err(MountSourceError::Stale)));
        assert_eq!(delivered, 0);
        assert!(work.work.items_examined > 23);
        assert!(work.live_allocation_bytes() >= 16 + 3 + 6);
        assert_eq!(work.budget, budget);
        assert!(work.cancellation().same_state(&cancellation));
        let bytes_capacity = bytes.logical_owned_bytes;
        let target_capacity = target.logical_owned_bytes;
        drop(bytes);
        work.release(bytes_capacity)?;
        drop(target);
        work.release(target_capacity)?;
        for page in [first, second] {
            let capacity = page.entries.capacity() * std::mem::size_of::<MountDirectoryEntry>()
                + page.entries.iter().map(|entry| entry.name.capacity()).sum::<usize>()
                + page.next_cursor.as_ref().map_or(0, Vec::capacity);
            drop(page);
            work.release(capacity as u64)?;
        }
        drop(resident);
        work.release(16)?;
        Ok(())
    }

    #[test]
    fn borrowed_driver_lookup_native_name_failure_restores_original_work()
    -> Result<(), Box<dyn std::error::Error>> {
        let (source, _) =
            shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
        let path = MountPath::root().child(if cfg!(windows) { vec![0, 0xd8] } else { vec![0xff] });
        let cancellation = CancellationToken::new();
        let budget = boundary_budget();
        let mut work = crate::demand::DemandWork::new(budget, &cancellation);
        work.record(crate::WorkCounters { items_examined: 8, ..crate::WorkCounters::UNCHARGED })?;
        let error = source.lookup_with_work(
            &path, &mut super::super::NativeMountWork { work: &mut work },
        ).expect_err("canonical native name conversion must reject an opaque portable name");
        assert!(matches!(error, MountSourceError::Unsupported(_) | MountSourceError::Invalid(_)));
        assert_eq!(work.budget, budget);
        assert!(work.work.items_examined >= 8);
        assert!(work.work.allocation_operations > 0);
        assert_eq!(work.work.backend_read_operations, 0);
        assert_eq!(work.live_allocation_bytes(), 0);
        assert!(work.cancellation().same_state(&cancellation));
        let prefix = work.work;
        cancellation.cancel();
        assert!(matches!(source.lookup_with_work(
            &path, &mut super::super::NativeMountWork { work: &mut work },
        ), Err(MountSourceError::Cancelled)));
        assert_eq!(work.work, prefix);
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
            let mut work = crate::demand::DemandWork::new(budget, &cancellation);
            work.allocate(std::mem::size_of::<[u8; 64]>() as u64)?;
            let resident = Box::new([0_u8; 64]);
            work.record(crate::WorkCounters { items_examined: 13, ..crate::WorkCounters::UNCHARGED })?;
            {
                let mut invocation = super::super::NativeMountWork { work: &mut work };
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
            assert_eq!(work.budget, budget);
            assert!(work.work.items_examined > 13);
            assert!(work.work.backend_read_operations > 0);
            assert!(work.work.source_entries_visited > 0);
            assert!(work.work.source_bytes_read >= 6);
            assert!(work.live_allocation_bytes() >= std::mem::size_of_val(resident.as_ref()) as u64);
            assert!(work.cancellation().same_state(&cancellation));
            work.work.verify(budget)?;
            drop(resident);
            work.release(std::mem::size_of::<[u8; 64]>() as u64)?;
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
                        .cloned()
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
        let change = pending.begin_applying_measured(source.limits, &mut first)?
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
        let change = pending.begin_applying_measured(source.limits, &mut second)?
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

            let held = detached.state.lock().await;
            caller.cancel();
            let mut work = crate::demand::DemandWork::new(WorkBudget::default(), &caller);
            assert!(matches!(
                detached.snapshot_with_work(&mut work, &caller).await,
                Err(MountSourceError::Cancelled),
            ));
            assert_eq!(work.work, crate::WorkCounters::default());

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
                    bytes: Bytes::from_static(b"first"),
                },
                admission: Arc::new(GroupedAdmission(AtomicU8::new(GROUPED_CLAIMED))),
                reply: GroupedReply::Callback(first_reply), origin: ViewOrigin::new(), recorded: false,
            },
            GroupedRequest {
                change: GroupedChange::CreateFile {
                    path, metadata: Box::new(metadata()), file_id: FileId::new(),
                    bytes: Bytes::from_static(b"second"),
                },
                admission: Arc::new(GroupedAdmission(AtomicU8::new(GROUPED_CLAIMED))),
                reply: GroupedReply::Callback(second_reply), origin: ViewOrigin::new(), recorded: false,
            },
        ];
        let (batch, first, second, work) = source.runtime.block_on(|| async {
            let cancellation = CancellationToken::new();
            let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
            let mut view = super::super::view_ledger::ViewLedgerAdmission::default();
            let mut checkout = source.checkout.lock().await;
            let batch = checkout.apply_grouped_requests_measured(
                requests, &mut work, &mut view, &cancellation, None,
            ).await;
            Ok::<_, MountSourceError>((batch,
                first.await.map_err(|_| MountSourceError::Stale)?,
                second.await.map_err(|_| MountSourceError::Stale)?, work.work))
        })?;
        assert!(matches!(first, Ok(GroupedOutcome::Created(id)) if id == first_id));
        assert!(matches!(second, Err(MountSourceError::AlreadyExists)));
        assert!(matches!(batch, Err(MountSourceError::AlreadyExists)));
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
                    bytes: Bytes::new(),
                },
                admission: Arc::new(GroupedAdmission(AtomicU8::new(GROUPED_CLAIMED))),
                reply: GroupedReply::Callback(reply), origin: ViewOrigin::new(), recorded: false,
            });
        }
        let (batch, first, second, work) = source.runtime.block_on(|| async {
            let cancellation = CancellationToken::new();
            let mut work = crate::demand::DemandWork::new(WorkBudget {
                bytes_encoded: encoded_bytes, ..WorkBudget::UNBOUNDED
            }, &cancellation);
            let mut view = super::super::view_ledger::ViewLedgerAdmission::default();
            let mut checkout = source.checkout.lock().await;
            let batch = checkout.apply_grouped_requests_measured(
                requests, &mut work, &mut view, &cancellation, None,
            ).await;
            Ok::<_, MountSourceError>((batch,
                first.await.map_err(|_| MountSourceError::Stale)?,
                second.await.map_err(|_| MountSourceError::Stale)?, work.work))
        })?;
        assert!(matches!(first, Err(MountSourceError::Engine(_))));
        assert!(matches!(second, Err(MountSourceError::Work(_))));
        assert!(matches!(batch, Err(MountSourceError::Engine(_))));
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
        source.observe_view(weak);
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
                    bytes: Bytes::new(),
                },
                admission: Arc::new(GroupedAdmission(AtomicU8::new(GROUPED_QUEUED))),
                reply: GroupedReply::Callback(reply),
                origin: requester,
                recorded: false,
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
                bytes: Bytes::new(),
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
                    .apply_grouped(abandoned_change, &source.cancellation),
            )
            .await;
            assert!(gave_up.is_err(), "the checkout was held");
            drop(held);
            source
                .checkout
                .apply_grouped(kept_change, &source.cancellation)
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
                bytes: Bytes::new(),
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
                    .apply_grouped(abandoned_change, &source.cancellation),
            );
            assert!(futures::poll!(drainer.as_mut()).is_pending());
            let checkout = Arc::clone(&source.checkout);
            let cancellation = source.cancellation.clone();
            let waiter =
                tokio::spawn(
                    async move { checkout.apply_grouped(kept_change, &cancellation).await },
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
                checkout
                    .publish_after_mutation(ViewChange::Everything, &source.cancellation)
                    .await
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
) -> Result<Arc<crate::demand::NativeDemandReadMeter>, OperationFailure<MountSourceError>> {
    let mut work = crate::demand::DemandWork::new(budget, cancellation);
    work.allocate(std::mem::size_of::<crate::demand::NativeDemandReadMeter>() as u64)
        .map_err(|error| OperationFailure::new(super::lazy::mount_demand_error(error), work.work))?;
    Ok(Arc::new(crate::demand::NativeDemandReadMeter(StdMutex::new(work))))
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

fn capture_name_layout(
    bytes: &[u8],
    profile: FilesystemProfile,
    limits: VolumeLimits,
    work: &mut crate::demand::DemandWork,
) -> Result<(NameEncoding, usize), MountSourceError> {
    use super::lazy::{mount_demand_error, mount_items};
    work.charge(crate::WorkCounters {
        source_path_components: 1, items_examined: 1, ..crate::WorkCounters::UNCHARGED
    }).map_err(mount_demand_error)?;
    match (profile, std::env::consts::OS) {
        (FilesystemProfile::Posix, "linux" | "macos") => Ok((NameEncoding::PosixBytes, bytes.len())),
        (FilesystemProfile::Windows, "windows") => Ok((NameEncoding::WindowsUtf16Le, bytes.len())),
        (FilesystemProfile::Portable | FilesystemProfile::Browser, "linux" | "macos") => {
            let value = std::str::from_utf8(bytes).map_err(|_| MountSourceError::Unsupported(
                "portable volume cannot represent a non-UTF-8 native name".to_owned(),
            ))?;
            let length = LogicalName::validate_unicode(NameEncoding::Utf8, value, limits.maximum_component_bytes)
                .map_err(engine_error)?;
            Ok((NameEncoding::Utf8, length))
        }
        (FilesystemProfile::Portable | FilesystemProfile::Browser, "windows") => {
            let units = crate::kernel::types::utf16le_units(bytes).ok_or_else(|| MountSourceError::Invalid(
                "native UTF-16 name has an odd byte length".to_owned(),
            ))?;
            let mut length = 0_usize;
            for value in char::decode_utf16(units) {
                mount_items(work, 1)?;
                let value = value.map_err(|_| MountSourceError::Unsupported(
                    "portable volume cannot represent an unpaired UTF-16 native name".to_owned(),
                ))?;
                length = length.checked_add(value.len_utf8())
                    .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            }
            Ok((NameEncoding::Utf8, length))
        }
        _ => Err(MountSourceError::Unsupported(
            "volume name profile is incompatible with this native mount".to_owned(),
        )),
    }
}

fn capture_copy_name(
    encoding: NameEncoding,
    bytes: &[u8],
    limits: VolumeLimits,
    work: &mut crate::demand::DemandWork,
) -> Result<LogicalName, MountSourceError> {
    use super::lazy::mount_demand_error;
    let mut scratch = CaptureScratchAllocation::new(work);
    let delta = crate::WorkCounters { bytes_copied: bytes.len() as u64, ..crate::WorkCounters::UNCHARGED };
    scratch.work.work.admit(&delta, &scratch.work.budget).map_err(MountSourceError::Work)?;
    let mut owned = scratch.vec(bytes.len())?;
    owned.extend_from_slice(bytes);
    scratch.work.record(delta).map_err(mount_demand_error)?;
    let name = LogicalName::new(encoding, owned, limits.maximum_component_bytes).map_err(engine_error)?;
    scratch.retain();
    Ok(name)
}

fn capture_native_name(
    bytes: &[u8],
    encoding: NameEncoding,
    length: usize,
    profile: FilesystemProfile,
    limits: VolumeLimits,
    work: &mut crate::demand::DemandWork,
) -> Result<LogicalName, MountSourceError> {
    use super::lazy::{mount_demand_error, mount_items};
    if cfg!(windows) && matches!(profile, FilesystemProfile::Portable | FilesystemProfile::Browser) {
        let mut scratch = CaptureScratchAllocation::new(work);
        scratch.work.work.admit(&crate::WorkCounters { bytes_encoded: length as u64, ..crate::WorkCounters::UNCHARGED }, &scratch.work.budget)
            .map_err(MountSourceError::Work)?;
        let mut owned = scratch.vec(length)?;
        let units = crate::kernel::types::utf16le_units(bytes).ok_or(MountSourceError::Stale)?;
        for value in char::decode_utf16(units) {
            mount_items(scratch.work, 1)?;
            let value = value.map_err(|_| MountSourceError::Stale)?;
            let mut buffer = [0_u8; 4];
            let encoded = value.encode_utf8(&mut buffer);
            owned.extend_from_slice(encoded.as_bytes());
            scratch.work.record(crate::WorkCounters { bytes_encoded: encoded.len() as u64, ..crate::WorkCounters::UNCHARGED })
                .map_err(mount_demand_error)?;
        }
        let name = LogicalName::new(encoding, owned, limits.maximum_component_bytes).map_err(engine_error)?;
        scratch.retain();
        Ok(name)
    } else {
        capture_copy_name(encoding, bytes, limits, work)
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
        crate::CaptureError::Cancelled => MountSourceError::Cancelled,
        crate::CaptureError::StaleVersion => MountSourceError::Stale,
        error => engine_error(error),
    }
}
