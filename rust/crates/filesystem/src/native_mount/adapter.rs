//! One native callback adapter for every embedded checkout consumer.

use super::view_gate::{ViewGate, ViewReadLease, ViewWriteLease};
use super::view_ledger::{ViewChange, ViewLedger, ViewStamp};
use super::{
    CaptureOptions, MountAttributePage, MountAttributeWriteMode, MountDirectoryEntry,
    MountDirectoryPage, MountFilesystem, MountLookup, MountNode, MountNodeKind, MountOpenFile,
    MountPath, MountPublication, MountRangeAllocation, MountSeekTarget, MountSourceError,
    MountViewLease, NativeMountError, capture_root_identity, capture_subtree, seal_checkout,
};
use crate::kernel::{
    AttributeClass, AttributeName, ExtentSeekTarget, FileKind, FileMetadata, FilePayload,
    FileRecord, GenerationMutationError, LogicalName, NameEncoding, NamespacePath, RebaseDecision,
};
use crate::model::{FilesystemProfile, VolumeConfig, VolumeLimits};
use crate::native_capture::{
    MAX_NATIVE_EXACT_CAPTURE_PATHS, NativeViewBaseline, capture_paths_batched_with_baseline,
    capture_subtrees_with_policy_and_baseline,
};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, AuthoredMutation, ByteRange, CancellationToken,
    Checkout, ContentChange, ContentTimes, DetachedFile, FileId, FsError, NamedAttributeWriteMode,
    NativeRootIdentity, ObjectId, OperationFailure, OperationId, VolumeId, WorkBudget,
};
use bytes::Bytes;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::OnceLock;
use std::sync::Weak;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::ThreadId;
use std::time::Duration;

const CALLBACK_TIMEOUT: Duration = Duration::from_secs(30);
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
}

struct CancelCaptureOnDrop(Arc<CaptureCommitGate>);

#[derive(Default)]
struct CaptureSourceGate {
    cancelled: bool,
    active: Vec<Weak<CaptureCommitGate>>,
}

struct HostCapturePolicy<'a> {
    baseline: Option<&'a NativeViewBaseline>,
    budget: WorkBudget,
    request: Option<&'a CaptureCommitGate>,
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

/// Blocking native callback adapter over one async canonical checkout.
pub struct CheckoutMountSource<A, O> {
    checkout: Arc<SharedCheckout<A, O>>,
    root: NamespacePath,
    limits: VolumeLimits,
    profile: FilesystemProfile,
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

static CALLBACK_RUNTIME: OnceLock<Result<tokio::runtime::Runtime, String>> = OnceLock::new();

/// Wakes one callback thread parked on its future. The flag keeps a wake
/// observable even when code polled on that thread consumes its park token.
struct ParkedCallback {
    thread: std::thread::Thread,
    woken: AtomicBool,
}

impl Wake for ParkedCallback {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.store(true, Ordering::Release);
        self.thread.unpark();
    }
}

impl ParkedCallback {
    fn poll_to_completion<F: Future>(mut future: Pin<Box<F>>) -> F::Output {
        let parked = Arc::new(Self {
            thread: std::thread::current(),
            woken: AtomicBool::new(false),
        });
        let waker = Waker::from(Arc::clone(&parked));
        let mut context = Context::from_waker(&waker);
        loop {
            if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
                return output;
            }
            while !parked.woken.swap(false, Ordering::Acquire) {
                std::thread::park();
            }
        }
    }
}

impl CallbackRuntime {
    pub(super) fn create() -> Result<Self, NativeMountError> {
        let runtime = CALLBACK_RUNTIME.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .thread_name("acyclic-fs-mount")
                .build()
                .map_err(|error| error.to_string())
        });
        match runtime {
            Ok(runtime) => Ok(Self {
                handle: runtime.handle().clone(),
            }),
            Err(error) => Err(NativeMountError::Driver(error.clone())),
        }
    }

    fn block_on<F: Future>(&self, create: impl FnOnce() -> F) -> F::Output {
        let future = Box::pin(async { create().await });
        let poll = || {
            let _runtime = self.handle.enter();
            ParkedCallback::poll_to_completion(future)
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
        self.block_on(|| async {
            tokio::time::timeout(CALLBACK_TIMEOUT, create())
                .await
                .map_err(|_| MountSourceError::Stale)?
        })
    }
}

struct DetachedMountState<A, O> {
    file: DetachedFile<A, O>,
    metadata: FileMetadata,
    mutation_epoch: u64,
}

impl<A, O> DetachedMountState<A, O> {
    /// Stamps a content change's times, as every native write does, then
    /// records the change.
    async fn record_content_change(
        &mut self,
        ledger: &ViewLedger,
        cancellation: &CancellationToken,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        if self.metadata.stamp_content_change(content_change_time()?) {
            self.file
                .set_attributes(self.metadata, None, boundary_budget(), cancellation)
                .await
                .map_err(engine_error)?;
        }
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
}

/// Exclusive access to a shared checkout. The retained view lease prevents a
/// native callback from observing a partial external SDK operation.
///
/// A change to the checkout's view that no exact [`ViewChange`] accounts for
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
    pub fn with_publication(checkout: Checkout<A, O>, publication: MountPublication) -> Self {
        let revision = Arc::new(AtomicU64::new(1));
        let ledger = Arc::new(ViewLedger::new());
        Self {
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
            view_gate: Arc::new(ViewGate::new()),
        }
    }

    /// Serializes external checkout access and excludes native callbacks.
    pub async fn lock(&self) -> SharedCheckoutGuard<'_, A, O> {
        let view = self.view_gate.write().await;
        let state = self.state.write().await;
        SharedCheckoutGuard { _view: view, state }
    }

    /// Admits one callback to the current view alongside every other reader.
    pub(super) async fn observe(
        &self,
        owner: ThreadId,
    ) -> Result<SharedCheckoutObservation<'_, A, O>, MountSourceError> {
        let view = self.view_gate.read_for_callback(owner, None).await?;
        let state = self.state.read().await;
        Ok(SharedCheckoutObservation { _view: view, state })
    }

    /// Whether a native durability request (`fsync`, or a close that
    /// publishes) publishes; [`MountPublication::Manual`] leaves every
    /// publication to an explicit sync or unmount.
    pub(super) async fn publishes_at_native_boundary(&self) -> bool {
        self.state.read().await.publishes_at_native_boundary()
    }

    /// Copies the current candidate and the revision it belongs to, for an
    /// optimistic transaction that installs only if that revision still holds.
    /// No reader is excluded: the copy records nothing into this checkout.
    pub(super) async fn candidate(&self) -> Result<(Checkout<A, O>, u64), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let state = self.state.read().await;
        state.ensure_publication_resolved()?;
        Ok((state.private_candidate(), state.revision()))
    }

    /// Advances with every change to the checkout; optimistic transactions
    /// install only over the revision they began from.
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
        cancellation: &CancellationToken,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let operation_id = self.retained_operation_id();
        let result = if force {
            super::publication::seal_checkout_with_permit_force(
                &mut self.checkout,
                operation_id,
                permit,
                boundary_budget(),
                cancellation,
            )
            .await
        } else {
            super::seal_checkout_with_permit(
                &mut self.checkout,
                operation_id,
                permit,
                boundary_budget(),
                cancellation,
            )
            .await
        };
        if result.is_ok()
            || (matches!(result, Err(MountSourceError::Stale))
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
        if self.publication == MountPublication::PerMutation {
            self.seal(cancellation).await?;
        }
        Ok(())
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

    pub(super) fn revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
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

    /// Installs a candidate prepared from a copy of this checkout. The caller
    /// records its change; one it leaves unrecorded invalidates everything
    /// when the guard drops.
    pub(super) fn install_candidate(&mut self, candidate: Checkout<A, O>) {
        self.checkout = candidate;
    }

    /// Records one change to the checkout's view. The exclusive guard keeps
    /// every lookup from overlapping the change it records.
    pub(super) fn record(&mut self, change: &ViewChange<'_>) {
        self.ledger.record(change);
        self.revision.fetch_add(1, Ordering::AcqRel);
        self.recorded_view = self.checkout.root().file_table;
    }
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

    pub(super) fn snapshot(&self) -> Result<(FileRecord, FileMetadata, u64), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync,
        O: AsyncObjectStore + Send + Sync,
    {
        self.runtime.wait(|| async {
            let state = self.state.lock().await;
            Ok((state.file.record(), state.metadata, state.mutation_epoch))
        })
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
        self.runtime.wait(|| async {
            let mut state = self.state.lock().await;
            state
                .file
                .write_range(offset, bytes, boundary_budget(), &self.cancellation)
                .await
                .map(|_| ())
                .map_err(engine_error)?;
            state
                .record_content_change(&self.ledger, &self.cancellation)
                .await
        })
    }

    fn resize(&self, logical_bytes: u64) -> Result<(), MountSourceError> {
        self.runtime.wait(|| async {
            let mut state = self.state.lock().await;
            state
                .file
                .resize(logical_bytes, boundary_budget(), &self.cancellation)
                .await
                .map(|_| ())
                .map_err(engine_error)?;
            state
                .record_content_change(&self.ledger, &self.cancellation)
                .await
        })
    }

    fn allocate_range(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        self.runtime.wait(|| async {
            let mut state = self.state.lock().await;
            let range = ByteRange { offset, length };
            match operation {
                MountRangeAllocation::PunchHole => {
                    state
                        .file
                        .zero_range(range, false, false, boundary_budget(), &self.cancellation)
                        .await
                }
                MountRangeAllocation::ZeroRange { extend } => {
                    state
                        .file
                        .zero_range(range, true, extend, boundary_budget(), &self.cancellation)
                        .await
                }
                MountRangeAllocation::Preallocate { keep_size } => {
                    state
                        .file
                        .preallocate(range, keep_size, boundary_budget(), &self.cancellation)
                        .await
                }
            }
            .map(|_| ())
            .map_err(engine_error)?;
            state
                .record_content_change(&self.ledger, &self.cancellation)
                .await
        })
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
        self.runtime.wait(|| async {
            let mut checkout = self.checkout.lock().await;
            checkout.ensure_publication_resolved()?;
            checkout
                .change_content_by_id(self.file_id, change, &self.cancellation)
                .await
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

impl<A, O> CheckoutMountSource<A, O> {
    pub(super) fn shared_checkout(&self) -> &SharedCheckout<A, O> {
        &self.checkout
    }

    pub(super) fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// The checkout's current generation, which publication advances.
    #[cfg(all(test, target_os = "macos"))]
    pub(super) fn generation_id(&self) -> Result<crate::GenerationId, MountSourceError>
    where
        A: Send + Sync,
        O: Send + Sync,
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

    /// Resolves one mounted path within a caller's callback future, so a
    /// composed source spends one runtime entry on its whole callback.
    pub(super) async fn lookup_async(
        &self,
        path: &MountPath,
        owner: ThreadId,
    ) -> Result<Option<MountLookup>, MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let path = self.path(path)?;
        let observation = self.checkout.observe(owner).await?;
        let mut checkout = observation.observer();
        let receipt = checkout
            .lookup_no_follow_with_metadata(&path, boundary_budget(), &self.cancellation)
            .await
            .map_err(engine_error)?;
        Ok(receipt.value.map(|value| MountLookup {
            node: mount_node(value.record),
            metadata: value.metadata,
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
        let runtime = CallbackRuntime::create()?;
        Ok(Self {
            checkout,
            root,
            limits: config.limits,
            profile: config.profile,
            cancellation: CancellationToken::new(),
            capture_gate: StdMutex::new(CaptureSourceGate::default()),
            runtime,
        })
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
            .wait(|| async { Ok(self.checkout.observe(owner).await?.volume_id()) })
    }

    /// Cancels future and in-flight canonical operations owned by this mount.
    pub fn cancel(&self) {
        let requests = {
            let mut gate = self
                .capture_gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            gate.cancelled = true;
            let requests = gate
                .active
                .iter()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>();
            for request in &requests {
                let _ = request.state.compare_exchange(
                    CAPTURE_PENDING,
                    CAPTURE_CANCELLED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
            }
            gate.active.clear();
            requests
        };
        self.cancellation.cancel();
        for request in requests {
            request.cancellation.cancel();
        }
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
        let mut checkout = self.checkout.lock().await;
        checkout
            .seal_with_permit(permit, false, &self.cancellation)
            .await
    }

    pub(super) async fn sync_async_with_permit_force(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let mut checkout = self.checkout.lock().await;
        checkout
            .seal_with_permit(permit, true, &self.cancellation)
            .await
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
        let mut checkout = self.checkout.lock().await;
        checkout.ensure_publication_resolved()?;
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
                self.checkout.view_gate.begin_transition();
                checkout.record(&ViewChange::Everything);
                self.checkout.view_gate.finish_transition();
                Ok(())
            }
            RebaseDecision::Conflicted { .. } => Err(MountSourceError::Stale),
        }
    }

    /// Adopts the workspace head after this checkout's mutations were
    /// synchronized and a separate fenced operation published that head.
    pub async fn advance_to_head_async(&self) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let mut checkout = self.checkout.lock().await;
        checkout.ensure_publication_resolved()?;
        checkout
            .refresh_head(WorkBudget::UNBOUNDED, &self.cancellation)
            .await
            .map_err(engine_error)?;
        self.checkout.view_gate.begin_transition();
        checkout.record(&ViewChange::Everything);
        self.checkout.view_gate.finish_transition();
        Ok(())
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
                if !bytes.len().is_multiple_of(2) {
                    return Err(MountSourceError::Invalid(
                        "native UTF-16 name has an odd byte length".to_owned(),
                    ));
                }
                let units = bytes
                    .chunks_exact(2)
                    .map(|unit| {
                        let bytes: [u8; 2] = unit.try_into().unwrap_or([0, 0]);
                        u16::from_le_bytes(bytes)
                    })
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
        self.runtime.wait(|| {
            self.capture_host_with_identity_async(
                source_root,
                paths,
                expected_root_identity,
                scope,
                HostCapturePolicy {
                    baseline,
                    budget: boundary_budget(),
                    request: None,
                },
            )
        })
    }

    /// The SDK lifecycle path uses the same capture transaction as callbacks,
    /// but its admitted budget must not inherit the callback deadline or cap.
    pub(super) async fn capture_host_with_identity_budgeted(
        self: &Arc<Self>,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: NativeRootIdentity,
        scope: HostCaptureScope,
        baseline: Arc<NativeViewBaseline>,
        budget: WorkBudget,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let source = Arc::clone(self);
        let source_root = source_root.to_path_buf();
        let paths = paths.to_vec();
        let runtime = tokio::runtime::Handle::current();
        let request = Arc::new(CaptureCommitGate {
            cancellation: CancellationToken::new(),
            state: AtomicU8::new(CAPTURE_PENDING),
        });
        {
            let mut gate = self
                .capture_gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if gate.cancelled {
                return Err(MountSourceError::Stale);
            }
            self.cancellation
                .check()
                .map_err(|error| MountSourceError::Engine(error.to_string()))?;
            gate.active.retain(|request| request.strong_count() > 0);
            gate.active.push(Arc::downgrade(&request));
        }
        let _cancel_on_drop = CancelCaptureOnDrop(Arc::clone(&request));
        tokio::task::spawn_blocking(move || {
            runtime.block_on(source.capture_host_with_identity_async(
                &source_root,
                &paths,
                expected_root_identity,
                scope,
                HostCapturePolicy {
                    baseline: Some(&baseline),
                    budget,
                    request: Some(&request),
                },
            ))
        })
        .await
        .map_err(|error| MountSourceError::Engine(error.to_string()))?
    }

    async fn capture_host_with_identity_async(
        &self,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: NativeRootIdentity,
        scope: HostCaptureScope,
        policy: HostCapturePolicy<'_>,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let cancellation = policy
            .request
            .map_or(&self.cancellation, |request| &request.cancellation);
        cancellation
            .check()
            .map_err(|error| MountSourceError::Engine(error.to_string()))?;
        self.cancellation
            .check()
            .map_err(|error| MountSourceError::Engine(error.to_string()))?;
        if paths.is_empty() {
            return Ok(());
        }
        if matches!(scope, HostCaptureScope::ExactPaths)
            && paths.len() > MAX_NATIVE_EXACT_CAPTURE_PATHS
        {
            return Err(MountSourceError::Invalid(
                "capture path set exceeds the native per-operation limit".to_owned(),
            ));
        }
        let paths = paths
            .iter()
            .map(|path| self.path(path))
            .collect::<Result<Vec<_>, _>>()?;
        let maximum_paths = match scope {
            HostCaptureScope::ExactPaths => u32::try_from(paths.len())
                .map_err(|_| MountSourceError::Invalid("too many capture paths".to_owned()))?,
            HostCaptureScope::Subtree => MAX_NATIVE_SUBTREE_CAPTURE_PATHS,
        };
        {
            let options = CaptureOptions {
                source_root: source_root.to_path_buf(),
                expected_root_identity,
                maximum_paths,
                maximum_extent_spans: 65_536,
            };
            for _ in 0..3 {
                let (mut candidate, revision) = self.checkout.candidate().await?;
                // Projected host reads may call back into this checkout. Never
                // hold its view gate while observing the virtualization root.
                match scope {
                    HostCaptureScope::ExactPaths => {
                        capture_paths_batched_with_baseline(
                            &mut candidate,
                            &paths,
                            &options,
                            64,
                            policy.budget,
                            cancellation,
                            policy.baseline,
                        )
                        .await
                        .map_err(engine_error)?;
                    }
                    HostCaptureScope::Subtree => {
                        let root = paths.first().cloned().ok_or_else(|| {
                            MountSourceError::Invalid("missing capture subtree".to_owned())
                        })?;
                        capture_subtrees_with_policy_and_baseline(
                            &mut candidate,
                            &[root],
                            &options,
                            &crate::CapturePolicy::allow_all(),
                            policy.budget,
                            cancellation,
                            policy.baseline,
                        )
                        .await
                        .map_err(engine_error)?;
                    }
                }
                if self
                    .commit_host_capture(candidate, revision, policy.request)
                    .await?
                {
                    return Ok(());
                }
            }
            Err(MountSourceError::Stale)
        }
    }

    async fn commit_host_capture(
        &self,
        candidate: Checkout<A, O>,
        revision: u64,
        request: Option<&CaptureCommitGate>,
    ) -> Result<bool, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let mut checkout = self.checkout.lock().await;
        checkout.ensure_publication_resolved()?;
        if checkout.revision() != revision {
            return Ok(false);
        }
        {
            let source_gate = self
                .capture_gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if source_gate.cancelled {
                return Err(MountSourceError::Stale);
            }
            self.cancellation
                .check()
                .map_err(|error| MountSourceError::Engine(error.to_string()))?;
            if let Some(request) = request {
                request
                    .cancellation
                    .check()
                    .map_err(|error| MountSourceError::Engine(error.to_string()))?;
                if checkout.publishes_at_native_boundary() {
                    return Err(MountSourceError::Invalid(
                        "lifecycle capture requires manual publication".to_owned(),
                    ));
                }
                request
                    .state
                    .compare_exchange(
                        CAPTURE_PENDING,
                        CAPTURE_COMMITTED,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .map_err(|_| MountSourceError::Stale)?;
                checkout.install_candidate(candidate);
                checkout.record(&ViewChange::Everything);
                return Ok(true);
            }
            checkout.install_candidate(candidate);
            checkout.record(&ViewChange::Everything);
        }
        checkout
            .publish_at_native_boundary(&self.cancellation)
            .await?;
        Ok(true)
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
}

impl<A, O> MountFilesystem for CheckoutMountSource<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn supports_posix_named_attributes(&self) -> bool {
        self.profile == FilesystemProfile::Posix
    }

    fn view_stamp(&self) -> Option<ViewStamp> {
        self.checkout
            .view_gate
            .is_stable()
            .then(|| self.checkout.ledger.stamp())
    }

    fn unchanged_since(&self, path: &MountPath, file_id: Option<FileId>, stamp: ViewStamp) -> bool {
        self.path(path)
            .is_ok_and(|path| self.checkout.unchanged_since(&path, file_id, stamp))
    }

    fn binding_epoch(&self) -> Option<u64> {
        Some(self.checkout.view_gate.generation())
    }

    fn view_is_stable(&self) -> bool {
        self.checkout.view_gate.is_stable()
    }

    fn acquire_view_lease(&self) -> Result<Box<dyn MountViewLease>, MountSourceError> {
        let owner = ViewGate::callback_owner();
        let lease = self.runtime.wait(|| {
            let gate = Arc::clone(&self.checkout.view_gate);
            async move { gate.read_for_callback(owner, None).await }
        })?;
        Ok(Box::new(lease))
    }

    fn acquire_binding_lease(
        &self,
        expected_epoch: Option<u64>,
    ) -> Result<Box<dyn MountViewLease>, MountSourceError> {
        let owner = ViewGate::callback_owner();
        let lease = self.runtime.wait(|| {
            let gate = Arc::clone(&self.checkout.view_gate);
            async move { gate.read_for_callback(owner, expected_epoch).await }
        })?;
        Ok(Box::new(lease))
    }

    fn lookup(&self, path: &MountPath) -> Result<Option<MountLookup>, MountSourceError> {
        let owner = ViewGate::callback_owner();
        self.runtime.wait(|| self.lookup_async(path, owner))
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

    fn create_file(
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
                    vec![AuthoredMutation::CreateFile {
                        path: path.clone(),
                        bytes: Bytes::new(),
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
                    kind: MountNodeKind::Regular,
                    logical_bytes: 0,
                    link_count: 1,
                    device: None,
                },
                metadata,
            })
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

    fn capture_host_subtree(
        &self,
        source_root: &Path,
        path: &MountPath,
    ) -> Result<(), MountSourceError> {
        let path = self.path(path)?;
        self.runtime.wait(|| async {
            let expected_root_identity =
                capture_root_identity(source_root).map_err(engine_error)?;
            let options = CaptureOptions {
                source_root: source_root.to_path_buf(),
                expected_root_identity,
                maximum_paths: MAX_NATIVE_SUBTREE_CAPTURE_PATHS,
                maximum_extent_spans: 65_536,
            };
            for _ in 0..3 {
                let (mut candidate, revision) = self.checkout.candidate().await?;
                capture_subtree(
                    &mut candidate,
                    path.clone(),
                    &options,
                    boundary_budget(),
                    &self.cancellation,
                )
                .await
                .map_err(engine_error)?;
                let mut checkout = self.checkout.lock().await;
                checkout.ensure_publication_resolved()?;
                if checkout.revision() != revision {
                    continue;
                }
                checkout.install_candidate(candidate);
                checkout.record(&ViewChange::Everything);
                return checkout
                    .publish_at_native_boundary(&self.cancellation)
                    .await;
            }
            Err(MountSourceError::Stale)
        })
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
    match error.error {
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

    #[cfg(target_os = "macos")]
    #[allow(unsafe_code)]
    unsafe extern "C" {
        fn nfs4_test_exclusive_replay_identity() -> std::ffi::c_int;
        fn nfs4_test_namedattr_exclusive_replay_identity() -> std::ffi::c_int;
        fn nfs4_test_readdir_cookie_verifier() -> std::ffi::c_int;
        fn nfs4_test_sync_acknowledgement() -> std::ffi::c_int;
    }

    #[cfg(target_os = "macos")]
    #[test]
    #[allow(unsafe_code)]
    fn exclusive_replay_is_bound_to_the_opened_file_handle() {
        // SAFETY: the test hook has no arguments and owns all callback state.
        assert_eq!(unsafe { nfs4_test_exclusive_replay_identity() }, 0);
    }

    #[cfg(target_os = "macos")]
    #[test]
    #[allow(unsafe_code)]
    fn named_attribute_exclusive_replay_is_bound_to_owner_and_name() {
        // SAFETY: the test hook has no arguments and owns all callback state.
        assert_eq!(
            unsafe { nfs4_test_namedattr_exclusive_replay_identity() },
            0
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    #[allow(unsafe_code)]
    fn readdir_continuations_are_bound_to_the_namespace_revision() {
        // SAFETY: the test hook has no arguments and mutates no shared state.
        assert_eq!(unsafe { nfs4_test_readdir_cookie_verifier() }, 0);
    }

    #[cfg(target_os = "macos")]
    #[test]
    #[allow(unsafe_code)]
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

    type MemorySource = CheckoutMountSource<
        crate::facade::MemoryAuthorityBackend,
        crate::facade::MemoryObjectBackend,
    >;

    #[test]
    fn cancelled_lifecycle_capture_cannot_commit_late() -> Result<(), Box<dyn std::error::Error>> {
        let (source, _) =
            shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
        let revision = source.checkout.revision();
        let candidate = source
            .runtime
            .block_on(|| async { source.checkout.lock().await.private_candidate() });
        let request = Arc::new(CaptureCommitGate {
            cancellation: CancellationToken::new(),
            state: AtomicU8::new(CAPTURE_PENDING),
        });
        drop(CancelCaptureOnDrop(Arc::clone(&request)));
        let result = source
            .runtime
            .block_on(|| source.commit_host_capture(candidate, revision, Some(&request)));
        assert!(result.is_err());
        assert_eq!(source.checkout.revision(), revision);
        let request = Arc::new(CaptureCommitGate {
            cancellation: CancellationToken::new(),
            state: AtomicU8::new(CAPTURE_PENDING),
        });
        source
            .capture_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active
            .push(Arc::downgrade(&request));
        source.cancel();
        assert!(request.cancellation.is_cancelled());
        let candidate = source
            .runtime
            .block_on(|| async { source.checkout.lock().await.private_candidate() });
        let result = source
            .runtime
            .block_on(|| source.commit_host_capture(candidate, revision, Some(&request)));
        assert!(result.is_err());
        assert_eq!(source.checkout.revision(), revision);
        Ok(())
    }

    #[test]
    fn dropping_active_capture_fences_its_detached_worker() -> Result<(), Box<dyn std::error::Error>>
    {
        let (source, _) =
            shared_sources_with_publication(FilesystemProfile::Portable, MountPublication::Manual)?;
        let source = Arc::new(source);
        let directory = tempfile::tempdir()?;
        std::fs::write(directory.path().join("file"), b"child")?;
        let identity = capture_root_identity(directory.path())?;
        #[cfg(unix)]
        let baseline = Arc::new(NativeViewBaseline::new(identity));
        #[cfg(windows)]
        let baseline = Arc::new(NativeViewBaseline);
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
                        WorkBudget::UNBOUNDED,
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

    #[test]
    fn direct_checkout_view_lease_excludes_concurrent_mutation()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = Arc::new(source(FilesystemProfile::Portable)?);
        let lease = source.acquire_view_lease()?;
        let writer = Arc::clone(&source);
        let (completed, receive) = std::sync::mpsc::sync_channel(1);
        let task = std::thread::spawn(move || {
            let result =
                writer.create_file(&MountPath::root().child(b"leased".to_vec()), metadata());
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
    fn a_rebind_invalidates_every_lookup() -> Result<(), Box<dyn std::error::Error>> {
        let (reader, writer) = tracking_sources(FilesystemProfile::Portable)?;
        let (published, untouched) = (native_test_path("published"), native_test_path("untouched"));
        writer.create_file(&published, metadata())?;
        writer.sync()?;
        let stamp = reader.view_stamp().ok_or("checkout has no view stamp")?;
        assert!(reader.unchanged_since(&untouched, None, stamp));
        reader.runtime.block_on(|| reader.refresh_async())?;
        assert!(!reader.unchanged_since(&untouched, None, stamp));
        assert!(!reader.unchanged_since(&MountPath::root(), None, stamp));
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
        let write_status = std::process::Command::new("powershell.exe")
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "[IO.File]::WriteAllBytes($env:ACYCLIC_FS_TEST_PATH, [Text.Encoding]::UTF8.GetBytes('projected'))",
            ])
            .env("ACYCLIC_FS_TEST_PATH", destination.join(first_name))
            .status()?;
        assert!(write_status.success());
        // Captures of external operations are deferred until callbacks flush.
        session.flush_callbacks()?;
        let first = windows_path(first_name);
        assert_eq!(source.read_range(&first, 0, 9)?.as_ref(), b"projected");

        let rename_status = std::process::Command::new("powershell.exe")
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "[IO.File]::Move($env:ACYCLIC_FS_TEST_SOURCE, $env:ACYCLIC_FS_TEST_DESTINATION)",
            ])
            .env("ACYCLIC_FS_TEST_SOURCE", destination.join(first_name))
            .env("ACYCLIC_FS_TEST_DESTINATION", destination.join(second_name))
            .status()?;
        assert!(rename_status.success());
        session.flush_callbacks()?;
        let second = windows_path(second_name);
        assert_eq!(source.lookup(&first)?, None);
        assert!(source.lookup(&second)?.is_some());

        let link_status = std::process::Command::new("powershell.exe")
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "New-Item -ItemType HardLink -Path $env:ACYCLIC_FS_TEST_DESTINATION -Target $env:ACYCLIC_FS_TEST_SOURCE | Out-Null",
            ])
            .env("ACYCLIC_FS_TEST_SOURCE", destination.join(second_name))
            .env("ACYCLIC_FS_TEST_DESTINATION", destination.join(linked_name))
            .status()?;
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

        let delete_status = std::process::Command::new("powershell.exe")
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "[IO.File]::Delete($env:ACYCLIC_FS_TEST_PATH)",
            ])
            .env("ACYCLIC_FS_TEST_PATH", destination.join(second_name))
            .status()?;
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
        let output = std::process::Command::new("rustc")
            .current_dir(temporary.path())
            .args(["--edition", "2021", "main.rs", "-o", "main"])
            .output()?;
        assert!(
            output.status.success(),
            "rustc failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(temporary.path().join("main").is_file());
        assert!(
            std::process::Command::new(temporary.path().join("main"))
                .status()?
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
        let mut child_a = std::process::Command::new(&executable)
            .args(["--ignored", "--exact", worker, "--test-threads=1"])
            .env("ACYCLIC_FS_UNIX_CHILD_MOUNT", &mount_a)
            .env("ACYCLIC_FS_UNIX_CHILD_READY", &ready_a)
            .env("ACYCLIC_FS_UNIX_CHILD_RELEASE", &release)
            .env("ACYCLIC_FS_UNIX_CHILD_ID", "a")
            .spawn()?;
        let mut child_b = std::process::Command::new(&executable)
            .args(["--ignored", "--exact", worker, "--test-threads=1"])
            .env("ACYCLIC_FS_UNIX_CHILD_MOUNT", &mount_b)
            .env("ACYCLIC_FS_UNIX_CHILD_READY", &ready_b)
            .env("ACYCLIC_FS_UNIX_CHILD_RELEASE", &release)
            .env("ACYCLIC_FS_UNIX_CHILD_ID", "b")
            .spawn()?;

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
        child: &mut std::process::Child,
        timeout: std::time::Duration,
    ) -> Result<std::process::ExitStatus, Box<dyn std::error::Error>> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if let Some(status) = child.try_wait()? {
                return Ok(status);
            }
            if std::time::Instant::now() >= deadline {
                child.kill()?;
                let _ = child.wait();
                return Err("mount child did not terminate within the bounded deadline".into());
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
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
