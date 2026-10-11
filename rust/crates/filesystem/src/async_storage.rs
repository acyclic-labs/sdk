//! Runtime-neutral asynchronous storage contracts for browser and remote use.

use crate::cancellation::CancellationToken;
use crate::foundation::{
    AuthorityId, Epoch, GenerationId, Head, OperationId, ProposedCommit, Sequence,
};
use crate::performance::{WorkBudget, WorkCounters};
use crate::storage::{
    AppendOutcome, AuthorityResult, AuthorityStore, CreateAuthorityOutcome, FenceOutcome,
    GuardedAppend, ObjectId, ObjectRead, ObjectReadRequest, ObjectResult, ObjectStore, ObjectWrite,
    PublicationPermit, PublicationReservation, ReplayLimit, ReservationOutcome,
};
use acyclic_stream::OwnedBytes;
use std::any::{Any, TypeId};
use std::future::Future;
use std::mem::size_of;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

/// An immutable read whose SDK-owned buffers retain the original native journal.
///
/// Known shared backend bytes are borrowed. An owned or opaque returned payload
/// carries its admitted logical custody inside the byte owner, so cloning a
/// view never retires that custody before the final view is destroyed.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub struct NativeObjectRead {
    read: ObjectRead,
}

/// One genuine native batch, including its physically owned result container.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub struct NativeObjectReadBatch {
    reads: Vec<ObjectRead>,
    allocation: crate::materializer::NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeObjectReadBatch {
    pub(crate) fn new(
        reads: Vec<ObjectRead>,
        allocation: crate::materializer::NativeAllocation,
    ) -> Result<Self, crate::storage::ObjectStoreError> {
        let bytes = reads.capacity().checked_mul(size_of::<ObjectRead>())
            .and_then(|bytes| u64::try_from(bytes).ok());
        if bytes != Some(allocation.bytes()) {
            drop(reads);
            drop(allocation);
            return Err(crate::storage::ObjectStoreError::Corrupt);
        }
        Ok(Self { reads, allocation })
    }

    /// Borrows the actual ordered results without another allocation.
    pub fn reads(&self) -> &[ObjectRead] { &self.reads }

    pub(crate) fn reads_mut(&mut self) -> &mut [ObjectRead] { &mut self.reads }

    /// Moves the container and its original guard; destroy the vector first.
    pub fn into_parts(self) -> (Vec<ObjectRead>, crate::materializer::NativeAllocation) {
        (self.reads, self.allocation)
    }
}


#[cfg(not(target_arch = "wasm32"))]
impl NativeObjectRead {
    pub(crate) fn shared(read: ObjectRead) -> Result<Self, crate::storage::ObjectStoreError> {
        if read.retention != crate::storage::ObjectReadRetention::Shared {
            return Err(crate::storage::ObjectStoreError::Corrupt);
        }
        Ok(Self { read })
    }

    /// Accepts a completed pre-funded owner without adopting or wrapping bytes.
    /// The expected scope is accounting identity, never namespace authority.
    pub(crate) fn intrinsic(
        read: ObjectRead, original: &crate::materializer::NativeMaterializationWork,
    ) -> Result<Self, crate::storage::ObjectStoreError> {
        let valid = match read.retention {
            crate::storage::ObjectReadRetention::Owned { logical_bytes } => {
                read.bytes.retained_capacity().and_then(|capacity| u64::try_from(capacity).ok())
                    == Some(logical_bytes)
                    && logical_bytes >= read.bytes.len() as u64
                    && read.bytes.matches_meter(&original.runtime_meter_owner())
            }
            crate::storage::ObjectReadRetention::Shared => false,
        };
        if !valid { return Err(crate::storage::ObjectStoreError::Corrupt); }
        Ok(Self { read })
    }

    /// Borrows the actual read without taking a second allocation claim.
    pub const fn read(&self) -> &ObjectRead { &self.read }

    pub(crate) fn read_mut(&mut self) -> &mut ObjectRead { &mut self.read }

    /// Moves the same view; its physical byte owner keeps original custody.
    pub fn into_read(self) -> ObjectRead { self.read }
}

/// Opaque SDK observation for one actual object-read receipt.
///
/// Only the SDK can construct an observer. Providers cannot assert that work
/// was observed: the default read boundary records its actual returned prefix.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct ObjectReadObservation<'a> {
    custody: &'a crate::kernel::ProofDataCustody,
}

impl<'a> ObjectReadObservation<'a> {
    pub(crate) fn new(custody: &'a crate::kernel::ProofDataCustody) -> Self {
        Self { custody }
    }

    pub(crate) fn is_for(self, custody: &crate::kernel::ProofDataCustody) -> bool {
        std::ptr::eq(self.custody, custody)
    }

    pub(crate) fn standing_bytes(self) -> u64 {
        self.custody.operation_cache_bytes()
    }

    pub(crate) fn remaining_budget(
        self,
        budget: WorkBudget,
        standing: u64,
    ) -> Result<WorkBudget, crate::storage::ObjectFailure> {
        let mut remaining = budget;
        remaining.peak_allocation_bytes = budget.peak_allocation_bytes.checked_sub(standing)
            .ok_or_else(|| crate::storage::ObjectFailure::before_work(
                crate::storage::ObjectStoreError::Work(crate::WorkError::BudgetExceeded {
                    counter: "peak_allocation_bytes",
                    observed: standing,
                    maximum: budget.peak_allocation_bytes,
                }),
            ))?;
        Ok(remaining)
    }

    pub(crate) fn scope_result(
        result: ObjectResult<ObjectRead>,
        standing: u64,
        budget: WorkBudget,
    ) -> ObjectResult<ObjectRead> {
        if standing == 0 {
            return result;
        }
        match result {
            Ok(mut receipt) => {
                receipt.work = WorkCounters::default().with_backend(receipt.work, standing)
                    .map_err(|error| crate::storage::ObjectFailure::new(error.into(), receipt.work))?;
                receipt.work.verify(budget)
                    .map_err(|error| crate::storage::ObjectFailure::new(error.into(), receipt.work))?;
                Ok(receipt)
            }
            Err(failure) => match WorkCounters::default().with_backend(*failure.work, standing) {
                Ok(work) => Err(crate::storage::ObjectFailure::new(failure.error, work)),
                Err(error) => Err(crate::storage::ObjectFailure::new(error.into(), *failure.work)),
            },
        }
    }

    pub(crate) fn complete(self, result: ObjectResult<ObjectRead>) -> ObjectResult<ObjectRead> {
        let work = match &result {
            Ok(receipt) => receipt.work,
            Err(failure) => *failure.work,
        };
        self.custody.record_completed_leaf_work(work).map_err(|error| {
            crate::storage::ObjectFailure::new(error.into(), work)
        })?;
        result
    }
}

/// Runtime-appropriate bound for storage futures.
///
/// Native service futures must move between executor workers. Browser futures
/// may retain thread-affine JavaScript transaction handles.
#[cfg(not(target_arch = "wasm32"))]
pub trait StorageFuture: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send> StorageFuture for T {}

/// Runtime-appropriate bound for storage futures.
#[cfg(target_arch = "wasm32")]
pub trait StorageFuture {}
#[cfg(target_arch = "wasm32")]
impl<T> StorageFuture for T {}

/// Heap allocated future with the platform's storage executor bound.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) type BoxStorageFuture<'a, T> = std::pin::Pin<Box<dyn Future<Output = T> + Send + 'a>>;
/// Heap allocated future with the platform's storage executor bound.
#[cfg(target_arch = "wasm32")]
pub(crate) type BoxStorageFuture<'a, T> = std::pin::Pin<Box<dyn Future<Output = T> + 'a>>;

/// Runtime-appropriate provider ownership bound.
#[cfg(not(target_arch = "wasm32"))]
pub trait StorageProvider: Send + Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync + ?Sized> StorageProvider for T {}

/// Runtime-appropriate provider ownership bound.
#[cfg(target_arch = "wasm32")]
pub trait StorageProvider {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> StorageProvider for T {}

/// Exact identity of one authenticated decoded-object representation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[doc(hidden)]
pub struct DecodedCacheKey {
    pub(crate) object_id: ObjectId,
    decoder: TypeId,
    limits: crate::kernel::DecodeLimits,
}

impl DecodedCacheKey {
    pub(crate) fn new<T: 'static>(
        object_id: ObjectId,
        limits: crate::kernel::DecodeLimits,
    ) -> Self {
        Self {
            object_id,
            decoder: TypeId::of::<T>(),
            limits,
        }
    }
}

/// Type-erased immutable decoded value retained by a disposable accelerator.
#[derive(Clone)]
#[doc(hidden)]
pub struct DecodedCacheValue {
    pub(crate) value: Arc<dyn Any + Send + Sync>,
    pub(crate) logical_bytes: u64,
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) native_custody: Option<Arc<crate::materializer::NativeAllocation>>,
}

/// Whether a decoded value became shared cache state or remains operation-owned.
#[doc(hidden)]
pub enum DecodedCacheAdmission {
    Uncached(DecodedCacheValue),
    Shared(DecodedCacheValue),
}

/// Polls a future exactly once for adapters whose contract guarantees that no
/// asynchronous suspension is possible.
///
/// This keeps synchronous adapters free of executors while
/// failing closed if an implementation unexpectedly blocks.
pub(crate) fn poll_ready<F: Future>(future: F) -> Option<F::Output> {
    let mut future = std::pin::pin!(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => Some(value),
        Poll::Pending => None,
    }
}

/// Drives an [`ImmediateObjectStore`] adapter exactly once.
///
/// Every such adapter is supplied by this module's blanket implementation and
/// contains no suspension point, so pending is an internal contract violation
/// rather than a recoverable filesystem outcome.
#[allow(
    clippy::expect_used,
    reason = "only crate-owned ImmediateObjectStore adapter futures reach this boundary; they contain no suspension point"
)]
pub(crate) fn poll_immediate<F: Future>(future: F) -> F::Output {
    poll_ready(future).expect("immediate storage adapter suspended")
}

/// Authority lineage semantics for one workspace fork.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GenerationFork {
    /// Reuse the source authority's durable lineage through the selected generation.
    PublishedPrefix,
    /// Start independent authority history from an authenticated unpublished generation.
    Independent,
}

/// Exact source and lineage semantics for one authority fork.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GenerationForkSource {
    /// Source authority containing the selected generation.
    pub authority: AuthorityId,
    /// Exact authenticated source generation.
    pub generation: GenerationId,
    /// Whether durable source lineage is reused.
    pub lineage: GenerationFork,
}

/// A new workspace's complete authority state: its own authority, holding
/// its creation record, and the retention authority that keeps its source
/// generation alive while the workspace may depend on it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceForkCommit {
    /// Source lineage the destination authority starts from, on backends
    /// that keep generation lineage; `None` starts it empty.
    pub lineage: Option<GenerationForkSource>,
    /// The new workspace's authority.
    pub destination: AuthorityId,
    /// Its creation record, the destination's first commit.
    pub creation: ProposedCommit,
    /// The authority retaining the source generation for the new workspace.
    pub retention: AuthorityId,
    /// The retention record, the retention authority's first commit.
    pub retained: ProposedCommit,
}

/// How one workspace fork's authority state resolved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkspaceForkOutcome {
    /// Both authorities hold exactly the requested first records, now or
    /// from an earlier attempt.
    Committed,
    /// The retention authority already holds a different first record.
    RetentionConflict,
    /// The destination authority already holds a different first record.
    CreationRejected,
}

/// Largest first record a fork compares when resolving a retry.
const MAXIMUM_FIRST_RECORD_BYTES: u64 = 4 * 1024;

/// Commits a workspace fork as ordered single-authority steps: the retention
/// record first, so the source generation is retained before any workspace
/// can depend on it, then the destination authority and its creation record.
/// Every step is idempotent, so a retry after a crash at any step, or after
/// a combined commit, completes exactly the missing steps.
///
/// This is the default [`AsyncAuthorityStore::commit_workspace_fork`], for
/// backends that commit what they can atomically and resolve the rest here.
///
/// The caller must retain admitted claims for the actual backing capacities of
/// both input payloads until this entire future completes, including while the
/// retention step runs with the creation payload still held. `budget` is the
/// remaining allowance after reserving those simultaneously live bytes. These
/// opaque `Bytes` inputs are borrowed from that custody: their lengths do not
/// reveal backing capacities, and neither this helper nor its child authority
/// operations may adopt or charge those original allocations again. Backend
/// copies, encodings, hashes, and replay buffers remain backend-owned work.
pub async fn commit_workspace_fork_in_steps<S: AsyncAuthorityStore + ?Sized>(
    store: &S,
    fork: WorkspaceForkCommit,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> AuthorityResult<WorkspaceForkOutcome> {
    authority_projection(commit_workspace_fork_steps_engine(
        store, fork, PublicationPermit::Unrestricted, ForkStepsWork::ordinary(budget, cancellation),
        None::<&mut fn(WorkBudget) -> std::future::Ready<crate::facade::FsResult<()>>>,
        &mut NoNativeForkGuard,
    ).await.map_err(ForkStepsFailure::measured))
}

/// Guarded ordered fork recovery, borrowing the same external input custody as
/// [`commit_workspace_fork_in_steps`]. Destination publications use the backend's
/// guarded hooks so native adapters can seal after their own physical planning.
pub async fn commit_workspace_fork_in_steps_guarded<S, F, Fut>(
    store: &S,
    fork: WorkspaceForkCommit,
    permit: PublicationPermit,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    guard: &mut F,
) -> crate::facade::FsResult<WorkspaceForkOutcome>
where
    S: AsyncAuthorityStore + ?Sized,
    F: FnMut(WorkBudget) -> Fut + Send,
    Fut: Future<Output = crate::facade::FsResult<()>> + Send,
{
    commit_workspace_fork_steps_engine(store, fork, permit, ForkStepsWork::ordinary(budget, cancellation), Some(guard), &mut NoNativeForkGuard)
        .await.map_err(ForkStepsFailure::measured)
}

struct ForkStepsFailure<E = crate::facade::FsError> {
    error: E,
    work: WorkCounters,
}

impl ForkStepsFailure {
    fn measured(self) -> crate::performance::OperationFailure<crate::facade::FsError> {
        crate::performance::OperationFailure::new(self.error, self.work)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native(self) -> crate::storage::AuthorityStoreError {
        use crate::{facade::FsError, storage::AuthorityStoreError};
        match self.error {
            FsError::Authority(error) => error,
            FsError::NativeKernel(error) => AuthorityStoreError::NativeOriginal(error),
            FsError::Work(error) => error.into(),
            FsError::Cancelled(_) => AuthorityStoreError::Cancelled,
            FsError::WorkspaceDeleted => AuthorityStoreError::Retired,
            _ => AuthorityStoreError::CorruptStatic("fork step returned a non-authority failure"),
        }
    }
}

impl<E> ForkStepsFailure<E> {
    fn map_error<T>(self, map: impl FnOnce(E) -> T) -> ForkStepsFailure<T> {
        ForkStepsFailure { error: map(self.error), work: self.work }
    }
}

/// One ordered recovery algorithm; only each backend operation's funding differs.
struct ForkStepsWork<'a> {
    work: WorkCounters,
    budget: WorkBudget,
    cancellation: &'a CancellationToken,
    #[cfg(not(target_arch = "wasm32"))]
    original: Option<&'a crate::NativeMaterializationWork>,
}

impl<'a> ForkStepsWork<'a> {
    fn ordinary(budget: WorkBudget, cancellation: &'a CancellationToken) -> Self {
        Self { work: WorkCounters::UNCHARGED, budget, cancellation,
            #[cfg(not(target_arch = "wasm32"))]
            original: None,
        }
    }

    fn failure(&self, error: impl Into<crate::facade::FsError>) -> ForkStepsFailure {
        ForkStepsFailure { error: error.into(), work: self.work }
    }

    fn remaining(&self) -> Result<WorkBudget, ForkStepsFailure> {
        self.work.remaining(self.budget).map_err(|error| self.failure(error))
    }

    fn accept<T>(&mut self, result: crate::facade::FsResult<T>) -> Result<T, ForkStepsFailure> {
        let receipt = result.map_err(|failure| {
            let failure = failure.map_with_prior_work(self.work, std::convert::identity);
            ForkStepsFailure { error: failure.error, work: *failure.work }
        })?;
        self.work = self.work.checked_add(receipt.work).map_err(|error| self.failure(error))?;
        self.work.verify(self.budget).map_err(|error| self.failure(error))?;
        Ok(receipt.value)
    }

    async fn first<S, F, Fut, G: NativeForkStepGuard>(
        &mut self, store: &S, authority: AuthorityId, commit: ProposedCommit, guard: Option<&mut F>,
        native_guard: &mut G,
    ) -> Result<bool, ForkStepsFailure<G::Error>>
    where S: AsyncAuthorityStore + ?Sized, F: FnMut(WorkBudget) -> Fut + Send,
        Fut: Future<Output = crate::facade::FsResult<()>> + Send,
    {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original {
            return native_guard.first(store, authority, commit, original, self.cancellation)
                .await.map_err(|error| ForkStepsFailure { error, work: original.receipt() });
        }
        let result = match guard {
            Some(guard) => store.create_authority_with_first_record_guarded(
                authority, commit, self.remaining().map_err(|failure| failure.map_error(Into::into))?, self.cancellation, guard,
            ).await,
            None => append_first_record(
                store, authority, commit, WorkCounters::UNCHARGED, self.remaining().map_err(|failure| failure.map_error(Into::into))?, self.cancellation,
            ).await.map(|receipt| crate::FsReceipt { value: receipt.value, work: receipt.work }).map_err(authority_to_fs),
        };
        self.accept(result).map_err(|failure| failure.map_error(Into::into))
    }

    async fn create<S, F, Fut, G: NativeForkStepGuard>(
        &mut self, store: &S, source: Option<GenerationForkSource>, destination: AuthorityId,
        operation: OperationId, guard: Option<&mut F>, native_guard: &mut G,
    ) -> Result<CreateAuthorityOutcome, ForkStepsFailure<G::Error>>
    where S: AsyncAuthorityStore + ?Sized, F: FnMut(WorkBudget) -> Fut + Send,
        Fut: Future<Output = crate::facade::FsResult<()>> + Send,
    {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original {
            return native_guard.create(store, source, destination, operation, original, self.cancellation)
                .await.map_err(|error| ForkStepsFailure { error, work: original.receipt() });
        }
        let remaining = self.remaining().map_err(|failure| failure.map_error(Into::into))?;
        let result = match (source, guard) {
            (Some(source), Some(guard)) => store.fork_generation_authority_guarded(
                source, destination, operation, remaining, self.cancellation, guard,
            ).await,
            (None, Some(guard)) => store.create_authority_guarded(
                destination, Epoch::GENESIS, remaining, self.cancellation, guard,
            ).await,
            (Some(source), None) => store.fork_generation_authority(
                source, destination, operation, remaining, self.cancellation,
            ).await.map(|receipt| crate::FsReceipt { value: receipt.value, work: receipt.work }).map_err(authority_to_fs),
            (None, None) => store.create_authority(
                destination, Epoch::GENESIS, remaining, self.cancellation,
            ).await.map(|receipt| crate::FsReceipt { value: receipt.value, work: receipt.work }).map_err(authority_to_fs),
        };
        self.accept(result).map_err(|failure| failure.map_error(Into::into))
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn commit_workspace_fork_in_steps_with_native_work<S: AsyncAuthorityStore + ?Sized>(
    store: &S, fork: WorkspaceForkCommit, original: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<WorkspaceForkOutcome, crate::storage::AuthorityStoreError> {
    original.admit(WorkCounters::UNCHARGED)?;
    cancellation.check().map_err(|_| crate::storage::AuthorityStoreError::Cancelled)?;
    commit_workspace_fork_steps_engine(
        store, fork, PublicationPermit::Unrestricted,
        ForkStepsWork { work: original.receipt(), budget: WorkBudget::default(), cancellation, original: Some(original) },
        None::<&mut fn(WorkBudget) -> std::future::Ready<crate::facade::FsResult<()>>>,
        &mut NoNativeForkGuard,
    ).await.map(|receipt| receipt.value).map_err(ForkStepsFailure::native)
}

async fn commit_workspace_fork_steps_engine<'a, S, F, Fut, G: NativeForkStepGuard>(
    store: &S, fork: WorkspaceForkCommit, permit: PublicationPermit,
    mut work: ForkStepsWork<'a>, mut guard: Option<&mut F>, native_guard: &mut G,
) -> Result<crate::facade::FsReceipt<WorkspaceForkOutcome>, ForkStepsFailure<G::Error>>
where S: AsyncAuthorityStore + ?Sized, F: FnMut(WorkBudget) -> Fut + Send,
    Fut: Future<Output = crate::facade::FsResult<()>> + Send,
{
    if permit != PublicationPermit::Unrestricted {
        return Err(work.failure(crate::storage::AuthorityStoreError::RejectedStatic(
            "authority backend cannot atomically evaluate fork publication leases",
        )).map_error(Into::into));
    }
    let WorkspaceForkCommit { lineage, destination, creation, retention, retained } = fork;
    // `creation` stays physically alive (including its intrinsic Bytes custody)
    // throughout retention publication and lineage recovery.
    let value = if !work.first(store, retention, retained,
        None::<&mut F>, &mut NoNativeForkGuard).await.map_err(|failure| failure.map_error(Into::into))? {
        WorkspaceForkOutcome::RetentionConflict
    } else {
        work.create(store, lineage, destination, creation.operation_id, guard.as_deref_mut(), native_guard).await?;
        if work.first(store, destination, creation, guard, native_guard).await? {
            WorkspaceForkOutcome::Committed
        } else {
            WorkspaceForkOutcome::CreationRejected
        }
    };
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = work.original { work.work = original.receipt(); }
    Ok(crate::facade::FsReceipt { value, work: work.work })
}

trait NativeForkStepGuard: Send {
    type Error: From<crate::FsError> + Send;
    #[cfg(not(target_arch = "wasm32"))]
    fn first<S: AsyncAuthorityStore + ?Sized>(
        &mut self, store: &S, authority: AuthorityId, commit: ProposedCommit,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;
    #[cfg(not(target_arch = "wasm32"))]
    fn create<S: AsyncAuthorityStore + ?Sized>(
        &mut self, store: &S, source: Option<GenerationForkSource>, destination: AuthorityId, operation: OperationId,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<CreateAuthorityOutcome, Self::Error>> + Send;
}

struct NoNativeForkGuard;
impl NativeForkStepGuard for NoNativeForkGuard {
    type Error = crate::FsError;
    #[cfg(not(target_arch = "wasm32"))]
    async fn first<S: AsyncAuthorityStore + ?Sized>(
        &mut self, store: &S, authority: AuthorityId, commit: ProposedCommit,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<bool, crate::FsError> {
        store.create_authority_with_first_record_with_native_work(authority, commit, original, cancellation)
            .await.map_err(Into::into)
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn create<S: AsyncAuthorityStore + ?Sized>(
        &mut self, store: &S, source: Option<GenerationForkSource>, destination: AuthorityId, operation: OperationId,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<CreateAuthorityOutcome, crate::FsError> {
        match source {
            Some(source) => store.fork_generation_authority_with_native_work(
                source, destination, operation, original, cancellation,
            ).await.map_err(Into::into),
            None => store.create_authority_with_native_work(
                destination, Epoch::GENESIS, original, cancellation,
            ).await.map_err(Into::into),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct GuardedNativeFork<'g, F> {
    callback: &'g mut F,
}
#[cfg(not(target_arch = "wasm32"))]
impl<E, F, Fut> NativeForkStepGuard for GuardedNativeFork<'_, F>
where E: From<crate::FsError> + Send + 'static,
    F: for<'n> FnMut(&'n crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static {
    type Error = E;
    async fn first<S: AsyncAuthorityStore + ?Sized>(
        &mut self, store: &S, authority: AuthorityId, commit: ProposedCommit,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<bool, E> {
        store.create_authority_with_first_record_guarded_with_native_work(
            authority, commit, original, cancellation, self.callback,
        ).await
    }
    async fn create<S: AsyncAuthorityStore + ?Sized>(
        &mut self, store: &S, source: Option<GenerationForkSource>, destination: AuthorityId, operation: OperationId,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<CreateAuthorityOutcome, E> {
        match source {
            Some(source) => store.fork_generation_authority_guarded_with_native_work(
                source, destination, operation, original, cancellation, self.callback,
            ).await,
            None => store.create_authority_guarded_with_native_work(
                destination, Epoch::GENESIS, original, cancellation, self.callback,
            ).await,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn commit_workspace_fork_in_steps_guarded_with_native_work<'a, S, E, F, Fut>(
    store: &S, fork: WorkspaceForkCommit, permit: PublicationPermit,
    original: &'a crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
) -> Result<WorkspaceForkOutcome, E>
where S: AsyncAuthorityStore + ?Sized, E: From<crate::FsError> + Send + 'static,
    F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static {
    original.admit(WorkCounters::UNCHARGED).map_err(crate::FsError::from)?;
    cancellation.check().map_err(crate::FsError::from)?;
    commit_workspace_fork_steps_engine(
        store, fork, permit,
        ForkStepsWork { work: original.receipt(), budget: WorkBudget::default(), cancellation, original: Some(original) },
        None::<&mut fn(WorkBudget) -> std::future::Ready<crate::FsResult<()>>>,
        &mut GuardedNativeFork { callback: guard },
    ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
}


/// Makes `commit` the first record of `authority`, creating the authority
/// if needed. Answers whether the authority's first record is exactly that
/// commit, including one an earlier attempt or a combined commit wrote under
/// another retry identity. The receipt's work includes `prior`.
///
/// This is the default
/// [`AsyncAuthorityStore::create_authority_with_first_record`], for backends
/// that resolve an existing authority here.
///
/// The caller retains the input payload's admitted backing-capacity claim for
/// the complete future and supplies a budget reduced by all caller-held live
/// bytes. Child authority operations borrow that input custody; they account
/// only their own allocations, copies, encodings, hashes, and replay buffers.
/// The original backing capacity must never be inferred from `Bytes::len()`.
pub async fn append_first_record<S: AsyncAuthorityStore + ?Sized>(
    store: &S,
    authority: AuthorityId,
    commit: ProposedCommit,
    prior: WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> AuthorityResult<bool> {
    authority_projection(append_first_record_engine(
        store, authority, commit, prior, budget, cancellation,
        None::<&mut fn(WorkBudget) -> std::future::Ready<crate::facade::FsResult<()>>>,
    ).await)
}

/// Guarded first-record recovery with the same caller-held payload custody as
/// [`append_first_record`]. Seals each publication and a successful retry.
pub async fn append_first_record_guarded<S, F, Fut>(
    store: &S,
    authority: AuthorityId,
    commit: ProposedCommit,
    prior: WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    guard: &mut F,
) -> crate::facade::FsResult<bool>
where
    S: AsyncAuthorityStore + ?Sized,
    F: FnMut(WorkBudget) -> Fut + Send,
    Fut: Future<Output = crate::facade::FsResult<()>> + Send,
{
    append_first_record_engine(
        store, authority, commit, prior, budget, cancellation, Some(guard),
    ).await
}

async fn append_first_record_engine<S, F, Fut>(
    store: &S,
    authority: AuthorityId,
    commit: ProposedCommit,
    prior: WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    mut guard: Option<&mut F>,
) -> crate::facade::FsResult<bool>
where
    S: AsyncAuthorityStore + ?Sized,
    F: FnMut(WorkBudget) -> Fut + Send,
    Fut: Future<Output = crate::facade::FsResult<()>> + Send,
{
    let remaining = remaining_authority(prior, budget).map_err(authority_to_fs)?;
    let created = match guard.as_deref_mut() {
        Some(guard) => store.create_authority_guarded(
            authority, Epoch::GENESIS, remaining, cancellation, guard,
        ).await,
        None => store.create_authority(
            authority, Epoch::GENESIS, remaining, cancellation,
        ).await.map(|receipt| crate::FsReceipt { value: receipt.value, work: receipt.work }).map_err(authority_to_fs),
    }.map_err(|failure| failure.map_with_prior_work(prior, std::convert::identity))?;
    let mut work = add_authority(prior, created.work, budget).map_err(authority_to_fs)?;
    let head = match created.value {
        CreateAuthorityOutcome::Created(head) | CreateAuthorityOutcome::Existing(head) => head,
    };
    let identity = (commit.operation_id, commit.fingerprint);
    if let Some(guard) = guard.as_deref_mut() {
        run_authority_guard(guard, &mut work, budget).await?;
    }
    let appended = store
        .compare_and_append(
            authority,
            head.epoch,
            Head::genesis(head.epoch),
            commit,
            remaining_authority(work, budget).map_err(authority_to_fs)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, crate::facade::FsError::from))?;
    work = add_authority(work, appended.work, budget).map_err(authority_to_fs)?;
    let retry = matches!(
        &appended.value,
        AppendOutcome::AlreadyCommitted(_) | AppendOutcome::Conflict { .. }
    );
    let matches = match appended.value {
        AppendOutcome::Committed(_) | AppendOutcome::AlreadyCommitted(_) => true,
        AppendOutcome::Conflict { .. } => {
            let first = store
                .replay(
                    authority,
                    Sequence::GENESIS,
                    ReplayLimit {
                        records: 1,
                        payload_bytes: MAXIMUM_FIRST_RECORD_BYTES,
                    },
                    remaining_authority(work, budget).map_err(authority_to_fs)?,
                    cancellation,
                )
                .await
                .map_err(|failure| failure.map_with_prior_work(work, crate::facade::FsError::from))?;
            work = add_authority(work, first.work, budget).map_err(authority_to_fs)?;
            first.value.first().is_some_and(|record| {
                record.sequence == Sequence::new(1)
                    && (record.operation_id, record.fingerprint) == identity
            })
        }
        AppendOutcome::Fenced { .. } | AppendOutcome::IdempotencyConflict { .. } => false,
    };
    if matches && retry {
        if let Some(guard) = guard {
            run_authority_guard(guard, &mut work, budget).await?;
        }
    }
    Ok(crate::FsReceipt {
        value: matches,
        work,
    })
}

fn remaining_authority(
    work: WorkCounters,
    budget: WorkBudget,
) -> Result<WorkBudget, crate::storage::AuthorityFailure> {
    work.remaining(budget)
        .map_err(|error| crate::storage::AuthorityFailure::new(error.into(), work))
}

fn add_authority(
    work: WorkCounters,
    more: WorkCounters,
    budget: WorkBudget,
) -> Result<WorkCounters, crate::storage::AuthorityFailure> {
    let combined = work
        .checked_add(more)
        .map_err(|error| crate::storage::AuthorityFailure::new(error.into(), work))?;
    combined
        .verify(budget)
        .map_err(|error| crate::storage::AuthorityFailure::new(error.into(), combined))?;
    Ok(combined)
}

fn authority_to_fs(
    failure: crate::storage::AuthorityFailure,
) -> crate::performance::OperationFailure<crate::facade::FsError> {
    crate::performance::OperationFailure::new(failure.error.into(), *failure.work)
}

fn authority_projection<T>(result: crate::facade::FsResult<T>) -> AuthorityResult<T> {
    result.map(|receipt| crate::storage::AuthorityReceipt { value: receipt.value, work: receipt.work }).map_err(|failure| {
        let error = match failure.error {
            crate::facade::FsError::Authority(error) => error,
            crate::facade::FsError::Work(error) => error.into(),
            crate::facade::FsError::Cancelled(_) => crate::storage::AuthorityStoreError::Cancelled,
            crate::facade::FsError::WorkspaceDeleted => crate::storage::AuthorityStoreError::Retired,
            _ => unreachable!("unguarded authority engine cannot produce a guard failure"),
        };
        crate::storage::AuthorityFailure::new(error, *failure.work)
    })
}

async fn run_authority_guard<F, Fut>(
    guard: &mut F,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> crate::facade::FsResult<()>
where
    F: FnMut(WorkBudget) -> Fut + Send,
    Fut: Future<Output = crate::facade::FsResult<()>> + Send,
{
    let receipt = guard(remaining_authority(*work, budget).map_err(authority_to_fs)?)
        .await
        .map_err(|failure| failure.map_with_prior_work(*work, std::convert::identity))?;
    *work = add_authority(*work, receipt.work, budget).map_err(authority_to_fs)?;
    Ok(crate::facade::FsReceipt { value: (), work: *work })
}

/// Nonblocking authority-store contract. Futures are sendable on native
/// targets and may remain JavaScript-thread-affine in browsers.
pub trait AsyncAuthorityStore: StorageProvider {
    /// Seals every actual destination publication against the same original ledger.
    /// The owner's callback error is returned intact, including during retries
    /// and recovery; adapters must seal after actual request preparation.
    /// The callback borrows only the identity synchronously; its future owns
    /// original lease/request custody through accepted native work. Cloning the
    /// work aliases that same ledger and never creates a remaining allowance.
    #[cfg(not(target_arch = "wasm32"))]
    fn commit_workspace_fork_guarded_with_native_work<'a, E, F, Fut>(
        &self, fork: WorkspaceForkCommit, permit: PublicationPermit,
        original: &'a crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
    ) -> impl Future<Output = Result<WorkspaceForkOutcome, E>> + StorageFuture
    where E: From<crate::FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static {
        let _ = (fork, permit, original, cancellation, guard);
        std::future::ready(Err(crate::FsError::from(crate::storage::AuthorityStoreError::NativeOriginalUnsupported).into()))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn create_authority_guarded_with_native_work<'a, E, F, Fut>(
        &self, authority: AuthorityId, epoch: Epoch,
        original: &'a crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
    ) -> impl Future<Output = Result<CreateAuthorityOutcome, E>> + StorageFuture
    where E: From<crate::FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static {
        let _ = (authority, epoch, original, cancellation, guard);
        std::future::ready(Err(crate::FsError::from(crate::storage::AuthorityStoreError::NativeOriginalUnsupported).into()))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn create_authority_with_first_record_guarded_with_native_work<'a, E, F, Fut>(
        &self, authority: AuthorityId, commit: ProposedCommit,
        original: &'a crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
    ) -> impl Future<Output = Result<bool, E>> + StorageFuture
    where E: From<crate::FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static {
        let _ = (authority, commit, original, cancellation, guard);
        std::future::ready(Err(crate::FsError::from(crate::storage::AuthorityStoreError::NativeOriginalUnsupported).into()))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn fork_generation_authority_guarded_with_native_work<'a, E, F, Fut>(
        &self, source: GenerationForkSource, destination: AuthorityId, operation: OperationId,
        original: &'a crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
    ) -> impl Future<Output = Result<CreateAuthorityOutcome, E>> + StorageFuture
    where E: From<crate::FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static {
        async move {
            if source.lineage == GenerationFork::PublishedPrefix {
                return Err(crate::FsError::from(crate::storage::AuthorityStoreError::NativeOriginalUnsupported).into());
            }
            let _ = operation;
            self.create_authority_guarded_with_native_work(destination, Epoch::GENESIS, original, cancellation, guard).await
        }
    }

    /// Commits the canonical fork using only the caller's original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn commit_workspace_fork_with_native_work(
        &self, fork: WorkspaceForkCommit, original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<WorkspaceForkOutcome, crate::storage::AuthorityStoreError>> + StorageFuture {
        commit_workspace_fork_in_steps_with_native_work(self, fork, original, cancellation)
    }

    /// Retains the exact source lineage and operation identity in original custody.
    #[cfg(not(target_arch = "wasm32"))]
    fn fork_generation_authority_with_native_work(
        &self, source: GenerationForkSource, destination: AuthorityId, operation: OperationId,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<CreateAuthorityOutcome, crate::storage::AuthorityStoreError>> + StorageFuture {
        async move {
            if source.lineage == GenerationFork::PublishedPrefix {
                return Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported);
            }
            let _ = operation;
            self.create_authority_with_native_work(destination, Epoch::GENESIS, original, cancellation).await
        }
    }

    /// Creates the same empty authority using original accounting before allocation.
    #[cfg(not(target_arch = "wasm32"))]
    fn create_authority_with_native_work(
        &self, authority: AuthorityId, epoch: Epoch,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<CreateAuthorityOutcome, crate::storage::AuthorityStoreError>> + StorageFuture {
        let _ = (authority, epoch, original, cancellation);
        std::future::ready(Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported))
    }

    /// Whether this backend can retain an immutable published-generation
    /// lineage prefix when it creates a child authority.
    fn supports_generation_lineage_prefix(&self) -> bool {
        false
    }

    /// Creates a destination authority whose canonical generation lineage is
    /// either an immutable native prefix of the source or an explicit
    /// independent lineage rooted at an unpublished authenticated generation.
    fn fork_generation_authority(
        &self,
        source: GenerationForkSource,
        destination_authority: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<CreateAuthorityOutcome>> + StorageFuture;

    /// Seals destination creation. This generic fallback cannot observe planning
    /// inside an opaque backend; allocating native providers must override it
    /// and invoke the guard after planning, immediately before publication.
    fn fork_generation_authority_guarded<F, Fut>(
        &self,
        source: GenerationForkSource,
        destination_authority: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        guard: &mut F,
    ) -> impl Future<Output = crate::facade::FsResult<CreateAuthorityOutcome>> + StorageFuture
    where
        F: FnMut(WorkBudget) -> Fut + Send,
        Fut: Future<Output = crate::facade::FsResult<()>> + Send,
    {
        async move {
            let mut work = WorkCounters::default();
            run_authority_guard(guard, &mut work, budget).await?;
            let receipt = self.fork_generation_authority(
                source, destination_authority, operation_id,
                remaining_authority(work, budget).map_err(authority_to_fs)?, cancellation,
            ).await.map_err(|failure| failure.map_with_prior_work(work, crate::facade::FsError::from))?;
            work = add_authority(work, receipt.work, budget).map_err(authority_to_fs)?;
            if matches!(&receipt.value, CreateAuthorityOutcome::Existing(_)) {
                run_authority_guard(guard, &mut work, budget).await?;
            }
            Ok(crate::facade::FsReceipt { value: receipt.value, work })
        }
    }

    /// Asynchronously creates one authority.
    fn create_authority(
        &self,
        authority_id: AuthorityId,
        genesis_epoch: Epoch,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<CreateAuthorityOutcome>> + StorageFuture;

    /// Guarded authority creation. Opaque backend planning is not visible to
    /// this fallback; native allocating providers override the postplanning seam.
    fn create_authority_guarded<F, Fut>(
        &self,
        authority_id: AuthorityId,
        genesis_epoch: Epoch,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        guard: &mut F,
    ) -> impl Future<Output = crate::facade::FsResult<CreateAuthorityOutcome>> + StorageFuture
    where
        F: FnMut(WorkBudget) -> Fut + Send,
        Fut: Future<Output = crate::facade::FsResult<()>> + Send,
    {
        async move {
            let mut work = WorkCounters::default();
            run_authority_guard(guard, &mut work, budget).await?;
            let receipt = self.create_authority(
                authority_id, genesis_epoch,
                remaining_authority(work, budget).map_err(authority_to_fs)?, cancellation,
            ).await.map_err(|failure| failure.map_with_prior_work(work, crate::facade::FsError::from))?;
            work = add_authority(work, receipt.work, budget).map_err(authority_to_fs)?;
            if matches!(&receipt.value, CreateAuthorityOutcome::Existing(_)) {
                run_authority_guard(guard, &mut work, budget).await?;
            }
            Ok(crate::facade::FsReceipt { value: receipt.value, work })
        }
    }

    /// Retires `authority_id` after its workspace deletion record is durable.
    /// Backends that support metadata reclamation may release authority state
    /// and answer [`crate::storage::AuthorityStoreError::Retired`] afterward.
    /// Stream-backed authorities retain every committed Stream record and use
    /// a separate filesystem marker to answer as retired. Retiring twice succeeds.
    fn retire_authority(
        &self,
        authority_id: AuthorityId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<()>> + StorageFuture {
        let _ = (authority_id, budget, cancellation);
        std::future::ready(Ok(crate::storage::AuthorityReceipt {
            value: (),
            work: crate::WorkCounters::default(),
        }))
    }

    /// Asynchronously reads the linearizable head.
    fn head(
        &self,
        authority_id: AuthorityId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<Head>> + StorageFuture;

    /// Asynchronously compares and appends one durable operation.
    fn compare_and_append(
        &self,
        authority_id: AuthorityId,
        epoch: Epoch,
        expected: Head,
        commit: ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<AppendOutcome>> + StorageFuture;

    /// Asynchronously compares and appends under an optional operation-window
    /// permit. Backends that cannot atomically evaluate lease gates reject
    /// managed permits rather than using a read-then-write approximation.
    fn compare_and_append_guarded(
        &self,
        request: GuardedAppend,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<AppendOutcome>> + StorageFuture {
        async move {
            if request.permit != PublicationPermit::Unrestricted {
                return Err(crate::storage::AuthorityFailure::before_work(
                    crate::storage::AuthorityStoreError::Rejected(
                        "authority backend cannot atomically evaluate operation leases".to_owned(),
                    ),
                ));
            }
            self.compare_and_append(
                request.authority_id,
                request.epoch,
                request.expected,
                request.commit,
                budget,
                cancellation,
            )
            .await
        }
    }

    /// Publishes the canonical guarded append in the original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn compare_and_append_guarded_with_native_work(
        &self,
        request: GuardedAppend,
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<
        (AppendOutcome, crate::NativeAllocation), crate::storage::AuthorityStoreError,
    >> + StorageFuture {
        let _ = (request, original, cancellation);
        std::future::ready(Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported))
    }

    /// Seals the actual canonical publication after planning at every provider
    /// CAS/retry. The owned callback retains original custody through terminal
    /// work, and its original error is never reformatted.
    #[cfg(not(target_arch = "wasm32"))]
    fn compare_and_append_guarded_with_native_owner<F, Fut, E>(
        &self,
        request: GuardedAppend,
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
        guard: &mut F,
    ) -> impl Future<Output = Result<(AppendOutcome, crate::NativeAllocation), E>> + StorageFuture
    where
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<crate::storage::AuthorityStoreError> + Send + 'static,
    {
        let _ = (request, original, cancellation, guard);
        std::future::ready(Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported.into()))
    }

    /// Durably creates an authority whose first record is `commit`, or
    /// confirms that it already exists with exactly that first record.
    /// Answers whether its first record is `commit`. Backends that can
    /// create an authority and append to it atomically do so in one durable
    /// commit; the default creates the authority, then appends.
    fn create_authority_with_first_record(
        &self,
        authority: AuthorityId,
        commit: ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<bool>> + StorageFuture {
        append_first_record(
            self,
            authority,
            commit,
            WorkCounters::default(),
            budget,
            cancellation,
        )
    }

    /// Creates the same initial authority record without a separate operation,
    /// work root, journal, or unchecked output allocation.
    #[cfg(not(target_arch = "wasm32"))]
    fn create_authority_with_first_record_with_native_work(
        &self,
        authority: AuthorityId,
        commit: ProposedCommit,
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<bool, crate::storage::AuthorityStoreError>> + StorageFuture {
        let _ = (authority, commit, original, cancellation);
        std::future::ready(Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported))
    }

    /// Guarded first-record publication. Native providers override this method
    /// to seal after canonical request preparation and before physical append.
    fn create_authority_with_first_record_guarded<F, Fut>(
        &self,
        authority: AuthorityId,
        commit: ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        guard: &mut F,
    ) -> impl Future<Output = crate::facade::FsResult<bool>> + StorageFuture
    where
        F: FnMut(WorkBudget) -> Fut + Send,
        Fut: Future<Output = crate::facade::FsResult<()>> + Send,
    {
        append_first_record_guarded(
            self, authority, commit, WorkCounters::default(), budget, cancellation, guard,
        )
    }

    /// Durably creates a forked workspace's authority state: the retention
    /// authority with its record and the destination authority with its
    /// creation record. Backends that can commit several authorities
    /// atomically do so in one durable commit; the default applies ordered,
    /// idempotent single-authority steps.
    fn commit_workspace_fork(
        &self,
        fork: WorkspaceForkCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<WorkspaceForkOutcome>> + StorageFuture {
        commit_workspace_fork_in_steps(self, fork, budget, cancellation)
    }

    /// Guarded fork publication with externally held original payload claims.
    fn commit_workspace_fork_guarded<F, Fut>(
        &self,
        fork: WorkspaceForkCommit,
        permit: PublicationPermit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        guard: &mut F,
    ) -> impl Future<Output = crate::facade::FsResult<WorkspaceForkOutcome>> + StorageFuture
    where
        F: FnMut(WorkBudget) -> Fut + Send,
        Fut: Future<Output = crate::facade::FsResult<()>> + Send,
    {
        commit_workspace_fork_in_steps_guarded(self, fork, permit, budget, cancellation, guard)
    }

    /// Reads only an existing reservation for the original operation identity.
    /// Never reserves again, generates a gate tail, or substitutes the current head.
    /// Backends without original reservation evidence reject before provider I/O.
    fn publication_reservation(
        &self,
        _authority_id: AuthorityId,
        _operation_id: OperationId,
        _budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<Option<PublicationReservation>>> + StorageFuture {
        async move {
            cancellation.check()
                .map_err(|_| crate::storage::AuthorityFailure::before_work(crate::storage::AuthorityStoreError::Cancelled))?;
            Err(crate::storage::AuthorityFailure::before_work(crate::storage::AuthorityStoreError::Rejected(
                "authority backend cannot query original publication reservation evidence".to_owned(),
            )))
        }
    }

    /// Atomically acquires the exclusive publication gate at an exact head.
    fn reserve_publication(
        &self,
        _authority_id: AuthorityId,
        _expected: Head,
        _operation_id: OperationId,
        _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<ReservationOutcome>> + StorageFuture {
        async move {
            Err(crate::storage::AuthorityFailure::before_work(
                crate::storage::AuthorityStoreError::Rejected(
                    "authority backend does not support durable publication reservations"
                        .to_owned(),
                ),
            ))
        }
    }

    /// Releases an exact publication reservation. Exact retries are idempotent.
    fn release_publication(
        &self,
        _reservation: PublicationReservation,
        _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<()>> + StorageFuture {
        async move {
            Err(crate::storage::AuthorityFailure::before_work(
                crate::storage::AuthorityStoreError::Rejected(
                    "authority backend does not support durable publication reservations"
                        .to_owned(),
                ),
            ))
        }
    }

    /// Queries the original reservation using the invocation's existing native root.
    #[cfg(not(target_arch = "wasm32"))]
    fn publication_reservation_with_native_work(
        &self, authority_id: AuthorityId, operation_id: OperationId,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<Option<PublicationReservation>, crate::storage::AuthorityStoreError>> + StorageFuture {
        let _ = (authority_id, operation_id);
        async move {
            original.admit(WorkCounters::UNCHARGED)?;
            cancellation.check().map_err(|_| crate::storage::AuthorityStoreError::Cancelled)?;
            Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported)
        }
    }

    /// Acquires the exact original publication proof without a separate work ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn reserve_publication_with_native_work(
        &self, authority_id: AuthorityId, expected: Head, operation_id: OperationId,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<PublicationReservation, crate::storage::AuthorityStoreError>> + StorageFuture {
        let _ = (authority_id, expected, operation_id);
        async move {
            original.admit(WorkCounters::UNCHARGED)?;
            cancellation.check().map_err(|_| crate::storage::AuthorityStoreError::Cancelled)?;
            Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported)
        }
    }

    /// Releases only the original proof on the caller's cleanup scope.
    #[cfg(not(target_arch = "wasm32"))]
    fn release_publication_with_native_work(
        &self, reservation: PublicationReservation,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<(), crate::storage::AuthorityStoreError>> + StorageFuture {
        let _ = reservation;
        async move {
            original.admit(WorkCounters::UNCHARGED)?;
            cancellation.check().map_err(|_| crate::storage::AuthorityStoreError::Cancelled)?;
            Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported)
        }
    }

    /// Asynchronously replays one bounded contiguous page.
    fn replay(
        &self,
        authority_id: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<Vec<crate::foundation::DurableCommit>>> + StorageFuture;

    /// Asynchronously advances a writer fence.
    fn fence(
        &self,
        authority_id: AuthorityId,
        expected: Head,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<FenceOutcome>> + StorageFuture;

    /// Asynchronously resolves one idempotent operation identity.
    fn find_operation(
        &self,
        authority_id: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<Option<crate::foundation::DurableCommit>>> + StorageFuture;

    /// Reads the canonical head against the caller's original native ledger.
    /// Opaque custom providers reject before any I/O, rather than allocating
    /// first and fabricating an after-the-fact receipt.
    #[cfg(not(target_arch = "wasm32"))]
    fn head_with_native_work(
        &self,
        authority_id: AuthorityId,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<Head, crate::storage::AuthorityStoreError>> + StorageFuture {
        let _ = (authority_id, work, cancellation);
        std::future::ready(Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported))
    }

    /// Produces a canonical replay page funded in the original ledger. The
    /// returned guard owns only actual newly produced output buffers.
    #[cfg(not(target_arch = "wasm32"))]
    fn replay_with_native_work(
        &self,
        authority_id: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<
        (Vec<crate::foundation::DurableCommit>, crate::materializer::NativeAllocation),
        crate::storage::AuthorityStoreError,
    >> + StorageFuture {
        let _ = (authority_id, after, limit, work, cancellation);
        std::future::ready(Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported))
    }

    /// Applies the canonical writer fence using the original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn fence_with_native_work(
        &self,
        authority_id: AuthorityId,
        expected: Head,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<FenceOutcome, crate::storage::AuthorityStoreError>> + StorageFuture {
        let _ = (authority_id, expected, work, cancellation);
        std::future::ready(Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported))
    }

    /// Resolves the original operation without allocating outside its ledger.
    /// Existing immutable payload owners are not charged as fresh allocations.
    #[cfg(not(target_arch = "wasm32"))]
    fn find_operation_with_native_work(
        &self,
        authority_id: AuthorityId,
        operation_id: OperationId,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<
        (Option<crate::foundation::DurableCommit>, crate::materializer::NativeAllocation),
        crate::storage::AuthorityStoreError,
    >> + StorageFuture {
        let _ = (authority_id, operation_id, work, cancellation);
        std::future::ready(Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported))
    }
}

/// Explicit opt-in for synchronous stores that are safe to complete inline.
///
/// Implementing this trait makes the synchronous authority methods satisfy
/// [`AsyncAuthorityStore`] without yielding. Blocking native backends must use
/// a provider-owned bounded blocking executor instead. Original native work
/// requires an explicit implementation; opaque stores reject before I/O.
pub trait ImmediateAuthorityStore: AuthorityStore {
    fn publication_reservation(
        &self, authority: AuthorityId, operation: OperationId, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Option<PublicationReservation>> {
        let _ = (authority, operation, budget);
        cancellation.check().map_err(|_| crate::storage::AuthorityFailure::before_work(crate::storage::AuthorityStoreError::Cancelled))?;
        Err(crate::storage::AuthorityFailure::before_work(crate::storage::AuthorityStoreError::RejectedStatic(
            "authority backend cannot query original publication reservation evidence")))
    }

    fn reserve_publication(
        &self, authority: AuthorityId, expected: Head, operation: OperationId,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> AuthorityResult<ReservationOutcome> {
        let _ = (authority, expected, operation, budget);
        cancellation.check().map_err(|_| crate::storage::AuthorityFailure::before_work(crate::storage::AuthorityStoreError::Cancelled))?;
        Err(crate::storage::AuthorityFailure::before_work(crate::storage::AuthorityStoreError::RejectedStatic(
            "authority backend does not support durable publication reservations")))
    }

    fn release_publication(
        &self, reservation: PublicationReservation, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<()> {
        let _ = (reservation, budget);
        cancellation.check().map_err(|_| crate::storage::AuthorityFailure::before_work(crate::storage::AuthorityStoreError::Cancelled))?;
        Err(crate::storage::AuthorityFailure::before_work(crate::storage::AuthorityStoreError::RejectedStatic(
            "authority backend does not support durable publication reservations")))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn publication_reservation_with_native_work(
        &self, authority: AuthorityId, operation: OperationId,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<Option<PublicationReservation>, crate::storage::AuthorityStoreError> {
        let _ = (authority, operation);
        original.admit(WorkCounters::UNCHARGED)?;
        cancellation.check().map_err(|_| crate::storage::AuthorityStoreError::Cancelled)?;
        Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn reserve_publication_with_native_work(
        &self, authority: AuthorityId, expected: Head, operation: OperationId,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<PublicationReservation, crate::storage::AuthorityStoreError> {
        let _ = (authority, expected, operation);
        original.admit(WorkCounters::UNCHARGED)?;
        cancellation.check().map_err(|_| crate::storage::AuthorityStoreError::Cancelled)?;
        Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn release_publication_with_native_work(
        &self, reservation: PublicationReservation,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(), crate::storage::AuthorityStoreError> {
        let _ = reservation;
        original.admit(WorkCounters::UNCHARGED)?;
        cancellation.check().map_err(|_| crate::storage::AuthorityStoreError::Cancelled)?;
        Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn commit_workspace_fork_guarded_with_native_work<'a, E, F, Fut>(
        &self, fork: WorkspaceForkCommit, permit: PublicationPermit,
        original: &'a crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
    ) -> impl Future<Output = Result<WorkspaceForkOutcome, E>> + StorageFuture
    where E: From<crate::FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static {
        let _ = (fork, permit, original, cancellation, guard);
        std::future::ready(Err(crate::FsError::from(crate::storage::AuthorityStoreError::NativeOriginalUnsupported).into()))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn create_authority_guarded_with_native_work<'a, E, F, Fut>(
        &self, authority: AuthorityId, epoch: Epoch,
        original: &'a crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
    ) -> impl Future<Output = Result<CreateAuthorityOutcome, E>> + StorageFuture
    where E: From<crate::FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static {
        let _ = (authority, epoch, original, cancellation, guard);
        std::future::ready(Err(crate::FsError::from(crate::storage::AuthorityStoreError::NativeOriginalUnsupported).into()))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn create_authority_with_first_record_guarded_with_native_work<'a, E, F, Fut>(
        &self, authority: AuthorityId, commit: ProposedCommit,
        original: &'a crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
    ) -> impl Future<Output = Result<bool, E>> + StorageFuture
    where E: From<crate::FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static {
        let _ = (authority, commit, original, cancellation, guard);
        std::future::ready(Err(crate::FsError::from(crate::storage::AuthorityStoreError::NativeOriginalUnsupported).into()))
    }

    /// Original-accounted empty-authority primitive used by independent forks.
    #[cfg(not(target_arch = "wasm32"))]
    fn create_authority_with_native_work(
        &self, authority: AuthorityId, epoch: Epoch,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<CreateAuthorityOutcome, crate::storage::AuthorityStoreError> {
        let _ = (authority, epoch, original, cancellation);
        Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported)
    }

    /// Canonical first-record producer. Opaque stores retain the ordered
    /// create/append/recovery fallback; atomic stores override this seam.
    fn create_authority_with_first_record(
        &self, authority: AuthorityId, commit: ProposedCommit, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<bool>> + StorageFuture {
        append_first_record(self, authority, commit, WorkCounters::default(), budget, cancellation)
    }

    /// Canonical guarded first-record producer, including successful recovery.
    fn create_authority_with_first_record_guarded<F, Fut>(
        &self, authority: AuthorityId, commit: ProposedCommit, budget: WorkBudget,
        cancellation: &CancellationToken, guard: &mut F,
    ) -> impl Future<Output = crate::facade::FsResult<bool>> + StorageFuture
    where
        F: FnMut(WorkBudget) -> Fut + Send,
        Fut: Future<Output = crate::facade::FsResult<()>> + Send,
    {
        append_first_record_guarded(self, authority, commit, WorkCounters::default(), budget, cancellation, guard)
    }

    /// Original-ledger atomic first-record publication.
    #[cfg(not(target_arch = "wasm32"))]
    fn create_authority_with_first_record_with_native_work(
        &self, authority: AuthorityId, commit: ProposedCommit,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<bool, crate::storage::AuthorityStoreError> {
        let _ = (authority, commit, original, cancellation);
        Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported)
    }

    /// Original-ledger canonical guarded append.
    #[cfg(not(target_arch = "wasm32"))]
    fn compare_and_append_guarded_with_native_work(
        &self, request: GuardedAppend, original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(AppendOutcome, crate::NativeAllocation), crate::storage::AuthorityStoreError> {
        let _ = (request, original, cancellation);
        Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported)
    }

    /// Plans first, then seals outside every provider and ledger lock.
    #[cfg(not(target_arch = "wasm32"))]
    fn compare_and_append_guarded_with_native_owner<F, Fut, E>(
        &self, request: GuardedAppend, original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken, guard: &mut F,
    ) -> impl Future<Output = Result<(AppendOutcome, crate::NativeAllocation), E>> + StorageFuture
    where
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<crate::storage::AuthorityStoreError> + Send + 'static,
    {
        let _ = (request, original, cancellation, guard);
        std::future::ready(Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported.into()))
    }

    /// Reads the canonical head directly against the original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn head_with_native_work(
        &self, authority_id: AuthorityId,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<Head, crate::storage::AuthorityStoreError> {
        let _ = (authority_id, work, cancellation);
        Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported)
    }

    /// Replays a bounded canonical page with custody of its actual new buffers.
    #[cfg(not(target_arch = "wasm32"))]
    fn replay_with_native_work(
        &self, authority_id: AuthorityId, after: Sequence, limit: ReplayLimit,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(Vec<crate::foundation::DurableCommit>, crate::materializer::NativeAllocation), crate::storage::AuthorityStoreError> {
        let _ = (authority_id, after, limit, work, cancellation);
        Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported)
    }

    /// Applies the canonical fence directly against the original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn fence_with_native_work(
        &self, authority_id: AuthorityId, expected: Head,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<FenceOutcome, crate::storage::AuthorityStoreError> {
        let _ = (authority_id, expected, work, cancellation);
        Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported)
    }

    /// Resolves an operation while preserving existing shared payload owners.
    #[cfg(not(target_arch = "wasm32"))]
    fn find_operation_with_native_work(
        &self, authority_id: AuthorityId, operation_id: OperationId,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(Option<crate::foundation::DurableCommit>, crate::materializer::NativeAllocation), crate::storage::AuthorityStoreError> {
        let _ = (authority_id, operation_id, work, cancellation);
        Err(crate::storage::AuthorityStoreError::NativeOriginalUnsupported)
    }
}

impl<T: ImmediateAuthorityStore + ?Sized> ImmediateAuthorityStore for Arc<T> {
    fn publication_reservation(&self, authority: AuthorityId, operation: OperationId, budget: WorkBudget, cancellation: &CancellationToken) -> AuthorityResult<Option<PublicationReservation>> {
        ImmediateAuthorityStore::publication_reservation(self.as_ref(), authority, operation, budget, cancellation)
    }
    fn reserve_publication(&self, authority: AuthorityId, expected: Head, operation: OperationId, budget: WorkBudget, cancellation: &CancellationToken) -> AuthorityResult<ReservationOutcome> {
        ImmediateAuthorityStore::reserve_publication(self.as_ref(), authority, expected, operation, budget, cancellation)
    }
    fn release_publication(&self, reservation: PublicationReservation, budget: WorkBudget, cancellation: &CancellationToken) -> AuthorityResult<()> {
        ImmediateAuthorityStore::release_publication(self.as_ref(), reservation, budget, cancellation)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn publication_reservation_with_native_work(&self, authority: AuthorityId, operation: OperationId, original: &crate::NativeMaterializationWork, cancellation: &CancellationToken) -> Result<Option<PublicationReservation>, crate::storage::AuthorityStoreError> {
        ImmediateAuthorityStore::publication_reservation_with_native_work(self.as_ref(), authority, operation, original, cancellation)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn reserve_publication_with_native_work(&self, authority: AuthorityId, expected: Head, operation: OperationId, original: &crate::NativeMaterializationWork, cancellation: &CancellationToken) -> Result<PublicationReservation, crate::storage::AuthorityStoreError> {
        ImmediateAuthorityStore::reserve_publication_with_native_work(self.as_ref(), authority, expected, operation, original, cancellation)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn release_publication_with_native_work(&self, reservation: PublicationReservation, original: &crate::NativeMaterializationWork, cancellation: &CancellationToken) -> Result<(), crate::storage::AuthorityStoreError> {
        ImmediateAuthorityStore::release_publication_with_native_work(self.as_ref(), reservation, original, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn commit_workspace_fork_guarded_with_native_work<'a, E, F, Fut>(
        &self, fork: WorkspaceForkCommit, permit: PublicationPermit,
        original: &'a crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
    ) -> impl Future<Output = Result<WorkspaceForkOutcome, E>> + StorageFuture
    where E: From<crate::FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static {
        ImmediateAuthorityStore::commit_workspace_fork_guarded_with_native_work(
            self.as_ref(), fork, permit, original, cancellation, guard,
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn create_authority_guarded_with_native_work<'a, E, F, Fut>(
        &self, authority: AuthorityId, epoch: Epoch,
        original: &'a crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
    ) -> impl Future<Output = Result<CreateAuthorityOutcome, E>> + StorageFuture
    where E: From<crate::FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static {
        ImmediateAuthorityStore::create_authority_guarded_with_native_work(
            self.as_ref(), authority, epoch, original, cancellation, guard,
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn create_authority_with_first_record_guarded_with_native_work<'a, E, F, Fut>(
        &self, authority: AuthorityId, commit: ProposedCommit,
        original: &'a crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
    ) -> impl Future<Output = Result<bool, E>> + StorageFuture
    where E: From<crate::FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static {
        ImmediateAuthorityStore::create_authority_with_first_record_guarded_with_native_work(
            self.as_ref(), authority, commit, original, cancellation, guard,
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn create_authority_with_native_work(
        &self, authority: AuthorityId, epoch: Epoch,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<CreateAuthorityOutcome, crate::storage::AuthorityStoreError> {
        ImmediateAuthorityStore::create_authority_with_native_work(self.as_ref(), authority, epoch, original, cancellation)
    }

    fn create_authority_with_first_record(
        &self, authority: AuthorityId, commit: ProposedCommit, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = AuthorityResult<bool>> + StorageFuture {
        ImmediateAuthorityStore::create_authority_with_first_record(self.as_ref(), authority, commit, budget, cancellation)
    }

    fn create_authority_with_first_record_guarded<F, Fut>(
        &self, authority: AuthorityId, commit: ProposedCommit, budget: WorkBudget,
        cancellation: &CancellationToken, guard: &mut F,
    ) -> impl Future<Output = crate::facade::FsResult<bool>> + StorageFuture
    where
        F: FnMut(WorkBudget) -> Fut + Send,
        Fut: Future<Output = crate::facade::FsResult<()>> + Send,
    {
        ImmediateAuthorityStore::create_authority_with_first_record_guarded(self.as_ref(), authority, commit, budget, cancellation, guard)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn create_authority_with_first_record_with_native_work(
        &self, authority: AuthorityId, commit: ProposedCommit,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<bool, crate::storage::AuthorityStoreError> {
        ImmediateAuthorityStore::create_authority_with_first_record_with_native_work(self.as_ref(), authority, commit, original, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn compare_and_append_guarded_with_native_work(
        &self, request: GuardedAppend, original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(AppendOutcome, crate::NativeAllocation), crate::storage::AuthorityStoreError> {
        ImmediateAuthorityStore::compare_and_append_guarded_with_native_work(self.as_ref(), request, original, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn compare_and_append_guarded_with_native_owner<F, Fut, E>(
        &self, request: GuardedAppend, original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken, guard: &mut F,
    ) -> impl Future<Output = Result<(AppendOutcome, crate::NativeAllocation), E>> + StorageFuture
    where
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<crate::storage::AuthorityStoreError> + Send + 'static,
    {
        ImmediateAuthorityStore::compare_and_append_guarded_with_native_owner(self.as_ref(), request, original, cancellation, guard)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn head_with_native_work(
        &self, authority_id: AuthorityId,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<Head, crate::storage::AuthorityStoreError> {
        ImmediateAuthorityStore::head_with_native_work(self.as_ref(), authority_id, work, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn replay_with_native_work(
        &self, authority_id: AuthorityId, after: Sequence, limit: ReplayLimit,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(Vec<crate::foundation::DurableCommit>, crate::materializer::NativeAllocation), crate::storage::AuthorityStoreError> {
        ImmediateAuthorityStore::replay_with_native_work(self.as_ref(), authority_id, after, limit, work, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn fence_with_native_work(
        &self, authority_id: AuthorityId, expected: Head,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<FenceOutcome, crate::storage::AuthorityStoreError> {
        ImmediateAuthorityStore::fence_with_native_work(self.as_ref(), authority_id, expected, work, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn find_operation_with_native_work(
        &self, authority_id: AuthorityId, operation_id: OperationId,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(Option<crate::foundation::DurableCommit>, crate::materializer::NativeAllocation), crate::storage::AuthorityStoreError> {
        ImmediateAuthorityStore::find_operation_with_native_work(self.as_ref(), authority_id, operation_id, work, cancellation)
    }
}

/// The objects one authority record makes reachable, which must be durable
/// before the record is appended.
#[derive(Clone, Copy, Debug)]
pub enum PublicationScope<'a> {
    /// The complete authenticated closure of one published generation.
    Closure {
        /// Every object the record reaches.
        objects: &'a [ObjectId],
        /// The store's [`AsyncObjectStore::collection_sweeps`] before the
        /// closure was proven.
        proven_at: u64,
    },
    /// Every object admitted so far, for a record whose closure was not
    /// enumerated.
    Everything,
}

/// Nonblocking immutable-object contract suitable for `IndexedDB` and remote I/O.
pub trait AsyncObjectStore: StorageProvider {
    /// Returns one exact decoded immutable representation when resident.
    ///
    /// # Errors
    ///
    /// Returns a storage error when disposable accelerator state is corrupt.
    fn decoded_cache_get(
        &self,
        _key: DecodedCacheKey,
    ) -> Result<Option<DecodedCacheValue>, crate::storage::ObjectStoreError> {
        Ok(None)
    }

    /// Borrows the same immutable cache entry under the original native context.
    #[cfg(not(target_arch = "wasm32"))]
    fn decoded_cache_get_with_native_work(
        &self,
        key: DecodedCacheKey,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<Option<DecodedCacheValue>, crate::storage::ObjectStoreError> {
        work.remaining()?;
        self.decoded_cache_get(key)
    }

    /// Offers one authenticated decoded representation to a disposable cache.
    ///
    /// # Errors
    ///
    /// Returns a storage error when the accelerator cannot safely admit the
    /// supplied immutable representation.
    fn decoded_cache_admit(
        &self,
        _key: DecodedCacheKey,
        value: DecodedCacheValue,
    ) -> Result<DecodedCacheAdmission, crate::storage::ObjectStoreError> {
        Ok(DecodedCacheAdmission::Uncached(value))
    }

    /// Admits the same already guarded value without resetting native custody.
    #[cfg(not(target_arch = "wasm32"))]
    fn decoded_cache_admit_with_native_work(
        &self,
        key: DecodedCacheKey,
        value: DecodedCacheValue,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<DecodedCacheAdmission, crate::storage::ObjectStoreError> {
        work.remaining()?;
        self.decoded_cache_admit(key, value)
    }

    /// Asynchronously admits one verified immutable object.
    fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<()>> + StorageFuture;

    /// Admits bytes using the caller's original native work and intrinsic custody.
    #[cfg(not(target_arch = "wasm32"))]
    fn put_with_native_work(
        &self,
        _object_id: ObjectId,
        _bytes: OwnedBytes,
        _work: &crate::materializer::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<(), crate::storage::ObjectStoreError>> + StorageFuture {
        async { Err(crate::storage::ObjectStoreError::NativeUnavailable) }
    }

    /// Asynchronously admits one object whose identity its construction
    /// already proved. A store that verifies digests on admission may skip
    /// hashing the same bytes again; the default verifies through
    /// [`Self::put`].
    fn put_hashed(
        &self,
        object: crate::storage::HashedObject,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<()>> + StorageFuture {
        let (object_id, bytes) = object.into_parts();
        self.put(object_id, bytes, budget, cancellation)
    }

    /// Reuses the constructor's digest proof and physical byte owner.
    #[cfg(not(target_arch = "wasm32"))]
    fn put_hashed_with_native_work(
        &self,
        _object: crate::storage::HashedObject,
        _work: &crate::materializer::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<(), crate::storage::ObjectStoreError>> + StorageFuture {
        async { Err(crate::storage::ObjectStoreError::NativeUnavailable) }
    }

    /// Asynchronously admits an ordered bounded group of verified immutable objects.
    ///
    /// Implementations with a real batch primitive override this method. A
    /// backend without one rejects the operation instead of hiding repeated
    /// single-object operations behind the batch contract.
    fn put_many(
        &self,
        writes: &[ObjectWrite],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<()>> + StorageFuture {
        let _ = (writes, budget, cancellation);
        async {
            Err(crate::storage::ObjectFailure::before_work(
                crate::storage::ObjectStoreError::Rejected(
                    "object backend has no batch-write primitive".to_owned(),
                ),
            ))
        }
    }

    /// Uses one provider batch primitive without resetting the native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn put_many_with_native_work(
        &self,
        _writes: &[ObjectWrite],
        _work: &crate::materializer::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<(), crate::storage::ObjectStoreError>> + StorageFuture {
        async { Err(crate::storage::ObjectStoreError::NativeUnavailable) }
    }

    /// Asynchronously admits an ordered bounded group of objects whose
    /// identities their construction already proved. A store that verifies
    /// digests on admission may skip hashing the same bytes again; the
    /// default verifies through [`Self::put_many`]. `budget` is what is
    /// left after the caller's own allocations, which include one write
    /// per object for this conversion.
    fn put_many_hashed(
        &self,
        objects: &[crate::storage::HashedObject],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<()>> + StorageFuture {
        let writes = objects
            .iter()
            .map(|object| ObjectWrite {
                object_id: object.object_id(),
                bytes: object.bytes().clone(),
            })
            .collect::<Vec<_>>();
        async move { self.put_many(&writes, budget, cancellation).await }
    }

    /// Makes every admitted object in `scope` crash-durable before an
    /// authority record may reference it, and keeps it from collection until
    /// the returned hold drops, which the caller does after writing the
    /// record. Ordinary stores already provide durability from
    /// `put`/`put_many` and never collect; a bounded staging adapter
    /// overrides this boundary to group physical writes and to admit the
    /// closure against a running collection.
    fn flush_before_publish(
        &self,
        _scope: PublicationScope<'_>,
        _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<crate::PublicationHold>> + StorageFuture {
        async {
            Ok(crate::storage::ObjectReceipt {
                value: crate::PublicationHold::none(),
                work: crate::WorkCounters::default(),
            })
        }
    }

    /// Makes the closure durable under the original native work owner.
    #[cfg(not(target_arch = "wasm32"))]
    fn flush_before_publish_with_native_work(
        &self,
        _scope: PublicationScope<'_>,
        _work: &crate::materializer::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<crate::PublicationHold, crate::storage::ObjectStoreError>> + StorageFuture {
        async { Err(crate::storage::ObjectStoreError::NativeUnavailable) }
    }
    /// Prepares the original canonical append after its closure has been flushed.
    /// The publisher keeps its `PublicationHold` live through this call and CAS.
    ///
    /// A hosted provider must durably retain these original values and its exact
    /// object-reference mapping in existing protected custody before returning.
    /// Preparation is not acceptance; unknown append outcomes must be reconciled
    /// against the exact authoritative record, never a later current head.
    /// The default explicitly supplies no hosted or historical custody.
    fn prepare_generation_publication(
        &self,
        _request: &crate::kernel::PublishGenerationRequest,
        _proof: &crate::kernel::GenerationProof,
        _append: &GuardedAppend,
        _scope: PublicationScope<'_>,
        _budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<()>> + StorageFuture {
        async move {
            cancellation.check().map_err(|_| {
                crate::storage::ObjectFailure::before_work(
                    crate::storage::ObjectStoreError::Cancelled,
                )
            })?;
            Ok(crate::storage::ObjectReceipt {
                value: (),
                work: crate::WorkCounters::default(),
            })
        }
    }

    /// Reads original protected proof evidence for exactly this settled record.
    ///
    /// Providers return the SHA256 from their protected original custody, not
    /// from caller-supplied bytes. The archive must have been admitted before
    /// publication and survive collection and ambiguous acceptance. `None`
    /// means this backend has no such capability; it does not authorize a
    /// current-head reconstruction or collection of unread original evidence.
    fn historical_generation_evidence(
        &self,
        _authority_id: crate::AuthorityId,
        _record: &crate::DurableCommit,
        _maximum_bytes: u64,
        _maximum_objects: u64,
        _budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<Option<crate::kernel::HistoricalGenerationEvidence>>> + StorageFuture {
        async move {
            cancellation.check().map_err(|_| {
                crate::storage::ObjectFailure::before_work(
                    crate::storage::ObjectStoreError::Cancelled,
                )
            })?;
            Ok(crate::storage::ObjectReceipt {
                value: None,
                work: crate::WorkCounters::default(),
            })
        }
    }

    /// Prepares those same original values under the caller's native ledger.
    /// The publication hold remains live through this provider callback and CAS.
    /// Unsupported providers fail before I/O, allocation, or publication.
    #[cfg(not(target_arch = "wasm32"))]
    fn prepare_generation_publication_with_native_work(
        &self,
        _request: &crate::kernel::PublishGenerationRequest,
        _proof: &crate::kernel::GenerationProof,
        _append: &crate::GuardedAppend,
        _scope: crate::PublicationScope<'_>,
        _original: &crate::NativeMaterializationWork,
        _cancellation: &crate::CancellationToken,
    ) -> impl Future<Output = Result<(), crate::storage::ObjectStoreError>> + StorageFuture {
        async { Err(crate::storage::ObjectStoreError::NativeUnavailable) }
    }

    /// The collection this store runs while it stays open, if it collects.
    fn collection(&self) -> Option<&Arc<crate::Collection>> {
        None
    }

    /// The count a proof records before it starts, for
    /// [`PublicationScope::Closure`].
    fn collection_sweeps(&self) -> u64 {
        self.collection()
            .map_or(0, |collection| collection.sweeps())
    }

    /// Asynchronously reads one complete bounded object.
    fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<ObjectRead>> + StorageFuture;

    /// Reads using the caller's original native budget, cancellation and journal.
    /// Unsupported custom providers fail before I/O or payload allocation.
    #[cfg(not(target_arch = "wasm32"))]
    fn read_with_native_work(
        &self,
        _object_id: ObjectId,
        _maximum_bytes: u64,
        _work: &crate::materializer::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<NativeObjectRead, crate::storage::ObjectStoreError>> + StorageFuture {
        async { Err(crate::storage::ObjectStoreError::NativeUnavailable) }
    }

    /// Reads through the same provider operation, recording its actual completed
    /// SDK prefix before a caller can decode, fail, or unwind afterward.
    #[doc(hidden)]
    fn read_with_custody(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        observation: Option<ObjectReadObservation<'_>>,
    ) -> impl Future<Output = ObjectResult<ObjectRead>> + StorageFuture {
        async move {
            cancellation.check().map_err(|_| crate::storage::ObjectFailure::before_work(
                crate::storage::ObjectStoreError::Cancelled,
            ))?;
            let standing = observation.map_or(0, ObjectReadObservation::standing_bytes);
            let remaining = match observation {
                Some(observation) => observation.remaining_budget(budget, standing)?,
                None => budget,
            };
            let result = self.read(object_id, maximum_bytes, remaining, cancellation).await;
            let result = match observation {
                Some(observation) => observation.complete(result),
                None => result,
            };
            ObjectReadObservation::scope_result(result, standing, budget)
        }
    }

    /// Asynchronously reads an ordered batch of complete bounded objects.
    ///
    /// Backends with transactional multi-get or batched I/O should override
    /// this method. The default is cancellation-aware and retains exact
    /// per-object work rather than pretending sequential execution was one
    /// physical operation.
    fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<Vec<ObjectRead>>> + StorageFuture;

    /// Reads the same actual backend frontier against the original native journal.
    #[cfg(not(target_arch = "wasm32"))]
    fn read_many_with_native_work(
        &self,
        _requests: &[ObjectReadRequest],
        _work: &crate::materializer::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<NativeObjectReadBatch, crate::storage::ObjectStoreError>> + StorageFuture {
        async { Err(crate::storage::ObjectStoreError::NativeUnavailable) }
    }

    /// Asynchronously probes one exact object identity.
    fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<bool>> + StorageFuture;
}

/// Explicit opt-in for synchronous stores that are safe to complete inline.
///
/// This marker also admits the synchronous kernel convenience functions.
/// Blocking native backends must own bounded async dispatch internally.
pub trait ImmediateObjectStore: ObjectStore {
    /// Admits one object through the original native accounting engine.
    #[cfg(not(target_arch = "wasm32"))]
    fn put_with_native_work(
        &self, _object_id: ObjectId, _bytes: OwnedBytes,
        _work: &crate::materializer::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> Result<(), crate::storage::ObjectStoreError> {
        Err(crate::storage::ObjectStoreError::NativeUnavailable)
    }

    /// Reuses an already proved immutable digest.
    #[cfg(not(target_arch = "wasm32"))]
    fn put_hashed_with_native_work(
        &self, _object: crate::storage::HashedObject,
        _work: &crate::materializer::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> Result<(), crate::storage::ObjectStoreError> {
        Err(crate::storage::ObjectStoreError::NativeUnavailable)
    }

    /// Admits one native batch through the provider's batch primitive.
    #[cfg(not(target_arch = "wasm32"))]
    fn put_many_with_native_work(
        &self, _writes: &[ObjectWrite],
        _work: &crate::materializer::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> Result<(), crate::storage::ObjectStoreError> {
        Err(crate::storage::ObjectStoreError::NativeUnavailable)
    }

    /// Completes the native durability boundary without a receipt ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn flush_before_publish_with_native_work(
        &self, _scope: PublicationScope<'_>,
        _work: &crate::materializer::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> Result<crate::PublicationHold, crate::storage::ObjectStoreError> {
        Err(crate::storage::ObjectStoreError::NativeUnavailable)
    }

    /// Performs the actual provider preparation with the original native ledger.
    /// Unlike the ordinary default, missing native support never authorizes CAS.
    #[cfg(not(target_arch = "wasm32"))]
    fn prepare_generation_publication_with_native_work(
        &self,
        _request: &crate::kernel::PublishGenerationRequest,
        _proof: &crate::kernel::GenerationProof,
        _append: &crate::GuardedAppend,
        _scope: crate::PublicationScope<'_>,
        _original: &crate::NativeMaterializationWork,
        _cancellation: &crate::CancellationToken,
    ) -> Result<(), crate::storage::ObjectStoreError> {
        Err(crate::storage::ObjectStoreError::NativeUnavailable)
    }
}

impl<T: ImmediateObjectStore + ?Sized> ImmediateObjectStore for Arc<T> {
    #[cfg(not(target_arch = "wasm32"))]
    fn put_with_native_work(
        &self, object_id: ObjectId, bytes: OwnedBytes,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), crate::storage::ObjectStoreError> {
        ImmediateObjectStore::put_with_native_work(&**self, object_id, bytes, work, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn put_hashed_with_native_work(
        &self, object: crate::storage::HashedObject,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), crate::storage::ObjectStoreError> {
        ImmediateObjectStore::put_hashed_with_native_work(&**self, object, work, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn put_many_with_native_work(
        &self, writes: &[ObjectWrite],
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), crate::storage::ObjectStoreError> {
        ImmediateObjectStore::put_many_with_native_work(&**self, writes, work, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn flush_before_publish_with_native_work(
        &self, scope: PublicationScope<'_>,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::PublicationHold, crate::storage::ObjectStoreError> {
        ImmediateObjectStore::flush_before_publish_with_native_work(&**self, scope, work, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn prepare_generation_publication_with_native_work(
        &self, request: &crate::kernel::PublishGenerationRequest,
        proof: &crate::kernel::GenerationProof, append: &crate::GuardedAppend,
        scope: crate::PublicationScope<'_>, original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), crate::storage::ObjectStoreError> {
        ImmediateObjectStore::prepare_generation_publication_with_native_work(
            &**self, request, proof, append, scope, original, cancellation,
        )
    }
}

/// Explicit sequential implementation for adapters whose physical backend has
/// no multi-get primitive. Backends must opt into this implementation rather
/// than silently inheriting it.
///
/// # Errors
///
/// Returns an empty-batch rejection, cancellation, budget failure, allocation
/// failure, or the first exact object-read failure with all spent work.
pub async fn read_many_sequential_async<S: AsyncObjectStore + ?Sized>(
    store: &S,
    requests: &[ObjectReadRequest],
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> ObjectResult<Vec<ObjectRead>> {
    let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
        budget, cancellation,
    );
    let result = read_many_sequential_engine(store, requests, &mut accounting, cancellation).await;
    let work = accounting.spent();
    match result {
        Ok(values) => Ok(crate::storage::ObjectReceipt { value: values.reads, work }),
        Err(error) => Err(crate::storage::ObjectFailure::new(
            sequential_read_error(error), work,
        )),
    }
}

/// Explicit sequential native adapter using the caller's original work and
/// allocation custody. Each underlying scalar read remains a scalar read.
#[cfg(not(target_arch = "wasm32"))]
pub async fn read_many_sequential_with_native_work<S: AsyncObjectStore + ?Sized>(
    store: &S,
    requests: &[ObjectReadRequest],
    work: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<NativeObjectReadBatch, crate::storage::ObjectStoreError> {
    let mut accounting = crate::kernel::allocation::KernelAccounting::native(work, cancellation);
    let result = read_many_sequential_engine(store, requests, &mut accounting, cancellation)
        .await.map_err(sequential_read_error)?;
    match result.allocation {
        crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) => {
            NativeObjectReadBatch::new(result.reads, allocation)
        }
        crate::kernel::allocation::KernelOwnedAllocation::Standalone => {
            drop(result.reads);
            Err(crate::storage::ObjectStoreError::Corrupt)
        }
    }
}

struct SequentialObjectReads {
    reads: Vec<ObjectRead>,
    allocation: crate::kernel::allocation::KernelOwnedAllocation,
}

async fn read_many_sequential_engine<S: AsyncObjectStore + ?Sized>(
    store: &S,
    requests: &[ObjectReadRequest],
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<SequentialObjectReads, crate::FsError> {
    accounting.check()?;
    if requests.is_empty() {
        return Err(crate::storage::ObjectStoreError::EmptyReadBatch.into());
    }
    let item_count = u64::try_from(requests.len())
        .map_err(|_| crate::performance::WorkError::Overflow)?;
    let requested_bytes = requests.len().checked_mul(size_of::<ObjectRead>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(crate::performance::WorkError::Overflow)?;
    accounting.admit(crate::performance::WorkCounters {
        items_examined: item_count,
        items_returned: item_count,
        ..crate::performance::WorkCounters::UNCHARGED
    })?;
    accounting.charge(crate::performance::WorkCounters {
        items_examined: item_count,
        ..crate::performance::WorkCounters::UNCHARGED
    })?;
    let allocation = accounting.claim_owned(requested_bytes, 1)?;
    let mut values = SequentialObjectReads { reads: Vec::new(), allocation };
    values.reads.try_reserve_exact(requests.len())
        .map_err(crate::storage::ObjectStoreError::MemoryTableAllocationFailed)?;
    let actual_bytes = values.reads.capacity().checked_mul(size_of::<ObjectRead>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(crate::performance::WorkError::Overflow)?;
    accounting.reconcile_owned(&mut values.allocation, requested_bytes, actual_bytes)?;
    for request in requests {
        accounting.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = accounting.native_work() {
            let read = store.read_with_native_work(
                request.object_id, request.maximum_bytes, work, cancellation,
            ).await?;
            values.reads.push(read.into_read());
            continue;
        }
        let result = store.read(
            request.object_id, request.maximum_bytes, accounting.remaining()?, cancellation,
        ).await;
        // A completed provider receipt must be recorded even if cancellation
        // became visible during the read; merging itself is not a new operation.
        match result {
            Ok(receipt) => {
                let retained = match receipt.value.retention {
                    crate::storage::ObjectReadRetention::Shared => 0,
                    crate::storage::ObjectReadRetention::Owned { logical_bytes } => logical_bytes,
                };
                merge_completed_object_read(accounting, receipt.work, retained)?;
                values.reads.push(receipt.value);
            }
            Err(failure) => {
                merge_completed_object_read(accounting, *failure.work, 0)?;
                return Err(failure.error.into());
            }
        }
    }
    accounting.charge(crate::performance::WorkCounters {
        items_returned: item_count,
        ..crate::performance::WorkCounters::UNCHARGED
    })?;
    Ok(values)
}

pub(crate) fn merge_completed_object_read(
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    completed: crate::performance::WorkCounters,
    retained: u64,
) -> Result<(), crate::FsError> {
    fn merge(
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut crate::performance::WorkCounters,
        budget: WorkBudget,
        completed: crate::performance::WorkCounters,
        retained: u64,
    ) -> Result<(), crate::FsError> {
        let peak = allocations.live_bytes().checked_add(completed.peak_allocation_bytes)
            .ok_or(crate::performance::WorkError::Overflow)?;
        let mut combined = work.checked_add(completed)?;
        combined.peak_allocation_bytes = combined.peak_allocation_bytes.max(peak);
        *work = combined;
        allocations.record_owned_bytes(retained, work)
            .map_err(|error| error.into_work_or(crate::FsError::HandleAllocationFailed))?;
        work.verify(budget)?;
        Ok(())
    }
    match accounting {
        crate::kernel::allocation::KernelAccounting::Standalone {
            allocations, work, budget, ..
        } => merge(allocations, work, *budget, completed, retained),
        crate::kernel::allocation::KernelAccounting::BorrowedStandalone {
            allocations, work, budget, ..
        } => merge(allocations, work, *budget, completed, retained),
        #[cfg(not(target_arch = "wasm32"))]
        crate::kernel::allocation::KernelAccounting::NativeOriginal { .. } => {
            Err(crate::storage::ObjectStoreError::Corrupt.into())
        }
    }
}

pub(crate) fn sequential_read_error(error: crate::FsError) -> crate::storage::ObjectStoreError {
    match error {
        crate::FsError::Object(error) => error,
        crate::FsError::Work(error) => crate::storage::ObjectStoreError::Work(error),
        crate::FsError::Cancelled(_) => crate::storage::ObjectStoreError::Cancelled,
        #[cfg(not(target_arch = "wasm32"))]
        crate::FsError::NativeKernel(error) => crate::storage::ObjectStoreError::Native(error),
        crate::FsError::HandleAllocationFailed => crate::storage::ObjectStoreError::SdkAllocationFailed,
        _ => crate::storage::ObjectStoreError::Corrupt,
    }
}

impl<T: ImmediateAuthorityStore + ?Sized> AsyncAuthorityStore for T {
    async fn publication_reservation(&self, authority: AuthorityId, operation: OperationId, budget: WorkBudget, cancellation: &CancellationToken) -> AuthorityResult<Option<PublicationReservation>> {
        ImmediateAuthorityStore::publication_reservation(self, authority, operation, budget, cancellation)
    }
    async fn reserve_publication(&self, authority: AuthorityId, expected: Head, operation: OperationId, budget: WorkBudget, cancellation: &CancellationToken) -> AuthorityResult<ReservationOutcome> {
        ImmediateAuthorityStore::reserve_publication(self, authority, expected, operation, budget, cancellation)
    }
    async fn release_publication(&self, reservation: PublicationReservation, budget: WorkBudget, cancellation: &CancellationToken) -> AuthorityResult<()> {
        ImmediateAuthorityStore::release_publication(self, reservation, budget, cancellation)
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn publication_reservation_with_native_work(&self, authority: AuthorityId, operation: OperationId, original: &crate::NativeMaterializationWork, cancellation: &CancellationToken) -> Result<Option<PublicationReservation>, crate::storage::AuthorityStoreError> {
        ImmediateAuthorityStore::publication_reservation_with_native_work(self, authority, operation, original, cancellation)
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn reserve_publication_with_native_work(&self, authority: AuthorityId, expected: Head, operation: OperationId, original: &crate::NativeMaterializationWork, cancellation: &CancellationToken) -> Result<PublicationReservation, crate::storage::AuthorityStoreError> {
        ImmediateAuthorityStore::reserve_publication_with_native_work(self, authority, expected, operation, original, cancellation)
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn release_publication_with_native_work(&self, reservation: PublicationReservation, original: &crate::NativeMaterializationWork, cancellation: &CancellationToken) -> Result<(), crate::storage::AuthorityStoreError> {
        ImmediateAuthorityStore::release_publication_with_native_work(self, reservation, original, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn commit_workspace_fork_guarded_with_native_work<'a, E, F, Fut>(
        &self, fork: WorkspaceForkCommit, permit: PublicationPermit,
        original: &'a crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
    ) -> Result<WorkspaceForkOutcome, E>
    where E: From<crate::FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static {
        ImmediateAuthorityStore::commit_workspace_fork_guarded_with_native_work(
            self, fork, permit, original, cancellation, guard,
        ).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn create_authority_guarded_with_native_work<'a, E, F, Fut>(
        &self, authority: AuthorityId, epoch: Epoch,
        original: &'a crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
    ) -> Result<CreateAuthorityOutcome, E>
    where E: From<crate::FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static {
        ImmediateAuthorityStore::create_authority_guarded_with_native_work(
            self, authority, epoch, original, cancellation, guard,
        ).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn create_authority_with_first_record_guarded_with_native_work<'a, E, F, Fut>(
        &self, authority: AuthorityId, commit: ProposedCommit,
        original: &'a crate::NativeMaterializationWork, cancellation: &CancellationToken, guard: &mut F,
    ) -> Result<bool, E>
    where E: From<crate::FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static {
        ImmediateAuthorityStore::create_authority_with_first_record_guarded_with_native_work(
            self, authority, commit, original, cancellation, guard,
        ).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn create_authority_with_native_work(
        &self, authority: AuthorityId, epoch: Epoch,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<CreateAuthorityOutcome, crate::storage::AuthorityStoreError> {
        ImmediateAuthorityStore::create_authority_with_native_work(self, authority, epoch, original, cancellation)
    }

    async fn create_authority_with_first_record(
        &self, authority: AuthorityId, commit: ProposedCommit, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<bool> {
        ImmediateAuthorityStore::create_authority_with_first_record(self, authority, commit, budget, cancellation).await
    }

    async fn create_authority_with_first_record_guarded<F, Fut>(
        &self, authority: AuthorityId, commit: ProposedCommit, budget: WorkBudget,
        cancellation: &CancellationToken, guard: &mut F,
    ) -> crate::facade::FsResult<bool>
    where
        F: FnMut(WorkBudget) -> Fut + Send,
        Fut: Future<Output = crate::facade::FsResult<()>> + Send,
    {
        ImmediateAuthorityStore::create_authority_with_first_record_guarded(self, authority, commit, budget, cancellation, guard).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn create_authority_with_first_record_with_native_work(
        &self, authority: AuthorityId, commit: ProposedCommit,
        original: &crate::NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<bool, crate::storage::AuthorityStoreError> {
        ImmediateAuthorityStore::create_authority_with_first_record_with_native_work(self, authority, commit, original, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn compare_and_append_guarded_with_native_work(
        &self, request: GuardedAppend, original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(AppendOutcome, crate::NativeAllocation), crate::storage::AuthorityStoreError> {
        ImmediateAuthorityStore::compare_and_append_guarded_with_native_work(self, request, original, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn compare_and_append_guarded_with_native_owner<F, Fut, E>(
        &self, request: GuardedAppend, original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken, guard: &mut F,
    ) -> Result<(AppendOutcome, crate::NativeAllocation), E>
    where
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<crate::storage::AuthorityStoreError> + Send + 'static,
    {
        ImmediateAuthorityStore::compare_and_append_guarded_with_native_owner(self, request, original, cancellation, guard).await
    }

    async fn fork_generation_authority(
        &self,
        source: GenerationForkSource,
        destination_authority: AuthorityId,
        _operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        cancellation.check().map_err(|_| {
            crate::storage::AuthorityFailure::before_work(
                crate::storage::AuthorityStoreError::Cancelled,
            )
        })?;
        if source.lineage == GenerationFork::PublishedPrefix {
            return Err(crate::storage::AuthorityFailure::before_work(
                crate::storage::AuthorityStoreError::Rejected(
                    "immediate authority stores do not retain generation lineage prefixes"
                        .to_owned(),
                ),
            ));
        }
        AuthorityStore::create_authority(self, destination_authority, Epoch::GENESIS, budget)
    }

    async fn create_authority(
        &self,
        authority_id: AuthorityId,
        genesis_epoch: Epoch,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        cancellation.check().map_err(|_| {
            crate::storage::AuthorityFailure::before_work(
                crate::storage::AuthorityStoreError::Cancelled,
            )
        })?;
        AuthorityStore::create_authority(self, authority_id, genesis_epoch, budget)
    }

    async fn head(
        &self,
        authority_id: AuthorityId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Head> {
        cancellation.check().map_err(|_| {
            crate::storage::AuthorityFailure::before_work(
                crate::storage::AuthorityStoreError::Cancelled,
            )
        })?;
        AuthorityStore::head(self, authority_id, budget)
    }

    async fn compare_and_append(
        &self,
        authority_id: AuthorityId,
        epoch: Epoch,
        expected: Head,
        commit: ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<AppendOutcome> {
        cancellation.check().map_err(|_| {
            crate::storage::AuthorityFailure::before_work(
                crate::storage::AuthorityStoreError::Cancelled,
            )
        })?;
        AuthorityStore::compare_and_append(self, authority_id, epoch, expected, commit, budget)
    }

    async fn replay(
        &self,
        authority_id: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Vec<crate::foundation::DurableCommit>> {
        cancellation.check().map_err(|_| {
            crate::storage::AuthorityFailure::before_work(
                crate::storage::AuthorityStoreError::Cancelled,
            )
        })?;
        AuthorityStore::replay(self, authority_id, after, limit, budget)
    }

    async fn fence(
        &self,
        authority_id: AuthorityId,
        expected: Head,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<FenceOutcome> {
        cancellation.check().map_err(|_| {
            crate::storage::AuthorityFailure::before_work(
                crate::storage::AuthorityStoreError::Cancelled,
            )
        })?;
        AuthorityStore::fence(self, authority_id, expected, budget)
    }

    async fn find_operation(
        &self,
        authority_id: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Option<crate::foundation::DurableCommit>> {
        cancellation.check().map_err(|_| {
            crate::storage::AuthorityFailure::before_work(
                crate::storage::AuthorityStoreError::Cancelled,
            )
        })?;
        AuthorityStore::find_operation(self, authority_id, operation_id, budget)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn head_with_native_work(
        &self, authority_id: AuthorityId,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<Head, crate::storage::AuthorityStoreError> {
        ImmediateAuthorityStore::head_with_native_work(self, authority_id, work, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn replay_with_native_work(
        &self, authority_id: AuthorityId, after: Sequence, limit: ReplayLimit,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(Vec<crate::foundation::DurableCommit>, crate::materializer::NativeAllocation), crate::storage::AuthorityStoreError> {
        ImmediateAuthorityStore::replay_with_native_work(self, authority_id, after, limit, work, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn fence_with_native_work(
        &self, authority_id: AuthorityId, expected: Head,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<FenceOutcome, crate::storage::AuthorityStoreError> {
        ImmediateAuthorityStore::fence_with_native_work(self, authority_id, expected, work, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn find_operation_with_native_work(
        &self, authority_id: AuthorityId, operation_id: OperationId,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(Option<crate::foundation::DurableCommit>, crate::materializer::NativeAllocation), crate::storage::AuthorityStoreError> {
        ImmediateAuthorityStore::find_operation_with_native_work(self, authority_id, operation_id, work, cancellation)
    }
}

impl<T: ImmediateObjectStore + ?Sized> AsyncObjectStore for T {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation.check().map_err(|_| {
            crate::storage::ObjectFailure::before_work(crate::storage::ObjectStoreError::Cancelled)
        })?;
        ObjectStore::put(self, object_id, bytes, budget)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn put_with_native_work(
        &self, object_id: ObjectId, bytes: OwnedBytes,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), crate::storage::ObjectStoreError> {
        ImmediateObjectStore::put_with_native_work(self, object_id, bytes, work, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn put_hashed_with_native_work(
        &self, object: crate::storage::HashedObject,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), crate::storage::ObjectStoreError> {
        ImmediateObjectStore::put_hashed_with_native_work(self, object, work, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn put_many_with_native_work(
        &self, writes: &[ObjectWrite],
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), crate::storage::ObjectStoreError> {
        ImmediateObjectStore::put_many_with_native_work(self, writes, work, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn flush_before_publish_with_native_work(
        &self, scope: PublicationScope<'_>,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::PublicationHold, crate::storage::ObjectStoreError> {
        ImmediateObjectStore::flush_before_publish_with_native_work(self, scope, work, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn prepare_generation_publication_with_native_work(
        &self, request: &crate::kernel::PublishGenerationRequest,
        proof: &crate::kernel::GenerationProof, append: &crate::GuardedAppend,
        scope: crate::PublicationScope<'_>, original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), crate::storage::ObjectStoreError> {
        ImmediateObjectStore::prepare_generation_publication_with_native_work(
            self, request, proof, append, scope, original, cancellation,
        )
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation.check().map_err(|_| {
            crate::storage::ObjectFailure::before_work(crate::storage::ObjectStoreError::Cancelled)
        })?;
        ObjectStore::put_many(self, writes, budget)
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        cancellation.check().map_err(|_| {
            crate::storage::ObjectFailure::before_work(crate::storage::ObjectStoreError::Cancelled)
        })?;
        ObjectStore::read(self, object_id, maximum_bytes, budget)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_with_native_work(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<NativeObjectRead, crate::storage::ObjectStoreError> {
        ObjectStore::read_with_native_work(self, object_id, maximum_bytes, work, cancellation)
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        cancellation.check().map_err(|_| {
            crate::storage::ObjectFailure::before_work(crate::storage::ObjectStoreError::Cancelled)
        })?;
        ObjectStore::read_many(self, requests, budget)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_many_with_native_work(
        &self,
        requests: &[ObjectReadRequest],
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<NativeObjectReadBatch, crate::storage::ObjectStoreError> {
        ObjectStore::read_many_with_native_work(self, requests, work, cancellation)
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        cancellation.check().map_err(|_| {
            crate::storage::ObjectFailure::before_work(crate::storage::ObjectStoreError::Cancelled)
        })?;
        ObjectStore::contains(self, object_id, budget)
    }
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/async_storage.rs"]
mod tests;
