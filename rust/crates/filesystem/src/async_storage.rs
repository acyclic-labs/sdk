//! Runtime-neutral asynchronous storage contracts for browser and remote use.

use crate::cancellation::CancellationToken;
use crate::foundation::{
    AuthorityId, Epoch, GenerationId, Head, OperationId, ProposedCommit, Sequence,
};
use crate::performance::{WorkBudget, WorkCounters};
use crate::storage::{
    AppendOutcome, AuthorityResult, CreateAuthorityOutcome, FenceOutcome,
    GuardedAppend, ObjectId, ObjectRead, ObjectReadRequest, ObjectResult, ObjectStore, ObjectWrite,
    PublicationPermit, PublicationReservation, ReplayLimit, ReservationOutcome,
};
use acyclic_stream::{OwnedBytes, ProviderPlatform, ProviderTask};
use std::any::{Any, TypeId};
use std::future::Future;
use std::mem::size_of;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn native_read_batch(
    reads: Vec<ObjectRead>, allocation: crate::materializer::NativeAllocation,
) -> Result<(Vec<ObjectRead>, crate::materializer::NativeAllocation), crate::storage::ObjectStoreError> {
    let bytes = reads.capacity().checked_mul(size_of::<ObjectRead>())
        .and_then(|bytes| u64::try_from(bytes).ok());
    if bytes != Some(allocation.bytes()) {
        drop(reads);
        drop(allocation);
        return Err(crate::storage::ObjectStoreError::Corrupt);
    }
    Ok((reads, allocation))
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn owned_read(
    read: ObjectRead, original: &crate::materializer::NativeMaterializationWork,
) -> Result<ObjectRead, crate::storage::ObjectStoreError> {
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
    Ok(read)
}

/// Opaque SDK observation for one actual object-read receipt.
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

/// Polls a future exactly once for adapters whose contract guarantees that no asynchronous suspension is possible.
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

/// A new workspace's complete authority state: its own authority, holding its creation record, and the retention authority that keeps its source generation alive while the workspace may depend on it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceForkCommit {
    /// Source lineage the destination authority starts from, on backends that keep generation lineage; `None` starts it empty.
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
    /// Both authorities hold exactly the requested first records, now or from an earlier attempt.
    Committed,
    /// The retention authority already holds a different first record.
    RetentionConflict,
    /// The destination authority already holds a different first record.
    CreationRejected,
}

/// Largest first record a fork compares when resolving a retry.
const MAXIMUM_FIRST_RECORD_BYTES: u64 = 4 * 1024;

pub(crate) async fn seal<G: AuthorityGuard>(guard: Option<&mut G>, work: &mut AuthorityWork<'_>)
    -> Result<(), AuthorityError<G::Error>> {
    if let Some(guard) = guard { guard.seal(work).await?; }
    Ok(())
}

pub(crate) async fn workspace_fork<S: AsyncAuthorityStore + ?Sized, G: AuthorityGuard>(
    store: &S, fork: WorkspaceForkCommit, permit: PublicationPermit, work: &mut AuthorityWork<'_>, mut guard: Option<&mut G>,
) -> Result<WorkspaceForkOutcome, AuthorityError<G::Error>> {
    #[cfg(not(target_arch = "wasm32"))]
    if work.original.is_some() { work.check()?; }
    if permit != PublicationPermit::Unrestricted {
        return Err(crate::AuthorityStoreError::RejectedStatic("authority backend cannot atomically evaluate fork publication leases").into());
    }
    let WorkspaceForkCommit { lineage, destination, creation, retention, retained } = fork;
    #[cfg(not(target_arch = "wasm32"))]
    let native = work.original.is_some();
    #[cfg(target_arch = "wasm32")]
    let native = false;
    let retained = if native {
        store.create_first_record(retention, retained, work, None::<&mut ()>).await
    } else {
        first_record(store, retention, retained, work, None::<&mut ()>).await
    }.map_err(|error| AuthorityError::Authority(error.into_error(std::convert::identity)))?;
    if !retained { return Ok(WorkspaceForkOutcome::RetentionConflict); }
    match lineage {
        Some(source) => store.fork_authority(source, destination, creation.operation_id, work, guard.as_deref_mut()).await?,
        None => store.create_authority(destination, Epoch::GENESIS, work, guard.as_deref_mut()).await?,
    };
    let created = if native || guard.is_some() {
        store.create_first_record(destination, creation, work, guard).await?
    } else {
        first_record(store, destination, creation, work, None::<&mut ()>).await
            .map_err(|error| AuthorityError::Authority(error.into_error(std::convert::identity)))?
    };
    Ok(if created { WorkspaceForkOutcome::Committed } else { WorkspaceForkOutcome::CreationRejected })
}

pub(crate) async fn first_record<S: AsyncAuthorityStore + ?Sized, G: AuthorityGuard>(
    store: &S, authority: AuthorityId, commit: ProposedCommit, work: &mut AuthorityWork<'_>, mut guard: Option<&mut G>,
) -> Result<bool, AuthorityError<G::Error>> {
    let created =store.create_authority(authority, Epoch::GENESIS, work, guard.as_deref_mut()).await?;
    let head = match created { CreateAuthorityOutcome::Created(head) | CreateAuthorityOutcome::Existing(head) => head };
    let identity = (commit.operation_id, commit.fingerprint);
    seal(guard.as_deref_mut(), work).await?;
    let (appended, allocation) = store.compare_and_append(GuardedAppend {
        authority_id: authority, epoch: head.epoch, expected: Head::genesis(head.epoch), commit,
        permit: PublicationPermit::Unrestricted,
    }, work, None::<&mut ()>).await.map_err(|error| AuthorityError::Authority(error.into_error(std::convert::identity)))?;
    let retry = matches!(&appended, AppendOutcome::AlreadyCommitted(_) | AppendOutcome::Conflict { .. });
    let matches = match appended {
        AppendOutcome::Committed(_) | AppendOutcome::AlreadyCommitted(_) => true,
        AppendOutcome::Conflict { .. } => {
            let (first, _allocation) = store.replay(authority, Sequence::GENESIS,
                ReplayLimit { records: 1, payload_bytes: MAXIMUM_FIRST_RECORD_BYTES }, work).await?;
            first.first().is_some_and(|record| record.sequence == Sequence::new(1)
                && (record.operation_id, record.fingerprint) == identity)
        }
        AppendOutcome::Fenced { .. } | AppendOutcome::IdempotencyConflict { .. } => false,
    };
    drop(allocation);
    if matches && retry { seal(guard.as_deref_mut(), work).await?; }
    Ok(matches)
}

/// Work shared by authority operations in one budget or native ledger.
pub struct AuthorityWork<'a> {
    pub(crate) work: WorkCounters,
    pub(crate) budget: WorkBudget,
    pub(crate) cancellation: &'a CancellationToken,
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) original: Option<&'a crate::NativeMaterializationWork>,
}

impl<'a> AuthorityWork<'a> {
    /// Starts authority work against a bounded budget.
    pub fn new(budget: WorkBudget, cancellation: &'a CancellationToken) -> Self {
        Self { work: WorkCounters::UNCHARGED, budget, cancellation,
            #[cfg(not(target_arch = "wasm32"))] original: None }
    }

    /// Uses the caller's existing native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn native(original: &'a crate::NativeMaterializationWork, cancellation: &'a CancellationToken) -> Self {
        Self { original: Some(original), ..Self::new(WorkBudget::UNBOUNDED, cancellation) }
    }

    /// Returns actual completed work.
    pub fn spent(&self) -> WorkCounters {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original { return original.receipt(); }
        self.work
    }

    /// Returns the remaining operation budget.
    pub fn remaining(&self) -> Result<WorkBudget, crate::AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original { return original.remaining().map_err(Into::into); }
        self.work.remaining(self.budget).map_err(Into::into)
    }

    /// Attaches actual work to a completed operation.
    pub fn receipt<T, E>(&self, result: Result<T, E>) -> crate::performance::MeasuredResult<crate::performance::OperationReceipt<T>, E> {
        result.map(|value| crate::performance::OperationReceipt { value, work: self.spent() })
            .map_err(|error| crate::performance::OperationFailure::new(error, self.spent()))
    }

    pub(crate) fn check(&self) -> Result<(), crate::AuthorityStoreError> {
        self.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = self.original { return original.admit(WorkCounters::UNCHARGED).map_err(Into::into); }
        self.work.verify(self.budget).map_err(Into::into)
    }

    pub(crate) fn record(&mut self, delta: WorkCounters) -> Result<(), crate::AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if self.original.is_some() { return Ok(()); }
        self.work = self.work.checked_add(delta)?;
        Ok(())
    }

    pub(crate) fn accept<T>(&mut self, result: AuthorityResult<T>) -> Result<T, crate::AuthorityStoreError> {
        let delta = match &result { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
        self.record(delta)?;
        result.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }
}

/// Publication failed in the authority provider or its owner guard.
#[derive(Debug)]
pub enum AuthorityError<E> {
    /// The authority provider rejected the operation.
    Authority(crate::AuthorityStoreError),
    /// The owner guard rejected the operation.
    Owner(E),
}

impl<E> AuthorityError<E> {
    /// Preserves owner errors while converting provider failures.
    pub fn into_error(self, convert: impl FnOnce(crate::AuthorityStoreError) -> E) -> E {
        match self { Self::Authority(error) => convert(error), Self::Owner(error) => error }
    }
}

impl<E> From<crate::AuthorityStoreError> for AuthorityError<E> {
    fn from(error: crate::AuthorityStoreError) -> Self { Self::Authority(error) }
}

#[cfg(not(target_arch = "wasm32"))]
impl<E> From<acyclic_stream::StreamError> for AuthorityError<E> {
    fn from(error: acyclic_stream::StreamError) -> Self { Self::Authority(error.into()) }
}

/// Seals authority publication after planning and during recovery.
pub trait AuthorityGuard: ProviderTask {
    /// The owner's rejection type.
    type Error: Send;
    /// Checks the current owner against the same operation work.
    fn seal(&mut self, work: &mut AuthorityWork<'_>)
        -> impl Future<Output = Result<(), AuthorityError<Self::Error>>> + ProviderTask;

    /// Seals the provider's actual native commit attempt.
    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit<P: acyclic_stream::StreamProvider>(
        &mut self, provider: &P, request: acyclic_stream::CommitRequest, deadline: Option<u64>,
        work: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = acyclic_stream::NativeStreamResult<acyclic_stream::CommitOutcome, AuthorityError<Self::Error>>> + Send {
        async move {
            if let Err(error) = self.seal(work).await { return Ok(Err(error)); }
            let Some(original) = work.original else { return Ok(Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into())); };
            let meter = original.native_read_meter().clone();
            let result = match deadline {
                Some(deadline) => provider.native_commit_before(request, deadline, meter).await,
                None => provider.native_commit(request, meter).await,
            };
            result.map(|result| result.map_err(AuthorityError::from))
        }
    }
}

impl AuthorityGuard for () {
    type Error = crate::AuthorityStoreError;
    async fn seal(&mut self, _: &mut AuthorityWork<'_>) -> Result<(), AuthorityError<Self::Error>> { Ok(()) }
}

impl<F, Fut> AuthorityGuard for &mut F
where F: FnMut(WorkBudget) -> Fut + ProviderTask, Fut: Future<Output = crate::FsResult<()>> + ProviderTask {
    type Error = crate::FsError;
    async fn seal(&mut self, work: &mut AuthorityWork<'_>) -> Result<(), AuthorityError<Self::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
        let result = self(work.remaining()?).await;
        match result {
            Ok(receipt) => {
                work.record(receipt.work)?;
                work.work.verify(work.budget).map_err(crate::AuthorityStoreError::from)?;
                Ok(())
            }
            Err(failure) => {
                match work.work.checked_add(*failure.work) {
                    Ok(combined) => { work.work = combined; Err(AuthorityError::Owner(failure.error)) }
                    Err(error) => Err(AuthorityError::Owner(crate::FsError::from(error))),
                }
            }
        }
    }
}

/// Owns a native guard for provider retries after the caller yields.
#[cfg(not(target_arch = "wasm32"))]
pub struct NativeAuthorityGuard<F>(#[doc = "The owner callback."] pub F);

#[cfg(not(target_arch = "wasm32"))]
impl<E, F, Fut> AuthorityGuard for NativeAuthorityGuard<F>
where E: Send + 'static, F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static {
    type Error = E;
    async fn seal(&mut self, work: &mut AuthorityWork<'_>) -> Result<(), AuthorityError<E>> {
        let Some(original) = work.original else { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); };
        (self.0)(original).await.map_err(AuthorityError::Owner)
    }

    async fn native_commit<P: acyclic_stream::StreamProvider>(
        &mut self, provider: &P, request: acyclic_stream::CommitRequest, deadline: Option<u64>,
        work: &mut AuthorityWork<'_>,
    ) -> acyclic_stream::NativeStreamResult<acyclic_stream::CommitOutcome, AuthorityError<E>> {
        let Some(original) = work.original else { return Ok(Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into())); };
        let meter = original.native_read_meter().clone();
        let original = original.clone();
        let mut callback = self.0.clone();
        let before = move || {
            let future = callback(&original);
            async move { future.await.map_err(AuthorityError::Owner) }
        };
        provider.native_commit_guarded(request, deadline,
            meter, before).await
    }
}

/// Retains an authority result's actual newly allocated native storage.
#[cfg(not(target_arch = "wasm32"))]
pub type AuthorityAllocation = Option<crate::NativeAllocation>;
/// Browser authority results need no native allocation guard.
#[cfg(target_arch = "wasm32")]
pub type AuthorityAllocation = Option<()>;

/// Nonblocking authority-store contract.
pub trait AsyncAuthorityStore: ProviderPlatform {
    /// Whether creation can retain a published-generation prefix.
    fn supports_generation_lineage_prefix(&self) -> bool { false }

    /// Creates an authority, sealing publication and recovery.
    fn create_authority<G: AuthorityGuard>(
        &self, authority: AuthorityId, epoch: Epoch, work: &mut AuthorityWork<'_>, guard: Option<&mut G>,
    ) -> impl Future<Output = Result<CreateAuthorityOutcome, AuthorityError<G::Error>>> + ProviderTask;

    /// Creates an authority rooted at one source generation.
    fn fork_authority<G: AuthorityGuard>(
        &self, source: GenerationForkSource, destination: AuthorityId, operation: OperationId,
        work: &mut AuthorityWork<'_>, mut guard: Option<&mut G>,
    ) -> impl Future<Output = Result<CreateAuthorityOutcome, AuthorityError<G::Error>>> + ProviderTask {
        async move {
            if source.lineage == GenerationFork::PublishedPrefix {
                #[cfg(not(target_arch = "wasm32"))]
                if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
                seal(guard.as_deref_mut(), work).await?;
                return Err(crate::AuthorityStoreError::RejectedStatic("authority store does not retain generation lineage prefixes").into());
            }
            let _ = operation;self.create_authority(destination, Epoch::GENESIS, work, guard.as_deref_mut()).await
        }
    }

    /// Creates or authenticates an authority's exact first record.
    fn create_first_record<G: AuthorityGuard>(
        &self, authority: AuthorityId, commit: ProposedCommit, work: &mut AuthorityWork<'_>, mut guard: Option<&mut G>,
    ) -> impl Future<Output = Result<bool, AuthorityError<G::Error>>> + ProviderTask {
        async move {
            #[cfg(not(target_arch = "wasm32"))]
            if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
            first_record(self, authority, commit, work, guard.as_deref_mut()).await
        }
    }

    /// Publishes retention before a forked workspace's creation.
    fn fork_workspace<G: AuthorityGuard>(
        &self, fork: WorkspaceForkCommit, permit: PublicationPermit, work: &mut AuthorityWork<'_>, mut guard: Option<&mut G>,
    ) -> impl Future<Output = Result<WorkspaceForkOutcome, AuthorityError<G::Error>>> + ProviderTask {
        async move {
            #[cfg(not(target_arch = "wasm32"))]
            if work.original.is_some() && guard.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
            workspace_fork(self, fork, permit, work, guard.as_deref_mut()).await
        }
    }

    /// Retires authority metadata after workspace deletion is durable.
    fn retire_authority(&self, authority: AuthorityId, work: &mut AuthorityWork<'_>)
        -> impl Future<Output = Result<(), crate::AuthorityStoreError>> + ProviderTask {
        let _ = (authority, work);
        std::future::ready(Ok(()))
    }

    /// Reads the linearizable authority head.
    fn head(&self, authority: AuthorityId, work: &mut AuthorityWork<'_>)
        -> impl Future<Output = Result<Head, crate::AuthorityStoreError>> + ProviderTask;

    /// Compares and appends under the caller's publication permit and guard.
    fn compare_and_append<G: AuthorityGuard>(
        &self, request: GuardedAppend, work: &mut AuthorityWork<'_>, mut guard: Option<&mut G>,
    ) -> impl Future<Output = Result<(AppendOutcome, AuthorityAllocation), AuthorityError<G::Error>>> + ProviderTask;

    /// Reads an existing reservation for the exact operation identity.
    fn publication_reservation(&self, authority: AuthorityId, operation: OperationId, work: &mut AuthorityWork<'_>)
        -> impl Future<Output = Result<Option<PublicationReservation>, crate::AuthorityStoreError>> + ProviderTask {
        async move {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = work.original { original.admit(WorkCounters::UNCHARGED)?; }
            work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
            #[cfg(not(target_arch = "wasm32"))]
            if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported); }
            let _ = (authority, operation);
            Err(crate::AuthorityStoreError::RejectedStatic("authority backend cannot query original publication reservation evidence"))
        }
    }

    /// Acquires the exclusive publication gate at an exact head.
    fn reserve_publication(&self, authority: AuthorityId, expected: Head, operation: OperationId, work: &mut AuthorityWork<'_>)
        -> impl Future<Output = Result<ReservationOutcome, crate::AuthorityStoreError>> + ProviderTask {
        async move {
            #[cfg(not(target_arch = "wasm32"))]
            if work.original.is_some() { work.check()?; return Err(crate::AuthorityStoreError::NativeOriginalUnsupported); }
            let _ = (authority, expected, operation, work);
            Err(crate::AuthorityStoreError::RejectedStatic("authority backend does not support durable publication reservations"))
        }
    }

    /// Releases the exact publication reservation.
    fn release_publication(&self, reservation: PublicationReservation, work: &mut AuthorityWork<'_>)
        -> impl Future<Output = Result<(), crate::AuthorityStoreError>> + ProviderTask {
        async move {
            #[cfg(not(target_arch = "wasm32"))]
            if work.original.is_some() { work.check()?; return Err(crate::AuthorityStoreError::NativeOriginalUnsupported); }
            let _ = (reservation, work);
            Err(crate::AuthorityStoreError::RejectedStatic("authority backend does not support durable publication reservations"))
        }
    }

    /// Replays one bounded page while retaining its native storage.
    fn replay(&self, authority: AuthorityId, after: Sequence, limit: ReplayLimit, work: &mut AuthorityWork<'_>)
        -> impl Future<Output = Result<(Vec<crate::DurableCommit>, AuthorityAllocation), crate::AuthorityStoreError>> + ProviderTask;

    /// Advances a writer fence.
    fn fence(&self, authority: AuthorityId, expected: Head, work: &mut AuthorityWork<'_>)
        -> impl Future<Output = Result<FenceOutcome, crate::AuthorityStoreError>> + ProviderTask;

    /// Resolves an idempotent operation while retaining its native storage.
    fn find_operation(&self, authority: AuthorityId, operation: OperationId, work: &mut AuthorityWork<'_>)
        -> impl Future<Output = Result<(Option<crate::DurableCommit>, AuthorityAllocation), crate::AuthorityStoreError>> + ProviderTask;
}

/// The objects one authority record makes reachable, which must be durable before the record is appended.
#[derive(Clone, Copy, Debug)]
pub enum PublicationScope<'a> {
    /// The complete authenticated closure of one published generation.
    Closure {
        /// Every object the record reaches.
        objects: &'a [ObjectId],
        /// The store's [`AsyncObjectStore::collection_sweeps`] before the closure was proven.
        proven_at: u64,
    },
    /// Every object admitted so far, for a record whose closure was not enumerated.
    Everything,
}

/// Nonblocking immutable-object contract suitable for `IndexedDB` and remote I/O.
pub trait AsyncObjectStore: ProviderPlatform {
    /// Returns one exact decoded immutable representation when resident.
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
    ) -> impl Future<Output = ObjectResult<()>> + ProviderTask;

    /// Admits bytes using the caller's original native work and intrinsic custody.
    #[cfg(not(target_arch = "wasm32"))]
    fn put_with_native_work(
        &self,
        _object_id: ObjectId,
        _bytes: OwnedBytes,
        _work: &crate::materializer::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<(), crate::storage::ObjectStoreError>> + ProviderTask {
        async { Err(crate::storage::ObjectStoreError::NativeUnavailable) }
    }

    /// Asynchronously admits one object whose identity its construction already proved.
    fn put_hashed(
        &self,
        object: crate::storage::HashedObject,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<()>> + ProviderTask {
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
    ) -> impl Future<Output = Result<(), crate::storage::ObjectStoreError>> + ProviderTask {
        async { Err(crate::storage::ObjectStoreError::NativeUnavailable) }
    }

    /// Asynchronously admits an ordered bounded group of verified immutable objects.
    fn put_many(
        &self,
        writes: &[ObjectWrite],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<()>> + ProviderTask {
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
    ) -> impl Future<Output = Result<(), crate::storage::ObjectStoreError>> + ProviderTask {
        async { Err(crate::storage::ObjectStoreError::NativeUnavailable) }
    }

    /// Asynchronously admits an ordered bounded group of objects whose identities their construction already proved.
    fn put_many_hashed(
        &self,
        objects: &[crate::storage::HashedObject],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<()>> + ProviderTask {
        let writes = objects
            .iter()
            .map(|object| ObjectWrite {
                object_id: object.object_id(),
                bytes: object.bytes().clone(),
            })
            .collect::<Vec<_>>();
        async move { self.put_many(&writes, budget, cancellation).await }
    }

    /// Makes every admitted object in `scope` crash-durable before an authority record may reference it, and keeps it from collection until the returned hold drops, which the caller does after writing the record.
    fn flush_before_publish(
        &self,
        _scope: PublicationScope<'_>,
        _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<crate::PublicationHold>> + ProviderTask {
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
    ) -> impl Future<Output = Result<crate::PublicationHold, crate::storage::ObjectStoreError>> + ProviderTask {
        async { Err(crate::storage::ObjectStoreError::NativeUnavailable) }
    }
    /// Prepares the original canonical append after its closure has been flushed.
    fn prepare_generation_publication(
        &self,
        _request: &crate::kernel::PublishGenerationRequest,
        _proof: &crate::kernel::GenerationProof,
        _append: &GuardedAppend,
        _scope: PublicationScope<'_>,
        _budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<()>> + ProviderTask {
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
    fn historical_generation_evidence(
        &self,
        _authority_id: crate::AuthorityId,
        _record: &crate::DurableCommit,
        _maximum_bytes: u64,
        _maximum_objects: u64,
        _budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<Option<crate::kernel::HistoricalGenerationEvidence>>> + ProviderTask {
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
    #[cfg(not(target_arch = "wasm32"))]
    fn prepare_generation_publication_with_native_work(
        &self,
        _request: &crate::kernel::PublishGenerationRequest,
        _proof: &crate::kernel::GenerationProof,
        _append: &crate::GuardedAppend,
        _scope: crate::PublicationScope<'_>,
        _original: &crate::NativeMaterializationWork,
        _cancellation: &crate::CancellationToken,
    ) -> impl Future<Output = Result<(), crate::storage::ObjectStoreError>> + ProviderTask {
        async { Err(crate::storage::ObjectStoreError::NativeUnavailable) }
    }

    /// The collection this store runs while it stays open, if it collects.
    fn collection(&self) -> Option<&Arc<crate::Collection>> {
        None
    }

    /// The count a proof records before it starts, for [`PublicationScope::Closure`].
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
    ) -> impl Future<Output = ObjectResult<ObjectRead>> + ProviderTask;

    /// Reads using the caller's original native budget, cancellation and journal.
    #[cfg(not(target_arch = "wasm32"))]
    fn read_with_native_work(
        &self,
        _object_id: ObjectId,
        _maximum_bytes: u64,
        _work: &crate::materializer::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<ObjectRead, crate::storage::ObjectStoreError>> + ProviderTask {
        async { Err(crate::storage::ObjectStoreError::NativeUnavailable) }
    }

    /// Reads through the same provider operation, recording its actual completed SDK prefix before a caller can decode, fail, or unwind afterward.
    #[doc(hidden)]
    fn read_with_custody(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        observation: Option<ObjectReadObservation<'_>>,
    ) -> impl Future<Output = ObjectResult<ObjectRead>> + ProviderTask {
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
    fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<Vec<ObjectRead>>> + ProviderTask;

    /// Reads the same actual backend frontier against the original native journal.
    #[cfg(not(target_arch = "wasm32"))]
    fn read_many_with_native_work(
        &self,
        _requests: &[ObjectReadRequest],
        _work: &crate::materializer::NativeMaterializationWork,
        _cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<(Vec<ObjectRead>, crate::materializer::NativeAllocation), crate::storage::ObjectStoreError>> + ProviderTask {
        async { Err(crate::storage::ObjectStoreError::NativeUnavailable) }
    }

    /// Asynchronously probes one exact object identity.
    fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<bool>> + ProviderTask;
}

/// Explicit opt-in for synchronous stores that are safe to complete inline.
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

/// Explicit sequential implementation for adapters whose physical backend has no multi-get primitive.
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
        Ok(values) => Ok(crate::storage::ObjectReceipt { value: values.0, work }),
        Err(error) => Err(crate::storage::ObjectFailure::new(
            sequential_read_error(error), work,
        )),
    }
}

/// Explicit sequential native adapter using the caller's original work and allocation custody.
#[cfg(not(target_arch = "wasm32"))]
pub async fn read_many_sequential_with_native_work<S: AsyncObjectStore + ?Sized>(
    store: &S,
    requests: &[ObjectReadRequest],
    work: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<(Vec<ObjectRead>, crate::materializer::NativeAllocation), crate::storage::ObjectStoreError> {
    let mut accounting = crate::kernel::allocation::KernelAccounting::native(work, cancellation);
    let result = read_many_sequential_engine(store, requests, &mut accounting, cancellation)
        .await.map_err(sequential_read_error)?;
    match result.1 {
        crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) => {
            crate::async_storage::native_read_batch(result.0, allocation)
        }
        crate::kernel::allocation::KernelOwnedAllocation::Standalone => {
            drop(result.0);
            Err(crate::storage::ObjectStoreError::Corrupt)
        }
    }
}

async fn read_many_sequential_engine<S: AsyncObjectStore + ?Sized>(
    store: &S,
    requests: &[ObjectReadRequest],
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(Vec<ObjectRead>, crate::kernel::allocation::KernelOwnedAllocation), crate::FsError> {
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
    let mut values = (Vec::new(), allocation);
    values.0.try_reserve_exact(requests.len())
        .map_err(crate::storage::ObjectStoreError::MemoryTableAllocationFailed)?;
    let actual_bytes = values.0.capacity().checked_mul(size_of::<ObjectRead>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(crate::performance::WorkError::Overflow)?;
    accounting.reconcile_owned(&mut values.1, requested_bytes, actual_bytes)?;
    for request in requests {
        accounting.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = accounting.native_work() {
            let read = store.read_with_native_work(
                request.object_id, request.maximum_bytes, work, cancellation,
            ).await?;
            values.0.push(read);
            continue;
        }
        let result = store.read(
            request.object_id, request.maximum_bytes, accounting.remaining()?, cancellation,
        ).await;
        match result {
            Ok(receipt) => {
                let retained = match receipt.value.retention {
                    crate::storage::ObjectReadRetention::Shared => 0,
                    crate::storage::ObjectReadRetention::Owned { logical_bytes } => logical_bytes,
                };
                merge_completed_object_read(accounting, receipt.work, retained)?;
                values.0.push(receipt.value);
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

impl<T: AsyncAuthorityStore + ?Sized> AsyncAuthorityStore for Arc<T> {
    fn supports_generation_lineage_prefix(&self) -> bool {
        self.as_ref().supports_generation_lineage_prefix()
    }

    async fn create_authority<G: AuthorityGuard>(&self, authority: AuthorityId, epoch: Epoch, work: &mut AuthorityWork<'_>, guard: Option<&mut G>) -> Result<CreateAuthorityOutcome, AuthorityError<G::Error>> {AsyncAuthorityStore::create_authority(self.as_ref(), authority, epoch, work, guard).await
    }
    async fn fork_authority<G: AuthorityGuard>(&self, source: GenerationForkSource, destination: AuthorityId, operation: OperationId, work: &mut AuthorityWork<'_>, guard: Option<&mut G>) -> Result<CreateAuthorityOutcome, AuthorityError<G::Error>> {
        AsyncAuthorityStore::fork_authority(self.as_ref(), source, destination, operation, work, guard).await
    }
    async fn create_first_record<G: AuthorityGuard>(&self, authority: AuthorityId, commit: ProposedCommit, work: &mut AuthorityWork<'_>, guard: Option<&mut G>) -> Result<bool, AuthorityError<G::Error>> {
        AsyncAuthorityStore::create_first_record(self.as_ref(), authority, commit, work, guard).await
    }
    async fn fork_workspace<G: AuthorityGuard>(&self, fork: WorkspaceForkCommit, permit: PublicationPermit, work: &mut AuthorityWork<'_>, guard: Option<&mut G>) -> Result<WorkspaceForkOutcome, AuthorityError<G::Error>> {
        AsyncAuthorityStore::fork_workspace(self.as_ref(), fork, permit, work, guard).await
    }
    async fn compare_and_append<G: AuthorityGuard>(&self, request: GuardedAppend, work: &mut AuthorityWork<'_>, guard: Option<&mut G>) -> Result<(AppendOutcome, AuthorityAllocation), AuthorityError<G::Error>> {
        AsyncAuthorityStore::compare_and_append(self.as_ref(), request, work, guard).await
    }
    async fn head(&self, authority: AuthorityId, work: &mut AuthorityWork<'_>) -> Result<Head, crate::AuthorityStoreError> {
        AsyncAuthorityStore::head(self.as_ref(), authority, work).await
    }
    async fn retire_authority(&self, authority: AuthorityId, work: &mut AuthorityWork<'_>) -> Result<(), crate::AuthorityStoreError> {
        AsyncAuthorityStore::retire_authority(self.as_ref(), authority, work).await
    }
    async fn replay(&self, authority: AuthorityId, after: Sequence, limit: ReplayLimit, work: &mut AuthorityWork<'_>) -> Result<(Vec<crate::DurableCommit>, AuthorityAllocation), crate::AuthorityStoreError> {
        AsyncAuthorityStore::replay(self.as_ref(), authority, after, limit, work).await
    }
    async fn fence(&self, authority: AuthorityId, expected: Head, work: &mut AuthorityWork<'_>) -> Result<FenceOutcome, crate::AuthorityStoreError> {
        AsyncAuthorityStore::fence(self.as_ref(), authority, expected, work).await
    }
    async fn find_operation(&self, authority: AuthorityId, operation: OperationId, work: &mut AuthorityWork<'_>) -> Result<(Option<crate::DurableCommit>, AuthorityAllocation), crate::AuthorityStoreError> {
        AsyncAuthorityStore::find_operation(self.as_ref(), authority, operation, work).await
    }
    async fn publication_reservation(&self, authority: AuthorityId, operation: OperationId, work: &mut AuthorityWork<'_>) -> Result<Option<PublicationReservation>, crate::AuthorityStoreError> {
        AsyncAuthorityStore::publication_reservation(self.as_ref(), authority, operation, work).await
    }
    async fn reserve_publication(&self, authority: AuthorityId, expected: Head, operation: OperationId, work: &mut AuthorityWork<'_>) -> Result<ReservationOutcome, crate::AuthorityStoreError> {
        AsyncAuthorityStore::reserve_publication(self.as_ref(), authority, expected, operation, work).await
    }
    async fn release_publication(&self, reservation: PublicationReservation, work: &mut AuthorityWork<'_>) -> Result<(), crate::AuthorityStoreError> {
        AsyncAuthorityStore::release_publication(self.as_ref(), reservation, work).await
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
    ) -> Result<ObjectRead, crate::storage::ObjectStoreError> {
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
    ) -> Result<(Vec<ObjectRead>, crate::materializer::NativeAllocation), crate::storage::ObjectStoreError> {
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
